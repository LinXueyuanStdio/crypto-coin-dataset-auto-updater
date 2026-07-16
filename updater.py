import os
import sys
import time
import shutil
import logging
import pandas as pd
from datetime import datetime
from binance.client import Client
from dotenv import load_dotenv
from huggingface_hub import HfApi
from xlin import cp, rm, element_mapping
from functools import partial
import re
import multiprocessing
import traceback
import warnings

# Load environment variables
load_dotenv()

# Configure logging so every attempt/retry is timestamped and immediately
# flushed to stdout. This makes it much easier to see, from the GitHub
# Actions logs, how long each retry took and why the job might be stuck
# looping (e.g. persistent proxy/Binance failures) instead of just seeing a
# wall of un-timestamped "retrying..." messages.
_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
)
logging.basicConfig(level=logging.INFO, handlers=[_stdout_handler], force=True)
logger = logging.getLogger("updater")

HF_TOKEN = os.getenv("HF_TOKEN")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# GitHub Actions runners are hosted in datacenters (e.g. Azure US) whose IPs
# are flagged by Binance's compliance geo-fencing, causing api.binance.com to
# reject requests with a "restricted location" (451) error. Binance publishes
# a separate public market-data mirror that serves the same public endpoints
# (klines, ping, etc.) used by this script without that geo-restriction, so we
# point the client at it instead of relying on a proxy (e.g. Tor exit nodes
# are themselves blocked by Binance/CloudFront, which made the previous proxy
# based workaround unreliable).
BINANCE_PUBLIC_DATA_API_URL = "https://data-api.binance.vision/api"


def create_binance_client(max_retries=3):
    """Create Binance client with retry logic."""
    for attempt in range(max_retries):
        start = time.monotonic()
        try:
            client = Client(
                BINANCE_API_KEY,
                BINANCE_API_SECRET,
                {
                    "timeout": 30,
                    "verify": True,
                },
                # Skip the constructor's built-in ping, since it targets the
                # default api.binance.com host (which we override below)
                # before we get a chance to point it at the public mirror.
                ping=False,
            )
            # Use Binance's public market-data mirror to avoid geo-restriction
            # blocks on the regular API host when running from GitHub Actions.
            # `client.API_URL` is set as a plain instance attribute during
            # `Client.__init__` (see python-binance's BaseClient), so
            # overriding it here is the supported way to repoint the client
            # at an alternate host; there is no constructor option for a
            # fully custom base URL.
            client.API_URL = BINANCE_PUBLIC_DATA_API_URL
            # `ping()` is Binance's public, unauthenticated connectivity
            # check (same call the constructor would have made); it doesn't
            # validate API key/secret, matching the previous behavior before
            # this change.
            client.ping()
            logger.info("Successfully connected to Binance API (took %.1fs)", time.monotonic() - start)
            return client
        except Exception as e:
            logger.warning(
                "create_binance_client attempt %d/%d failed after %.1fs: %s",
                attempt + 1, max_retries, time.monotonic() - start, e,
            )
            if attempt < max_retries - 1:
                logger.info("Waiting 10 seconds before retry...")
                time.sleep(10)
            else:
                raise


# Initialize Binance client
client = create_binance_client()

# Base directory of the script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(BASE_DIR, "data")
NEW_DATA_FOLDER = os.path.join(BASE_DIR, "new_data")
MERGED_FOLDER = os.path.join(BASE_DIR, "merged_data")  # New folder for merged files


def clean_folder(folder_path):
    """Clean the specified folder."""
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    logger.info("Cleaned folder: %s", folder_path)


def download_dataset(dataset_slug, output_dir):
    """Download the dataset."""
    api = HfApi(token=HF_TOKEN)
    api.auth_check(dataset_slug, repo_type="dataset")
    api.hf_hub_download(
        repo_id=dataset_slug,
        filename="*",
        repo_type="dataset",
        revision="main",
        cache_dir=output_dir,
        force_download=True,
    )
    logger.info("Dataset downloaded to %s", output_dir)


def fetch_binance_data(
    symbol,
    interval,
    start_date,
    end_date,
    output_file,
    max_retries=3,
):
    """Fetch historical data from Binance with retry logic."""
    for attempt in range(max_retries):
        start = time.monotonic()
        try:
            client = create_binance_client()
            klines = client.get_historical_klines(
                symbol,
                interval,
                start_date,
                end_date,
            )
            columns = [
                "Open time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "Close time",
                "Quote asset volume",
                "Number of trades",
                "Taker buy base asset volume",
                "Taker buy quote asset volume",
                "Ignore",
            ]
            df = pd.DataFrame(klines, columns=columns)
            df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
            df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")
            df.to_csv(output_file, index=False)
            logger.info("Fetched data saved to %s (took %.1fs)", output_file, time.monotonic() - start)
            return
        except Exception as e:
            logger.warning(
                "fetch_binance_data(%s, %s) attempt %d/%d failed after %.1fs: %s",
                symbol, interval, attempt + 1, max_retries, time.monotonic() - start, e,
            )
            if attempt < max_retries - 1:
                logger.info("Waiting 20 seconds before retry...")
                time.sleep(20)
            else:
                raise Exception(
                    f"Failed to fetch data for {symbol} at interval {interval} after {max_retries} attempts"
                )


def merge_datasets(existing_file, new_file, output_file):
    """Merge existing and new datasets."""
    def _safe_parse(dt_series):
        """Safely parse a datetime series that may contain mixed formats (date-only & full timestamps)."""
        if pd.api.types.is_datetime64_any_dtype(dt_series):
            return dt_series
        # First attempt: mixed (pandas >=2.0)
        try:
            parsed = pd.to_datetime(dt_series, format="mixed", errors="coerce")
        except TypeError:
            # Older pandas without format='mixed'
            parsed = pd.to_datetime(dt_series, errors="coerce", infer_datetime_format=True)
        # Fallback for any remaining NaT: try common explicit formats
        if parsed.isna().any():
            remaining = dt_series[parsed.isna()]
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
                try:
                    trial = pd.to_datetime(remaining, format=fmt, errors="coerce")
                    parsed.loc[trial.notna()] = trial.dropna()
                except Exception:
                    pass
        # If still NaT values exist, warn & drop later
        if parsed.isna().any():
            sample = dt_series[parsed.isna()].head(5).tolist()
            warnings.warn(
                f"Some 'Open time' values could not be parsed ({len(parsed.isna())} NaT). Sample: {sample}"
            )
        return parsed

    new_data = pd.read_csv(new_file)
    if "Open time" not in new_data.columns:
        raise ValueError(f"Missing 'Open time' column in new file: {new_file}")
    new_data["Open time"] = _safe_parse(new_data["Open time"])
    if os.path.exists(existing_file):
        existing_data = pd.read_csv(existing_file)
        if "Open time" not in existing_data.columns:
            warnings.warn(f"Existing file missing 'Open time' column, using only new data: {existing_file}")
            merged_data = new_data
        else:
            existing_data["Open time"] = _safe_parse(existing_data["Open time"])
            merged_data = pd.concat([existing_data, new_data])
    else:
        merged_data = new_data
    # Drop rows with unparsable times
    before = len(merged_data)
    merged_data = merged_data.dropna(subset=["Open time"])  # remove rows where datetime failed
    after = len(merged_data)
    if after < before:
        logger.warning("Dropped %d rows with invalid 'Open time' in merge of %s + %s", before - after, existing_file, new_file)
    merged_data.drop_duplicates(subset="Open time", inplace=True)
    merged_data.sort_values(by="Open time", inplace=True)
    merged_data.to_csv(output_file, index=False)
    logger.info("Merged dataset (from %s to %s) saved to %s", merged_data['Open time'].min(), merged_data['Open time'].max(), output_file)


def upload(upload_folder, dataset_slug, version_notes):
    """Upload the updated dataset using the proxy."""
    # Temporarily remove proxy settings for upload
    original_http_proxy = os.environ.pop("HTTP_PROXY", None)
    original_https_proxy = os.environ.pop("HTTPS_PROXY", None)
    try:
        api = HfApi(token=HF_TOKEN)
        api.upload_folder(
            folder_path=upload_folder,
            repo_id=dataset_slug,
            repo_type="dataset",
            commit_message=version_notes,
            create_pr=False,
            commit_description=version_notes,
        )
        logger.info("Dataset updated.")
    finally:
        # Restore proxy settings after upload
        if original_http_proxy:
            os.environ["HTTP_PROXY"] = original_http_proxy
        if original_https_proxy:
            os.environ["HTTPS_PROXY"] = original_https_proxy


def main():
    dataset_slug = "linxy/CryptoCoin"
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(NEW_DATA_FOLDER, exist_ok=True)
    os.makedirs(MERGED_FOLDER, exist_ok=True)

    logger.info("Starting dataset update process...")

    # Step 1: Clean folders (do not remove metadata until after successful upload)
    # clean_folder(DATA_FOLDER)
    # clean_folder(NEW_DATA_FOLDER)
    # clean_folder(MERGED_FOLDER)

    # Step 2: Download dataset into DATA_FOLDER
    # download_dataset(dataset_slug, DATA_FOLDER)

    # Step 3: Fetch new data for all timeframes
    # past 2 days
    start_date = (datetime.now() - pd.DateOffset(days=2)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    timeframes = {
        # "1M": Client.KLINE_INTERVAL_1MONTH,
        # "1w": Client.KLINE_INTERVAL_1WEEK,
        # "3d": Client.KLINE_INTERVAL_3DAY,
        "1d": Client.KLINE_INTERVAL_1DAY,
        "12h": Client.KLINE_INTERVAL_12HOUR,
        "8h": Client.KLINE_INTERVAL_8HOUR,
        "6h": Client.KLINE_INTERVAL_6HOUR,
        "4h": Client.KLINE_INTERVAL_4HOUR,
        "2h": Client.KLINE_INTERVAL_2HOUR,
        "1h": Client.KLINE_INTERVAL_1HOUR,
        "30m": Client.KLINE_INTERVAL_30MINUTE,
        "15m": Client.KLINE_INTERVAL_15MINUTE,
        "5m": Client.KLINE_INTERVAL_5MINUTE,
        # "3m": Client.KLINE_INTERVAL_3MINUTE,
        # "1m": Client.KLINE_INTERVAL_1MINUTE,
        # "1s": Client.KLINE_INTERVAL_1SECOND,
    }
    available_pairs = [
        "1INCHUSDT",
        "AAVEUSDT",
        "ADAUSDT",
        "ALGOUSDT",
        "AVAXUSDT",
        "BATUSDT",
        "BCHUSDT",
        "BNBUSDT",
        "BTCUSDT",
        "CHZUSDT",
        "COMPUSDT",
        "CRVUSDT",
        "DOGEUSDT",
        "DOTUSDT",
        "EOSUSDT",
        "ETCUSDT",
        "ETHUSDT",
        "FILUSDT",
        "FTMUSDT",
        "FTTUSDT",
        "HBARUSDT",
        "HNTUSDT",
        "ICPUSDT",
        "KSMUSDT",
        "LDOUSDT",
        "LINKUSDT",
        "LTCUSDT",
        "LUNAUSDT",
        "MANAUSDT",
        "MATICUSDT",
        "RUNEUSDT",
        "SANDUSDT",
        "SHIBUSDT",
        "SNXUSDT",
        "SOLUSDT",
        "SUSHIUSDT",
        "TRXUSDT",
        "UNIUSDT",
        "WAVESUSDT",
        "XEMUSDT",
        "XLMUSDT",
        "XRPUSDT",
        "YFIUSDT",
        "ZILUSDT",
        "ZRXUSDT",
    ]
    jobs = []
    for pair in available_pairs:
        for tf_name, tf_interval in timeframes.items():
            jobs.append((pair, tf_name, tf_interval))

    def f(row):
        pair, tf_name, tf_interval = row
        new_file = os.path.join(NEW_DATA_FOLDER, f"{pair}_{tf_name}.csv")
        if os.path.exists(new_file):
            logger.info("File %s already exists. Skipping...", new_file)
            return True, new_file
        logger.info("Fetching data for %s at interval %s from %s to %s", pair, tf_name, start_date, end_date)
        fetch_binance_data(pair, tf_interval, start_date, end_date, new_file)
        if pd.read_csv(new_file).empty:
            logger.warning("%s is empty after fetching data.", new_file)
            return False, None
        return True, new_file

    logger.info("Fetching new data from Binance...")
    fetch_workers = int(os.getenv("FETCH_WORKERS", str(multiprocessing.cpu_count())))
    logger.info("Using %d fetch workers", fetch_workers)
    element_mapping(jobs, f, thread_pool_size=fetch_workers)

    # Step 4: Merge new data with old datasets and save the merged files in MERGED_FOLDER
    logger.info("Merging datasets...")
    for pair in available_pairs:
        for tf_name, _ in timeframes.items():
            old_file = os.path.join(DATA_FOLDER, f"{pair}_{tf_name}.csv")
            new_file = os.path.join(NEW_DATA_FOLDER, f"{pair}_{tf_name}.csv")
            merged_file = os.path.join(MERGED_FOLDER, f"{pair}_{tf_name}.csv")
            if os.path.exists(merged_file):
                logger.info("Merged file %s already exists. Skipping...", merged_file)
                continue
            merge_datasets(old_file, new_file, merged_file)

    logger.info("Copying merged files to %s...", DATA_FOLDER)
    for pair in available_pairs:
        for tf_name, _ in timeframes.items():
            merged_file = os.path.join(MERGED_FOLDER, f"{pair}_{tf_name}.csv")
            cp(merged_file, DATA_FOLDER, force_overwrite=True, verbose=True)
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d %H:%M:%S")
    with open("data/README.md", "r") as file:
        readme = file.read()
    readme = re.sub(
        r"Last updated on `(.*?)`", f"Last updated on `{today_str}`", readme
    )
    with open("data/README.md", "w") as file:
        file.write(readme)

    logger.info("Uploading updated datasets...")
    # Step 5: Upload updated datasets from MERGED_FOLDER with a retry loop until successful
    current_date = datetime.now().strftime("%B, %d %Y, %H:%M:%S")
    upload_successful = False
    upload_attempt = 0
    max_upload_attempts = 10
    while not upload_successful and upload_attempt < max_upload_attempts:
        upload_attempt += 1
        start = time.monotonic()
        try:
            upload(DATA_FOLDER, dataset_slug, f"Updated at {current_date}")
            upload_successful = True
        except Exception as e:
            logger.error(
                "Upload attempt %d/%d failed after %.1fs: %s. Retrying in 60 seconds...",
                upload_attempt, max_upload_attempts, time.monotonic() - start, e,
            )
            time.sleep(60)
    if not upload_successful:
        raise RuntimeError(f"Upload failed after {max_upload_attempts} attempts")

    # Step 6: Once upload is successful, clean all folders
    # rm(DATA_FOLDER, debug=True)
    # rm(NEW_DATA_FOLDER, debug=True)
    # rm(MERGED_FOLDER, debug=True)
    logger.info("All folders cleaned.")


if __name__ == "__main__":
    max_attempts = 10  # Maximum number of global attempts
    attempt = 0
    run_start = time.monotonic()
    logger.info("Updater started. Global max attempts: %d", max_attempts)
    while attempt < max_attempts:
        attempt_start = time.monotonic()
        try:
            main()
            logger.info("Updater finished successfully in %.1fs (total elapsed %.1fs)",
                        time.monotonic() - attempt_start, time.monotonic() - run_start)
            break  # Exit loop if main() succeeds
        except Exception as e:
            attempt += 1
            traceback.print_exc()
            logger.error(
                "Global attempt %d/%d failed after %.1fs (total elapsed %.1fs): %s",
                attempt, max_attempts, time.monotonic() - attempt_start, time.monotonic() - run_start, e,
            )
            logger.info("Retrying in 60 seconds...")
            time.sleep(60)
    else:
        logger.error("Max attempts reached after %.1fs total. Exiting.", time.monotonic() - run_start)
        sys.exit(1)
