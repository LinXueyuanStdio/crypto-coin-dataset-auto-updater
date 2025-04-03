import os
import sys
import time
import shutil
import pandas as pd
from datetime import datetime
from binance.client import Client
from dotenv import load_dotenv
from huggingface_hub import HfApi
from xlin import cp, rm, element_mapping
from functools import partial
import re

# Load environment variables
load_dotenv()

HF_TOKEN = os.getenv("HF_TOKEN")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# Configure proxy settings (will be used for Binance API calls)
proxies = {
    "http": os.getenv("HTTP_PROXY"),
    "https": os.getenv("HTTPS_PROXY"),
}


def create_binance_client(max_retries=3):
    """Create Binance client with retry logic."""
    local_proxies = {
        "http": os.getenv("HTTP_PROXY"),
        "https": os.getenv("HTTPS_PROXY"),
    }

    for attempt in range(max_retries):
        try:
            client = Client(
                BINANCE_API_KEY,
                BINANCE_API_SECRET,
                {
                    "proxies": local_proxies,
                    "timeout": 30,
                    "verify": True,
                },
            )
            # Test the connection
            client.ping()
            print("Successfully connected to Binance API")
            return client
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                print("Waiting 10 seconds before retry...")
                time.sleep(10)
                os.system("sudo service tor restart")
                time.sleep(5)
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
    print(f"Cleaned folder: {folder_path}")


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
    print(f"Dataset downloaded to {output_dir}")


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
            print(f"Fetched data saved to {output_file}")
            return
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                print("Waiting 20 seconds before retry...")
                time.sleep(20)
                os.system("sudo service tor restart")
                time.sleep(5)
            else:
                raise Exception(
                    f"Failed to fetch data for {symbol} at interval {interval} after {max_retries} attempts"
                )


def merge_datasets(existing_file, new_file, output_file):
    """Merge existing and new datasets."""
    new_data = pd.read_csv(new_file)
    new_data["Open time"] = pd.to_datetime(new_data["Open time"])
    if os.path.exists(existing_file):
        existing_data = pd.read_csv(existing_file)
        existing_data["Open time"] = pd.to_datetime(existing_data["Open time"])
        merged_data = pd.concat([existing_data, new_data])
    else:
        merged_data = new_data
    merged_data.drop_duplicates(subset="Open time", inplace=True)
    merged_data.sort_values(by="Open time", inplace=True)
    merged_data.to_csv(output_file, index=False)
    print(f"Merged dataset (from {merged_data['Open time'].min()} to {merged_data['Open time'].max()}) saved to {output_file}")


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
        print("Dataset updated.")
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

    # Step 1: Clean folders (do not remove metadata until after successful upload)
    # clean_folder(DATA_FOLDER)
    # clean_folder(NEW_DATA_FOLDER)
    # clean_folder(MERGED_FOLDER)

    # Step 2: Download dataset into DATA_FOLDER
    # download_dataset(dataset_slug, DATA_FOLDER)

    # Step 3: Fetch new data for all timeframes
    # past 2 days
    start_date = (datetime.now() - pd.DateOffset(years=10)).strftime("%Y-%m-%d")
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
            print(f"File {new_file} already exists. Skipping...")
            return True, new_file
        print(f"Fetching data for {pair} at interval {tf_name} from {start_date} to {end_date}")
        fetch_binance_data(pair, tf_interval, start_date, end_date, new_file)
        if pd.read_csv(new_file).empty:
            print(f"Warning: {new_file} is empty after fetching data.")
            return False, None
        return True, new_file

    element_mapping(jobs, f, thread_pool_size=5)

    # Step 4: Merge new data with old datasets and save the merged files in MERGED_FOLDER
    for pair in available_pairs:
        for tf_name, _ in timeframes.items():
            old_file = os.path.join(DATA_FOLDER, f"{pair}_{tf_name}.csv")
            new_file = os.path.join(NEW_DATA_FOLDER, f"{pair}_{tf_name}.csv")
            merged_file = os.path.join(MERGED_FOLDER, f"{pair}_{tf_name}.csv")
            if os.path.exists(merged_file):
                print(f"Merged file {merged_file} already exists. Skipping...")
                continue
            merge_datasets(old_file, new_file, merged_file)

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

    # Step 5: Upload updated datasets from MERGED_FOLDER with a retry loop until successful
    current_date = datetime.now().strftime("%B, %d %Y, %H:%M:%S")
    upload_successful = False
    while not upload_successful:
        try:
            upload(DATA_FOLDER, dataset_slug, f"Updated at {current_date}")
            upload_successful = True
        except Exception as e:
            print(f"Upload failed: {e}. Retrying in 60 seconds...")
            time.sleep(60)

    # Step 6: Once upload is successful, clean all folders
    # rm(DATA_FOLDER, debug=True)
    # rm(NEW_DATA_FOLDER, debug=True)
    # rm(MERGED_FOLDER, debug=True)
    print("All folders cleaned.")


if __name__ == "__main__":
    max_attempts = 10  # Maximum number of global attempts
    attempt = 0
    while attempt < max_attempts:
        try:
            main()
            break  # Exit loop if main() succeeds
        except Exception as e:
            attempt += 1
            print(f"Global attempt {attempt}/{max_attempts} failed: {e}")
            print("Retrying in 60 seconds...")
            time.sleep(60)
    else:
        print("Max attempts reached. Exiting.")
        sys.exit(1)
