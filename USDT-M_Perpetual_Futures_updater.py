import io
import json
import os
import re
import sys
import time
import threading
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
import pandas as pd
from huggingface_hub import HfApi

# ----- logging (handlers attached lazily in __main__ to keep import side-effect-free) -----
logger = logging.getLogger("futures_updater")

# ----- HTTP session + optional auto-proxy -----
# A single pooled Session reuses connections (HTTP keep-alive) so high
# concurrency through a local proxy does not exhaust OS sockets (on Windows,
# a new connection per request quickly triggers WinError 10048). PROXY defaults
# to a local dev proxy for convenience; CI sets PROXY="" to disable it. When
# set it is applied to BOTH the CDN downloads (this session, via the process
# env at request time) and the Hugging Face upload (huggingface_hub reads the
# same env), so a plain `python USDT-M_Perpetual_Futures_updater.py` just works.
PROXY = os.getenv("PROXY", "http://127.0.0.1:4780")

SESSION = requests.Session()
_adapter = HTTPAdapter(pool_connections=32, pool_maxsize=128)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


def configure_proxy():
    """Route both CDN downloads and the HF upload through PROXY, if set."""
    if PROXY:
        os.environ["HTTP_PROXY"] = PROXY
        os.environ["HTTPS_PROXY"] = PROXY
        logger.info("Using proxy %s", PROXY)


# ----- config -----
BASE_URL = "https://data.binance.vision/data/futures/um"

INTERVALS = ["1d", "12h", "8h", "6h", "4h", "2h", "1h", "30m", "15m", "5m"]

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "count",
    "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]
METRICS_COLUMNS = [
    "create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
    "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
    "count_long_short_ratio", "sum_taker_long_short_vol_ratio",
]
FUNDING_COLUMNS = ["calc_time", "funding_interval_hours", "last_funding_rate"]

# Hardcoded fallback list used when the Binance API is unreachable.
# Updated 2026-07-13: removed 8 delisted symbols (EOS, FTM, FTT, HNT, LUNA,
# MATIC->POL, WAVES, XEM) that no longer trade on Binance Futures.
FALLBACK_SYMBOLS = [
    "1INCHUSDT", "AAVEUSDT", "ADAUSDT", "ALGOUSDT", "AVAXUSDT", "BATUSDT",
    "BCHUSDT", "BNBUSDT", "BTCUSDT", "CHZUSDT", "COMPUSDT", "CRVUSDT",
    "DOGEUSDT", "DOTUSDT", "ETCUSDT", "ETHUSDT", "FILUSDT",
    "HBARUSDT", "ICPUSDT", "KSMUSDT",
    "LDOUSDT", "LINKUSDT", "LTCUSDT", "MANAUSDT",
    "RUNEUSDT", "SANDUSDT", "1000SHIBUSDT", "SNXUSDT", "SOLUSDT", "SUSHIUSDT",
    "TRXUSDT", "UNIUSDT", "XLMUSDT", "XRPUSDT",
    "YFIUSDT", "ZILUSDT", "ZRXUSDT",
]

# Mutable module-level list: starts as the fallback, updated by resolve_symbols().
SYMBOLS = list(FALLBACK_SYMBOLS)


@dataclass(frozen=True)
class DataType:
    name: str
    path_segment: str
    per_interval: bool
    has_monthly: bool
    has_daily: bool
    time_col: str
    ms_time_cols: tuple
    columns: tuple
    floor: str
    output_suffix: str
    enabled: bool = True


DATA_TYPES = [
    DataType("klines", "klines", True, True, True, "open_time",
             ("open_time", "close_time"), tuple(KLINE_COLUMNS), "2020-01", ""),
    DataType("markPrice", "markPriceKlines", True, True, True, "open_time",
             ("open_time", "close_time"), tuple(KLINE_COLUMNS), "2020-01", "markPrice"),
    DataType("indexPrice", "indexPriceKlines", True, True, True, "open_time",
             ("open_time", "close_time"), tuple(KLINE_COLUMNS), "2020-01", "indexPrice"),
    DataType("premiumIndex", "premiumIndexKlines", True, True, True, "open_time",
             ("open_time", "close_time"), tuple(KLINE_COLUMNS), "2020-01", "premiumIndex"),
    DataType("metrics", "metrics", False, False, True, "create_time",
             (), tuple(METRICS_COLUMNS), "2021-01", "metrics"),
    DataType("fundingRate", "fundingRate", False, True, False, "calc_time",
             ("calc_time",), tuple(FUNDING_COLUMNS), "2020-01", "fundingRate"),
]


def output_filename(dt, symbol, interval):
    if dt.per_interval:
        if dt.output_suffix:
            return f"{symbol}_{dt.output_suffix}_{interval}.csv"
        return f"{symbol}_{interval}.csv"
    return f"{symbol}_{dt.output_suffix}.csv"


def parse_ym(s):
    y, m = s.split("-")
    return int(y), int(m)


def next_month(y, m):
    return (y + 1, 1) if m == 12 else (y, m + 1)


def months_range(start_ym, end_ym_exclusive):
    out = []
    cur = start_ym
    while cur < end_ym_exclusive:
        out.append(cur)
        cur = next_month(*cur)
    return out


def file_url(dt, symbol, interval, freq, period):
    if freq == "monthly":
        y, m = period
        stamp = f"{y:04d}-{m:02d}"
    else:
        stamp = period.isoformat()  # datetime.date -> "YYYY-MM-DD"
    if dt.per_interval:
        base = f"{BASE_URL}/{freq}/{dt.path_segment}/{symbol}/{interval}"
        return f"{base}/{symbol}-{interval}-{stamp}.zip"
    base = f"{BASE_URL}/{freq}/{dt.path_segment}/{symbol}"
    return f"{base}/{symbol}-{dt.path_segment}-{stamp}.zip"


def _month_of(d):
    return (d.year, d.month)


def _days_in_month(ym, end_date):
    y, m = ym
    first = date(y, m, 1)
    ny, nm = next_month(y, m)
    last = date(ny, nm, 1) - timedelta(days=1)
    out, d = [], first
    while d <= last and d <= end_date:
        out.append(d)
        d += timedelta(days=1)
    return out


def enumerate_periods(dt, last_dt, end_date):
    if last_dt is None:
        fy, fm = parse_ym(dt.floor)
        start = date(fy, fm, 1)
    else:
        start = last_dt.date()
    if start > end_date:
        return [], []

    months, days = [], []
    if dt.has_monthly and dt.has_daily:
        months = months_range(_month_of(start), _month_of(end_date))
        first_of_end = date(end_date.year, end_date.month, 1)
        d = max(start, first_of_end)
        while d <= end_date:
            days.append(d)
            d += timedelta(days=1)
    elif dt.has_monthly and not dt.has_daily:
        months = months_range(_month_of(start), next_month(*_month_of(end_date)))
    else:  # daily only
        d = start
        while d <= end_date:
            days.append(d)
            d += timedelta(days=1)
    return months, days


def read_zip_csv(content, columns):
    df = pd.read_csv(io.BytesIO(content), compression="zip", header=None, dtype=str)
    if len(df) and str(df.iloc[0, 0]).strip() == columns[0]:
        df = df.iloc[1:].reset_index(drop=True)
    df.columns = columns
    return df


def normalize_times(df, dt):
    for col in dt.ms_time_cols:
        df[col] = pd.to_datetime(pd.to_numeric(df[col], errors="coerce"), unit="ms")
    if dt.time_col not in dt.ms_time_cols:
        df[dt.time_col] = pd.to_datetime(df[dt.time_col], errors="coerce")
    return df


def download_series_file(url, columns, max_retries=3, timeout=30):
    for attempt in range(max_retries):
        try:
            resp = SESSION.get(url, timeout=timeout)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return read_zip_csv(resp.content, columns)
        except requests.RequestException:
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise
    return None


# A missing monthly zip only triggers daily fallback when the month is recent
# (its monthly bulk may simply not be published yet). For older months a 404
# means the data genuinely does not exist (delisted / pre-listing), so falling
# back would sweep ~30 non-existent daily files per month for years of history.
FALLBACK_MONTHS = 2


def _months_before_end(ym, end_date):
    return (end_date.year - ym[0]) * 12 + (end_date.month - ym[1])


def fetch_series(dt, symbol, interval, last_dt, end_date, downloader=download_series_file):
    months, days = enumerate_periods(dt, last_dt, end_date)
    day_set = set(days)
    frames = []
    for period in months:
        frame = downloader(file_url(dt, symbol, interval, "monthly", period), list(dt.columns))
        if frame is not None and len(frame):
            frames.append(frame)
        elif dt.has_daily and 0 <= _months_before_end(period, end_date) <= FALLBACK_MONTHS:
            # Monthly bulk for this recent month is unavailable (e.g. a just-ended
            # month whose monthly zip is not published yet). Fall back to that
            # month's daily dumps so the month is never silently skipped.
            day_set.update(_days_in_month(period, end_date))
    for day in sorted(day_set):
        frame = downloader(file_url(dt, symbol, interval, "daily", day), list(dt.columns))
        if frame is not None and len(frame):
            frames.append(frame)
    if not frames:
        return None
    return normalize_times(pd.concat(frames, ignore_index=True), dt)


def latest_stored_time(path, time_col):
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, usecols=[time_col])
    except (ValueError, pd.errors.EmptyDataError):
        return None
    if df.empty:
        return None
    ts = pd.to_datetime(df[time_col], errors="coerce").max()
    return None if pd.isna(ts) else ts.to_pydatetime()


def merge_frames(existing_df, new_df, time_col):
    new_df = new_df.copy()
    new_df[time_col] = pd.to_datetime(new_df[time_col], errors="coerce")
    if existing_df is not None and len(existing_df):
        existing_df = existing_df.copy()
        existing_df[time_col] = pd.to_datetime(existing_df[time_col], errors="coerce")
        merged = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        merged = new_df
    merged = merged.dropna(subset=[time_col])
    merged = merged.drop_duplicates(subset=time_col).sort_values(time_col)
    return merged.reset_index(drop=True)


def merge_datasets(existing_file, new_file, output_file, time_col):
    new_df = pd.read_csv(new_file, dtype=str)
    existing_df = pd.read_csv(existing_file, dtype=str) if os.path.exists(existing_file) else None
    merged = merge_frames(existing_df, new_df, time_col)
    merged.to_csv(output_file, index=False)
    return merged


class Budget:
    def __init__(self, minutes):
        self.limit_seconds = float(minutes) * 60.0
        self.start = time.monotonic()

    def exceeded(self):
        return (time.monotonic() - self.start) >= self.limit_seconds


@dataclass
class Job:
    dt: DataType
    symbol: str
    interval: object  # str | None


def fetch_usdt_perpetual_symbols():
    """Fetch all TRADING USDT-M perpetual symbols from Binance Futures API.

    Returns a sorted list of symbol strings (e.g. ['BTCUSDT', 'ETHUSDT', ...]).
    Falls back to FALLBACK_SYMBOLS if the API is unreachable or returns no results.
    Results are cached in SYMBOLS_CACHE for SYMBOLS_CACHE_TTL_HOURS.
    """
    # ---- check cache first ----
    if os.path.exists(SYMBOLS_CACHE):
        try:
            with open(SYMBOLS_CACHE, encoding="utf-8") as f:
                cached = json.load(f)
            age = time.time() - cached.get("_fetched_at", 0)
            if age < SYMBOLS_CACHE_TTL_HOURS * 3600:
                symbols = cached.get("symbols", [])
                if symbols:
                    logger.info(
                        "Using cached symbol list (%d symbols, age=%.1fh)",
                        len(symbols), age / 3600,
                    )
                    return symbols
        except (ValueError, OSError, KeyError, TypeError):
            pass

    # ---- fetch from API ----
    # fapi.binance.com is the futures-specific REST host; the public data
    # mirror (data-api.binance.vision) doesn't serve exchangeInfo, so we
    # talk to the API directly. This is a lightweight public GET — it
    # almost never trips geo-restrictions the way klines do.
    urls = [
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "https://api.binance.com/fapi/v1/exchangeInfo",
    ]

    for url in urls:
        try:
            resp = SESSION.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            symbols = [
                s["symbol"] for s in data.get("symbols", [])
                if (s.get("quoteAsset") == "USDT"
                    and s.get("contractType") == "PERPETUAL"
                    and s.get("status") == "TRADING")
            ]
            symbols.sort()
            logger.info(
                "Fetched %d USDT-M perpetual symbols from %s",
                len(symbols), url,
            )

            if symbols:
                # persist to cache
                try:
                    with open(SYMBOLS_CACHE, "w", encoding="utf-8") as f:
                        json.dump(
                            {"_fetched_at": time.time(), "symbols": symbols},
                            f,
                        )
                except OSError:
                    pass
                return symbols
        except Exception as e:
            logger.warning("Failed to fetch symbols from %s: %s", url, e)

    # ---- fallback ----
    logger.warning(
        "Could not fetch symbol list; falling back to %d hardcoded symbols",
        len(FALLBACK_SYMBOLS),
    )
    return list(FALLBACK_SYMBOLS)


def resolve_symbols(force_refresh=False):
    """Update the module-level SYMBOLS list from the Binance API (or cache).

    Called once at the start of main().  When *force_refresh* is True the
    on-disk cache is ignored and the API is queried unconditionally.
    """
    global SYMBOLS
    if force_refresh and os.path.exists(SYMBOLS_CACHE):
        try:
            os.remove(SYMBOLS_CACHE)
        except OSError:
            pass
    SYMBOLS = fetch_usdt_perpetual_symbols()
    logger.info("resolve_symbols: %d symbols loaded", len(SYMBOLS))


def build_jobs():
    jobs = []
    for symbol in SYMBOLS:
        for dt in DATA_TYPES:
            if not dt.enabled:
                continue
            if dt.per_interval:
                for interval in INTERVALS:
                    jobs.append(Job(dt, symbol, interval))
            else:
                jobs.append(Job(dt, symbol, None))
    return jobs


# ----- progress index -----
# `_index.json` (stored in the dataset, cloned with the data) maps each output
# CSV to its last stored timestamp. It lets a run decide which files need
# updating without reading every CSV, and skip files already current — no read,
# no fetch, no rewrite, no re-upload.
INDEX_FILENAME = "_index.json"


def load_index(data_folder):
    path = os.path.join(data_folder, INDEX_FILENAME)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (ValueError, OSError):
        return {}


def save_index(data_folder, index):
    path = os.path.join(data_folder, INDEX_FILENAME)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=0, sort_keys=True)


def index_last_dt(index, filename):
    value = index.get(filename)
    if not value:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(ts) else ts.to_pydatetime()


def needs_update(last_dt, end_date):
    return last_dt is None or last_dt.date() < end_date


def build_index_from_files(data_folder):
    """One-time bootstrap: derive the index from CSVs already on disk."""
    index = {}
    for job in build_jobs():
        filename = output_filename(job.dt, job.symbol, job.interval)
        path = os.path.join(data_folder, filename)
        if os.path.exists(path):
            last = latest_stored_time(path, job.dt.time_col)
            if last is not None:
                index[filename] = last.strftime("%Y-%m-%d %H:%M:%S")
    return index


def process_job(dt, symbol, interval, data_folder, end_date, last_dt, downloader=download_series_file):
    out_name = output_filename(dt, symbol, interval)
    data_path = os.path.join(data_folder, out_name)
    new_df = fetch_series(dt, symbol, interval, last_dt, end_date, downloader=downloader)
    if new_df is None or new_df.empty:
        return None
    existing_df = pd.read_csv(data_path, dtype=str) if os.path.exists(data_path) else None
    merged = merge_frames(existing_df, new_df, dt.time_col)
    merged.to_csv(data_path, index=False)
    new_last = pd.to_datetime(merged[dt.time_col], errors="coerce").max()
    return data_path, (None if pd.isna(new_last) else new_last.to_pydatetime())


from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SYMBOLS_CACHE = os.path.join(BASE_DIR, ".symbols_cache.json")
SYMBOLS_CACHE_TTL_HOURS = 24

README_TEMPLATE = """# USDT-M Perpetual Futures (Binance)

Binance USDT-margined perpetual futures data, auto-updated from the public
`data.binance.vision` bulk data mirror.

币安 U 本位永续合约数据集，自动从 `data.binance.vision` 公共数据镜像更新。

## Files / 文件

- `{symbol}_{interval}.csv` — OHLCV klines (量价)
- `{symbol}_markPrice_{interval}.csv` — mark price klines (标记价格)
- `{symbol}_indexPrice_{interval}.csv` — index price klines (指数价格)
- `{symbol}_premiumIndex_{interval}.csv` — premium index klines (溢价指数)
- `{symbol}_metrics.csv` — open interest + long/short ratios + taker buy/sell ratio (持仓量与多空比)
- `{symbol}_fundingRate.csv` — funding rate history (资金费率)
- `_index.json` — updater bookkeeping (last timestamp per file; used to update only what changed)

```python
from datasets import load_dataset
dataset = load_dataset("linxy/USDT-M_Perpetual_Futures", data_files=["BTCUSDT_1d.csv"], split="train")
```

Last updated on `pending`
"""


def ensure_readme(path):
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(README_TEMPLATE)


def stamp_readme(path):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            body = f.read()
    else:
        body = README_TEMPLATE
    if "Last updated on `" in body:
        body = re.sub(r"Last updated on `.*?`", f"Last updated on `{now}`", body)
    else:
        body += f"\n\nLast updated on `{now}`\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def run_update(data_folder, end_date=None, budget=None, max_workers=None):
    if end_date is None:
        end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    if budget is None:
        budget = Budget(float(os.getenv("MAX_RUNTIME_MIN", "90")))
    if max_workers is None:
        max_workers = int(os.getenv("FETCH_WORKERS", "8" if PROXY else "64"))

    index = load_index(data_folder)
    if not index:
        index = build_index_from_files(data_folder)
        if index:
            logger.info("Bootstrapped index from %d existing files", len(index))

    all_jobs = build_jobs()
    pending = []
    for job in all_jobs:
        filename = output_filename(job.dt, job.symbol, job.interval)
        last_dt = index_last_dt(index, filename)
        if needs_update(last_dt, end_date):
            pending.append((job, filename, last_dt))
    logger.info("%d/%d series need update (end_date=%s)", len(pending), len(all_jobs), end_date)

    jobs = iter(pending)
    produced = 0
    inflight = {}  # future -> output filename

    def submit_next(ex):
        try:
            job, filename, last_dt = next(jobs)
        except StopIteration:
            return False
        fut = ex.submit(process_job, job.dt, job.symbol, job.interval, data_folder, end_date, last_dt)
        inflight[fut] = filename
        return True

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for _ in range(max_workers):
            if budget.exceeded() or not submit_next(ex):
                break
        while inflight:
            done, _ = wait(set(inflight), return_when=FIRST_COMPLETED)
            for f in done:
                filename = inflight.pop(f)
                try:
                    result = f.result()
                    if result is not None:
                        _, new_last = result
                        if new_last is not None:
                            index[filename] = new_last.strftime("%Y-%m-%d %H:%M:%S")
                        produced += 1
                except Exception as e:
                    logger.warning("series failed: %s", e)
            if not budget.exceeded():
                for _ in range(len(done)):
                    if not submit_next(ex):
                        break

    save_index(data_folder, index)
    logger.info("run_update produced data for %d series; index has %d entries", produced, len(index))
    return produced


PERIODIC_PUSH_INTERVAL_MIN = int(os.getenv("PERIODIC_PUSH_INTERVAL_MIN", "30"))


def _periodic_push_worker(data_folder, dataset_slug, stop_event):
    """Daemon-thread target: upload data/ to HF every PERIODIC_PUSH_INTERVAL_MIN.

    The first push is delayed by one full interval so the updater has time to
    produce a meaningful batch.  If the HF_TOKEN is missing or the upload
    fails the error is logged and the thread keeps retrying — the final upload
    in main() is the authoritative one.
    """
    # Wait for the first interval before the initial push.
    while not stop_event.wait(PERIODIC_PUSH_INTERVAL_MIN * 60):
        try:
            api = HfApi(token=os.getenv("HF_TOKEN"))
            notes = datetime.now(timezone.utc).strftime(
                "auto-save at %Y-%m-%d %H:%M:%S UTC"
            )
            api.upload_folder(
                folder_path=data_folder,
                repo_id=dataset_slug,
                repo_type="dataset",
                commit_message=notes,
                commit_description=notes,
                create_pr=False,
            )
            logger.info("Periodic push completed: %s", notes)
        except Exception as e:
            logger.warning("Periodic push failed (will retry next interval): %s", e)


def upload(upload_folder, dataset_slug, version_notes):
    api = HfApi(token=os.getenv("HF_TOKEN"))
    api.upload_folder(
        folder_path=upload_folder,
        repo_id=dataset_slug,
        repo_type="dataset",
        commit_message=version_notes,
        commit_description=version_notes,
        create_pr=False,
    )
    logger.info("Dataset uploaded to %s", dataset_slug)


def main():
    configure_proxy()
    dataset_slug = os.getenv("DATASET_SLUG", "linxy/USDT-M_Perpetual_Futures")
    data_folder = os.path.join(BASE_DIR, os.getenv("DATA_DIR", "data"))
    os.makedirs(data_folder, exist_ok=True)

    logger.info("Starting USDT-M perpetual futures update -> %s", dataset_slug)
    resolve_symbols()

    # Start periodic auto-save so that if the process is killed (e.g. CI
    # timeout) we keep whatever data has already been written to disk.
    stop_event = threading.Event()
    push_thread = threading.Thread(
        target=_periodic_push_worker,
        args=(data_folder, dataset_slug, stop_event),
        daemon=True,
    )
    push_thread.start()
    logger.info("Periodic auto-save started (every %d min)", PERIODIC_PUSH_INTERVAL_MIN)

    try:
        run_update(data_folder)
    finally:
        stop_event.set()
        logger.info("Periodic auto-save stopped")

    readme_path = os.path.join(data_folder, "README.md")
    ensure_readme(readme_path)
    stamp_readme(readme_path)

    # Final authoritative upload — retry up to 10 times.
    notes = datetime.now(timezone.utc).strftime("Updated at %B %d %Y %H:%M:%S UTC")
    for attempt in range(1, 11):
        try:
            upload(data_folder, dataset_slug, notes)
            break
        except Exception as e:
            logger.error("Upload attempt %d/10 failed: %s. Retrying in 60s...", attempt, e)
            time.sleep(60)
    else:
        raise RuntimeError("Upload failed after 10 attempts")


if __name__ == "__main__":
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)

    max_attempts = 10
    attempt = 0
    while attempt < max_attempts:
        try:
            main()
            logger.info("Updater finished successfully.")
            break
        except Exception as e:
            attempt += 1
            import traceback
            traceback.print_exc()
            logger.error("Global attempt %d/%d failed: %s. Retrying in 60s...", attempt, max_attempts, e)
            time.sleep(60)
    else:
        logger.error("Max attempts reached. Exiting.")
        sys.exit(1)
