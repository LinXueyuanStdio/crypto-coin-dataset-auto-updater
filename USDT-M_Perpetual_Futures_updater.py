import io
import json
import os
import re
import sys
import time
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
import pandas as pd
from dotenv import load_dotenv
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
    """Return the data-file path relative to the data folder root.

    Files are stored in per-symbol subdirectories as Parquet:
        BTCUSDT/BTCUSDT_1d.parquet
        BTCUSDT/BTCUSDT_markPrice_1d.parquet
        BTCUSDT/BTCUSDT_metrics.parquet
    """
    if dt.per_interval:
        if dt.output_suffix:
            name = f"{symbol}_{dt.output_suffix}_{interval}.parquet"
        else:
            name = f"{symbol}_{interval}.parquet"
    else:
        name = f"{symbol}_{dt.output_suffix}.parquet"
    return f"{symbol}/{name}"


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


def _read_file(path, columns=None):
    """Read a data file — parquet or csv, whichever exists.

    Tries the given *path* first.  If that fails or does not exist,
    probes the alternate extension (``.csv`` ↔ ``.parquet``).
    """
    candidates = [path]
    if path.endswith(".parquet"):
        candidates.append(path[:-len(".parquet")] + ".csv")
    elif path.endswith(".csv"):
        candidates.append(path[:-len(".csv")] + ".parquet")

    for p in candidates:
        if not os.path.exists(p):
            continue
        try:
            if p.endswith(".parquet"):
                return pd.read_parquet(p, columns=columns) if columns else pd.read_parquet(p)
            return pd.read_csv(p, dtype=str, **(dict(usecols=columns) if columns else {}))
        except Exception:
            continue
    return None


def latest_stored_time(path, time_col):
    df = _read_file(path, columns=[time_col])
    if df is None or df.empty:
        return None
    if df.empty:
        return None
    ts = pd.to_datetime(df[time_col], errors="coerce").max()
    return None if pd.isna(ts) else ts.to_pydatetime()


def merge_frames(existing_df, new_df, time_col):
    # Normalise all timestamp columns — existing parquets may have
    # string timestamps from migration (which used dtype=str).
    TS_COLS = {'open_time', 'close_time', 'calc_time', 'create_time'}
    new_df = new_df.copy()
    for col in TS_COLS & set(new_df.columns):
        new_df[col] = pd.to_datetime(new_df[col], errors='coerce')
    if existing_df is not None and len(existing_df):
        existing_df = existing_df.copy()
        for col in TS_COLS & set(existing_df.columns):
            existing_df[col] = pd.to_datetime(existing_df[col], errors='coerce')
        merged = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        merged = new_df
    merged = merged.dropna(subset=[time_col])
    merged = merged.drop_duplicates(subset=time_col).sort_values(time_col)
    return merged.reset_index(drop=True)


def merge_datasets(existing_file, new_file, output_file, time_col):
    new_df = _read_file(new_file)
    existing_df = _read_file(existing_file)
    merged = merge_frames(existing_df, new_df, time_col)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merged.to_parquet(output_file, index=False)
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


def _parse_binance_symbols(raw_symbols):
    """Extract TRADING USDT-M perpetual symbol names from raw exchange info.

    Args:
        raw_symbols: list of symbol dicts from Binance exchangeInfo API.

    Returns:
        Sorted list of symbol strings (e.g. ['BTCUSDT', 'ETHUSDT', ...]),
        including non-ASCII names like '我踏马来了USDT'.
    """
    return sorted(
        s["symbol"].encode("utf-8", errors="surrogatepass").decode("utf-8", errors="replace")
        for s in raw_symbols
        if (s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING")
    )


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
            symbols = _parse_binance_symbols(data.get("symbols", []))
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
                            f, ensure_ascii=False,
                        )
                except OSError:
                    pass
                return symbols
        except Exception as e:
            logger.warning("Failed to fetch symbols from %s: %s", url, e)

    # ---- fallback 1: committed symbols.json (updated offline) ----
    if os.path.exists(SYMBOLS_FILE):
        try:
            with open(SYMBOLS_FILE, encoding="utf-8") as f:
                data = json.load(f)
            symbols = sorted(data.get("symbols", []))
            if symbols:
                logger.info(
                    "Using symbols.json fallback (%d symbols, age=%.1fd)",
                    len(symbols),
                    (time.time() - data.get("_fetched_at", 0)) / 86400,
                )
                return symbols
        except (ValueError, OSError, KeyError, TypeError):
            pass

    # ---- fallback 2: hardcoded list ----
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
    """Merge-safe index write — never loses entries pushed by parallel runs.

    Reads the current on-disk index first (which may have been updated by
    ``git pull`` bringing in commits from another parallel batch), merges
    in the local updates, then writes back. This ensures the index is an
    ever-growing union, not a last-writer-wins snapshot.
    """
    path = os.path.join(data_folder, INDEX_FILENAME)
    existing = load_index(data_folder)
    existing.update(index)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=0, sort_keys=True)


def index_last_dt(index, filename):
    value = index.get(filename)
    if not value:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(ts) else ts.to_pydatetime()


def needs_update(last_dt, end_date, data_path=None, time_col=None):
    """Determine whether a series needs fetching.

    Uses the in-memory index timestamp first.  When the index suggests the
    series is already current, we perform a secondary check against the
    actual CSV on disk — the index may be stale if it was written from a
    parallel run whose ``save_index`` we pulled after the fetch was done.
    """
    if last_dt is not None and last_dt.date() >= end_date:
        # Secondary check: read the actual CSV timestamp
        if data_path and time_col:
            actual_last = latest_stored_time(data_path, time_col)
            if actual_last is not None and actual_last.date() >= end_date:
                return False
            # Index says current but CSV disagrees — trust the CSV
            return True
        return False
    return True


def build_index_from_files(data_folder):
    """Bootstrap the index from data files already on disk.

    Probes both nested (per-symbol) and flat (legacy) layouts, and both
    .parquet and .csv formats, so the migration is seamless.
    """
    index = {}
    for job in build_jobs():
        filename = output_filename(job.dt, job.symbol, job.interval)
        path = os.path.join(data_folder, filename)
        # Check nested path (parquet or legacy csv)
        if _read_file(path) is not None:
            last = latest_stored_time(path, job.dt.time_col)
            if last is not None:
                index[filename] = last.strftime("%Y-%m-%d %H:%M:%S")
                continue
        # Fallback: legacy flat layout (pre-migration)
        flat_name = filename.split("/")[-1]  # strip symbol/ prefix
        flat_path = os.path.join(data_folder, flat_name)
        if _read_file(flat_path) is not None:
            last = latest_stored_time(flat_path, job.dt.time_col)
            if last is not None:
                index[filename] = last.strftime("%Y-%m-%d %H:%M:%S")
    return index


def process_job(dt, symbol, interval, data_folder, end_date, last_dt, downloader=download_series_file):
    out_name = output_filename(dt, symbol, interval)
    data_path = os.path.join(data_folder, out_name)
    label = os.path.splitext(out_name)[0]

    # ---- idempotency check: re-read index from disk ----
    # Another parallel batch may have already processed this job and its
    # _index.json entry was pulled by the shell wrapper since we last loaded.
    fresh_index = load_index(data_folder)
    fresh_last = index_last_dt(fresh_index, out_name)
    if not needs_update(fresh_last, end_date, data_path, dt.time_col):
        logger.info("[%s] already up-to-date (another batch did it) — skip", label)
        return None

    t0 = time.monotonic()
    logger.info("[%s] fetching ...", label)
    try:
        new_df = fetch_series(dt, symbol, interval, last_dt, end_date, downloader=downloader)
    except Exception:
        elapsed = time.monotonic() - t0
        logger.error("[%s] FAILED (%.1fs)", label, elapsed)
        raise
    if new_df is None or new_df.empty:
        elapsed = time.monotonic() - t0
        logger.info("[%s] no new data (%.1fs)", label, elapsed)
        return None
    new_rows = len(new_df)
    existing_df = _read_file(data_path)
    existing_rows = len(existing_df) if existing_df is not None else 0
    merged = merge_frames(existing_df, new_df, dt.time_col)
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    merged.to_parquet(data_path, index=False)
    new_last = pd.to_datetime(merged[dt.time_col], errors="coerce").max()
    elapsed = time.monotonic() - t0
    ts = new_last.strftime("%Y-%m-%d") if new_last is not pd.NaT else "?"
    delta = len(merged) - existing_rows
    logger.info(
        "[%s] done — +%d rows (fetched %d, was %d, now %d through %s) (%.1fs)",
        label, delta, new_rows, existing_rows, len(merged), ts, elapsed,
    )
    return data_path, (None if pd.isna(new_last) else new_last.to_pydatetime())


from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SYMBOLS_CACHE = os.path.join(BASE_DIR, ".symbols_cache.json")
SYMBOLS_FILE = os.path.join(BASE_DIR, "symbols.json")
SYMBOLS_CACHE_TTL_HOURS = 24

README_TEMPLATE = """---
license: mit
task_categories:
- time-series-forecasting
language:
- en
- zh
tags:
- finance
- cryptocurrency
- futures
- perpetual
- binance
pretty_name: USDT-M Perpetual Futures
size_categories:
- 10M<n<100M
---

# USDT-M Perpetual Futures (Binance)

Binance USDT-margined **perpetual futures** historical data, including OHLCV klines,
mark / index / premium-index prices, open-interest & long/short ratios, and funding rates.

Auto-updated daily from the official [Binance public data mirror](https://data.binance.vision).

币安 **U 本位永续合约** 历史数据集，包含 K 线、标记价格、指数价格、溢价指数、持仓量及多空比、资金费率，
每日从 Binance 官方数据镜像自动更新。

Last updated on `pending`

## Usage

Data is stored as Parquet in per-symbol subdirectories.

```python
import pandas as pd
from datasets import load_dataset

# OHLCV klines
klines = load_dataset("linxy/USDT-M_Perpetual_Futures", data_files=["BTCUSDT/BTCUSDT_1d.parquet"], split="train")

# Or read directly with pandas
df = pd.read_parquet("hf://datasets/linxy/USDT-M_Perpetual_Futures/BTCUSDT/BTCUSDT_1d.parquet")
```

## Data Types

Files are organised in per-symbol subdirectories (`{symbol}/{symbol}_{suffix}.parquet`).

| File pattern | Content |
|---|---|
| `{symbol}/{symbol}_{interval}.parquet` | OHLCV klines |
| `{symbol}/{symbol}_markPrice_{interval}.parquet` | Mark price klines |
| `{symbol}/{symbol}_indexPrice_{interval}.parquet` | Index price klines |
| `{symbol}/{symbol}_premiumIndex_{interval}.parquet` | Premium index klines |
| `{symbol}/{symbol}_metrics.parquet` | Open interest, long/short ratios, taker buy/sell |
| `{symbol}/{symbol}_fundingRate.parquet` | Funding rate history |
| `{symbol}/{symbol}_info.json` | Per-symbol metadata (precision, tick sizes, sector, etc.) |
| `meta.json` | Dataset-level metadata (full symbol list, intervals, etc.) |
| `_index.json` | Updater bookkeeping (internal) |

## Intervals

`1d` `12h` `8h` `6h` `4h` `2h` `1h` `30m` `15m` `5m`

## Kline / Price Fields

| Field | Description |
|---|---|
| `open_time` | Interval start timestamp (ms) |
| `open` | Opening price |
| `high` | Highest price |
| `low` | Lowest price |
| `close` | Closing price |
| `volume` | Base asset volume |
| `close_time` | Interval end timestamp (ms) |
| `quote_volume` | Quote asset (USDT) volume |
| `count` | Number of trades |
| `taker_buy_volume` | Taker buy base volume |
| `taker_buy_quote_volume` | Taker buy quote volume |
| `ignore` | Placeholder (unused) |

## Metrics Fields

| Field | Description |
|---|---|
| `create_time` | Observation timestamp |
| `symbol` | Trading pair |
| `sum_open_interest` | Total open interest (base) |
| `sum_open_interest_value` | Total open interest (USDT) |
| `count_toptrader_long_short_ratio` | Top-trader long/short accounts ratio |
| `sum_toptrader_long_short_ratio` | Top-trader long/short positions ratio |
| `count_long_short_ratio` | Global long/short accounts ratio |
| `sum_taker_long_short_vol_ratio` | Taker buy/sell volume ratio |

## Funding Rate Fields

| Field | Description |
|---|---|
| `calc_time` | Funding timestamp |
| `funding_interval_hours` | Interval (hours) |
| `last_funding_rate` | Funding rate |

## Available Symbols

See `meta.json` for the full list.  The dataset covers **all TRADING USDT-M
perpetual contracts** on Binance Futures (currently {n_symbols} symbols).

## Sources

- **Data:** [data.binance.vision](https://data.binance.vision) (Binance public market-data mirror)
- **Updater:** [GitHub](https://github.com/LinXueyuanStdio/crypto-coin-dataset-auto-updater)
- **Processing:** Automated daily incremental updates; monthly bulk zips + daily fallback;
  new data is merged with existing CSVs and de-duplicated by timestamp.

## Bias, Risks, and Limitations

1. **Exchange-specific bias:** Data reflects Binance's order book only, not global markets.
2. **Temporal gaps:** Missing data during Binance outages, API failures, or delistings.
3. **Market volatility:** Cryptocurrency markets are highly volatile — models may be unstable.
4. **Latency:** Daily bulk dumps are published with a delay; intra-day data is not real-time.
5. **Delisted symbols:** Some historical contracts were delisted; their data stops at delisting date.

## Citation

```bibtex
@misc{LinXueyuanStdio2025,
  title = {USDT-M Perpetual Futures (Binance)},
  author = {Xueyuan Lin},
  year = {2025},
  publisher = {Hugging Face},
  howpublished = {\\url{https://huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures}},
}
```

## Author

- LinXueyuanStdio (GitHub: [@LinXueyuanStdio](https://github.com/LinXueyuanStdio))

Last updated on `pending`
"""


def ensure_readme(path):
    if not os.path.exists(path):
        # Use the full (pre-COINS) symbol count from symbols.json if available.
        n = len(SYMBOLS)
        if os.path.exists(SYMBOLS_FILE):
            try:
                with open(SYMBOLS_FILE, encoding="utf-8") as f:
                    n = len(json.load(f).get("symbols", []))
            except (ValueError, OSError, KeyError):
                pass
        body = README_TEMPLATE.replace("{n_symbols}", str(n))
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)


def stamp_readme(path):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    ensure_readme(path)
    with open(path, "r", encoding="utf-8") as f:
        body = f.read()
    if "Last updated on `" in body:
        body = re.sub(r"Last updated on `.*?`", f"Last updated on `{now}`", body)
    else:
        body += f"\n\nLast updated on `{now}`\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def run_update(data_folder, end_date=None, budget=None, max_workers=None,
               batch_total=None, batch_index=None):
    if end_date is None:
        end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    if budget is None:
        budget = Budget(float(os.getenv("MAX_RUNTIME_MIN", "90")))
    if max_workers is None:
        max_workers = int(os.getenv("FETCH_WORKERS", "8" if PROXY else "64"))
    if batch_total is None:
        batch_total = int(os.getenv("BATCH_TOTAL", "1"))
    if batch_index is None:
        batch_index = int(os.getenv("BATCH_INDEX", "0"))

    index = load_index(data_folder)
    if not index:
        index = build_index_from_files(data_folder)
        if index:
            logger.info("Bootstrapped index from %d existing files", len(index))

    all_jobs = build_jobs()

    # ---- batch sharding by symbol (stable, idempotent) ----
    if batch_total > 1:
        all_symbols = sorted({job.symbol for job in all_jobs})
        # Contiguous sharding: each batch gets a contiguous block of symbols.
        # batch 0 → first 1/N, batch N-1 → last 1/N.
        chunk = (len(all_symbols) + batch_total - 1) // batch_total
        start = batch_index * chunk
        end = start + chunk if batch_index < batch_total - 1 else len(all_symbols)
        my_symbols = set(all_symbols[start:end])
        assigned_jobs = sum(1 for job in all_jobs if job.symbol in my_symbols)
        logger.info(
            "Batch %d/%d: %d symbols (%d jobs) of %d total symbols (%d total jobs)",
            batch_index + 1, batch_total,
            len(my_symbols), assigned_jobs,
            len(all_symbols), len(all_jobs),
        )
    else:
        my_symbols = None

    pending = []
    for job in all_jobs:
        if my_symbols is not None and job.symbol not in my_symbols:
            continue
        filename = output_filename(job.dt, job.symbol, job.interval)
        data_path = os.path.join(data_folder, filename)
        last_dt = index_last_dt(index, filename)
        if needs_update(last_dt, end_date, data_path, job.dt.time_col):
            pending.append((job, filename, last_dt))

    logger.info("%d/%d series need update (end_date=%s)", len(pending), len(all_jobs), end_date)

    jobs = iter(pending)
    produced = 0
    failed = 0
    inflight = {}  # future -> output filename
    total_pending = len(pending)
    last_log_at = time.monotonic()
    PROGRESS_LOG_EVERY_S = 60  # emit a progress line at most every N seconds
    CHECKPOINT_EVERY_N = 20    # save + reload index every N completed jobs
    checkpoint_pending = 0
    t_start = time.monotonic()

    # per-symbol stats for the run summary
    symbol_updated = {}  # symbol -> list of series filenames that got new data
    symbol_failed = {}   # symbol -> list of (filename, error) tuples

    def submit_next(ex):
        try:
            job, filename, last_dt = next(jobs)
        except StopIteration:
            return False
        fut = ex.submit(process_job, job.dt, job.symbol, job.interval, data_folder, end_date, last_dt)
        inflight[fut] = (filename, job.symbol)
        return True

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for _ in range(max_workers):
            if budget.exceeded() or not submit_next(ex):
                break
        while inflight:
            done, _ = wait(set(inflight), return_when=FIRST_COMPLETED)
            for f in done:
                filename, symbol = inflight.pop(f)
                try:
                    result = f.result()
                    if result is not None:
                        _, new_last = result
                        if new_last is not None:
                            index[filename] = new_last.strftime("%Y-%m-%d %H:%M:%S")
                        produced += 1
                        symbol_updated.setdefault(symbol, []).append(filename)
                except Exception as e:
                    failed += 1
                    symbol_failed.setdefault(symbol, []).append((filename, str(e)))
                    logger.warning("series failed: %s", e)

                checkpoint_pending += 1

            # ---- periodic checkpoint: save + reload index ----
            # Saves our progress to disk so the shell wrapper can push it.
            # Reloads from disk to incorporate other batches' _index.json
            # entries that were git-pulled by the wrapper since last load.
            if checkpoint_pending >= CHECKPOINT_EVERY_N:
                save_index(data_folder, index)
                fresh = load_index(data_folder)
                fresh.update(index)  # our in-memory entries win over disk
                index = fresh
                checkpoint_pending = 0

            now = time.monotonic()
            if now - last_log_at >= PROGRESS_LOG_EVERY_S:
                done_so_far = produced + failed
                pct = done_so_far * 100.0 / total_pending if total_pending else 0
                elapsed = now - t_start
                rate = done_so_far / max(elapsed, 1)
                logger.info(
                    "progress: %d/%d (%.1f%%) ok=%d fail=%d elapsed=%.0fs — %.1f series/s",
                    done_so_far, total_pending, pct, produced, failed, elapsed, rate,
                )
                last_log_at = now
            if not budget.exceeded():
                for _ in range(len(done)):
                    if not submit_next(ex):
                        break

    save_index(data_folder, index)
    elapsed = time.monotonic() - t_start
    logger.info("run_update produced data for %d series; index has %d entries (%.0fs)", produced, len(index), elapsed)

    # ---- write run summary ----
    summary = {
        "start_time": datetime.fromtimestamp(t_start).strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_seconds": round(elapsed, 1),
        "budget_limit_min": budget.limit_seconds / 60.0,
        "batch_index": batch_index,
        "batch_total": batch_total,
        "total_pending": total_pending,
        "produced": produced,
        "failed": failed,
        "remaining": total_pending - produced - failed,
        "symbols_updated": len(symbol_updated),
        "symbols_failed": len(symbol_failed),
        "per_symbol": {
            sym: {
                "updated": len(symbol_updated.get(sym, [])),
                "failed": len(symbol_failed.get(sym, [])),
                "series": sorted(symbol_updated.get(sym, [])),
                "errors": [e[:200] for _, e in symbol_failed.get(sym, [])],
            }
            for sym in sorted(set(list(symbol_updated) + list(symbol_failed)))
        },
    }
    summary_dir = os.path.join(BASE_DIR, "output")
    os.makedirs(summary_dir, exist_ok=True)
    summary_path = os.path.join(summary_dir, "run_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=True)
    logger.info("Run summary saved to %s", summary_path)

    return produced


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
    load_dotenv()
    configure_proxy()
    dataset_slug = os.getenv("DATASET_SLUG", "linxy/USDT-M_Perpetual_Futures")
    data_folder = os.path.join(BASE_DIR, os.getenv("DATA_DIR", "data"))
    os.makedirs(data_folder, exist_ok=True)

    batch_total = int(os.getenv("BATCH_TOTAL", "1"))
    batch_index = int(os.getenv("BATCH_INDEX", "0"))
    is_master = (batch_index == 0)

    logger.info("Starting USDT-M perpetual futures update -> %s", dataset_slug)
    logger.info("Batch %d/%d (master=%s)", batch_index + 1, batch_total, is_master)
    resolve_symbols()

    # COINS env var (comma-separated) restricts to a symbol subset.
    #   e.g. COINS=BTCUSDT,ETHUSDT poetry run python USDT-M_Perpetual_Futures_updater.py
    coins_filter = os.getenv("COINS", "").strip()
    if coins_filter:
        wanted = {c.strip() for c in coins_filter.split(",") if c.strip()}
        before = len(SYMBOLS)
        SYMBOLS[:] = [s for s in SYMBOLS if s in wanted]
        logger.info(
            "COINS filter: %d symbols → %d (%s)",
            before, len(SYMBOLS), ", ".join(sorted(wanted)),
        )

    run_update(data_folder)

    # ---- master batch responsibilities ----
    # Only the master batch updates shared metadata files (README, meta.json)
    # and performs the final HF upload. Non-master batches rely on the
    # auto-push in run_futures_update.sh to checkpoint their data.
    if is_master:
        readme_path = os.path.join(data_folder, "README.md")
        ensure_readme(readme_path)
        stamp_readme(readme_path)

        if not os.getenv("HF_TOKEN"):
            logger.warning("HF_TOKEN not set — skipping upload (local dev run)")
        elif os.getenv("COINS", "").strip():
            logger.warning("COINS is set — skipping upload (local test run)")
        else:
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
    else:
        logger.info(
            "Non-master batch — skipping README/meta.json update and HF upload "
            "(data will be checkpointed by wrapper's auto-push)"
        )


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
