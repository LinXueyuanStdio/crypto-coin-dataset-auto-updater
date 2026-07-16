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
    label = out_name.replace(".csv", "")
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
    existing_df = pd.read_csv(data_path, dtype=str) if os.path.exists(data_path) else None
    existing_rows = len(existing_df) if existing_df is not None else 0
    merged = merge_frames(existing_df, new_df, dt.time_col)
    merged.to_csv(data_path, index=False)
    new_last = pd.to_datetime(merged[dt.time_col], errors="coerce").max()
    elapsed = time.monotonic() - t0
    ts = new_last.strftime("%Y-%m-%d") if new_last is not pd.NaT else "?"
    logger.info(
        "[%s] done — +%d new rows (was %d, now %d total through %s) (%.1fs)",
        label, new_rows, existing_rows, len(merged), ts, elapsed,
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

```python
from datasets import load_dataset

# OHLCV klines
klines = load_dataset("linxy/USDT-M_Perpetual_Futures", data_files=["BTCUSDT_1d.csv"], split="train")

# Mark price
mark = load_dataset("linxy/USDT-M_Perpetual_Futures", data_files=["BTCUSDT_markPrice_1d.csv"], split="train")

# Open interest + long/short ratios
metrics = load_dataset("linxy/USDT-M_Perpetual_Futures", data_files=["BTCUSDT_metrics.csv"], split="train")

# Funding rate
funding = load_dataset("linxy/USDT-M_Perpetual_Futures", data_files=["BTCUSDT_fundingRate.csv"], split="train")
```

## Data Types

| File pattern | Content |
|---|---|
| `{symbol}_{interval}.csv` | OHLCV klines |
| `{symbol}_markPrice_{interval}.csv` | Mark price klines |
| `{symbol}_indexPrice_{interval}.csv` | Index price klines |
| `{symbol}_premiumIndex_{interval}.csv` | Premium index klines |
| `{symbol}_metrics.csv` | Open interest, long/short ratios, taker buy/sell |
| `{symbol}_fundingRate.csv` | Funding rate history |
| `{symbol}_info.json` | Per-symbol metadata (precision, tick sizes, sector, etc.) |
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
    pending = []
    for i, job in enumerate(all_jobs):
        # ---- batch sharding (stable by all_jobs index, idempotent) ----
        # Shard BEFORE needs_update so a job always belongs to the same batch,
        # regardless of what was already processed in prior runs.
        if batch_total > 1 and i % batch_total != batch_index:
            continue
        filename = output_filename(job.dt, job.symbol, job.interval)
        data_path = os.path.join(data_folder, filename)
        last_dt = index_last_dt(index, filename)
        if needs_update(last_dt, end_date, data_path, job.dt.time_col):
            pending.append((job, filename, last_dt))

    if batch_total > 1:
        assigned = (len(all_jobs) + batch_total - 1 - batch_index) // batch_total
        logger.info(
            "Batch %d/%d: ~%d/%d jobs assigned to this batch (%d need update)",
            batch_index + 1, batch_total, assigned, len(all_jobs), len(pending),
        )

    logger.info("%d/%d series need update (end_date=%s)", len(pending), len(all_jobs), end_date)

    jobs = iter(pending)
    produced = 0
    failed = 0
    inflight = {}  # future -> output filename
    total_pending = len(pending)
    last_log_at = time.monotonic()
    PROGRESS_LOG_EVERY_S = 60  # emit a progress line at most every N seconds
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
                "errors": [e for _, e in symbol_failed.get(sym, [])],
            }
            for sym in sorted(set(list(symbol_updated) + list(symbol_failed)))
        },
    }
    summary_dir = os.path.join(BASE_DIR, "output")
    os.makedirs(summary_dir, exist_ok=True)
    summary_path = os.path.join(summary_dir, "run_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
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
