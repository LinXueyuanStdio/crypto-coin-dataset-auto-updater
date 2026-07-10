import io
import os
import re
import sys
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
from huggingface_hub import HfApi

# ----- logging (handlers attached lazily in __main__ to keep import side-effect-free) -----
logger = logging.getLogger("futures_updater")

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

SYMBOLS = [
    "1INCHUSDT", "AAVEUSDT", "ADAUSDT", "ALGOUSDT", "AVAXUSDT", "BATUSDT",
    "BCHUSDT", "BNBUSDT", "BTCUSDT", "CHZUSDT", "COMPUSDT", "CRVUSDT",
    "DOGEUSDT", "DOTUSDT", "EOSUSDT", "ETCUSDT", "ETHUSDT", "FILUSDT",
    "FTMUSDT", "FTTUSDT", "HBARUSDT", "HNTUSDT", "ICPUSDT", "KSMUSDT",
    "LDOUSDT", "LINKUSDT", "LTCUSDT", "LUNAUSDT", "MANAUSDT", "MATICUSDT",
    "RUNEUSDT", "SANDUSDT", "SHIBUSDT", "SNXUSDT", "SOLUSDT", "SUSHIUSDT",
    "TRXUSDT", "UNIUSDT", "WAVESUSDT", "XEMUSDT", "XLMUSDT", "XRPUSDT",
    "YFIUSDT", "ZILUSDT", "ZRXUSDT",
]


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


def enumerate_periods(dt, last_dt, end_date):
    from datetime import date
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
            resp = requests.get(url, timeout=timeout)
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


def fetch_series(dt, symbol, interval, last_dt, end_date, downloader=download_series_file):
    months, days = enumerate_periods(dt, last_dt, end_date)
    frames = []
    for period in months:
        frame = downloader(file_url(dt, symbol, interval, "monthly", period), list(dt.columns))
        if frame is not None and len(frame):
            frames.append(frame)
    for day in days:
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
    new_df = pd.read_csv(new_file)
    existing_df = pd.read_csv(existing_file) if os.path.exists(existing_file) else None
    merged = merge_frames(existing_df, new_df, time_col)
    merged.to_csv(output_file, index=False)
    return merged
