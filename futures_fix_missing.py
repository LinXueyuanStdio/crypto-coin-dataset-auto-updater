#!/usr/bin/env python
"""Validate and interactively repair Binance USDT-M perpetual futures data."""

import argparse
import hashlib
import hmac
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode

import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv


BASE_URL = "https://fapi.binance.com"
INTERVALS = ["1d", "12h", "8h", "6h", "4h", "2h", "1h", "30m", "15m", "5m"]
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]
KLINE_REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]
FUNDING_COLUMNS = ["calc_time", "funding_interval_hours", "last_funding_rate"]
METRICS_COLUMNS = [
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
]
METRICS_NUMERIC_COLUMNS = [c for c in METRICS_COLUMNS if c not in ("create_time", "symbol")]
FUTURES_DATA_MAX_LOOKBACK_DAYS = 30
FUTURES_DATA_START_SAFETY = pd.Timedelta(hours=1)
INDEX_FILENAME = "_index.json"

KLINE_KIND_SPECS = {
    "ohlcv": {"suffix": "", "endpoint": "/fapi/v1/klines", "symbol_param": "symbol"},
    "markPrice": {"suffix": "markPrice", "endpoint": "/fapi/v1/markPriceKlines", "symbol_param": "symbol"},
    "indexPrice": {"suffix": "indexPrice", "endpoint": "/fapi/v1/indexPriceKlines", "symbol_param": "pair"},
    "premiumIndex": {"suffix": "premiumIndex", "endpoint": "/fapi/v1/premiumIndexKlines", "symbol_param": "symbol"},
}

METRICS_ENDPOINTS = {
    "sum_open_interest": ("/futures/data/openInterestHist", "sumOpenInterest"),
    "sum_open_interest_value": ("/futures/data/openInterestHist", "sumOpenInterestValue"),
    "count_toptrader_long_short_ratio": ("/futures/data/topLongShortPositionRatio", "longShortRatio"),
    "sum_toptrader_long_short_ratio": ("/futures/data/topLongShortPositionRatio", "longShortRatio"),
    "count_long_short_ratio": ("/futures/data/globalLongShortAccountRatio", "longShortRatio"),
    "sum_taker_long_short_vol_ratio": ("/futures/data/takerlongshortRatio", "buySellRatio"),
}

PROXY_ENV_NAMES = (
    "ALL_PROXY",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "all_proxy",
    "http_proxy",
    "https_proxy",
)


@dataclass(frozen=True)
class RepairTask:
    kind: str
    symbol: str
    path: Path
    start: pd.Timestamp
    end: pd.Timestamp
    reason: str


@dataclass(frozen=True)
class RepairResult:
    task: RepairTask
    accepted: bool
    wrote: bool
    message: str


class BinanceClient:
    def __init__(self, api_key: str | None = None, secret_key: str | None = None, timeout: int = 30):
        self.api_key = api_key
        self.secret_key = secret_key
        self.timeout = timeout
        self.session = requests.Session()

    def get(self, endpoint: str, params: dict, *, signed: bool = False) -> dict | list | None:
        params = dict(params)
        headers = {}
        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        if signed and self.secret_key:
            params["timestamp"] = int(time.time() * 1000)
            qs = urlencode(params)
            signature = hmac.new(self.secret_key.encode(), qs.encode(), hashlib.sha256).hexdigest()
            params["signature"] = signature

        last_error = None
        for attempt in range(1, 4):
            try:
                response = self.session.get(
                    f"{BASE_URL}{endpoint}",
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < 3:
                    time.sleep(min(2**attempt, 10))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                if attempt < 3:
                    retry_after = int(response.headers.get("Retry-After", min(2**attempt, 10)))
                    time.sleep(retry_after)
                continue
            if response.status_code >= 400:
                print(f"API error {response.status_code} {endpoint}: {response.text[:300]}", file=sys.stderr)
                return None
            return response.json()

        print(f"API request failed {endpoint}: {last_error}", file=sys.stderr)
        return None


def configure_environment() -> tuple[str | None, str | None]:
    load_dotenv(find_dotenv(), override=False)
    proxy_url = os.getenv("BINANCE_PROXY") or os.getenv("PROXY_URL")
    if proxy_url:
        for name in PROXY_ENV_NAMES:
            os.environ.setdefault(name, proxy_url)
    api_key = os.getenv("BINANCE_API_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY") or os.getenv("BINANCE_API_SECRET")
    return api_key, secret_key


def timestamp_ms(ts: pd.Timestamp) -> int:
    return int(pd.Timestamp(ts).timestamp() * 1000)


def futures_data_earliest_timestamp(now: pd.Timestamp | None = None) -> pd.Timestamp:
    current = pd.Timestamp.utcnow().tz_localize(None) if now is None else pd.Timestamp(now)
    if current.tzinfo is not None:
        current = current.tz_convert(None)
    return (current - pd.Timedelta(days=FUTURES_DATA_MAX_LOOKBACK_DAYS) + FUTURES_DATA_START_SAFETY).ceil("5min")


def symbol_dirs(data_dir: Path, symbols: list[str] | None = None) -> list[Path]:
    if symbols:
        return [data_dir / symbol for symbol in symbols if (data_dir / symbol).is_dir()]
    if not data_dir.exists():
        return []
    return sorted(p for p in data_dir.iterdir() if p.is_dir())


def parse_symbols(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    symbols = [part.strip().upper() for part in raw.split(",") if part.strip()]
    return symbols or None


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def kline_filename(symbol: str, kind: str, interval: str) -> str:
    suffix = KLINE_KIND_SPECS[kind]["suffix"]
    if suffix:
        return f"{symbol}_{suffix}_{interval}.parquet"
    return f"{symbol}_{interval}.parquet"


def interval_freq(interval: str) -> str:
    if interval.endswith("m"):
        return interval[:-1] + "min"
    if interval.endswith("h"):
        return interval
    if interval.endswith("d"):
        return interval[:-1] + "D"
    raise ValueError(f"unsupported interval: {interval}")


def interval_timedelta(interval: str) -> pd.Timedelta:
    return pd.Timedelta(interval_freq(interval))


def normalize_kline_file_frame(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    work = df.copy()
    for col in KLINE_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA
    work["open_time"] = pd.to_datetime(work["open_time"], errors="coerce").dt.floor(interval_freq(interval))
    work["close_time"] = pd.to_datetime(work["close_time"], errors="coerce")
    for col in KLINE_COLUMNS:
        if col in ("open_time", "close_time"):
            continue
        work[col] = work[col].map(lambda value: None if pd.isna(value) else str(value))
    work = work.dropna(subset=["open_time"])
    work = work.drop_duplicates(subset=["open_time"], keep="last").sort_values("open_time")
    return work[KLINE_COLUMNS].reset_index(drop=True)


def align_funding_time(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.floor("s")


def normalize_funding_file_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in FUNDING_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA
    work["calc_time"] = align_funding_time(work["calc_time"])
    work = work.dropna(subset=["calc_time"])
    work = work.drop_duplicates(subset=["calc_time"], keep="last").sort_values("calc_time")
    return work[FUNDING_COLUMNS].reset_index(drop=True)


def infer_funding_interval_hours(df: pd.DataFrame, default: int = 8) -> int:
    if "calc_time" in df.columns:
        times = pd.to_datetime(df["calc_time"], errors="coerce").dropna().sort_values()
        diffs = times.diff().dropna()
        if len(diffs):
            hours = diffs.dt.total_seconds().div(3600).round().astype(int)
            hours = hours[hours > 0]
            if len(hours):
                return int(hours.mode().iloc[0])
    if "funding_interval_hours" in df.columns:
        values = pd.to_numeric(df["funding_interval_hours"], errors="coerce").dropna()
        if len(values):
            return int(values.mode().iloc[0])
    return default


def normalize_metrics_file_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in METRICS_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA
    work["create_time"] = pd.to_datetime(work["create_time"], errors="coerce").dt.floor("5min")
    work = work.dropna(subset=["create_time"])
    work = work.drop_duplicates(subset=["create_time"], keep="last").sort_values("create_time")
    return work[METRICS_COLUMNS].reset_index(drop=True)


def consecutive_ranges(index: pd.DatetimeIndex, freq: str) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if len(index) == 0:
        return []
    step = pd.tseries.frequencies.to_offset(freq)
    ranges = []
    start = index[0]
    previous = index[0]
    for ts in index[1:]:
        if ts - previous != step:
            ranges.append((start, previous))
            start = ts
        previous = ts
    ranges.append((start, previous))
    return ranges


def missing_ranges(
    frame: pd.DataFrame,
    time_col: str,
    required_columns: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    freq: str,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if pd.Timestamp(end) < pd.Timestamp(start):
        return []
    expected = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq=freq)
    if len(expected) == 0:
        return []
    work = frame.copy()
    work[time_col] = pd.to_datetime(work[time_col], errors="coerce")
    work = work.dropna(subset=[time_col]).drop_duplicates(subset=[time_col], keep="last")
    work = work.set_index(time_col).sort_index()
    aligned = work.reindex(expected)
    missing_mask = aligned[required_columns].isna().any(axis=1)
    return consecutive_ranges(expected[missing_mask.to_numpy()], freq)


def plan_kline_repairs(
    data_dir: Path,
    symbols: list[str] | None = None,
    *,
    intervals: list[str] | None = None,
    kinds: list[str] | None = None,
) -> list[RepairTask]:
    tasks = []
    wanted_intervals = intervals or INTERVALS
    wanted_kinds = kinds or list(KLINE_KIND_SPECS)
    for directory in symbol_dirs(data_dir, symbols):
        symbol = directory.name
        for kind in wanted_kinds:
            if kind not in KLINE_KIND_SPECS:
                continue
            for interval in wanted_intervals:
                path = directory / kline_filename(symbol, kind, interval)
                if not path.exists():
                    continue
                raw_df = read_parquet(path)
                if "open_time" not in raw_df.columns:
                    tasks.append(RepairTask(kind, symbol, path, pd.NaT, pd.NaT, "missing open_time column"))
                    continue
                raw_time = pd.to_datetime(raw_df["open_time"], errors="coerce")
                normalized_time = raw_time.dt.floor(interval_freq(interval))
                off_grid = raw_time.ne(normalized_time)
                duplicated_after_normalize = normalized_time.duplicated(keep=False)
                if off_grid.any() or duplicated_after_normalize.any():
                    bad_times = normalized_time[off_grid | duplicated_after_normalize].dropna()
                    tasks.append(
                        RepairTask(
                            kind,
                            symbol,
                            path,
                            bad_times.min() if len(bad_times) else raw_time.min(),
                            bad_times.max() if len(bad_times) else raw_time.max(),
                            f"off-grid or duplicate {kind} {interval} open_time",
                        )
                    )

                df = normalize_kline_file_frame(raw_df, interval)
                if df.empty:
                    continue
                start = df["open_time"].min()
                end = df["open_time"].max()
                for gap_start, gap_end in missing_ranges(
                    df,
                    "open_time",
                    KLINE_REQUIRED_COLUMNS,
                    start,
                    end,
                    interval_freq(interval),
                ):
                    tasks.append(
                        RepairTask(
                            kind,
                            symbol,
                            path,
                            gap_start,
                            gap_end,
                            f"missing {kind} {interval} rows or values",
                        )
                    )
    return tasks


def plan_funding_repairs(data_dir: Path, symbols: list[str] | None = None) -> list[RepairTask]:
    tasks = []
    for directory in symbol_dirs(data_dir, symbols):
        symbol = directory.name
        path = directory / f"{symbol}_fundingRate.parquet"
        if not path.exists():
            continue
        df = read_parquet(path)
        if "calc_time" not in df.columns:
            tasks.append(RepairTask("funding", symbol, path, pd.NaT, pd.NaT, "missing calc_time column"))
            continue
        raw_time = pd.to_datetime(df["calc_time"], errors="coerce")
        normalized_time = align_funding_time(raw_time)
        off_grid = raw_time.ne(normalized_time)
        duplicated_after_normalize = normalized_time.duplicated(keep=False)
        if off_grid.any() or duplicated_after_normalize.any():
            bad_times = normalized_time[off_grid | duplicated_after_normalize].dropna()
            tasks.append(
                RepairTask(
                    "funding",
                    symbol,
                    path,
                    bad_times.min() if len(bad_times) else raw_time.min(),
                    bad_times.max() if len(bad_times) else raw_time.max(),
                    "off-grid or duplicate funding timestamp",
                )
            )
    return tasks


def plan_metrics_repairs(
    data_dir: Path,
    symbols: list[str] | None = None,
    *,
    now: pd.Timestamp | None = None,
) -> list[RepairTask]:
    tasks = []
    earliest_api_time = futures_data_earliest_timestamp(now)
    for directory in symbol_dirs(data_dir, symbols):
        symbol = directory.name
        metrics_path = directory / f"{symbol}_metrics.parquet"
        kline_path = directory / f"{symbol}_5m.parquet"
        if not metrics_path.exists():
            continue
        raw_df = read_parquet(metrics_path)
        if "create_time" not in raw_df.columns:
            tasks.append(RepairTask("metrics", symbol, metrics_path, pd.NaT, pd.NaT, "missing create_time column"))
            continue
        raw_time = pd.to_datetime(raw_df["create_time"], errors="coerce")
        normalized_time = raw_time.dt.floor("5min")
        off_grid = raw_time.ne(normalized_time)
        duplicated_after_normalize = normalized_time.duplicated(keep=False)
        if off_grid.any() or duplicated_after_normalize.any():
            bad_times = normalized_time[off_grid | duplicated_after_normalize].dropna()
            tasks.append(
                RepairTask(
                    "metrics",
                    symbol,
                    metrics_path,
                    bad_times.min() if len(bad_times) else raw_time.min(),
                    bad_times.max() if len(bad_times) else raw_time.max(),
                    "off-grid or duplicate metrics timestamp",
                )
            )

        df = normalize_metrics_file_frame(raw_df)
        if kline_path.exists():
            kline = read_parquet(kline_path)
            if "open_time" in kline.columns and len(kline):
                kline_times = pd.to_datetime(kline["open_time"], errors="coerce").dropna()
                expected_start = max(kline_times.min().floor("5min"), earliest_api_time)
                expected_end = min(kline_times.max().floor("5min"), pd.Timestamp.utcnow().tz_localize(None).floor("5min"))
            else:
                expected_start = max(df["create_time"].min(), earliest_api_time)
                expected_end = df["create_time"].max()
        elif len(df):
            expected_start = max(df["create_time"].min(), earliest_api_time)
            expected_end = df["create_time"].max()
        else:
            continue

        if pd.isna(expected_start) or pd.isna(expected_end) or expected_start > expected_end:
            continue

        for start, end in missing_ranges(
            df,
            "create_time",
            METRICS_NUMERIC_COLUMNS,
            expected_start,
            expected_end,
            "5min",
        ):
            tasks.append(RepairTask("metrics", symbol, metrics_path, start, end, "missing metrics rows or values"))
    return tasks


def interval_from_kline_task(task: RepairTask) -> str:
    name = task.path.name
    stem = name[:-len(".parquet")] if name.endswith(".parquet") else Path(name).stem
    for interval in sorted(INTERVALS, key=len, reverse=True):
        if stem.endswith(f"_{interval}"):
            return interval
    raise ValueError(f"could not infer interval from {task.path}")


def fetch_kline_range(
    client: BinanceClient,
    kind: str,
    symbol: str,
    interval: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    spec = KLINE_KIND_SPECS[kind]
    pages = []
    start_ms = timestamp_ms(start)
    end_ms = timestamp_ms(end)
    step_ms = int(interval_timedelta(interval).total_seconds() * 1000)
    limit = 500
    while start_ms <= end_ms:
        chunk_end = min(end_ms, start_ms + (limit - 1) * step_ms)
        params = {
            spec["symbol_param"]: symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": chunk_end,
            "limit": limit,
        }
        data = client.get(spec["endpoint"], params, signed=False)
        if data:
            page = pd.DataFrame(data, columns=KLINE_COLUMNS)
            if not page.empty:
                page["open_time"] = pd.to_datetime(page["open_time"].astype("int64"), unit="ms")
                page["close_time"] = pd.to_datetime(page["close_time"].astype("int64"), unit="ms")
                page = page[
                    (page["open_time"] >= pd.Timestamp(start))
                    & (page["open_time"] <= pd.Timestamp(end))
                ]
                pages.append(page)
        start_ms = chunk_end + step_ms
    if not pages:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    return normalize_kline_file_frame(pd.concat(pages, ignore_index=True), interval)


def fetch_funding_range(client: BinanceClient, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    pages = []
    start_ms = timestamp_ms(start)
    end_ms = timestamp_ms(pd.Timestamp(end) + pd.Timedelta(milliseconds=999))
    step_ms = int(pd.Timedelta("8h").total_seconds() * 1000)
    limit = 1000
    while start_ms <= end_ms:
        chunk_end = min(end_ms, start_ms + (limit - 1) * step_ms)
        data = client.get(
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "startTime": start_ms, "endTime": chunk_end, "limit": limit},
            signed=False,
        )
        if data:
            page = pd.DataFrame(data)
            if {"fundingTime", "fundingRate"}.issubset(page.columns):
                pages.append(
                    pd.DataFrame(
                        {
                            "calc_time": pd.to_datetime(page["fundingTime"].astype("int64"), unit="ms"),
                            "funding_interval_hours": 8,
                            "last_funding_rate": page["fundingRate"].astype(str),
                        }
                    )
                )
        start_ms = chunk_end + step_ms
    if not pages:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    combined = normalize_funding_file_frame(pd.concat(pages, ignore_index=True))
    combined["funding_interval_hours"] = infer_funding_interval_hours(combined)
    return combined


def fetch_metrics_range(client: BinanceClient, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    field_frames = []
    request_start = max(pd.Timestamp(start).ceil("5min"), futures_data_earliest_timestamp())
    request_end = pd.Timestamp(end).floor("5min")
    if request_start > request_end:
        return pd.DataFrame(columns=METRICS_COLUMNS)

    for field, (endpoint, response_key) in METRICS_ENDPOINTS.items():
        pages = []
        start_ms = timestamp_ms(request_start)
        end_ms = timestamp_ms(request_end)
        period_ms = int(pd.Timedelta("5min").total_seconds() * 1000)
        while start_ms <= end_ms:
            chunk_end = min(end_ms, start_ms + 499 * period_ms)
            data = client.get(
                endpoint,
                {
                    "symbol": symbol,
                    "period": "5m",
                    "startTime": start_ms,
                    "endTime": chunk_end + period_ms,
                    "limit": 500,
                },
                signed=False,
            )
            if data:
                page = pd.DataFrame(data)
                if {"timestamp", response_key}.issubset(page.columns):
                    part = pd.DataFrame(
                        {
                            "create_time": pd.to_datetime(page["timestamp"].astype("int64"), unit="ms"),
                            field: pd.to_numeric(page[response_key], errors="coerce"),
                        }
                    )
                    part = part[(part["create_time"] >= request_start) & (part["create_time"] <= request_end)]
                    pages.append(part)
            start_ms = chunk_end + period_ms
        if pages:
            frame = pd.concat(pages, ignore_index=True).drop_duplicates(subset=["create_time"], keep="last")
        else:
            frame = pd.DataFrame(columns=["create_time", field])
        field_frames.append(frame)

    if not field_frames:
        return pd.DataFrame(columns=METRICS_COLUMNS)
    merged = field_frames[0]
    for frame in field_frames[1:]:
        merged = merged.merge(frame, on="create_time", how="outer")
    merged["symbol"] = symbol
    return normalize_metrics_file_frame(merged)


def task_time_col(task: RepairTask) -> str:
    if task.kind == "funding":
        return "calc_time"
    if task.kind == "metrics":
        return "create_time"
    return "open_time"


def task_context_delta(task: RepairTask, context_rows: int) -> pd.Timedelta:
    if task.kind == "funding":
        return pd.Timedelta(hours=8 * context_rows)
    if task.kind == "metrics":
        return pd.Timedelta(minutes=5 * context_rows)
    return interval_timedelta(interval_from_kline_task(task)) * context_rows


def local_context(df: pd.DataFrame, task: RepairTask, time_col: str, context_rows: int) -> pd.DataFrame:
    if time_col not in df.columns or df.empty:
        return df.head(0)
    work = df.copy()
    work[time_col] = pd.to_datetime(work[time_col], errors="coerce")
    work = work.sort_values(time_col)
    delta = task_context_delta(task, context_rows)
    start = pd.Timestamp(task.start) - delta
    end = pd.Timestamp(task.end) + delta
    return work[(work[time_col] >= start) & (work[time_col] <= end)]


def merge_repair(existing: pd.DataFrame, fetched: pd.DataFrame, task: RepairTask) -> pd.DataFrame:
    if task.kind == "funding":
        combined = pd.concat([existing, fetched], ignore_index=True)
        return normalize_funding_file_frame(combined)
    if task.kind in KLINE_KIND_SPECS:
        combined = pd.concat([existing, fetched], ignore_index=True)
        return normalize_kline_file_frame(combined, interval_from_kline_task(task))
    combined = pd.concat([existing, fetched], ignore_index=True)
    return normalize_metrics_file_frame(combined)


def preview_frame(title: str, frame: pd.DataFrame, max_rows: int = 20) -> None:
    print(f"\n{title}")
    if frame.empty:
        print("  <empty>")
        return
    with pd.option_context("display.max_rows", max_rows, "display.max_columns", None, "display.width", 220):
        print(frame.head(max_rows).to_string(index=False))
        if len(frame) > max_rows:
            print(f"  ... {len(frame) - max_rows} more row(s)")


def write_index_entry(data_dir: Path, task: RepairTask, repaired: pd.DataFrame) -> None:
    index_path = data_dir / INDEX_FILENAME
    if not index_path.exists():
        return
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    rel = str(task.path.relative_to(data_dir))
    time_col = task_time_col(task)
    if time_col in repaired.columns and len(repaired):
        last = pd.to_datetime(repaired[time_col], errors="coerce").max()
        if not pd.isna(last):
            index[rel] = last.strftime("%Y-%m-%d %H:%M:%S")
            index_path.write_text(json.dumps(index, indent=0, sort_keys=True) + "\n", encoding="utf-8")


def write_repaired_parquet(data_dir: Path, task: RepairTask, repaired: pd.DataFrame) -> Path:
    stamp = f"{time.strftime('%Y%m%d-%H%M%S')}-{time.time_ns()}"
    backup = task.path.with_name(f"{task.path.name}.bak-{stamp}")
    tmp_path = task.path.with_name(f"{task.path.name}.tmp-{stamp}")
    shutil.copy2(task.path, backup)
    try:
        repaired.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, task.path)
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise
    write_index_entry(data_dir, task, repaired)
    return backup


def approval_prompt(task: RepairTask) -> str:
    return (
        f"\nRepair {task.kind} {task.symbol} {task.start} -> {task.end} "
        f"({task.reason})? [y]es/[n]o/[a]ll/[q]uit: "
    )


def apply_repair_task(
    task: RepairTask,
    *,
    data_dir: Path,
    fetcher: Callable[[RepairTask], pd.DataFrame],
    prompt: Callable[[str], str] = input,
    apply: bool = False,
    context_rows: int = 6,
    auto_accept: bool = False,
) -> RepairResult:
    existing = read_parquet(task.path)
    time_col = task_time_col(task)
    fetched = fetcher(task)
    if task.kind == "funding":
        fetched = normalize_funding_file_frame(fetched)
        if not fetched.empty:
            fetched["funding_interval_hours"] = infer_funding_interval_hours(fetched, infer_funding_interval_hours(existing))
    elif task.kind in KLINE_KIND_SPECS:
        fetched = normalize_kline_file_frame(fetched, interval_from_kline_task(task))
    else:
        fetched = normalize_metrics_file_frame(fetched)
    if fetched.empty:
        print("\n" + "=" * 88)
        print(f"{task.kind.upper()} {task.symbol}: {task.reason}")
        print(f"File: {task.path}")
        print(f"Range: {task.start} -> {task.end}")
        preview_frame("Local context", local_context(existing, task, time_col, context_rows))
        preview_frame("Fetched candidate", fetched)
        return RepairResult(task, accepted=False, wrote=False, message="no fetched candidate")
    repaired = merge_repair(existing, fetched, task)

    print("\n" + "=" * 88)
    print(f"{task.kind.upper()} {task.symbol}: {task.reason}")
    print(f"File: {task.path}")
    print(f"Range: {task.start} -> {task.end}")
    preview_frame("Local context", local_context(existing, task, time_col, context_rows))
    preview_frame("Fetched candidate", fetched)
    preview_frame("Merged preview", local_context(repaired, task, time_col, context_rows))

    if not apply:
        return RepairResult(task, accepted=False, wrote=False, message="dry-run")
    if auto_accept:
        answer = "y"
    else:
        answer = prompt(approval_prompt(task)).strip().lower()
    if answer in ("q", "quit", "exit"):
        raise KeyboardInterrupt("repair aborted by user")
    if answer not in ("y", "yes", "a", "all"):
        return RepairResult(task, accepted=False, wrote=False, message="declined")
    backup = write_repaired_parquet(data_dir, task, repaired)
    return RepairResult(task, accepted=True, wrote=True, message=f"wrote {task.path}, backup {backup}")


def print_task_summary(tasks: list[RepairTask], max_details: int = 20) -> None:
    if not tasks:
        print("No repair tasks found.")
        return
    by_kind = {}
    for task in tasks:
        by_kind[task.kind] = by_kind.get(task.kind, 0) + 1
    print(f"Found {len(tasks)} repair task(s): " + ", ".join(f"{k}={v}" for k, v in sorted(by_kind.items())))
    for task in tasks[:max_details]:
        print(f"  {task.kind:8} {task.symbol:16} {task.start} -> {task.end}  {task.reason}")
    if len(tasks) > max_details:
        print(f"  ... {len(tasks) - max_details} more task(s)")


def make_fetcher(client: BinanceClient) -> Callable[[RepairTask], pd.DataFrame]:
    def fetch(task: RepairTask) -> pd.DataFrame:
        if task.kind in KLINE_KIND_SPECS:
            return fetch_kline_range(
                client,
                task.kind,
                task.symbol,
                interval_from_kline_task(task),
                task.start,
                task.end,
            )
        if task.kind == "funding":
            return fetch_funding_range(client, task.symbol, task.start, task.end)
        if task.kind == "metrics":
            return fetch_metrics_range(client, task.symbol, task.start, task.end)
        raise ValueError(f"unsupported repair kind: {task.kind}")

    return fetch


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data", help="Data directory, defaults to ./data")
    parser.add_argument("--symbols", help="Comma-separated symbols to scan, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--apply", action="store_true", help="Allow approved repairs to write parquet files")
    parser.add_argument("--yes", action="store_true", help="Automatically approve every repair task when used with --apply")
    parser.add_argument("--non-interactive", action="store_true", help="Report/fetch only; never prompt or write")
    parser.add_argument("--context-rows", type=int, default=6, help="Rows before/after the repair range to print")
    parser.add_argument("--max-tasks", type=int, default=0, help="Limit number of repair tasks processed")
    parser.add_argument("--intervals", help="Comma-separated kline intervals to scan, defaults to all updater intervals")
    parser.add_argument("--skip-ohlcv", action="store_true", help="Do not scan standard OHLCV kline files")
    parser.add_argument("--skip-mark-price", action="store_true", help="Do not scan markPrice kline files")
    parser.add_argument("--skip-index-price", action="store_true", help="Do not scan indexPrice kline files")
    parser.add_argument("--skip-premium-index", action="store_true", help="Do not scan premiumIndex kline files")
    parser.add_argument("--skip-funding", action="store_true", help="Do not scan fundingRate files")
    parser.add_argument("--skip-metrics", action="store_true", help="Do not scan metrics files")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    api_key, secret_key = configure_environment()
    data_dir = Path(args.data_dir)
    symbols = parse_symbols(args.symbols)
    intervals = parse_symbols(args.intervals) if args.intervals else None
    if intervals:
        intervals = [interval.lower() for interval in intervals]

    tasks = []
    kline_kinds = []
    if not args.skip_ohlcv:
        kline_kinds.append("ohlcv")
    if not args.skip_mark_price:
        kline_kinds.append("markPrice")
    if not args.skip_index_price:
        kline_kinds.append("indexPrice")
    if not args.skip_premium_index:
        kline_kinds.append("premiumIndex")
    if kline_kinds:
        tasks.extend(plan_kline_repairs(data_dir, symbols, intervals=intervals, kinds=kline_kinds))
    if not args.skip_funding:
        tasks.extend(plan_funding_repairs(data_dir, symbols))
    if not args.skip_metrics:
        tasks.extend(plan_metrics_repairs(data_dir, symbols))
    tasks.sort(key=lambda t: (t.kind, t.symbol, str(t.start), str(t.end)))
    if args.max_tasks and args.max_tasks > 0:
        tasks = tasks[: args.max_tasks]
    print_task_summary(tasks)
    if not tasks:
        return 0

    client = BinanceClient(api_key=api_key, secret_key=secret_key)
    fetcher = make_fetcher(client)
    apply_writes = args.apply and not args.non_interactive
    auto_accept = bool(args.yes)
    for task in tasks:
        def prompt_once(text: str) -> str:
            nonlocal auto_accept
            answer = input(text)
            if answer.strip().lower() in ("a", "all"):
                auto_accept = True
            return answer

        result = apply_repair_task(
            task,
            data_dir=data_dir,
            fetcher=fetcher,
            prompt=prompt_once,
            apply=apply_writes,
            context_rows=args.context_rows,
            auto_accept=auto_accept,
        )
        print(result.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
