# USDT-M Perpetual Futures auto-updater — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `USDT-M_Perpetual_Futures_updater.py` + its GitHub workflow that keep the HF dataset `linxy/USDT-M_Perpetual_Futures` fresh by pulling Binance USDT-M perpetual futures klines, mark/index/premium klines, metrics, and funding rate from the `data.binance.vision` bulk CDN — full backfill on first run, gap-aware incremental after.

**Architecture:** A single self-contained script (side-effect-free at import time; all execution under `__main__`). A `DataType` registry drives per-type URL building, enumeration, and normalization. For each output series the script reads the last stored timestamp, enumerates the missing monthly+daily CDN dumps (walk-from-floor, skip 404), downloads/normalizes/merges, and uploads once per run under a soft time budget so a large backfill converges over several runs.

**Tech Stack:** Python 3.10, pandas, requests, huggingface_hub, pytest (dev). No Binance API key (bulk CDN is public). `concurrent.futures.ThreadPoolExecutor` for network-bound fan-out.

## Global Constraints

- Deliverable script path (exact, hyphenated — keep as one self-contained file): `USDT-M_Perpetual_Futures_updater.py` at repo root.
- Deliverable workflow path (exact): `.github/workflows/USDT-M_Perpetual_Futures_update.yaml`.
- Column names: keep Binance native **snake_case** headers verbatim — never rename to spaced style (`open_time`, not `Open time`).
- Bulk base URL: `https://data.binance.vision/data/futures/um`.
- CDN filename rule: kline variants use `{SYM}-{interval}-{period}.zip` (variant name only in the path); non-kline types use `{SYM}-{path_segment}-{period}.zip`.
- Floors (walk start when a series is empty): klines/markPrice/indexPrice/premiumIndex/fundingRate = `2020-01`; metrics = `2021-01`.
- Time normalization: `open_time`,`close_time`,`calc_time` are 13-digit ms → parse to UTC datetime; `create_time` is a datetime string → parse to datetime. Non-time columns stay as-published (read as str to avoid float precision loss).
- `end_date` = yesterday UTC (today's daily dump isn't published yet).
- Import must be side-effect-free (no network / no client construction at module top level) so tests can `importlib`-load the hyphenated file.
- Only `HF_TOKEN` is required at runtime. Do not reference `BINANCE_API_KEY`/`BINANCE_API_SECRET`.
- Do NOT modify the existing spot `updater.py` / `.github/workflows/update.yaml`.
- Spec: `docs/superpowers/specs/2026-07-10-usdt-m-perpetual-futures-updater-design.md`.

---

### Task 1: Scaffolding — test harness + module skeleton (constants, DataType registry, `output_filename`)

**Files:**
- Create: `USDT-M_Perpetual_Futures_updater.py`
- Create: `tests/conftest.py`
- Create: `tests/test_futures.py`
- Modify: `pyproject.toml` (add pytest dev group)

**Interfaces:**
- Produces:
  - `BASE_URL: str`, `INTERVALS: list[str]`, `SYMBOLS: list[str]`
  - `KLINE_COLUMNS`, `METRICS_COLUMNS`, `FUNDING_COLUMNS: list[str]`
  - `@dataclass(frozen=True) DataType(name, path_segment, per_interval, has_monthly, has_daily, time_col, ms_time_cols: tuple, columns: tuple, floor: str, output_suffix: str, enabled: bool=True)`
  - `DATA_TYPES: list[DataType]` (6 entries: klines, markPrice, indexPrice, premiumIndex, metrics, fundingRate)
  - `output_filename(dt: DataType, symbol: str, interval: str|None) -> str`

- [ ] **Step 1: Add pytest dev dependency to `pyproject.toml`**

Add this block after the `[tool.poetry.dependencies]` table:

```toml
[tool.poetry.group.dev.dependencies]
pytest = "*"
```

- [ ] **Step 2: Create the test module loader `tests/conftest.py`**

The script filename has hyphens (not a valid module name), so load it by path. Import must not touch the network (guaranteed by keeping top-level side-effect-free).

```python
import importlib.util
import pathlib

import pytest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "USDT-M_Perpetual_Futures_updater.py"


@pytest.fixture(scope="session")
def fut():
    spec = importlib.util.spec_from_file_location("futures_updater", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
```

- [ ] **Step 3: Write the failing test in `tests/test_futures.py`**

```python
def test_registry_has_six_enabled_types(fut):
    names = [dt.name for dt in fut.DATA_TYPES]
    assert names == ["klines", "markPrice", "indexPrice", "premiumIndex", "metrics", "fundingRate"]
    assert all(dt.enabled for dt in fut.DATA_TYPES)


def test_kline_type_flags(fut):
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    assert kl.per_interval and kl.has_monthly and kl.has_daily
    assert kl.time_col == "open_time"
    assert kl.ms_time_cols == ("open_time", "close_time")
    assert kl.floor == "2020-01"
    assert kl.path_segment == "klines"


def test_metrics_and_funding_flags(fut):
    m = next(dt for dt in fut.DATA_TYPES if dt.name == "metrics")
    assert m.per_interval is False and m.has_monthly is False and m.has_daily is True
    assert m.time_col == "create_time" and m.ms_time_cols == () and m.floor == "2021-01"
    f = next(dt for dt in fut.DATA_TYPES if dt.name == "fundingRate")
    assert f.has_monthly is True and f.has_daily is False
    assert f.time_col == "calc_time" and f.ms_time_cols == ("calc_time",)


def test_output_filename(fut):
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    mp = next(dt for dt in fut.DATA_TYPES if dt.name == "markPrice")
    me = next(dt for dt in fut.DATA_TYPES if dt.name == "metrics")
    fr = next(dt for dt in fut.DATA_TYPES if dt.name == "fundingRate")
    assert fut.output_filename(kl, "BTCUSDT", "1d") == "BTCUSDT_1d.csv"
    assert fut.output_filename(mp, "BTCUSDT", "1h") == "BTCUSDT_markPrice_1h.csv"
    assert fut.output_filename(me, "BTCUSDT", None) == "BTCUSDT_metrics.csv"
    assert fut.output_filename(fr, "ETHUSDT", None) == "ETHUSDT_fundingRate.csv"


def test_symbols_and_intervals(fut):
    assert "BTCUSDT" in fut.SYMBOLS and "ETHUSDT" in fut.SYMBOLS
    assert fut.INTERVALS[0] == "1d" and "5m" in fut.INTERVALS
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -v`
Expected: FAIL — `USDT-M_Perpetual_Futures_updater.py` doesn't exist / attributes missing.

- [ ] **Step 5: Create `USDT-M_Perpetual_Futures_updater.py` with the skeleton**

Top-level contains ONLY imports, constants, dataclass, registry, and pure helpers — no execution.

```python
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
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -v`
Expected: PASS (5 tests).

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml tests/conftest.py tests/test_futures.py "USDT-M_Perpetual_Futures_updater.py"
git commit -m "feat(futures): scaffold updater module, DataType registry, output_filename"
```

---

### Task 2: URL building (`parse_ym`, `next_month`, `months_range`, `file_url`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Consumes: `DataType`, `BASE_URL` (Task 1)
- Produces:
  - `parse_ym(s: str) -> tuple[int, int]`  (`"2020-01"` → `(2020, 1)`)
  - `next_month(y: int, m: int) -> tuple[int, int]`
  - `months_range(start_ym: tuple, end_ym_exclusive: tuple) -> list[tuple[int,int]]`
  - `file_url(dt: DataType, symbol: str, interval: str|None, freq: str, period) -> str` where `freq in {"monthly","daily"}`, `period` is `(y,m)` for monthly or a `datetime.date` for daily.

- [ ] **Step 1: Write the failing tests**

```python
import datetime as _dt


def test_month_helpers(fut):
    assert fut.parse_ym("2021-01") == (2021, 1)
    assert fut.next_month(2020, 12) == (2021, 1)
    assert fut.next_month(2020, 5) == (2020, 6)
    assert fut.months_range((2020, 11), (2021, 2)) == [(2020, 11), (2020, 12), (2021, 1)]
    assert fut.months_range((2021, 5), (2021, 5)) == []


def test_file_url_klines(fut):
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    assert fut.file_url(kl, "BTCUSDT", "1d", "daily", _dt.date(2026, 7, 8)) == (
        "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-07-08.zip"
    )
    assert fut.file_url(kl, "BTCUSDT", "1h", "monthly", (2025, 5)) == (
        "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2025-05.zip"
    )


def test_file_url_markprice_uses_variant_only_in_path(fut):
    mp = next(dt for dt in fut.DATA_TYPES if dt.name == "markPrice")
    assert fut.file_url(mp, "BTCUSDT", "1h", "daily", _dt.date(2026, 7, 6)) == (
        "https://data.binance.vision/data/futures/um/daily/markPriceKlines/BTCUSDT/1h/BTCUSDT-1h-2026-07-06.zip"
    )


def test_file_url_metrics_and_funding(fut):
    me = next(dt for dt in fut.DATA_TYPES if dt.name == "metrics")
    fr = next(dt for dt in fut.DATA_TYPES if dt.name == "fundingRate")
    assert fut.file_url(me, "BTCUSDT", None, "daily", _dt.date(2026, 7, 8)) == (
        "https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-07-08.zip"
    )
    assert fut.file_url(fr, "BTCUSDT", None, "monthly", (2026, 6)) == (
        "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-06.zip"
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k "month_helpers or file_url" -v`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement the URL/period helpers**

Append to `USDT-M_Perpetual_Futures_updater.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k "month_helpers or file_url" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): URL + month-range builders"
```

---

### Task 3: Gap-aware period enumeration (`enumerate_periods`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Consumes: `DataType`, `parse_ym`, `next_month`, `months_range` (Tasks 1–2)
- Produces: `enumerate_periods(dt: DataType, last_dt: datetime|None, end_date: datetime.date) -> tuple[list[tuple[int,int]], list[datetime.date]]` returning `(monthly_periods, daily_periods)`.

Rules:
- `start = date(floor)` if `last_dt is None` else `last_dt.date()` (the last stored day/month is re-fetched to catch new rows; dedup handles overlap).
- If `start > end_date`: return `([], [])`.
- `has_monthly and has_daily` (klines family): monthly = `months_range(month_of(start), month_of(end))` (complete months before end's month); daily = every day from `max(start, first_of(end_month))` through `end_date`.
- `has_monthly and not has_daily` (funding): monthly = `months_range(month_of(start), next_month(*month_of(end)))` (includes end's month; re-fetched); daily = `[]`.
- `has_daily and not has_monthly` (metrics): daily = every day from `start` through `end_date`; monthly = `[]`.

- [ ] **Step 1: Write the failing tests**

```python
def _by_name(fut, name):
    return next(dt for dt in fut.DATA_TYPES if dt.name == name)


def test_enumerate_klines_backfill(fut):
    kl = _by_name(fut, "klines")
    months, days = fut.enumerate_periods(kl, None, _dt.date(2026, 7, 8))
    assert months[0] == (2020, 1)
    assert months[-1] == (2026, 6)          # complete months, June is last before July
    assert days == [_dt.date(2026, 7, d) for d in range(1, 9)]


def test_enumerate_klines_small_gap_same_month(fut):
    kl = _by_name(fut, "klines")
    last = _dt.datetime(2026, 7, 5, 12, 0)
    months, days = fut.enumerate_periods(kl, last, _dt.date(2026, 7, 8))
    assert months == []                      # start month == end month
    assert days == [_dt.date(2026, 7, d) for d in range(5, 9)]


def test_enumerate_klines_multimonth_gap(fut):
    kl = _by_name(fut, "klines")
    last = _dt.datetime(2026, 5, 20, 0, 0)
    months, days = fut.enumerate_periods(kl, last, _dt.date(2026, 7, 8))
    assert months == [(2026, 5), (2026, 6)]  # May file covers 21–31, June full
    assert days == [_dt.date(2026, 7, d) for d in range(1, 9)]


def test_enumerate_metrics_daily_only(fut):
    me = _by_name(fut, "metrics")
    months, days = fut.enumerate_periods(me, _dt.datetime(2026, 7, 6), _dt.date(2026, 7, 8))
    assert months == []
    assert days == [_dt.date(2026, 7, 6), _dt.date(2026, 7, 7), _dt.date(2026, 7, 8)]


def test_enumerate_funding_monthly_incl_current(fut):
    fr = _by_name(fut, "fundingRate")
    months, days = fut.enumerate_periods(fr, _dt.datetime(2026, 6, 15), _dt.date(2026, 7, 8))
    assert months == [(2026, 6), (2026, 7)]
    assert days == []


def test_enumerate_nothing_when_up_to_date(fut):
    kl = _by_name(fut, "klines")
    months, days = fut.enumerate_periods(kl, _dt.datetime(2026, 7, 9), _dt.date(2026, 7, 8))
    assert months == [] and days == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k enumerate -v`
Expected: FAIL — `enumerate_periods` not defined.

- [ ] **Step 3: Implement `enumerate_periods`**

Append:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k enumerate -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): gap-aware period enumeration"
```

---

### Task 4: Header-aware zip CSV reader + time normalization (`read_zip_csv`, `normalize_times`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Consumes: `DataType`, `KLINE_COLUMNS`, `METRICS_COLUMNS` (Task 1)
- Produces:
  - `read_zip_csv(content: bytes, columns: list[str]) -> pandas.DataFrame` — reads a single-member zip; drops a leading header row iff `row0[0] == columns[0]`; assigns `columns`; values read as str.
  - `normalize_times(df: pandas.DataFrame, dt: DataType) -> pandas.DataFrame` — ms cols → datetime; `time_col` (if not ms) → parsed datetime.

- [ ] **Step 1: Write the failing tests**

```python
import io as _io
import zipfile as _zip


def _zip_bytes(csv_text):
    buf = _io.BytesIO()
    with _zip.ZipFile(buf, "w") as z:
        z.writestr("data.csv", csv_text)
    return buf.getvalue()


def test_read_zip_csv_with_header(fut):
    text = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1783468800000,1,2,0,1,5,1783555199999,10,3,4,6,0\n"
    df = fut.read_zip_csv(_zip_bytes(text), list(fut.KLINE_COLUMNS))
    assert list(df.columns) == list(fut.KLINE_COLUMNS)
    assert len(df) == 1
    assert df.iloc[0]["open_time"] == "1783468800000"


def test_read_zip_csv_without_header(fut):
    text = "1783468800000,1,2,0,1,5,1783555199999,10,3,4,6,0\n"
    df = fut.read_zip_csv(_zip_bytes(text), list(fut.KLINE_COLUMNS))
    assert len(df) == 1
    assert df.iloc[0]["close_time"] == "1783555199999"


def test_normalize_times_klines_ms(fut):
    kl = _by_name(fut, "klines")
    text = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1783468800000,1,2,0,1,5,1783555199999,10,3,4,6,0\n"
    df = fut.normalize_times(fut.read_zip_csv(_zip_bytes(text), list(fut.KLINE_COLUMNS)), kl)
    import pandas as pd
    assert pd.api.types.is_datetime64_any_dtype(df["open_time"])
    assert str(df.iloc[0]["open_time"]) == "2026-07-08 00:00:00"


def test_normalize_times_metrics_string(fut):
    me = _by_name(fut, "metrics")
    text = "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio\n2026-07-08 00:05:00,BTCUSDT,99414.3,6303136243.2,1.58,1.39,1.42,1.75\n"
    df = fut.normalize_times(fut.read_zip_csv(_zip_bytes(text), list(fut.METRICS_COLUMNS)), me)
    import pandas as pd
    assert pd.api.types.is_datetime64_any_dtype(df["create_time"])
    assert str(df.iloc[0]["create_time"]) == "2026-07-08 00:05:00"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k "zip_csv or normalize" -v`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement the reader + normalizer**

Append:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k "zip_csv or normalize" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): header-aware zip CSV reader + time normalization"
```

---

### Task 5: Download with retry + 404 handling (`download_series_file`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Consumes: `read_zip_csv` (Task 4)
- Produces: `download_series_file(url: str, columns: list[str], max_retries: int=3, timeout: int=30) -> pandas.DataFrame|None` — 200 → DataFrame; 404 → `None`; other errors retried then raised.

- [ ] **Step 1: Write the failing tests**

```python
class _Resp:
    def __init__(self, status, content=b""):
        self.status_code = status
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"status {self.status_code}")


def test_download_200_returns_df(fut, monkeypatch):
    text = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1,1,2,0,1,5,2,10,3,4,6,0\n"
    monkeypatch.setattr(fut.requests, "get", lambda url, timeout=30: _Resp(200, _zip_bytes(text)))
    df = fut.download_series_file("http://x/y.zip", list(fut.KLINE_COLUMNS))
    assert df is not None and len(df) == 1


def test_download_404_returns_none(fut, monkeypatch):
    monkeypatch.setattr(fut.requests, "get", lambda url, timeout=30: _Resp(404))
    assert fut.download_series_file("http://x/missing.zip", list(fut.KLINE_COLUMNS)) is None


def test_download_retries_then_raises(fut, monkeypatch):
    calls = {"n": 0}

    def boom(url, timeout=30):
        calls["n"] += 1
        raise fut.requests.ConnectionError("network down")

    monkeypatch.setattr(fut.requests, "get", boom)
    monkeypatch.setattr(fut.time, "sleep", lambda s: None)
    import pytest
    with pytest.raises(fut.requests.RequestException):
        fut.download_series_file("http://x/y.zip", list(fut.KLINE_COLUMNS), max_retries=3)
    assert calls["n"] == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k download -v`
Expected: FAIL — `download_series_file` not defined.

- [ ] **Step 3: Implement `download_series_file`**

Append:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k download -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): download_series_file with retry + 404 skip"
```

---

### Task 6: Series fetch orchestration (`fetch_series`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Consumes: `enumerate_periods`, `file_url`, `download_series_file`, `normalize_times` (Tasks 2–5)
- Produces: `fetch_series(dt, symbol, interval, last_dt, end_date, downloader=download_series_file) -> pandas.DataFrame|None` — enumerates periods, downloads each (skip `None`), concats, normalizes; `None` if nothing fetched. `downloader(url, columns)` is injectable for tests.

- [ ] **Step 1: Write the failing tests**

```python
def test_fetch_series_concats_and_normalizes(fut, monkeypatch):
    kl = _by_name(fut, "klines")
    import pandas as pd

    def fake_dl(url, columns):
        # one row per requested file; open_time encodes the day for uniqueness
        day = url.split("-")[-1].replace(".zip", "")  # e.g. 2026-07-08
        ts = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
        row = [str(ts), "1", "2", "0", "1", "5", str(ts + 1), "10", "3", "4", "6", "0"]
        return pd.DataFrame([row], columns=list(kl.columns))

    df = fut.fetch_series(kl, "BTCUSDT", "1d", _dt.datetime(2026, 7, 6), _dt.date(2026, 7, 8), downloader=fake_dl)
    assert df is not None
    assert len(df) == 3  # days 6,7,8
    assert pd.api.types.is_datetime64_any_dtype(df["open_time"])


def test_fetch_series_returns_none_when_all_404(fut):
    kl = _by_name(fut, "klines")
    df = fut.fetch_series(kl, "BTCUSDT", "1d", _dt.datetime(2026, 7, 6), _dt.date(2026, 7, 8),
                          downloader=lambda url, columns: None)
    assert df is None


def test_fetch_series_empty_range_returns_none(fut):
    kl = _by_name(fut, "klines")
    called = {"n": 0}

    def dl(url, columns):
        called["n"] += 1
        return None

    df = fut.fetch_series(kl, "BTCUSDT", "1d", _dt.datetime(2026, 7, 9), _dt.date(2026, 7, 8), downloader=dl)
    assert df is None and called["n"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k fetch_series -v`
Expected: FAIL — `fetch_series` not defined.

- [ ] **Step 3: Implement `fetch_series`**

Append:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k fetch_series -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): fetch_series orchestration"
```

---

### Task 7: Stored-timestamp read + merge (`latest_stored_time`, `merge_frames`, `merge_datasets`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Produces:
  - `latest_stored_time(path: str, time_col: str) -> datetime|None` — max parsed timestamp of an existing CSV, else `None`.
  - `merge_frames(existing_df: pandas.DataFrame|None, new_df: pandas.DataFrame, time_col: str) -> pandas.DataFrame` — concat, parse `time_col`, drop NaT, dedup on `time_col`, sort.
  - `merge_datasets(existing_file: str, new_file: str, output_file: str, time_col: str) -> pandas.DataFrame` — file wrapper around `merge_frames`.

- [ ] **Step 1: Write the failing tests**

```python
def test_latest_stored_time_missing_file(fut, tmp_path):
    assert fut.latest_stored_time(str(tmp_path / "nope.csv"), "open_time") is None


def test_latest_stored_time_reads_max(fut, tmp_path):
    p = tmp_path / "BTCUSDT_1d.csv"
    p.write_text("open_time,open\n2026-07-01 00:00:00,1\n2026-07-03 00:00:00,2\n")
    ts = fut.latest_stored_time(str(p), "open_time")
    assert ts is not None and ts.year == 2026 and ts.month == 7 and ts.day == 3


def test_merge_frames_dedups_and_sorts(fut):
    import pandas as pd
    existing = pd.DataFrame({"open_time": ["2026-07-01", "2026-07-02"], "close": ["a", "b"]})
    new = pd.DataFrame({"open_time": pd.to_datetime(["2026-07-02", "2026-07-03"]), "close": ["B", "c"]})
    merged = fut.merge_frames(existing, new, "open_time")
    assert list(merged["open_time"].dt.day) == [1, 2, 3]           # sorted, deduped
    assert len(merged) == 3


def test_merge_datasets_no_existing(fut, tmp_path):
    import pandas as pd
    newf = tmp_path / "new.csv"
    pd.DataFrame({"calc_time": ["2026-06-01 00:00:00"], "last_funding_rate": ["0.0001"]}).to_csv(newf, index=False)
    out = tmp_path / "out.csv"
    merged = fut.merge_datasets(str(tmp_path / "absent.csv"), str(newf), str(out), "calc_time")
    assert len(merged) == 1 and out.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k "stored_time or merge" -v`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement the merge helpers**

Append:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k "stored_time or merge" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): stored-timestamp read + gap-aware merge"
```

---

### Task 8: Soft time budget + per-series processing (`Budget`, `build_jobs`, `process_job`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Consumes: `output_filename`, `latest_stored_time`, `fetch_series`, `merge_frames` (Tasks 1,6,7)
- Produces:
  - `class Budget(minutes: float)` with `exceeded() -> bool`.
  - `@dataclass Job(dt: DataType, symbol: str, interval: str|None)` and `build_jobs() -> list[Job]`.
  - `process_job(dt, symbol, interval, data_folder, end_date, downloader=download_series_file) -> str|None` — reads last stored time, fetches, merges into `data_folder/<output_filename>`, returns the written path or `None`.

- [ ] **Step 1: Write the failing tests**

```python
def test_budget(fut):
    assert fut.Budget(0).exceeded() is True
    assert fut.Budget(1000).exceeded() is False


def test_build_jobs_counts(fut):
    jobs = fut.build_jobs()
    per_interval_types = sum(1 for dt in fut.DATA_TYPES if dt.per_interval and dt.enabled)
    flat_types = sum(1 for dt in fut.DATA_TYPES if not dt.per_interval and dt.enabled)
    expected = len(fut.SYMBOLS) * (per_interval_types * len(fut.INTERVALS) + flat_types)
    assert len(jobs) == expected
    assert any(j.symbol == "BTCUSDT" and j.dt.name == "metrics" and j.interval is None for j in jobs)


def test_process_job_writes_then_resumes(fut, tmp_path, monkeypatch):
    import pandas as pd
    kl = _by_name(fut, "klines")

    # first run: nothing stored -> fake fetch returns 2 rows
    def fetch_first(dt, symbol, interval, last_dt, end_date, downloader=None):
        assert last_dt is None
        return pd.DataFrame({
            "open_time": pd.to_datetime(["2026-07-01", "2026-07-02"]),
            "close": ["1", "2"],
        })

    monkeypatch.setattr(fut, "fetch_series", fetch_first)
    path = fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 2))
    assert path is not None and pd.read_csv(path).shape[0] == 2

    # second run: last stored is 2026-07-02 -> fetch returns overlap + 1 new
    def fetch_second(dt, symbol, interval, last_dt, end_date, downloader=None):
        assert last_dt is not None and last_dt.day == 2
        return pd.DataFrame({
            "open_time": pd.to_datetime(["2026-07-02", "2026-07-03"]),
            "close": ["2", "3"],
        })

    monkeypatch.setattr(fut, "fetch_series", fetch_second)
    fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 3))
    df = pd.read_csv(path)
    assert df.shape[0] == 3  # deduped 2026-07-02


def test_process_job_none_when_no_new_data(fut, tmp_path, monkeypatch):
    kl = _by_name(fut, "klines")
    monkeypatch.setattr(fut, "fetch_series", lambda *a, **k: None)
    assert fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 3)) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k "budget or build_jobs or process_job" -v`
Expected: FAIL — symbols not defined.

- [ ] **Step 3: Implement Budget, jobs, and process_job**

Append (note `from dataclasses import dataclass` is already imported at top; add `field` is not needed):

```python
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


def process_job(dt, symbol, interval, data_folder, end_date, downloader=download_series_file):
    out_name = output_filename(dt, symbol, interval)
    data_path = os.path.join(data_folder, out_name)
    last_dt = latest_stored_time(data_path, dt.time_col)
    new_df = fetch_series(dt, symbol, interval, last_dt, end_date, downloader=downloader)
    if new_df is None or new_df.empty:
        return None
    existing_df = pd.read_csv(data_path) if os.path.exists(data_path) else None
    merged = merge_frames(existing_df, new_df, dt.time_col)
    merged.to_csv(data_path, index=False)
    return data_path
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k "budget or build_jobs or process_job" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): budget, job list, per-series process_job"
```

---

### Task 9: Run loop, README, upload, and `__main__` wiring (`run_update`, `ensure_readme`, `stamp_readme`, `upload`, `main`)

**Files:**
- Modify: `USDT-M_Perpetual_Futures_updater.py`
- Test: `tests/test_futures.py`

**Interfaces:**
- Consumes: `build_jobs`, `process_job`, `Budget` (Task 8)
- Produces:
  - `run_update(data_folder, end_date=None, budget=None, max_workers=None) -> int` — bounded ThreadPool over `build_jobs()`; stops submitting when `budget.exceeded()`; returns count of series that produced data.
  - `ensure_readme(path) -> None` (writes template if absent), `stamp_readme(path) -> None` (updates/append "Last updated on `...`").
  - `upload(upload_folder, dataset_slug, version_notes) -> None` (HfApi upload_folder).
  - `main() -> None`.

- [ ] **Step 1: Write the failing tests**

```python
def test_ensure_and_stamp_readme(fut, tmp_path):
    p = tmp_path / "README.md"
    fut.ensure_readme(str(p))
    assert p.exists()
    body = p.read_text(encoding="utf-8")
    assert "USDT-M" in body and "Last updated on" in body
    fut.stamp_readme(str(p))
    assert "Last updated on `" in p.read_text(encoding="utf-8")


def test_run_update_respects_budget_and_counts(fut, tmp_path, monkeypatch):
    # tiny universe so the test is fast and deterministic
    kl = _by_name(fut, "klines")
    monkeypatch.setattr(fut, "SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(fut, "DATA_TYPES", [kl])
    monkeypatch.setattr(fut, "INTERVALS", ["1d", "1h"])

    processed = []

    def fake_process(dt, symbol, interval, data_folder, end_date, downloader=None):
        processed.append(interval)
        return os.path.join(data_folder, f"{symbol}_{interval}.csv")

    monkeypatch.setattr(fut, "process_job", fake_process)
    n = fut.run_update(str(tmp_path), end_date=_dt.date(2026, 7, 8), budget=fut.Budget(1000), max_workers=2)
    assert n == 2 and set(processed) == {"1d", "1h"}


def test_run_update_zero_budget_skips_all(fut, tmp_path, monkeypatch):
    monkeypatch.setattr(fut, "process_job", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not run")))
    n = fut.run_update(str(tmp_path), end_date=_dt.date(2026, 7, 8), budget=fut.Budget(0), max_workers=2)
    assert n == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_futures.py -k "readme or run_update" -v`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement run loop, README, upload, main**

Append:

```python
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
        max_workers = int(os.getenv("FETCH_WORKERS", "32"))

    jobs = iter(build_jobs())
    produced = 0
    inflight = set()

    def submit_next(ex):
        try:
            job = next(jobs)
        except StopIteration:
            return False
        inflight.add(ex.submit(process_job, job.dt, job.symbol, job.interval, data_folder, end_date))
        return True

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for _ in range(max_workers):
            if budget.exceeded() or not submit_next(ex):
                break
        while inflight:
            done, pending = wait(inflight, return_when=FIRST_COMPLETED)
            inflight = pending
            for f in done:
                try:
                    if f.result() is not None:
                        produced += 1
                except Exception as e:  # noqa: BLE001 - keep going on a single-series failure
                    logger.warning("series failed: %s", e)
            if not budget.exceeded():
                for _ in range(len(done)):
                    if not submit_next(ex):
                        break
    logger.info("run_update produced data for %d series", produced)
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
    dataset_slug = os.getenv("DATASET_SLUG", "linxy/USDT-M_Perpetual_Futures")
    data_folder = os.path.join(BASE_DIR, os.getenv("DATA_DIR", "data"))
    os.makedirs(data_folder, exist_ok=True)

    logger.info("Starting USDT-M perpetual futures update -> %s", dataset_slug)
    run_update(data_folder)

    readme_path = os.path.join(data_folder, "README.md")
    ensure_readme(readme_path)
    stamp_readme(readme_path)

    notes = datetime.now(timezone.utc).strftime("Updated at %B %d %Y %H:%M:%S UTC")
    for attempt in range(1, 11):
        try:
            upload(data_folder, dataset_slug, notes)
            break
        except Exception as e:  # noqa: BLE001
            logger.error("Upload attempt %d/10 failed: %s. Retrying in 60s...", attempt, e)
            time.sleep(60)
    else:
        raise RuntimeError("Upload failed after 10 attempts")
```

- [ ] **Step 4: Add the side-effect-free `__main__` block at the very end**

```python
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
        except Exception as e:  # noqa: BLE001
            attempt += 1
            import traceback
            traceback.print_exc()
            logger.error("Global attempt %d/%d failed: %s. Retrying in 60s...", attempt, max_attempts, e)
            time.sleep(60)
    else:
        logger.error("Max attempts reached. Exiting.")
        sys.exit(1)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `poetry run pytest tests/test_futures.py -k "readme or run_update" -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Run the FULL test suite**

Run: `poetry run pytest tests/ -v`
Expected: PASS (all tasks' tests, ~30).

- [ ] **Step 7: Commit**

```bash
git add "USDT-M_Perpetual_Futures_updater.py" tests/test_futures.py
git commit -m "feat(futures): run loop, README, upload, main + __main__"
```

---

### Task 10: GitHub Actions workflow

**Files:**
- Create: `.github/workflows/USDT-M_Perpetual_Futures_update.yaml`

**Interfaces:**
- Consumes: `USDT-M_Perpetual_Futures_updater.py`, secret `HF_TOKEN`.

- [ ] **Step 1: Create the workflow file**

```yaml
name: Update USDT-M Perpetual Futures Dataset

on:
  # Offset a couple of hours from the spot job so they don't hit HF at once
  schedule:
    - cron: "0 2 * * *"
  workflow_dispatch:

jobs:
  update-futures-dataset:
    runs-on: ubuntu-latest
    # Raise toward the GH Actions max so the script's soft MAX_RUNTIME_MIN
    # budget (not the hard kill) ends each run and progress is uploaded.
    timeout-minutes: 350

    steps:
      - name: Check out repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.10"

      - name: Install Poetry
        run: pip install poetry

      - name: Update Poetry lock file
        run: poetry lock

      - name: Install project dependencies
        run: poetry install

      - name: Clone the dataset repository
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
        run: git clone https://hf_user:${HF_TOKEN}@huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures data

      - name: Run the futures dataset update script with retries
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
          MAX_RUNTIME_MIN: "300"
          PYTHONUNBUFFERED: "1"
        run: |
          max_retries=5
          retry=0
          until poetry run python "USDT-M_Perpetual_Futures_updater.py"; do
            retry=$((retry + 1))
            if [ "$retry" -ge "$max_retries" ]; then
              echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] updater failed $retry times, giving up."
              exit 1
            fi
            echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Update failed (attempt $retry/$max_retries), retrying in 30s..."
            sleep 30
          done

      - name: Squash dataset repository history to a single commit
        working-directory: data
        run: |
          set -e
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git config user.name "github-actions[bot]"
          BRANCH=$(git rev-parse --abbrev-ref HEAD)
          git fetch origin "$BRANCH"
          git reset --hard "origin/$BRANCH"
          BEFORE_SHA=$(git rev-parse HEAD)
          git checkout --orphan squash-tmp
          git add -A
          git commit -m "Squash history to reduce repository size (was ${BEFORE_SHA}, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
          git branch -D "$BRANCH"
          git branch -m "$BRANCH"
          git push origin "$BRANCH" --force-with-lease="$BRANCH:$BEFORE_SHA"
```

- [ ] **Step 2: Validate YAML parses**

Run: `poetry run python -c "import yaml; yaml.safe_load(open('.github/workflows/USDT-M_Perpetual_Futures_update.yaml')); print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add ".github/workflows/USDT-M_Perpetual_Futures_update.yaml"
git commit -m "ci(futures): daily workflow for USDT-M perpetual futures dataset"
```

---

### Task 11: Real-CDN integration smoke + end-to-end verification

**Files:**
- Create: `tests/test_integration_cdn.py`

**Interfaces:**
- Consumes: `download_series_file`, `fetch_series`, `process_job` (real network).

- [ ] **Step 1: Write an opt-in integration test (real CDN)**

```python
import datetime as _dt
import importlib.util
import os
import pathlib

import pytest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "USDT-M_Perpetual_Futures_updater.py"


def _load():
    spec = importlib.util.spec_from_file_location("futures_updater", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.skipif(os.getenv("RUN_CDN_TESTS") != "1", reason="set RUN_CDN_TESTS=1 to hit the real CDN")
def test_real_download_klines_monthly():
    fut = _load()
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    url = fut.file_url(kl, "BTCUSDT", "1h", "monthly", (2025, 5))
    df = fut.download_series_file(url, list(kl.columns))
    assert df is not None and len(df) > 100


@pytest.mark.skipif(os.getenv("RUN_CDN_TESTS") != "1", reason="set RUN_CDN_TESTS=1 to hit the real CDN")
def test_real_missing_returns_none():
    fut = _load()
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    url = fut.file_url(kl, "BTCUSDT", "1h", "monthly", (2000, 1))  # long before listing
    assert fut.download_series_file(url, list(kl.columns)) is None
```

- [ ] **Step 2: Run the integration tests against the real CDN**

Run: `RUN_CDN_TESTS=1 poetry run pytest tests/test_integration_cdn.py -v`
Expected: PASS (real klines monthly returns rows; pre-listing month returns None).

- [ ] **Step 3: End-to-end single-series smoke (real download, temp output, no upload)**

Run:
```bash
RUN_CDN_TESTS=1 poetry run python -c "
import importlib.util, datetime, tempfile, os, pandas as pd
spec = importlib.util.spec_from_file_location('f','USDT-M_Perpetual_Futures_updater.py')
f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)
kl = next(d for d in f.DATA_TYPES if d.name=='klines')
me = next(d for d in f.DATA_TYPES if d.name=='metrics')
fr = next(d for d in f.DATA_TYPES if d.name=='fundingRate')
tmp = tempfile.mkdtemp()
end = datetime.date(2026,7,8)
# tiny incremental window so it's fast: pretend last stored is 5 days ago
last = datetime.datetime(2026,7,3)
for dt,iv in [(kl,'1d'),(me,None),(fr,None)]:
    df = f.fetch_series(dt, 'BTCUSDT', iv, last, end)
    print(dt.name, 'rows=', 0 if df is None else len(df), 'cols=', None if df is None else list(df.columns)[:4])
    assert df is not None and len(df) > 0
print('E2E OK ->', tmp)
"
```
Expected: each of klines/metrics/fundingRate prints `rows= >0` with the native snake_case columns, and `E2E OK`.

- [ ] **Step 4: Run the /verify skill on the change**

Drive the updater end-to-end (the Step 3 smoke exercises real download → normalize → non-empty frames for all three data shapes). Confirm columns are native snake_case and time columns are datetimes.

- [ ] **Step 5: Commit**

```bash
git add tests/test_integration_cdn.py
git commit -m "test(futures): real-CDN integration smoke"
```

---

## Self-Review

**1. Spec coverage:**
- Bulk CDN, no REST, no API key → Tasks 1,5,9 (only `HF_TOKEN`). ✅
- 6 data types + native snake_case columns + verified schemas → Task 1 registry, Task 4 columns. ✅
- Output file naming → Task 1 `output_filename` (all four kline variants + metrics + funding). ✅
- Time normalization (ms→datetime, string parse, header-aware) → Task 4. ✅
- Monthly+daily walk-from-floor, gap-aware, floors 2020-01/2021-01 → Task 3 `enumerate_periods`. ✅
- 404 = skip → Tasks 5,6. ✅
- Full backfill (last_dt None → from floor) + gap-aware incremental (last_dt → resume) → Tasks 3,8. ✅
- Soft time budget + multi-run resume + one upload/run → Tasks 8,9. ✅
- Concurrency above cpu_count (32) → Task 9 `FETCH_WORKERS`. ✅
- README ensure + stamp → Task 9. ✅
- Workflow: clone target dataset, drop Binance secrets, add MAX_RUNTIME_MIN, timeout↑, squash, cron offset → Task 10. ✅
- No change to spot updater/workflow → nothing in the plan touches them. ✅

**2. Placeholder scan:** No TBD/TODO; every code step shows full code; every test step shows assertions. The README template's literal `Last updated on \`pending\`` is intentional content, replaced by `stamp_readme`. ✅

**3. Type consistency:** `fetch_series(dt, symbol, interval, last_dt, end_date, downloader=...)` signature identical in Tasks 6, 8, 11. `process_job(dt, symbol, interval, data_folder, end_date, downloader=...)` identical in Tasks 8, 9. `merge_frames(existing_df, new_df, time_col)` identical in Tasks 7, 8. `file_url(dt, symbol, interval, freq, period)` identical in Tasks 2, 6, 11. `Budget(minutes).exceeded()` identical in Tasks 8, 9. ✅
