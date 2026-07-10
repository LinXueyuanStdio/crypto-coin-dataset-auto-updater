# USDT-M Perpetual Futures auto-updater — Design

**Date:** 2026-07-10
**Status:** Approved (pending spec review)
**Target dataset:** [`linxy/USDT-M_Perpetual_Futures`](https://huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures) (already created on HF)

## Goal

Add a sibling to the existing spot updater that keeps a Hugging Face dataset of **Binance USDT-margined perpetual futures** data fresh on a daily schedule. Beyond OHLCV ("量价"), it must also pull funding rate, open interest, long/short ratios, taker buy/sell ratio, and mark/index/premium-index klines — i.e. everything Binance publishes for USDT-M perps.

Deliverables (new files, copies/adaptations of the existing ones):
- `USDT-M_Perpetual_Futures_updater.py` (repo root, sibling of `updater.py`)
- `.github/workflows/USDT-M_Perpetual_Futures_update.yaml` (sibling of `.github/workflows/update.yaml`)

## Key decision: bulk CDN, not REST

The existing spot `updater.py` fetches via the REST mirror `data-api.binance.vision`. That path does **not** work for futures:

- `data-api.binance.vision/fapi/...` → **404** (the spot mirror does not serve futures endpoints).
- `fapi.binance.com` (real futures REST) → **451 restricted location** from datacenter IPs (Azure/GCP), which is exactly what GitHub Actions runners use.

The reliable source is the **`data.binance.vision` bulk data CDN** (Binance's public historical data dumps, S3/CloudFront-backed, un-geofenced). Verified reachable and current (HTTP 200 for July 2026 files). Consequences:

- **No Binance API key needed** — the CDN is unauthenticated. The workflow only needs `HF_TOKEN`.
- Data arrives as daily/monthly `.zip` files each containing exactly one `.csv`.
- Trade-off: daily dumps lag by ~T+1 (a given day's file appears the next day). Acceptable for a daily-cron historical dataset; handled by a small lookback window.

## Data types, source paths, and output files

Base prefix: `https://data.binance.vision/data/futures/um/` (`um` = USDT-M).
All kline variants use the filename pattern `{SYM}-{interval}-{date}.zip` — the variant name lives only in the **path**, never the filename.

| # | Data | Source path | Output CSV | Time (dedup) column |
|---|------|-------------|------------|---------------------|
| 1 | OHLCV klines | `daily/klines/{SYM}/{iv}/{SYM}-{iv}-{date}.zip` | `{SYM}_{iv}.csv` | `open_time` |
| 2 | Mark price klines | `daily/markPriceKlines/{SYM}/{iv}/{SYM}-{iv}-{date}.zip` | `{SYM}_markPrice_{iv}.csv` | `open_time` |
| 3 | Index price klines | `daily/indexPriceKlines/{SYM}/{iv}/{SYM}-{iv}-{date}.zip` | `{SYM}_indexPrice_{iv}.csv` | `open_time` |
| 4 | Premium index klines | `daily/premiumIndexKlines/{SYM}/{iv}/{SYM}-{iv}-{date}.zip` | `{SYM}_premiumIndex_{iv}.csv` | `open_time` |
| 5 | Metrics (OI + long/short + taker ratio) | `daily/metrics/{SYM}/{SYM}-metrics-{date}.zip` | `{SYM}_metrics.csv` | `create_time` |
| 6 | Funding rate | `monthly/fundingRate/{SYM}/{SYM}-fundingRate-{YYYY-MM}.zip` | `{SYM}_fundingRate.csv` | `calc_time` |

### Verified schemas (native headers kept as-is — snake_case, no spaces)

- **Klines / mark / index / premium** (identical 12-col schema; for mark/index/premium the volume-family fields are 0):
  `open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore`
  `open_time`/`close_time` are 13-digit ms.
- **Metrics** (5-minute granularity):
  `create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio`
  `create_time` is a datetime string (`YYYY-MM-DD HH:MM:SS`).
- **Funding rate** (~8h cadence):
  `calc_time,funding_interval_hours,last_funding_rate`
  `calc_time` is 13-digit ms.

### Column-value normalization

- **Column names:** keep native snake_case exactly as published — no renaming to the spot dataset's spaced style. (Per user: use `open_time`, not `Open time`.)
- **Time values:** parse ms integer columns (`open_time`, `close_time`, `calc_time`) to UTC datetime for readable output and clean sort/dedup; `create_time` already ships as a datetime string and is parsed the same way. *(Open to veto: keep raw ms instead.)*
- Current dumps include a header row; older dumps may not. The loader is **header-aware**: it detects whether row 0 is the known header token and assigns the fixed schema either way.

### Symbols and intervals

- **Symbols:** reuse the existing 44-pair spot list from `updater.py`. Some spot pairs are not (or no longer) USDT-M perps (e.g. `FTTUSDT`, `WAVESUSDT`, `HNTUSDT`, `LUNAUSDT`); those simply 404 and are **skipped gracefully**.
- **Intervals:** reuse the spot active set — `1d, 12h, 8h, 6h, 4h, 2h, 1h, 30m, 15m, 5m`.
- Scale: ~42 files/symbol (4 kline variants × 10 intervals + metrics + fundingRate) × 44 symbols ≈ **~1,850 files**. Interval list and symbol list are single config constants so the set is easy to trim.

## Architecture — reuse vs replace

Reused from `updater.py` (kept, lightly generalized):
- Folder scaffolding: `data/`, `new_data/`, `merged_data/`.
- `merge_datasets(...)` — **generalized to take a `time_col` parameter** (was hardcoded to `Open time`) so klines / metrics / funding all merge through one function: parse time col → concat with existing → drop NaT → dedup on `time_col` → sort → write.
- `upload(...)` with the HF token + the upload retry loop.
- Global retry loop, structured logging, and the `data/README.md` "Last updated on" stamping.

Replaced (the fetch layer):
- Remove `binance.client.Client`, `create_binance_client`, `fetch_binance_data`, and all proxy/API-key handling.
- Add `download_zip_csv(url) -> pd.DataFrame | None`: `requests.get` → on 200, `pd.read_csv(BytesIO(content), compression='zip')` with header detection; on 404 return `None`; retry on transient network errors.
- Add per-type fetchers that build URLs, iterate the lookback window (daily) or month set (funding), concat the day/month frames, and write one `new_data/*.csv` per (symbol, type[, interval]).

Parallelism: keep `xlin.element_mapping` with a thread pool over the (symbol × type × interval) job list, mirroring the spot updater.

## Incremental window & robustness

- **Daily types** (klines, mark/index/premium, metrics): fetch dates `[today-N … today-1]`, default `N = 3` (covers T+1 lag + a couple of missed runs); merge dedups overlaps.
- **Funding rate** (monthly): fetch current month + previous month zips (covers month boundaries); the current-month file is re-fetched every run to pick up new rows.
- **Lookback is an env var** (e.g. `LOOKBACK_DAYS`) so a one-time full **backfill** run is trivial (set it large). First scheduled run against the dataset seeds only the lookback window; full history backfill is an explicit separate run, out of scope for the daily job.
- **404 = skip**, not fail (delisted symbols, not-yet-published dates, intervals a variant lacks). Only genuine network/parse errors trigger retries.
- Generate a `data/README.md` from a template if the dataset lacks one (documents file naming + `load_dataset` usage), then stamp "Last updated on".

## Workflow (`USDT-M_Perpetual_Futures_update.yaml`)

Copy of `update.yaml` with:
- Clone `https://…@huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures` into `data`.
- Run `USDT-M_Perpetual_Futures_updater.py`.
- Drop `BINANCE_API_KEY` / `BINANCE_API_SECRET` env (unused); keep `HF_TOKEN`.
- Keep the history-squash step (repo-size control).
- Cron offset a couple hours from the spot job (e.g. `0 2 * * *`) so the two daily jobs don't hit HF simultaneously.

## Dependencies

No new dependencies required — `pandas`, `requests`, `huggingface_hub`, and `xlin` are already in `pyproject.toml`. `python-binance` becomes unused by this script but stays declared (still used by `updater.py`); harmless.

## Out of scope

- Full historical backfill (enabled via `LOOKBACK_DAYS` but run manually, not by the daily cron).
- Additional bulk datasets not requested (aggTrades, trades, bookDepth, bookTicker, liquidationSnapshot).
- Any change to the existing spot `updater.py` / `update.yaml`.
