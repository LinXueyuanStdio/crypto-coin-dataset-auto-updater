# USDT-M Perpetual Futures auto-updater — Design

**Date:** 2026-07-10
**Status:** Approved (pending spec review)
**Target dataset:** [`linxy/USDT-M_Perpetual_Futures`](https://huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures) (already created on HF)

## Goal

Add a sibling to the existing spot updater that keeps a Hugging Face dataset of **Binance USDT-margined perpetual futures** data fresh. Beyond OHLCV ("量价"), it must also pull funding rate, open interest, long/short ratios, taker buy/sell ratio, and mark/index/premium-index klines — i.e. everything Binance publishes for USDT-M perps.

Two behaviors are required:
- **Full historical backfill** — on first run (empty dataset) it pulls *all* available history for every series.
- **Gap-aware incremental** — each subsequent run computes the gap between the latest timestamp already stored (per output file) and the latest data available on the CDN, then batch-fills everything in between. The window is **not** a fixed hardcoded range: because the daily job can fail or be skipped, a real gap may span several days (or, mid-backfill, months). The dataset itself is the source of truth for "where we left off."

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
- Trade-off: daily dumps lag by ~T+1 (a given day's file appears the next day). Acceptable for a daily-cron historical dataset; the gap-aware enumeration (below) simply treats yesterday as the latest available day.

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
- Add `latest_stored_time(existing_file, time_col)` → reads the existing merged CSV and returns its max timestamp (or `None`).
- Add per-type gap-aware fetchers: given `(symbol, type, interval, last_dt)`, enumerate the monthly+daily period URLs from `last_dt` (or the type floor) to yesterday, download+concat (skip 404), and write one `new_data/*.csv` per series.
- Main loop builds the (symbol × type × interval) job list, honors the soft `MAX_RUNTIME_MIN` budget, then merges + uploads once.

Parallelism: keep `xlin.element_mapping` with a thread pool over the (symbol × type × interval) job list, mirroring the spot updater.

## Incremental & backfill (gap-aware, enumeration by walk-from-floor)

The fetch range is derived **per output file** from the data already stored — never a fixed window.

**Per series (symbol, type, interval):**
1. Read the existing output CSV from the cloned dataset → `last_dt = max(time_col)`, or `None` if the file is absent/empty (→ full backfill).
2. `end` = yesterday UTC (today's daily dump isn't published until tomorrow).
3. Enumerate the needed periods between `last_dt` (or the type's floor) and `end`, download each, skip 404s, concat, then merge into the existing file (dedup on `time_col`).

**Enumeration = monthly for bulk + daily for the tail** (avoids day-by-day over years):

| Type | Bulk unit | Tail unit | Floor (walk start when empty) |
|------|-----------|-----------|-------------------------------|
| klines / mark / index / premium | monthly (`monthly/…/{SYM}-{iv}-{YYYY-MM}.zip`) | daily for the current month | `2020-01` |
| funding rate | monthly (`monthly/fundingRate/…`) | — (monthly re-fetched each run) | `2020-01` |
| metrics | **daily only** (no monthly dump exists) | daily | `2021-01` |

- **Backfill** (`last_dt is None`): walk monthly zips from the floor to the last complete month (skip 404s before the symbol's listing date), then daily zips for the current month. Metrics walks **daily** from its floor.
- **Incremental** (`last_dt` recent): the same walk, but starting at `last_dt`'s period — so a one-day gap fetches one daily file and a three-month outage fetches three monthly files + the current month's days. Self-healing regardless of how many runs were missed.
- Per-symbol memo of the first month that returns 200, reused across that symbol's intervals/variants, to prune 404 probes before its listing date.

**Multi-run backfill (soft time budget).** A full first backfill is ~200k small downloads (metrics daily-only from 2021 dominates at ~88k) and won't fit one 6h CI run. Rather than a single giant run, the job carries a **soft wall-clock budget** (`MAX_RUNTIME_MIN`, default ~90): once exceeded it stops starting new series, then merges + uploads what it has. Because enumeration is gap-aware, the **next run resumes exactly where this one stopped** — the backfill converges over several daily runs, and steady state stays cheap. One upload per run keeps HF commit count sane (the workflow's squash step collapses history).

**Concurrency.** Downloads are network-bound, so the thread pool is raised well above `cpu_count()` (e.g. 32) for the fetch phase.

**Robustness.** 404 = skip (delisted symbols, pre-listing dates, intervals a variant lacks); only genuine network/parse errors retry. **Missing-monthly daily fallback:** if a needed monthly zip is absent (e.g. a recently-ended month whose bulk file isn't published yet), the kline family falls back to that month's daily dumps so a multi-month catch-up never silently skips a month; funding (no daily dump) cannot fall back and retries next run once its monthly zip appears. Header-aware CSV loader (current dumps have a header row; older ones don't). Generate `data/README.md` from a template if absent (file-naming + `load_dataset` usage), then stamp "Last updated on".

**Per-type toggles.** Each data category (klines / markPrice / indexPrice / premiumIndex / metrics / fundingRate) and the interval list are config constants, so the heavy variants can be trimmed or disabled without code changes.

## Known limitations

- **Funding rate lags up to one month intra-month.** Binance publishes funding rate only as **monthly** bulk dumps, and a given month's zip appears only after that month completes (verified: `monthly/fundingRate/.../2026-07.zip` → 404 mid-July; `.../2026-06.zip` → 200). There is **no daily funding dump** (verified 404). So within the current month, the newest funding data available in bulk is through the end of the last completed month; the current month's rows land once the month closes and the next run picks them up. The geo-safe REST alternative (`fapi/v1/fundingRate`) is unavailable (451 from CI), so this lag is inherent. Klines and metrics are daily dumps and only lag ~T+1.

## Workflow (`USDT-M_Perpetual_Futures_update.yaml`)

Copy of `update.yaml` with:
- Clone `https://…@huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures` into `data`.
- Run `USDT-M_Perpetual_Futures_updater.py`.
- Drop `BINANCE_API_KEY` / `BINANCE_API_SECRET` env (unused); keep `HF_TOKEN`; add `MAX_RUNTIME_MIN` (soft budget, default ~90).
- Raise `timeout-minutes` toward the GH Actions max (~350) so the soft budget — not the hard kill — ends each run and progress is always uploaded.
- Keep the history-squash step (repo-size control).
- Cron offset a couple hours from the spot job (e.g. `0 2 * * *`) so the two daily jobs don't hit HF simultaneously.

## Dependencies

No new dependencies required — `pandas`, `requests`, `huggingface_hub`, and `xlin` are already in `pyproject.toml`. `python-binance` becomes unused by this script but stays declared (still used by `updater.py`); harmless.

## Out of scope

- Additional bulk datasets not requested (aggTrades, trades, bookDepth, bookTicker, liquidationSnapshot).
- Any change to the existing spot `updater.py` / `update.yaml`.
- Using the S3 XML listing API for enumeration — rejected because it's region-blocked/untestable from the dev environment; the walk-from-floor approach uses only the CDN, which is reachable everywhere the CI job and the maintainer run.
