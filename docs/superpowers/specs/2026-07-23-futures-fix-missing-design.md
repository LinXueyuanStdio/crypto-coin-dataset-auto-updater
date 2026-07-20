# Futures Fix Missing Design

## Goal

Add `futures_fix_missing.py` to validate and optionally repair local Binance USDT-M perpetual futures parquet data under `data/`.

## Scope

The script only targets the existing per-symbol futures layout:

- `{symbol}/{symbol}_{interval}.parquet`
- `{symbol}/{symbol}_markPrice_{interval}.parquet`
- `{symbol}/{symbol}_indexPrice_{interval}.parquet`
- `{symbol}/{symbol}_premiumIndex_{interval}.parquet`
- `{symbol}/{symbol}_fundingRate.parquet`
- `{symbol}/{symbol}_metrics.parquet`
- `{symbol}/{symbol}_5m.parquet` as the metrics coverage reference

It does not inspect spot or delivery futures data.

## Validation

Kline-style data is valid when `open_time` lands on the interval grid, timestamps are unique, and required OHLCV columns are present for every expected internal bar.

Funding data is valid when `calc_time` is second-aligned after normalizing Binance's millisecond offsets, timestamps are unique, and required columns are present. Known API offsets such as `2026-07-20 08:00:00.001` are treated as repairable timestamp normalization defects. The funding interval is inferred from API timestamp spacing or inherited from the local file so 4-hour funding symbols are not rewritten as 8-hour funding.

Metrics data is valid when `create_time` is on the 5-minute grid, timestamps are unique, required columns are present, numeric metric fields are not null inside the covered window, and local coverage starts no later than the symbol's 5-minute kline start when that period is available from Binance's recent futures-data API window.

## Repair Flow

The script scans local parquet files, groups issues into repair tasks, fetches candidate rows from Binance Futures API, prints nearby local rows plus fetched and merged previews, and asks for approval before writing. Accepted repairs write a timestamped backup beside the original parquet, then replace the parquet with sorted, de-duplicated data using the original column order. `_index.json` is updated when present.

`--dry-run` is the default. `--apply` enables approved writes. In non-interactive mode the script reports issues but does not write.

## Binance Access

The script loads `.env`, maps `BINANCE_PROXY` into `requests`, and reads `BINANCE_API_KEY` plus either `BINANCE_SECRET_KEY` or `BINANCE_API_SECRET`. Public futures endpoints are usable without credentials, but credentials are added when present for compatibility with existing project scripts.

## Testing

Unit tests use temporary parquet files and fake fetchers. They cover funding timestamp normalization, metrics missing-range detection, interactive approval/write behavior, and declined repair behavior.
