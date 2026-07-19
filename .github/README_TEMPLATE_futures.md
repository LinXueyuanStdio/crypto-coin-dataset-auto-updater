---
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

## Quick start / 快速开始

This dataset contains many symbols — a full clone downloads hundreds of GB.
Use `GIT_LFS_SKIP_SMUDGE` to clone metadata only, then pull only the symbols you need:

```bash
# Clone metadata only (a few seconds — no large files)
GIT_LFS_SKIP_SMUDGE=1 git clone https://huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures

cd USDT-M_Perpetual_Futures

# Pull only the symbols you need
git lfs pull --include="BTCUSDT/**"
git lfs pull --include="ETHUSDT/**"

# Or multiple at once
git lfs pull --include="BTCUSDT/**" --include="ETHUSDT/**" --include="SOLUSDT/**"
```

To update an existing clone without downloading everything:

```bash
cd USDT-M_Perpetual_Futures
GIT_LFS_SKIP_SMUDGE=1 git pull origin main
git lfs pull --include="BTCUSDT/**"
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

```python
available_timeframes = [
    "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d"
]
```

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

```python
columns = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "count",
    "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]
```

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

{symbol_list}

The dataset covers **all TRADING USDT-M perpetual contracts** on Binance Futures
(currently {n_symbols} symbols).

## Sources

- **Data:** [data.binance.vision](https://data.binance.vision) (Binance public market-data mirror)
- **Updater:** [GitHub](https://github.com/LinXueyuanStdio/crypto-coin-dataset-auto-updater)
- **Processing:** Automated daily incremental updates; monthly bulk zips + daily fallback;
  new data is merged with existing parquet files and de-duplicated by timestamp.

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
  howpublished = {\url{https://huggingface.co/datasets/linxy/USDT-M_Perpetual_Futures}},
}
```

## Author

- LinXueyuanStdio (GitHub: [@LinXueyuanStdio](https://github.com/LinXueyuanStdio))

Last updated on `pending`
