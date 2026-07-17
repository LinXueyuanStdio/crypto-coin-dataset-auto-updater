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
- spot
- binance
pretty_name: CryptoCoin
size_categories:
- 1M<n<10M
---

# CryptoCoin (Binance Spot)

Binance **spot** OHLCV klines for USDT trading pairs, auto-updated daily.

币安 **现货** K 线数据集，每日自动更新。

Last updated on `pending`

## Quick start / 快速开始

数据集包含大量币种，完整下载体积很大。建议按需下载：

```bash
# 只克隆元数据，跳过 LFS 大文件（几秒完成）
GIT_LFS_SKIP_SMUDGE=1 git clone https://huggingface.co/datasets/linxy/CryptoCoin

cd CryptoCoin

# 只下载你需要的币种
git lfs pull --include="BTCUSDT/**"
git lfs pull --include="ETHUSDT/**"

# 或者一次指定多个
git lfs pull --include="BTCUSDT/**" --include="ETHUSDT/**" --include="SOLUSDT/**"
```

更新已有仓库时同样适用：

```bash
cd CryptoCoin
GIT_LFS_SKIP_SMUDGE=1 git pull origin main
git lfs pull --include="BTCUSDT/**"
```

## Usage / 使用

Data is stored as Parquet in per-symbol subdirectories. Files use snake_case column names.

```python
import pandas as pd
from datasets import load_dataset

# Load a single kline series
klines = load_dataset("linxy/CryptoCoin", data_files=["BTCUSDT/BTCUSDT_1d.parquet"], split="train")

# Or read directly with pandas
df = pd.read_parquet("hf://datasets/linxy/CryptoCoin/BTCUSDT/BTCUSDT_1d.parquet")
```

## File Structure

Files are organized in per-symbol subdirectories:

```
BTCUSDT/BTCUSDT_1d.parquet
BTCUSDT/BTCUSDT_1h.parquet
ETHUSDT/ETHUSDT_5m.parquet
...
```

## Intervals / 时间间隔

`1d` `12h` `8h` `6h` `4h` `2h` `1h` `30m` `15m` `5m`

## Kline Fields / 字段

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

## Available Symbols / 可用币种

See `_index.json` for the full list. Covers all TRADING spot USDT pairs on Binance.

当前覆盖 {n_symbols} 个币种。

## Sources / 来源

- **Data:** Binance spot API via [data-api.binance.vision](https://data-api.binance.vision)
- **Updater:** [GitHub](https://github.com/LinXueyuanStdio/crypto-coin-dataset-auto-updater)

## Bias, Risks, and Limitations

1. **Exchange-specific bias:** Data reflects Binance's order book only, not global markets.
2. **Temporal gaps:** Missing data during Binance outages or API failures.
3. **Market volatility:** Cryptocurrency markets are highly volatile — models may be unstable.
4. **Latency:** Data is updated with ~15 min delay after interval close.

## 偏差、风险与局限性

1. **交易所特定偏差**：数据仅反映币安的订单簿情况，而非全球市场活动
2. **时间限制**：币安宕机或API故障期间存在数据缺失
3. **市场波动性**：加密货币市场高度波动，可能影响模型稳定性
4. **延迟性**：数据更新发生在时间间隔结束后约15分钟

## Citation

```bibtex
@misc{LinXueyuanStdio2025,
  title = {CryptoCoin (Binance Spot)},
  author = {Xueyuan Lin},
  year = {2025},
  publisher = {Hugging Face},
  howpublished = {\url{https://huggingface.co/datasets/linxy/CryptoCoin}},
}
```

## Author

- LinXueyuanStdio (GitHub: [@LinXueyuanStdio](https://github.com/LinXueyuanStdio))

Last updated on `pending`
