# Crypto Coin Dataset Auto Updater

Automatically update cryptocurrency datasets on Huggingface with Binance official data.

加密货币数据集自动更新器：使用 Binance 官方数据，自动更新加密货币数据集，上传到 Huggingface。

数据集地址：[linxy/CryptoCoin](https://huggingface.co/datasets/linxy/CryptoCoin)

# 数据集使用方法

数据文件格式：`{symbol}_{interval}.csv`，例如：`BTCUSDT_1d.csv`,`ETHUSDT_1h.csv`。

```python
>>> from datasets import load_dataset
>>> dataset = load_dataset("linxy/CryptoCoin", data_files=["BTCUSDT_1d.csv"], split="train")
>>> dataset
Dataset({
    features: ['Open time', 'open', 'high', 'low', 'close', 'volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'],
    num_rows: 2649
})
```

可用的交易对：

```py
available_pairs = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "LTCUSDT",
    "BCHUSDT", "EOSUSDT", "TRXUSDT", "ETCUSDT", "LINKUSDT",
    "DOTUSDT", "ADAUSDT", "SOLUSDT", "MATICUSDT", "AVAXUSDT",
    "FILUSDT", "XLMUSDT", "DOGEUSDT", "SHIBUSDT", "LUNAUSDT",
    "UNIUSDT", "AAVEUSDT", "SANDUSDT", "MANAUSDT", "FTMUSDT",
    "ALGOUSDT", "MATICUSDT", "XEMUSDT", "ZRXUSDT", "BATUSDT",
    "CHZUSDT", "SUSHIUSDT", "CRVUSDT", "YFIUSDT", "COMPUSDT",
    "SNXUSDT", "1INCHUSDT", "LDOUSDT", "RUNEUSDT", "KSMUSDT",
    "ZILUSDT", "HBARUSDT", "MATICUSDT", "FTTUSDT", "WAVESUSDT",
    "HNTUSDT", "ICPUSDT", "FILUSDT", "XEMUSDT", "ZRXUSDT",
    "BATUSDT", "CHZUSDT", "SUSHIUSDT", "CRVUSDT", "YFIUSDT",
    "COMPUSDT", "SNXUSDT", "1INCHUSDT", "LDOUSDT", "RUNEUSDT",
    "KSMUSDT", "ZILUSDT", "HBARUSDT", "MATICUSDT", "FTTUSDT",
    "WAVESUSDT", "HNTUSDT", "ICPUSDT", "FILUSDT", "XEMUSDT",
    "ZRXUSDT", "BATUSDT", "CHZUSDT", "SUSHIUSDT", "CRVUSDT",
    "YFIUSDT", "COMPUSDT", "SNXUSDT", "1INCHUSDT", "LDOUSDT",
    "RUNEUSDT", "KSMUSDT", "ZILUSDT", "HBARUSDT", "MATICUSDT",
    "FTTUSDT", "WAVESUSDT", "HNTUSDT", "ICPUSDT", "FILUSDT",
    "XEMUSDT", "ZRXUSDT", "BATUSDT", "CHZUSDT", "SUSHIUSDT",
    "CRVUSDT", "YFIUSDT", "COMPUSDT", "SNXUSDT", "1INCHUSDT",
    "LDOUSDT", "RUNEUSDT", "KSMUSDT", "ZILUSDT", "HBARUSDT",
    "MATICUSDT", "FTTUSDT", "WAVESUSDT", "HNTUSDT", "ICPUSDT",
    "FILUSDT", "XEMUSDT", "ZRXUSDT", "BATUSDT", "CHZUSDT",
    "SUSHIUSDT", "CRVUSDT", "YFIUSDT", "COMPUSDT", "SNXUSDT",
    "1INCHUSDT", "LDOUSDT", "RUNEUSDT", "KSMUSDT", "ZILUSDT",
    "HBARUSDT", "MATICUSDT", "FTTUSDT", "WAVESUSDT", "HNTUSDT",
    "ICPUSDT", "FILUSDT", "XEMUSDT", "ZRXUSDT", "BATUSDT",
    "CHZUSDT", "SUSHIUSDT", "CRVUSDT", "YFIUSDT", "COMPUSDT",
    "SNXUSDT", "1INCHUSDT", "LDOUSDT", "RUNEUSDT", "KSMUSDT",
    "ZILUSDT", "HBARUSDT", "MATICUSDT", "FTTUSDT", "WAVESUSDT",
]
```

可用时间间隔：
```python
available_timeframes = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h",
    "12h", "1d"
]
```

## 手动更新

```sh
pip install poetry
poetry install
python updater.py
```

## **偏差、风险与局限性**
1. **交易所特定偏差**：数据仅反映币安的订单簿情况，而非全球市场活动
2. **时间限制**：币安宕机或API故障期间存在数据缺失
3. **市场波动性**：加密货币市场高度波动，可能影响模型稳定性
4. **延迟性**：数据更新发生在时间间隔结束后约15分钟

## Citation
```bibtex
@misc{LinXueyuanStdio2025,
  title = {Crypto Coin Historical Data},
  author = {Xueyuan Lin},
  year = {2025},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/LinXueyuanStdio/crypto-coin-dataset-auto-updater}}
}
```


## Author
- LinXueyuanStdio (GitHub: [@LinXueyuanStdio](https://github.com/LinXueyuanStdio))
