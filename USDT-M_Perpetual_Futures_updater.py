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
