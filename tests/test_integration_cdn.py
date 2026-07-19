import importlib.util
import os
import pathlib

import pytest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "futures_updater.py"


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
