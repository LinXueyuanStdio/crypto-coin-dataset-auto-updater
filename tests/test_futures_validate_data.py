import importlib.util
from pathlib import Path

import pandas as pd
import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "futures_validate_data.py"


@pytest.fixture(scope="session")
def v():
    spec = importlib.util.spec_from_file_location("futures_validate_data", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_classify_file(v):
    assert v.classify_file("BTCUSDT_1d.parquet") == ("ohlcv", "1d")
    assert v.classify_file("BTCUSDT_5m.parquet") == ("ohlcv", "5m")
    assert v.classify_file("BTCUSDT_markPrice_1d.parquet") == ("markPrice", "1d")
    assert v.classify_file("BTCUSDT_indexPrice_30m.parquet") == ("indexPrice", "30m")
    assert v.classify_file("BTCUSDT_premiumIndex_1d.parquet") == ("premiumIndex", "1d")
    assert v.classify_file("BTCUSDT_fundingRate.parquet") == ("funding", None)
    assert v.classify_file("BTCUSDT_metrics.parquet") == ("metrics", None)
    assert v.classify_file("BTCUSDT_info.json") == ("info", None)


def test_expected_files_count_and_shape(v):
    files = v.expected_files("BTCUSDT")
    assert len(files) == 43
    assert "BTCUSDT_1d.parquet" in files
    assert "BTCUSDT_markPrice_1d.parquet" in files
    assert "BTCUSDT_metrics.parquet" in files
    assert "BTCUSDT_fundingRate.parquet" in files
    assert "BTCUSDT_info.json" in files
    # 不应出现双下划线（历史拼接 bug）
    assert not any("__" in f for f in files)


def test_parse_symbols(v):
    assert v.parse_symbols("btc,ETH") == ["BTC", "ETH"]
    assert v.parse_symbols(" BTCUSDT , ethusdt ") == ["BTCUSDT", "ETHUSDT"]
    assert v.parse_symbols(None) is None
    assert v.parse_symbols("") is None


def test_validate_schema_detects_bad_columns(v, tmp_path):
    sym_dir = tmp_path / "BTCUSDT"
    sym_dir.mkdir()
    good = pd.DataFrame({c: [0] for c in v.KLINE_COLUMNS})
    good.to_parquet(sym_dir / "BTCUSDT_1d.parquet", index=False)
    bad = pd.DataFrame({"open_time": [0], "open": [1]})
    bad.to_parquet(sym_dir / "BTCUSDT_4h.parquet", index=False)

    report = v.validate_schema(str(tmp_path), None)

    assert report["summary"]["files_checked"] == 2
    assert report["summary"]["bad_columns_count"] == 1
    assert report["summary"]["empty_count"] == 0
    bad_issue = report["issues"][0]
    assert bad_issue["type"] == "bad_columns"
    assert bad_issue["file"] == "BTCUSDT_4h.parquet"


def test_validate_coverage_complete_and_incomplete(v, tmp_path, monkeypatch):
    monkeypatch.setattr(v, "_load_symbols_json", lambda: ["BTCUSDT", "ETHUSDT"])

    # BTCUSDT 完整（43 文件）
    btc = tmp_path / "BTCUSDT"
    btc.mkdir()
    for f in v.expected_files("BTCUSDT"):
        (btc / f).touch()

    # ETHUSDT 缺 fundingRate 和 1d kline（41 文件）
    eth = tmp_path / "ETHUSDT"
    eth.mkdir()
    for f in v.expected_files("ETHUSDT") - {"ETHUSDT_fundingRate.parquet", "ETHUSDT_1d.parquet"}:
        (eth / f).touch()

    report = v.validate_coverage(str(tmp_path), None, live=False)

    s = report["summary"]
    assert s["live_symbol_count"] == 2
    assert s["disk_symbol_count"] == 2
    assert s["complete_symbol_count"] == 1
    assert s["incomplete_symbol_count"] == 1
    assert s["missing_file_count"] == 2
    assert s["missing_symbol_count"] == 0


def test_validate_coverage_symbols_filter_scopes_missing(v, tmp_path, monkeypatch):
    monkeypatch.setattr(v, "_load_symbols_json", lambda: ["BTCUSDT", "ETHUSDT"])
    btc = tmp_path / "BTCUSDT"
    btc.mkdir()
    for f in v.expected_files("BTCUSDT"):
        (btc / f).touch()

    # 只检查 BTCUSDT 时，不应把 ETHUSDT 算作 missing_symbol
    report = v.validate_coverage(str(tmp_path), ["BTCUSDT"], live=False)

    assert report["summary"]["live_symbol_count"] == 1
    assert report["summary"]["disk_symbol_count"] == 1
    assert report["summary"]["missing_symbol_count"] == 0
