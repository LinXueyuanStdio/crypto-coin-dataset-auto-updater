import importlib.util
import json
from pathlib import Path

import numpy as np
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


def test_load_onboard_date(v, tmp_path):
    sym_dir = tmp_path / "BTCUSDT"
    sym_dir.mkdir()
    (sym_dir / "BTCUSDT_info.json").write_text(
        v.json.dumps({"onboardDate": 1567965300000}), encoding="utf-8"
    )
    path = sym_dir / "BTCUSDT_1d.parquet"
    onboard = v._load_onboard_date(path, "BTCUSDT")
    assert onboard == pd.Timestamp(1567965300000, unit="ms")

    # 无 info.json -> None
    missing_dir = tmp_path / "ETHUSDT"
    missing_dir.mkdir()
    assert v._load_onboard_date(missing_dir / "ETHUSDT_1d.parquet", "ETHUSDT") is None


def test_gap_start_after_onboard(v):
    # 数据最早时间晚于上市日期：沿用数据最早时间
    assert v._gap_start_after_onboard(
        pd.Timestamp("2025-06-01"), pd.Timestamp("2025-05-01"), "1D"
    ) == pd.Timestamp("2025-06-01")
    # 上市前有占位数据：起点抬升到上市日期（对齐网格）
    assert v._gap_start_after_onboard(
        pd.Timestamp("2020-01-01"), pd.Timestamp("2025-05-16 08:30"), "15min"
    ) == pd.Timestamp("2025-05-16 08:30")
    # 无上市日期：沿用数据最早时间
    assert v._gap_start_after_onboard(
        pd.Timestamp("2020-01-01"), None, "1D"
    ) == pd.Timestamp("2020-01-01")


def test_collect_duplicates(v):
    floor_time = pd.Series(pd.to_datetime([
        "2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02", "2026-01-02", "2026-01-03",
    ]))
    dup, dup_times = v._collect_duplicates(floor_time)
    # 01-01 出现 2 次（多余 1），01-02 出现 3 次（多余 2），共 3 个多余重复行
    assert dup == 3
    by_time = {d["time"]: d["count"] for d in dup_times}
    assert by_time["2026-01-01T00:00:00"] == 2
    assert by_time["2026-01-02T00:00:00"] == 3

    # 无重复
    dup2, dup_times2 = v._collect_duplicates(pd.Series(pd.to_datetime(["2026-01-01", "2026-01-02"])))
    assert dup2 == 0
    assert dup_times2 == []


def test_generate_and_load_ignore_file(v, tmp_path):
    report = tmp_path / "continuity.json"
    report.write_text(
        v.json.dumps({
            "issues": [
                {"symbol": "BTCUSDT", "file": "BTCUSDT_metrics.parquet", "kind": "metrics",
                 "gaps": [{"start": "2020-01-01T00:00:00", "end": "2026-07-18T23:55:00"}]},
                {"symbol": "ETHUSDT", "file": "ETHUSDT_indexPrice_1d.parquet", "kind": "indexPrice",
                 "gaps": [{"start": "2021-01-01T00:00:00", "end": "2021-01-01T00:00:00"}]},
            ]
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    ignore_path = tmp_path / "ignore.json"
    v.generate_ignore_file(report, ignore_path)

    loaded = v.load_ignore_file(ignore_path)
    assert ("BTCUSDT", "BTCUSDT_metrics.parquet") in loaded
    assert loaded[("ETHUSDT", "ETHUSDT_indexPrice_1d.parquet")] == [
        (pd.Timestamp("2021-01-01T00:00:00"), pd.Timestamp("2021-01-01T00:00:00"))
    ]
    # 缺失文件 -> 空
    assert v.load_ignore_file(tmp_path / "nope.json") == {}


def test_filter_ignored_gaps(v):
    gaps = [
        {"start": "2021-01-01T00:00:00", "end": "2021-01-01T00:00:00"},
        {"start": "2021-02-01T00:00:00", "end": "2021-02-01T00:00:00"},
    ]
    ignored = [(pd.Timestamp("2021-01-01T00:00:00"), pd.Timestamp("2021-01-01T00:00:00"))]
    assert v._filter_ignored_gaps(gaps, ignored) == [
        {"start": "2021-02-01T00:00:00", "end": "2021-02-01T00:00:00"}
    ]
    # 无忽略 -> 原样
    assert v._filter_ignored_gaps(gaps, []) == gaps
    # 忽略区间覆盖多个 gap
    assert v._filter_ignored_gaps(gaps, [(pd.Timestamp("2021-01-01"), pd.Timestamp("2021-02-01"))]) == []


# --------------------------------------------------------------------------- #
# outliers 维度
# --------------------------------------------------------------------------- #
def test_rolling_mad_z_flags_spike(v):
    rng = np.random.default_rng(42)
    base = rng.normal(0, 0.01, 300)
    s = pd.Series(np.concatenate([base[:150], [0.5], base[150:]]))
    z = v._rolling_mad_z(s, 121)
    assert abs(z.iloc[150]) > 10
    # 平稳段 z 应远小于阈值
    assert (z.iloc[10:140].abs() < 10).all()


def test_validate_outliers_detects_price_spike(v, tmp_path):
    sym_dir = tmp_path / "BTCUSDT"
    sym_dir.mkdir()
    times = pd.date_range("2024-01-01", periods=300, freq="1h")
    rng = np.random.default_rng(7)
    close = 100 + rng.normal(0, 0.5, 300).cumsum()
    close[150] = 1000.0  # 孤立尖峰
    df = pd.DataFrame({"open_time": times, "close": close})
    df.to_parquet(sym_dir / "BTCUSDT_1h.parquet", index=False)

    report = v.validate_outliers(str(tmp_path), None, window=61, threshold=10.0)

    assert report["summary"]["files_checked"] == 1
    assert report["summary"]["total_spike_count"] >= 1
    issue = report["issues"][0]
    assert issue["kind"] == "ohlcv"
    spike_times = [s["time"] for s in issue["spikes"]]
    # close[150]=1000 的收益体现在 index 150（上涨）与 151（回落）两处
    assert "2024-01-07T06:00:00" in spike_times  # index 150
    assert "2024-01-07T07:00:00" in spike_times  # index 151


def test_validate_outliers_skips_non_kline(v, tmp_path):
    sym_dir = tmp_path / "BTCUSDT"
    sym_dir.mkdir()
    # 只有 funding 文件时也能跑（funding 纳入检查）
    f = pd.DataFrame({"calc_time": pd.date_range("2024-01-01", periods=200, freq="8h"),
                      "last_funding_rate": np.full(200, 0.0001)})
    f.to_parquet(sym_dir / "BTCUSDT_fundingRate.parquet", index=False)
    report = v.validate_outliers(str(tmp_path), None)
    assert report["summary"]["files_checked"] == 1
    assert report["summary"]["files_with_spikes"] == 0


# --------------------------------------------------------------------------- #
# halt 维度
# --------------------------------------------------------------------------- #
def test_detect_frozen_periods(v, tmp_path):
    sym_dir = tmp_path / "TESTUSDT"
    sym_dir.mkdir()
    # 上市前占位(2天) + 交易(3天) + 下架冻结(3天) + 恢复交易(2天)
    times = pd.date_range("2024-01-01", periods=10, freq="1D")
    vol = [0, 0, 100, 200, 300, 0, 0, 0, 400, 500]
    high = [10, 10, 12, 13, 14, 9, 9, 9, 15, 16]
    low = [10, 10, 11, 12, 13, 9, 9, 9, 14, 15]
    df = pd.DataFrame({"open_time": times, "high": high, "low": low, "volume": vol})
    path = sym_dir / "TESTUSDT_1d.parquet"
    df.to_parquet(path, index=False)

    periods = v._detect_frozen_periods(path, "TESTUSDT", min_days=2)

    # 只报下架冻结期（index 5..7），上市前占位（index 0..1）被排除
    assert len(periods) == 1
    assert periods[0] == (pd.Timestamp("2024-01-06"), pd.Timestamp("2024-01-08"))


def test_detect_frozen_periods_min_days_filter(v, tmp_path):
    sym_dir = tmp_path / "TESTUSDT"
    sym_dir.mkdir()
    times = pd.date_range("2024-01-01", periods=5, freq="1D")
    vol = [100, 0, 200, 300, 400]  # 仅 1 天零成交
    high = [12, 9, 13, 14, 15]
    low = [11, 9, 12, 13, 14]
    df = pd.DataFrame({"open_time": times, "high": high, "low": low, "volume": vol})
    path = sym_dir / "TESTUSDT_1d.parquet"
    df.to_parquet(path, index=False)

    assert v._detect_frozen_periods(path, "TESTUSDT", min_days=2) == []


def test_load_ohlcv_gaps(v, tmp_path):
    ignore = tmp_path / "ignore.json"
    ignore.write_text(json.dumps({"entries": [
        {"symbol": "A", "kind": "ohlcv", "gaps": [{"start": "2024-01-01", "end": "2024-01-02"}]},
        {"symbol": "B", "kind": "funding", "gaps": [{"start": "2024-01-01", "end": "2024-01-02"}]},
        {"symbol": "C", "kind": "ohlcv", "gaps": [{"start": "2024-02-01", "end": "2024-02-02"}]},
    ]}), encoding="utf-8")

    gaps = v._load_ohlcv_gaps(ignore)
    assert len(gaps) == 2
    assert [g[0] for g in gaps] == ["A", "C"]
    assert v._load_ohlcv_gaps(tmp_path / "nope.json") == []


def test_validate_halt_frozen_and_missing(v, tmp_path):
    sym_dir = tmp_path / "TESTUSDT"
    sym_dir.mkdir()
    times = pd.date_range("2024-01-01", periods=10, freq="1D")
    vol = [0, 0, 100, 200, 300, 0, 0, 0, 400, 500]
    high = [10, 10, 12, 13, 14, 9, 9, 9, 15, 16]
    low = [10, 10, 11, 12, 13, 9, 9, 9, 14, 15]
    pd.DataFrame({"open_time": times, "high": high, "low": low, "volume": vol}).to_parquet(
        sym_dir / "TESTUSDT_1d.parquet", index=False
    )

    ignore = tmp_path / "ignore.json"
    ignore.write_text(json.dumps({"entries": [
        {"symbol": "TESTUSDT", "kind": "ohlcv",
         "gaps": [{"start": "2024-02-01T00:00:00", "end": "2024-02-02T00:00:00"}]},
    ]}), encoding="utf-8")

    report = v.validate_halt(str(tmp_path), None, ignore_file=ignore, min_days=2)

    assert report["summary"]["frozen_period_count"] == 1
    assert report["summary"]["missing_period_count"] == 1
    assert report["summary"]["total_period_count"] == 2
    assert {p["type"] for p in report["periods"]} == {"frozen", "missing"}
    frozen = next(p for p in report["periods"] if p["type"] == "frozen")
    assert frozen["symbol"] == "TESTUSDT"
    assert frozen["start"] == "2024-01-06T00:00:00"
