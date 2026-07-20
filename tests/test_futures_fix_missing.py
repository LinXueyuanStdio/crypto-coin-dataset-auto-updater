import importlib.util
import builtins
from pathlib import Path

import pandas as pd


SCRIPT = Path(__file__).resolve().parents[1] / "futures_fix_missing.py"


def load_module():
    spec = importlib.util.spec_from_file_location("futures_fix_missing", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_normalize_funding_frame_aligns_millisecond_offsets():
    fix = load_module()
    df = pd.DataFrame(
        {
            "calc_time": [
                pd.Timestamp("2026-07-20 00:00:00"),
                pd.Timestamp("2026-07-20 08:00:00.001"),
                pd.Timestamp("2026-07-20 16:00:00.005"),
            ],
            "funding_interval_hours": [8, 8, 8],
            "last_funding_rate": ["0.0001", "0.0002", "0.0003"],
        }
    )

    normalized = fix.normalize_funding_file_frame(df)

    assert list(normalized["calc_time"]) == [
        pd.Timestamp("2026-07-20 00:00:00"),
        pd.Timestamp("2026-07-20 08:00:00"),
        pd.Timestamp("2026-07-20 16:00:00"),
    ]
    assert normalized["last_funding_rate"].tolist() == ["0.0001", "0.0002", "0.0003"]


def test_fetch_funding_range_includes_subsecond_api_offsets():
    fix = load_module()
    calls = []

    class FakeClient:
        def get(self, endpoint, params, *, signed=False):
            calls.append((endpoint, params))
            return []

    fix.fetch_funding_range(
        FakeClient(),
        "BTCUSDT",
        pd.Timestamp("2026-07-20 08:00:00"),
        pd.Timestamp("2026-07-20 08:00:00"),
    )

    assert calls[0][1]["endTime"] == int(pd.Timestamp("2026-07-20 08:00:00.999").timestamp() * 1000)


def test_fetch_funding_range_infers_four_hour_interval():
    fix = load_module()

    class FakeClient:
        def get(self, endpoint, params, *, signed=False):
            return [
                {"fundingTime": int(pd.Timestamp("2026-07-20 08:00:00.001").timestamp() * 1000), "fundingRate": "0.1"},
                {"fundingTime": int(pd.Timestamp("2026-07-20 12:00:00.001").timestamp() * 1000), "fundingRate": "0.2"},
            ]

    fetched = fix.fetch_funding_range(
        FakeClient(),
        "BTCUSDT",
        pd.Timestamp("2026-07-20 08:00:00"),
        pd.Timestamp("2026-07-20 12:00:00"),
    )

    assert fetched["funding_interval_hours"].tolist() == [4, 4]


def test_metrics_missing_ranges_use_kline_start_and_recent_api_window(tmp_path):
    fix = load_module()
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    pd.DataFrame(
        {
            "open_time": pd.date_range("2026-07-20 00:00:00", periods=4, freq="5min"),
            "open": [1, 1, 1, 1],
            "high": [1, 1, 1, 1],
            "low": [1, 1, 1, 1],
            "close": [1, 1, 1, 1],
        }
    ).to_parquet(symbol_dir / "BTCUSDT_5m.parquet", index=False)
    pd.DataFrame(
        {
            "create_time": [pd.Timestamp("2026-07-20 00:10:00")],
            "symbol": ["BTCUSDT"],
            "sum_open_interest": [1.0],
            "sum_open_interest_value": [2.0],
            "count_toptrader_long_short_ratio": [3.0],
            "sum_toptrader_long_short_ratio": [4.0],
            "count_long_short_ratio": [5.0],
            "sum_taker_long_short_vol_ratio": [6.0],
        }
    ).to_parquet(symbol_dir / "BTCUSDT_metrics.parquet", index=False)

    tasks = fix.plan_metrics_repairs(
        tmp_path,
        ["BTCUSDT"],
        now=pd.Timestamp("2026-07-23 00:00:00"),
    )

    assert [(task.start, task.end) for task in tasks] == [
        (pd.Timestamp("2026-07-20 00:00:00"), pd.Timestamp("2026-07-20 00:05:00")),
        (pd.Timestamp("2026-07-20 00:15:00"), pd.Timestamp("2026-07-20 00:15:00")),
    ]


def test_metrics_off_grid_timestamps_create_normalization_task(tmp_path):
    fix = load_module()
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    pd.DataFrame(
        {
            "create_time": [pd.Timestamp("2026-07-20 00:05:00.001")],
            "symbol": ["BTCUSDT"],
            "sum_open_interest": [1.0],
            "sum_open_interest_value": [2.0],
            "count_toptrader_long_short_ratio": [3.0],
            "sum_toptrader_long_short_ratio": [4.0],
            "count_long_short_ratio": [5.0],
            "sum_taker_long_short_vol_ratio": [6.0],
        }
    ).to_parquet(symbol_dir / "BTCUSDT_metrics.parquet", index=False)

    tasks = fix.plan_metrics_repairs(
        tmp_path,
        ["BTCUSDT"],
        now=pd.Timestamp("2026-07-23 00:00:00"),
    )

    assert len(tasks) == 1
    assert tasks[0].start == pd.Timestamp("2026-07-20 00:05:00")
    assert tasks[0].reason == "off-grid or duplicate metrics timestamp"


def test_normalize_kline_frame_aligns_open_time_offsets():
    fix = load_module()
    df = pd.DataFrame(
        {
            "open_time": [pd.Timestamp("2026-07-20 00:00:00.001")],
            "open": ["1"],
            "high": ["2"],
            "low": ["0.5"],
            "close": ["1.5"],
            "volume": ["10"],
            "close_time": [pd.Timestamp("2026-07-20 00:04:59.999")],
            "quote_volume": ["15"],
            "count": ["3"],
            "taker_buy_volume": ["4"],
            "taker_buy_quote_volume": ["6"],
            "ignore": ["0"],
        }
    )

    normalized = fix.normalize_kline_file_frame(df, "5m")

    assert normalized["open_time"].tolist() == [pd.Timestamp("2026-07-20 00:00:00")]
    assert normalized["close"].tolist() == ["1.5"]
    assert normalized["count"].tolist() == ["3"]


def test_kline_missing_ranges_create_repair_task(tmp_path):
    fix = load_module()
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    pd.DataFrame(
        {
            "open_time": [
                pd.Timestamp("2026-07-20 00:00:00"),
                pd.Timestamp("2026-07-20 00:10:00"),
            ],
            "open": [1, 1],
            "high": [1, 1],
            "low": [1, 1],
            "close": [1, 1],
            "volume": [1, 1],
            "close_time": [
                pd.Timestamp("2026-07-20 00:04:59.999"),
                pd.Timestamp("2026-07-20 00:14:59.999"),
            ],
            "quote_volume": [1, 1],
            "count": [1, 1],
            "taker_buy_volume": [1, 1],
            "taker_buy_quote_volume": [1, 1],
            "ignore": [0, 0],
        }
    ).to_parquet(symbol_dir / "BTCUSDT_5m.parquet", index=False)

    tasks = fix.plan_kline_repairs(tmp_path, ["BTCUSDT"], intervals=["5m"], kinds=["ohlcv"])

    assert len(tasks) == 1
    assert tasks[0].kind == "ohlcv"
    assert tasks[0].start == pd.Timestamp("2026-07-20 00:05:00")
    assert tasks[0].end == pd.Timestamp("2026-07-20 00:05:00")


def test_approved_funding_repair_writes_backup_and_normalized_parquet(tmp_path):
    fix = load_module()
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    path = symbol_dir / "BTCUSDT_fundingRate.parquet"
    pd.DataFrame(
        {
            "calc_time": [pd.Timestamp("2026-07-20 08:00:00.001")],
            "funding_interval_hours": [8],
            "last_funding_rate": ["0.0001"],
        }
    ).to_parquet(path, index=False)
    task = fix.RepairTask(
        kind="funding",
        symbol="BTCUSDT",
        path=path,
        start=pd.Timestamp("2026-07-20 08:00:00"),
        end=pd.Timestamp("2026-07-20 08:00:00"),
        reason="off-grid funding timestamp",
    )

    result = fix.apply_repair_task(
        task,
        data_dir=tmp_path,
        fetcher=lambda _: pd.DataFrame(
            {
                "calc_time": [pd.Timestamp("2026-07-20 08:00:00.001")],
                "funding_interval_hours": [8],
                "last_funding_rate": ["0.0002"],
            }
        ),
        prompt=lambda _: "y",
        apply=True,
        context_rows=1,
    )

    repaired = pd.read_parquet(path)
    backups = list(symbol_dir.glob("BTCUSDT_fundingRate.parquet.bak-*"))
    assert result.accepted is True
    assert len(backups) == 1
    assert repaired["calc_time"].tolist() == [pd.Timestamp("2026-07-20 08:00:00")]
    assert repaired["last_funding_rate"].tolist() == ["0.0002"]


def test_declined_repair_leaves_parquet_unchanged(tmp_path):
    fix = load_module()
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    path = symbol_dir / "BTCUSDT_fundingRate.parquet"
    original = pd.DataFrame(
        {
            "calc_time": [pd.Timestamp("2026-07-20 08:00:00.001")],
            "funding_interval_hours": [8],
            "last_funding_rate": ["0.0001"],
        }
    )
    original.to_parquet(path, index=False)
    task = fix.RepairTask(
        kind="funding",
        symbol="BTCUSDT",
        path=path,
        start=pd.Timestamp("2026-07-20 08:00:00"),
        end=pd.Timestamp("2026-07-20 08:00:00"),
        reason="off-grid funding timestamp",
    )

    result = fix.apply_repair_task(
        task,
        data_dir=tmp_path,
        fetcher=lambda _: pd.DataFrame(
            {
                "calc_time": [pd.Timestamp("2026-07-20 08:00:00")],
                "funding_interval_hours": [8],
                "last_funding_rate": ["0.0002"],
            }
        ),
        prompt=lambda _: "n",
        apply=True,
        context_rows=1,
    )

    assert result.accepted is False
    pd.testing.assert_frame_equal(pd.read_parquet(path), original)
    assert list(symbol_dir.glob("*.bak-*")) == []


def test_empty_fetched_candidate_does_not_write_backup(tmp_path):
    fix = load_module()
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    path = symbol_dir / "BTCUSDT_indexPrice_1d.parquet"
    original = pd.DataFrame(
        {
            "open_time": [pd.Timestamp("2026-07-20")],
            "open": ["1"],
            "high": ["2"],
            "low": ["0.5"],
            "close": ["1.5"],
            "volume": ["0"],
            "close_time": [pd.Timestamp("2026-07-20 23:59:59.999")],
            "quote_volume": ["0"],
            "count": ["86400"],
            "taker_buy_volume": ["0"],
            "taker_buy_quote_volume": ["0"],
            "ignore": ["0"],
        }
    )
    original.to_parquet(path, index=False)
    task = fix.RepairTask(
        kind="indexPrice",
        symbol="BTCUSDT",
        path=path,
        start=pd.Timestamp("2026-07-21"),
        end=pd.Timestamp("2026-07-21"),
        reason="missing indexPrice 1d rows or values",
    )

    result = fix.apply_repair_task(
        task,
        data_dir=tmp_path,
        fetcher=lambda _: pd.DataFrame(columns=fix.KLINE_COLUMNS),
        prompt=lambda _: "y",
        apply=True,
        context_rows=0,
    )

    assert result.wrote is False
    assert result.message == "no fetched candidate"
    pd.testing.assert_frame_equal(pd.read_parquet(path), original)
    assert list(symbol_dir.glob("*.bak-*")) == []


def test_yes_flag_applies_without_prompting(tmp_path, monkeypatch):
    fix = load_module()
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    path = symbol_dir / "BTCUSDT_fundingRate.parquet"
    pd.DataFrame(
        {
            "calc_time": [pd.Timestamp("2026-07-20 08:00:00.001")],
            "funding_interval_hours": [8],
            "last_funding_rate": ["0.0001"],
        }
    ).to_parquet(path, index=False)
    task = fix.RepairTask(
        kind="funding",
        symbol="BTCUSDT",
        path=path,
        start=pd.Timestamp("2026-07-20 08:00:00"),
        end=pd.Timestamp("2026-07-20 08:00:00"),
        reason="off-grid funding timestamp",
    )

    monkeypatch.setattr(fix, "configure_environment", lambda: (None, None))
    monkeypatch.setattr(fix, "plan_kline_repairs", lambda *args, **kwargs: [])
    monkeypatch.setattr(fix, "plan_funding_repairs", lambda *args, **kwargs: [task])
    monkeypatch.setattr(fix, "plan_metrics_repairs", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        fix,
        "make_fetcher",
        lambda client: lambda _: pd.DataFrame(
            {
                "calc_time": [pd.Timestamp("2026-07-20 08:00:00")],
                "funding_interval_hours": [8],
                "last_funding_rate": ["0.0002"],
            }
        ),
    )
    monkeypatch.setattr(builtins, "input", lambda _: (_ for _ in ()).throw(AssertionError("prompted")))

    rc = fix.main(["--data-dir", str(tmp_path), "--apply", "--yes"])

    assert rc == 0
    assert pd.read_parquet(path)["last_funding_rate"].tolist() == ["0.0002"]
