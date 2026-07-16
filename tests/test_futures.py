import os


def test_registry_has_six_enabled_types(fut):
    names = [dt.name for dt in fut.DATA_TYPES]
    assert names == ["klines", "markPrice", "indexPrice", "premiumIndex", "metrics", "fundingRate"]
    assert all(dt.enabled for dt in fut.DATA_TYPES)


def test_kline_type_flags(fut):
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    assert kl.per_interval and kl.has_monthly and kl.has_daily
    assert kl.time_col == "open_time"
    assert kl.ms_time_cols == ("open_time", "close_time")
    assert kl.floor == "2020-01"
    assert kl.path_segment == "klines"


def test_metrics_and_funding_flags(fut):
    m = next(dt for dt in fut.DATA_TYPES if dt.name == "metrics")
    assert m.per_interval is False and m.has_monthly is False and m.has_daily is True
    assert m.time_col == "create_time" and m.ms_time_cols == () and m.floor == "2021-01"
    f = next(dt for dt in fut.DATA_TYPES if dt.name == "fundingRate")
    assert f.has_monthly is True and f.has_daily is False
    assert f.time_col == "calc_time" and f.ms_time_cols == ("calc_time",)


def test_output_filename(fut):
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    mp = next(dt for dt in fut.DATA_TYPES if dt.name == "markPrice")
    me = next(dt for dt in fut.DATA_TYPES if dt.name == "metrics")
    fr = next(dt for dt in fut.DATA_TYPES if dt.name == "fundingRate")
    assert fut.output_filename(kl, "BTCUSDT", "1d") == "BTCUSDT/BTCUSDT_1d.parquet"
    assert fut.output_filename(mp, "BTCUSDT", "1h") == "BTCUSDT/BTCUSDT_markPrice_1h.parquet"
    assert fut.output_filename(me, "BTCUSDT", None) == "BTCUSDT/BTCUSDT_metrics.parquet"
    assert fut.output_filename(fr, "ETHUSDT", None) == "ETHUSDT/ETHUSDT_fundingRate.parquet"


def test_symbols_and_intervals(fut):
    assert "BTCUSDT" in fut.SYMBOLS and "ETHUSDT" in fut.SYMBOLS
    assert fut.INTERVALS[0] == "1d" and "5m" in fut.INTERVALS


import datetime as _dt


def test_month_helpers(fut):
    assert fut.parse_ym("2021-01") == (2021, 1)
    assert fut.next_month(2020, 12) == (2021, 1)
    assert fut.next_month(2020, 5) == (2020, 6)
    assert fut.months_range((2020, 11), (2021, 2)) == [(2020, 11), (2020, 12), (2021, 1)]
    assert fut.months_range((2021, 5), (2021, 5)) == []


def test_file_url_klines(fut):
    kl = next(dt for dt in fut.DATA_TYPES if dt.name == "klines")
    assert fut.file_url(kl, "BTCUSDT", "1d", "daily", _dt.date(2026, 7, 8)) == (
        "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-07-08.zip"
    )
    assert fut.file_url(kl, "BTCUSDT", "1h", "monthly", (2025, 5)) == (
        "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2025-05.zip"
    )


def test_file_url_markprice_uses_variant_only_in_path(fut):
    mp = next(dt for dt in fut.DATA_TYPES if dt.name == "markPrice")
    assert fut.file_url(mp, "BTCUSDT", "1h", "daily", _dt.date(2026, 7, 6)) == (
        "https://data.binance.vision/data/futures/um/daily/markPriceKlines/BTCUSDT/1h/BTCUSDT-1h-2026-07-06.zip"
    )


def test_file_url_metrics_and_funding(fut):
    me = next(dt for dt in fut.DATA_TYPES if dt.name == "metrics")
    fr = next(dt for dt in fut.DATA_TYPES if dt.name == "fundingRate")
    assert fut.file_url(me, "BTCUSDT", None, "daily", _dt.date(2026, 7, 8)) == (
        "https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-07-08.zip"
    )
    assert fut.file_url(fr, "BTCUSDT", None, "monthly", (2026, 6)) == (
        "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-06.zip"
    )


def _by_name(fut, name):
    return next(dt for dt in fut.DATA_TYPES if dt.name == name)


def test_enumerate_klines_backfill(fut):
    kl = _by_name(fut, "klines")
    months, days = fut.enumerate_periods(kl, None, _dt.date(2026, 7, 8))
    assert months[0] == (2020, 1)
    assert months[-1] == (2026, 6)          # complete months, June is last before July
    assert days == [_dt.date(2026, 7, d) for d in range(1, 9)]


def test_enumerate_klines_small_gap_same_month(fut):
    kl = _by_name(fut, "klines")
    last = _dt.datetime(2026, 7, 5, 12, 0)
    months, days = fut.enumerate_periods(kl, last, _dt.date(2026, 7, 8))
    assert months == []                      # start month == end month
    assert days == [_dt.date(2026, 7, d) for d in range(5, 9)]


def test_enumerate_klines_multimonth_gap(fut):
    kl = _by_name(fut, "klines")
    last = _dt.datetime(2026, 5, 20, 0, 0)
    months, days = fut.enumerate_periods(kl, last, _dt.date(2026, 7, 8))
    assert months == [(2026, 5), (2026, 6)]  # May file covers 21–31, June full
    assert days == [_dt.date(2026, 7, d) for d in range(1, 9)]


def test_enumerate_metrics_daily_only(fut):
    me = _by_name(fut, "metrics")
    months, days = fut.enumerate_periods(me, _dt.datetime(2026, 7, 6), _dt.date(2026, 7, 8))
    assert months == []
    assert days == [_dt.date(2026, 7, 6), _dt.date(2026, 7, 7), _dt.date(2026, 7, 8)]


def test_enumerate_funding_monthly_incl_current(fut):
    fr = _by_name(fut, "fundingRate")
    months, days = fut.enumerate_periods(fr, _dt.datetime(2026, 6, 15), _dt.date(2026, 7, 8))
    assert months == [(2026, 6), (2026, 7)]
    assert days == []


def test_enumerate_nothing_when_up_to_date(fut):
    kl = _by_name(fut, "klines")
    months, days = fut.enumerate_periods(kl, _dt.datetime(2026, 7, 9), _dt.date(2026, 7, 8))
    assert months == [] and days == []


import io as _io
import zipfile as _zip


def _zip_bytes(csv_text):
    buf = _io.BytesIO()
    with _zip.ZipFile(buf, "w") as z:
        z.writestr("data.csv", csv_text)
    return buf.getvalue()


def test_read_zip_csv_with_header(fut):
    text = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1783468800000,1,2,0,1,5,1783555199999,10,3,4,6,0\n"
    df = fut.read_zip_csv(_zip_bytes(text), list(fut.KLINE_COLUMNS))
    assert list(df.columns) == list(fut.KLINE_COLUMNS)
    assert len(df) == 1
    assert df.iloc[0]["open_time"] == "1783468800000"


def test_read_zip_csv_without_header(fut):
    text = "1783468800000,1,2,0,1,5,1783555199999,10,3,4,6,0\n"
    df = fut.read_zip_csv(_zip_bytes(text), list(fut.KLINE_COLUMNS))
    assert len(df) == 1
    assert df.iloc[0]["close_time"] == "1783555199999"


def test_normalize_times_klines_ms(fut):
    kl = _by_name(fut, "klines")
    text = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1783468800000,1,2,0,1,5,1783555199999,10,3,4,6,0\n"
    df = fut.normalize_times(fut.read_zip_csv(_zip_bytes(text), list(fut.KLINE_COLUMNS)), kl)
    import pandas as pd
    assert pd.api.types.is_datetime64_any_dtype(df["open_time"])
    assert str(df.iloc[0]["open_time"]) == "2026-07-08 00:00:00"


def test_normalize_times_metrics_string(fut):
    me = _by_name(fut, "metrics")
    text = "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio\n2026-07-08 00:05:00,BTCUSDT,99414.3,6303136243.2,1.58,1.39,1.42,1.75\n"
    df = fut.normalize_times(fut.read_zip_csv(_zip_bytes(text), list(fut.METRICS_COLUMNS)), me)
    import pandas as pd
    assert pd.api.types.is_datetime64_any_dtype(df["create_time"])
    assert str(df.iloc[0]["create_time"]) == "2026-07-08 00:05:00"


class _Resp:
    def __init__(self, status, content=b""):
        self.status_code = status
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"status {self.status_code}")


def test_download_200_returns_df(fut, monkeypatch):
    text = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1,1,2,0,1,5,2,10,3,4,6,0\n"
    monkeypatch.setattr(fut.SESSION, "get", lambda url, timeout=30: _Resp(200, _zip_bytes(text)))
    df = fut.download_series_file("http://x/y.zip", list(fut.KLINE_COLUMNS))
    assert df is not None and len(df) == 1


def test_download_404_returns_none(fut, monkeypatch):
    monkeypatch.setattr(fut.SESSION, "get", lambda url, timeout=30: _Resp(404))
    assert fut.download_series_file("http://x/missing.zip", list(fut.KLINE_COLUMNS)) is None


def test_download_retries_then_raises(fut, monkeypatch):
    calls = {"n": 0}

    def boom(url, timeout=30):
        calls["n"] += 1
        raise fut.requests.ConnectionError("network down")

    monkeypatch.setattr(fut.SESSION, "get", boom)
    monkeypatch.setattr(fut.time, "sleep", lambda s: None)
    import pytest
    with pytest.raises(fut.requests.RequestException):
        fut.download_series_file("http://x/y.zip", list(fut.KLINE_COLUMNS), max_retries=3)
    assert calls["n"] == 3


def test_fetch_series_concats_and_normalizes(fut):
    kl = _by_name(fut, "klines")
    import pandas as pd

    def fake_dl(url, columns):
        # extract date from last 3 dash-separated segments e.g. ...BTCUSDT-1d-2026-07-06.zip
        day = "-".join(url.split("-")[-3:]).replace(".zip", "")
        ts = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
        row = [str(ts), "1", "2", "0", "1", "5", str(ts + 1), "10", "3", "4", "6", "0"]
        return pd.DataFrame([row], columns=list(kl.columns))

    df = fut.fetch_series(kl, "BTCUSDT", "1d", _dt.datetime(2026, 7, 6), _dt.date(2026, 7, 8), downloader=fake_dl)
    assert df is not None
    assert len(df) == 3
    assert pd.api.types.is_datetime64_any_dtype(df["open_time"])


def test_fetch_series_returns_none_when_all_404(fut):
    kl = _by_name(fut, "klines")
    df = fut.fetch_series(kl, "BTCUSDT", "1d", _dt.datetime(2026, 7, 6), _dt.date(2026, 7, 8),
                          downloader=lambda url, columns: None)
    assert df is None


def test_fetch_series_empty_range_returns_none(fut):
    kl = _by_name(fut, "klines")
    called = {"n": 0}

    def dl(url, columns):
        called["n"] += 1
        return None

    df = fut.fetch_series(kl, "BTCUSDT", "1d", _dt.datetime(2026, 7, 9), _dt.date(2026, 7, 8), downloader=dl)
    assert df is None and called["n"] == 0


def test_latest_stored_time_missing_file(fut, tmp_path):
    assert fut.latest_stored_time(str(tmp_path / "nope.csv"), "open_time") is None


def test_latest_stored_time_reads_max(fut, tmp_path):
    p = tmp_path / "BTCUSDT_1d.csv"
    p.write_text("open_time,open\n2026-07-01 00:00:00,1\n2026-07-03 00:00:00,2\n")
    ts = fut.latest_stored_time(str(p), "open_time")
    assert ts is not None and ts.year == 2026 and ts.month == 7 and ts.day == 3


def test_merge_frames_dedups_and_sorts(fut):
    import pandas as pd
    existing = pd.DataFrame({"open_time": ["2026-07-01", "2026-07-02"], "close": ["a", "b"]})
    new = pd.DataFrame({"open_time": pd.to_datetime(["2026-07-02", "2026-07-03"]), "close": ["B", "c"]})
    merged = fut.merge_frames(existing, new, "open_time")
    assert list(merged["open_time"].dt.day) == [1, 2, 3]           # sorted, deduped
    assert len(merged) == 3


def test_merge_datasets_no_existing(fut, tmp_path):
    import pandas as pd
    newf = tmp_path / "new.csv"
    pd.DataFrame({"calc_time": ["2026-06-01 00:00:00"], "last_funding_rate": ["0.0001"]}).to_csv(newf, index=False)
    out = tmp_path / "out.csv"
    merged = fut.merge_datasets(str(tmp_path / "absent.csv"), str(newf), str(out), "calc_time")
    assert len(merged) == 1 and out.exists()


def test_budget(fut):
    assert fut.Budget(0).exceeded() is True
    assert fut.Budget(1000).exceeded() is False


def test_build_jobs_counts(fut):
    jobs = fut.build_jobs()
    per_interval_types = sum(1 for dt in fut.DATA_TYPES if dt.per_interval and dt.enabled)
    flat_types = sum(1 for dt in fut.DATA_TYPES if not dt.per_interval and dt.enabled)
    expected = len(fut.SYMBOLS) * (per_interval_types * len(fut.INTERVALS) + flat_types)
    assert len(jobs) == expected
    assert any(j.symbol == "BTCUSDT" and j.dt.name == "metrics" and j.interval is None for j in jobs)


def test_process_job_writes_then_resumes(fut, tmp_path, monkeypatch):
    import pandas as pd
    kl = _by_name(fut, "klines")

    # first run: nothing stored -> fake fetch returns 2 rows
    def fetch_first(dt, symbol, interval, last_dt, end_date, downloader=None):
        assert last_dt is None
        return pd.DataFrame({
            "open_time": pd.to_datetime(["2026-07-01", "2026-07-02"]),
            "close": ["1", "2"],
        })

    monkeypatch.setattr(fut, "fetch_series", fetch_first)
    path, new_last = fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 2), None)
    assert path is not None and pd.read_parquet(path).shape[0] == 2
    assert new_last.day == 2

    # second run: last stored is 2026-07-02 -> fetch returns overlap + 1 new
    def fetch_second(dt, symbol, interval, last_dt, end_date, downloader=None):
        assert last_dt is not None and last_dt.day == 2
        return pd.DataFrame({
            "open_time": pd.to_datetime(["2026-07-02", "2026-07-03"]),
            "close": ["2", "3"],
        })

    monkeypatch.setattr(fut, "fetch_series", fetch_second)
    _, new_last2 = fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 3), _dt.datetime(2026, 7, 2))
    df = pd.read_parquet(path)
    assert df.shape[0] == 3  # deduped 2026-07-02
    assert new_last2.day == 3


def test_process_job_none_when_no_new_data(fut, tmp_path, monkeypatch):
    kl = _by_name(fut, "klines")
    monkeypatch.setattr(fut, "fetch_series", lambda *a, **k: None)
    assert fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 3), None) is None


def test_ensure_and_stamp_readme(fut, tmp_path):
    p = tmp_path / "README.md"
    fut.ensure_readme(str(p))
    assert p.exists()
    body = p.read_text(encoding="utf-8")
    assert "USDT-M" in body and "Last updated on" in body
    fut.stamp_readme(str(p))
    assert "Last updated on `" in p.read_text(encoding="utf-8")


def test_run_update_respects_budget_and_counts(fut, tmp_path, monkeypatch):
    kl = _by_name(fut, "klines")
    monkeypatch.setattr(fut, "SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(fut, "DATA_TYPES", [kl])
    monkeypatch.setattr(fut, "INTERVALS", ["1d", "1h"])

    processed = []

    def fake_process(dt, symbol, interval, data_folder, end_date, last_dt, downloader=None):
        processed.append(interval)
        return os.path.join(data_folder, f"{symbol}_{interval}.csv"), _dt.datetime(2026, 7, 8)

    monkeypatch.setattr(fut, "process_job", fake_process)
    n = fut.run_update(str(tmp_path), end_date=_dt.date(2026, 7, 8), budget=fut.Budget(1000), max_workers=2)
    assert n == 2 and set(processed) == {"1d", "1h"}


def test_run_update_zero_budget_skips_all(fut, tmp_path, monkeypatch):
    monkeypatch.setattr(fut, "process_job", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not run")))
    n = fut.run_update(str(tmp_path), end_date=_dt.date(2026, 7, 8), budget=fut.Budget(0), max_workers=2)
    assert n == 0


def test_fetch_series_daily_fallback_when_monthly_missing(fut):
    import pandas as pd
    kl = _by_name(fut, "klines")

    def dl(url, columns):
        if "/monthly/" in url:
            return None  # monthly not published -> must fall back to daily
        day = "-".join(url.split("-")[-3:]).replace(".zip", "")
        ts = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
        return pd.DataFrame([[str(ts)] + ["1"] * 11], columns=list(kl.columns))

    # gap June 28 -> Aug 3: monthly [(2026,6),(2026,7)] both "missing" -> daily fallback
    df = fut.fetch_series(kl, "BTCUSDT", "1d", _dt.datetime(2026, 6, 28), _dt.date(2026, 8, 3), downloader=dl)
    assert df is not None
    days_present = set(df["open_time"].dt.date)
    assert _dt.date(2026, 7, 15) in days_present   # mid-missing-month recovered via daily
    assert _dt.date(2026, 7, 31) in days_present
    assert _dt.date(2026, 6, 30) in days_present


def test_fetch_series_no_daily_fallback_for_old_missing_months(fut):
    kl = _by_name(fut, "klines")
    daily_months = set()

    def dl(url, columns):
        if "/daily/" in url:
            parts = url.split("-")
            daily_months.add(parts[-3] + "-" + parts[-2])  # YYYY-MM of the daily file
        return None  # everything missing (delisted-like)

    # Delisted-like: last data 2022-11, end 2026-07-09. Old missing months must
    # NOT trigger a daily sweep -- only recent months (<= FALLBACK_MONTHS) may.
    fut.fetch_series(kl, "FTTUSDT", "1d", _dt.datetime(2022, 11, 8), _dt.date(2026, 7, 9), downloader=dl)
    assert "2022-12" not in daily_months
    assert "2024-01" not in daily_months
    assert daily_months and all(m >= "2026-05" for m in daily_months), daily_months


def test_fetch_series_funding_no_daily_fallback(fut):
    fr = _by_name(fut, "fundingRate")
    calls = {"daily": 0}

    def dl(url, columns):
        if "/daily/" in url:
            calls["daily"] += 1
        return None

    df = fut.fetch_series(fr, "BTCUSDT", None, _dt.datetime(2026, 6, 15), _dt.date(2026, 7, 8), downloader=dl)
    assert df is None
    assert calls["daily"] == 0  # funding has no daily dump -> never falls back


def test_process_job_preserves_value_text_across_merge(fut, tmp_path, monkeypatch):
    import pandas as pd
    kl = _by_name(fut, "klines")

    def fetch1(dt, symbol, interval, last_dt, end_date, downloader=None):
        return pd.DataFrame({"open_time": pd.to_datetime(["2026-07-01"]), "open": ["1.50000000"], "volume": ["10"]})

    monkeypatch.setattr(fut, "fetch_series", fetch1)
    p, _ = fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 1), None)

    def fetch2(dt, symbol, interval, last_dt, end_date, downloader=None):
        return pd.DataFrame({"open_time": pd.to_datetime(["2026-07-02"]), "open": ["2.00000000"], "volume": ["20"]})

    monkeypatch.setattr(fut, "fetch_series", fetch2)
    fut.process_job(kl, "BTCUSDT", "1d", str(tmp_path), _dt.date(2026, 7, 2), _dt.datetime(2026, 7, 1))
    df = pd.read_parquet(p)
    assert df["open"].iloc[0] == "1.50000000"  # original value survives re-read + merge


def test_index_save_load_roundtrip(fut, tmp_path):
    idx = {"BTCUSDT_1d.csv": "2026-07-09 00:00:00", "BTCUSDT_metrics.csv": "2026-07-09 23:55:00"}
    fut.save_index(str(tmp_path), idx)
    assert (tmp_path / "_index.json").exists()
    assert fut.load_index(str(tmp_path)) == idx


def test_load_index_missing_returns_empty(fut, tmp_path):
    assert fut.load_index(str(tmp_path)) == {}


def test_index_last_dt(fut):
    idx = {"BTCUSDT_1d.csv": "2026-07-09 00:00:00"}
    ts = fut.index_last_dt(idx, "BTCUSDT_1d.csv")
    assert ts is not None and ts.year == 2026 and ts.month == 7 and ts.day == 9
    assert fut.index_last_dt(idx, "MISSING.csv") is None


def test_needs_update(fut):
    end = _dt.date(2026, 7, 9)
    assert fut.needs_update(None, end) is True                       # never fetched
    assert fut.needs_update(_dt.datetime(2026, 7, 8), end) is True   # a day behind
    assert fut.needs_update(_dt.datetime(2026, 7, 9, 12), end) is False  # already at end_date
    assert fut.needs_update(_dt.datetime(2026, 7, 10), end) is False     # ahead


def test_build_index_from_files(fut, tmp_path, monkeypatch):
    kl = _by_name(fut, "klines")
    monkeypatch.setattr(fut, "SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(fut, "DATA_TYPES", [kl])
    monkeypatch.setattr(fut, "INTERVALS", ["1d"])
    # flat CSV file (legacy layout) — build_index_from_files finds it via fallback
    (tmp_path / "BTCUSDT_1d.csv").write_text("open_time,open\n2026-07-01 00:00:00,1\n2026-07-05 00:00:00,2\n")
    idx = fut.build_index_from_files(str(tmp_path))
    assert idx == {"BTCUSDT/BTCUSDT_1d.parquet": "2026-07-05 00:00:00"}


def test_run_update_skips_up_to_date_via_index(fut, tmp_path, monkeypatch):
    kl = _by_name(fut, "klines")
    monkeypatch.setattr(fut, "SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(fut, "DATA_TYPES", [kl])
    monkeypatch.setattr(fut, "INTERVALS", ["1d"])
    fut.save_index(str(tmp_path), {"BTCUSDT/BTCUSDT_1d.parquet": "2026-07-08 00:00:00"})
    monkeypatch.setattr(fut, "process_job",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not run for up-to-date file")))
    n = fut.run_update(str(tmp_path), end_date=_dt.date(2026, 7, 8), budget=fut.Budget(1000), max_workers=2)
    assert n == 0  # last_dt (07-08) == end_date -> skipped entirely


def test_run_update_updates_index(fut, tmp_path, monkeypatch):
    kl = _by_name(fut, "klines")
    monkeypatch.setattr(fut, "SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(fut, "DATA_TYPES", [kl])
    monkeypatch.setattr(fut, "INTERVALS", ["1d"])

    def fake_process(dt, symbol, interval, data_folder, end_date, last_dt, downloader=None):
        assert last_dt is None  # no index entry -> full backfill
        return os.path.join(data_folder, "BTCUSDT_1d.csv"), _dt.datetime(2026, 7, 8, 0, 0, 0)

    monkeypatch.setattr(fut, "process_job", fake_process)
    fut.run_update(str(tmp_path), end_date=_dt.date(2026, 7, 8), budget=fut.Budget(1000), max_workers=2)
    assert fut.load_index(str(tmp_path)).get("BTCUSDT/BTCUSDT_1d.parquet") == "2026-07-08 00:00:00"


# ---- dynamic symbol discovery ----

def test_fallback_symbols_is_nonempty(fut):
    assert len(fut.FALLBACK_SYMBOLS) >= 30
    assert "BTCUSDT" in fut.FALLBACK_SYMBOLS
    assert "ETHUSDT" in fut.FALLBACK_SYMBOLS


def test_symbols_is_mutable_copy(fut):
    assert fut.SYMBOLS == fut.FALLBACK_SYMBOLS
    assert fut.SYMBOLS is not fut.FALLBACK_SYMBOLS  # independent list


def test_resolve_symbols_updates_module_list(fut, monkeypatch, tmp_path):
    monkeypatch.setattr(fut, "SYMBOLS_CACHE", str(tmp_path / ".symbols_cache.json"))
    monkeypatch.setattr(fut, "fetch_usdt_perpetual_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    fut.resolve_symbols()
    assert fut.SYMBOLS == ["BTCUSDT", "ETHUSDT"]


def test_fetch_symbols_returns_fallback_on_api_failure(fut, monkeypatch):
    monkeypatch.setattr(fut.SESSION, "get", lambda url, timeout=15: (
        (_ for _ in ()).throw(fut.requests.ConnectionError("offline"))))
    # No cache and no symbols.json -> ultimate fallback
    monkeypatch.setattr(fut, "SYMBOLS_CACHE", "/nonexistent/cache.json")
    monkeypatch.setattr(fut, "SYMBOLS_FILE", "/nonexistent/symbols.json")
    symbols = fut.fetch_usdt_perpetual_symbols()
    assert symbols == fut.FALLBACK_SYMBOLS


def test_fetch_symbols_uses_cache_when_fresh(fut, tmp_path, monkeypatch):
    cache = tmp_path / "cache.json"
    cache.write_text('{"_fetched_at": 99999999999, "symbols": ["BTCUSDT", "ETHUSDT"]}')
    monkeypatch.setattr(fut, "SYMBOLS_CACHE", str(cache))
    # Must not call the API
    called = {"n": 0}
    monkeypatch.setattr(fut.SESSION, "get", lambda *a, **k: called.update({"n": called["n"] + 1}))
    symbols = fut.fetch_usdt_perpetual_symbols()
    assert symbols == ["BTCUSDT", "ETHUSDT"]
    assert called["n"] == 0


def test_fetch_symbols_filters_usdt_perpetual_trading(fut, monkeypatch):
    class FakeResp:
        status_code = 200
        @staticmethod
        def json():
            return {"symbols": [
                {"symbol": "BTCUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "ETHUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "BTCDOWNUSDT", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "SETTLING"},
                {"symbol": "BTCUSDT_210625", "quoteAsset": "USDT", "contractType": "CURRENT_QUARTER", "status": "TRADING"},
                {"symbol": "BTCBUSD", "quoteAsset": "BUSD", "contractType": "PERPETUAL", "status": "TRADING"},
            ]}
        @staticmethod
        def raise_for_status():
            pass

    monkeypatch.setattr(fut.SESSION, "get", lambda url, timeout=15: FakeResp)
    monkeypatch.setattr(fut, "SYMBOLS_CACHE", "/nonexistent/cache2.json")
    monkeypatch.setattr(fut, "SYMBOLS_FILE", "/nonexistent/symbols.json")
    symbols = fut.fetch_usdt_perpetual_symbols()
    assert symbols == ["BTCUSDT", "ETHUSDT"]


def test_fetch_symbols_falls_back_to_symbols_json(fut, tmp_path, monkeypatch):
    # API unreachable, no cache -> should read from symbols.json
    symbols_file = tmp_path / "symbols.json"
    symbols_file.write_text('{"_fetched_at": 99999999999, "symbols": ["BTCUSDT", "ETHUSDT", "ADAUSDT"]}')
    monkeypatch.setattr(fut, "SYMBOLS_CACHE", "/nonexistent/cache3.json")
    monkeypatch.setattr(fut, "SYMBOLS_FILE", str(symbols_file))
    monkeypatch.setattr(fut.SESSION, "get", lambda url, timeout=15: (
        (_ for _ in ()).throw(fut.requests.ConnectionError("offline"))))
    symbols = fut.fetch_usdt_perpetual_symbols()
    assert symbols == ["ADAUSDT", "BTCUSDT", "ETHUSDT"]


def test_build_jobs_uses_current_symbols(fut, monkeypatch):
    monkeypatch.setattr(fut, "SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(fut, "INTERVALS", ["1d", "1h"])
    kl = _by_name(fut, "klines")
    mp = _by_name(fut, "markPrice")
    monkeypatch.setattr(fut, "DATA_TYPES", [kl, mp])
    jobs = fut.build_jobs()
    # 1 symbol × 2 types × 2 intervals = 4
    assert len(jobs) == 4
    symbols_seen = {j.symbol for j in jobs}
    assert symbols_seen == {"BTCUSDT"}
