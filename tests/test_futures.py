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
    assert fut.output_filename(kl, "BTCUSDT", "1d") == "BTCUSDT_1d.csv"
    assert fut.output_filename(mp, "BTCUSDT", "1h") == "BTCUSDT_markPrice_1h.csv"
    assert fut.output_filename(me, "BTCUSDT", None) == "BTCUSDT_metrics.csv"
    assert fut.output_filename(fr, "ETHUSDT", None) == "ETHUSDT_fundingRate.csv"


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
    monkeypatch.setattr(fut.requests, "get", lambda url, timeout=30: _Resp(200, _zip_bytes(text)))
    df = fut.download_series_file("http://x/y.zip", list(fut.KLINE_COLUMNS))
    assert df is not None and len(df) == 1


def test_download_404_returns_none(fut, monkeypatch):
    monkeypatch.setattr(fut.requests, "get", lambda url, timeout=30: _Resp(404))
    assert fut.download_series_file("http://x/missing.zip", list(fut.KLINE_COLUMNS)) is None


def test_download_retries_then_raises(fut, monkeypatch):
    calls = {"n": 0}

    def boom(url, timeout=30):
        calls["n"] += 1
        raise fut.requests.ConnectionError("network down")

    monkeypatch.setattr(fut.requests, "get", boom)
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
