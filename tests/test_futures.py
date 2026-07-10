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
