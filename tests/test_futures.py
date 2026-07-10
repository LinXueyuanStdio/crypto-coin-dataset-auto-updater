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
