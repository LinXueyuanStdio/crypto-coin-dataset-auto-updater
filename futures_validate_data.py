#!/usr/bin/env python
"""Validate Binance USDT-M perpetual futures parquet data (read-only).

五个维度的独立体检，每个维度产出一个 JSON 报告，**不修改任何数据文件**：

    coverage    文件覆盖率——对照 Binance 实时 symbol 列表，核对每个 symbol
                应存在的 43 个文件是否齐全（缺/多文件、缺/残留 symbol）。
    schema      parquet 可读性 + schema 一致性——文件能否打开、是否空、列名
                列数是否符合规范。
    continuity  时间连续性——时间戳是否对齐 interval 网格、是否重复、区间内
                是否断档。
    values      数值合理性——OHLC 关系、非负、NaN。
    index       _index.json 一致性——索引键与磁盘文件双向核对、时间戳漂移。

复用 futures_fix_missing.py 的检测逻辑（missing_ranges / normalize_* /
interval_freq / kline_filename / infer_funding_interval_hours）和
futures_updater.py 的 fetch_usdt_perpetual_symbols() 获取实时 symbol 列表。

用法：

    python futures_validate_data.py --validate=coverage --symbols BTCUSDT,ETHUSDT
    python futures_validate_data.py --validate=all                 # 全量 5 个维度
    python futures_validate_data.py --validate=schema --no-live     # 覆盖维度不联网
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from futures_fix_missing import (  # noqa: E402
    INTERVALS,
    KLINE_COLUMNS,
    FUNDING_COLUMNS,
    METRICS_COLUMNS,
    KLINE_REQUIRED_COLUMNS,
    METRICS_NUMERIC_COLUMNS,
    KLINE_KIND_SPECS,
    interval_freq,
    kline_filename,
    missing_ranges,
    infer_funding_interval_hours,
    normalize_kline_file_frame,
    normalize_funding_file_frame,
    normalize_metrics_file_frame,
)
from futures_updater import fetch_usdt_perpetual_symbols, BLACKLIST_SYMBOLS  # noqa: E402

INDEX_FILENAME = "_index.json"
NON_SYMBOL_DIRS = {".git", ".claude", ".github"}
VALIDATORS = ("coverage", "schema", "continuity", "values", "index")
OUTPUT_PREFIX = "futures_validate"


# --------------------------------------------------------------------------- #
# 通用 helpers
# --------------------------------------------------------------------------- #
def _ts_str(ts):
    """pd.Timestamp / datetime -> ISO 字符串，用于 JSON 序列化。"""
    if ts is None or pd.isna(ts):
        return None
    return pd.Timestamp(ts).isoformat()


def iter_symbol_dirs(data_dir, symbols_filter=None):
    """列出 data 目录下的 symbol 子目录（排除 .git/.claude 等）。"""
    data_dir = Path(data_dir)
    if not data_dir.is_dir():
        return []
    dirs = [
        p for p in sorted(data_dir.iterdir())
        if p.is_dir() and p.name not in NON_SYMBOL_DIRS
    ]
    if symbols_filter is not None:
        wanted = set(symbols_filter)
        dirs = [p for p in dirs if p.name in wanted]
    return dirs


def iter_symbol_parquets(symbol_dir):
    """列出某个 symbol 目录下的所有 parquet 文件（不含 _info.json）。"""
    return sorted(
        p.name for p in Path(symbol_dir).iterdir()
        if p.is_file() and p.name.endswith(".parquet")
    )


def expected_files(symbol):
    """一个 symbol 应存在的全部 43 个文件名（相对 symbol 目录）。"""
    out = set()
    for kind in KLINE_KIND_SPECS:
        for iv in INTERVALS:
            out.add(kline_filename(symbol, kind, iv))
    out.add(f"{symbol}_metrics.parquet")
    out.add(f"{symbol}_fundingRate.parquet")
    out.add(f"{symbol}_info.json")
    return out


def classify_file(fname):
    """从文件名解析 (kind, interval)。

    kind 为 ohlcv/markPrice/indexPrice/premiumIndex/funding/metrics/info。
    interval 对 kline 是 "5m"/"1d" 等，对 funding/metrics/info 是 None。
    """
    stem = fname[:-len(".parquet")] if fname.endswith(".parquet") else Path(fname).stem
    if stem.endswith("_fundingRate"):
        return "funding", None
    if stem.endswith("_metrics"):
        return "metrics", None
    if stem.endswith("_info"):
        return "info", None
    # 有 suffix 的 kline 种类优先匹配，避免被 ohlcv 的空 suffix 抢走
    for kind in ("markPrice", "indexPrice", "premiumIndex"):
        suffix = KLINE_KIND_SPECS[kind]["suffix"]
        for iv in sorted(INTERVALS, key=len, reverse=True):
            if stem.endswith(f"_{suffix}_{iv}"):
                return kind, iv
    for iv in sorted(INTERVALS, key=len, reverse=True):
        if stem.endswith(f"_{iv}"):
            return "ohlcv", iv
    return "unknown", None


def time_col_of(kind):
    return {
        "funding": "calc_time",
        "metrics": "create_time",
    }.get(kind, "open_time")


def new_report(validator, data_dir, symbols_filter):
    return {
        "validator": validator,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "data_dir": str(Path(data_dir).resolve()),
        "symbols_filter": sorted(symbols_filter) if symbols_filter else None,
        "summary": {},
        "issues": [],
    }


def write_report(report, output_dir):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    name = f"{OUTPUT_PREFIX}_{report['validator']}.json"
    path = out / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    return path


# --------------------------------------------------------------------------- #
# 维度 1：coverage
# --------------------------------------------------------------------------- #
def validate_coverage(data_dir, symbols_filter, live=True):
    report = new_report("coverage", data_dir, symbols_filter)

    # 实时 / 权威 symbol 列表
    if live:
        live_symbols = sorted(s for s in fetch_usdt_perpetual_symbols() if s not in BLACKLIST_SYMBOLS)
        symbol_source = "binance (cache/api/symbols.json/fallback chain)"
    else:
        live_symbols = sorted(s for s in _load_symbols_json() if s not in BLACKLIST_SYMBOLS)
        symbol_source = "symbols.json (offline)"

    disk_symbols = sorted(
        d.name for d in iter_symbol_dirs(data_dir, symbols_filter)
        if d.name not in BLACKLIST_SYMBOLS
    )

    live_set = set(live_symbols)
    disk_set = set(disk_symbols)

    # 用户指定 --symbols 时，只关心这些 symbol 的覆盖情况
    if symbols_filter is not None:
        wanted = set(symbols_filter)
        live_set &= wanted
        disk_set &= wanted

    # 只对磁盘上存在的 symbol 做文件级核对（missing symbol 已在集合差里体现）
    missing_symbols = sorted(live_set - disk_set)
    stale_symbols = sorted(disk_set - live_set)
    checked_symbols = sorted(disk_set)

    missing_files = []
    extra_files = []
    complete = 0
    for sym in checked_symbols:
        actual = set(iter_symbol_parquets(Path(data_dir) / sym))
        if (Path(data_dir) / sym / f"{sym}_info.json").exists():
            actual.add(f"{sym}_info.json")
        exp = expected_files(sym)
        miss = sorted(exp - actual)
        extra = sorted(actual - exp)
        if not miss and not extra:
            complete += 1
        else:
            if miss:
                missing_files.append({"symbol": sym, "files": miss, "count": len(miss)})
            if extra:
                extra_files.append({"symbol": sym, "files": extra, "count": len(extra)})

    report["summary"] = {
        "symbol_source": symbol_source,
        "live_symbol_count": len(live_set),
        "disk_symbol_count": len(disk_set),
        "missing_symbol_count": len(missing_symbols),   # 实时有、磁盘无
        "stale_symbol_count": len(stale_symbols),       # 磁盘有、实时无（下架残留）
        "checked_symbol_count": len(checked_symbols),
        "complete_symbol_count": complete,
        "incomplete_symbol_count": len(checked_symbols) - complete,
        "missing_file_count": sum(m["count"] for m in missing_files),
        "extra_file_count": sum(e["count"] for e in extra_files),
    }
    report["issues"] = (
        [{"symbol": s, "type": "missing_symbol"} for s in missing_symbols]
        + [{"symbol": s, "type": "stale_symbol"} for s in stale_symbols]
        + [{"type": "missing_files", **m} for m in missing_files]
        + [{"type": "extra_files", **e} for e in extra_files]
    )
    return report


def _load_symbols_json():
    path = Path(_BASE_DIR) / "symbols.json"
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return sorted(data.get("symbols", []))
    except (OSError, ValueError):
        return []


# --------------------------------------------------------------------------- #
# 维度 2：schema
# --------------------------------------------------------------------------- #
def _expected_columns(kind):
    if kind == "funding":
        return tuple(FUNDING_COLUMNS)
    if kind == "metrics":
        return tuple(METRICS_COLUMNS)
    if kind in KLINE_KIND_SPECS:
        return tuple(KLINE_COLUMNS)
    return None


def validate_schema(data_dir, symbols_filter):
    report = new_report("schema", data_dir, symbols_filter)

    unreadable = []
    empty_files = []
    bad_columns = []
    checked = 0

    for sym_dir in iter_symbol_dirs(data_dir, symbols_filter):
        sym = sym_dir.name
        for fname in iter_symbol_parquets(sym_dir):
            path = sym_dir / fname
            checked += 1
            kind, _ = classify_file(fname)
            try:
                pf = pq.ParquetFile(path)
                nrows = pf.metadata.num_rows
                cols = tuple(pf.schema_arrow.names)
            except Exception as exc:  # 损坏 / 不可读
                unreadable.append({"symbol": sym, "file": fname, "error": str(exc)[:200]})
                continue
            if nrows == 0:
                empty_files.append({"symbol": sym, "file": fname, "kind": kind})
                continue
            expected = _expected_columns(kind)
            if expected is not None and cols != expected:
                bad_columns.append({
                    "symbol": sym, "file": fname, "kind": kind,
                    "expected": list(expected), "actual": list(cols),
                })

    report["summary"] = {
        "files_checked": checked,
        "unreadable_count": len(unreadable),
        "empty_count": len(empty_files),
        "bad_columns_count": len(bad_columns),
    }
    report["issues"] = (
        [{"type": "unreadable", **u} for u in unreadable]
        + [{"type": "empty", **e} for e in empty_files]
        + [{"type": "bad_columns", **b} for b in bad_columns]
    )
    return report


# --------------------------------------------------------------------------- #
# 维度 3：continuity
# --------------------------------------------------------------------------- #
def _check_kline_continuity(path, sym, kind, interval):
    raw = pd.read_parquet(path)
    if "open_time" not in raw.columns:
        return {"offgrid_count": None, "duplicate_count": None, "gaps": None,
                "note": "missing open_time column"}
    freq = interval_freq(interval)
    raw_time = pd.to_datetime(raw["open_time"], errors="coerce")
    floor_time = raw_time.dt.floor(freq)
    offgrid = int(raw_time.ne(floor_time).sum())
    dup = int(floor_time.duplicated().sum())

    df = normalize_kline_file_frame(raw, interval)
    gaps = []
    if not df.empty:
        start, end = df["open_time"].min(), df["open_time"].max()
        for gs, ge in missing_ranges(df, "open_time", KLINE_REQUIRED_COLUMNS, start, end, freq):
            gaps.append({"start": _ts_str(gs), "end": _ts_str(ge)})
    return {"offgrid_count": offgrid, "duplicate_count": dup, "gaps": gaps}


def _check_funding_continuity(path, sym):
    raw = pd.read_parquet(path)
    if "calc_time" not in raw.columns:
        return {"offgrid_count": None, "duplicate_count": None, "gaps": None,
                "note": "missing calc_time column"}
    raw_time = pd.to_datetime(raw["calc_time"], errors="coerce")
    floor_time = raw_time.dt.floor("s")
    offgrid = int(raw_time.ne(floor_time).sum())
    dup = int(floor_time.duplicated().sum())

    df = normalize_funding_file_frame(raw)
    gaps = []
    if not df.empty:
        hours = infer_funding_interval_hours(df)
        start, end = df["calc_time"].min(), df["calc_time"].max()
        for gs, ge in missing_ranges(df, "calc_time", FUNDING_COLUMNS, start, end, f"{hours}h"):
            gaps.append({"start": _ts_str(gs), "end": _ts_str(ge)})
    return {"offgrid_count": offgrid, "duplicate_count": dup, "gaps": gaps}


def _check_metrics_continuity(path, sym):
    raw = pd.read_parquet(path)
    if "create_time" not in raw.columns:
        return {"offgrid_count": None, "duplicate_count": None, "gaps": None,
                "note": "missing create_time column"}
    raw_time = pd.to_datetime(raw["create_time"], errors="coerce")
    floor_time = raw_time.dt.floor("5min")
    offgrid = int(raw_time.ne(floor_time).sum())
    dup = int(floor_time.duplicated().sum())

    df = normalize_metrics_file_frame(raw)
    gaps = []
    if not df.empty:
        start, end = df["create_time"].min(), df["create_time"].max()
        for gs, ge in missing_ranges(df, "create_time", METRICS_NUMERIC_COLUMNS, start, end, "5min"):
            gaps.append({"start": _ts_str(gs), "end": _ts_str(ge)})
    return {"offgrid_count": offgrid, "duplicate_count": dup, "gaps": gaps}


def validate_continuity(data_dir, symbols_filter):
    report = new_report("continuity", data_dir, symbols_filter)

    issues = []
    checked = 0
    with_gaps = 0
    total_gaps = 0

    for sym_dir in iter_symbol_dirs(data_dir, symbols_filter):
        sym = sym_dir.name
        for fname in iter_symbol_parquets(sym_dir):
            path = sym_dir / fname
            kind, interval = classify_file(fname)
            checked += 1
            try:
                if kind in KLINE_KIND_SPECS:
                    res = _check_kline_continuity(path, sym, kind, interval)
                elif kind == "funding":
                    res = _check_funding_continuity(path, sym)
                elif kind == "metrics":
                    res = _check_metrics_continuity(path, sym)
                else:
                    continue
            except Exception as exc:
                issues.append({"symbol": sym, "file": fname, "kind": kind,
                               "error": str(exc)[:200]})
                continue
            gaps = res.get("gaps") or []
            if gaps:
                with_gaps += 1
                total_gaps += len(gaps)
            if res.get("offgrid_count") or res.get("duplicate_count") or gaps:
                issues.append({
                    "symbol": sym, "file": fname, "kind": kind, "interval": interval,
                    **res,
                })

    report["summary"] = {
        "files_checked": checked,
        "files_with_issues": len(issues),
        "files_with_gaps": with_gaps,
        "total_gap_count": total_gaps,
    }
    report["issues"] = issues
    return report


# --------------------------------------------------------------------------- #
# 维度 4：values
# --------------------------------------------------------------------------- #
def validate_values(data_dir, symbols_filter):
    report = new_report("values", data_dir, symbols_filter)

    issues = []
    checked = 0
    with_ohlc = 0
    with_negative = 0
    with_nan = 0

    for sym_dir in iter_symbol_dirs(data_dir, symbols_filter):
        sym = sym_dir.name
        for fname in iter_symbol_parquets(sym_dir):
            path = sym_dir / fname
            kind, _ = classify_file(fname)
            if kind not in KLINE_KIND_SPECS:
                continue  # funding/metrics 单独处理
            checked += 1
            try:
                df = pd.read_parquet(path, columns=["open", "high", "low", "close", "volume"])
            except Exception as exc:
                issues.append({"symbol": sym, "file": fname, "error": str(exc)[:200]})
                continue
            num = df.apply(pd.to_numeric, errors="coerce")
            nan_counts = {c: int(num[c].isna().sum()) for c in num.columns if num[c].isna().any()}

            valid = num.dropna(subset=["open", "high", "low", "close"])
            high_bad = int((valid["high"] < valid[["open", "close"]].max(axis=1)).sum())
            low_bad = int((valid["low"] > valid[["open", "close"]].min(axis=1)).sum())

            # 非负检查：premiumIndex 的 OHLC 是溢价率，可为负；其余价格/成交量应 >= 0
            if kind == "premiumIndex":
                neg_cols = ["volume"]
            else:
                neg_cols = ["open", "high", "low", "close", "volume"]
            negative = {}
            for c in neg_cols:
                cnt = int((num[c] < 0).sum())
                if cnt > 0:
                    negative[c] = cnt

            if high_bad or low_bad or negative or nan_counts:
                if high_bad or low_bad:
                    with_ohlc += 1
                if negative:
                    with_negative += 1
                if nan_counts:
                    with_nan += 1
                issues.append({
                    "symbol": sym, "file": fname, "kind": kind,
                    "high_lt_max_open_close": high_bad,
                    "low_gt_min_open_close": low_bad,
                    "negative_counts": negative,
                    "nan_counts": nan_counts,
                })

    report["summary"] = {
        "files_checked": checked,
        "files_with_ohlc_violation": with_ohlc,
        "files_with_negative": with_negative,
        "files_with_nan": with_nan,
    }
    report["issues"] = issues
    return report


# --------------------------------------------------------------------------- #
# 维度 5：index
# --------------------------------------------------------------------------- #
def validate_index(data_dir, symbols_filter):
    report = new_report("index", data_dir, symbols_filter)
    data_dir = Path(data_dir)

    # 磁盘 parquet 相对路径集合（{symbol}/{file}）
    disk = set()
    for sym_dir in iter_symbol_dirs(data_dir, symbols_filter):
        for fname in iter_symbol_parquets(sym_dir):
            disk.add(f"{sym_dir.name}/{fname}")

    # 读取 _index.json
    idx_path = data_dir / INDEX_FILENAME
    if not idx_path.exists():
        report["summary"] = {"error": "_index.json not found"}
        return report
    try:
        with open(idx_path, encoding="utf-8") as f:
            index = json.load(f)
    except (OSError, ValueError) as exc:
        report["summary"] = {"error": f"cannot read _index.json: {exc}"}
        return report

    idx_keys = set(index.keys())
    if symbols_filter is not None:
        wanted = set(symbols_filter)
        idx_keys = {k for k in idx_keys if k.split("/")[0] in wanted}

    disk_without_index = sorted(disk - idx_keys)
    index_without_disk = sorted(idx_keys - disk)

    # 时间戳漂移：对磁盘有索引的文件，抽查索引值 vs 文件真实 max 时间戳。
    # 无 symbols 过滤时只抽样，避免全量读文件。
    drift = []
    sample_keys = list(disk & idx_keys)
    if symbols_filter is None:
        # 每 50 个取 1 个，且最多 300 个
        sample_keys = sample_keys[::50][:300]
    for rel in sample_keys:
        path = data_dir / rel
        fname = Path(rel).name
        kind, _ = classify_file(fname)
        tcol = time_col_of(kind)
        try:
            df = pd.read_parquet(path, columns=[tcol])
            actual = pd.to_datetime(df[tcol], errors="coerce").max()
        except Exception:
            continue
        indexed = pd.to_datetime(index.get(rel), errors="coerce")
        if pd.isna(actual) or pd.isna(indexed):
            continue
        # 索引应记录到数据的最新时间；允许当天粒度误差
        if abs((actual - indexed)) > pd.Timedelta(days=1):
            drift.append({
                "file": rel,
                "indexed": _ts_str(indexed),
                "actual_max": _ts_str(actual),
            })

    report["summary"] = {
        "index_key_count": len(idx_keys),
        "disk_parquet_count": len(disk),
        "disk_without_index": len(disk_without_index),
        "index_without_disk": len(index_without_disk),
        "drift_count": len(drift),
        "drift_sampled": len(sample_keys),
    }
    report["issues"] = (
        [{"type": "disk_without_index", "file": f} for f in disk_without_index[:5000]]
        + [{"type": "index_without_disk", "file": f} for f in index_without_disk[:5000]]
        + [{"type": "timestamp_drift", **d} for d in drift]
    )
    return report


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
VALIDATOR_FUNCS = {
    "coverage": validate_coverage,
    "schema": validate_schema,
    "continuity": validate_continuity,
    "values": validate_values,
    "index": validate_index,
}


def parse_symbols(raw):
    if not raw:
        return None
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--validate", choices=list(VALIDATOR_FUNCS) + ["all"],
                        default="all", help="要运行的维度（默认 all）")
    parser.add_argument("--data-dir", default=os.path.join(_BASE_DIR, "data"),
                        help="数据目录，默认 ./data")
    parser.add_argument("--symbols", help="逗号分隔的 symbol，如 BTCUSDT,ETHUSDT")
    parser.add_argument("--output-dir", default=os.path.join(_BASE_DIR, "output"),
                        help="报告输出目录，默认 ./output")
    parser.add_argument("--no-live", action="store_true",
                        help="coverage 维度不联网，只用 symbols.json")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    symbols = parse_symbols(args.symbols)
    to_run = list(VALIDATOR_FUNCS) if args.validate == "all" else [args.validate]

    paths = []
    for name in to_run:
        if name == "coverage":
            report = VALIDATOR_FUNCS[name](args.data_dir, symbols, live=not args.no_live)
        else:
            report = VALIDATOR_FUNCS[name](args.data_dir, symbols)
        path = write_report(report, args.output_dir)
        paths.append(path)
        s = report["summary"]
        print(f"[{name}] -> {path}")
        print(f"    {json.dumps(s, ensure_ascii=False, default=str)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
