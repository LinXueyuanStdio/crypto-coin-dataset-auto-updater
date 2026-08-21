#!/usr/bin/env python
"""Validate Binance USDT-M perpetual futures parquet data (read-only).

七个维度的独立体检，每个维度产出一个 JSON 报告，**不修改任何数据文件**：

    coverage    文件覆盖率——对照 Binance 实时 symbol 列表，核对每个 symbol
                应存在的 43 个文件是否齐全（缺/多文件、缺/残留 symbol）。
    schema      parquet 可读性 + schema 一致性——文件能否打开、是否空、列名
                列数是否符合规范。
    continuity  时间连续性——时间戳是否对齐 interval 网格、是否重复、区间内
                是否断档。
    values      数值合理性——OHLC 关系、非负、NaN。
    outliers    异常值/尖峰——滚动 MAD z-score 检测价格与资金费率的孤立尖峰
                （闪崩、插针等，数据本身合法但统计上极端）。
    halt        休市信息——生成 futures_halt_periods.json（下架冻结期 + 成交
                K 线缺失的维护期），供回测引擎跳过不可交易时段。
    index       _index.json 一致性——索引键与磁盘文件双向核对、时间戳漂移。

复用 futures_fix_missing.py 的检测逻辑（missing_ranges / normalize_* /
interval_freq / kline_filename / infer_funding_interval_hours）和
futures_updater.py 的 fetch_usdt_perpetual_symbols() 获取实时 symbol 列表。

用法：

    python futures_validate_data.py --validate=coverage --symbols BTCUSDT,ETHUSDT
    python futures_validate_data.py --validate=all                 # 全量 7 个维度
    python futures_validate_data.py --validate=schema --no-live     # 覆盖维度不联网
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
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
    funding_missing_ranges,
    normalize_kline_file_frame,
    normalize_funding_file_frame,
    normalize_metrics_file_frame,
)
from futures_updater import fetch_usdt_perpetual_symbols, BLACKLIST_SYMBOLS  # noqa: E402

INDEX_FILENAME = "_index.json"
NON_SYMBOL_DIRS = {".git", ".claude", ".github"}
VALIDATORS = ("coverage", "schema", "continuity", "values", "outliers", "halt", "index")
OUTPUT_PREFIX = "futures_validate"
HALT_FILENAME = "futures_halt_periods.json"


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
def _load_onboard_date(path, sym):
    """读取 symbol 的上市日期（info.json 的 onboardDate，毫秒时间戳）。

    返回 naive UTC 的 pd.Timestamp；info 缺失 / 无 onboardDate / 解析失败时返回 None。
    上市日期之前本就没有行情数据，continuity 校验据此不把上市前空档算作 gap。
    """
    info_path = path.parent / f"{sym}_info.json"
    if not info_path.exists():
        return None
    try:
        info = json.loads(info_path.read_text(encoding="utf-8"))
        onboard = info.get("onboardDate")
        if onboard is None:
            return None
        return pd.to_datetime(int(onboard), unit="ms", utc=True).tz_localize(None)
    except (OSError, ValueError, TypeError):
        return None


def _gap_start_after_onboard(data_min, onboard, freq):
    """gap 检测的起点：不早于上市日期（对齐到 freq 网格）。

    数据可能包含上市前的占位/指数价格（volume=0），但它们不应参与 gap 判定；
    只有上市日期之后的真实断档才算 gap。
    """
    if onboard is None or pd.isna(onboard):
        return data_min
    onboard_grid = pd.Timestamp(onboard).floor(freq)
    return max(data_min, onboard_grid)


def _collect_duplicates(floor_time):
    """统计 floor 后的时间戳重复，返回 (重复行数, 重复明细)。

    重复行数是多余的重复行（不含每组第一个）；重复明细列出重复次数最多的前 20 个
    时间戳及其出现次数，避免报告无限膨胀。
    """
    dup = int(floor_time.duplicated().sum())
    duplicate_times = []
    if dup > 0:
        counts = floor_time.dropna().value_counts()
        repeated = counts[counts > 1]
        duplicate_times = [
            {"time": _ts_str(t), "count": int(c)}
            for t, c in repeated.head(20).items()
        ]
    return dup, duplicate_times


def _check_kline_continuity(path, sym, kind, interval):
    raw = pd.read_parquet(path)
    if "open_time" not in raw.columns:
        return {"offgrid_count": None, "duplicate_count": None, "gaps": None,
                "note": "missing open_time column"}
    freq = interval_freq(interval)
    raw_time = pd.to_datetime(raw["open_time"], errors="coerce")
    floor_time = raw_time.dt.floor(freq)
    offgrid = int(raw_time.ne(floor_time).sum())
    dup, duplicate_times = _collect_duplicates(floor_time)

    df = normalize_kline_file_frame(raw, interval)
    gaps = []
    if not df.empty:
        end = df["open_time"].max()
        start = _gap_start_after_onboard(df["open_time"].min(), _load_onboard_date(path, sym), freq)
        for gs, ge in missing_ranges(df, "open_time", KLINE_REQUIRED_COLUMNS, start, end, freq):
            gaps.append({"start": _ts_str(gs), "end": _ts_str(ge)})
    return {"offgrid_count": offgrid, "duplicate_count": dup, "duplicate_times": duplicate_times, "gaps": gaps}


def _check_funding_continuity(path, sym):
    raw = pd.read_parquet(path)
    if "calc_time" not in raw.columns:
        return {"offgrid_count": None, "duplicate_count": None, "gaps": None,
                "note": "missing calc_time column"}
    raw_time = pd.to_datetime(raw["calc_time"], errors="coerce")
    floor_time = raw_time.dt.floor("s")
    offgrid = int(raw_time.ne(floor_time).sum())
    dup, duplicate_times = _collect_duplicates(floor_time)

    df = normalize_funding_file_frame(raw)
    gaps = []
    if not df.empty:
        start = _gap_start_after_onboard(df["calc_time"].min(), _load_onboard_date(path, sym), "1h")
        for gs, ge in funding_missing_ranges(df, start):
            gaps.append({"start": _ts_str(gs), "end": _ts_str(ge)})
    return {"offgrid_count": offgrid, "duplicate_count": dup, "duplicate_times": duplicate_times, "gaps": gaps}


def _check_metrics_continuity(path, sym):
    raw = pd.read_parquet(path)
    if "create_time" not in raw.columns:
        return {"offgrid_count": None, "duplicate_count": None, "gaps": None,
                "note": "missing create_time column"}
    raw_time = pd.to_datetime(raw["create_time"], errors="coerce")
    floor_time = raw_time.dt.floor("5min")
    offgrid = int(raw_time.ne(floor_time).sum())
    dup, duplicate_times = _collect_duplicates(floor_time)

    df = normalize_metrics_file_frame(raw)
    gaps = []
    if not df.empty:
        end = df["create_time"].max()
        start = _gap_start_after_onboard(df["create_time"].min(), _load_onboard_date(path, sym), "5min")
        for gs, ge in missing_ranges(df, "create_time", METRICS_NUMERIC_COLUMNS, start, end, "5min"):
            gaps.append({"start": _ts_str(gs), "end": _ts_str(ge)})
    return {"offgrid_count": offgrid, "duplicate_count": dup, "duplicate_times": duplicate_times, "gaps": gaps}


def validate_continuity(data_dir, symbols_filter, ignore_file=None):
    report = new_report("continuity", data_dir, symbols_filter)

    ignore = load_ignore_file(ignore_file) if ignore_file else {}

    issues = []
    checked = 0
    with_gaps = 0
    total_gaps = 0
    with_duplicates = 0
    total_duplicates = 0

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
            if ignore:
                gaps = _filter_ignored_gaps(gaps, ignore.get((sym, fname), []))
            res = dict(res)
            res["gaps"] = gaps
            if gaps:
                with_gaps += 1
                total_gaps += len(gaps)
            dup = res.get("duplicate_count") or 0
            if dup:
                with_duplicates += 1
                total_duplicates += dup
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
        "files_with_duplicates": with_duplicates,
        "total_duplicate_count": total_duplicates,
    }
    report["issues"] = issues
    return report


# --------------------------------------------------------------------------- #
# 忽略清单（ignore file）：记录已知不可在线修复的 gap，validate 时跳过
# --------------------------------------------------------------------------- #
IGNORE_FILENAME = "futures_ignore_continuity.json"


def _gap_reason(kind):
    """按 kind 给出 gap 不可在线补充的原因。

    注意：kline 查不到并非接口保留期限制（历史接口保留期很长），而是币安
    指数价格（indexPrice/markPrice/premiumIndex）在这些时段本就无数据——
    多为币安指数/标记价格当日全局中断（同一时刻影响多数币），或指数成分调整、
    币被移出指数。
    """
    if kind == "metrics":
        return "metrics 接口仅保留最近 30 天，历史缺失无法在线补"
    if kind == "funding":
        return "funding 历史缺失无法在线补"
    if kind == "ohlcv":
        return "K线在该时段无成交记录，币安数据源缺失，线上接口返回空"
    return "币安指数/标记价格在该时段无数据（指数成分调整、币被移出指数，或币安当日全局中断指数/标记价格推送），线上接口返回空"


def generate_ignore_file(report_path, output_path):
    """从 continuity 报告生成带 reason 的忽略清单。

    每个有 gap 的文件产出一条 entry，记录 symbol/file/kind/reason 及要忽略的 gap
    时间范围。下次 validate 读取该清单后，不再把这些 gap 视为缺失。
    """
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    entries = []
    for issue in report.get("issues", []):
        gaps = issue.get("gaps") or []
        if not gaps:
            continue
        kind = issue.get("kind")
        entries.append({
            "symbol": issue.get("symbol"),
            "file": issue.get("file"),
            "kind": kind,
            "reason": _gap_reason(kind),
            "gaps": gaps,
        })
    ignore = {
        "version": 1,
        "description": "已知不可在线修复的 continuity gap 忽略清单（validate 时跳过，不再视为缺失）",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "entry_count": len(entries),
        "entries": entries,
    }
    Path(output_path).write_text(
        json.dumps(ignore, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return ignore


def load_ignore_file(ignore_path):
    """读取忽略清单，返回 {(symbol, file): [(start, end), ...]} 的映射。

    缺失或损坏时返回空 dict（等价于无忽略）。
    """
    p = Path(ignore_path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    result = {}
    for entry in data.get("entries", []):
        key = (entry.get("symbol"), entry.get("file"))
        gaps = entry.get("gaps") or []
        result.setdefault(key, []).extend(
            (pd.Timestamp(g["start"]), pd.Timestamp(g["end"])) for g in gaps
        )
    return result


def _filter_ignored_gaps(gaps, ignored):
    """从 gap 列表中剔除被忽略清单覆盖的 gap。

    gaps: [{"start": str, "end": str}, ...]
    ignored: [(pd.Timestamp, pd.Timestamp), ...]
    覆盖判定：存在忽略区间 [is, ie] 使得 gap 完全落在其内。
    """
    if not ignored:
        return gaps
    kept = []
    for g in gaps:
        gs = pd.Timestamp(g["start"])
        ge = pd.Timestamp(g["end"])
        if any(is_ <= gs and ge <= ie for is_, ie in ignored):
            continue
        kept.append(g)
    return kept


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
# 维度 6：outliers（异常值/尖峰）
# --------------------------------------------------------------------------- #
def _rolling_mad_z(series, window):
    """滚动 MAD z-score（中心窗口）。

    返回与 series 等长的 z 值序列。MAD 为 0（窗口内数值恒定）时 z 为 NaN，
    该处不判定为尖峰——低流动性币大部分收益为 0，MAD 恒 0 时无法用相对离散
    衡量，宁可不报也不误报。
    """
    s = pd.Series(series, dtype="float64")
    med = s.rolling(window, center=True, min_periods=max(5, window // 4)).median()
    mad = (s - med).abs().rolling(window, center=True, min_periods=max(5, window // 4)).median()
    z = 0.6745 * (s - med) / mad.replace(0, np.nan)
    return z


def _spike_return(close, kind):
    """按 kind 计算用于尖峰检测的收益序列。"""
    close = pd.to_numeric(close, errors="coerce")
    if kind == "premiumIndex":
        # 溢价率可为负且接近 0，log return 无意义，用绝对差分
        return close.diff()
    # 价格类：log return，clip 下界避免 log(0)/负值
    return np.log(close.clip(lower=1e-12)).diff()


def _check_outliers_kline(path, kind, window, threshold):
    df = pd.read_parquet(path, columns=["open_time", "close"])
    if df.empty or "close" not in df.columns:
        return {"spike_count": 0, "spikes": [], "note": "missing close column"}
    t = pd.to_datetime(df["open_time"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    ret = _spike_return(close, kind)
    z = _rolling_mad_z(ret, window)
    mask = z.abs() > threshold
    idxs = np.where(mask.to_numpy())[0]
    spikes = [
        {
            "time": _ts_str(t.iloc[i]),
            "value": round(float(close.iloc[i]), 12),
            "return": round(float(ret.iloc[i]), 12),
            "zscore": round(float(z.iloc[i]), 2),
        }
        for i in idxs
    ]
    return {"spike_count": int(len(idxs)), "spikes": spikes[:50]}


def _check_outliers_funding(path, window, threshold):
    df = pd.read_parquet(path, columns=["calc_time", "last_funding_rate"])
    if df.empty or "last_funding_rate" not in df.columns:
        return {"spike_count": 0, "spikes": [], "note": "missing last_funding_rate column"}
    t = pd.to_datetime(df["calc_time"], errors="coerce")
    rate = pd.to_numeric(df["last_funding_rate"], errors="coerce")
    z = _rolling_mad_z(rate, window)
    mask = z.abs() > threshold
    idxs = np.where(mask.to_numpy())[0]
    spikes = [
        {
            "time": _ts_str(t.iloc[i]),
            "value": round(float(rate.iloc[i]), 12),
            "zscore": round(float(z.iloc[i]), 2),
        }
        for i in idxs
    ]
    return {"spike_count": int(len(idxs)), "spikes": spikes[:50]}


def validate_outliers(data_dir, symbols_filter, window=121, threshold=10.0):
    report = new_report("outliers", data_dir, symbols_filter)

    issues = []
    checked = 0
    with_spikes = 0
    total_spikes = 0
    by_kind = Counter()

    for sym_dir in iter_symbol_dirs(data_dir, symbols_filter):
        sym = sym_dir.name
        for fname in iter_symbol_parquets(sym_dir):
            path = sym_dir / fname
            kind, interval = classify_file(fname)
            if kind not in KLINE_KIND_SPECS and kind != "funding":
                continue
            checked += 1
            try:
                if kind == "funding":
                    res = _check_outliers_funding(path, window, threshold)
                    interval = None
                else:
                    res = _check_outliers_kline(path, kind, window, threshold)
            except Exception as exc:
                issues.append({"symbol": sym, "file": fname, "kind": kind,
                               "error": str(exc)[:200]})
                continue
            n = res.get("spike_count") or 0
            if n:
                with_spikes += 1
                total_spikes += n
                by_kind[kind] += n
                issues.append({
                    "symbol": sym, "file": fname, "kind": kind, "interval": interval,
                    "spike_count": n,
                    "spikes": res.get("spikes") or [],
                })

    report["summary"] = {
        "files_checked": checked,
        "files_with_spikes": with_spikes,
        "total_spike_count": total_spikes,
        "by_kind": dict(by_kind),
        "params": {"window": window, "threshold": threshold},
    }
    report["issues"] = issues
    return report


# --------------------------------------------------------------------------- #
# 维度 7：halt（休市信息，供回测引擎跳过不可交易时段）
# --------------------------------------------------------------------------- #
def _detect_frozen_periods(path, sym, min_days=2):
    """从 ohlcv 1d K 线检测下架冻结期。

    冻结判据：连续 (volume==0 且 high==low) 的天段——下架后币安用零成交、
    价格冻结的占位记录填充（见 docs/known-issues.md #5），不是数据缺失。

    只检测「首个真实成交（volume>0）之后」的冻结段：数据起点的零成交段是
    上市前占位，不算下架。不能用 onboardDate 过滤——对下架后重新上线的币，
    onboardDate 是「重新上线日」，下架冻结期恰恰落在它之前，会被误排除。
    段长 < min_days 的忽略，避免小币单日偶然零成交的噪声。

    返回 [(start, end), ...]，闭区间：start/end 均为冻结日的 open_time。
    """
    try:
        df = pd.read_parquet(path, columns=["open_time", "high", "low", "volume"])
    except Exception:
        return []
    if df.empty or "volume" not in df.columns:
        return []
    t = pd.to_datetime(df["open_time"], errors="coerce").to_numpy()
    vol = pd.to_numeric(df["volume"], errors="coerce").to_numpy()
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy()
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy()

    frozen = (vol == 0) & (high == low)
    traded = vol > 0
    if not traded.any():
        return []  # 全程零成交，整体为占位，无下架冻结
    first_traded = int(np.argmax(traded))

    n = len(frozen)
    periods = []
    i = 0
    while i < n:
        if frozen[i] and i >= first_traded:
            j = i
            while j < n and frozen[j]:
                j += 1
            start, end = t[i], t[j - 1]
            if (end - start) >= pd.Timedelta(days=min_days):
                periods.append((pd.Timestamp(start), pd.Timestamp(end)))
            i = j
        else:
            i += 1
    return periods


def _load_ohlcv_gaps(ignore_file):
    """从忽略清单提取 ohlcv（成交 K 线）kind 的 gap 作为维护期。

    忽略清单由 --gen-ignore 从 continuity 报告生成，记录已知不可在线补的 gap。
    其中 ohlcv kind 的 gap 表示成交 K 线缺失（交易所维护/宕机/无成交记录），
    回测时该时段无成交数据、不可交易。返回 [(symbol, start, end), ...]。
    """
    if not ignore_file:
        return []
    p = Path(ignore_file)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    out = []
    for entry in data.get("entries", []):
        if entry.get("kind") != "ohlcv":
            continue
        sym = entry.get("symbol")
        for g in entry.get("gaps") or []:
            out.append((sym, g.get("start"), g.get("end")))
    return out


def validate_halt(data_dir, symbols_filter, ignore_file=None, min_days=2):
    report = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "description": (
            "休市/不可交易时段清单，供回测引擎跳过。periods 里每个时段的 "
            "start/end 为闭区间（两端都不可交易），bar 的 open_time 落在 "
            "[start, end] 内即视为休市。type=frozen 为下架冻结期（连续零成交、"
            "价格冻结）；type=missing 为成交 K 线缺失（交易所维护/宕机）。"
        ),
        "data_dir": str(Path(data_dir).resolve()),
        "symbols_filter": sorted(symbols_filter) if symbols_filter else None,
        "periods": [],
        "summary": {},
    }

    frozen_periods = []
    missing_periods = []
    syms_with_frozen = set()
    syms_with_missing = set()

    # frozen：读每个 symbol 的 ohlcv 1d K 线
    for sym_dir in iter_symbol_dirs(data_dir, symbols_filter):
        sym = sym_dir.name
        path = sym_dir / f"{sym}_1d.parquet"
        if not path.exists():
            continue
        for s, e in _detect_frozen_periods(path, sym, min_days=min_days):
            frozen_periods.append({
                "symbol": sym, "type": "frozen",
                "start": _ts_str(s), "end": _ts_str(e),
                "reason": "下架冻结期（连续零成交、价格冻结）",
            })
            syms_with_frozen.add(sym)

    # missing：从忽略清单提取 ohlcv gap
    wanted = set(symbols_filter) if symbols_filter is not None else None
    for sym, gs, ge in _load_ohlcv_gaps(ignore_file):
        if wanted is not None and sym not in wanted:
            continue
        missing_periods.append({
            "symbol": sym, "type": "missing",
            "start": gs, "end": ge,
            "reason": "成交 K 线缺失（交易所维护/宕机）",
        })
        syms_with_missing.add(sym)

    report["periods"] = frozen_periods + missing_periods
    report["summary"] = {
        "frozen_period_count": len(frozen_periods),
        "missing_period_count": len(missing_periods),
        "total_period_count": len(report["periods"]),
        "symbols_with_frozen": len(syms_with_frozen),
        "symbols_with_missing": len(syms_with_missing),
        "min_days": min_days,
    }
    return report


def write_halt_report(report, output_dir):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / HALT_FILENAME
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    return path


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
VALIDATOR_FUNCS = {
    "coverage": validate_coverage,
    "schema": validate_schema,
    "continuity": validate_continuity,
    "values": validate_values,
    "outliers": validate_outliers,
    "halt": validate_halt,
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
    parser.add_argument("--ignore-file",
                        default=os.path.join(_BASE_DIR, "output", IGNORE_FILENAME),
                        help="continuity 维度读取的忽略清单路径；halt 维度据此提取 ohlcv 缺口")
    parser.add_argument("--gen-ignore", action="store_true",
                        help="从现有 continuity 报告生成忽略清单后退出")
    parser.add_argument("--outlier-window", type=int, default=121,
                        help="outliers 维度滚动 MAD 窗口（中心窗口，默认 121）")
    parser.add_argument("--outlier-threshold", type=float, default=10.0,
                        help="outliers 维度尖峰 z-score 阈值（默认 10）")
    parser.add_argument("--min-days", type=int, default=2,
                        help="halt 维度下架冻结期最小天数（默认 2）")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)

    if args.gen_ignore:
        report_path = os.path.join(args.output_dir, f"{OUTPUT_PREFIX}_continuity.json")
        if not os.path.exists(report_path):
            print(f"continuity 报告不存在：{report_path}，请先运行 --validate=continuity", file=sys.stderr)
            return 2
        ignore = generate_ignore_file(report_path, args.ignore_file)
        print(f"[gen-ignore] {ignore['entry_count']} entries -> {args.ignore_file}")
        return 0

    symbols = parse_symbols(args.symbols)
    to_run = list(VALIDATOR_FUNCS) if args.validate == "all" else [args.validate]

    paths = []
    for name in to_run:
        if name == "coverage":
            report = VALIDATOR_FUNCS[name](args.data_dir, symbols, live=not args.no_live)
        elif name == "continuity":
            report = VALIDATOR_FUNCS[name](args.data_dir, symbols, ignore_file=args.ignore_file)
        elif name == "outliers":
            report = VALIDATOR_FUNCS[name](args.data_dir, symbols,
                                           window=args.outlier_window,
                                           threshold=args.outlier_threshold)
        elif name == "halt":
            report = VALIDATOR_FUNCS[name](args.data_dir, symbols,
                                           ignore_file=args.ignore_file,
                                           min_days=args.min_days)
            path = write_halt_report(report, args.output_dir)
            paths.append(path)
            print(f"[halt] -> {path}")
            print(f"    {json.dumps(report['summary'], ensure_ascii=False, default=str)}")
            continue
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
