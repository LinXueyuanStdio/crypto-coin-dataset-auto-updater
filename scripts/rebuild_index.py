#!/usr/bin/env python3
"""Rebuild _index.json from existing parquet files (reads only time columns).

Usage:
  python3 scripts/rebuild_index.py           # spot (open_time)
  python3 scripts/rebuild_index.py --futures  # futures (TIME_COLS mapping)

Scoped to BATCH_SYMS (newline-separated) when set in environment;
full scan otherwise.
"""
import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Per-dataset time-column resolution
# ---------------------------------------------------------------------------
TIME_COLS_FUTURES = {
    "klines": "open_time",
    "markPrice": "open_time",
    "indexPrice": "open_time",
    "premiumIndex": "open_time",
    "metrics": "create_time",
    "fundingRate": "calc_time",
}


def _time_col(filename: str, futures: bool) -> str:
    """Return the correct time column for *filename*."""
    if not futures:
        return "open_time"
    base = Path(filename).stem
    for key, col in TIME_COLS_FUTURES.items():
        if key in base:
            return col
    return "open_time"


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def _iter_parquets(dirs: list[str] | None):
    """Yield (relative_path, absolute_or_cwd_path) for every .parquet file."""
    if dirs:
        # Scoped — only walk the given symbol directories
        for sym in dirs:
            if not os.path.isdir(sym):
                continue
            for root, _dirs, files in os.walk(sym):
                for f in files:
                    if f.endswith(".parquet"):
                        yield os.path.join(root, f).lstrip("./")
    else:
        # Full scan
        for root, dirs_, files in os.walk("."):
            if ".git" in root.split(os.sep):
                continue
            for f in files:
                if f.endswith(".parquet"):
                    yield os.path.join(root, f).lstrip("./")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Rebuild _index.json from parquet files")
    parser.add_argument(
        "--futures", action="store_true",
        help="Use futures TIME_COLS mapping (default: spot, open_time only)",
    )
    args = parser.parse_args()

    # Determine scope
    batch_syms = os.getenv("BATCH_SYMS", "").strip()
    if batch_syms:
        target_dirs = [d for d in batch_syms.split("\n") if d]
        if len(target_dirs) <= 5:
            print(f"Scoping rebuild to {len(target_dirs)} symbol(s): {target_dirs}")
        else:
            print(f"Scoping rebuild to {len(target_dirs)} symbol(s): {target_dirs[:5]}…")
    else:
        target_dirs = None
        print("Full rebuild (no BATCH_SYMS)")

    idx: dict[str, str] = {}
    total = errors = 0
    t0 = time.monotonic()
    last_report = t0

    for fp in _iter_parquets(target_dirs):
        total += 1
        time_col = _time_col(fp, args.futures)
        try:
            df = pd.read_parquet(fp, columns=[time_col])
        except Exception:
            errors += 1
            # Print the first few errors for visibility
            if errors <= 5:
                print(f"  ⚠ skip {fp} (read error)")
            continue
        if time_col not in df.columns:
            continue
        ts = pd.to_datetime(df[time_col], errors="coerce").max()
        if pd.notna(ts):
            idx[fp] = ts.strftime("%Y-%m-%d %H:%M:%S")

        # Progress heartbeat every 15 s or every 500 files
        now = time.monotonic()
        if now - last_report >= 15 or total % 500 == 0:
            print(f"  … {total} files scanned, {len(idx)} indexed, {errors} errors")
            last_report = now

    with open("_index.json", "w", encoding="utf-8") as out:
        json.dump(idx, out, indent=0, sort_keys=True, ensure_ascii=False)

    elapsed = time.monotonic() - t0
    if errors > 5:
        print(f"  ({errors - 5} more errors silenced)")
    print(f"Index rebuilt: {len(idx)} entries ({total} files, {errors} errors) in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
