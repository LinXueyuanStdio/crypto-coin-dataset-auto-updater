#!/usr/bin/env python3
"""Fetch listing dates for all symbols and update meta.json.

Tries data.binance.vision CDN (HEAD requests on monthly kline files) to find
the earliest available month for each symbol. Falls back to downloading
a small parquet via HuggingFace API to extract the earliest timestamp.

Usage:
  poetry run python scripts/update_meta_listing_date.py --push
  poetry run python scripts/update_meta_listing_date.py --hf-fallback --push
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from huggingface_hub import HfApi, CommitOperationAdd

# ---------- config ----------
CDN_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
SESSION = requests.Session()
SESSION.mount("https://", HTTPAdapter(pool_connections=16, pool_maxsize=32))
SESSION.mount("http://", HTTPAdapter(pool_connections=16, pool_maxsize=32))

# Months to probe (from earliest possible to recent)
PROBE_MONTHS = [f"{y}-{m:02d}" for y in range(2019, 2027) for m in range(1, 13)]


def cdn_file_exists(symbol, month):
    """Check if a monthly kline zip exists on CDN."""
    url = f"{CDN_BASE}/{symbol}/5m/{symbol}-5m-{month}.zip"
    try:
        r = SESSION.head(url, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


def cdn_earliest_month(symbol):
    """Binary search for the earliest month with data on CDN."""
    lo, hi = 0, len(PROBE_MONTHS) - 1
    earliest = None
    while lo <= hi:
        mid = (lo + hi) // 2
        month = PROBE_MONTHS[mid]
        if cdn_file_exists(symbol, month):
            earliest = month
            hi = mid - 1  # look earlier
        else:
            lo = mid + 1  # look later
    return earliest


def hf_earliest_date(symbol, repo_id, token):
    """Download the smallest parquet for *symbol* and extract earliest timestamp."""
    api = HfApi(token=token)
    try:
        files = api.list_repo_files(repo_id, repo_type="dataset")
    except Exception:
        return None

    # Pick smallest file: fundingRate first, then metrics, then 1d klines
    candidates = []
    for f in files:
        if f.startswith(f"{symbol}/") and f.endswith(".parquet"):
            candidates.append(f)
    if not candidates:
        return None

    # Prefer smallest files
    preferred = [f for f in candidates if "fundingRate" in f] or \
                [f for f in candidates if "metrics" in f] or \
                [f for f in candidates if "_1d.parquet" in f] or \
                candidates[:1]
    path = preferred[0]

    try:
        df = pd.read_parquet(f"hf://datasets/{repo_id}/{path}")
    except Exception:
        return None

    # Find time column
    for col in ["calc_time", "create_time", "open_time"]:
        if col in df.columns:
            ts = pd.to_datetime(df[col], errors="coerce").min()
            if pd.notna(ts):
                return ts.strftime("%Y-%m")
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default="linxy/USDT-M_Perpetual_Futures")
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--hf-fallback", action="store_true",
                        help="Use HF API to download parquets (slower, works without CDN access)")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--symbols", help="Comma-separated list (default: all from symbols.json)")
    args = parser.parse_args()

    hf_token = os.environ.get("HF_TOKEN")
    if args.push and not hf_token:
        print("ERROR: HF_TOKEN not set — cannot push")
        sys.exit(1)

    # Read symbols
    if args.symbols:
        symbols = sorted(s.strip() for s in args.symbols.split(",") if s.strip())
    else:
        symbols_file = os.path.join(os.path.dirname(__file__), "..", "symbols.json")
        if not os.path.exists(symbols_file):
            print(f"ERROR: {symbols_file} not found")
            sys.exit(1)
        with open(symbols_file, encoding="utf-8") as f:
            symbols = sorted(json.load(f).get("symbols", []))
    print(f"Target: {len(symbols)} symbols")

    # Read existing meta.json
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    meta_path = os.path.join(data_dir, "meta.json")
    existing = {}
    if os.path.exists(meta_path):
        with open(meta_path, encoding="utf-8") as f:
            existing = json.load(f)
        print(f"Existing meta.json: {len(existing)} entries")

    # Fetch listing dates
    results = {}
    done = 0
    t0 = time.monotonic()

    def fetch_one(sym):
        month = cdn_earliest_month(sym)
        if month:
            return sym, month
        if args.hf_fallback and hf_token:
            month = hf_earliest_date(sym, args.repo, hf_token)
            if month:
                return sym, month
        return sym, None

    print(f"Fetching listing dates ({args.workers} workers, hf_fallback={args.hf_fallback})...")
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(fetch_one, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym, month = fut.result()
            done += 1
            if month:
                results[sym] = month
            if done % 20 == 0 or (done <= 5 and month):
                status = f"→ {month}" if month else "→ NOT FOUND"
                print(f"  [{done}/{len(symbols)}] {sym} {status}")

    elapsed = time.monotonic() - t0
    print(f"Found: {len(results)}/{len(symbols)} in {elapsed:.0f}s")

    # Merge
    for sym in symbols:
        if sym not in existing:
            existing[sym] = {}
        if sym in results:
            existing[sym]["listing_date"] = results[sym]

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, sort_keys=True, ensure_ascii=False)
    print(f"Written: {meta_path} ({len(existing)} entries, {len(results)} with listing_date)")

    # Push
    if args.push:
        api = HfApi(token=hf_token)
        api.create_commit(
            repo_id=args.repo,
            repo_type="dataset",
            commit_message="feat: add listing_date to meta.json",
            operations=[CommitOperationAdd(path_in_repo="meta.json", path_or_fileobj=meta_path)],
        )
        print(f"Pushed meta.json to {args.repo}")


if __name__ == "__main__":
    main()
