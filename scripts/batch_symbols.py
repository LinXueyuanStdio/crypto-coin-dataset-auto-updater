#!/usr/bin/env python3
"""Resolve batch symbols for LFS pull.

Usage:
  python3 scripts/batch_symbols.py --futures [--batch-total N] [--batch-index I]
  python3 scripts/batch_symbols.py --spot      [--batch-total N] [--batch-index I]
  python3 scripts/batch_symbols.py --symbols BTCUSDT,ETHUSDT  (COINS override)

Outputs one symbol per line to stdout.  Also writes the comma-joined list
to GITHUB_OUTPUT / GITHUB_ENV when those vars are set (CI).
"""
import argparse, json, os, sys

FALLBACK_SPOT = [
    "1INCHUSDT", "AAVEUSDT", "ADAUSDT", "ALGOUSDT", "AVAXUSDT",
    "BATUSDT", "BCHUSDT", "BNBUSDT", "BTCUSDT", "CHZUSDT",
    "COMPUSDT", "CRVUSDT", "DOGEUSDT", "DOTUSDT", "EOSUSDT",
    "ETCUSDT", "ETHUSDT", "FILUSDT", "FTMUSDT", "HBARUSDT",
    "ICPUSDT", "KSMUSDT", "LDOUSDT", "LINKUSDT", "LTCUSDT",
    "MANAUSDT", "MATICUSDT", "RUNEUSDT", "SANDUSDT", "SHIBUSDT",
    "SNXUSDT", "SOLUSDT", "SUSHIUSDT", "TRXUSDT", "UNIUSDT",
    "WAVESUSDT", "XEMUSDT", "XLMUSDT", "XRPUSDT", "YFIUSDT",
    "ZILUSDT", "ZRXUSDT",
]


def resolve_futures():
    with open(os.environ["GITHUB_WORKSPACE"] + "/symbols.json", encoding="utf-8") as f:
        return json.load(f)["symbols"]


def resolve_spot():
    try:
        import requests
        for url in [
            "https://api.binance.com/api/v3/exchangeInfo",
            "https://api-gcp.binance.com/api/v3/exchangeInfo",
        ]:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            syms = sorted(
                s["symbol"].encode("utf-8", errors="surrogatepass").decode("utf-8", errors="replace")
                for s in data.get("symbols", [])
                if s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
                and "SPOT" in (s.get("permissions") or [])
            )
            if syms:
                return syms
    except Exception:
        pass
    return list(FALLBACK_SPOT)


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--futures", action="store_true")
    group.add_argument("--spot", action="store_true")
    group.add_argument("--symbols", help="Comma-separated COINS override (skip batch logic)")
    parser.add_argument("--batch-total", type=int, default=int(os.getenv("BATCH_TOTAL", "1")))
    parser.add_argument("--batch-index", type=int, default=int(os.getenv("BATCH_INDEX", "0")))
    args = parser.parse_args()

    # COINS override
    if args.symbols:
        coins = [s.strip() for s in args.symbols.split(",") if s.strip()]
        syms = sorted(set(coins))
    elif args.futures:
        syms = resolve_futures()
    elif args.spot:
        syms = resolve_spot()
    else:
        parser.error("Must specify --futures, --spot, or --symbols")

    # Batch shard
    bt, bi = args.batch_total, args.batch_index
    if bt > 1 and not args.symbols:
        chunk = (len(syms) + bt - 1) // bt
        start = bi * chunk
        end = start + chunk if bi < bt - 1 else len(syms)
        syms = syms[start:end]

    # Output: one per line to stdout
    for s in syms:
        print(s)

    # Also export COINS to GITHUB_ENV / GITHUB_OUTPUT for later steps
    coins_str = ",".join(syms)
    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a") as f:
            f.write(f"COINS={coins_str}\n")
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"coins={coins_str}\n")


if __name__ == "__main__":
    main()
