"""Fetch fresh symbol list from Binance and write to symbols.json."""
import json, time, urllib.request

urls = [
    "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "https://api.binance.com/fapi/v1/exchangeInfo",
]

for url in urls:
    try:
        resp = urllib.request.urlopen(url, timeout=15)
        data = json.loads(resp.read().decode())
        symbols = sorted(
            s["symbol"] for s in data.get("symbols", [])
            if (s.get("quoteAsset") == "USDT"
                and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING")
        )
        if symbols:
            with open("symbols.json", "w", encoding="utf-8") as f:
                json.dump(
                    {"_fetched_at": time.time(), "symbols": symbols},
                    f, ensure_ascii=False, indent=2,
                )
            print(f"Wrote {len(symbols)} symbols to symbols.json (from {url})")
            break
    except Exception as e:
        print(f"Failed: {url}: {e}")
else:
    print("Could not fetch symbols")
