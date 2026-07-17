"""Download Binance symbols via updater's _parse_binance_symbols, save as JSON."""
import importlib.util, json, time, urllib.request, os, sys

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("updater", os.path.join(base, "USDT-M_Perpetual_Futures_updater.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
_parse_binance_symbols = mod._parse_binance_symbols

urls = ["https://fapi.binance.com/fapi/v1/exchangeInfo",
        "https://api.binance.com/fapi/v1/exchangeInfo"]

for url in urls:
    try:
        resp = urllib.request.urlopen(url, timeout=15)
        raw = resp.read().decode("utf-8")
        data = json.loads(raw)
        symbols = _parse_binance_symbols(data.get("symbols", []))
        print(f"{len(symbols)} symbols from {url}")

        bad = [s for s in symbols if not s.isascii()]
        for s in bad:
            try:
                s.encode("utf-8")
                print(f"  Non-ASCII OK: {s}")
            except Exception as e:
                print(f"  Non-ASCII BAD: {repr(s)} — {e}")

        out = os.path.join(os.path.dirname(__file__), "test_symbols.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"_fetched_at": time.time(), "symbols": symbols},
                      f, ensure_ascii=False, indent=2)
        print(f"Saved → {out}")
        break
    except Exception as e:
        print(f"Failed {url}: {e}")
