import json
import os
import re
import sys
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
import requests
from requests.adapters import HTTPAdapter

# ----- logging (handlers attached lazily in __main__) -----
logger = logging.getLogger("cryptocoin_updater")

# ----- HTTP session + optional auto-proxy -----
PROXY = os.getenv("PROXY", "http://127.0.0.1:4780")

SESSION = requests.Session()
_adapter = HTTPAdapter(pool_connections=32, pool_maxsize=128)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


def configure_proxy():
    """Route HTTP requests through PROXY, if set."""
    if PROXY:
        os.environ["HTTP_PROXY"] = PROXY
        os.environ["HTTPS_PROXY"] = PROXY
        logger.info("Using proxy %s", PROXY)


# ----- config -----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SYMBOLS_CACHE = os.path.join(BASE_DIR, ".symbols_cache_spot.json")
SYMBOLS_FILE = os.path.join(BASE_DIR, "symbols.json")
SYMBOLS_CACHE_TTL_HOURS = 24

INTERVALS = ["1d", "12h", "8h", "6h", "4h", "2h", "1h", "30m", "15m", "5m"]

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "count",
    "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]

# Hardcoded fallback symbol list (sorted, deduplicated).
FALLBACK_SYMBOLS = sorted({
    "1INCHUSDT", "AAVEUSDT", "ADAUSDT", "ALGOUSDT", "AVAXUSDT",
    "BATUSDT", "BCHUSDT", "BNBUSDT", "BTCUSDT", "CHZUSDT",
    "COMPUSDT", "CRVUSDT", "DOGEUSDT", "DOTUSDT", "EOSUSDT",
    "ETCUSDT", "ETHUSDT", "FILUSDT", "FTMUSDT", "HBARUSDT",
    "ICPUSDT", "KSMUSDT", "LDOUSDT", "LINKUSDT", "LTCUSDT",
    "MANAUSDT", "MATICUSDT", "RUNEUSDT", "SANDUSDT", "SHIBUSDT",
    "SNXUSDT", "SOLUSDT", "SUSHIUSDT", "TRXUSDT", "UNIUSDT",
    "WAVESUSDT", "XEMUSDT", "XLMUSDT", "XRPUSDT", "YFIUSDT",
    "ZILUSDT", "ZRXUSDT",
})

SYMBOLS = list(FALLBACK_SYMBOLS)

# Binance public market-data mirror to avoid geo-restriction blocks.
BINANCE_PUBLIC_DATA_API_URL = "https://data-api.binance.vision/api"


# ============================================================================
# Binance client
# ============================================================================

def create_binance_client(max_retries=3):
    """Create Binance client pointed at the public data mirror."""
    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")
    for attempt in range(max_retries):
        start = time.monotonic()
        try:
            client = Client(api_key, api_secret, {"timeout": 30, "verify": True}, ping=False)
            client.API_URL = BINANCE_PUBLIC_DATA_API_URL
            client.ping()
            logger.info("Connected to Binance API (took %.1fs)", time.monotonic() - start)
            return client
        except Exception as e:
            logger.warning(
                "create_binance_client attempt %d/%d failed after %.1fs: %s",
                attempt + 1, max_retries, time.monotonic() - start, e,
            )
            if attempt < max_retries - 1:
                time.sleep(10)
            else:
                raise


# ============================================================================
# Data type (spot klines only — simpler than futures)
# ============================================================================

@dataclass(frozen=True)
class DataType:
    name: str
    time_col: str
    columns: tuple
    output_suffix: str  # empty for plain klines


DATA_TYPES = [
    DataType("klines", "open_time", tuple(KLINE_COLUMNS), ""),
]


def output_filename(dt, symbol, interval):
    """Return the data-file path relative to the data folder root.

    Files are stored in per-symbol subdirectories as Parquet:
        BTCUSDT/BTCUSDT_1d.parquet
    """
    if dt.output_suffix:
        name = f"{symbol}_{dt.output_suffix}_{interval}.parquet"
    else:
        name = f"{symbol}_{interval}.parquet"
    return f"{symbol}/{name}"


# ============================================================================
# Index (progress tracking)
# ============================================================================

INDEX_FILENAME = "_index.json"


def load_index(data_folder):
    path = os.path.join(data_folder, INDEX_FILENAME)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (ValueError, OSError):
        return {}


def save_index(data_folder, index):
    """Merge-safe index write — never loses entries pushed by parallel runs."""
    path = os.path.join(data_folder, INDEX_FILENAME)
    existing = load_index(data_folder)
    existing.update(index)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=0, sort_keys=True)


def index_last_dt(index, filename):
    value = index.get(filename)
    if not value:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(ts) else ts.to_pydatetime()


# ============================================================================
# File reading (parquet / csv backward compat)
# ============================================================================

def _read_file(path, columns=None):
    """Read a data file — parquet or csv, whichever exists."""
    candidates = [path]
    if path.endswith(".parquet"):
        candidates.append(path[:-len(".parquet")] + ".csv")
    elif path.endswith(".csv"):
        candidates.append(path[:-len(".csv")] + ".parquet")

    for p in candidates:
        if not os.path.exists(p):
            continue
        try:
            if p.endswith(".parquet"):
                return pd.read_parquet(p, columns=columns) if columns else pd.read_parquet(p)
            return pd.read_csv(p, dtype=str, **(dict(usecols=columns) if columns else {}))
        except Exception:
            continue
    return None


def latest_stored_time(path, time_col):
    df = _read_file(path, columns=[time_col])
    if df is None or df.empty:
        return None
    ts = pd.to_datetime(df[time_col], errors="coerce").max()
    return None if pd.isna(ts) else ts.to_pydatetime()


# ============================================================================
# Data fetching
# ============================================================================

def fetch_binance_data(symbol, interval, start_date, end_date, max_retries=3):
    """Fetch historical klines from Binance with retry logic."""
    client = create_binance_client()
    for attempt in range(max_retries):
        t0 = time.monotonic()
        try:
            klines = client.get_historical_klines(symbol, interval, start_date, end_date, limit=1000)
            df = pd.DataFrame(klines, columns=list(KLINE_COLUMNS))
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            logger.info("Fetched %s %s: %d rows (%.1fs)", symbol, interval, len(df), time.monotonic() - t0)
            return df
        except Exception as e:
            logger.warning(
                "fetch_binance_data(%s, %s) attempt %d/%d failed after %.1fs: %s",
                symbol, interval, attempt + 1, max_retries, time.monotonic() - t0, e,
            )
            if attempt < max_retries - 1:
                time.sleep(20)
            else:
                raise Exception(
                    f"Failed to fetch {symbol} {interval} after {max_retries} attempts"
                )


# ============================================================================
# Merge
# ============================================================================

def merge_frames(existing_df, new_df, time_col):
    TS_COLS = {"open_time", "close_time"}
    new_df = new_df.copy()
    for col in TS_COLS & set(new_df.columns):
        new_df[col] = pd.to_datetime(new_df[col], errors="coerce")
    if existing_df is not None and len(existing_df):
        existing_df = existing_df.copy()
        for col in TS_COLS & set(existing_df.columns):
            existing_df[col] = pd.to_datetime(existing_df[col], errors="coerce")
        merged = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        merged = new_df
    merged = merged.dropna(subset=[time_col])
    merged = merged.drop_duplicates(subset=time_col).sort_values(time_col)
    return merged.reset_index(drop=True)


# ============================================================================
# Symbol resolution
# ============================================================================

def _parse_binance_symbols(raw_symbols):
    """Extract TRADING spot USDT symbol names from raw exchange info."""
    return sorted(
        s["symbol"].encode("utf-8", errors="surrogatepass").decode("utf-8", errors="replace")
        for s in raw_symbols
        if (s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and "SPOT" in (s.get("permissions") or []))
    )


def fetch_spot_symbols():
    """Fetch all TRADING spot USDT symbols from Binance API."""
    if os.path.exists(SYMBOLS_CACHE):
        try:
            with open(SYMBOLS_CACHE, encoding="utf-8") as f:
                cached = json.load(f)
            age = time.time() - cached.get("_fetched_at", 0)
            if age < SYMBOLS_CACHE_TTL_HOURS * 3600:
                symbols = cached.get("symbols", [])
                if symbols:
                    logger.info("Using cached spot symbol list (%d symbols, age=%.1fh)", len(symbols), age / 3600)
                    return symbols
        except (ValueError, OSError, KeyError, TypeError):
            pass

    urls = [
        "https://api.binance.com/api/v3/exchangeInfo",
        "https://api-gcp.binance.com/api/v3/exchangeInfo",
        "https://data-api.binance.vision/api/v3/exchangeInfo",
    ]
    for url in urls:
        try:
            resp = SESSION.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            symbols = _parse_binance_symbols(data.get("symbols", []))
            if symbols:
                logger.info("Fetched %d spot symbols from %s", len(symbols), url)
                try:
                    with open(SYMBOLS_CACHE, "w", encoding="utf-8") as f:
                        json.dump({"_fetched_at": time.time(), "symbols": symbols}, f, ensure_ascii=False)
                except OSError:
                    pass
                return symbols
        except Exception as e:
            logger.warning("Failed to fetch spot symbols from %s: %s", url, e)

    # Fallback: symbols.json
    if os.path.exists(SYMBOLS_FILE):
        try:
            with open(SYMBOLS_FILE, encoding="utf-8") as f:
                data = json.load(f)
            symbols = sorted(data.get("symbols", []))
            if symbols:
                logger.info("Using symbols.json fallback (%d symbols)", len(symbols))
                return symbols
        except (ValueError, OSError, KeyError, TypeError):
            pass

    logger.warning("Could not fetch spot symbols; falling back to %d hardcoded symbols", len(FALLBACK_SYMBOLS))
    return list(FALLBACK_SYMBOLS)


def resolve_symbols(force_refresh=False):
    global SYMBOLS
    if force_refresh and os.path.exists(SYMBOLS_CACHE):
        try:
            os.remove(SYMBOLS_CACHE)
        except OSError:
            pass
    SYMBOLS = fetch_spot_symbols()
    logger.info("resolve_symbols: %d symbols loaded", len(SYMBOLS))


# ============================================================================
# Job building
# ============================================================================

@dataclass
class Job:
    dt: DataType
    symbol: str
    interval: str


def build_jobs():
    jobs = []
    for symbol in SYMBOLS:
        for dt in DATA_TYPES:
            for interval in INTERVALS:
                jobs.append(Job(dt, symbol, interval))
    return jobs


# ============================================================================
# Bootstrap index from existing files
# ============================================================================

def build_index_from_files(data_folder):
    index = {}
    for job in build_jobs():
        filename = output_filename(job.dt, job.symbol, job.interval)
        path = os.path.join(data_folder, filename)
        if _read_file(path) is not None:
            last = latest_stored_time(path, job.dt.time_col)
            if last is not None:
                index[filename] = last.strftime("%Y-%m-%d %H:%M:%S")
                continue
        # Fallback: legacy flat layout
        flat_name = filename.split("/")[-1]
        flat_path = os.path.join(data_folder, flat_name)
        if _read_file(flat_path) is not None:
            last = latest_stored_time(flat_path, job.dt.time_col)
            if last is not None:
                index[filename] = last.strftime("%Y-%m-%d %H:%M:%S")
    return index


# ============================================================================
# Update logic
# ============================================================================

def needs_update(last_dt, end_date, data_path=None, time_col=None):
    """Determine whether a series needs fetching."""
    if last_dt is not None and last_dt.date() >= end_date:
        if data_path and time_col:
            actual_last = latest_stored_time(data_path, time_col)
            if actual_last is not None and actual_last.date() >= end_date:
                return False
            return True
        return False
    return True


def process_job(dt, symbol, interval, data_folder, end_date, last_dt):
    out_name = output_filename(dt, symbol, interval)
    data_path = os.path.join(data_folder, out_name)
    label = f"{symbol}/{interval}"

    # Re-read index from disk — another batch may have done this job
    fresh_index = load_index(data_folder)
    fresh_last = index_last_dt(fresh_index, out_name)
    if not needs_update(fresh_last, end_date, data_path, dt.time_col):
        logger.info("[%s] already up-to-date (another batch did it) — skip", label)
        return None

    t0 = time.monotonic()
    start_str = (last_dt or datetime(2017, 1, 1)).strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    logger.info("[%s] fetching %s → %s ...", label, start_str, end_str)
    try:
        new_df = fetch_binance_data(symbol, interval, start_str, end_str)
    except Exception:
        elapsed = time.monotonic() - t0
        logger.error("[%s] FAILED (%.1fs)", label, elapsed)
        raise

    if new_df is None or new_df.empty:
        elapsed = time.monotonic() - t0
        logger.info("[%s] no new data (%.1fs)", label, elapsed)
        return None

    new_rows = len(new_df)
    existing_df = _read_file(data_path)
    existing_rows = len(existing_df) if existing_df is not None else 0
    merged = merge_frames(existing_df, new_df, dt.time_col)
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    merged.to_parquet(data_path, index=False)
    new_last = pd.to_datetime(merged[dt.time_col], errors="coerce").max()
    elapsed = time.monotonic() - t0
    ts = new_last.strftime("%Y-%m-%d") if new_last is not pd.NaT else "?"
    delta = len(merged) - existing_rows
    logger.info(
        "[%s] done — +%d rows (fetched %d, was %d, now %d through %s) (%.1fs)",
        label, delta, new_rows, existing_rows, len(merged), ts, elapsed,
    )
    return data_path, (None if pd.isna(new_last) else new_last.to_pydatetime())


class Budget:
    def __init__(self, minutes):
        self.limit_seconds = float(minutes) * 60.0
        self.start = time.monotonic()

    def exceeded(self):
        return (time.monotonic() - self.start) >= self.limit_seconds


def run_update(data_folder, end_date=None, budget=None, max_workers=None,
               batch_total=None, batch_index=None):
    if end_date is None:
        end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    if budget is None:
        budget = Budget(float(os.getenv("MAX_RUNTIME_MIN", "120")))
    if max_workers is None:
        max_workers = int(os.getenv("FETCH_WORKERS", "16"))
    if batch_total is None:
        batch_total = int(os.getenv("BATCH_TOTAL", "1"))
    if batch_index is None:
        batch_index = int(os.getenv("BATCH_INDEX", "0"))

    index = load_index(data_folder)
    if not index:
        index = build_index_from_files(data_folder)
        if index:
            logger.info("Bootstrapped index from %d existing files", len(index))

    all_jobs = build_jobs()

    # ---- batch sharding by symbol ----
    if batch_total > 1:
        all_symbols = sorted({job.symbol for job in all_jobs})
        chunk = (len(all_symbols) + batch_total - 1) // batch_total
        start = batch_index * chunk
        end = start + chunk if batch_index < batch_total - 1 else len(all_symbols)
        my_symbols = set(all_symbols[start:end])
        assigned = sum(1 for job in all_jobs if job.symbol in my_symbols)
        logger.info(
            "Batch %d/%d: %d symbols (%d jobs) of %d total",
            batch_index + 1, batch_total, len(my_symbols), assigned, len(all_jobs),
        )
    else:
        my_symbols = None

    # Reload index from disk — the wrapper may have pulled other batches'
    # _index.json since startup, making our in-memory copy stale.
    fresh = load_index(data_folder)
    if fresh:
        fresh.update(index)  # our entries win (newer data from our own fetch)
        index = fresh

    pending = []
    for job in all_jobs:
        if my_symbols is not None and job.symbol not in my_symbols:
            continue
        filename = output_filename(job.dt, job.symbol, job.interval)
        data_path = os.path.join(data_folder, filename)
        last_dt_val = index_last_dt(index, filename)
        if needs_update(last_dt_val, end_date, data_path, job.dt.time_col):
            pending.append((job, filename, last_dt_val))

    total = sum(1 for j in all_jobs if my_symbols is None or j.symbol in my_symbols)
    logger.info("%d/%d series need update (end_date=%s)", len(pending), total, end_date)

    jobs_iter = iter(pending)
    produced = 0
    failed = 0
    inflight = {}
    total_pending = len(pending)
    last_log_at = time.monotonic()
    PROGRESS_LOG_EVERY_S = 60
    CHECKPOINT_EVERY_N = 20
    checkpoint_pending = 0
    t_start = time.monotonic()

    symbol_updated = {}
    symbol_failed = {}

    def submit_next(ex):
        try:
            job, filename, last_dt_val = next(jobs_iter)
        except StopIteration:
            return False
        fut = ex.submit(process_job, job.dt, job.symbol, job.interval, data_folder, end_date, last_dt_val)
        inflight[fut] = (filename, job.symbol)
        return True

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for _ in range(max_workers):
            if budget.exceeded() or not submit_next(ex):
                break
        while inflight:
            done, _ = wait(set(inflight), return_when=FIRST_COMPLETED)
            for f in done:
                filename, symbol = inflight.pop(f)
                try:
                    result = f.result()
                    if result is not None:
                        _, new_last = result
                        if new_last is not None:
                            index[filename] = new_last.strftime("%Y-%m-%d %H:%M:%S")
                        produced += 1
                        symbol_updated.setdefault(symbol, []).append(filename)
                except Exception as e:
                    failed += 1
                    symbol_failed.setdefault(symbol, []).append((filename, str(e)))
                    logger.warning("series failed: %s", e)

                checkpoint_pending += 1

            if checkpoint_pending >= CHECKPOINT_EVERY_N:
                # Reload index from disk (other batches' updates) but do NOT
                # save — writing _index.json blocks git pull --rebase
                fresh = load_index(data_folder)
                fresh.update(index)
                index = fresh
                checkpoint_pending = 0

            now = time.monotonic()
            if now - last_log_at >= PROGRESS_LOG_EVERY_S:
                done_so_far = produced + failed
                pct = done_so_far * 100.0 / total_pending if total_pending else 0
                elapsed = now - t_start
                rate = done_so_far / max(elapsed, 1)
                logger.info(
                    "progress: %d/%d (%.1f%%) ok=%d fail=%d elapsed=%.0fs — %.1f series/s",
                    done_so_far, total_pending, pct, produced, failed, elapsed, rate,
                )
                last_log_at = now
            if not budget.exceeded():
                for _ in range(len(done)):
                    if not submit_next(ex):
                        break

    save_index(data_folder, index)
    elapsed = time.monotonic() - t_start
    logger.info("run_update produced data for %d series; index has %d entries (%.0fs)", produced, len(index), elapsed)

    # Write run summary
    summary = {
        "start_time": datetime.fromtimestamp(t_start).strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_seconds": round(elapsed, 1),
        "budget_limit_min": budget.limit_seconds / 60.0,
        "batch_index": batch_index,
        "batch_total": batch_total,
        "total_pending": total_pending,
        "produced": produced,
        "failed": failed,
        "remaining": total_pending - produced - failed,
        "symbols_updated": len(symbol_updated),
        "symbols_failed": len(symbol_failed),
        "per_symbol": {
            sym: {
                "updated": len(symbol_updated.get(sym, [])),
                "failed": len(symbol_failed.get(sym, [])),
                "series": sorted(symbol_updated.get(sym, [])),
                "errors": [e[:200] for _, e in symbol_failed.get(sym, [])],
            }
            for sym in sorted(set(list(symbol_updated) + list(symbol_failed)))
        },
    }
    summary_dir = os.path.join(BASE_DIR, "output")
    os.makedirs(summary_dir, exist_ok=True)
    summary_path = os.path.join(summary_dir, "run_summary_cryptocoin.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    logger.info("Run summary saved to %s", summary_path)

    return produced


def _build_symbol_list(index_path):
    """Build a collapsible markdown symbol list from _index.json keys."""
    if not os.path.exists(index_path):
        return "_No _index.json found — run the updater first._"
    try:
        with open(index_path, encoding="utf-8") as f:
            idx = json.load(f)
    except (ValueError, OSError):
        return "_Cannot read _index.json._"
    # Keys are like "BTCUSDT/BTCUSDT_1d.parquet" → extract symbol name
    symbols = sorted({k.split("/")[0] for k in idx})
    if not symbols:
        return "_No symbols in _index.json._"
    # Format as a Python list, ~5 symbols per line
    quoted = [f'"{s}"' for s in symbols]
    rows = []
    for i in range(0, len(quoted), 5):
        rows.append("    " + ", ".join(quoted[i:i + 5]) + ("," if i + 5 < len(quoted) else ""))
    body = "available_pairs = [\n" + "\n".join(rows) + "\n]"
    return f"""<details>
<summary>Click to expand — {len(symbols)} symbols / 点击展开</summary>

```python
{body}
```

</details>"""


def refresh_readme(path):
    """Always regenerate README.md from .github/README_TEMPLATE_spot.md.

    The template is the source of truth — local README.md is overwritten
    so that template edits propagate on every master-batch run.
    """
    template_path = os.path.join(BASE_DIR, ".github", "README_TEMPLATE_spot.md")
    if not os.path.exists(template_path):
        logger.warning("README_TEMPLATE_spot.md not found — skipping README refresh")
        return
    with open(template_path, encoding="utf-8") as f:
        body = f.read()
    # Replace placeholders
    n = len(SYMBOLS)
    if os.path.exists(SYMBOLS_FILE):
        try:
            with open(SYMBOLS_FILE, encoding="utf-8") as f:
                n = len(json.load(f).get("symbols", []))
        except (ValueError, OSError, KeyError):
            pass
    body = body.replace("{n_symbols}", str(n))
    index_path = os.path.join(os.path.dirname(path), "_index.json")
    body = body.replace("{symbol_list}", _build_symbol_list(index_path))
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = body.replace("Last updated on `pending`", f"Last updated on `{now}`")
    if "Last updated on `" not in body:
        body += f"\n\nLast updated on `{now}`\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def stamp_readme(path):
    """Update only the timestamp in an existing README.md (lightweight)."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if not os.path.exists(path):
        refresh_readme(path)
        return
    with open(path, "r", encoding="utf-8") as f:
        body = f.read()
    if "Last updated on `" in body:
        body = re.sub(r"Last updated on `.*?`", f"Last updated on `{now}`", body)
    else:
        body += f"\n\nLast updated on `{now}`\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def upload(data_folder, dataset_slug, version_notes):
    """Upload the dataset to Hugging Face via HfApi."""
    from huggingface_hub import HfApi
    api = HfApi(token=os.getenv("HF_TOKEN"))
    api.upload_folder(
        folder_path=data_folder,
        repo_id=dataset_slug,
        repo_type="dataset",
        commit_message=version_notes,
        commit_description=version_notes,
        create_pr=False,
    )
    logger.info("Dataset uploaded to %s", dataset_slug)


# ============================================================================
# Main
# ============================================================================

def main():
    load_dotenv()
    configure_proxy()
    dataset_slug = os.getenv("DATASET_SLUG", "linxy/CryptoCoin")
    data_folder = os.path.join(BASE_DIR, os.getenv("DATA_DIR", "data"))
    os.makedirs(data_folder, exist_ok=True)

    batch_total = int(os.getenv("BATCH_TOTAL", "1"))
    batch_index = int(os.getenv("BATCH_INDEX", "0"))
    is_master = (batch_index == 0)

    logger.info("Starting CryptoCoin spot update -> %s", dataset_slug)
    logger.info("Batch %d/%d (master=%s)", batch_index + 1, batch_total, is_master)
    resolve_symbols()

    # COINS env var restricts to a symbol subset
    coins_filter = os.getenv("COINS", "").strip()
    if coins_filter:
        wanted = {c.strip() for c in coins_filter.split(",") if c.strip()}
        before = len(SYMBOLS)
        SYMBOLS[:] = [s for s in SYMBOLS if s in wanted]
        logger.info("COINS filter: %d → %d", before, len(SYMBOLS))

    run_update(data_folder)

    # ---- master batch responsibilities ----
    if is_master:
        readme_path = os.path.join(data_folder, "README.md")
        refresh_readme(readme_path)

        if not os.getenv("HF_TOKEN"):
            logger.warning("HF_TOKEN not set — skipping upload (local dev run)")
        elif os.getenv("COINS", "").strip():
            logger.warning("COINS is set — skipping upload (local test run)")
        else:
            notes = datetime.now(timezone.utc).strftime("Updated at %B %d %Y %H:%M:%S UTC")
            for attempt in range(1, 11):
                try:
                    upload(data_folder, dataset_slug, notes)
                    break
                except Exception as e:
                    logger.error("Upload attempt %d/10 failed: %s. Retrying in 60s...", attempt, e)
                    time.sleep(60)
            else:
                raise RuntimeError("Upload failed after 10 attempts")
    else:
        logger.info(
            "Non-master batch — skipping README update and HF upload "
            "(data will be checkpointed by wrapper's auto-push)"
        )


if __name__ == "__main__":
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)

    max_attempts = 10
    attempt = 0
    while attempt < max_attempts:
        try:
            main()
            logger.info("Updater finished successfully.")
            break
        except Exception as e:
            attempt += 1
            import traceback
            traceback.print_exc()
            logger.error("Global attempt %d/%d failed: %s. Retrying in 60s...", attempt, max_attempts, e)
            time.sleep(60)
    else:
        logger.error("Max attempts reached. Exiting.")
        sys.exit(1)
