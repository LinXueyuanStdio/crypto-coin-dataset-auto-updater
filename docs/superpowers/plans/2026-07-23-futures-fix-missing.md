# Futures Fix Missing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `futures_fix_missing.py` to validate and interactively repair Binance USDT-M perpetual futures kline, funding, and metrics parquet files.

**Architecture:** A single CLI script owns scanning, task planning, API fetch, preview, approval, and parquet writes. Tests import the script as a module and inject fake fetchers/prompts so no network is required.

**Tech Stack:** Python 3.10, pandas, requests, python-dotenv, pyarrow parquet, pytest.

---

### Task 1: Configuration

**Files:**
- Modify: `.env`

- [x] Write `.env` with `BINANCE_API_KEY`, `BINANCE_SECRET_KEY`, and `BINANCE_PROXY`.

### Task 2: Tests

**Files:**
- Create: `tests/test_futures_fix_missing.py`

- [ ] Add tests for funding grid normalization.
- [ ] Add tests for metrics missing coverage detection.
- [ ] Add tests for approved repair writing a backup and parquet.
- [ ] Add tests for declined repair leaving parquet unchanged.
- [ ] Run the focused tests and verify they fail because the script does not exist yet.

### Task 3: Implementation

**Files:**
- Create: `futures_fix_missing.py`

- [ ] Implement `.env` loading and proxy configuration.
- [ ] Implement parquet read/write helpers.
- [ ] Implement OHLCV, markPrice, indexPrice, and premiumIndex validation and API fetch.
- [ ] Implement funding validation and API fetch.
- [ ] Implement metrics validation and API fetch.
- [ ] Implement interactive approval and safe backup writes.
- [ ] Implement CLI arguments: `--data-dir`, `--symbols`, `--apply`, `--non-interactive`, `--context-rows`, and `--max-tasks`.

### Task 4: Verification

**Files:**
- Test: `tests/test_futures_fix_missing.py`

- [ ] Run focused tests until passing.
- [ ] Run existing futures tests to catch regressions.
- [ ] Run a dry-run smoke check against one local symbol.
