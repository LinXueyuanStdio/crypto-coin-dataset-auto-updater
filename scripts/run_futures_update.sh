#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_futures_update.sh — wrapper that runs the Python futures updater and
# periodically pushes partial progress to Hugging Face so that data is never
# lost if the process is killed (CI timeout, OOM, etc.).
#
# Usage:
#   ./scripts/run_futures_update.sh
#
# Environment variables honoured:
#   PUSH_INTERVAL_SEC  – seconds between auto-pushes (default 1800 = 30 min)
#   DATA_DIR           – path to the cloned HF dataset repo   (default data/)
# ---------------------------------------------------------------------------
set -euo pipefail

PUSH_INTERVAL_SEC="${PUSH_INTERVAL_SEC:-600}"
DATA_DIR="${DATA_DIR:-data}"
UPDATER_SCRIPT="USDT-M_Perpetual_Futures_updater.py"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
LOG_FILE="${LOG_FILE:-${OUTPUT_DIR}/futures_update.log}"
# COINS: comma-separated list to restrict processing (for quick local tests).
#   e.g. COINS=BTCUSDT,ETHUSDT bash scripts/run_futures_update.sh
COINS="${COINS:-}"

# Force UTF-8 everywhere — avoids mojibake in the log file on Windows.
export PYTHONIOENCODING=utf-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# ---- helpers ---------------------------------------------------------------
mkdir -p "$OUTPUT_DIR"

# Tee all output (stdout+stderr) to both terminal and the log file.
exec > >(tee -a "$LOG_FILE") 2>&1

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

push_progress() {
    log ">>> Auto-pushing progress to HF …"
    # git diff only covers tracked files; status --porcelain catches new files too.
    if [ -n "$(git -C "$DATA_DIR" status --porcelain)" ]; then
        git -C "$DATA_DIR" add -A
        git -C "$DATA_DIR" commit -m "auto-save $(date -u +%Y-%m-%dT%H:%M:%SZ)" || true
        git -C "$DATA_DIR" push origin main 2>&1 || log "WARNING: push failed (will retry later)"
    else
        log "No changes to push."
    fi
}

cleanup() {
    log ">>> Caught signal – pushing final state before exit …"
    push_progress
    log ">>> Cleanup done."
    exit 0
}

trap cleanup SIGTERM SIGINT SIGHUP

# ---- main ------------------------------------------------------------------
log "=== Starting futures updater wrapper ==="
log "Push interval: ${PUSH_INTERVAL_SEC}s  |  Data dir: $DATA_DIR"

# Ensure git user is configured for auto-push commits (the workflow's squash
# step does this too, but only after the updater finishes — we need it now).
if ! git -C "$DATA_DIR" config user.email >/dev/null 2>&1; then
    git -C "$DATA_DIR" config user.email "github-actions[bot]@users.noreply.github.com"
    git -C "$DATA_DIR" config user.name "github-actions[bot]"
    log "Configured git user in $DATA_DIR"
fi

# Launch the Python updater in the background.
# COINS (and other env vars) are inherited by the Python process.
poetry run python "$UPDATER_SCRIPT" &
UPDATER_PID=$!
log "Python updater started (PID=$UPDATER_PID)"

# Periodic push loop — runs while the updater is alive.
while kill -0 "$UPDATER_PID" 2>/dev/null; do
    # Wait for the push interval, but check the updater is still alive
    # every few seconds so we don't hang after it exits.
    waited=0
    while [ "$waited" -lt "$PUSH_INTERVAL_SEC" ]; do
        sleep 10
        waited=$((waited + 10))
        if ! kill -0 "$UPDATER_PID" 2>/dev/null; then
            break
        fi
    done

    if ! kill -0 "$UPDATER_PID" 2>/dev/null; then
        break
    fi
    push_progress
done

# Updater has exited — grab its exit code.
wait "$UPDATER_PID" || UPDATER_RC=$?
log "Python updater exited (rc=${UPDATER_RC:-0})"

# Final push.
push_progress
log "=== Wrapper finished (updater rc=${UPDATER_RC:-0}) ==="
exit "${UPDATER_RC:-0}"
