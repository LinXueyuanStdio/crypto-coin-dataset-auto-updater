#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_cryptocoin_update.sh — wrapper that runs the spot CryptoCoin updater and
# periodically pushes partial progress to Hugging Face.
#
# Designed for safe parallel execution: each auto-push pulls latest from
# remote first (merge-safe), and failed pushes retry with stash+rebase.
#
# Usage:
#   ./scripts/run_cryptocoin_update.sh
#
# Environment variables honoured:
#   PUSH_INTERVAL_SEC  – seconds between auto-pushes (default 60)
#   DATA_DIR           – path to the cloned HF dataset repo (default data/)
#   BATCH_TOTAL        – total parallel batches (default 1)
#   BATCH_INDEX        – this run's batch number, 0-based (default 0)
# ---------------------------------------------------------------------------
set -euo pipefail

PUSH_INTERVAL_SEC="${PUSH_INTERVAL_SEC:-60}"
DATA_DIR="${DATA_DIR:-data}"
UPDATER_SCRIPT="updater.py"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
LOG_FILE="${LOG_FILE:-${OUTPUT_DIR}/cryptocoin_update.log}"
COINS="${COINS:-}"

export PYTHONIOENCODING=utf-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# ---- helpers ---------------------------------------------------------------
mkdir -p "$OUTPUT_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

# ---------------------------------------------------------------------------
# merge_safe_pull — rebase local commits on top of origin/main.
#   Called after local commit in push_progress, so working tree is clean.
# ---------------------------------------------------------------------------
merge_safe_pull() {
    log "Rebasing on origin/main …"
    GIT_LFS_SKIP_SMUDGE=1 git -C "$DATA_DIR" pull --rebase origin main
}

# ---------------------------------------------------------------------------
# lfs_ensure
# ---------------------------------------------------------------------------
lfs_ensure() {
    git -C "$DATA_DIR" lfs install >/dev/null 2>&1 || true
    # Track large parquet files
    git -C "$DATA_DIR" lfs track '*_5m.parquet' '*_15m.parquet' '*_30m.parquet' >/dev/null 2>&1 || true
    # Catch remaining ≥9 MiB parquet files not covered above
    large=$(find "$DATA_DIR" -name '*.parquet' -size +9M \
        ! -name '*_5m.parquet' \
        ! -name '*_15m.parquet' \
        ! -name '*_30m.parquet' \
        -printf '%P\n' 2>/dev/null || true)
    if [ -n "$large" ]; then
        echo "$large" | while IFS= read -r f; do
            git -C "$DATA_DIR" lfs track "$f" >/dev/null 2>&1 || true
        done
        log "LFS-tracked $(echo "$large" | wc -l) extra file(s) ≥9 MiB"
    fi
}

# ---------------------------------------------------------------------------
# push_progress
# ---------------------------------------------------------------------------
push_progress() {
    log ">>> Auto-pushing progress to HF …"

    if [ ! -d "$DATA_DIR/.git" ]; then
        log "data/ is not a git repo — skipping push (local dev)"
        return 0
    fi

    lfs_ensure
    git -C "$DATA_DIR" add -A
    # Exclude _index.json — committing it mid-run would overwrite other batches' entries
    git -C "$DATA_DIR" reset -- _index.json 2>/dev/null || true

    if [ -z "$(git -C "$DATA_DIR" status --porcelain)" ]; then
        log "No changes to push."
        return 0
    fi

    merge_safe_pull

    # Re-stage (LFS tracking may have changed .gitattributes)
    git -C "$DATA_DIR" add -A 2>/dev/null || true
    git -C "$DATA_DIR" reset -- _index.json 2>/dev/null || true

    local commit_msg="auto-save $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if ! git -C "$DATA_DIR" commit -m "$commit_msg" 2>&1; then
        log "Nothing to commit."
        return 0
    fi
    log "Commit OK, pushing …"

    push_with_retry
}

# ---------------------------------------------------------------------------
# push_with_retry — retries on rate-limit (429) or race condition.
#   Uses git pull --rebase to recover from races.
# ---------------------------------------------------------------------------
push_with_retry() {
    local max_attempts=5
    local attempt=0

    while [ "$attempt" -lt "$max_attempts" ]; do
        attempt=$((attempt + 1))

        if push_out=$(git -C "$DATA_DIR" push origin main 2>&1); then
            log "Push OK"
            return 0
        fi

        log "Push failed (attempt $attempt/$max_attempts): $(echo "$push_out" | head -1)"

        if echo "$push_out" | grep -q "429\|rate.limit\|Too Many Requests"; then
            retry_sec=$(echo "$push_out" | grep -oP 'Retry after \K\d+' | head -1)
            wait="${retry_sec:-$((attempt * 60))}"
            log "Rate limited — waiting ${wait}s…"
            sleep "$wait"
            continue
        fi

        if [ "$attempt" -lt "$max_attempts" ]; then
            log "Push conflict — rebasing and retrying…"
            GIT_LFS_SKIP_SMUDGE=1 git -C "$DATA_DIR" pull --rebase origin main || {
                log "Rebase failed — aborting and resetting to origin/main"
                git -C "$DATA_DIR" rebase --abort 2>/dev/null || true
                git -C "$DATA_DIR" reset --hard origin/main
                lfs_ensure
                git -C "$DATA_DIR" add -A
                if [ -z "$(git -C "$DATA_DIR" status --porcelain)" ]; then
                    log "No changes after reset"
                    return 0
                fi
                git -C "$DATA_DIR" commit -m "auto-save $(date -u +%Y-%m-%dT%H:%M:%SZ) [retry $attempt]"
            }
            sleep $((attempt * 5))
        fi
    done

    log "ERROR: Push failed after $max_attempts attempts"
    return 1
}

# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------
cleanup() {
    log ">>> Caught signal – pushing final state before exit …"
    push_progress
    log ">>> Cleanup done."
    exit ${UPDATER_RC:-1}
}

trap cleanup SIGTERM SIGINT SIGHUP

# ---- main ------------------------------------------------------------------
log "=== Starting CryptoCoin updater wrapper ==="
log "Push interval: ${PUSH_INTERVAL_SEC}s  |  Data dir: $DATA_DIR"
log "Batch: $((BATCH_INDEX+1))/${BATCH_TOTAL:-1} (index=${BATCH_INDEX:-0})  |  Workers: ${FETCH_WORKERS:-16}"

# Ensure git user is configured for auto-push commits.
if ! git -C "$DATA_DIR" config user.email >/dev/null 2>&1; then
    git -C "$DATA_DIR" config user.email "github-actions[bot]@users.noreply.github.com"
    git -C "$DATA_DIR" config user.name "github-actions[bot]"
    log "Configured git user in $DATA_DIR"
fi

# Launch the Python updater in the background.
poetry run python "$UPDATER_SCRIPT" &
UPDATER_PID=$!
log "Python updater started (PID=$UPDATER_PID)"

# Periodic push loop
while kill -0 "$UPDATER_PID" 2>/dev/null; do
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

wait "$UPDATER_PID" || UPDATER_RC=$?
log "Python updater exited (rc=${UPDATER_RC:-0})"

# Final push of data files (excludes _index.json)
push_progress

# Push _index.json separately — save_index is merge-safe
if [ -f "$DATA_DIR/_index.json" ]; then
    log "Pushing _index.json …"
    git -C "$DATA_DIR" add _index.json
    git -C "$DATA_DIR" commit -m "update _index.json" 2>/dev/null || true
    push_with_retry || log "WARNING: _index.json push failed (non-fatal)"
fi

log "=== Wrapper finished (updater rc=${UPDATER_RC:-0}) ==="
exit "${UPDATER_RC:-0}"
