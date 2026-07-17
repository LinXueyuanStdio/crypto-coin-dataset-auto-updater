#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_futures_update.sh — wrapper that runs the Python futures updater and
# periodically pushes partial progress to Hugging Face so that data is never
# lost if the process is killed (CI timeout, OOM, etc.).
#
# Designed for safe parallel execution: each auto-push pulls latest from
# remote first (merge-safe), and failed pushes retry with stash+rebase.
#
# Usage:
#   ./scripts/run_futures_update.sh
#
# Environment variables honoured:
#   PUSH_INTERVAL_SEC  – seconds between auto-pushes (default 60)
#   DATA_DIR           – path to the cloned HF dataset repo   (default data/)
#   BATCH_TOTAL        – total parallel batches (default 1)
#   BATCH_INDEX        – this run's batch number, 0-based (default 0)
# ---------------------------------------------------------------------------
set -euo pipefail

PUSH_INTERVAL_SEC="${PUSH_INTERVAL_SEC:-60}"
DATA_DIR="${DATA_DIR:-data}"
UPDATER_SCRIPT="USDT-M_Perpetual_Futures_updater.py"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
LOG_FILE="${LOG_FILE:-${OUTPUT_DIR}/futures_update.log}"
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

# ---------------------------------------------------------------------------
# merge_safe_pull — rebase local commits on top of origin/main.
#   Called after local commit in push_progress, so working tree is clean.
# ---------------------------------------------------------------------------
merge_safe_pull() {
    log "Rebasing on origin/main …"
    # _index.json is local-only — discard changes so rebase can proceed
    git -C "$DATA_DIR" checkout -- _index.json 2>/dev/null || true
    GIT_LFS_SKIP_SMUDGE=1 git -C "$DATA_DIR" pull --rebase origin main
}

# ---------------------------------------------------------------------------
# lfs_ensure — make sure all large CSV files are tracked by git-lfs before
#   committing, so the HF repo doesn't accumulate giant blobs.
# ---------------------------------------------------------------------------
lfs_ensure() {
    git -C "$DATA_DIR" lfs install >/dev/null 2>&1 || true

    # Preemptively LFS-track patterns that always grow large.
    git -C "$DATA_DIR" lfs track '*_5m.parquet' '*_15m.parquet' '*_30m.parquet' '*_metrics.parquet' >/dev/null 2>&1 || true

    # Catch any remaining Parquet files ≥ 9 MiB that didn't match the
    # wildcard patterns above. Exclude files already covered.
    large=$(find "$DATA_DIR" -name '*.parquet' -size +9M \
        ! -name '*_5m.parquet' \
        ! -name '*_15m.parquet' \
        ! -name '*_30m.parquet' \
        ! -name '*_metrics.parquet' \
        -printf '%P\n' 2>/dev/null || true)
    if [ -n "$large" ]; then
        echo "$large" | while IFS= read -r f; do
            git -C "$DATA_DIR" lfs track "$f" >/dev/null 2>&1 || true
        done
        log "LFS-tracked $(echo "$large" | wc -l) extra file(s) ≥9 MiB"
    fi
}

# ---------------------------------------------------------------------------
# push_progress — the core checkpoint routine.
#   1. LFS setup
#   2. Stage everything
#   3. Pull latest (merge-safe — handles parallel runs)
#   4. Commit + push
#   5. On push conflict (another run pushed first):
#      undo commit → stash → pull → pop → recommit → push (up to 3 retries)
# ---------------------------------------------------------------------------
push_progress() {
    log ">>> Auto-pushing progress to HF …"

    if [ ! -d "$DATA_DIR/.git" ]; then
        log "data/ is not a git repo — skipping push (local dev)"
        return 0
    fi

    lfs_ensure
    git -C "$DATA_DIR" add -A
    # Exclude _index.json — each batch updates it independently via save_index,
    # committing it mid-run would overwrite other batches' entries.
    git -C "$DATA_DIR" reset -- _index.json 2>/dev/null || true

    if [ -z "$(git -C "$DATA_DIR" status --porcelain)" ]; then
        log "No changes to push."
        return 0
    fi

    # Rebase on remote (tree is clean — nothing to stash)
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
#   Uses git pull --rebase to recover from races (another parallel run pushed
#   between our pull and push). No stash — the tree is clean at this point.
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

        # Rate limit — extract Retry-After from HF's 429 response
        if echo "$push_out" | grep -q "429\|rate.limit\|Too Many Requests"; then
            retry_sec=$(echo "$push_out" | grep -oP 'Retry after \K\d+' | head -1)
            wait="${retry_sec:-$((attempt * 60))}"
            log "Rate limited — waiting ${wait}s…"
            sleep "$wait"
            continue
        fi

        # Push conflict — rebase and retry
        if [ "$attempt" -lt "$max_attempts" ]; then
            log "Push conflict — rebasing and retrying…"
            git -C "$DATA_DIR" checkout -- _index.json 2>/dev/null || true
            GIT_LFS_SKIP_SMUDGE=1 git -C "$DATA_DIR" pull --rebase origin main || {
                log "Rebase failed — aborting and resetting to origin/main"
                git -C "$DATA_DIR" rebase --abort 2>/dev/null || true
                git -C "$DATA_DIR" reset --hard origin/main
                lfs_ensure
                git -C "$DATA_DIR" add -A
                git -C "$DATA_DIR" reset -- _index.json 2>/dev/null || true
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
# cleanup — signal handler: push final state before exit
# ---------------------------------------------------------------------------
cleanup() {
    log ">>> Caught signal – pushing final state before exit …"
    push_progress
    log ">>> Cleanup done."
    exit ${UPDATER_RC:-1}
}

trap cleanup SIGTERM SIGINT SIGHUP

# ---- main ------------------------------------------------------------------
log "=== Starting futures updater wrapper ==="
log "Push interval: ${PUSH_INTERVAL_SEC}s  |  Data dir: $DATA_DIR"
log "Batch: $((BATCH_INDEX+1))/${BATCH_TOTAL:-1} (index=${BATCH_INDEX:-0})  |  Workers: ${FETCH_WORKERS:-64}"

# Ensure git user is configured for auto-push commits.
if ! git -C "$DATA_DIR" config user.email >/dev/null 2>&1; then
    git -C "$DATA_DIR" config user.email "github-actions[bot]@users.noreply.github.com"
    git -C "$DATA_DIR" config user.name "github-actions[bot]"
    log "Configured git user in $DATA_DIR"
fi

# Launch the Python updater in the background.
# BATCH_TOTAL, BATCH_INDEX, and other env vars are inherited by the Python process.
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

# Final push of data files (excludes _index.json).
push_progress

# Push _index.json separately — save_index is merge-safe (reads existing,
# merges, writes back), so the on-disk file has all batches' entries.
# This must be the LAST push to avoid overwriting other batches' entries.
if [ -f "$DATA_DIR/_index.json" ]; then
    log "Pushing _index.json …"
    git -C "$DATA_DIR" add _index.json
    git -C "$DATA_DIR" commit -m "update _index.json" 2>/dev/null || true
    push_with_retry || log "WARNING: _index.json push failed (non-fatal)"
fi

log "=== Wrapper finished (updater rc=${UPDATER_RC:-0}) ==="
exit "${UPDATER_RC:-0}"
