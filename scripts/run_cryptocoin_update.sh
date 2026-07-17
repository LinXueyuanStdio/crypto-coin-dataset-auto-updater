#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_cryptocoin_update.sh — wrapper that runs the spot CryptoCoin updater and
# periodically pushes partial progress to Hugging Face.
#
# Periodic checkpoints: stage only changed *.parquet of this batch, commit
# locally, then git pull --rebase (fail on conflict).
# Final push (updater exited): stash, pull --rebase, stash pop, stage + commit
# + push data, then push _index.json.
#
# Usage:
#   ./scripts/run_cryptocoin_update.sh
#
# Environment variables honoured:
#   PUSH_INTERVAL_SEC  – seconds between auto-pushes (default 60)
#   DATA_DIR           – path to the cloned HF dataset repo (default data/)
#   BATCH_TOTAL        – total parallel batches (default 1)
#   BATCH_INDEX        – this run's batch number, 0-based (default 0)
#   COINS              – comma-separated symbols for this batch (default all)
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
# Tee all output (stdout+stderr) to both terminal and the log file.
exec > >(tee -a "$LOG_FILE") 2>&1

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

# ---------------------------------------------------------------------------
# stage_batch_parquets — stage *.parquet for this batch's symbols.
#   COINS is always set by the workflow (comma-separated batch symbols).
# ---------------------------------------------------------------------------
stage_batch_parquets() {
    echo "${COINS:-}" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$' \
    | while IFS= read -r sym; do
        git -C "$DATA_DIR" add "$sym"/*.parquet 2>/dev/null || true
    done
}

# ---------------------------------------------------------------------------
# push_progress — periodic checkpoint (updater still running).
#   Only stages changed *.parquet, nothing else. Commits first so the tree
#   is clean for git pull --rebase.
# ---------------------------------------------------------------------------
push_progress() {
    log ">>> Auto-pushing progress to HF …"

    if [ ! -d "$DATA_DIR/.git" ]; then
        log "data/ is not a git repo — skipping push (local dev)"
        return 0
    fi

    stage_batch_parquets

    if [ -z "$(git -C "$DATA_DIR" diff --cached --name-only)" ]; then
        log "No changes to push."
        return 0
    fi

    # Commit first so the working tree is clean for rebase.
    local commit_msg="auto-save $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if ! git -C "$DATA_DIR" commit -m "$commit_msg" 2>&1; then
        log "Nothing to commit."
        return 0
    fi
    log "Commit OK, rebasing on origin/main …"

    # Pull --rebase: if conflict, abort and fail. rebase --abort
    # takes us back to our local commit, so no data is lost.
    if ! GIT_LFS_SKIP_SMUDGE=1 git -C "$DATA_DIR" pull --rebase origin main; then
        log "ERROR: rebase conflict — aborting (local data preserved)"
        git -C "$DATA_DIR" rebase --abort 2>/dev/null || true
        return 1
    fi

    log "Pushing …"
    push_with_retry
}

# ---------------------------------------------------------------------------
# final_push — called after the updater has exited.
#   Uses stash because the updater is dead (safe to move files aside).
# ---------------------------------------------------------------------------
final_push() {
    log ">>> Final push …"

    if [ ! -d "$DATA_DIR/.git" ]; then
        log "data/ is not a git repo — skipping push (local dev)"
        return 0
    fi

    # All git ops in this function skip LFS smudge — the blobs may be
    # missing on remote (e.g. pushed by another batch, not yet on LFS store).
    export GIT_LFS_SKIP_SMUDGE=1

    # Stash any pending changes (safe — updater has exited)
    log "Stashing changes …"
    if git -C "$DATA_DIR" stash 2>&1; then
        STASHED=true
    else
        log "Nothing to stash"
        STASHED=false
    fi

    # Pull latest from remote — retry on transient HF errors
    for attempt in 1 2 3; do
        if git -C "$DATA_DIR" pull --rebase origin main 2>&1; then
            break
        fi
        if [ "$attempt" -ge 3 ]; then
            log "FATAL: git pull --rebase failed after 3 attempts — abandoning this run"
            if [ "$STASHED" = true ]; then
                git -C "$DATA_DIR" stash pop 2>/dev/null || true
            fi
            exit 1
        fi
        log "Pull attempt $attempt/3 failed, retrying in $((attempt * 10))s…"
        sleep $((attempt * 10))
    done

    # Pop stash
    if [ "$STASHED" = true ]; then
        if ! git -C "$DATA_DIR" stash pop 2>&1; then
            log "FATAL: stash pop conflict — abandoning this run"
            exit 1
        fi
    fi

    # Final push: commit everything — updater has exited, no risk of races
    git -C "$DATA_DIR" add -A

    if [ -z "$(git -C "$DATA_DIR" diff --cached --name-only)" ]; then
        log "No changes to push."
    else
        local commit_msg="auto-save $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        git -C "$DATA_DIR" commit -m "$commit_msg" 2>&1 || true
        log "Commit OK, pushing …"
        push_with_retry
    fi
}

# ---------------------------------------------------------------------------
# push_with_retry — retries on rate-limit (429) or push conflict.
#   Assumes the working tree is clean (caller has already committed).
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

        # Push conflict — another batch pushed first, rebase and retry
        if [ "$attempt" -lt "$max_attempts" ]; then
            log "Push conflict — rebasing and retrying…"
            GIT_LFS_SKIP_SMUDGE=1 git -C "$DATA_DIR" pull --rebase origin main || {
                log "Rebase failed — aborting (local data preserved)"
                git -C "$DATA_DIR" rebase --abort 2>/dev/null || true
                return 1
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
    final_push
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
    push_progress || log "Checkpoint push failed — will retry next cycle"
done

# Updater has exited — grab its exit code.
wait "$UPDATER_PID" || UPDATER_RC=$?
log "Python updater exited (rc=${UPDATER_RC:-0})"

# Final push with stash (safe — updater has exited).
final_push

log "=== Wrapper finished (updater rc=${UPDATER_RC:-0}) ==="
exit "${UPDATER_RC:-0}"
