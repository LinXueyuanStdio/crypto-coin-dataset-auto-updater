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
#   PUSH_INTERVAL_SEC  – seconds between auto-pushes (default 1800 = 30 min)
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
# merge_safe_pull — pull latest from remote, handling conflicts gracefully.
#   On success (no local changes to lose): pulls with rebase.
#   On rebase conflict: stashes local changes, resets to origin/main,
#   then pops the stash back — local uncommitted data is never lost.
# ---------------------------------------------------------------------------
merge_safe_pull() {
    if [ -z "$(git -C "$DATA_DIR" status --porcelain)" ]; then
        # No local changes — safe to just pull
        log "Pulling latest from origin (clean working tree)…"
        git -C "$DATA_DIR" fetch origin main --quiet
        git -C "$DATA_DIR" reset --hard origin/main
        return 0
    fi

    # Dirty tree (unstaged .gitattributes from LFS, new CSV files, etc.).
    # pull --rebase would fail — go straight to stash + reset + pop.
    log "Pulling latest from origin (dirty tree — stash + reset + pop)…"
    git -C "$DATA_DIR" stash --include-untracked 2>/dev/null || true
    git -C "$DATA_DIR" fetch origin main --quiet
    git -C "$DATA_DIR" reset --hard origin/main
    if git -C "$DATA_DIR" stash pop 2>/dev/null; then
        log "Stash popped cleanly"
    else
        log "Stash pop had conflicts (files preserved — updater will reconcile)"
        git -C "$DATA_DIR" checkout --theirs . 2>/dev/null || true
        git -C "$DATA_DIR" reset HEAD . 2>/dev/null || true
    fi
}

# ---------------------------------------------------------------------------
# lfs_ensure — make sure all large CSV files are tracked by git-lfs before
#   committing, so the HF repo doesn't accumulate giant blobs.
# ---------------------------------------------------------------------------
lfs_ensure() {
    git -C "$DATA_DIR" lfs install >/dev/null 2>&1 || true

    # Preemptively LFS-track patterns that always grow large.
    # Redirect stdout too — "already supported" prints to stdout, not stderr.
    git -C "$DATA_DIR" lfs track '*_5m.csv' '*_15m.csv' '*_30m.csv' '*_metrics.csv' >/dev/null 2>&1 || true

    # Catch any remaining CSV ≥ 9 MiB that didn't match the wildcard patterns
    # above. Exclude files already covered by *_5m.csv, *_15m.csv, *_30m.csv,
    # and *_metrics.csv to avoid redundant per-file "git lfs track" calls
    # (which would just print "already supported").
    large=$(find "$DATA_DIR" -name '*.csv' -size +9M \
        ! -name '*_5m.csv' \
        ! -name '*_15m.csv' \
        ! -name '*_30m.csv' \
        ! -name '*_metrics.csv' \
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

    # 1. LFS tracking
    lfs_ensure

    # 2. Stage everything
    git -C "$DATA_DIR" add -A

    if [ -z "$(git -C "$DATA_DIR" status --porcelain)" ]; then
        log "No changes to push."
        return 0
    fi

    # 3. Sync with remote (stash → fetch → reset → pop — handles dirty tree)
    merge_safe_pull

    # 4. Commit
    local commit_msg="auto-save $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if ! git -C "$DATA_DIR" commit -m "$commit_msg" 2>&1; then
        log "Nothing to commit."
        return 0
    fi
    log "Commit OK, pushing …"

    # 5. Push with retry on race condition
    push_with_retry
}

# ---------------------------------------------------------------------------
# push_with_retry — handles the race where another parallel run pushed after
#   our pull but before our push.
# ---------------------------------------------------------------------------
push_with_retry() {
    local max_attempts=3
    local attempt=0

    while [ "$attempt" -lt "$max_attempts" ]; do
        attempt=$((attempt + 1))

        if push_out=$(git -C "$DATA_DIR" push origin main 2>&1); then
            log "Push OK"
            return 0
        fi

        log "Push failed (attempt $attempt/$max_attempts): $push_out"

        if [ "$attempt" -lt "$max_attempts" ]; then
            log "Someone else pushed — undoing commit, re-syncing, and retrying…"

            # a. Undo our commit but keep changes in working tree
            git -C "$DATA_DIR" reset --soft HEAD~1

            # b. Stash changes temporarily
            git -C "$DATA_DIR" stash --include-untracked

            # c. Sync to latest remote state
            git -C "$DATA_DIR" fetch origin main --quiet
            git -C "$DATA_DIR" reset --hard origin/main

            # d. Pop our changes back
            if git -C "$DATA_DIR" stash pop 2>/dev/null; then
                log "Stash popped cleanly"
            else
                log "Stash pop had conflicts — keeping our version"
                git -C "$DATA_DIR" checkout --theirs . 2>/dev/null || true
                git -C "$DATA_DIR" reset HEAD . 2>/dev/null || true
            fi

            # e. Re-stage and re-commit
            lfs_ensure
            git -C "$DATA_DIR" add -A
            if [ -z "$(git -C "$DATA_DIR" status --porcelain)" ]; then
                log "No changes after re-sync (merged into remote already)"
                return 0
            fi
            git -C "$DATA_DIR" commit -m "auto-save $(date -u +%Y-%m-%dT%H:%M:%SZ) [retry $attempt]"
        fi
    done

    log "ERROR: Push failed after $max_attempts attempts — changes remain in working tree"
    return 1
}

# ---------------------------------------------------------------------------
# cleanup — signal handler: push final state before exit
# ---------------------------------------------------------------------------
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
log "Batch: ${BATCH_INDEX:-0}/${BATCH_TOTAL:-1}  |  Workers: ${FETCH_WORKERS:-64}"

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

# Final push.
push_progress
log "=== Wrapper finished (updater rc=${UPDATER_RC:-0}) ==="
exit "${UPDATER_RC:-0}"
