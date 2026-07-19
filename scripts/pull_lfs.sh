#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# pull_lfs.sh — pull LFS blobs for a list of symbols with progress logging.
#
# Usage:
#   echo "$SYMS" | bash scripts/pull_lfs.sh
#   COINS=BTCUSDT,ETHUSDT bash scripts/pull_lfs.sh
#
# Reads symbols from stdin (one per line) or $COINS (comma-separated).
# Writes counts to /tmp/lfs_pull_ok.txt and /tmp/lfs_pull_fail.txt.
# ---------------------------------------------------------------------------
set -euo pipefail

BATCH_COUNT="${BATCH_COUNT:-0}"

rm -f /tmp/lfs_pull_ok.txt /tmp/lfs_pull_fail.txt
touch /tmp/lfs_pull_ok.txt /tmp/lfs_pull_fail.txt

if [ -n "${COINS:-}" ] && [ "${1:-}" != "--stdin" ]; then
    SYMS=$(echo "$COINS" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$')
else
    SYMS=$(cat)
fi

BATCH_COUNT=$(echo "$SYMS" | grep -c . 2>/dev/null || echo 0)

if [ "$BATCH_COUNT" -eq 0 ]; then
    echo "No symbols to pull — skipping"
    exit 0
fi

echo "::group::LFS pull (${BATCH_COUNT} symbols)"

i=0
echo "$SYMS" | while IFS= read -r sym; do
    [ -z "$sym" ] && continue
    i=$((i + 1))
    printf '  [%d/%d] %s ... ' "$i" "$BATCH_COUNT" "$sym"
    if git lfs pull --include="${sym}/**" 2>&1; then
        echo "ok" >> /tmp/lfs_pull_ok.txt
    else
        rc=$?
        echo "  ⚠️  ${sym} (exit ${rc})"
        echo "fail" >> /tmp/lfs_pull_fail.txt
    fi
done

OK=$(wc -l < /tmp/lfs_pull_ok.txt 2>/dev/null || echo 0)
FAIL=$(wc -l < /tmp/lfs_pull_fail.txt 2>/dev/null || echo 0)

echo "::endgroup::"
echo "LFS pull: ${OK}/${BATCH_COUNT} ok, ${FAIL} failed"

if [ "$FAIL" -gt 0 ]; then
    echo "⚠️  ${FAIL} symbol(s) failed — updater will fetch fresh data for them"
fi
