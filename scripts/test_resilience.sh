#!/usr/bin/env bash
# test_resilience.sh — prove crash recovery works
# Usage: bash scripts/test_resilience.sh
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
COMPOSE="docker compose"

echo "=== Resilience Test ==="

# 1. Ensure stack is up
echo "[1/6] Checking stack health..."
curl -sf "${API_URL}/health" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d['status'] in ('healthy','degraded') else 1)"
echo "  Stack is up."

# 2. Submit 20 prompts in the background
echo "[2/6] Submitting 20 prompts..."
for i in $(seq 1 20); do
  curl -sf -X POST "${API_URL}/process" \
    -H "Content-Type: application/json" \
    -d "{\"user_id\":\"resilience_user\",\"prompt_id\":\"resilience_${i}\",\"text\":\"Resilience test prompt number ${i}\"}" \
    > /dev/null &
done
echo "  Prompts submitted."

# 3. Kill one worker mid-processing
sleep 2
echo "[3/6] Killing one worker container..."
WORKER=$($COMPOSE ps -q worker | head -1)
if [ -n "$WORKER" ]; then
  docker kill "$WORKER" 2>/dev/null && echo "  Killed worker: $WORKER"
else
  echo "  No worker container found via compose — skipping kill step."
fi

# 4. Wait for reaper + remaining workers to finish
echo "[4/6] Waiting 30s for recovery..."
sleep 30

# 5. Check all prompts reached a terminal state
echo "[5/6] Verifying all prompts are in terminal state..."
FAILED=0
for i in $(seq 1 20); do
  STATUS=$(curl -sf "${API_URL}/result/resilience_${i}" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "error")
  if [[ "$STATUS" != "completed" && "$STATUS" != "failed" ]]; then
    echo "  WARN: resilience_${i} is still '${STATUS}'"
    FAILED=$((FAILED+1))
  fi
done

# 6. Report
if [ "$FAILED" -eq 0 ]; then
  echo "[6/6] PASS — all 20 prompts reached a terminal state."
  exit 0
else
  echo "[6/6] FAIL — ${FAILED} prompt(s) did not reach terminal state."
  exit 1
fi
