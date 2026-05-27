#!/usr/bin/env bash
# load_test.sh — hammer /process and print summary stats
# Usage: bash scripts/load_test.sh [N_REQUESTS] [CONCURRENCY]
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
N=${1:-100}
CONCUR=${2:-20}

echo "=== Load Test ==="
echo "  Requests   : $N"
echo "  Concurrency: $CONCUR"
echo "  Target     : ${API_URL}/process"
echo ""

START=$(date +%s%3N)
SUCCESS=0; FAIL=0
PIDS=()

run_request() {
  local i=$1
  local resp
  resp=$(curl -sf -w "\n%{http_code}" -X POST "${API_URL}/process" \
    -H "Content-Type: application/json" \
    -d "{\"user_id\":\"load_user\",\"prompt_id\":\"load_${i}\",\"text\":\"Load test prompt ${i}: explain concept number ${i}\"}" \
    --max-time 35 2>/dev/null || echo "CURL_ERROR")
  local code
  code=$(echo "$resp" | tail -1)
  if [[ "$code" == "200" ]]; then
    echo "ok"
  else
    echo "fail:$code"
  fi
}

export -f run_request
export API_URL

RESULTS=$(seq 1 "$N" | xargs -P "$CONCUR" -I{} bash -c 'run_request "$@"' _ {})

END=$(date +%s%3N)
ELAPSED=$(( (END - START) ))

SUCCESS=$(echo "$RESULTS" | grep -c "^ok$" || true)
FAIL=$(echo "$RESULTS" | grep -c "^fail" || true)

echo ""
echo "=== Results ==="
echo "  Total requests : $N"
echo "  Successful     : $SUCCESS"
echo "  Failed         : $FAIL"
echo "  Total time     : ${ELAPSED}ms"
echo "  Throughput     : $(echo "scale=1; $N * 1000 / $ELAPSED" | bc) req/s"
echo ""
echo "Cache + rate-limit stats:"
curl -sf "${API_URL}/metrics" | grep -E "prompt_(cache|requests)|llm_calls|rate_limit" || echo "  (metrics endpoint not available)"
