#!/usr/bin/env bash
# Runs the k6 load test against a running stack (docker compose up) and writes the results to
# docs/benchmark.md and the benchmark table in README.md.
#
#   ./scripts/benchmark.sh                 # 300 jobs/s for 60 s
#   RATE=500 DURATION=120s ./scripts/benchmark.sh
set -euo pipefail

cd "$(dirname "$0")/.."

BASE_URL=${BASE_URL:-http://localhost:8080}
RATE=${RATE:-300}
DURATION=${DURATION:-60s}
JOB_MS=${JOB_MS:-5}
RESULTS=loadtest/results
mkdir -p "$RESULTS"

echo "Waiting for the API at $BASE_URL ..."
for _ in $(seq 1 120); do
  if curl -fsS "$BASE_URL/actuator/health" 2>/dev/null | grep -q '"UP"'; then break; fi
  sleep 2
done
curl -fsS "$BASE_URL/actuator/health" | grep -q '"UP"' || { echo "API is not healthy"; exit 1; }

echo "Waiting for workers to register ..."
for _ in $(seq 1 60); do
  if [ "$(curl -fsS "$BASE_URL/api/workers" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')" -ge 1 ]; then break; fi
  sleep 2
done

echo "Warm-up (JIT, connection pools, consumer group assignment) ..."
k6 run -q -e BASE_URL="$BASE_URL" -e RATE=50 -e DURATION=15s loadtest/dispatch.js > /dev/null || true
sleep 5

START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1
echo "Load test: $RATE jobs/s for $DURATION (start $START)"
set +e
k6 run -e BASE_URL="$BASE_URL" -e RATE="$RATE" -e DURATION="$DURATION" -e JOB_MS="$JOB_MS" \
  --summary-export "$RESULTS/k6-summary.json" loadtest/dispatch.js
K6_EXIT=$?
set -e

echo "Waiting for the pipeline to drain ..."
for _ in $(seq 1 180); do
  IN_FLIGHT=$(curl -fsS "$BASE_URL/api/jobs/stats?since=$START" | python3 -c 'import json,sys; print(json.load(sys.stdin)["inFlight"])')
  [ "$IN_FLIGHT" = "0" ] && break
  sleep 1
done
curl -fsS "$BASE_URL/api/jobs/stats?since=$START" > "$RESULTS/pipeline-stats.json"
curl -fsS "$BASE_URL/api/workers" > "$RESULTS/workers.json"

python3 scripts/render_benchmark.py \
  --k6 "$RESULTS/k6-summary.json" \
  --pipeline "$RESULTS/pipeline-stats.json" \
  --workers "$RESULTS/workers.json" \
  --rate "$RATE" --duration "$DURATION" --job-ms "$JOB_MS" --k6-exit "$K6_EXIT"

echo "k6 exit code: $K6_EXIT (99 = a threshold was crossed)"
