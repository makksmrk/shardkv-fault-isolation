#!/usr/bin/env bash

# Run a local ShardKV brownout experiment with configurable load and timing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"

VARIANT="${1:-isolated}"
DURATION="${DURATION:-35}"
WARMUP="${WARMUP:-5}"
CLIENTS="${CLIENTS:-64}"
KEYS="${KEYS:-100000}"
TIMEOUT="${TIMEOUT:-5}"
BROWNOUT_AT="${BROWNOUT_AT:-12}"
BROWNOUT_SECONDS="${BROWNOUT_SECONDS:-8}"

if [[ "$VARIANT" != "baseline" && "$VARIANT" != "isolated" ]]; then
  echo "Usage: $0 [baseline|isolated]" >&2
  exit 1
fi

if [[ ! "$DURATION" =~ ^[0-9]+$ ||
      ! "$BROWNOUT_AT" =~ ^[0-9]+$ ||
      ! "$BROWNOUT_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "DURATION, BROWNOUT_AT, and BROWNOUT_SECONDS must be integer seconds." >&2
  exit 1
fi

if (( BROWNOUT_AT + BROWNOUT_SECONDS >= DURATION )); then
  echo "Brownout must end before DURATION so the backend can recover during the run." >&2
  exit 1
fi

BACKEND_BIN="${BUILD_DIR}/shardkv_backend"
GATEWAY_BIN="${BUILD_DIR}/shardkv_gateway_${VARIANT}"

# build binaries if they are not available
if [[ ! -x "$BACKEND_BIN" || ! -x "$GATEWAY_BIN" ]]; then
  echo "[build] compiling ShardKV"

  cmake \
    -S "$PROJECT_ROOT" \
    -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release

  cmake --build "$BUILD_DIR" -j
fi

STAMP="$(date +%Y%m%d-%H%M%S)"

if [[ -n "${RUN_DIR:-}" ]]; then
  RUN_DIR="${RUN_DIR}"
else
  RUN_DIR="${PROJECT_ROOT}/results/local/${VARIANT}-brownout-${STAMP}"
fi

mkdir -p "$RUN_DIR"

PIDS=()
BACKEND_PIDS=()
INJECTOR_PID=""

# resume stopped processes and terminate all experiment processes on exit
cleanup() {
  if [[ ${#BACKEND_PIDS[@]} -ge 3 ]]; then
    kill -CONT "${BACKEND_PIDS[2]}" >/dev/null 2>&1 || true
  fi

  if [[ -n "$INJECTOR_PID" ]]; then
    kill "$INJECTOR_PID" >/dev/null 2>&1 || true
  fi

  for pid in "${PIDS[@]:-}"; do
    kill -CONT "$pid" >/dev/null 2>&1 || true
    kill "$pid" >/dev/null 2>&1 || true
  done

  for pid in "${PIDS[@]:-}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}

trap cleanup EXIT INT TERM

# start three local backend processes
for i in 0 1 2; do
  port=$((9101 + i))

  "$BACKEND_BIN" \
    "$port" \
    "$RUN_DIR/backend${i}_metrics.csv" \
    >"$RUN_DIR/backend${i}.log" 2>&1 &

  pid="$!"

  BACKEND_PIDS+=("$pid")
  PIDS+=("$pid")
done

sleep 0.5

# start the selected gateway variant
"$GATEWAY_BIN" \
  9000 \
  127.0.0.1:9101 \
  127.0.0.1:9102 \
  127.0.0.1:9103 \
  "$RUN_DIR/gateway_metrics.csv" \
  >"$RUN_DIR/gateway.log" 2>&1 &

PIDS+=("$!")

sleep 0.5

# seed the key-value store before starting the load test
echo "[seed] preparing $KEYS keys"

python3 "${PROJECT_ROOT}/tools/seed_data.py" \
  --host 127.0.0.1 \
  --port 9000 \
  --keys "$KEYS" \
  --timeout "$TIMEOUT"

# stop backend 2 temporarily to inject the brownout
(
  sleep "$BROWNOUT_AT"

  echo "SIGSTOP backend 2 at t=${BROWNOUT_AT}s" \
    | tee -a "$RUN_DIR/brownout-events.txt"

  kill -STOP "${BACKEND_PIDS[2]}"

  sleep "$BROWNOUT_SECONDS"

  resume_at=$((BROWNOUT_AT + BROWNOUT_SECONDS))

  echo "SIGCONT backend 2 at t=${resume_at}s" \
    | tee -a "$RUN_DIR/brownout-events.txt"

  kill -CONT "${BACKEND_PIDS[2]}"
) &

INJECTOR_PID="$!"

# run the load generator while the brownout is injected
echo "[load] variant=$VARIANT clients=$CLIENTS duration=${DURATION}s brownout=${BROWNOUT_AT}s+${BROWNOUT_SECONDS}s"

python3 "${PROJECT_ROOT}/tools/loadgen.py" \
  --host 127.0.0.1 \
  --port 9000 \
  --clients "$CLIENTS" \
  --duration "$DURATION" \
  --warmup "$WARMUP" \
  --seed 42 \
  --timeout "$TIMEOUT" \
  --csv "$RUN_DIR/load.csv"

wait "$INJECTOR_PID"
INJECTOR_PID=""

echo "[done] results: $RUN_DIR"