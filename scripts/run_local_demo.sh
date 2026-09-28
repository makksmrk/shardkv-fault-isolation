#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"

VARIANT="${1:-isolated}"
DURATION="${DURATION:-15}"
WARMUP="${WARMUP:-2}"
CLIENTS="${CLIENTS:-32}"
KEYS="${KEYS:-100000}"
TIMEOUT="${TIMEOUT:-5}"

if [[ "$VARIANT" != "baseline" && "$VARIANT" != "isolated" ]]; then
  echo "Usage: $0 [baseline|isolated]" >&2
  exit 1
fi

BACKEND_BIN="${BUILD_DIR}/shardkv_backend"
GATEWAY_BIN="${BUILD_DIR}/shardkv_gateway_${VARIANT}"

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
  RUN_DIR="${PROJECT_ROOT}/results/local/${VARIANT}-normal-${STAMP}"
fi

mkdir -p "$RUN_DIR"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill -CONT "$pid" >/dev/null 2>&1 || true
    kill "$pid" >/dev/null 2>&1 || true
  done

  for pid in "${PIDS[@]:-}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}

trap cleanup EXIT INT TERM

for i in 0 1 2; do
  port=$((9101 + i))

  "$BACKEND_BIN" \
    "$port" \
    "$RUN_DIR/backend${i}_metrics.csv" \
    >"$RUN_DIR/backend${i}.log" 2>&1 &

  PIDS+=("$!")
done

sleep 0.5

"$GATEWAY_BIN" \
  9000 \
  127.0.0.1:9101 \
  127.0.0.1:9102 \
  127.0.0.1:9103 \
  "$RUN_DIR/gateway_metrics.csv" \
  >"$RUN_DIR/gateway.log" 2>&1 &

PIDS+=("$!")

sleep 0.5

echo "[seed] preparing $KEYS keys"

python3 "${PROJECT_ROOT}/tools/seed_data.py" \
  --host 127.0.0.1 \
  --port 9000 \
  --keys "$KEYS" \
  --timeout "$TIMEOUT"

echo "[load] variant=$VARIANT clients=$CLIENTS duration=${DURATION}s"

python3 "${PROJECT_ROOT}/tools/loadgen.py" \
  --host 127.0.0.1 \
  --port 9000 \
  --clients "$CLIENTS" \
  --duration "$DURATION" \
  --warmup "$WARMUP" \
  --seed 42 \
  --timeout "$TIMEOUT" \
  --csv "$RUN_DIR/load.csv"

echo "[done] results: $RUN_DIR"