#!/usr/bin/env bash
set -euo pipefail

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

if [[ ! -x build/shardkv_backend || ! -x "build/shardkv_gateway_${VARIANT}" ]]; then
  echo "[build] compiling ShardKV"
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
  cmake --build build -j
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${RUN_DIR:-results/local/${VARIANT}-normal-${STAMP}}"
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
  ./build/shardkv_backend "$port" "$RUN_DIR/backend${i}_metrics.csv" \
    >"$RUN_DIR/backend${i}.log" 2>&1 &
  PIDS+=("$!")
done

sleep 0.5

GATEWAY_BIN="./build/shardkv_gateway_${VARIANT}"
"$GATEWAY_BIN" 9000 \
  127.0.0.1:9101 \
  127.0.0.1:9102 \
  127.0.0.1:9103 \
  "$RUN_DIR/gateway_metrics.csv" \
  >"$RUN_DIR/gateway.log" 2>&1 &
PIDS+=("$!")

sleep 0.5

echo "[seed] preparing $KEYS keys"
python3 ./seed_data.py --host 127.0.0.1 --port 9000 --keys "$KEYS" --timeout "$TIMEOUT"

echo "[load] variant=$VARIANT clients=$CLIENTS duration=${DURATION}s"
python3 ./loadgen.py \
  --host 127.0.0.1 --port 9000 \
  --clients "$CLIENTS" --duration "$DURATION" --warmup "$WARMUP" \
  --seed 42 --timeout "$TIMEOUT" \
  --csv "$RUN_DIR/load.csv"

echo "[done] results: $RUN_DIR"
