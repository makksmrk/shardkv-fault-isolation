#!/usr/bin/env bash
set -euo pipefail

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

if [[ ! "$DURATION" =~ ^[0-9]+$ || ! "$BROWNOUT_AT" =~ ^[0-9]+$ || ! "$BROWNOUT_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "DURATION, BROWNOUT_AT, and BROWNOUT_SECONDS must be integer seconds." >&2
  exit 1
fi

if (( BROWNOUT_AT + BROWNOUT_SECONDS >= DURATION )); then
  echo "Brownout must end before DURATION so the backend can recover during the run." >&2
  exit 1
fi

if [[ ! -x build/shardkv_backend || ! -x "build/shardkv_gateway_${VARIANT}" ]]; then
  echo "[build] compiling ShardKV"
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
  cmake --build build -j
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${RUN_DIR:-results/local/${VARIANT}-brownout-${STAMP}}"
mkdir -p "$RUN_DIR"

PIDS=()
BACKEND_PIDS=()
INJECTOR_PID=""
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

for i in 0 1 2; do
  port=$((9101 + i))
  ./build/shardkv_backend "$port" "$RUN_DIR/backend${i}_metrics.csv" \
    >"$RUN_DIR/backend${i}.log" 2>&1 &
  pid="$!"
  BACKEND_PIDS+=("$pid")
  PIDS+=("$pid")
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

(
  sleep "$BROWNOUT_AT"
  echo "SIGSTOP backend 2 at t=${BROWNOUT_AT}s" | tee -a "$RUN_DIR/brownout-events.txt"
  kill -STOP "${BACKEND_PIDS[2]}"
  sleep "$BROWNOUT_SECONDS"
  resume_at=$((BROWNOUT_AT + BROWNOUT_SECONDS))
  echo "SIGCONT backend 2 at t=${resume_at}s" | tee -a "$RUN_DIR/brownout-events.txt"
  kill -CONT "${BACKEND_PIDS[2]}"
) &
INJECTOR_PID="$!"

echo "[load] variant=$VARIANT clients=$CLIENTS duration=${DURATION}s brownout=${BROWNOUT_AT}s+${BROWNOUT_SECONDS}s"
python3 ./loadgen.py \
  --host 127.0.0.1 --port 9000 \
  --clients "$CLIENTS" --duration "$DURATION" --warmup "$WARMUP" \
  --seed 42 --timeout "$TIMEOUT" \
  --csv "$RUN_DIR/load.csv"

wait "$INJECTOR_PID"
INJECTOR_PID=""
echo "[done] results: $RUN_DIR"
