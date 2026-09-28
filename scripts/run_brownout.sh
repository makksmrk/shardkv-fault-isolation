#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ $# -lt 4 || $# -gt 5 ]]; then
  echo "Usage: $0 <gateway-host> <backend-ssh-host> <backend-pid> <output.csv> [ssh-key]" >&2
  exit 1
fi

GATEWAY_HOST="$1"
BACKEND_SSH="$2"
BACKEND_PID="$3"
OUT="$4"
SSH_KEY="${5:-}"

if [[ ! "$BACKEND_PID" =~ ^[0-9]+$ ]]; then
  echo "backend-pid must be a numeric process ID." >&2
  exit 1
fi

SSH=(ssh)

if [[ -n "$SSH_KEY" ]]; then
  SSH+=(
    -i "$SSH_KEY"
    -o IdentitiesOnly=yes
  )
fi

SSH+=("$BACKEND_SSH")

cleanup() {
  "${SSH[@]}" "kill -CONT $BACKEND_PID" >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

(
  sleep 30

  echo "[brownout] SIGSTOP backend pid=$BACKEND_PID at t=30s"
  "${SSH[@]}" "kill -STOP $BACKEND_PID"

  sleep 15

  echo "[brownout] SIGCONT backend pid=$BACKEND_PID at t=45s"
  "${SSH[@]}" "kill -CONT $BACKEND_PID"
) &

INJECTOR_PID=$!

python3 "${PROJECT_ROOT}/tools/loadgen.py" \
  --host "$GATEWAY_HOST" \
  --port 9000 \
  --clients 64 \
  --duration 70 \
  --warmup 10 \
  --seed 42 \
  --timeout 5 \
  --csv "$OUT"

wait "$INJECTOR_PID"

trap - EXIT INT TERM