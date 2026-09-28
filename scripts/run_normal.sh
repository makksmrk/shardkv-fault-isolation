#!/usr/bin/env bash

# Run a normal-load test against a remote ShardKV gateway.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <gateway-host> <output.csv>" >&2
  exit 1
fi

GATEWAY_HOST="$1"
OUT="$2"

# run the load generator with the standard experiment configuration
python3 "${PROJECT_ROOT}/tools/loadgen.py" \
  --host "$GATEWAY_HOST" \
  --port 9000 \
  --clients 64 \
  --duration 70 \
  --warmup 10 \
  --seed 42 \
  --timeout 5 \
  --csv "$OUT"