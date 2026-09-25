#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <gateway-host> <output.csv>" >&2
  exit 1
fi

GATEWAY_HOST="$1"
OUT="$2"

python3 ./loadgen.py \
  --host "$GATEWAY_HOST" --port 9000 \
  --clients 64 --duration 70 --warmup 10 \
  --seed 42 --timeout 5 \
  --csv "$OUT"
