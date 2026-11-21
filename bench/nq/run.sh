#!/usr/bin/env bash
# Measure one configuration: start the server on STORE_DIR with the given environment, run measure.py
# (clients pinned to NQ_CLIENT_CPUS, default 6,7), and stop the server.
# Usage: run.sh STORE_DIR DATA_DIR TAG [VAR=value ...] [-- measure.py options]
# Example: run.sh .scratch/nq-run/store .scratch/nq-data reclustered-64 BLOWFISH_ANN_NPROBE=64
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
STORE="$1" DATA="$(realpath "$2")" TAG="$3"
shift 3
ENVS=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do ENVS+=("$1"); shift; done
[ "${1:-}" = "--" ] && shift
PORT="${NQ_PORT:-18100}"
OUT="${NQ_OUT:-$(dirname "$DATA")/nq-run/results.jsonl}"
LOG="$(dirname "$OUT")/server-$TAG.log"
env "${ENVS[@]}" "$HERE/serve.sh" "$STORE" "$PORT" > "$LOG" 2>&1 &
until curl -s -o /dev/null "http://127.0.0.1:$PORT/"; do sleep 0.5; done
PID=$(ss -ltnpH "sport = :$PORT" | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2)
trap 'kill $PID 2>/dev/null; while kill -0 $PID 2>/dev/null; do sleep 0.2; done' EXIT
cd "$HERE"
uv run python -I measure.py "$DATA" --base-url "http://127.0.0.1:$PORT" --tag "$TAG" \
  --cpus "${NQ_CLIENT_CPUS-6,7}" --out "$OUT" "$@"
