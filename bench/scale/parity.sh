#!/usr/bin/env bash
# Recall and parity against turbopuffer for one server binary and environment (1M oracle).
# Usage: [ENV=VAL ...] parity.sh BINARY KINDS OUT.json   (port: SCALE_PORT, default 8392)
# Needs TPUF_URL (or the exe.dev integration URL) and the oracle namespace name in config.local ("tpuf_namespace").
source "$(dirname "$0")/common.sh"
BIN="$1"; KINDS="$2"; OUTJSON="$3"; PORT="${SCALE_PORT:-8392}"
W="$SCRATCH/parity"; rm -rf "$W"; mkdir -p "$W/cache"
python3 "$BENCH_DIR/mkconfig.py" "$CONFIG" "$W/storage.json" "$W/cache" --memory-gib 8 --read-only
RAYON_NUM_THREADS=6 MALLOC_ARENA_MAX=2 "$BIN" "$W/storage.json" "127.0.0.1:$PORT" > "$W/server.log" 2>&1 &
PID=$!; trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$W"' EXIT
until curl -s -o /dev/null "http://127.0.0.1:$PORT/"; do sleep 0.5; done
: "${TPUF_URL:?set TPUF_URL to your turbopuffer endpoint}"; TPUF_URL="$TPUF_URL" uv run "$BENCH_DIR/oracle_compare.py" \
  --blowfish "http://127.0.0.1:$PORT" --bf-ns "$(cfg namespace)" --tpuf-ns "$(cfg tpuf_namespace)" \
  --data "$BENCH_DIR/data/oracle-1m.npz" --kinds "$KINDS" --out "$OUTJSON"
