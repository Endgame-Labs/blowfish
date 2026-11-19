#!/usr/bin/env bash
# Run the compat suite against a fresh local blowfish, in normal and forced-indexing modes.
# Usage: scripts/compat_both.sh   (from the repo root; builds the release binary first)
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/.cargo/env 2>/dev/null || true
cargo build --release -q -p blowfish
run=.scratch/compat-both-$$
mkdir -p "$run"
pid=""
bp_pid=""
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null; [ -n "$bp_pid" ] && kill "$bp_pid" 2>/dev/null; wait 2>/dev/null; rm -rf "$run"; }
trap cleanup EXIT
port=$((18000 + RANDOM % 1000))
status=0
for mode in normal forced; do
  rm -rf "$run/data" && mkdir -p "$run/data"
  envs=()
  [ "$mode" = forced ] && envs=(BF_INDEX_ROWS=1 BF_INDEX_AGE_MS=1 BF_INDEX_INTERVAL_MS=50)
  env "${envs[@]}" target/release/blowfish serve --storage-url "file://$PWD/$run/data" \
    --listen "127.0.0.1:$port" > "$run/server-$mode.log" 2>&1 &
  pid=$!
  for _ in $(seq 60); do curl -sf "http://127.0.0.1:$port/" >/dev/null && break; sleep 0.5; done
  # A second server with a 1-byte unindexed limit and a slow indexer answers writes 429 until it
  # publishes: the SDK retry scenarios (compat/src/compat/sdk_retry.py and the TypeScript smoke).
  bp_port=$((port + 1))
  BF_MAX_UNINDEXED_BYTES=1 BF_INDEX_AGE_MS=2000 BF_INDEX_INTERVAL_MS=200 \
    target/release/blowfish serve --storage-url "file://$PWD/$run/data-bp" \
    --listen "127.0.0.1:$bp_port" > "$run/server-bp-$mode.log" 2>&1 &
  bp_pid=$!
  for _ in $(seq 60); do curl -sf "http://127.0.0.1:$bp_port/" >/dev/null && break; sleep 0.5; done
  echo "== compat ($mode)"
  (cd compat && COMPAT_BACKPRESSURE_URL="http://127.0.0.1:$bp_port" \
    uv run compat check --base-url "http://127.0.0.1:$port" --api-key x) 2>&1 | grep -E "passed|FAIL" || status=1
  echo "== typescript smoke ($mode)"
  COMPAT_BACKPRESSURE_URL="http://127.0.0.1:$bp_port" compat/smoke-ts/run.sh "http://127.0.0.1:$port" 2>&1 | tail -3 || status=1
  kill "$bp_pid"; wait "$bp_pid" 2>/dev/null || true; bp_pid=""; rm -rf "$run/data-bp"
  kill "$pid"; wait "$pid" 2>/dev/null || true; pid=""
done
exit $status
