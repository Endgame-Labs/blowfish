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
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null; wait 2>/dev/null; rm -rf "$run"; }
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
  echo "== compat ($mode)"
  (cd compat && uv run compat check --base-url "http://127.0.0.1:$port" --api-key x) 2>&1 | tail -3 || status=1
  kill "$pid"; wait "$pid" 2>/dev/null || true; pid=""
done
exit $status
