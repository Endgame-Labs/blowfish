#!/usr/bin/env bash
# Run the worktree's release blowfish for the NQ benchmark, in the foreground.
# Usage: serve.sh STORE_DIR [PORT]
#   NQ_CPUS     CPUs for the server (taskset list, default 0-5; empty for no pinning)
#   NQ_MEM      memory cap of the systemd scope (default 12G)
#   NQ_BIN      server binary (default target/release/blowfish in the repo)
# Every other setting goes through the environment (BLOWFISH_*, BF_*), for example
# BLOWFISH_ANN_NPROBE=160 or BLOWFISH_CACHE_MEMORY_BYTES=3221225472.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STORE="$(realpath -m "$1")"
PORT="${2:-18100}"
mkdir -p "$STORE"
BIN="${NQ_BIN:-$ROOT/target/release/blowfish}"
PIN=()
if [ -n "${NQ_CPUS-0-5}" ]; then PIN=(taskset -c "${NQ_CPUS-0-5}"); fi
exec systemd-run --user --scope --quiet -p MemoryMax="${NQ_MEM:-12G}" -p MemorySwapMax=0 \
  "${PIN[@]}" "$BIN" serve --listen "127.0.0.1:$PORT" --storage-url "file://$STORE"
