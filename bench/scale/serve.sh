#!/usr/bin/env bash
# Run the benchmark server inside a memory-capped systemd scope (foreground).
# Usage: serve.sh STORAGE_JSON [extra env assignments are taken from the environment]
# Engine knobs: BF_INDEX_ROWS, BF_INDEX_BYTES-style variables read by EngineConfig::from_env,
# BF_ANN_NPROBE, BF_ANN_RERANK, BF_DECODED_CACHE_BYTES.
source "$(dirname "$0")/common.sh"
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-6}" MALLOC_ARENA_MAX=2 AWS_TIMEOUT=180s
exec systemd-run --user --scope --unit="${SCALE_UNIT:-scale-server}" -p MemoryMax=16G -p MemoryHigh=14G \
  "$SCRATCH/target/release/blowfish-scale-server" "$1" "${2:-127.0.0.1:8300}"
