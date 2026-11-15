#!/usr/bin/env bash
# Stream the dataset into a running scale server in a 6G scope.
# Usage: load.sh ROWS [--resume] [extra scale_load.py args]
source "$(dirname "$0")/common.sh"
export OPENBLAS_NUM_THREADS=2 OMP_NUM_THREADS=2 MALLOC_ARENA_MAX=2
rows="$1"; shift
exec systemd-run --user --scope --unit=scale-loader -p MemoryMax=6G \
  uv run "$ROOT/crates/bf-engine/examples/scale_load.py" \
  --hub-endpoint "$(cfg hub_endpoint)" --scratch "$SCRATCH/load" --rows "$rows" \
  --namespace "$(cfg namespace)" --batch 2000 --vector-type f16 "$@"
