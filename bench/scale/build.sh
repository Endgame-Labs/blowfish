#!/usr/bin/env bash
# Build the benchmark HTTP server (release, separate workspace) into $SCRATCH/target.
source "$(dirname "$0")/common.sh"
cd "$ROOT/crates/bf-engine/examples/scale-server"
CARGO_TARGET_DIR="$SCRATCH/target" cargo build --release
