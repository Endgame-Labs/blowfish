#!/usr/bin/env bash
# Run the native embedding and reranking compat scenarios against a fresh local blowfish that
# talks to a deterministic fake provider (no model weights needed).
# Usage: scripts/compat_models.sh   (from the repo root; builds the release binary first)
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/.cargo/env 2>/dev/null || true
cargo build --release -q -p blowfish
run=.scratch/compat-models-$$
mkdir -p "$run/data"
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; rm -rf "$run"; }
trap cleanup EXIT
fake_port=$((19000 + RANDOM % 500))
port=$((18500 + RANDOM % 400))
(cd compat && uv run python -m compat.fake_models "$fake_port") &
pids+=($!)
fake="http://127.0.0.1:$fake_port"
BLOWFISH_MODELS_JSON="{
  \"fake/embed\": {\"provider\":\"openai_embeddings\",\"base_url\":\"$fake\",\"dims\":64,\"allowed_dims\":[16]},
  \"fake/rerank\": {\"provider\":\"rerank\",\"base_url\":\"$fake\"}
}" target/release/blowfish serve --storage-url "file://$PWD/$run/data" --listen "127.0.0.1:$port" > "$run/server.log" 2>&1 &
pids+=($!)
for _ in $(seq 60); do curl -sf "http://127.0.0.1:$port/" >/dev/null && break; sleep 0.5; done
(cd compat && COMPAT_MODELS_EMBED=fake/embed COMPAT_MODELS_RERANK=fake/rerank \
  uv run compat check --models --base-url "http://127.0.0.1:$port" --api-key x)
