#!/usr/bin/env bash
# Download BEIR NQ (corpus with Cohere embed-english-v3 vectors, test queries, and test qrels) into DATA_DIR/raw.
# Usage: download.sh DATA_DIR   (about 6 GB; public on the Hugging Face Hub, no token needed)
set -euo pipefail
cd "$1"
B="${HF_ENDPOINT:-https://huggingface.co}/datasets/CohereLabs/beir-embed-english-v3/resolve/main/nq"
for f in queries/test.parquet qrels/test.parquet corpus/0000.parquet corpus/0001.parquet corpus/0002.parquet; do
  mkdir -p "raw/$(dirname "$f")"
  curl -sSL --retry 5 -C - -o "raw/$f" "$B/$f"
done
