#!/bin/bash
# Round C38: paired cold runs of a baseline and a candidate server build (and an A/A control).
# usage: c38_pairs.sh CONFIG BASE_BIN CAND_BIN PAIRS LABEL_PREFIX
# Run from the repo root on the measurement host. Shapes: 7.9 percent filter, token filter,
# text_length, id < 1000, unfiltered top_k 10, and a filter that matches nothing.
set -u
export PATH=$HOME/.local/bin:$PATH
CONFIG=$1 BASE=$2 CAND=$3 PAIRS=$4 PREFIX=$5
run() { # label, filter, arms-json, second arm name
  uv run crates/bf-engine/examples/cold_ab.py --config "$CONFIG" --binary "$BASE" --arms "$3" \
    --scratch .scratch/runs --label "$PREFIX-$1" --queries "$PAIRS" --block-size 262144 \
    --coalesce-blocks 32 --s3-only-arm base --s3-only-arm "$4" ${2:+--filters "$2"} \
    > ".scratch/runs/$PREFIX-$1.out" 2>&1
  echo "done $1"
}
mkdir -p .scratch/runs
AB="{\"base\":{\"BINARY\":\"$BASE\"},\"cand\":{\"BINARY\":\"$CAND\"}}"
AA="{\"base\":{\"BINARY\":\"$BASE\"},\"base2\":{\"BINARY\":\"$BASE\"}}"
run aa '["title_initial","Eq","s"]' "$AA" base2
run f79 '["title_initial","Eq","s"]' "$AB" cand
run token '["text","ContainsAllTokens","american"]' "$AB" cand
run length '["text_length","Gte",300]' "$AB" cand
run id1000 '["id","Lt",1000]' "$AB" cand
run unf '' "$AB" cand
run empty '["title_initial","Eq","__c26_no_match__"]' "$AB" cand
