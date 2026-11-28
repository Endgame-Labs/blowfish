#!/bin/bash
# Round C40: paired cold runs (with warm repeats) of a baseline and a candidate server build.
# usage: c40_pairs.sh CONFIG BASE_BIN CAND_BIN PAIRS LABEL_PREFIX WARM_REPEATS [CASE ...]
# Run from the repo root on the measurement host. Cases: aa, f79 (7.9 percent filter), f064 (0.64
# percent filter), length (text_length >= 300), unf (unfiltered top_k 10).
set -u
export PATH=$HOME/.local/bin:$PATH
CONFIG=$1 BASE=$2 CAND=$3 PAIRS=$4 PREFIX=$5 WARM=$6
shift 6
CASES=${*:-"aa f79 f064 length unf"}
run() { # label, filter, arms-json, second arm name
  uv run crates/bf-engine/examples/cold_ab.py --config "$CONFIG" --binary "$BASE" --arms "$3" \
    --scratch .scratch/runs --label "$PREFIX-$1" --queries "$PAIRS" --block-size 262144 \
    --coalesce-blocks 32 --warm-repeats "$WARM" --s3-only-arm base --s3-only-arm "$4" ${2:+--filters "$2"} \
    > ".scratch/runs/$PREFIX-$1.out" 2>&1
  echo "done $1"
}
mkdir -p .scratch/runs
AB="{\"base\":{\"BINARY\":\"$BASE\"},\"cand\":{\"BINARY\":\"$CAND\"}}"
AA="{\"base\":{\"BINARY\":\"$BASE\"},\"base2\":{\"BINARY\":\"$BASE\"}}"
for c in $CASES; do
  case $c in
    aa) run aa '["title_initial","Eq","s"]' "$AA" base2 ;;
    f79) run f79 '["title_initial","Eq","s"]' "$AB" cand ;;
    f064) run f064 '["title_initial","Eq","z"]' "$AB" cand ;;
    length) run length '["text_length","Gte",300]' "$AB" cand ;;
    unf) run unf '' "$AB" cand ;;
  esac
done
