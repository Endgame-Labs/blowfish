# B4 offline BM25 experiments

`impact_bm25` compares impact-ordered sorted access and row-aligned frequency
scoring with the production reader. Nothing selects these paths in the library.
The example embeds the current reader, analyzer, and scorer to inspect private
postings without adding library APIs. The integrated experiment copies the
reader's query planner and changes its final dense scoring loop; keep that copy
in sync when repeating the experiment after a planner change.

Use the first 1,000,000 Tevatron MS MARCO passages, five consecutive 200,000-row
segments, and `bench/scale/data/bm25-msmarco.json`. `corpus.jsonl` must contain
exactly those first million JSON objects, in their original order. Preparation
uses the existing T1 experiment, with phrase positions enabled.

```bash
source ~/.cargo/env
run="$PWD/.scratch/b4-reproduction"
mkdir -p "$run"
trap 'rm -rf "$run"' EXIT
export BF_TEXT_EXPERIMENT="$run"
export CARGO_TARGET_DIR="$run/target"
cp bench/scale/data/bm25-msmarco.json "$run/workload.json"
# Stream the corpus into "$run/corpus.jsonl" before preparation.
cargo test --release -p bf-text --lib experiments::prepare_msmarco -- --ignored --nocapture
cargo build --release -p bf-text --example impact_bm25 --example exact_bm25
bin="$CARGO_TARGET_DIR/release/examples/impact_bm25"
"$bin" "$run" baseline
"$bin" "$run" impact
"$bin" "$run" impact-perfect
"$bin" "$run" dense
"$bin" "$run" dense-scalar
"$bin" "$run" dense-integrated
"$bin" "$run" dense-integrated-scalar
"$bin" "$run" cold
"$CARGO_TARGET_DIR/release/examples/exact_bm25" "$run/workload.json" \
  < "$run/corpus.jsonl" > "$run/exhaustive.json"
```

Preserve measurements before the cleanup trap runs. Check the shared worktree
`QUIET` file before builds or loads, and use an isolated Graviton host for cycles.
Compare the exhaustive utility's IDs and f32 score bits with `b4-truth.json`;
its IDs are original document IDs, whereas the example uses corpus ordinals.
For this corpus's first million passages, these IDs agree on the tested results.
All variants require baseline truth first and assert ordered hits and score bits
on both the warm-up and measured pass. Only baseline writes the truth file.

Modes:

| Mode | Work measured |
| --- | --- |
| `baseline` | Unchanged reader, sequential segments, and shared threshold |
| `impact` | 128-posting impact blocks for lists with df > 4,096 or >32 blocks, sorted within blocks; suffix bounds use independent max TF and min length |
| `impact-perfect` | Same sorted access with free exact block maxima under current namespace statistics |
| `dense` | Row-aligned u32 frequencies, exact unbounded window maxima, cached normalizers, and NEON scoring for windows with at least 16 candidates |
| `dense-scalar` | Same optimistic planner and frequency cache, with the existing saturation-table scalar scorer |
| `dense-integrated` | Same frequency cache and NEON kernel, retaining the reader's planner, bounded decoded cache, bound cache, and posting reads |
| `dense-integrated-scalar` | Same integrated experiment, with cached scalar scoring |
| `cold` | Baseline with decoded postings and saturation/bound caches reset for every segment/query; counts required posting payload blocks |

The impact queue contains one next block per term. Suffix maxima remain valid
when namespace average length differs from the segment's build-time average.
Each touched row retains term contributions, canonical f32 addition establishes
lower scores, and an indexed top-k heap supports increasing scores. Termination
checks untouched rows and every unresolved touched row, and requires winners to
have exact scores. It never completes a candidate using free random accesses.
Its posting-byte delta is computed over **every long list**, including terms not
in the queries, using the existing bitpacking width calculation. Directly
persisted format-7 files, dictionary-size changes, and phrase migrations are not
implemented by this experiment.

The dense cache is deliberately optimistic: every long query term gets a full
row-aligned array before measurement. Both dense modes share it, so the scalar
comparison isolates the scoring kernel. The standalone planner also gets exact
window maxima for free. A standalone pass is insufficient evidence for a format
change; the integrated mode retains planning and read costs. Its frequency cache
is additional to the original decoded cache, and it does not simulate a persisted
row-aligned representation's byte size or cold I/O.

For warm counters, use perf's control FIFOs so fixture loading and warm-up are
excluded. Set `BF_TEXT_PERF_CONTROL` and `BF_TEXT_PERF_ACK` to the FIFO paths and
run `perf stat -D -1 --control=fifo:CONTROL,ACK -e instructions:u,cycles:u` around
the binary, pinned to one CPU. Each mode enables counters for its second pass
only. `cold` runs one pass and reports logical posting blocks, not S3 GETs,
cache-block amplification, or network latency.

`cargo test -p bf-text --example impact_bm25` runs the new reference-bit and
randomized exhaustive-comparison tests, as well as tests in the embedded source.
The NEON test must also run natively on aarch64; other architectures use a scalar
fallback and cannot establish SIMD speed or ARM bit parity.
