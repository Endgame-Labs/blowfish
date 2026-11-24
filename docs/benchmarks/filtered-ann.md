# Filtered ANN: retain neighbors while reducing search work

Initial profiling used main `0541703`. Final controls compare main `5c5fec8` with candidate `6b8a4ae`: an allowed-row
coverage planner, cached membership counts, and sparse code scoring. The resident-scan change is present in both arms. Both arms read one frozen, format-4 snapshot of full BEIR NQ: 2,681,468 rows by 1,024 f16 dimensions. The first
64 distinct query vectors use ID-prefix and SplitMix64 bucket filters at 1%, 10%, and 50%. Exhaustive truth is computed
from the stored f16 vectors. This rebuild has 111 segments; D4's deleted scalar snapshot had 65. These are paired results
on the rebuilt snapshot, rather than a reproduction of D4's exact physical objects or its hardware.

All **768 quality comparisons** (six filters, 64 queries, and 48/64 probes) retain every true neighbor found by current
main. Ordered row objects, including distances, also match in all 768 comparisons, before and after merging `557fc8f`. The 40,000-vector regression compares
true-neighbor sets per query at both probe counts, in addition to the original negative control and format-4 assertions.
The sparse scorer's unit test compares raw score and error-bound bits against the block scorer across dimensions, block
boundaries, zero markers, selected subsets, and finite cuts.

Both arms have the following recall@10 against exhaustive filtered truth:

| Filter | 48 probes | 64 probes | Queries losing baseline true neighbors |
|---|---:|---:|---:|
| ID 1% | 1.0000000 | 1.0000000 | 0 |
| ID 10% | 1.0000000 | 1.0000000 | 0 |
| ID 50% | 0.9703125 | 0.9828125 | 0 |
| Bucket 1% | 1.0000000 | 1.0000000 | 0 |
| Bucket 10% | 0.9984375 | 1.0000000 | 0 |
| Bucket 50% | 0.9359375 | 0.9531250 | 0 |

## Profile before changing the planner

An isolated, warm, 48-probe trace follows a separate 64-query prewarm. Scan durations below sum time inside the scoring
kernel across segment tasks. Planning and reranking are elapsed stage differences, so they are not additive CPU counters.
The long controls below use process-wide perf counters for CPU accounting.

| Filter | Nonempty clusters/query | Code rows/query | Summed scan time (ms) | Planning (ms) | Rerank stage (ms) | Reranked rows/query |
|---|---:|---:|---:|---:|---:|---:|
| ID 1% | 260 | 34,638 | 0.727 | 0.214 | 0.353 | 102.0 |
| ID 10% | 1,822 | 281,890 | 6.325 | 1.269 | 0.391 | 59.4 |
| ID 50% | 1,252 | 238,658 | 6.363 | 5.649 | 0.681 | 65.8 |
| Bucket 1% | 11,231 | 2,249,370 | 56.850 | 2.272 | 1.310 | 99.6 |
| Bucket 10% | 6,666 | 1,357,623 | 33.478 | 2.505 | 1.343 | 114.9 |
| Bucket 50% | 1,253 | 281,472 | 7.588 | 5.684 | 0.984 | 89.4 |

The existing planner already skips leaves with zero allowed rows. Its inverse-selectivity minimum still exhausts localized
filters' nonempty leaves. The hash-1% scanner evaluates about 2.25 million codes for 26,735 allowed vectors. At 50%,
recomputing membership counts dominates planning despite the predicate bitmap already being cached.

A separate control recreates only the old three-times-selectivity cap on pinned main, retaining the original block scorer.
At D4's matched probe settings, ID 10% visits 1,408 clusters and 239,207 code rows at 48 probes, versus main's 1,822 and
281,890: **17.8% more code rows**. Summed scan time grows from 5.49 to 6.21 ms, while planning and reranking stay nearly
flat. Hash 1% at 256 probes visits 7,505 clusters and 1,649,761 code rows, versus scaled main at 48 probes visiting 11,231
and 2,249,370: **36.3% more code rows**. Summed scan time grows from 45.07 to 59.22 ms, versus about 0.37 ms more planning
and unchanged reranking. Extra coarse-code work explains the regressions; this recreated cap control is an attribution
experiment, rather than D4's original binary or deleted snapshot.

## Final search work and stages

The following are equal-weight means of the 64-query warm profiles at 48 configured probes. Allowed rows use exact
membership counts. Main exhausts every nonempty leaf for ID 1%, ID 10%, and bucket 1%; the other masks retain identical
cluster choices between arms. Code rows describe the full blocks visited, while evaluated codes describe actual coarse
scoring, so sparse scoring changes the latter without changing diagnostic scan-row accounting.

| Filter | Clusters, main → F1 | Allowed rows, main → F1 | Code rows, main → F1 | Evaluated codes, main → F1 |
|---|---:|---:|---:|---:|
| ID 1% | 260 → 260 | 26,815 → 26,815 | 34,638 → 34,638 | 34,638 → 34,638 |
| ID 10% | 1,822 → 1,481 | 268,147 → 235,668 | 281,890 → 248,589 | 281,890 → 248,589 |
| ID 50% | 1,252 → 1,252 | 233,079 → 233,079 | 238,658 → 238,658 | 238,658 → 238,658 |
| Bucket 1% | 11,231 → 11,231 | 26,735 → 26,735 | 2,249,370 → 2,249,370 | 2,249,370 → 27,762 |
| Bucket 10% | 6,666 → 6,666 | 135,220 → 135,220 | 1,357,623 → 1,357,623 | 1,357,623 → 1,357,623 |
| Bucket 50% | 1,253 → 1,253 | 140,744 → 140,744 | 281,472 → 281,472 | 281,472 → 281,472 |

Stage measurements below are elapsed traces, as above, rather than an additive process-CPU partition. Rank includes query
rotation and center ranking; planning follows ranking. The counted controls below supply the actual whole-process CPU.

| Filter | Rank ms, main → F1 | Planning ms, main → F1 | Summed scan ms, main → F1 | Rerank ms, main → F1 | Reranked rows, main → F1 |
|---|---:|---:|---:|---:|---:|
| ID 1% | 0.423 → 0.139 | 0.267 → 0.185 | 0.802 → 0.801 | 0.776 → 0.696 | 102.0 → 102.0 |
| ID 10% | 0.432 → 0.182 | 1.289 → 0.288 | 6.262 → 5.622 | 0.513 → 0.532 | 59.4 → 59.4 |
| ID 50% | 0.425 → 0.350 | 5.631 → 0.491 | 6.566 → 6.555 | 0.769 → 0.801 | 65.8 → 65.8 |
| Bucket 1% | 0.417 → 0.407 | 2.358 → 1.500 | 59.534 → 27.396 | 1.443 → 1.391 | 99.6 → 99.6 |
| Bucket 10% | 0.428 → 0.428 | 2.591 → 1.111 | 35.149 → 35.295 | 1.504 → 1.520 | 114.9 → 114.9 |
| Bucket 50% | 0.434 → 0.412 | 5.723 → 0.452 | 8.094 → 7.933 | 1.114 → 1.110 | 89.4 → 89.4 |

## Rejected coverage rules

A literal rerank-budget-plus-witness stop, even with the unfiltered geometric minimum, reduces hash-1% recall at 48 probes
from 1.000000 to 0.8328125: 47 of 64 queries lose true neighbors. ID 10% falls to 0.9921875, with five losing queries.
Larger coverage equivalent to one unfiltered probe population still loses neighbors. Two populations preserve ID 10%, but
lose true neighbors on three ID-50% queries at 48 probes and four at 64. Filling a candidate pool does not establish that
approximate center ranking has covered the nearest filtered vectors.

The final planner counts exact allowed members until it covers two unfiltered probe populations plus the existing witness
margin, and at least the rerank budget. It increases that goal by `sqrt(source_rows / rows_in_wholly_excluded_leaves)`.
The benefit fades smoothly as fewer source rows are eliminated, avoiding a hard selectivity boundary; with no excluded
leaves, it retains the original plan. The original scaled probe plan remains its ceiling. Measured broad and distributed
masks keep main's geometry. Per-query validation remains necessary; this is not a universal ANN recall proof.

Empty filtered segments skip even center-code ranking. Sparse masks derive selected ordinals using one bounded scratch
membership bitmap per scan, then score them using the existing coarse tables and fine-rescore pass. Widely spaced external
row IDs retain bitmap intersection; bitmap length is capped at one u64 word per scanned row, with normal scratch-capacity
reuse. The cutoff is one sixteenth
both across the scan and within a leaf. Checksums and factor validation still cover whole blocks, and dense and unfiltered
scans use the same block kernel. Immutable predicates and derived membership counts share the existing bounded decoded
cache. No wire format, bucket format, or runtime setting changes.

A sampled profile of the first sparse implementation put 19.5% of CPU in selected-code lookups and 9.5% in repeated bitmap
intersection. Fixed-width table and code chunks removed repeated bounds checks; a single scratch bitmap removed those
repeated sparse merges. In separate 3,000-query trials, hash-1% CPU fell from 48.48 to 46.46 to 40.96 ms/query, while
instructions fell from 219.26M to 161.95M to 143.20M. These exploratory trials are excluded from the final paired totals.

## Long paired controls

Both arms run on one on-demand c7g.8xlarge, pinned to the same six cores, with six Rayon and Tokio workers, a 3 GiB block
cache, no disk cache, 4,108,168,192 decoded bytes, read-only S3 storage, one-hour retention, and recall sampling disabled.
There are no concurrent builds, transfers, truth computations, or unrelated loads during timing. Each arm cycles the same
64 vectors through 2,000 prewarm requests, then 1,000 serial and 2,000 16-client requests. Perf FIFO acknowledgements enable
counters only for those 3,000 measured requests. A second pass reverses both arm and filter order.

CPU and instructions are arithmetic means of the two equal-sized passes. Throughput pools both 2,000-request concurrent intervals (the harmonic mean of q/s). All 28 arms have **100% PMU coverage**, and process CPU agrees within **1.82%**. Each arm counts exactly 3,000 requests: 84,000 counted and 56,000 prewarm requests in total.

| Filter | Main CPU ms/query | F1 CPU ms/query | Change | Limit |
|---|---:|---:|---:|---:|
| ID 1% | 4.737 | 3.897 | -17.7% | 4.90 |
| ID 10% | 12.090 | 8.805 | -27.2% | 9.60 |
| Bucket 1% | 67.183 | 40.329 | -40.0% | 46.40 |
| Bucket 10% | 45.780 | 43.617 | -4.7% | Main + 3% |
| ID 50% | 19.895 | 14.105 | -29.1% | Main + 3% |
| Bucket 50% | 22.926 | 17.548 | -23.5% | Main + 3% |
| Unfiltered | 3.476 | 3.553 | +2.2% | Main + 3% |

Every CPU gate passes. All six filtered cases improve in both orders. The unfiltered control is +2.2% CPU and +0.4% instructions across both orders; its first-order CPU pair alone is +3.3%.

| Filter | Main instructions/query (M) | F1 instructions/query (M) | Main 16-client q/s | F1 16-client q/s |
|---|---:|---:|---:|---:|
| ID 1% | 25.154 | 13.242 | 1364.5 | 1203.0 |
| ID 10% | 73.237 | 44.154 | 552.4 | 776.7 |
| Bucket 1% | 363.692 | 143.075 | 98.9 | 167.7 |
| Bucket 10% | 237.367 | 223.604 | 149.8 | 156.8 |
| ID 50% | 138.947 | 62.037 | 354.4 | 538.1 |
| Bucket 50% | 149.003 | 76.453 | 318.4 | 311.7 |
| Unfiltered | 22.362 | 22.448 | 1228.0 | 1307.7 |

ID-1% throughput falls 11.8%, despite lower CPU and search execution time; this is a throughput limitation of these controls, not a throughput win. Both arms issue only one snapshot HEAD per counted request, with no object-body reads. Search overlaps snapshot validation, and the roughly 11 ms HEAD latency dominates this small case. The diagnostics do not fully explain the residual throughput difference. Bucket-50% throughput pools to -2.1%: its candidate runs are 446.8 and 239.4 q/s, with mean snapshot HEAD latency rising from 11.19 to 17.68 ms in the slower run. Other filtered cases gain throughput. CPU and instruction improvements should not be presented as uniform throughput gains.

Per-order CPU ms/query:

| Filter | Main, first order | F1, first order | Main, reversed | F1, reversed |
|---|---:|---:|---:|---:|
| ID 1% | 4.726 | 3.809 | 4.749 | 3.984 |
| ID 10% | 12.123 | 8.895 | 12.057 | 8.715 |
| Bucket 1% | 67.045 | 40.558 | 67.322 | 40.101 |
| Bucket 10% | 45.276 | 43.574 | 46.285 | 43.660 |
| ID 50% | 19.819 | 14.180 | 19.971 | 14.030 |
| Bucket 50% | 23.037 | 17.321 | 22.816 | 17.775 |
| Unfiltered | 3.448 | 3.560 | 3.505 | 3.545 |

CPU thresholds from D4 refer to another host and snapshot. The three explicit absolute targets pass here; all remaining
CPU limits use the paired pinned-main control on this same rebuilt snapshot.

## Reproduction

Prepare NQ using `bench/nq/download.sh` and `bench/nq/prepare.py`. During the load, add a uint `bucket` column computed by
`bucket(ids)` in `crates/bf-engine/bench/filtered_nq.py`; the ID is the corpus row number. Index every row, build the recluster
tier, freeze the prefix, and keep both arms read-only. Do not run a writable server on that frozen prefix.

Generate the six workloads and exhaustive truth:

```sh
source ~/.cargo/env
OPENBLAS_NUM_THREADS=8 uv run --project bench/nq python crates/bf-engine/bench/filtered_nq.py \
  .scratch/run/data .scratch/run/work
cargo build --release -p bf-engine --example filtered_bench
```

`filtered_bench STORAGE_JSON quality OUTPUT WORKLOAD_JSON` saves ordered rows. Compare each returned ID set with exhaustive
truth and assert that `baseline_true_ids - candidate_true_ids` is empty for every query, at both 48 and 64 probes. The
workload generator was rerun against the full corpus and reproduced the independently prepared truth exactly.

`profile` prewarms all supplied requests and emits `FILTER_PROFILE_BEGIN` before the measured pass; set
`BF_TRACE_STAGES=1`. `COVERAGE` reports exact allowed counts, and `SCAN` separates code rows visited from codes actually
evaluated. `warm` implements the 2,000/1,000/2,000 schedule. Use `BF_ANN_NPROBE=48`, `BF_ANN_RERANK_FACTOR=32`,
`BF_DECODED_CACHE_BYTES=4108168192`, and `BF_RETENTION_SECS=3600` for both arms. On Linux, the driver sets identical
worker counts, cache budgets, probe counts, and core affinity, excludes startup and prewarm, and validates counters:

```sh
source ~/.cargo/env
uv run --no-project python crates/bf-engine/bench/filtered_perf.py \
  --binary target/release/examples/filtered_bench \
  --config .scratch/run/storage.local --workload .scratch/run/work/id-10.json \
  --out .scratch/run/counters/id-10 --probes 48
```

Keep configuration, workload outputs, perf FIFOs, and logs under `.scratch/`, and remove them and the owned bucket prefix
when the run ends.
