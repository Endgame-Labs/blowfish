# Namespace ANN directory (perf iteration 4 design)

## Goal

A cold ANN query should take a fixed number of sequential object-store waves, at most 4, whatever
the namespace size. The bytes and requests it needs should grow with the number of probes, not the
number of segments. That is the property that lets turbopuffer go from 1M to 10M documents at
roughly double the cold latency. Today blowfish needs 5-6 waves, and every query opens per-segment
ANN metadata, so cold cost grows with segment count.

## Today (after iteration 3)

- Each segment has its own `.bfv`:
  - an IVF over that segment's rows, with rotated centers;
  - RaBitQ 1-bit codes stored contiguously per cluster;
  - full vectors for reranking.
- `ann.rs` ranks the leaf clusters of every segment together, scans the globally nearest clusters, and
  reranks one merged pool. To rank, it must first fetch every segment's header and centers. That
  costs one wave, plus bytes and requests proportional to the segment count.
- The reranking budget is a fixed factor (`ann_rerank_factor`, now 32 × k).

## Design

### 1. A persisted directory of all leaf clusters

The indexer writes one immutable object per published segment set:
`ann/<attr>/dir.<generation>.bfd`. The manifest references it by generation, next to the segment
list.

For every leaf cluster in every live segment, the directory stores:

| Field | Purpose |
|---|---|
| segment ID and local cluster ID | address |
| RaBitQ 1-bit code of the rotated center, plus f32 norm and correction | ranking clusters without full centers |
| live row count (after deletes, from the live mask generation) | skip empty clusters and size the probe |
| byte range of the cluster's codes in the segment's `.bfv` | wave-3 read without opening the segment |
| byte range of the cluster's full vectors in the segment's `.bfv` | wave-4 rerank read |
| per-cluster filter summary (optional, phase 2) | skip clusters that can't match a filter |

The directory also holds the shared rotation seed and transform width, the metric, and a checksum.

**Size:** about 140 bytes per leaf cluster at 1024 dims.
- 10M docs in about 50-60 segments have about 30K leaves, about 4 MB, which is one ranged read and
  fine to pin in memory.
- Past about 100M docs, add a top level: k-means over leaf centers, with leaves grouped by parent so
  each parent's children are one range. That's phase 3, not needed now.

**Building it** is cheap and incremental: concatenate the per-segment center codes the indexer
already has when it publishes a segment, and drop entries for retired segments. There is no global
re-clustering, and the segments' own IVFs are unchanged.

Every segment must use the same rotation seed for its centers to be comparable. Write new segments
with the namespace seed. Segments with another seed are listed in the directory as "legacy", and
the query path falls back to opening them, as it does today.

### 2. The cold query, in waves

1. **Snapshot:** the manifest slot, which is already inline, plus the WAL tail probe in parallel.
2. **Index level:** in parallel:
   - the directory (pinned after first use);
   - any filter bitmaps the query needs;
   - unindexed WAL payloads.

   Rank leaf centers from the directory with the RaBitQ estimator, and pick `nprobe` leaves
   globally.
3. **Probed codes:** one ranged GET per chosen leaf, or merged when adjacent, straight from the
   byte ranges in the directory. No segment headers. Scan them with the existing popcount kernel.
4. **Rerank and hydrate:** fetch full vectors for the rerank candidates, using direct byte ranges
   from the directory, and, in parallel, the columns needed to hydrate the likely winners.

Warm queries skip waves 2-4 when everything is cached. With an ids-only response and the
directory pinned, it's 3 waves.

### 3. Error-bounded reranking

RaBitQ gives each estimated distance a bound. Instead of reranking a fixed 32 × k, rerank only the
candidates whose lower bound is below the current k-th best upper bound. That is turbopuffer's
"under 1% reranked". Keep `ann_rerank_factor` as a cap, and add a test that results equal the
fixed-factor path whenever the bound selects a superset.

### 4. Hydration without whole ID indexes

ID indexes are read whole: 56 MB at 4.86M docs. Hydrating winners by segment-local row only needs
each segment's docs column footer, the row-group directory, so hydration should never touch the
ID index. Chunk the ID index by key range for the upsert and delete path, which is a separate
change.

## Non-goals for this iteration

- SPFresh-style split and merge of clusters, and a global re-clustering.
- Changing the segment `.bfv` layout, beyond writing the shared rotation seed.
- Weakening strong consistency.

## Acceptance (measured with `bench/scale/` and `oracle_compare.py`)

- Cold ANN at 100K: at most 4 sequential waves (3 for ids-only), fewer requests than 240, and cold
  p50 at least 30% below iteration 3 in the same session.
- The number of segments touched before wave 3 is 0. Prove it with a test that builds 20 segments
  and asserts wave and request counts, like the existing `cold_ann_has_three_read_stages` test.
- Recall@10 is at least 0.95, and turbopuffer parity overlap doesn't drop.
- Old segments still load and query, and old binaries reading new manifests fail cleanly.
- The full compat suite passes in both modes.
