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

## Iteration 5: fragments and one-bit centers

### Fragments make publication incremental

Each segment now writes a **fragment** (`ann.<column>.bff`, manifest file key `annfrag:<attribute>`) when its ANN
index is built, and the directory is the concatenation of the fragments' resident parts. A fragment holds:

- a resident part: the segment's mean center (`centroid`, f32), and per non-empty leaf cluster its row count, the code
  and vector byte ranges and checksums inside the segment's `.bfv`, the byte range of its blob inside the fragment,
  and the one-bit center code;
- a blob area: per cluster the exact rotated center, the row set, and the row checksums, read only for probed clusters.

Publishing a segment set reads only the fragments of segments the previous directory does not already list (one
ranged read each), copies the other sections from the previous directory in memory, and writes one new object. Nothing
opens an existing segment, so ingest cost per publication does not grow with the segment count (test
`directory_cost_and_publication_do_not_grow_with_segment_count`: 60 publications, zero segment-index reads, at most
two fragment reads and one directory read each). The directory (`BFDIR002`) holds no blobs: it is all resident
metadata, read whole in the snapshot wave.

Segments built before fragments existed have no `annfrag:` file, so the namespace has no directory and queries use
the per-segment path, as before. Compaction rewrites such segments with fragments.

### One-bit centers

Each center is stored as a RaBitQ code of its offset from the segment's mean center: `padded / 8` sign bytes, the
offset's norm, and the code's inner-product correction (`alpha`). The query side builds a per-segment byte lookup
table (128 lookups per cluster at 1024 dims) and estimates `|q - c|^2 = |q - g|^2 + |c - g|^2 - 2 <q - g, c - g>`,
dividing the one-bit inner product by `alpha`. Centering on the segment mean matters: raw centers of normalized
embeddings share one large mean component, and their sign codes would all agree.

Size: 196 bytes per leaf at 1024 dims (136 for the code and correction, 60 for the entry), against about 1.1 KB
resident for the int8 version. At 10M documents (about 22K leaves) that is about 4.4 MB. Recall@10 at 100K was unchanged
(0.984).

### Still open

- Filtered ANN does not use the directory (per-cluster filter summaries are not built): selective and broad filtered
  queries open every segment's index and ID index.
- Rerank is still sparse ranges, about 120 requests per query at 100K, in one wave. Interleaving full vectors with
  the codes would read every probed row's vector (about 48 MB at 100K), so it is not worth it; the remaining lever is a
  smaller rerank pool.
- A segment's vectors are ordered by cluster, not by cluster proximity, so rerank ranges rarely merge.

## Iteration 6: filters through the directory, and row-to-cluster maps

Fragments (`BFFRG002`) and directories (`BFDIR003`) gained, per clustered segment, a **membership map**: one u16 (u32
past 65,535 clusters) per row giving its leaf cluster, stored after the blobs in the fragment and read by a ranged GET
(about 2 bytes per row, 250 KB for a 125K-row segment, cached decoded). Ordinals within a cluster follow ascending row
order, so the map alone yields, for any allowed row, its cluster and the byte offset of its vector.

A filtered ANN query now takes the same path as an unfiltered one:

1. The snapshot wave loads the directory, each segment's allowed-row bitmap (live rows that pass the filter, from the
   segment's existing filter file), and each segment's membership map. No segment index or ID index is opened.
2. Exact allowed rows per cluster come from the map. If at most `exact_fetch_rows` (8,192) rows are allowed, their
   vectors are fetched directly (`PartialIndex::exact_at`, merged ranges) and ranked exactly: no blobs, no codes.
   Otherwise the nearest clusters (probe count up to `filtered_probe_cap` = 3 times the unfiltered count, until the
   allowed candidates reach the rerank budget) are scanned by code with the allowed rows as the filter, then reranked.
3. Winners are hydrated by segment row (`projected_rows`), so no ID index is read.

Old fragments and directories (no map) make the directory unusable for the namespace until compaction rewrites them:
queries use the per-segment path, as for segments without fragments. A directory in an unknown format is ignored, not an
error. The unindexed-tail fall-back (any tail upsert disables the directory path) is unchanged and is the next fix.

## Iteration 7: the unindexed tail and the directory

An unindexed tail no longer disables the directory path. `NamespaceState::shadowed` still records every tail ID, but the
directory search does not need it to exclude segment rows up front:

1. The directory search asks for `k + max(2k, 20)` winners when `shadowed` is not empty.
2. Winners whose ID is in `shadowed` (an overwrite or delete in the tail) are dropped by ID. IDs that are new in the tail
   (the bulk-load case) never appear in a segment, so nothing is dropped and the extra winners are trimmed.
3. If the drops leave fewer than k winners while the pool was full, the query falls back to the per-segment path (rare:
   it needs most of the nearest rows to be overwritten).

Filtered queries compute their per-segment allowed bitmaps without the shadow lookup (`snapshot::allowed_with(.., false)`),
which would have opened every segment's ID index, and drop shadowed winners the same way. The test
`unindexed_tail_keeps_the_directory_path` deletes and moves the query's own three nearest neighbours in the tail and checks
that results stay exact with no `.bfi` read.

The indexer also stopped invalidating the resident tail: a node that publishes a checkpoint adopts it in memory
(`NamespaceState::fast_forward`, using each tail ID's last-write sequence), and a manifest that republishes the checkpoint
the node already holds (garbage collection, epoch claims) leaves the tail alone.

## Iteration 8: chunked text dictionaries and corpus statistics

BM25 cold bytes were the last cost in the cold path that grew with the data: each text segment's FST dictionary (about
2.5 MB per 100K rows) and the whole corpus-statistics FST (5.9 MB at 1M) were read whole by every cold query.

**Text segments, version 3 (`BFTEXT01`, footer version 3).** Layout: postings, term records, dictionary chunks, packed
lengths, chunk index, footer. The dictionary is cut into chunks of 2,048 consecutive terms; each chunk is its own small FST
mapping a term to the same `(record offset << 28) | document frequency` handle as version 2. The chunk index (first term,
offset, length, and crc32 per chunk, about 20 bytes per 2,048 terms plus the first term) sits next to the lengths in the
metadata tail, so opening a segment is still one tail read. A query binary-searches the index and reads only the chunks that
hold its terms, in one batch. Prefix queries read the chunks that overlap the prefix range, and Boolean `ContainsAllTokens`
or fuzzy scans read every chunk (they scanned the whole FST before). Decoded chunks are memoized per reader (64 at most).
Document lengths are bit-packed per 128-row block (`minimum | bits | packed offsets`), about 1 byte per row instead of 4.
Versions 1 and 2 still read; older binaries reject version 3 cleanly ("unknown text segment format").

**Corpus statistics (`BFSTAT02`).** The statistics object is a list of FST chunks (4,096 terms each, crc32 per chunk). Its
chunk index (per field: documents, total length, and `(first term, offset, length, crc)` per chunk) is carried in the
manifest next to the object reference (`SegmentFile::stats_index`, about 100 bytes per chunk), so it arrives with the
snapshot and a query range-reads only the chunks that hold its terms. The legacy whole-object format still reads.

**Snapshot-wave prefetch.** The open hint keeps each BM25 clause's query value. In the snapshot wave, the warm-up tokenizes
it with the manifest's schema, fetches the statistics chunks, and, for each segment, opens the text index and then runs
`prefetch_plan` (dictionary chunks, then term records). The query proper then finds all of it cached and only reads posting
blocks and the winning rows' IDs. Hybrid queries no longer wait for statistics before starting the ANN half, because the
statistics are already cached when `global_stats` runs.

**ID indexes over 128 KiB** (was 1 MiB) are no longer read whole by BM25 queries; hits take their IDs from the `id` column
of the documents file. A query over many segments already pays that final sequential wave.

Not changed: the indexer still rewrites the whole statistics object on each publication (about 5 MB at 1M, written in
parallel with the segment uploads). Per-segment lengths are still read whole at open (about 1 byte per row now).

## Iteration 11: block fragments (one read per probed cluster, in locality order)

At 10M rows in 70 segments, a cold unfiltered ANN query probed about 540 clusters (64 probes) and made two reads for each:
the cluster's blob from the fragment and its quantized codes from the segment's index object, so one query made about 1,100
requests, and the requests (not the rerank, which is about 70 small reads) set the cold latency.

A **block fragment** (`annfrag2:<attribute>`, magic `BFFRG003`) holds, for every non-empty cluster, the blob and then the
cluster's codes, so a probe is one ranged read. The blocks are ordered along a greedy nearest-neighbor tour of the cluster
centers, so the clusters a query probes in one segment tend to sit side by side and merge into fewer reads (measured on a real
200K-row segment: 8 probed clusters make 4.4 ranges, against 7.8 in cluster-ID order). A block fragment has no row-to-cluster map.

A second directory (`ann2:<attribute>` in `ann_dirs`, magic `BFDIR004`, its own hint copy) lists the block fragments. Unfiltered
queries use it, and filtered ones keep using the plain directory and its maps. Both sets of objects sit side by side:

- Readers that predate block fragments ignore the new manifest keys and keep using `ann:<attribute>`.
- A segment built before block fragments gets its `annfrag2:` file from the next indexing pass, which publishes even when there is
  nothing to flush (one whole read of the segment's index object each, a one-time cost; 70 segments of 440 MB took 540 s).
- `BF_ANN_BLOCKS=0` makes queries ignore the block layout (A/B only).

The scan prefetch: the snapshot wave starts the scan's reads (and the exact reads of flat segments) as soon as the directory is
loaded, in parallel with the WAL tail read, for a lone unfiltered ANN query. The query's own scan then finds them in the block
cache. A tail that shadows rows only wastes those bytes.

Two smaller changes: the scan wave no longer warms every probed segment's documents footer (70 requests and 10 MB; the few
segments with likely winners read it in the speculative document read), and the reranks run on the async runtime instead of
on the CPU workers (they are small reads, and holding one of eight worker permits through each read's latency queued them).
