# Segment engine 🐡

The engine stores immutable segment indexes in object storage and keeps only
segment descriptors and the unindexed WAL overlay resident. A cold node opens the
latest manifest and replays entries after `indexed_through`. Queries and writes
resolve the same snapshot: live rows in segments plus a newest-wins tail.

## Public API

`Engine::new(Storage)` starts background indexing, namespace discovery, and GC.
`Engine::with_config(Storage, EngineConfig)` accepts explicit tuning; disable
`indexing` for deterministic maintenance or a WAL-only reference node.

| Method | Behavior |
| --- | --- |
| `write` | Validate, group, and durably commit an atomic request; revalidate after a competing WAL slot |
| `query` | Search segment indexes and the tail, including multi-query and RRF |
| `query_exhaustive` | Materialize the same snapshot and run the exhaustive reference evaluator |
| `index_namespace(ns, force)` | Flush a tail, rebuild changed text indexes, and compact an eligible size tier |
| `collect_garbage(ns)` | Fence stalled builders, retire deleted namespaces, and reclaim eligible files |
| `recall` | Compare ANN and exact results on one snapshot, optionally returning ground truth |
| `metadata`, `schema`, `update_schema` | Inspect exact counts, inspect settings, and commit schema changes |
| `delete_namespace`, `list_namespaces`, `warm`, `explain` | Namespace lifecycle, prefetch, and plan inspection |

Point writes use sorted ID dictionaries and bloom filters to fetch affected rows.
By-filter writes and namespace copies use temporary materialized snapshots.
Schema drops commit metadata without copying surviving vectors into the WAL;
background rebuilds process one segment at a time. Metadata adjusts checkpoint counts using only tail-shadowed IDs.

## Immutable formats and publication

Each segment lives under `ns/<namespace>/segments/<ulid>/` and records its WAL range,
physical row count, build-time schema, object lengths, and metadata lengths.

| File | Contents |
| --- | --- |
| `docs.bfc` | Columns in 256-row blocks; vectors are separate binary f32/f16 column blocks |
| `ids.bfi` | Sorted global IDs and a bloom filter |
| `filter.bfx` | Sorted value dictionaries and Roaring row bitmaps; scalar ranges use binary seeks |
| `ann.<column>.bfv` | bf-vector IVF/RaBitQ indexes and exact rerank vectors |
| `fts.<column>.bft` | bf-text term dictionaries, postings, lengths, and phrase positions |
| `live.<generation>.bfl` | Independently published live-row masks |

Engine-owned files have per-block CRC32 checksums and a checksummed, versioned
32-byte footer. The directory and footer are fetched together. The `.bfv` and `.bft`
formats retain their libraries' independent versions and checksums. Corrupt or
unsupported files fail closed.

Indexers claim an epoch by occupying the next create-only manifest slot. A publish
names exactly its predecessor and epoch. A newer claimant occupies the slot a
stalled publisher needs, so that publisher cannot advance. Advisory create-only
leases reduce duplicate work. This implements the small transaction-object
protocol directly, reusing conditional-create retry identities rather than adding
a second storage layer.

Manifest slots contain tiny references to immutable payloads. GC retains WAL and
manifest slots permanently, preserving their CAS fences. Small WAL entries inline
their payload, which GC atomically redacts while retaining slot occupancy and retry identity. Retirement grace
periods begin at publication, including for WAL checkpoints. GC inventories orphan
segment files before claiming a newer epoch, fencing their paused builders before
deleting them. Tombstone checkpoints retire deleted namespaces' segments. Empty
segments are retired immediately, with physical deletion after retention.

New WAL payload keys identify their attempted slot. Abandoned payloads can therefore
be reclaimed once that slot is checkpointed and permanently occupied. Legacy
UUID-only payloads remain readable; unreferenced legacy payloads cannot safely be
classified by attempted slot and are conservatively retained.

## Query execution

ANN passes filter/live bitmaps to each segment index, reranks candidates, and scores
the tail exactly. kNN is exact. Default ANN effort is 128 probes and 64 times k
reranking, matching bf-vector's tested defaults. Index builds and query scoring use
blocking executors. Projection fetches vector columns only when requested. A zero
cosine query preserves M1's all-distance-one behavior.

BM25 and token filters use bf-text posting readers. Corpus statistics are global:
checkpoint totals exclude deleted rows, queries subtract tail-shadowed segment
rows, and then add the tail. Missing/null text is excluded; present empty text
counts with normalization length one. Tokenizer changes rebuild indexes;
transitional queries analyze stored columns with the current settings. Score
expressions reuse indexed per-clause BM25 scores and the exact expression evaluator.

`bf-types::FullTextConfig` is the shared bf-text parameter type. All documented
tokenizers, including CJK dictionaries and pre-tokenized arrays, use its validation.
The temporary `text_fallback` module has been removed. bf-text's documented oracle
exceptions for historical Unicode versions, Greek stemming, and a stopword/subword
edge case still apply.

Scalar order-by scans one 256-row column block at a time and retains bounded top-k
winners, including diversification. Aggregations retain accumulator states rather
than documents; group scans retain only the smallest requested group keys and
expand array products incrementally. General score expressions and by-filter
writes still materialize eligible rows.
The exhaustive diagnostic path deliberately materializes the full snapshot.

## Tuning and integration

`EngineConfig` defaults are 1,000 distinct tail mutations or 8 MiB of logical WAL,
a five-second maximum tail age, a 250 ms poll interval, four segments per size
tier, a 30-second advisory lease, and one-hour retention. Writes return 429 at
2 GB of unindexed logical WAL. Eventual queries include at most 128 MiB; if even
the first atomic entry exceeds the cap before a checkpoint exists, they return
retryable 409. Eventual snapshots refresh before half the retention window.

Environment overrides recognized by `Engine::new` are `BF_INDEX_ROWS`,
`BF_INDEX_AGE_MS`, `BF_INDEX_INTERVAL_MS`, `BF_MAX_UNINDEXED_BYTES`, and
`BF_ANN_NPROBE`. Remaining settings use `EngineConfig`. The HTTP/config crate can
expose these directly without changing the engine API.

`recall_sample_rate` defaults to 0.01. Sampled ANN queries run an exact comparison
in the background and record `blowfish_ann_recall` through the metrics facade.
Install a recorder in the binary to export it.

`segment::ObjectReader` adapts both index libraries to bf-storage's known-length
range cache. Segment searches run concurrently on bounded blocking workers.
Independent metadata reads start together, and ANN batches code and rerank ranges.
See the M5 investigation below for cache budgets and the remaining cold BM25 limit.

## Validation

Run `cargo fmt`, `cargo clippy -p bf-engine -p bf-wal -p bf-types --all-targets -- -D warnings`,
and `cargo test -p bf-engine -p bf-wal -p bf-types`.

Tests preserve M1 semantics and compare mixed write/query workloads with a separate
WAL-only reference node. Coverage includes conditional and by-filter writes,
scalar and text filters, vector ranks, score expressions, aggregates, groups,
diversification, RRF, projection, compaction, GC, and cold reopen after WAL pruning.
Additional tests cover cross-node conditional writes during indexing, same-checkpoint
compaction refresh, f16 files, CJK tokenizers, phrase positions, empty/missing text,
schema rebuilds, corruption, eventual caps, orphan files, and namespace recreation.
A 5,000-row IVF test requires mean recall@10 of at least 0.95.

Fault injection fails each normal-path read, listing, create, and delete before or
after its effect, including lost successful PUT responses. Every acknowledged
write survives recovery exactly once through commit, indexing, compaction, and GC.

The unchanged compatibility suite passes **111/111**, with one-row and one-millisecond
index thresholds and persisted segment files verified. Official Python and
TypeScript SDK write, ANN, BM25, and metadata smokes also pass.

## Million-document benchmark

```sh
RAYON_NUM_THREADS=8 cargo run --release -p bf-engine --example bench -- \
  --rows 1000000 --dims 768 --batch 5000 --queries 20 --recall-queries 10 \
  --compaction-segments 4 --url file:///tmp/blowfish-bench
```

The example creates a fresh namespace, writes bounded batches, waits for indexing,
and drains eligible compactions before measuring queries. Defaults match the command
above. Set `--compaction-segments 1000000` to retain the 200 small input segments.
`--nprobe` controls ANN effort. Results are emitted as JSON; the store remains on
disk for inspection.

Measured on 2026-11-14 in release mode on an 8-vCPU, 32-GiB Linux worker with an
AMD EPYC 9354P and eight rayon threads. The corpus is one million f32 vectors of
768 dimensions, distributed across 256 synthetic clusters, with four text tokens
per document drawn from 4,096 varying terms and two common terms. Queries perturb
stored vectors. Both runs use top-10 results, 128 probes, and 64 times k reranking.

| Measurement | Default tiered compaction | 200 input segments retained |
| --- | ---: | ---: |
| Live segments | 5 | 200 |
| Write throughput, excluding generation and indexing | 3,898 rows/s | 3,430 rows/s |
| Write time | 256.56 s | 291.57 s |
| Indexing and compaction time | 271.94 s | 130.55 s |
| Total load wall time | 559.27 s | 460.22 s |
| ANN p50 / p99 | 238.25 / 275.95 ms | 4,400.25 / 5,888.89 ms |
| Mean recall@10 | 0.99 | 1.00 |
| BM25 p50 / p99 | 113.22 / 157.63 ms | 826.92 / 4,669.86 ms |
| Cold engine open | 9.82 ms | 9.47 ms |
| Retained segment bytes | 25,668,273,320 | 6,706,241,019 |

Latency uses three warm-up queries followed by 20 timed samples; p99 is the largest
of those 20 samples, not a sustained service-tail estimate. Recall averages ten
same-snapshot ANN/exact comparisons. The synthetic corpus is easier than general
embeddings. The shared worker was not isolated during ingestion.

Cold opening measures a new engine loading the manifest and empty tail, with the
OS page cache retained; it does not include a first ANN query. Retained bytes
include compacted inputs inside the default one-hour GC window. This run used the
uncached storage adapter, so it also exposes the cost of per-segment range calls.

Copies and branches share immutable segment files within the same bucket. Only the source's unindexed mutations are copied into the destination WAL. Immutable segment pins and a copy descriptor become durable before the destination WAL CAS, so a cold reader can recover the same references after a crash. New live-row masks belong to the destination namespace. GC rereads cross-namespace pins after claiming its epoch, which protects files inherited by branches, including branches of branches.

Segment pins are currently conservative: obsolete pins remain after compaction, a failed copy, or namespace deletion. This can retain storage indefinitely. Reclaiming them requires proving both that no committed namespace references the files and that no paused copier can still commit its pin. Until that protocol exists, GC favors retention over deleting a referenced segment.

Late interaction accepts `[][N]f32` columns and nested query-vector arrays with `ANN` or `kNN`. It requires cosine distance and ranks by the sum, over query vectors, of the minimum distance to any document vector. Empty document-vector arrays are omitted from ranked results. Immutable segments support this representation, but late-interaction candidate search remains exhaustive. Vector arrays accept and return JSON arrays; base64 vector-array encoding is unsupported.

## M5 performance investigation

The benchmark accepts `--reuse <namespace>` to query an existing corpus, and
`--recall-queries 0` to isolate query profiling from the exhaustive recall scan.
`--consistency eventual` separates index execution from strong snapshot refresh.
First-query measurements use a newly opened storage handle, so both engine and
byte caches start empty; the operating system page cache remains uncontrolled.
S3 runs take `BENCH_ENDPOINT`, `BENCH_PROFILE`, and `BENCH_REGION` from the
benchmark environment. Keep private deployment settings out of this document.

### WAL commit safety

Entries whose uncompressed JSON is at most 64 KiB now commit with one conditional
PUT, carrying the payload inside a version-three slot. Larger entries retain the
two-PUT external-payload path. Readers accept legacy version-two slots.

GC replaces an inline slot with an identical final redacted record, preserving
its key, write identity, sequence, and timestamp. The overwrite is atomic, and
never exposes an absent key: a stalled create-only writer therefore still loses.
Concurrent collectors compute the same redacted bytes; neither can restore data.
An ambiguous overwrite is resolved by reading back those bytes. A retry of the
original create still recognizes its identity without restoring the payload.
Redacted reads return `Pruned`, using the existing checkpoint/tombstone recovery
path. WAL reads bypass immutable range caches. Tombstones contain no payload.

This extends the immutable-record protocol to **permanent slot occupancy with a
single monotonic redaction transition**. All nodes reading a prefix must be
upgraded before version-three writes begin; old binaries fail closed on the new
slot version. Tests cover concurrent collectors, losing writers, original-write
retries, lost successful redaction responses, namespace recreation, legacy slots,
large external payloads, and the engine's crash-injection matrix.

Profiling the retained million-row corpus with Linux `perf record -F 499
--call-graph dwarf,8192` found ID deserialization at 4.99% of sampled cycles,
JSON sequence checks at 3.51%, integer parsing at 2.73%, byte-vector decoding at
2.44%, and number parsing at 2.31%. RaBitQ estimation accounted for 4.27%, and
query-table preparation for 3.55%. Repeated metadata decoding and per-range
HEAD/header probing therefore preceded kernel tuning. Sampling perturbs timings;
profiled latencies are not used as benchmark results.

Immutable reads now use `Storage::range_reader_with_length` and batched cache
reads. `StorageConfig.cache` controls byte-cache memory, disk, block size, and I/O
concurrency. `EngineConfig.decoded_cache_bytes` separately bounds a node-local LRU
of decoded directories, IDs/blooms, live masks, filter dictionaries, IVF metadata,
and text readers. Its default is 512 MiB, with conservative serialized-size charges
of 16 times metadata bytes (32 times filter dictionaries). Active queries can pin
evicted entries until they finish. Zero disables decoded admission. Cache keys
include immutable object generations; storage handles and decoded caches must not
be shared across unrelated backends. Performance fields report per-query logical
bytes, including decoded metadata hits, instead of guessing from namespace age.

`query_concurrency` and `search_concurrency` each default to eight. Query drivers
and segment scorers use bounded blocking tasks, keeping CPU work off Tokio reactor
threads. Independent masks, IDs, and index metadata open together; ANN code reads
and rerank vectors each use one coalesced batch. The counting-store regression
requires exactly three dependent cold ANN read stages, zero HEADs, and zero backend
reads for an identical warm query. That test uses 512 fetch permits to isolate
read dependencies; the production default of 16 permits can introduce additional
network waves inside a batch. It also tests a cold single-term BM25 query within
three stages. General BM25 queries can still need additional posting-block stages:
bf-text exposes demand-driven posting reads, not a public batch-prefetch plan.


### M5 benchmark results

Measured on the same retained **1M × 768** local corpus and five live segments as
the M4 table above. Both runs use 64-times-k reranking, top ten, three warm-ups,
50 measured queries, ten exact recall comparisons, and 64-KiB cache blocks.
The engine's default remains 128 probes; `BF_ANN_NPROBE=32` (or `--nprobe 32` in
the benchmark) meets the requested warm target on this synthetic corpus.

| Local file store | M4, 128 probes | M5, 128 probes | M5, 32 probes |
| --- | ---: | ---: | ---: |
| ANN p50 / p99 | 238.25 / 275.95 ms | 46.96 / 60.14 ms | **23.42 / 33.14 ms** |
| BM25 p50 / p99 | 113.22 / 157.63 ms | 8.62 / 14.19 ms | **8.16 / 16.33 ms** |
| Recall@10 | 0.99 | 0.99 | **0.99** |
| ANN execution-only p50 | not separated | 43 ms | 19 ms |
| BM25 execution-only p50 | not separated | 5 ms | 4 ms |

Independent cold engine and storage handles measured **102.55 ms ANN** and
**68.24 ms BM25** at 32 probes, including namespace discovery. The OS page cache
was retained. Cold BM25 now runs before the warm workload on a separate storage
handle, so it does not inherit ANN's ID, live-mask, or byte-cache entries. A run
concurrent with remote compaction measured 71.14 ms ANN p50; the final table was
measured after that load finished. These are workload measurements on a shared
worker, not isolated hardware guarantees. The default 128-probe ANN run still
exceeds the 30-ms target; probe tuning supplies the documented speed/recall tradeoff.

The **100k × 768 R2** run used four 25,000-row input batches, compacted to one live
segment, 64 probes, 64-KiB cache blocks, three warm-ups, 20 measured queries, and
five recall comparisons. Warm denotes the post-warm-up phase with varying vectors
and terms, not a guarantee that every rerank/posting range is resident. All queries
used strong consistency. Only a unique
`dev/perf-bench/` prefix was used, and all its objects were deleted afterward.

| R2 measurement | Result |
| --- | ---: |
| Cold ANN / BM25, independent caches | 1,671.31 / 859.99 ms |
| Warm ANN p50 / p99 | 465.52 / 732.43 ms |
| Warm BM25 p50 / p99 | 190.30 / 293.60 ms |
| ANN / BM25 execution-only p50 | 344 / 65 ms |
| Recall@10 | 1.00 |
| Load wall time / indexing and compaction | 270.85 / 181.77 s |
| Write throughput, excluding generation and indexing | 1,188 rows/s |
| 25,000-row write batch p50 | 20,186.59 ms |

An earlier R2 attempt returned a storage 503 during compaction after ingestion.
It was cleaned up; a fresh run with error diagnostics enabled completed without
reproducing the failure. Both attempts' prefixes were deleted. The successful
cold results include strong refreshes and bounded I/O concurrency; they are not
three bare network round trips. Achieving that wall-clock budget for arbitrary
queries still needs bf-text posting-batch planning and sufficiently wide fetch
concurrency. bf-vector's interleaved code/vector layout also limits coalescing
across distant clusters.

Small durable writes were measured separately with
`cargo run --release -p bf-wal --example latency`, using the same environment
variables plus `BENCH_URL` for a disposable prefix. Each measurement excludes
three warm-ups and reports the median of 20 document-bearing commits.

| R2 small-write durability measurement | Two-PUT baseline | Inline slot |
| --- | ---: | ---: |
| Single conditional-PUT control p50 | 509.80 ms | 173.76 ms |
| WAL commit p50 | 1,016.84 ms | **183.73 ms** |
| Commit / contemporaneous PUT control | 1.99× | **1.06×** |

Conditions changed between runs, so the absolute improvement cannot all
be attributed to the code. The control ratio and the slot/payload regression tests
show the removed round trip. These are WAL durability latencies, excluding HTTP,
validation, and indexing; large batches above the inline threshold still use two PUTs.


The same small-write example on the local file store measured **0.061 ms WAL
commit p50**, against **0.053 ms** for its single-PUT control (20 samples, after
three warm-ups, with the OS cache retained). The million-row local corpus was
reused for query comparison; its original ingestion-throughput figures above
remain M4 measurements.
