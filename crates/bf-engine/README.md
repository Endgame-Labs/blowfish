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
manifest slot records permanently, preserving their CAS fences. Retirement grace
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

Order-by, aggregates, and general score expressions currently materialize eligible
scalar rows for reference result assembly. Streaming scalar result merging and
bounded by-filter materialization remain performance improvements.
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

`segment::ObjectReader` isolates range access for both index libraries. This
worktree's storage interface supplies `Storage::get_range`; when merging the cache
worker, connect this adapter to its cached range reader. Current range calls can
include backend HEAD/header requests, and segment searches execute sequentially,
so this implementation does not yet meet a three-round-trip cold-query budget.

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

The unchanged compatibility suite passes **96/96**, with one-row and one-millisecond
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
