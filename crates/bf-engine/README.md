# Segment engine 🐡

`Engine::new(Storage)` opens an independent node and starts a background indexer.
`Engine::with_config(Storage, EngineConfig)` controls flush size, flush age,
compaction, retention, backpressure, and query tuning. Set `indexing: false` for
explicit maintenance or a WAL-only reference deployment. `index_namespace(ns,
force)` flushes a tail and compacts an eligible size tier; `collect_garbage(ns)`
reclaims retired objects after the retention window.

A namespace handle retains segment descriptors and the unindexed WAL overlay.
Cold nodes load a manifest checkpoint and replay only subsequent WAL entries.
Point writes read affected IDs from the sorted ID index and bloom filter; patches,
conditions, and deletes use the same overlay. By-filter writes and schema drops
materialize a temporary snapshot. The reference query evaluator remains available
and currently materializes a temporary query snapshot from segment column blocks.

Segments use immutable ULID directories. Documents are columnar in 256-row blocks;
vector columns contain little-endian f32 or f16 values. Every block has a CRC32,
and each file ends with a checksummed directory and versioned 32-byte footer. The
manifest records object and metadata lengths, so a reader can fetch the footer and
directory together. Scalar/array value dictionaries are sorted and have Roaring
row bitmaps. ID dictionaries include a bloom filter. Live rows have independently
published immutable generations. Compaction merges segments in logarithmic size
tiers and rebuilds their files.

Indexers claim an epoch by occupying the next create-only manifest slot. Every
publication names exactly its predecessor and epoch; a newer claimant occupies
the slot a stalled publisher would need, fencing it. Advisory create-only leases
reduce duplicate work. This implements the needed transaction-object protocol
directly, reusing the WAL's conditional-create identity handling instead of adding
a second storage abstraction. Indexer and WAL sequence numbers are independent.

Manifest slots contain small references to immutable manifest payloads. Retired
payloads, old live generations, compacted segment files, and checkpointed WAL
payloads are reclaimable after publication-time grace periods. Permanent WAL and
manifest slot records are retained as CAS fences. Retention defaults to one hour;
operations should complete inside that window. Namespace deletion commits the M1
tombstone, so stale readers cannot resurrect pre-deletion documents.

Writes return 429 once the logical unindexed WAL exceeds the configured limit
(default 2 GB). Eventual snapshots bound included tail bytes at 128 MiB. Thresholds
can also be set through `BF_INDEX_ROWS`, `BF_INDEX_AGE_MS`,
`BF_INDEX_INTERVAL_MS`, `BF_MAX_UNINDEXED_BYTES`, and `BF_ANN_NPROBE` without changing
the HTTP binary. A compatibility run with all three index thresholds set to one
passed **96/96** and created segment files on disk.

Tests preserve all M1 semantics and compare deterministic mixed workloads with a
separate WAL-only reference node. They exercise patches, deletes, conditional and
by-filter writes, vector/text ranks, expressions, aggregations, RRF, compaction,
GC, and cold reopen after WAL reclamation. Fault injection fails each persistent
mutation before and after its durable effect, covering commit, indexing,
compaction, and GC. Acknowledged writes survive recovery exactly once.

Vector columns now also have `.bfv` indexes built with bf-vector. ANN searches
segment-local filtered live rows, reranks candidates, scores the tail exactly,
and merges through the reference result assembler. kNN is exact. The default is
128 probes and a rerank budget of 64 times k; both are configurable. Large index
builds and query scoring run on blocking executors. Projection fetches only needed
vector columns. A zero cosine query retains M1's all-distance-one semantics.

`query_exhaustive` exposes the reference path for differential testing. `_debug/recall`
compares ANN and exact searches on one snapshot and returns optional ground-truth
rows. Independently, `recall_sample_rate` defaults to 0.01 and records background
exact comparisons in the `blowfish_ann_recall` histogram through `metrics`.
A 5,000-row randomized IVF test requires mean recall@10 >= 0.95, compares filtered
kNN with exhaustive results, and verifies that f16 indexes persist f16 elements.
