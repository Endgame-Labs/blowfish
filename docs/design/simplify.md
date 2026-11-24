# One way to do each thing: what to delete

Status: proposed (2026-11-23). Applies SPEC decision 25: when an approach is better, it is the only one. No
setting selects an inferior algorithm or layout, and no code is kept to maintain one. Old on-disk formats are read
only to be upgraded, automatically, and there is no path back.

This is an audit of `main` at the time of writing: environment variables, settings, `examples/` tools, and code
paths. Each is classified as **delete** (it selects or maintains an inferior approach), **upgrade-only** (it reads
an old format, so it moves into an upgrade module and the hot path stops branching on it), **constant** (a tuning
knob with no reason for an operator to turn it; fold it into code), or **keep** (capacity, operations, diagnostics,
tests, or API compatibility).

## A safety prerequisite: a manifest fence

Deleting formats is only safe if an older binary can't act on a newer bucket.

Today nothing stops it:
- The manifest's `format_version` is checked only by `snapshot.rs`, on the query path.
- `collect_garbage`, `index_namespace`, `recluster`, and branch code read the manifest with plain serde, which
  ignores unknown fields.
- There is no bucket-level format marker.

The consequences:
- An older writer republishes manifests without fields it doesn't know (slab tiers, lineage, new file kinds).
- An older GC sweeps the objects those fields reference, once they pass the retention window.
- After `disk-footprint.md` phase D4, those objects hold the only copy of some vectors.

The fix, in the first release that changes a format:

1. **Every manifest consumer checks the version.** Bump `format_version` to 2 and check it in one function that
   every reader calls (`Wal::latest_manifest` itself). Unknown versions fail closed.
2. **Released binaries also fail closed.** Binaries already released don't check the version, so version-2
   manifests also drop or rename one field they require (for example, `indexed_through` moves inside a new
   `checkpoint` object). A 0.1.13 or 0.1.14 binary then fails to decode, and its GC and indexer log "deferred" and do
   nothing. Its queries return 500 rather than wrong results.
3. **A bucket marker.** Write `_system/format/<n>.json` (create-only, one per format generation). A binary reads the
   highest at startup and refuses to start as a writer if that is newer than it knows.

This is round S2 below, and the disk-footprint and auto-recluster work depends on it.

## Settings and environment variables

| Variable or setting | What it does | Verdict |
|---|---|---|
| `BF_ANN_FASTSCAN=0` | builds the row-major code layout | **delete**. The row-major reader becomes upgrade-only |
| `blowfish reindex` target-layout flag (in progress) | rewrites indexes back to the old layout "to keep a downgrade path" | **delete** before merge. Reindex becomes the automatic upgrade pass below; a one-shot `blowfish upgrade` that runs the same pass is fine (a read-only deployment needs it) |
| `BF_ANN_DIRECTORY=0` | forces the per-segment ANN path | **delete**, along with the per-segment path itself once every segment is in a directory (S5) |
| `BF_ANN_BLOCKS=0` | plain directory layout for unfiltered queries | **delete**. The plain and block layouts merge into one (`disk-footprint.md` D1) |
| `BF_ANN_SLABS=0` | ignores slabs | **delete** |
| `BF_ANN_PAGED=0` | reads the whole flat slab directory | **delete** |
| `BF_ANN_PAGED_MIN_ENTRIES` | the size below which no paged copy is written | **delete**. Paged becomes the only directory format at every size (`graph-index.md`) |
| `BF_ANN_SCAN_MEMO=0`, `BF_ANN_SPECULATE=0` | turn off prefetch-time scanning and speculation (marked "A/B only") | **delete** |
| `BF_READER_COPY=0` | reads the full manifest slot for ANN queries | **delete** |
| `BF_READER_COPY_MIN_BYTES` | threshold for the compact copy | **constant**, until the split manifest (S6) replaces both copies |
| `BF_RECLUSTER_GLOBAL` | per-cell versus cross-cell leaf assignment | **delete** once round 21's two-level assignment lands. Cross-cell becomes the only assignment |
| `BF_RECLUSTER_GROUPS`, `BF_RECLUSTER_REFINE` | assignment search width and refinement rounds | **constant** |
| `BF_IVF_CLUSTER_FACTOR` | larger per-segment clusters (an old A/B) | **delete** |
| `BF_ANN_SEGMENT_SPREAD` | exponent of the as-loaded probe multiplier (`0` turns it off) | **constant** 0.3. It matters less once automatic recluster covers most rows |
| `BF_ANN_PAGE_MARGIN`, `BF_ANN_PAGE_KAPPA`, `BF_ANN_PAGE_ENTRIES` | paged directory tuning | **constant**. Revisit with the center graph |
| `BF_NO_AVX512` | forces the scalar scan on AVX-512 machines | **delete** the variable. Keep runtime CPU detection, and keep the scalar path, which is what non-AVX-512 and non-NEON CPUs run and what tests compare SIMD against |
| `BF_HEDGE_BUDGET` (`0` disables hedging) | hedged ranged reads | **keep** as a number. Hedging is not an inferior algorithm, and stores differ |
| `ann_nprobe`, `ann_rerank_factor`, `BF_EXACT_FETCH` | recall and latency trade-offs per deployment | **keep** (the turbopuffer-style recall knob and its relatives) |
| Memory and capacity: cache sizes, `BF_DECODED_CACHE_BYTES`, `BF_MAX_SEGMENT_ROWS`, `BF_MAX_UNINDEXED_BYTES`, `BF_BULK_FLUSH_*`, `BF_DEFER_COMPACTION_SEGMENTS`, `BF_BUILD_CONCURRENCY`, `BF_QUERY_CONCURRENCY`, `BF_SEARCH_CONCURRENCY`, `BF_FANOUT_QUERIES`, `BF_SPARSE_BLOCK_BYTES`, `BF_READ_GAP_KB`, `BF_WAL_PIPELINE`, `BF_WARM_CONNECTIONS`, `BF_WARM_FLOOR`, decay (`BF_BUSY_DECAY_MS`, `BF_IDLE_DECAY_MS`, `BF_DECAY_LINGER_MS`) | sizing for the machine | **keep**. Several could become constants later; none selects an algorithm |
| Indexing cadence (`index_rows`, `index_bytes`, `index_age_ms`, `index_interval_ms`, `compaction_segments`, `retention_ms`, `lease_duration_ms`) | | **keep** |
| `storage.single_writer`, `--read-only` | stores without conditional writes; query-only replicas | **keep**. These are deployment modes, not alternative algorithms |
| Diagnostics: `BF_TRACE_STAGES`, `BF_TRACE_READS`, `BF_HEAP_DUMP_*`, `BF_MEM_LOG_SECS`, `BF_RECALL_SAMPLE_INTERVAL_MS` | | **keep** |
| Test and bench only: `BF_TEST_STORAGE_*`, `BF_CACHE_BENCH_*`, `BF_VECTOR_REAL_*`, `BF_SCALE_TIMEOUT_SECONDS`, `BF_MODELS_TEST_UNSET_KEY` | | **keep** |
| Automatic recluster | always on (`auto-recluster.md`) | no setting, by design |

## Tools in `examples/`

| Tool | Verdict |
|---|---|
| `bf-engine/examples/recluster.rs` | **delete** when automatic recluster ships (`auto-recluster.md` R2) |
| `bf-engine/examples/repage.rs` | **delete**. A flat slab directory is an old format, and the upgrade pass (or the automatic rebuild) pages it |
| `bf-engine/examples/readercopy.rs` | **delete**. The next publication writes the compact copy; a namespace that never publishes again gets it from the upgrade pass |
| `bench.rs`, `scale_query.rs`, `scale-server`, `scale_*.py`, `bf-storage` and `bf-text` benches, `bf-wal/examples/latency.rs`, `bf-models` probes | **keep** (measurement harness) |

## Code paths

| Path | Verdict |
|---|---|
| Row-major code layouts in `bf-vector` (non-compact 24-byte headers, compact row-major codes, unrotated centers) | **upgrade-only**. Move the readers into `bf-vector::upgrade`, so `Index` scans only the fast-scan layout |
| Plain fragments (`.bff`) and the block fragments' copy of the codes (`.bfb`), and `Layout::Plain` versus `Layout::Blocks` | **delete** after the single ANN object (`disk-footprint.md` D1). Old fragments are upgrade-only |
| `segment::backfill_blocks` | already an automatic upgrade. It folds into the general upgrade pass, then goes when D1 replaces block fragments |
| The per-segment ANN search in `ann::search` (the path when no directory answers) | **delete** once every published segment set has a directory (always true for new publications), with the upgrade pass building missing ones |
| Flat slab directory writer and reader, plus the hint of the whole directory | **delete** the writer. The reader becomes upgrade-only (S4) |
| "Slabs only while they cover exactly the live segments", and "retire slabs when a covered segment is gone" | **replace** with one plan over tiers and uncovered segments, plus lineage (`auto-recluster.md` R1 and R4) |
| `Engine::recluster`'s epoch claim and `page_slab_directory` | **delete** (`auto-recluster.md` R2) |
| Segments without `idfilter.bff` (the old ID-check path through the Bloom filter in `ids.bfi`) | **upgrade-only**. The upgrade pass writes the filter, and the Bloom filter inside `ids.bfi` is dropped (`disk-footprint.md` D3) |
| Text segment versions 1 to 3 (`bf-text/src/segment.rs` reads 1, 2, 3, and 4) | **upgrade-only**. The upgrade pass rebuilds an old text index from the segment's documents, and the query path reads version 4 only |
| Full manifest slot plus the compact `manifest-latest` copy for ANN readers | **replace** both with the split manifest (a small root plus segment-list chunks), an open item in SPEC §5. Until then, keep both, without the switch |
| Manifest fields read only for old manifests (`ann_slabs` after `ann_tiers`, payload-pointer slots) | **upgrade-only**. The next publication rewrites them |
| `blowfish/src/legacy.rs` (the pre-v2 turbopuffer wire protocol) | **keep**. It's part of the turbopuffer API that SDKs still call (SPEC §2), not an internal alternative |
| Scalar SIMD fallbacks in `bf-vector` | **keep**. They're the only path on CPUs without AVX-512 or NEON, and the reference in tests |

## The upgrade pass

The indexer runs one background pass per namespace, after indexing and compaction and before recluster, one segment
at a time. It holds the same lease and publishes through the same fenced cycle as compaction. It handles each old
format in the tables above:
- code layout to fast-scan (the reindex work, minus its target flag);
- text version 1 to 3 to version 4;
- missing `idfilter.bff`;
- missing block fragments or directories;
- flat to paged slab directory;
- later, the single ANN object and `docs.bfc` version 2.

A metric reports namespaces and bytes still in old formats.

Once a release has shipped where every format older than N is upgraded on sight, the next release may delete the
upgrade readers for formats older than N. The release notes say so, and a binary refuses to start on a bucket whose
marker is older than it can upgrade. Until then, old readers live only in upgrade modules, and no query path
branches on format.

## Order

| Round | Work | Why this order |
|---|---|---|
| S1 | Delete the A/B-only switches: `BF_ANN_FASTSCAN`, `BF_ANN_DIRECTORY`, `BF_ANN_BLOCKS`, `BF_ANN_SLABS`, `BF_ANN_PAGED`, `BF_ANN_SCAN_MEMO`, `BF_ANN_SPECULATE`, `BF_READER_COPY`, `BF_IVF_CLUSTER_FACTOR`, and `BF_NO_AVX512`. Fold the constants. Drop reindex's target-layout flag. Update `docs/configuration.md`. | No format changes and no risk; it shrinks what later rounds have to touch |
| S2 | Manifest fence and bucket marker. | Every later deletion and format change depends on it |
| S3 | The upgrade pass, holding today's old-format readers in upgrade modules: code layouts, text versions, ID filters, block fragments. `blowfish upgrade` runs the same pass once. | Puts old formats behind one door before new formats arrive |
| S4 | With automatic recluster R2: delete `recluster.rs`, `repage.rs`, and `readercopy.rs`, the recluster epoch claim, and the flat slab directory writer. Delete `BF_RECLUSTER_GLOBAL` once round 21 lands. | Those tools become redundant only when the job exists |
| S5 | With disk-footprint D1: delete `.bff`, `.bfb`, the two directory layouts, `backfill_blocks`, and the per-segment ANN search path. | Needs the single ANN object |
| S6 | Split manifest replaces the full slot and the compact copy. | Its own design, already open in SPEC §5 |
| S7 | After one release with S3 shipped: delete upgrade readers older than the marker's minimum. | Gives every deployment one release in which to upgrade |
