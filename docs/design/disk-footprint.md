# Disk footprint: one copy of every vector

Status: proposed (2026-11-23). Answers SPEC decision 24 ("disk footprint (vectors stored twice)"). Follows
decision 25: one layout, reached by automatic upgrade.

## The problem, in numbers

The last full inventory (10M Wikipedia, 1,024-dim f16, iteration 10, before block fragments and before
recluster) measured 56.25 GB, 5,625 bytes a row: 2,465 bytes in `docs.bfc` and 2,216 in `ann.*.bfv`. Since then
each segment gained two more ANN objects, and recluster added slabs that hold yet another copy. Vectors are no
longer stored twice. On a reclustered namespace, **every vector is stored three times and every one-bit code four
times.**

Where every byte of a row goes today, from the format code (`bf-vector/src/format.rs`, `fastscan.rs`,
`directory.rs`; `bf-engine/src/segment.rs`, `recluster.rs`). The setting is 1,024 dims, f16, 200,000-row
segments (447 clusters of about 446 rows), and 200-row slab leaves:

| Object | What it holds per row | Bytes a row |
|---|---|---:|
| `docs.bfc`, vector column | presence byte + f16 vector | 2,049 |
| `docs.bfc`, other columns | id, title, text, and so on, as uncompressed JSON per 256-row block | 416 |
| segment `ann.N.bfv` | fast-scan code (128 bits + four f32 factors = 144), f16 vector (2,048), row CRC (4), roaring row ID (about 2), the cluster's f32 center shared by its rows (about 9) | 2,207 |
| segment `ann.N.bff` (plain fragment) | each cluster's exact center, row set, and row CRCs again, plus a u16 row-to-cluster map | 18 |
| segment `ann2.*.bfb` (block fragment) | the same cluster blobs again, **plus the codes again** | 160 |
| slab `ann.*.bfv` | **the codes and the vector again**, with CRC, roaring, and the leaf center at 200 rows | 2,219 |
| slab `ann2.*.bfb` | the slab's codes again, with its blobs | 171 |
| `fts.*.bft`, `filter.bfx`, `ids.bfi`, `idfilter.bff`, live masks | text, filters, and IDs (10M inventory) | 928 |

Checks against measurements: segment `.bfv` computes to 2,207 bytes a row against 2,216 measured at 10M, and
the slab objects to 2,390 against 2,380 measured at 100M (238 GB for 100M rows).

| Total | Bytes a row | 10M | 100M |
|---|---:|---:|---:|
| As loaded (no slabs) | 5,777 | 57.8 GB | 578 GB |
| Reclustered | 8,167 | 81.7 GB | 817 GB |
| Of that, vector and code bytes | 6,824 | 68.2 GB | 682 GB |
| The minimum for the same vectors (one f16 copy, one code, its center) | about 2,215 | 22.2 GB | 222 GB |

One f16 copy of the 10M vectors is 20.5 GB. The redundant vector and code bytes on a reclustered 10M namespace are
about 46 GB, and about 460 GB at 100M. The 100M prefix measured 945 GB after recluster, because retired segments
were still inside their retention window. Text, filters, and IDs are taken at the 10M per-row figures. The 100M
corpus is multilingual, so its text bytes differ somewhat.

Why each copy exists:

- `docs.bfc` vectors: compaction reads vectors from here (`read_packed`), and so does returning the vector
  attribute.
- Segment `.bfv` vectors: reranking for queries on the per-segment and directory paths, and recluster's input.
- `.bfb` codes: the block directory layout wants one ranged read per probed cluster (blob and codes together),
  so it copies the codes out of `.bfv`. `.bff` exists because filtered queries need its row-to-cluster map, which
  `.bfb` lacks.
- Slab `.bfv` and `.bfb`: the reclustered layout, built beside the segments and never replacing them.

## Options considered

1. **Drop the vector column from `docs.bfc`; vectors live only in ANN objects.** Saves 2,049 bytes a row. The rerank
   already reads vectors from the ANN object, so cold ANN bytes don't change. Returning the vector attribute reads
   2 KB ranges from the ANN object (a row's cluster comes from the row-to-cluster map, and its slot from the
   cluster's row set) instead of whole 256-row column blocks (512 KB), which is fewer bytes. Compaction reads
   vectors cluster by cluster, as recluster already does, and reorders them by row.
2. **Keep vectors in `docs.bfc` and drop them from the ANN object.** Rerank would read single rows out of 512 KB
   checksummed column blocks, which means either per-row checksums in the docs format or 256 times the bytes per
   rerank read. It's worse on every cold query. Rejected.
3. **One ANN object per segment or slab.** Merge `.bfv`, `.bff`, and `.bfb` into one object: a resident part (the
   directory entries and the row-to-cluster map), then per leaf in tour order its blob and codes (so a probe is
   still one ranged read), then the vectors region in leaf order. This drops the second code copy and the third
   blob copy (about 175 bytes a row per segment layout and 150 per slab), and the plain and block directory layouts
   become one.
4. **Slabs replace segment ANN objects.** Once a recluster tier covers a segment, the segment's ANN object is dead
   weight. Retiring it saves about 2,210 bytes for every covered row. It needs two things first: filtered ANN on
   slabs (an open item in SPEC §5; a segment's filter bitmap shifts into global rows by the segment's base), and
   compaction that no longer invalidates slabs (the lineage table in `auto-recluster.md`).
5. **f16 instead of f32.** blowfish stores the declared type. Storing an `[N]f32` column at f16 would change the
   values the API returns, which breaks the round trip the turbopuffer API promises. The only way to have both
   would be two copies, which is what this doc removes. Keep the declared precision. f32 columns cost 4,096 bytes
   a row for one copy, and the docs should point users at `[N]f16`. Recall is unaffected either way.
6. **Compress vectors.** f16 embeddings gain 10 to 20% from byte-plane splitting plus zstd. That breaks 2 KB
   per-row ranged reads, so a rerank would read and decompress whole leaves, and it adds CPU on the warm path.
   Rejected.
7. **Compress the other document columns.** These are JSON per 256-row block, with no compression today. zstd at
   level 3 should give 2 to 3 times on Wikipedia text (unmeasured; I assume 2.5 times in the tables). That's about
   250 bytes a row, and hydration reads stay one block per column.
8. **Small items.** `ids.bfi` still carries a 10-bit-per-ID Bloom filter that the 24-bit `idfilter.bff` superseded
   (1.25 bytes a row). Directory entries keep a one-bit center code beside the exact center; that's fine, since
   it's the ranking key.

## Recommendation

Do 3, 1, 7, and then 4, in that order. Keep declared precision (5), and don't compress vectors (6).

| Bytes a row | Today | After |
|---|---:|---:|
| Reclustered | 8,167 | 3,589 (3,340 with column zstd) |
| As loaded | 5,777 | about 3,550 |

| | Today | After | After, with column zstd |
|---|---:|---:|---:|
| 10M reclustered | 81.7 GB | 35.9 GB | 33.4 GB |
| 100M reclustered | 817 GB | 359 GB | 334 GB |
| S3 a month, 10M ($0.023/GB) | $1.88 | $0.83 | $0.77 |
| S3 a month, 100M | $18.78 | $8.26 | $7.68 |
| R2 a month, 100M ($0.015/GB) | $12.25 | $5.38 | $5.01 |

Ingest uploads fall by the same proportion: a segment build writes about 2,200 bytes a row less (the vector
column and the duplicate code and blob copies), and compaction rewrites the same amount less. In-region transfer is
free, but upload time and PUT parts aren't.

Effect on cold reads and recall:

- **3 (one ANN object):** a cold unfiltered query reads the same leaf ranges, now from one object, so more of them
  merge. A filtered query reads the row-to-cluster map from the same object instead of a second one. The codes are
  byte-identical, so recall is unchanged.
- **1 (no vectors in documents):** cold ANN is unchanged. A query that returns the vector reads fewer bytes.
  Compaction reads the same number of vector bytes from a different object.
- **7 (column zstd):** hydration reads about 2.5 times fewer bytes for text attributes, plus a decompress of one
  block per column.
- **4 (slabs only):** reclustered unfiltered queries are unchanged (they already use slabs). Filtered ANN moves to
  slabs, which is a new path and needs its own cold measurement. Uncovered segments keep their own ANN object until
  a tier covers them.

Memory: the decoded cache holds one fragment per segment instead of two, and peak build memory doesn't change
(the builder already copies packed input once). Recluster reads its input from the tier's own slabs plus uncovered
segments, at the same bytes.

## On-disk and object-store format changes

- **Single ANN object** (`ann.<column>.bfa`, new magic and format version 2), used for both segments and slabs:
  1. Header and resident part: the shape, the mean center, one directory entry per leaf (the same fields as today's
     `DirEntry`, with ranges into this object), and the row-to-cluster map (u16, or u32 above 65,535 leaves).
  2. Per leaf, in tour order: the blob (exact rotated center, roaring row set, row CRCs), then its fast-scan codes.
  3. The vectors region, in leaf order.
  4. The footer, as today.

  The namespace directory lists these objects, one layout (`Layout::Plain` and `Layout::Blocks` merge), and the
  paged slab directory points at the same objects.
- **`docs.bfc` version 2:** no vector columns. The segment directory records which vector attributes exist and
  their dims and type, so projection knows to fetch vectors from the ANN object.
- **Text columns** in `docs.bfc` version 2 are zstd-compressed per block (the block CRC covers the compressed
  bytes).
- **Manifest:** a segment's file map gains `annv2:<attr>` and loses `ann:`, `annfrag:`, and `annfrag2:` for that
  attribute. A segment covered by a recluster tier drops its ANN entry entirely (phase 4).

## Compatibility and migration

- **Automatic upgrade** (the indexer's upgrade pass, the same mechanism as today's `backfill_blocks`). For each
  segment whose files are an older format:
  - Read the old `.bfv` and `.bff`, write the single ANN object with the same codes and vectors (no retraining),
    and publish.
  - Rewrite `docs.bfc` without the vector column and with compressed text columns.
  - Retire the old files; GC deletes them after the retention window.

  It runs one segment at a time, behind ingest and compaction. At 100M it reads about 470 GB (old `.bfv`, `.bff`,
  and `docs.bfc`) and writes about 240 GB once, which is a few hours on one 8-vCPU node in-region. The order is ANN first, then documents, so a
  segment is never left without its vectors.
- **Slabs** in the old layout are rebuilt by the automatic recluster (an older-format tier forces a rebuild).
- **Readers during the upgrade:** old and new segments can mix in one namespace. The upgrade-only reader handles the
  old files, and the directory build handles both kinds of entry until the pass finishes. Per decision 25, old
  readers live in an upgrade module and leave the hot path.
- **Older binaries:** new magics fail loudly. An older writer on an upgraded bucket is dangerous, because an older
  GC would not protect files it doesn't know. The manifest fence in `simplify.md` must ship in the release before
  phase 1. Phase 4 also removes the last full copy of some vectors outside slabs, so it must not ship until the
  fence has been out at least one release. There is no downgrade path.

## How to measure success

- **Bucket inventory** (objects and bytes by file type) of a fresh 10M load, as loaded and after automatic
  recluster, before and after. The target is 3,600 bytes a row or less reclustered, against 8,167.
- **Upgrade:** an old 10M namespace upgraded in place. Bytes before and after, upgrade time and peak RSS on a
  c7gd.large, and queries answered correctly throughout (a recall and parity run while the pass runs).
- **No regression:** 10M in-region cold and warm ANN at 48 probes, BM25 cold at 1M, and filtered ANN cold, all within
  run-to-run spread. Recall@10 identical (codes are byte-identical through phases 1 to 3).
- **Ingest:** docs/s at 10M on c7gd.xlarge at least as fast as iteration 16's 4,500, with upload bytes per row reported.
- Text column compression ratio, measured, not assumed.

## Plan (one worker per round)

| Round | Work | Depends on |
|---|---|---|
| D1 | Single ANN object for new segments and slabs; one directory layout; delete `.bff` and `.bfb` writers and `Layout::Plain`/`Blocks`. Upgrade pass for old segments. | Manifest fence (`simplify.md` S2) |
| D2 | `docs.bfc` version 2 without vector columns: projection and compaction read vectors from the ANN object. Upgrade pass rewrites old documents. | D1 |
| D3 | zstd text columns in `docs.bfc` version 2 (one format bump together with D2 if both are ready). Drop the Bloom filter from `ids.bfi`. | D2 |
| D4 | Slabs only: filtered ANN on slabs, retire covered segments' ANN objects. 10M filtered cold run. | Auto-recluster R1 and R4 (tiers and lineage), the fence out one release |
| D5 | In-region inventory and benchmark run at 10M, then 100M with Kyle's go-ahead (AWS cost). | D4 |

### D4 after round AR3: what is left

Status (2026-11-24): the reader migration, fenced retirement, and crash tests are implemented below.
NQ measurements and their limits are recorded in the Round D4 section of `docs/benchmarks.md`.

Round AR3 built R4's lineage: compaction now merges covered segments and records where their rows went. It did
not retire the covered segments' ANN objects. On NQ (2.68M rows, 1,024 dims in f16, everything covered by a base),
those objects total 6.0 GB, about 2,240 bytes per covered row, in line with the estimate above. Retiring them
makes the tier the only copy of those vectors, so every path that reads a covered segment's ANN object has to read
through the tier first:

- **A vector locator.** Given a segment and its rows, return their packed vectors: map each row to the tier's
  global row (the cover's base, or back through the lineage map), then to a slab and the slab's local row (slab row
  lists are ascending global rows; keep them resident, 4 bytes a row). Every item below is built on it.
- **Projection and exact search** (`read_ann_packed`, `indexed.rs`) read through the locator.
- **Filtered ANN** on the tiers: shift each segment's filter bitmap into global rows (the live-row masks already
  do this through `tier_mask`), and probe the tiers under it. Today a filtered query falls back to the segments'
  own indexes.
- **Recluster input** (`recluster_with_spill`) and **compaction** (`merge_packed`) copy vectors through the
  locator. A base rebuild then reads its predecessor's slabs, so the predecessor stays until the new base installs.
  It already does.
- **The trigger** (`auto_recluster::decide`), the **upgrade passes** (`reindex`, `segment_upgrade`),
  **statistics**, **planning**, and **snapshot warm-up** stop requiring `annv2` on every segment.
- **Branches and copies** pin segments, not tiers. They have to pin and copy the tiers covering a segment whose
  ANN object is gone, or a copy would lose those vectors.
- **Dropping a tier** (stale tiers, schema rebuilds) has to rewrite its segments' ANN objects first, through the
  locator. Otherwise the vectors are gone.
- **Retirement** itself: an index pass drops `annv2:` from fully covered segments in one fenced publication, and
  GC deletes the objects after the retention window. The crash points are the publication (before or after its
  write) and GC. In each, a restart must answer from the tier and GC must keep every object the manifest names.

Every bullet is a path where a missed case loses vectors, not just speed, so D4 needs a round of its own. Lineage
and the fence (format 4) are in place for it.

### D4 publication and reader invariants

D4 uses manifest and bucket format 5. Format-4 manifests remain readable and upgrade automatically; a
format-4 binary must reject the format-5 bucket marker before reading, writing, or collecting garbage.
ANN objects keep their existing encoding and global slab row IDs. A slab's file map adds `vector_rows`,
a sorted list of little-endian u32 global rows, followed by a CRC32. Its resident list costs four bytes
per vector row. Binary search identifies the slab and row ordinal; the existing ANN index resolves the
row's cluster and packed vector range. No second vector payload is introduced.

The index publication first makes row lists durable for any existing tiers, then drops fully covered
segments' `annv2:` references, and records their objects for normal retention and GC. Older tiers need a
one-time scan to recover their row lists through the existing ANN reader. New tier builds write the
lists directly. A failed or ambiguous upload or publication leaves the preceding complete source
reachable. Retirement and directory rebuilding share the indexer's fenced publication. Segment
directories list only segments that still own ANN objects.

Additional reader requirements emerged during implementation:

- Parallel query workers select one segment for their work, but must retain the complete original
  snapshot as their vector source. Resolving a tier against that reduced segment list incorrectly makes
  its other covers unreachable. Exact and scalar workers, and ANN plans over uncovered segments, pass the original source explicitly.
- A row-list file is slab metadata, so legacy upgrade fixtures and readers must identify slabs by their
  document role, rather than by an otherwise empty file map.
- An empty segment-directory map no longer identifies a cold namespace: a fully covered namespace
  retains its resident tiers while retiring those directories. Snapshot warm-up checks both maps,
  so warm queries do not repeatedly fetch large directory hints or repeat speculative scans.

Filtered tier queries shift exact per-segment filter results back through lineage into global rows,
then intersect them with each slab's resident row list. Their probe plan uses those matching counts
and the full filter selectivity. The historical segment probe cap under-probed a tier when matches
were spread throughout its global clustering; a tier now preserves the expected unfiltered
candidate coverage, bounded by the number of clusters. Paged directory selection uses the same
selectivity-adjusted budget, computed from actual matching vector rows. The budget adds a margin
of one surviving witness, plus one standard deviation of the trials needed for `top_k + 1`
survivors under independent thinning. This depends on selectivity and `top_k`, rather than a
fixed probe multiplier; correlated filters still require measured recall checks. A filter that
keeps every vector row uses the unfiltered budget. Sparse global slab IDs have no dense row-to-cluster map, so they use the
existing masked code scan, rather than the dense segment map's direct exact-fetch shortcut.
Snapshot warm-up and the planner share bounded cached predicate results for each immutable
segment live generation. The key includes its files, schema, and predicate; WAL shadows are
removed afterward. This avoids repeatedly decoding high-cardinality ID-filter dictionaries,
without reusing a mask across a flush, schema change, or compaction.
Predicate bitmaps and decoded column blocks share one twelfth of the existing decoded-cache
budget. Predicates protect up to one forty-eighth of the total, while columns borrow every byte
not actually occupied by predicates. Predicate replacement or eviction immediately returns those
bytes. Both classes use a byte-bounded CLOCK pool, with value destructors outside its lock; hot
ANN cluster caches keep their existing independent shards. This avoids a fixed reservation
refusing an ID column that otherwise fits whole, keeps large dictionaries from evicting their
own predicate results, and preserves the general LRU budget reserved by compiled warm tiers.

Branches and copies use version-2 pins containing covering tiers and vector lineage, as well as
segments. GC protects every pinned slab, directory, row list, coarse-cell object, and lineage map.
Dropping a stale tier first rebuilds any surviving segment's ANN object through the original snapshot's
locator. Replacing a base reads its predecessor while building, then publishes its replacement before
retiring the predecessor's objects.

Tier replacement also checks complete coverage before retiring any predecessor: every current segment
that predecessor supplies, including lineage targets, must be covered by the replacement. A late,
smaller base or a partially overlapping delta is discarded before publication. Otherwise it could
uncover rows whose own ANN objects were already retired. Newly ingested segments outside the old
tiers keep their own ANN objects and do not prevent a complete replacement from installing.
