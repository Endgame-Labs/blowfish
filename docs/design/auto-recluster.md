# Automatic recluster

Status: rounds R1 to R3 built (2026-11-23), R4's lineage built (2026-11-24); R5 proposed. Implements SPEC decision 21 under decision 25: recluster is always on, with no off
switch and no manual mode. `examples/recluster.rs` goes away once this ships.

## The problem, in numbers

A namespace as loaded has one independent clustering per segment. A recluster replaces that with spatial slabs
of 200-row leaves, which find the same neighbors in far fewer rows:

| Workload | As loaded | Reclustered |
|---|---|---|
| BEIR NQ 2.68M, VM, 16 clients (iteration 20) | 387 q/s at recall@10 0.958 (default probes scaled 2.6 times) | 573 q/s at 0.954, 538 at 0.960 |
| 10M, cold, fresh process, R2 (iteration 12) | 2,916 ms p50 at 160 probes, 116.5 MB | 1,082 ms at 42 probes, 47.1 MB |
| 10M, rows scanned to recall 0.943 | 583K | about 130K |
| Default probes on an as-loaded layout | `min(S^0.3, 4)` times the probes; at 10M with 70 segments, an estimated 3.5 times the rows and cold bytes | as configured |

Today recluster is a manual example, and it doesn't survive normal operation:

- **Any new segment turns the slabs off.** `from_slabs` answers only while the slabs cover exactly the live
  segments (`slabs_cover`). The first flush after a recluster sends every unfiltered query back to the per-segment
  path.
- **Any compaction throws the slabs away.** The indexer retires a slab set as soon as one covered segment is gone
  (`indexer.rs`, the `stale` filter). A bulk load defers compaction until writes pause, so a recluster run right
  after a load is discarded by the deferred merges.
- **It stops indexing while it runs.** `Engine::recluster` claims a new indexer epoch at the start and publishes
  with it at the end. Any other publication in between fences it (at 100M the run is 1.7 h). The example runs with
  indexing off for this reason.
- **GC would delete it mid-build.** GC sweeps unreferenced objects under `slabs/` once they're older than the
  retention window (1 h by default), and its own epoch claim would fence the final publish. A build longer than an
  hour on a live server loses either way.
- **Cost:** at 100M the run took 1.7 h on a c7gd.2xlarge (copy 684 s, assignment about 326 s, 160 slab builds
  6,248 s), with 6.0 GB peak RSS and a 200 GB local spill file. At 10M it took 2,967 s on 4 vCPUs with 1.2 to
  2.4 GB peak RSS and a 20 GB spill.

## Design

### Layout: a base tier, delta tiers, and uncovered segments

`Manifest::ann_slabs` becomes `ann_tiers: attr -> [SlabSet]`: a base and a delta, plus, while a new base builds, a
second delta over the rows written since it started (see "Deltas beside a base build"). Each is
today's `SlabSet` (slabs, their covered segments with bases and live generations, and the paged directory). Rows
in no tier are in **uncovered** segments, which keep their own ANN objects.

One query plan covers all three:
- The base probes as it would alone. The delta, and the uncovered segments' entries in the namespace directory,
  each probe `sqrt(their share of the rows)` of a standalone probe (with the existing `S^0.3` rule for uncovered
  segments). The base and the delta share one rerank bound and hydrate only the global top k. (Round R3 first
  ranked both tiers' leaves as one list. A delta's leaves hold as many rows as the base's over a sparser sample, so
  their centers sit nearer every query, and that walk spent most of the probe on them: see `docs/benchmarks.md`.)
- Deletes and updates in covered segments are masks over global rows, as today. Rows whose newest version is in an
  uncovered segment or the WAL tail shadow older ones, as today.
- This is the open SPEC item "slabs that cover new segments", and it is what makes slabs useful while writes
  continue.

### Compaction and lineage

Covered segments compact without invalidating a tier (round R4). When compaction merges covered segments, the
manifest records a **lineage entry** for each covered source segment: the segments that hold its surviving rows
now, and for each a small map object with two roaring bitmaps, the source rows placed there and the target rows
they became.

- Segments are in ID order, so compaction interleaves its sources' rows: an offset can't map them, but the two
  bitmaps are aligned (the k-th row of one is the k-th of the other), so `rank` in one and `select` in the other
  map a row either way.
- A tier keeps addressing rows by its original global row numbers.
- A winner maps to the segment holding it now at hydrate time; only winners pay.
- A delete after the merge sets the target's live generation, and the tier's mask maps the target's live rows
  back through the maps. The mask is built once per tier and set of live objects and maps (all immutable).
- A merge of a merged segment composes its maps, so a tier row is always one hop from where it lives.
- A merge never mixes coverage classes (the tiers covering a segment's rows, directly or through lineage): base
  rows merge with base rows, delta rows with delta rows, and uncovered segments with each other. Every segment is
  fully covered by its class's tiers or not covered at all, so a delta build reads whole segments, and the search
  beside the tiers skips whole segments.
- Lineage entries are dropped when no tier covers their source segments (a new base covers the targets itself).
- Segments a running or waiting build covers are still left alone, so its install check stays simple.

`Manifest.lineage` is a format-4 field: the bucket and manifest fences went to 4, and v0.1.15 fails closed on an
upgraded bucket.

### Deltas beside a base build

A base build takes minutes (seven on NQ), and under steady writes the rows written meanwhile used to stay outside
the tiers until it finished. Now a delta builds beside it (round AR3):
- It covers the segments outside the base and outside the building base's segments, against the current base's
  cells. It installs as an extra tier: the base, the delta from before, and this one.
- A tier being installed replaces the tiers holding rows of the segments it covers (directly or through lineage),
  and keeps the rest. So the new base replaces the old base and the old delta and keeps the delta built beside it,
  and the next ordinary delta (over every row outside the base) replaces that one. A delta whose rows a newer base
  holds is discarded (`superseded`).
- A delta no longer needs to have been built against the installed base: queries never read the coarse cells, so
  they only shape how well the delta clusters.

### When it runs

The indexer evaluates this after each publication, from manifest numbers only, so the check costs nothing.

1. **Gain estimate.** For each vector attribute, compare the rows a default query scans now with the rows it would
   scan after a rebuild:
   - Now: for each tier, `probes x 200`; for uncovered segments, `probes x min(S^0.3, 4) x` their mean rows per
     cluster (about `sqrt(rows)`).
   - After: `probes x 200` over all rows.
   - The gain `G` is their ratio. Below 100,000 live rows, or with a single segment, `G` is 1.
   - A tier written in an older format counts as `G = infinity` (the automatic upgrade).
2. **Base build:** no base, and `G >= 1.25`.
3. **Delta build:** uncovered rows reach 4% of the base's rows and a quarter of the delta's. The delta is rebuilt
   from its own rows plus the uncovered rows, against the base's coarse cells. (Round R3 lowered this from the
   10% first proposed: on NQ, 5% of rows outside the base cost 35% more instructions per query, and a delta build of
   a few percent of the rows is cheap. The quarter-of-the-delta rule keeps a growing delta from being rebuilt more
   than about eight times before it merges.)
4. **Merge:** the delta reaches half the base, or masked rows reach 20% of a tier. (Round R3 estimates masked rows
   from manifest numbers: the tiers' rows minus the live rows outside the uncovered segments.) All rows are rebuilt into a new
   base, and the delta is dropped.
5. **Bulk-load guard:** while a bulk load is running (the indexer flushed in bulk mode in the last 5 minutes),
   steps 2 to 4 wait, unless uncovered rows exceed the covered rows. A load-then-query namespace therefore pays
   about one full build after the load, plus at most a few doubling builds during it.
6. **Compaction first:** a build starts only when the indexer would not compact now (`tier()` without `force`
   returns nothing). Otherwise the build would cover segments that are about to disappear. (Round R3 first asked
   `tier()` with `force`, which lists merges the indexer defers while a long write tail is pending: under 1,000
   upserts a second on NQ, every evaluation deferred and the rows outside the base reached 300,000 in six minutes.)
7. **Order:** a merge first, then a delta, then a masked-rows rebuild. (A masked rebuild of NQ took 10 minutes
   under 1,000 upserts a second, and with it ahead of the delta, the rows outside the tiers stayed above 20% while
   it ran.)
8. **Beside a base build:** while a new base builds, only a delta over the rows outside it is considered, by the
   delta thresholds of step 3 (see "Deltas beside a base build").

What the policy costs, simulated for a namespace growing steadily in 200,000-row segments (first base at 1M rows):

| Final size | Rows built, as a multiple of the final size | Largest uncovered share after 5M rows |
|---|---:|---:|
| 10M | 3.7 | 10% |
| 100M | 3.9 | 10% |

A doubling-only policy (rebuild everything whenever uncovered rows equal covered rows) builds 1.7 to 2 times the
final size, but it leaves up to 31% of rows uncovered. I chose the tiered version: query cost stays within about 10%
of a fresh recluster at any time, and the build work happens in the background.

### How it runs: background, bounded, and yielding

- **One base job and one delta job per process, run by the namespace's indexer** (the lease holder). They use a
  dedicated build pool of half the cores (at least one), so queries keep the rest. A delta may start beside a base
  build, never the reverse; beside one, it sizes its slabs from half the memory.
- **Memory:**
  - The job reserves from the engine's memory budget. `max_slab_rows` is the memory available after the block
    cache, the decoded cache, the unindexed limit, and any in-flight segment build, divided by 12,000 bytes a row,
    with a floor of 50,000 rows.
  - If that floor doesn't fit, the job doesn't start. It reports `blowfish_recluster_deferred_total{reason="memory"}`
    and tries again after the next publication.
  - It builds one slab at a time, as today, and between slabs it waits whenever a flush or compaction is queued. On
    a 4 GB machine, load and recluster therefore never peak together: ingest peaks at 2.3 to 3.0 GB, and recluster
    at 1.2 to 2.4 GB.
- **Disk:**
  - The spill file is `rows x width` (2 KB a row for 1,024 dims in f16): about 20 GB at 10M and 200 GB at 100M for
    a base build, and a tenth to a half of that for a delta.
  - It goes in the engine's scratch directory (the disk cache directory when one is set, otherwise the system
    temporary directory), and needs 1.1 times its size free.
  - Each build owns a `process-<pid>-<uuid>` directory under `blowfish-recluster`, with an exclusive
    `owner.lock` held until cleanup. A drop guard removes its spill and directory on success, failure,
    cancellation, shutdown, or panic unwinding. Open blocking-worker descriptors remain valid after unlinking.
    Startup and new builds sweep only directories whose owner lock can be acquired; another live process's
    spill stays. Creation, sweeping, and removal serialize on the root directory's lock, so recovery cannot
    race a creator before its owner lock exists. Legacy loose spills have no ownership lock and are left alone.
  - Otherwise the job reports `reason="disk"` and waits. It never fills the disk the cache or the OS needs.
- **Input:** for covered rows, the tier's own slabs (one sequential read per slab); for uncovered rows, the segment
  ANN objects. Rows masked as deleted are skipped. Nothing is read twice.
- **Incremental:** a delta build reuses the base's coarse cells (kept in the tier), so it skips coarse training and
  reads only delta and uncovered rows. A merge reuses the coarse cells too, unless the base has grown four times
  since they were trained.

### Publishing safely against concurrent writes

1. **No epoch claim at the start.** The job snapshots the manifest it starts from (its segment list `C` and their
   live generations) and leaves the indexer running.
2. **Build record:**
   - Before the first upload, the job writes a create-only record
     `ns/<ns>/indexer/build-<uuid>-<n>.json` naming its slab prefix.
   - It writes the next `n` every 10 minutes. Objects are never overwritten.
   - GC treats objects under a prefix with a record younger than twice that interval as referenced. A crashed
     build's objects become ordinary orphans, and GC deletes them after the retention window.
3. **Publish through the normal indexer cycle.** When the slabs and directory are uploaded, the next index cycle on
   this node does the following under its own claim:
   - reads the latest manifest;
   - checks that every segment in `C` is still live (compaction leaves a build's segments alone);
   - installs the tier with `C`'s recorded live generations (deletes since the snapshot become the usual mask);
   - retires the tiers it replaces;
   - publishes with `publish_fenced`.

   A fenced publish retries on the next cycle. A failed coverage check (only possible before lineage) discards the
   build and retires its objects.
4. **Multi-node:** only the lease holder builds. If the lease moves mid-build, the old holder's next publish is
   fenced, so it stops. The new holder starts its own build, and the old build's objects age out.

Writes are never blocked or slowed beyond sharing CPU, memory, and disk with the job, and queries use the current
layout until the new one is published.

## On-disk and object-store format changes

- **Manifest:**
  - `ann_tiers` replaces `ann_slabs`. Each tier gains its coarse cell centers (32 x 1,024 x f32, 128 KB, as an
    object reference) and a format version.
  - `lineage: [ { source, parts: [ { target, rows, map } ] } ]` is added (format 4).
  - The manifest's `format_version` became 3 with `ann_tiers`, and 4 with `lineage`.
- **`ns/<ns>/indexer/build-*.json`** build records (small, create-only).
- Slab objects and the paged directory are unchanged by this doc. `graph-index.md` and `disk-footprint.md` change
  them, and both rely on this job to rewrite old ones.

## Compatibility and migration

- **Existing reclustered namespaces:** an `ann_slabs` entry is read as the base tier on first load and rewritten as
  `ann_tiers` at the next publication. A base whose slabs or directory are an older format is rebuilt by trigger 1
  (`G = infinity`).
- **Namespaces that were never reclustered** get their first base build automatically once compaction settles.
- **Read-only deployments** never build. The writer serving the bucket does.
- **Older binaries:**
  - A 0.1.14-or-earlier query node ignores `ann_tiers` and answers from segments: correct, but slow.
  - An older writer would publish manifests that drop `ann_tiers` and `lineage`, and its GC would sweep the tier
    objects.
  - The manifest fence makes both fail closed. Without it, mixing versions on one bucket is not supported.
- `examples/recluster.rs`, `repage.rs`, the `Engine::recluster` epoch claim, and `BF_RECLUSTER_GLOBAL` are deleted
  (see `simplify.md`).

## Memory and S3 cost

| | 10M | 100M |
|---|---:|---:|
| Base build time | about 50 min on 4 vCPU (2,967 s measured); slower on half the cores | 1.7 h measured on 8 vCPU; about 3 h on half the cores |
| Peak RSS of the job | 1.2 to 2.4 GB (measured, 4 GB machines) | 6.0 GB (measured, 16 GB) |
| Spill file, base build | 20 GB | 200 GB |
| Objects read (in-region, free transfer) | about 22 GB | about 220 GB |
| Objects written | about 24 GB of slabs | about 238 GB today, about 225 GB with the single ANN object |
| GET requests | about 22K | about 220K, about $0.09 |
| PUT parts (16 MB) | about 1,500 | about 15,000, about $0.08 |
| Storage overlap (old and new tier until GC, 1 h) | negligible | 238 GB-hours, about $0.01 |
| Steady growth to this size, total build work | 3.7 times one base build | 3.9 times, about 6.5 h of 8-vCPU time spread over the load |

At 100M, a load-then-query namespace pays about one base build after the load (1.7 to 3 h), plus doubling builds
while the guard trips. On a single c7gd.2xlarge doing both, the job takes CPU from ingest. The bulk-load guard
exists so that it mostly doesn't.

## How to measure success

- **NQ, load and walk away:** load with the default binary, wait for the job, and measure q/s at 16 clients. It
  should match a manual recluster with the same code within 5% (573 q/s today). Record the time from the last write
  to "covered".
- **10M on a c7gd.large (4 GB, swap off):** a load followed by the automatic build, with peak RSS at most 3.5 GB
  and ingest docs/s within 10% of iteration 16's rate.
- **Continuous writes:** 10M plus 1,000 upserts a second for an hour. Uncovered rows stay at or under 10% after
  the first base, warm p50 and recall stay within 10% of a fresh recluster, and no write is rejected beyond
  ordinary backpressure.
- **Safety:** kill the process mid-build and mid-publish (the crash-injection wrapper). No acknowledged write is
  lost, the namespace answers correctly throughout, and orphaned objects are collected. Run GC during a build longer
  than the retention window.
- **100M in-region** (with Kyle's go-ahead): load with the job on, and report the time to covered, peak RSS, cold
  and warm p50 / p90, and recall against iteration 18.

## Plan (one worker per round)

| Round | Work | Gate |
|---|---|---|
| R1 | One query plan over slabs plus uncovered segments; compaction skips covered segments; `ann_tiers` (base only) with migration from `ann_slabs`; manifest fence. | A recluster followed by writes keeps slab-speed queries (NQ and 10M) |
| R2 | The background job: base trigger, memory reservation and yield, disk check, build records and GC protection, publish through the index cycle, metrics. Delete `examples/recluster.rs` and the epoch claim. | NQ and c7gd.large 10M measurements above; crash tests |
| R3 | Delta tier, merge policy, bulk-load guard, reuse of coarse cells. | Continuous-write test |
| R4 | Lineage: compaction of covered segments, the masked-rows trigger, removal of "compaction skips covered". | Compaction during a 10M continuous-write test with tiers intact |
| R5 | 100M in-region run with the job on (AWS cost; ask first). | Numbers above |

Round 21 is making cross-cell assignment two-level so it scales to 10M and 100M cells. Whatever it
lands becomes the only assignment this job uses.
