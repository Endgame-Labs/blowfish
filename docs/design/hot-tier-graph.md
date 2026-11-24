# Disposable row graphs for hot recluster tiers (Round G3)

Status: measured query-path experiment (commit 285ea88, not on main; round G4 is turning it into an automatic default path). The current implementation
fails the end-to-end Qdrant gate. This is a proposal for SPEC 27's hot tier, reopening decision 26
only for an expendable RAM accelerator. The bucket's IVF representation, manifest, recovery path,
and cold query plan stay authoritative. No stored format or default server behavior changes.


## Why rows, rather than leaf centers

The earlier center-graph study did not avoid enough leaf scoring. G3 compares a graph over actual
rows, the same graph with rotated eight-bit navigation and exact rerank, and minimum-score packet
navigation. The full measurements live in `docs/benchmarks.md`, under Round G3. Both row scorers
pass the instruction-cost gate at recall 0.95 and 0.96. The packet graph fails: reaching 0.96 scans
roughly 127,000 rows per query, so grouping rows does not rescue its cost curve.

Use HNSW links built with exact f16 cosine distances, M=16, and construction ef=100. Navigate the
winning variant with symmetric eight-bit scalar codes after the existing seeded orthogonal
rotation, then rerank 30 reached rows against original f16 vectors with the original f32 query.
The experiment uses `hnsw_rs` 0.3.5, licensed MIT OR Apache-2.0. Quantization changes navigation
only; it does not change response-distance semantics. These measurements concern cosine f16 NQ,
not an automatic choice for every metric, dimension, filter, or corpus.

## Proposed automatic lifecycle

A hot graph is a whole, resident object. Individual edge or code misses must never initiate S3
reads during a graph walk. Missing or unadmitted graphs immediately use the existing IVF planner.

1. Record eligible ANN query heat per immutable tier, with a decaying counter and a minimum dwell
   time. Estimate saved query CPU from the measured IVF work and a sampled graph candidate. Promote
   only when expected savings amortize construction within a configurable internal horizon.
2. Reserve a process-wide construction allowance before source reads. Admission includes existing
   graph allocations, exact-vector storage, codes, links, builder peak, input buffers, per-worker
   visited arrays, and foreground headroom. Run one low-priority builder at a time. Refuse promotion
   when the reservation cannot fit; never turn admission failure into a failed user query.
3. Read immutable recluster slabs through ordinary S3 object reads that bypass the foreground range
   cache. Validate the existing slab format and CRCs, extract original tier row numbers, and build
   dense graph node IDs with an explicit node-to-tier-row map. No bucket lock or manifest update is
   needed for the accelerator.
4. Recheck the source tier generation before publication. Publish an immutable `Arc` only if that
   exact tier still belongs to the namespace. A concurrent delete or recluster discards the build.
5. Select the graph only for supported, unfiltered ANN requests with a compatible query shape and
   candidate budget. Query heat and measured graph cost feed eviction. Evict entire graphs, using
   hysteresis to avoid build/evict oscillation. Active queries retain their snapshot until completion.
6. A restart begins with IVF and rebuilds only tiers that become hot again. Losing all graphs is a
   performance event, with no data recovery work or correctness consequence.

Production identity must include the storage backend, namespace generation, vector attribute,
immutable directory identity, metric, dimensions, vector precision, rotation seed, and graph version.
Directory key strings alone are insufficient across multiple storage backends. Graph ownership and
budget accounting belong to an engine instance, rather than a process-global singleton.

Heat thresholds are policy proposals, not calibrated results. The experiment exposes explicit
promotion to measure construction and query cost separately; it does not yet implement this policy.

## Snapshot, masks, deltas, and WAL tail

Each query starts with the existing namespace snapshot and lineage placement. A graph covers exactly
one immutable recluster tier. Its nodes map to that tier's row coordinates, then through the current
lineage placement to segments. It never substitutes corpus IDs for tier row numbers.

Live masks filter result eligibility, while dead nodes remain navigable. This preserves connectivity
after deletes and replacements. Remove masked rows before selecting the exact rerank pool. If the
pool cannot supply top-k, fall back to IVF. A heavily masked graph should be evicted and rebuilt
from a new compacted tier instead of increasing its beam without bound.

Other tiers and uncovered segments retain their existing plans. Union their candidates with graph
candidates, apply the existing shadowed-ID logic, and merge the WAL tail through the normal engine
path. A tail upsert can outrank a graph result immediately; an indexed delta remains searchable before
it joins the base graph. The prototype bypasses the joint IVF scan barrier when any tier uses a graph,
so each participating tier computes its own top-k. It preserves results, at the cost of extra delta
scan work. A shipping planner can share candidate bounds without making a graph participant wait
on an IVF barrier it never enters.

Filtered requests keep the existing filtered planner in this prototype. A future graph filter planner
needs its own recall and selectivity study; postfiltering an unfiltered top-30 is insufficient.
Zero queries, unsupported precision or metric, large top-k, missing entries, and insufficient live
candidates keep using IVF. Hydration, projected attributes, IDs, and response serialization use the
existing segment/document path. The graph does not answer from a second document store.

## Memory and construction limits

The kill-test harness retains every f16 vector alongside codes and links. Its compressed search
therefore uses more RAM than f16-only search. The real-query prototype releases those construction
vectors, retains source-index metadata and row locations, and exactly reranks through the existing
sparse range cache. It retains 3.62 GB (3.37 GiB) on NQ before the range cache and worker scratch.
Capacity reporting excludes allocator metadata and per-worker scratch; report process RSS separately.
Cache-budget sweeps are necessary because range-cache misses can erase the graph's in-process lead.

The native HNSW builder temporarily uses substantially more memory than the exported graph. A
small host must refuse this construction rather than rely on swap. A shipping implementation needs
a compact adjacency representation, reliable peak accounting, cancellation, shared reservations,
and either a smaller builder or a separately budgeted build service. The prototype's conservative
admission estimate is a guard, not an allocator-enforced hard limit.

An independently budgeted exact-vector cache could later omit the full retained f16 corpus. Batch
all rerank misses into existing storage reads, keep that cache separate from graph residency, and
measure both the warm-hit path and recovery from misses. The prototype uses the current shared sparse range cache; a separate row-exact cache is future work.

## Optional S3 persistence

Persistence is deferred. Rebuildable RAM graphs already satisfy correctness and leave cold storage
unchanged. A future sidecar would need immutable source identity, algorithm/version parameters,
rotation identity, checksums, resource limits, and an independently reclaimable object reference.
Loading a corrupt, stale, or unsupported sidecar must silently return to IVF and schedule a rebuild.
Publishing such a sidecar requires an explicit format review before any object or manifest change.

## Prototype surface and validation

`bf_vector::hot_graph::HotGraph` builds from packed f16 vectors plus original tier row numbers,
reports retained capacity, supplies reusable scratch, and searches with optional live masks.
`Engine::build_hot_graph` reads an existing tier, builds off the async executor, checks its identity
again, and installs the disposable entry. The module is compiled only with
`experimental-hot-graph`. The normal binary does not enable it. The trusted, query-only `G3_NQ_GRAPH_IMPORT` control reuses
an EC2-built graph over NQ document IDs, translates IDs into a fresh fixture's tier coordinates, and
loads exact-rerank metadata from its normal objects. This measures query cost on a small host; it
does not demonstrate bounded construction there, or implement a shipping persistence path.

The loopback-only `hot_graph_server` example calls the real `Engine::query`, exposes build readiness
and a graph-use counter, and permits benchmark-only ef/rerank changes. Its endpoints are not public
API additions. `prepare_hot_graph_nq` creates a fresh fixture through writes, indexing, and reclustering.
The integration test covers low-budget refusal, source extraction, node mapping, graph use, live
masks, WAL deletes, WAL upserts, uncovered indexed segments, multiple recluster tiers, filtered
fallback, and zero-query fallback. Vector tests cover invalid input and masked exact rerank.


## Bounded construction result

The G3 example now implements partition-and-stitch Vamana with R=32, L=64, and two pruning passes.
It streams codes, keeps one partition resident, and spills global adjacency. At about 47% overlap,
full NQ construction including SQ8 took 350 s and 0.887 GiB peak RSS on 32 Graviton3 cores. This
solves the monolithic builder's memory peak in the experiment, while missing the proposed 70 s
build target. At 23.65% overlap, recall is more than 0.01 below monolithic HNSW at comparable work;
higher overlap recovers the curve. Four medoid entry points offer little further improvement.
This is an experimental original builder, with warm local input and substantial local spill, rather
than a production S3 background builder. Promotion must account for build CPU amortization and spill
capacity, as well as resident memory. No bounded on-M4 construction or shipped-path win is claimed.

The final paired M4 result is 5,472 versus Qdrant's 8,502 q/s near recall 0.95, and 5,136 versus
6,976 near 0.961. The search core remains promising, but the current prototype should stay disabled
while shared engine overhead and the automatic lifecycle are addressed.


## Concrete shipping sequence

1. Build from immutable tier data after installation, using size-capped spatial partitions,
   two-pass R32/L64 Vamana, approximately 46.5% boundary overlap on this NQ layout, and streamed
   SQ8 encoding. Spill adjacency and codes until construction ends, then load the resident graph.
   Bound and subdivide oversized partitions; the measured NQ peak does not establish a 100M bound.
   Run at low priority, with one builder and at most two background CPU workers while serving.
   The 350 s measurement used all 32 cores; background completion time must be measured separately.
2. Promote only after ten minutes of sustained eligible query heat, when a 15-minute EWMA forecasts
   query CPU savings of at least twice the measured construction CPU cost over the next 24 hours. Estimate savings
   from sampled namespace IVF and graph CPU, rather than transferring NQ constants to other data.
   These are proposed internal policy thresholds, with no new user-facing algorithm knobs.
3. Reserve the maximum of construction and load-phase memory, plus the existing foreground working
   set and a 20% process headroom allowance. Account for codes, compact links, source metadata,
   row maps, allocator overhead, and visited bytes per query worker. Refuse admission when it cannot
   fit. The current NQ prototype's useful warm regime is about 5.2 GiB process RSS; a 4 GiB host
   therefore stays on IVF. Total charged memory, including local file cache, needs a cgroup gate.
4. Own graphs per engine and backend, keyed by namespace generation, attribute, and immutable tier
   identity. Small writes keep using the same graph with current masks and WAL shadowing. Indexed
   deltas keep their existing plan and merge normally. Recluster replacement invalidates the old
   key; namespace deletion cancels construction and evicts immediately. Recheck identity before
   publication. Rebuild or fall back when more than 10% of source rows are masked, pending a measured
   mask-recall calibration. Evict a whole graph after 30 idle minutes, or for a higher-benefit tier.
5. Select query effort through the existing recall monitoring policy, with conservative per-namespace
   calibration. Fixed NQ ef104/160 values are experiment controls, not shipping defaults. Keep filters,
   unsupported metrics and precision, oversized top-k, and insufficient live candidates on IVF.
6. Enable this only after Q2 reduces shared query overhead and a bounded constructor runs on the
   target host, followed by the same matched-recall HTTP gate. Add optional persistence only after
   a separate format review; no new `.bfa` section or manifest reference is justified by this study.

Still missing are a production bounded builder and its foreground interference measurements,
subpartitioning for skew and 100M tiers, compact adjacency, process-wide reservations tied to
engine ownership, heat admission, cancellation and eviction, mask-recall calibration, and a real
on-M4 bounded-build comparison. Bulk reads bypass the foreground cache in the prototype, while
rerank metadata opens use its existing reader; production construction must isolate those too.
