# Automatic, disposable graphs for warm tiers

The bucket's IVF tiers remain authoritative. A warm graph is an engine-owned RAM accelerator:
there is no sidecar, manifest field, feature flag, graph import, promotion API, or algorithm setting.
A restart starts on IVF. Losing, refusing, cancelling, or evicting a graph affects performance only.
The G3 measurements and rejected alternatives remain in `docs/benchmarks.md` and branch history.
G4's release gates and measurements are recorded in that document's Round G4 section.

## Supported queries

Graphs accelerate unfiltered ANN over immutable cosine-f16 tiers, with finite, nonzero vectors,
at most 4,096 dimensions, and a candidate requirement no larger than the 30-row exact rerank pool.
The candidate requirement includes offsets and extra candidates for WAL shadowing. Queries retain
the original f32 query for exact distances against the stored f16 rows.

Filters, exact kNN, other metrics or precision, zero queries, oversized requests, missing graphs,
and insufficient live candidates use IVF. Unsupported shapes retain the ordinary joint IVF plan,
including its shared candidate bounds. A resident graph alone does not disable that plan.

Graph traversal is entirely resident: links, codes, and row maps cannot initiate object reads.
Reached candidates use the existing sparse exact-vector reader and ordinary projected hydration.
ANN response distances, IDs, projected attributes, and WAL merging follow the existing engine path.
No second document store or alternate wire response is introduced.

## Bounded construction

`bf_vector::warm_graph::HotGraphBuilder` accepts clusters of little-endian f16 vectors and their
original tier row numbers. Dense node IDs follow source slab and IVF-leaf order. This keeps the
navigation codes of nearby source rows together without changing their tier coordinates.

1. Stream validated rows into a uniquely owned local spill. Construction reads bypass foreground
   range admission, and source metadata opens run outside decoded-cache admission. Source readers
   retained for exact rerank can use normal foreground caches after installation.
2. Compute normalized empirical means of the original spatial slabs, one row at a time. Assign
   each row to its source slab, and optionally its nearest other slab when the two nearest center
   distances differ by at most a factor of 1.07. There is at most one extra copy per row.
3. Subdivide these overlapping partitions with median projection splits before loading vectors.
   The row cap is dimension-dependent, at most 65,536, with a 384 MiB partition allowance. A median
   split always progresses, including for identical, collinear, or severely skewed rows. Compute
   boundary membership before subdivision: using the smaller child means instead can spend overlap
   inside one coarse slab and lose the intended cross-slab connections.
4. Build one capped partition at a time using two-pass Vamana, R=32, L=64, and pruning alpha 1 then
   1.2. Reciprocal edges are robust-pruned. Boundary copies merge into the global graph with another
   robust prune against original f16 distances. Encode rotated SQ8 per partition, and stream global
   codes and adjacency to spill files.
5. Load fixed-width compact links, degree bytes, SQ8, and the explicit node-to-tier-row map. Two
   additional ring links guarantee connectivity even for degenerate partitions. Partition medoids
   provide resident entry points. Construction f16 vectors and all owned spill files are released.

The source, code, and adjacency files are disposable implementation details, not persisted index
formats. Repeated source reads use a 64 KiB block cache with CLOCK eviction, capped at the smaller
of the reserved SQ8 payload and 2 GiB. Anonymous mapping ownership makes its physical pages directly
releasable before resident graph loading, avoiding allocator retention of a second working set. Linux flushes and releases consumed spill pages with `posix_fadvise`; macOS uses uncached
spill descriptors. Streaming center and projection passes also release consumed pages. Dropping a
builder cleans its unique directory on success, refusal, error, or cancellation. The engine wraps
it in the existing process-owned spill lock, so startup recovery can reclaim a crashed build while
leaving another process's active build alone. It uses the disk-cache directory when available, and
the standard temporary directory otherwise; a disk-cache setting is not required. Measurement
processes direct temporary files into their owned run directory.

One process-wide construction slot prevents competing graph builders. A dedicated Rayon pool has
at most two workers, using Linux nice 5 or macOS utility QoS. CPU passes check cancellation at row and bounded-partition boundaries, and periodically check process memory headroom. Automatic recluster construction takes priority;
it also subtracts existing graph reservations when sizing its own work.

Source decoding, local I/O, and construction all run on that pool. The blocking job retains the
memory charge, construction slot, spill owner, and shutdown tracking if its async waiter is cancelled.
A running build checks the current manifest every 30 seconds, and checks its source again before
installation. Namespace deletion, shutdown, generation replacement, or tier replacement cancels it.
A stale completed graph is discarded without modifying the bucket. No success is assumed from the
G3 construction timing: bounded on-host construction and foreground interference require G4's gates.

## Ownership and memory

Each engine owns its entries. Scoped engines have independent graph identities while sharing their
parent's foreground cache charge. Within an engine, keys include the namespace, namespace creation
generation, vector attribute, and immutable tier directory identity. The backend is scoped by engine
ownership; identical relative keys in different stores cannot select each other's graphs. Supported
metric, precision, dimensions, rotation, and graph implementation are fixed by the immutable source
and current process. No graph survives a binary restart or crosses a backend.

Admission reserves before source reads. The conservative estimate includes padded SQ8, compact
links, row maps, source-index metadata, exact-rerank locations, partition construction and load peaks,
allocator allowance, per-query visited bytes, and bounded heaps. A process-wide counter accounts for
active graphs and builders, while a separate charge accounts for configured foreground caches.
Admission leaves 20% of the machine or cgroup limit as headroom. A charged-memory or host-available
check can refuse a build even when the reservation arithmetic would fit.

Visited arrays belong to an entry's bounded scratch pool. A semaphore caps outstanding graph queries
at the engine's query concurrency. Scratch is returned before hydration, and an evicted graph's
active queries retain its charge until their immutable snapshots and scratch are dropped. No visited
array is left indefinitely in an unaccounted worker-thread cache.

The search frontier periodically removes nodes already worse than the current beam threshold. That
threshold can only improve, so these nodes could never be expanded. This bounds stale heap capacity
without changing the beam's search result. The long improving-walk test checks both the capacity bound
and visitation-generation wraparound.

Whole graphs are evicted under process pressure, starting with the least predicted CPU benefit per
reserved byte. A new candidate may replace an incumbent only after ten minutes of residence and at
least twice its predicted benefit per byte. Idle graphs leave after 30 minutes. Refused, unprofitable,
or evicted candidates wait 30 minutes before another construction attempt, avoiding rapid rebuilds.

Reservation is an admission policy, not a replacement allocator or operating-system memory limit.
Report process RSS and charged memory separately: resident capacity excludes some allocator and file
cache effects. The target-host memory gate must validate the complete working set while serving.

## Heat, recall, and effort

A first or cold query stays on IVF. Promotion requires ten minutes of sustained eligible hot queries;
a gap longer than a minute resets that dwell. A 15-minute decaying rate and measured foreground
worker CPU estimate the namespace's future savings. Initial scheduling uses a conservative construction
estimate of one CPU millisecond per source row. It is only a scheduling estimate.

A finished graph initially serves recall trials, while foreground queries retain IVF. The existing
paced exact-recall sampler, or the ordinary recall endpoint, compares those trials against exhaustive
results. Query effort starts at twice the graph degree, not an NQ benchmark setting. The controller
uses at least eight observations, keeps a rolling window of up to 32, and increases ef when mean
recall is below 0.96. Its internal ceiling is 4,096. A graph that still cannot meet the target at the
ceiling is discarded.

Foreground selection additionally requires predicted CPU savings over the next 24 hours to cover
at least twice measured construction CPU. The builder records its worker CPU and source-transform
CPU, with a conservative 10% allowance for source decoding. Graph query CPU is measured on the
ordinary query worker. The pre-promotion IVF EWMA supplies the comparison. A calibrated graph that
remains unprofitable for a full observation window is discarded. All thresholds are internal policy;
there are no per-corpus ef constants or user-facing graph controls.

Both automatic and explicit exhaustive recall comparisons bypass cache admission, so calibration
does not fill foreground caches with a one-pass full-corpus scan. Existing recall pacing still limits
expensive background comparisons. Read-only diagnostics report construction wall time, measured CPU,
reservation, retained capacity, current effort, calibration samples, and successful graph plans.

## Writes, masks, and tier changes

A graph covers one immutable recluster tier. Its nodes map to tier rows, then through current lineage
placement to the segments now holding those rows. Small writes preserve the graph. Current indexed
live masks remove dead or superseded rows before selecting the exact rerank pool, while dead nodes
remain navigable. Mask counts are cached against the actual mask allocation, without retaining old
masks. WAL shadowing and tail upserts or deletes use the ordinary engine union and merge.

Delta tiers and uncovered indexed segments retain their independent search plans. Graph and IVF
participants contribute their own top-k candidates, avoiding a scan barrier that a graph participant
cannot enter. If every graph declines because of an unsupported shape or heavy masking, the normal
joint IVF plan remains intact.

Above 10% masked source rows, evict the graph and use IVF. Also fall back when live candidates cannot
supply the request. A new recluster directory starts a new identity and heat window; a build or ready
graph over the old identity cannot answer it. Observed namespace deletion evicts immediately. A remote
delete or replacement is observed through normal snapshot refresh or the running build's source check.

## Validation and measurement tools

Vector tests cover source validation, cancellation, spill cleanup, original row coordinates, live
masks, bounded subdivision of duplicate vectors, compact navigation, bounded frontier capacity, and
visitation wraparound. Engine tests exercise exact-recall calibration, WAL updates and deletes,
indexed masks, delta tiers, backend isolation, generation cancellation, unsupported-shape equality,
and active-snapshot reservation lifetime.

`warm_graph_bench` uses the production builder, reports construction CPU and peak RSS before loading
an exact corpus, and sweeps all-query recall and distance evaluations. Its resident query region can
be compared with the monolithic HNSW and resident IVF measurement baselines. Those baselines are
examples only and cannot be loaded by the server. The rejected packet graph, duplicate experimental
partition builder, explicit engine prototype, and trusted import were removed.

`hot_graph_server` is loopback-only, uses the normal automatic lifecycle, and exposes ordinary query,
ordinary recall, and read-only measurement status. `cold_graph_gate` runs identical read-only cold
queries and records exact returned rows, score bits, backend operations, and latency for an ABBA
comparison. Commands and measurement boundaries are in `crates/bf-vector/examples/g3/README.md`.

No 100M construction result, arbitrary-filter graph planner, optional persistence, PCA navigation,
or adaptive early-termination win follows from the G3 results. Any such addition needs its own
quality, memory, and performance gate.
