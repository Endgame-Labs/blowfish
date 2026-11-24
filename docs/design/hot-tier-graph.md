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
original tier row numbers. Dense node IDs follow source slab and IVF-leaf order. It builds one
whole-tier graph over resident SQ8, eliminating both partition boundaries and random f16 stitching.
The initial partition-and-stitch implementation and its rejected controls are measured in Round G4.

1. Validate each row, normalize it, apply the fixed signed Hadamard rotation, and stream SQ8 plus
   its scale into a uniquely owned local spill. Accumulate a normalized empirical mean. Original
   f16 rows are released after each bounded source cluster; no f16 corpus or source cache is kept.
2. Reserve the whole construction before reading source data. Load compact SQ8 and scales from the
   flushed spool, releasing consumed file pages. Refuse the optional graph if it cannot fit.
3. Choose the code nearest the normalized empirical mean as a resident entry. A zero mean uses
   the first valid normalized row. Build two-pass Vamana with final degree 32, L=64, and pruning
   alpha 1 then 1.2, using only SQ8 construction distances.
4. Each construction row owns a fixed-capacity 64-neighbor array under a compact lock. Reverse
   additions prune on overflow, and each pass prunes every row back to 32. Searches copy a bounded
   array before releasing its lock. The constructor frontier periodically removes stale nodes
   outside the monotonically improving beam threshold.
5. Convert construction adjacency into fixed-width immutable links, append two connectivity-ring
   edges, and retain the original tier-row map. Release all construction adjacency and owned spool
   files. Only ordinary queries read original f16 rows, for exact reranking of the top 30.

There are no spatial partitions to overflow or subdivide in this constructor. Oversized tiers are
refused by the process reservation and continue on IVF. Reservation accounts separately for the
source, construction, publication, and retained-query peaks, including simultaneous old adjacency
and new compact links, two construction scratch sets, query scratch, and allocator allowance. Fixed
neighbor arrays and compact locks avoid a hidden per-node allocator or operating-system lock budget.

The SQ8 spool is a disposable implementation detail, not a persisted index format. Linux releases
consumed clean spool pages with `posix_fadvise`; macOS uses uncached descriptors. Dropping a builder
cleans its unique directory on success, refusal, error, or cancellation. The engine uses the existing
process-owned spill lock, so startup recovery can reclaim a crashed build while leaving another
process's active build alone. It uses the disk-cache directory when available, and the standard
temporary directory otherwise. Measurement processes place temporary files in their owned run.

One process-wide construction slot prevents competing graph builders. A dedicated Rayon pool has
at most two workers, using Linux idle scheduling with nice 19 (nice 19 alone when idle scheduling is denied), or macOS utility QoS. CPU passes check cancellation at source-row, graph-point, and bounded-phase boundaries, and periodically check process memory headroom. Automatic recluster construction takes priority;
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
links, row maps, source-index metadata, exact-rerank locations, global construction and publication peaks,
allocator allowance, per-query visited bytes, and bounded heaps. A process-wide counter accounts for
active graphs and builders, while a separate charge accounts for enabled foreground caches. Local mapped storage disables the ordinary
raw-byte cache, so its configured raw-byte budget is not charged a second time.
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

A fresh process starts on IVF. Promotion requires ten minutes of sustained eligible queries,
including requests with payload-cache misses; a gap longer than a minute resets that dwell.
Only the completed foreground query updates its heat timestamp; graph searches and recall trials
must not retime it before the arrival-rate observation. Admission, eviction, and read-only status
also apply elapsed decay when evaluating a rate between arrivals. A 15-minute decaying rate and measured foreground
worker CPU estimate the namespace's future savings. Initial scheduling estimates one CPU millisecond per source row. This is a scheduling estimate,
not a construction-time guarantee; installation uses measured construction cost.

A finished graph initially serves recall trials, while foreground queries retain IVF. The existing
paced exact-recall sampler, or the ordinary recall endpoint, compares those trials against exhaustive
results. Query effort starts at twice the graph degree, not an NQ benchmark setting. Each trial
also evaluates ordinary IVF on the same request and exact reference. The controller targets that
measured IVF quality, with no nominal per-corpus recall constant. It collects at least 32 paired
query observations, using an approximate 95% interval over query-level recall differences; common
query difficulty and correlated neighbors are not treated as independent successes. A variance
floor prevents an all-equal early window from claiming zero uncertainty.

Confident inferiority increases trial effort; confident excess quality explores lower effort.
Otherwise, at least 128 observations and a nonnegative measured quality difference can validate a
candidate. An uncertain deficit collects up to 512 observations before increasing. A bracket of
failed and validated efforts narrows to within 12.5%, while a validated serving effort remains
available during cheaper trials. Sustained excess quality revisits old lower bounds, so an unlucky
early window cannot permanently ratchet effort. The internal ceiling is 4,096; a graph unable to
match IVF there is discarded. Read-only status distinguishes serving effort, trial effort, paired
quality, and uncertainty. Existing sampler pacing bounds production comparison cost; an ordinary
explicit recall request can contribute the same paired observations.

Foreground selection additionally requires predicted CPU savings over the next 24 hours to cover
at least twice measured construction CPU. The builder records its worker CPU and source-transform
CPU, with a conservative 10% allowance for source decoding. Graph query CPU is measured on the
ordinary query workers, charging each poll on its current thread and excluding I/O waits across
CPU-to-I/O continuation. Each graph recall trial also runs the ordinary IVF plan for the same
request and snapshot, with the same admission, worker, and query concurrency. These paired CPU
observations refine the pre-promotion IVF EWMA. Exhaustive scans can displace CPU caches even
when they bypass application cache admission. Warm cost therefore comes from bounded blocks of
up to eight recent sampled requests on one immutable snapshot and trial effort: graph plans run together,
then IVF plans run together, without exhaustive scans between those timed plans. The first cold
query remains included in the average; there is no excluded identical-query warmup. Exact recall
observations keep their original truth comparison. A per-graph queue retains at most 256 KiB of
serialized requests under the metadata reservation, resets across snapshot or effort changes, and
never retains truth responses or snapshots. Individual scan-interleaved IVF timings do not update
the warm reference estimate; both trial CPU estimates come from the same block method. Promotion
requires a positive measured cost block;
queries remain on IVF if a block cannot be collected. Only exhaustive truth work is throttled to one
search worker. A calibrated graph that
remains unprofitable for a full observation window is discarded. All thresholds are internal policy;
there are no per-corpus ef constants or user-facing graph controls.

Both automatic and explicit recall trials dispatch ANN and its IVF reference through the same
query-worker path as ordinary warm queries, with CPU charged per poll across I/O continuations.
Exhaustive truth remains on its separately paced blocking worker. Trials use ordinary cache admission for ANN, so measured
query cost reflects the serving path. Their exhaustive reference pass separately bypasses cache
admission and graph selection. Calibration therefore cannot fill foreground caches with a
one-pass full-corpus scan, or mistake repeated uncached sparse mapping opens for serving cost. Existing recall pacing still limits
expensive background comparisons. Read-only diagnostics report construction wall time, measured CPU,
reservation, retained capacity, current effort, calibration samples, and successful graph plans.
Cumulative navigation, exact-rerank, and hydration CPU counters distinguish search cost from storage
and result assembly without exposing graph controls.

## Writes, masks, and tier changes

A graph covers one immutable recluster tier. Its nodes map to tier rows, then through current lineage
placement to the segments now holding those rows. Small writes preserve the graph. Current indexed
live masks remove dead or superseded rows before selecting the exact rerank pool, while dead nodes
remain navigable. Mask counts are cached against the actual mask allocation, without retaining old
masks. WAL shadowing and tail upserts or deletes use the ordinary engine union and merge.

Delta tiers and uncovered indexed segments retain their independent search plans. Graph and IVF
participants contribute their own top-k candidates, avoiding a scan barrier that a graph participant
cannot enter. Unsupported shapes and heavy masks retain the normal joint IVF plan. If a graph
selected during planning later declines, the complete query retries with the ordinary joint IVF plan,
restoring its shared probe bounds even after eviction, scratch admission loss, or insufficient candidates. Discarded partial graph plans contribute neither recall samples nor CPU savings to calibration.

Above 10% masked source rows, evict the graph and use IVF. Also fall back when live candidates cannot
supply the request. A new recluster directory starts a new identity and heat window; a build or ready
graph over the old identity cannot answer it. Observed namespace deletion evicts immediately. A remote
delete or replacement is observed through normal snapshot refresh or the running build's source check.

## Validation and measurement tools

Vector tests cover source validation, cancellation, spill cleanup, original row coordinates, live
masks, fixed construction adjacency, mid-build cancellation, compact navigation, bounded frontier capacity, and
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
