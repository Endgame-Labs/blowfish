# R2 scale benchmark 🐡

## Scope and status

This run measures real documents and precomputed embeddings on an isolated R2
prefix. Results below distinguish measured values from estimates. A production
service remains on the same VM, with an independent 8 GiB memory limit.

## Method

- Hardware: eight x86-64 vCPUs on an AMD EPYC 9354P, 32 GiB RAM, local NVMe,
  and Cloudflare R2 Standard. Release builds use Rust 1.100.0; Python scripts
  run through uv 0.13.7 with NumPy 2.5.4, PyArrow 26.0.1, and HTTPX 0.28.1.
  The server uses six Rayon threads; the loader uses two OpenBLAS threads.
  Engine defaults permit eight active query drivers and eight segment-scoring
  tasks across the engine. Client concurrency 32 therefore includes queuing.
- The benchmark server runs in a user systemd scope with `MemoryMax=16G` and
  `MemoryHigh=14G`. The loader has a separate `MemoryMax=6G` scope. Available RAM,
  process RSS, process high-water RSS, and scope memory are sampled during loading.
  The loader pauses while Linux `MemAvailable` is below 4 GiB.
- Dataset: **CohereLabs/wikipedia-2023-11-embed-multilingual-v3, English, with
  1024-dimensional embed-multilingual-v3 embeddings**, rather than the unavailable
  older 768-dimensional English set. Dataset revision:
  `ade45fb52bd549f5e8c065636fe4160a43c2af36`. Each Parquet shard contains 100,000
  Wikipedia passages. Shards are downloaded through a keyless Hub integration,
  processed in batches, and deleted immediately afterward.
- Documents retain their passage text, article title, and original embedding values.
  The final rehearsal and full run use f16 after checking every
  source element for exact f16 representability, so it discards no source precision.
  `title_initial` (lowercased first title character) and `text_length` (Unicode
  character count) are filterable. Titles and passage text are stored without
  equality indexes; passage text has a full-text index.
- Holdouts: 200 rows, evenly spaced through the first 100,000 source rows, are
  excluded from ingestion. Their original embeddings are query vectors; their
  article titles are BM25 queries. Exact cosine top-10 is accumulated over every
  successfully ingested vector, using a normalized NumPy matrix multiplication
  and a running top-k. Separate exact results are maintained for both filters.
  A numerical check compared streaming results with a full exact scan over 373
  vectors, seven queries, and uneven 23-row batches.
- Single queries and hybrid subqueries use `top_k=10`. Hybrid requests combine
  ANN and BM25 with multi-query RRF, equal weights, rank constant 60, and a final
  limit of ten rows. Selective ANN uses `title_initial Eq "z"`; broad ANN uses
  `text_length Gte 300`. Both strong and eventual consistency are measured.
- During ingestion, the storage cache has a 1 GiB memory budget, a 48 GiB disk budget, 64 KiB blocks,
  and 128 concurrent fetches. Engine decoded metadata has its default 512 MiB
  budget. ANN uses 128 probes and a rerank factor of 64. Sampled background exact
  recall is disabled because ground truth is computed separately.
- The final query profile uses a 7 GiB memory payload budget, a 48 GiB disk
  budget, **4,000-byte blocks**, 128-block coalescing, and 128 concurrent fetches.
  Maximum coalescing is 500 KiB per fetch. The non-power-of-two block size avoids
  the disk-read allocation doubling measured below. The decoded-cache accounting
  budget is 24 GiB through `BF_DECODED_CACHE_BYTES=25769803776`; this is a
  conservative metadata charge, not a physical-memory reservation.
- The loader sends 25,000 documents per batch, with one in-flight write. Both
  loader and server use `MALLOC_ARENA_MAX=2` to limit allocator retention.
  It explicitly collects cyclic garbage between requests and batches: HTTPX
  responses can retain their serialized request bodies through stream cycles,
  including after retries. A regression test disables automatic collection,
  exercises a transport error and repeated 429s through HTTPX, and checks request
  lifetimes. Disabling explicit collection reproduces the retention failure.
  Ingest throughput is end-to-end: it includes shard downloads, decoding,
  lossless-conversion checks, request serialization, backoff, and the incremental
  exact ground-truth computation. It is not an isolated HTTP write-capacity test.
- The storage client uses `AWS_TIMEOUT=180s`; its default 30-second request
  timeout caused larger segment uploads to fail during the rehearsal.
- Index thresholds are 40,000 changed rows, 256 MiB of logical WAL, or 30 seconds;
  polling is every 500 ms. Backpressure starts at 384 MiB of unindexed WAL.
  Loading uses one-hour retention and a preferred compaction fan-in of four.
  After full indexing, a separate consolidation pass uses a fan-in of two and
  two-minute retention for final reclamation. Four 75,000-row segments cannot
  merge under the 200,000-row cap; pairs can merge into 150,000-row segments. Default
  production retention remains one hour. Segment limits are 200,000 rows and a
  2 GiB source/input byte budget; index overhead can exceed the source budget.

For each query type, consistency level, and concurrency (1, 8, or 32), the query
matrix uses 32 held-out inputs. Cold measurements start a fresh server and an
empty application disk cache before each concurrency-sized burst: 32 independent
single-query starts at concurrency 1, four fresh eight-query bursts at concurrency
8, or one fresh 32-query burst at concurrency 32. Queries within a burst can
share cache fills and include server queuing. Cold QPS excludes process startup
time. Warm measurements follow a complete sequential prewarm of the same inputs.
The operating system's page cache is not flushed. The server is read-only during query measurements, so background indexing and GC
are disabled. The HTTP binary still refreshes its auth-key store every five
seconds, even with authentication disabled; process counters include those
background LISTs. Per-query backend diagnostics exclude that separate task and are collected
separately at concurrency 1 through the engine API. With 32 samples, p99 is descriptive and has
limited statistical confidence; recall is assessed separately over all 200 inputs.
The HTTP runner allows 1,800 seconds per request, configurable through
`BF_SCALE_TIMEOUT_SECONDS`. The preliminary baseline used the previous 600-second
limit and had no deadline expirations. Final runs use 1,800 seconds.

The reproducible loader is `crates/bf-engine/examples/scale_load.py`, run with
`uv`. Storage endpoints, profiles, credentials, bucket names, cache directories,
query vectors, and raw results are runtime inputs and are excluded from this
report. `scale_query.rs` calls the same engine API as HTTP, captures per-query
backend diagnostics, and avoids changing the turbopuffer response schema.
The final HTTP matrix uses the standalone benchmark entrypoint under
`crates/bf-engine/examples/scale-server`. It constructs the unmodified production
`blowfish::router`, including its normal auth-key-store refresh, and supplies
`EngineConfig::from_env()`. This is necessary because the main CLI constructs
`EngineConfig::default()` and does not yet forward `BF_DECODED_CACHE_BYTES`.
The main CLI therefore needs separate tuning support from its owner. The helper
accepts only loopback listen addresses and takes storage configuration from an
external JSON file. Its independent Cargo manifest keeps HTTP dependencies out
of the engine's dependency graph. Build it with:

```sh
CARGO_TARGET_DIR=.scratch/scale-10m/target cargo build --release \
  --manifest-path crates/bf-engine/examples/scale-server/Cargo.toml
```

Ingest and consolidation use the Rust server from `475018f`; the loader applies
the cyclic-GC fix from `8db7f44` at its documented restart. Final query runs and
compatibility checks use `1eec148`; preliminary engine-only decoded-cache trials
use `47c0d69`, with the same query implementation.

## Engine changes used by this run

- Indexing splits its work by row and source-byte budgets. Compaction selects
  groups within the same limits, so capped segments cannot grow into a single
  namespace-sized IVF or text build. A single oversized document remains an
  indivisible input; the byte budget is not a strict resident-memory bound.
- Resident dense vectors use compact base64-backed bytes instead of thousands
  of JSON numeric values. Writes and indexing share decoded metadata, and row
  shadowing uses ID lookup instead of scanning complete documents.
- Immutable BM25 checkpoint corpus statistics are shared through `Arc`, avoiding
  a full vocabulary clone for every query.
- Projected document columns read up to 16 blocks concurrently, retaining
  per-block checksum validation and a bounded amount of in-flight data.
- An idle indexer at a known checkpoint polls remote state at the configured
  age interval, rather than every 500 ms. Local writes, explicit fences, and
  eligible compaction still trigger immediate work.
- Single filtered ANN requests without projected attributes hydrate only the
  columns their filters need. Recursive filter dependencies are preserved, and
  RRF clauses retain their existing hydration to preserve overlapping projections.
  The regression compares results with an exhaustive reference and verifies
  that unused, large text columns do not add backend reads.

- Decoded-cache eviction releases the shared cache lock before dropping retired
  metadata. Large posting-directory destructors previously blocked unrelated cache
  lookups. A thread-and-channel regression reproduces the original failure.
- `BF_DECODED_CACHE_BYTES` tunes the existing decoded-metadata accounting budget.
  Zero disables retention; invalid values retain the 512 MiB default. The engine
  diagnostic example uses the same environment configuration as the server.

These changes bound individual builds and remove avoidable copying and remote
round trips. The segment builder still materializes its bounded input; it is
not a fully streaming IVF builder.

## Storage accounting

Raw data size counts native f32 vector bytes, UTF-8 title and passage bytes, and
16 bytes per document for the numeric ID and filter value. It excludes Parquet
compression, Python objects, HTTP base64 expansion, and JSON framing. Object sizes
include the storage layer's 24-byte retry header. Steady state means indexing and
eligible compaction have finished, and the configured GC retention has elapsed.

The segment format intentionally retains two vector copies: native vectors in
`docs.bfc`, and full-precision reranking vectors in `ann.*.bfv`. The latter also
contains RaBitQ codes, centroids, row maps, and checksums. Text postings, filter
indexes, and ID indexes add to that baseline. Retired segments and WAL payloads
remain until GC; small immutable WAL and manifest slot records remain permanently
for stale-writer fencing.
For this dataset, both native f16 copies together occupy the same vector bytes
as one raw f32 copy. That depends on the verified lossless f16 source values;
arbitrary f32 embeddings have a different storage baseline.

The earlier approximately 31 GB local run cannot be attributed exactly without
its original object inventory. Its approximately 3 GB of vectors also differs
from this run's 1024-dimensional, lossless-f16 storage baseline; the two runs are
not a controlled storage before/after comparison. Zstd-compressed WAL payloads, two permanent vector
copies, and overlapping compaction generations during the one-hour retention
window all contribute. A local file-store disk total can additionally include
range-cache copies, whereas the R2 inventory here counts source-of-truth objects
only. This branch avoids resident decimal JSON vectors and reduces uncompressed WAL
serialization by storing dense vectors as little-endian f32 base64 internally; API projection still
honors the requested array or base64 response format. The measured inventory below
is the basis for current storage claims. WAL payloads are zstd-compressed, so
physical WAL savings depend on data, and retention is a separate source of space.

## Request accounting and cost assumptions

Backend diagnostics and process metrics count logical object-store operations,
including not-found consistency checks. Class A is PUT plus LIST; Class B is GET,
ranged GET, and HEAD. A logical LIST can require multiple physical pages, and
transport retries can issue additional requests, so these counters are not a
Cloudflare billing export. Inventory requests made by the measurement harness
are recorded separately from server operations where possible.
Query counts are deltas around the timed client windows: startup, cache prefill,
the separate diagnostic pass, and inventory requests are excluded. Auth-key polls
during those windows are included in the HTTP process counters. Cache memory-hit
bytes include encoded sizes of decoded metadata and repeated logical ranges.

Published R2 Standard rates, checked on 2026-11-15: $0.015/GB-month, $4.50 per
million Class A operations, $0.36 per million Class B operations, and free egress.
The monthly free tier includes 10 GB-month, one million Class A operations, and
10 million Class B operations. Cost estimates must state the query rate, cache
assumption, and whether the shared account's free tier remains available.
Cloudflare rounds usage up to billing units: whole GB-months and whole millions
of operations. Proportional estimates therefore differ from an account invoice.
Authentication and authorization failures are free; other failed operations can
still be billable, including failed conditional PUTs.
Storage pricing uses decimal GB; RAM budgets use binary GiB. R2 averages daily
storage peaks over the billing period, so load-day retention can affect that
month's storage charge.

https://developers.cloudflare.com/r2/pricing/

https://developers.cloudflare.com/r2/platform/limits/

https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3

## Measurements

The fresh load completed with **10,000,000 real documents**. End-to-end wall time
was **19,992.06 seconds (5 h 33 min), or 500.20 documents/s**. Raw source size is
45,037,644,544 bytes. The final checkpoint covers WAL sequence 403 and has a
publication-grace timestamp 64.06 seconds after the final ingest acknowledgment.
The first immutable manifest slot following that timestamp is published at
20,060.14 seconds from load start, or **68.08 seconds after the final
acknowledgment**. With one ordered index publisher, this gives the inferred
durable completion time of the final checkpoint. It is based on R2 object
`LastModified`, rather than a readiness poll. A later poll confirms all 10M rows
indexed at 20,634.47 seconds from load start, 642.41 seconds after the final write.
It initially contains 101 segments, with no segment exceeding 200,000 rows.
The subsequent consolidation and GC pass takes 6,164 seconds, leaving 60 segments.
The query matrix was only partly measured; see below.
The loader was deliberately restarted from its 3,899,800-document checkpoint to
apply the cyclic-GC fix, before reaching its memory cap. The server stayed running.
Final wall time includes the pause and replay; the loader's resumed internal
elapsed counter excludes them and is not the headline throughput denominator.
The restart replayed 50,000 already accepted upserts from the last atomic
checkpoint; the final namespace and ground truth contain 10M unique documents.
Across both loader processes, there were 1,279 backpressure retries, zero
transport errors, and zero low-memory pauses. Server process peak RSS was
13.15 GiB, and its scope peaked at 14.00 GiB, with zero hard-limit or OOM events.
The loader's observed scope peak was 4.77 GiB before the fix and about 2.01 GiB
afterward.

Backoff starts at 250 ms for each batch, grows by 1.5 after each 429, and caps at
10 seconds. Only one write is in flight, and retries repeat idempotent upserts.
The acknowledged namespace contains all requested IDs after the retries.

### Storage immediately after full indexing

This inventory predates the separate consolidation pass and its shorter retention
window. It contains 101 active segments and 66,403,476,744 bytes overall.

| Component | Bytes |
| --- | ---: |
| Active document columns | 24,645,051,861 |
| Active ANN indexes | 22,188,421,192 |
| Active full-text indexes | 8,869,979,354 |
| Active filter indexes | 682,108,519 |
| Active ID indexes | 116,697,941 |
| Active live masks | 6,456,175 |
| Superseded segment files | 1,145,476,574 |
| Retained WAL payloads | 4,408,042,494 |
| Manifest payloads | 4,341,116,245 |
| Manifest slots, WAL slots, and indexer records | 126,389 |

Active segment files occupy 56,508,715,042 bytes. Manifest payloads include
checkpoint corpus statistics; retaining many versions can also consume gigabytes.
This is distinct from the small fencing records that remain permanently.

### Ingest request counts

Process counters through the final write acknowledgment report 12,615 logical
Class A operations and 49,529 logical Class B operations: 3,175 PUTs, 9,440 LISTs,
24,062 GETs, 23,782 ranged GETs, and 1,685 HEADs. They also report 16,579 deletes,
which R2 does not charge. This includes startup, indexing, maintenance, and
backpressure handling during the load; dataset downloads are separate.
One conflicting PUT is included in the PUT count.

Through the later final-checkpoint confirmation, the counters reach 12,866 Class A
and 50,683 Class B operations, equivalent to about $0.076 at proportional published
rates before any free tier. This is a logical-operation estimate, not an invoice;
pagination, transport retries, consolidation, and measurement-harness requests
are additional accounting considerations.

The separate consolidation and data-GC pass adds 2,186 logical Class A and
32,883 logical Class B operations, before the subsequent idle sample. Server
payload counters through the final ingest acknowledgment record 128.54 GB
uploaded and 139.53 GB downloaded from R2. Consolidation adds 40.26 GB uploaded
and 31.63 GB downloaded. These count logical payload bytes, exclude protocol
overhead and dataset downloads, and illustrate write and read amplification
that the final retained footprint alone does not show.

### Retained 10M namespace after consolidation and GC

The stopped namespace occupies **56,251,706,941 bytes (56.25 GB), or 5,625.17
bytes/document**, including all remaining metadata. This is 1.249 times the
45,037,644,544-byte raw source representation. There are no WAL payloads or
superseded segment files. The inventory contains 1,435 objects.
Replacing the raw source's f32 vector elements with their equivalent, lossless
f16 values gives a 24,557,644,544-byte raw baseline. The retained footprint is
2.291 times that baseline. Both comparisons exclude Parquet compression.

| File type | Objects | Bytes | Bytes/document |
| --- | ---: | ---: | ---: |
| Document columns, `.bfc` | 60 | 24,645,058,456 | 2,464.51 |
| ANN indexes, `.bfv` | 60 | 22,161,490,280 | 2,216.15 |
| Full-text indexes, `.bft` | 60 | 8,444,255,818 | 844.43 |
| Filter indexes, `.bfx` | 60 | 679,970,985 | 68.00 |
| ID indexes, `.bfi` | 60 | 116,682,558 | 11.67 |
| Live masks, `.bfl` | 60 | 5,828,361 | 0.58 |
| Manifest payloads | 4 | 198,282,602 | 19.83 |
| Manifest fencing slots | 668 | 73,480 | 0.0073 |
| WAL fencing slots | 403 | 64,401 | 0.0064 |

Final segment sizes are 21 at 200,000 rows, 35 at 150,000 rows, two at 175,000
rows, one at 199,800 rows, and one at 200 rows. The tiny final segment is below
the size tier of its neighbors. Consolidation reduces full-text index size as
well as removing obsolete generations. Its server peaks at 11.34 GiB process
RSS and about 14.00 GiB of scope memory.

The retained footprint is a snapshot after data-file reclamation. Multiple
manifest payloads remain within the metadata retention and fencing lifecycle.
An additional 60-second idle sample records 23 LISTs, 15 GETs, two PUTs, and
three deletes. This short sample includes auth-key polling and ongoing manifest cleanup and does
not characterize an entire default one-hour retention cycle. Query measurements
disable background maintenance and account for their own operations separately.

### Illustrative monthly R2 cost

At the published proportional rate, retaining the stopped 56.25 GB namespace
costs about **$0.84/month** before free allowances. A standalone, rounded
illustration is $0.855/month, or $0.705/month if the 10 GB-month storage allowance
is entirely available. These are full-month storage assumptions.

For an online, read-only node serving **one warm, strong ANN query per second**
for 30 days, the measured query path adds one HEAD per query: 2,592,000 Class B
requests. The binary's nominal five-second auth-key refresh separately adds
roughly 518,400 Class A LISTs per month. Actual refresh periods also include
request time. This fixed polling cost should not be extrapolated by multiplying
the measured per-query LIST counts by a different query rate.

| Monthly illustration | Storage | Class A | Class B | Total |
| --- | ---: | ---: | ---: | ---: |
| Proportional rates, before free allowances | $0.844 | $2.333 | $0.933 | $4.110 |
| Rounded standalone usage, before free allowances | $0.855 | $4.500 | $1.080 | $6.435 |
| Rounded usage with all monthly free allowances available | $0.705 | $0 | $0 | $0.705 |

The query rate is a billing assumption; provision compute using the measured
throughput. The illustration excludes compute, ongoing ingestion, indexer and GC
maintenance, and physical retries or pagination beyond the logical counters.
Account-wide usage shares allowances and billing units, so these are not claims
about an incremental invoice. Once the worker stops its benchmark node, retaining
the namespace generates no auth-poll traffic from that node.

Ingest through checkpoint confirmation, the consolidation pass, and the recorded
idle sample together report 15,077 logical Class A and 83,581 Class B operations.
Their proportional request cost is about **$0.098** before free allowances.
This excludes the subsequent query benchmark and measurement-harness requests.

### Initial 10M ANN diagnostics

These are 32-query warm, strong-consistency engine measurements on the original
101 segments, with 64 KiB cache blocks, 128 probes, and a rerank factor of 64.
Each configuration first prewarms all 32 inputs. Result ID lists are identical
across the cache budgets. Both runs make exactly one snapshot HEAD per measured
query and no search requests to R2. Snapshot HEADs average 56–60 ms and overlap
local execution.

| Memory cache | p50 | p90 | p99 | Recall@10 | Mean logical memory reads/query | Mean logical disk reads/query |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 GiB | 13,706 ms | 17,874 ms | 24,424 ms | 0.9969 | 543 MB | 706 MB |
| 8 GiB | 7,714 ms | 12,313 ms | 17,801 ms | 0.9969 | 1,160 MB | 89 MB |

The warm bottleneck is local cache handling. The storage planner holds a shared
lock across disk-cache lookups, and every disk hit opens a block file and verifies
its SHA-256 digest. Logical byte counters omit block-read amplification. This
worker leaves storage-crate changes to its owner and evaluates cache geometry
and segment fanout within the existing APIs. The 8 GiB-cache process peaked at
11.72 GiB RSS, inside its 16 GiB limit. Tail estimates from 32 samples remain noisy.

### Tuned 10M ANN preflight

After consolidation to 60 segments, an 8 GiB memory cache with 4 KiB blocks and
128-block coalescing measures warm, strong ANN p50/p90/p99 of
**1,172/1,340/1,368 ms**. The 48 GiB disk budget, 128 fetches, 512 MiB decoded
metadata budget, 128 probes, and rerank factor of 64 remain fixed. Ordered top-10
ID lists match all 32 earlier queries exactly; recall@10 remains 0.996875.
The process peaks at 8.70 GiB RSS.

All measured search reads come from memory: 1,012.35 MB of logical reads per
query, zero disk bytes, and zero missed bytes. Each query makes exactly one
snapshot HEAD, averaging 52.77 ms. Median local execution is 1,169 ms; stages
can overlap. Local execution and cache planning remain the dominant costs.
Both segment fanout and block geometry changed, so this comparison does not
attribute the entire speedup to either change individually.

The 8 GiB profile passes this fresh-cache preflight but is rejected by a subsequent
restart safety check. While sequentially prewarming an existing disk cache, process
RSS reaches 13.67 GiB before the planned concurrent measurement begins. The worker
stops it deliberately; cgroup maximum-limit, OOM, and OOM-kill counters remain zero.

The storage crate undercharges disk-loaded blocks. `Disk::get` reads a block and
its 40-byte header into an initially empty `Vec`, using `Read::take().read_to_end()`.
A local probe on the benchmark toolchain gives length 4,136 and capacity 8,192
for a 4,096-byte payload. The returned `Bytes` slice retains that allocation,
while the memory-cache weight charges only 4,096 bytes. The same probe gives
capacity 131,072 for a 65,536-byte payload. Thus the nominal payload budget is
not a resident-memory bound. Fixing allocation or charging belongs in
`bf-storage`; this worker is restricted to `bf-engine` and benchmark documentation.
Reducing the payload budget to 4 GiB completes the restart and concurrency-32
check at 10.02 GiB peak RSS. However, mean logical disk reads of 44.87 MB/query
accompany p50/p90/p99 of 82,870/143,040/144,410 ms and just 0.221 QPS.
There are still no search requests to R2; this is local work and contention.

A **4,000-byte block** plus its header has length 4,040 and capacity 4,096 in
the same allocation probe. Using that block size with an 8 GiB payload budget
passes both the fresh-cache and restarted concurrency checks:

| Engine preflight | p50 ms | p90 ms | p99 ms | QPS | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Concurrency 1, after sequential prewarm | 1,202 | 1,278 | 1,446 | 0.830 | 8.49 GiB |
| Concurrency 32, restarted with the disk cache | 13,792 | 23,688 | 23,769 | 1.344 | 8.57 GiB |

Both runs measure 32 inputs, return recall@10 of 0.996875, serve all search reads
from memory, and make one consistency HEAD/query. HEADs average 56.64 ms at
concurrency 1 and 146.89 ms at concurrency 32. The later decoded-cache trials retain this
4,000-byte block geometry. Full allocation-accounting and cache-planning fixes remain
storage-crate follow-ups.

### Decoded-metadata cache calibration

Sixty text-index metadata directories occupy 1,212,963,050 encoded bytes. The
engine's conservative 16-times-encoded-size estimate charges 19,407,408,800 bytes.
ANN metadata adds 161,515,400 encoded bytes and a 2,584,246,400-byte charge. Each
individual text directory fits under the default 512 MiB charge limit, but the
complete working set does not. This causes repeated decoding and eviction even
when all encoded search bytes are memory hits. A larger accounting budget does
not reserve that amount of physical RAM; resident memory must be measured.

A preliminary 6 GiB raw-cache, 24 GiB decoded-accounting-budget trial measures
warm strong BM25 p50/p90/p99 of **83/299/637 ms**, versus **4,098 ms median** with
the earlier 512 MiB metadata budget. It makes one HEAD per query, averages
1,444 MB of logical memory reads, and has no disk reads or R2 search requests.
All 32 ordered result lists match the earlier baseline. Peak RSS is 6.01 GiB.
Both the cache-lock fix and the accounting-budget change are present in the new
trial, so this comparison does not isolate their individual effects.

The same 6 GiB profile is inadequate for the combined hybrid working set:
concurrency 32 measures 69,010/181,609/181,627 ms and 0.176 QPS, despite zero R2
search reads. It averages 26.16 MB of logical disk-cache reads per query and
peaks at 11.44 GiB RSS. Even a small number of local misses becomes costly under
the storage planner's shared lock. The subsequent raw-cache trial changes only
the payload budget to 8 GiB. That trial crosses the proactive 13.5 GiB RSS
guard during prewarm (13.52 GiB at the stop), so it is rejected without publishing
a latency result. The 7 GiB trial completes at 12.63 GiB peak RSS. Its concurrency-32 hybrid
p50/p90/p99 is 16,696/59,053/59,076 ms, or 0.541 QPS. Logical disk reads still
average 14.61 MB/query, with no R2 search misses and one consistency HEAD/query.
This is the selected uniform query profile. The host still has ample available RAM;
the tighter guard protects the worker process budget and production reserve.

<!-- HTTP_MATRIX_START -->
### HTTP query measurements (iteration 0 baseline)

The run's query matrix was cut short when the worker ran out of model credits.
The completed groups below use commit `736162f`, a 1 GiB memory cache, and a
512 MiB decoded-metadata budget, except the hybrid row, which uses the tuned
24 GiB decoded budget. Each group has 32 held-out queries, strong consistency,
and `top_k=10`. Latency includes queuing. Recall@10 for ANN is 0.9969.

| Query | Concurrency | Cold p50 ms | Cold p99 ms | Cold QPS | Warm p50 ms | Warm p99 ms | Warm QPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ANN | 1 | 44,644 | 56,095 | 0.022 | 1,239 | 1,553 | 0.792 |
| ANN | 8 | 150,075 | 167,572 | 0.052 | 6,261 | 7,198 | 1.253 |
| ANN | 32 | 262,855 | 375,143 | 0.085 | 14,100 | 22,039 | 1.446 |
| BM25 | 1 | 50,108 | 68,591 | 0.019 | 4,454 | 4,722 | 0.223 |
| BM25 | 8 | 78,239 | 97,731 | 0.096 | 31,300 | 32,195 | 0.260 |
| BM25 | 32 | 134,088 | 193,717 | 0.165 | 81,480 | 132,933 | 0.241 |
| Hybrid | 32 | 603,280 | 817,582 | 0.039 | 34,114 | 112,892 | 0.283 |

These are the numbers later iterations must beat. Warm queries made one R2
consistency HEAD each and no other R2 requests, so warm time is local engine
and cache work. Cold queries read several GB from R2 per burst.
<!-- HTTP_MATRIX_END -->

### One-million-document rehearsal

The rehearsal completed with 1,000,000 real documents, fully indexed through
WAL sequence 36, in 11 segments. The largest segment contains 200,000 rows.
All source embeddings passed the lossless f16 check.

An earlier f32 debugging pass was discarded after diagnosing upload timeouts and
allocator retention. During the subsequent f16 rehearsal, the initial 50,000-row
loader hit its 6 GiB cgroup limit. It resumed from its atomic truth checkpoint at
299,800 rows, using 25,000-row batches and explicit buffer release. The VM still
had about 25 GiB of available memory at the isolated loader failure. Consequently,
this rehearsal is a recovery test, and its throughput includes that interruption.

| Measurement | Rehearsal result |
| --- | ---: |
| Ingest wall time, including recovery | 1,645 s |
| Ingest throughput over that wall time | 608 documents/s |
| Server process peak RSS | 10.60 GiB |
| Server scope peak memory | 12.29 GiB |
| Successful 25,000-row loader scope peak | 3.69 GiB |
| Raw source bytes | 4,495,782,905 |
| R2 bytes before final reclamation | 12,378,041,558 |
| R2 bytes after final reclamation | 5,652,528,230 |
| Steady-state bytes/document | 5,653 |

The post-GC rehearsal inventory contains 2,455,488,026 bytes of document columns,
2,218,308,072 bytes of ANN indexes, 867,063,139 bytes of full-text indexes,
67,385,570 bytes of filter indexes, 10,672,575 bytes of ID indexes, 620,558 bytes
of live masks, and 32,990,290 bytes of manifest data and fencing records.
No WAL payloads remain. Retention accounts for the measured difference between
12.38 GB before GC and 5.65 GB afterward; those values do not reconstruct the
older 31 GB run.

A 100-query warm, strong-consistency engine preflight measured ANN p50/p90/p99
of 162/250/328 ms and recall@10 of 0.992. Each query made exactly one snapshot
HEAD request and no backend requests during search. Mean logical cache reads
were 113.15 MB from memory and 6.15 MB from disk, with zero missed bytes. Snapshot
HEAD requests averaged 64.46 ms; stage timings overlap and must not be added.

An HTTP comparison of the idle-indexer polling fix used the same 100-query warm
ANN workload. Before the fix, p50/p90/p99 were 155/248/326 ms, with one HEAD,
0.26 GETs, and 0.29 LISTs per query. Afterward, they were 147/213/297 ms, with
one HEAD, zero GETs, and 0.03 LISTs per query. These process-wide operation counts
include background maintenance. The fix preserves periodic remote-write polling,
immediate local-write processing, explicit fences, and eligible compaction.

The rehearsal prefix was deleted and verified empty before starting the fresh
10M load.

## Iteration 1 (about 5M docs)

Scope change: the run stopped the load at **4,859,800 documents** (real English Wikipedia passages, 1024-dim
f16 embeddings, 70 segments, fully indexed and garbage-collected) instead of 10M, so later iterations can
scale up in steps (500K, 2.5M, then 10M). Iteration 0 numbers below are at 10M with 60 segments, so the
comparison favors iteration 0 on data size, not iteration 1. Harness: `bench/scale/` (no private settings
in tracked files). Server defaults throughout: nprobe 512, rerank factor 64, decoded cache default (4 GiB
on this 32 GiB host), 8 GiB block cache with 4,000-byte blocks.

### What changed, and the evidence

1. **Cross-segment ANN** (`bf-engine/src/ann.rs`). The old path ran IVF per segment: 128 probes and a
   64x rerank in each of 60 segments. One cold query made about 30,000 ranged GETs (122,341 GETs and 2.27 GB
   for four queries) and rerank-fetched about 38,000 vectors. Now every segment's cluster centers are ranked
   together, only the globally nearest clusters are scanned, and one merged candidate pool is reranked and
   hydrated. Cold ANN dropped to about 5,060 GETs and 419 MB per query. Probes scale as
   `nprobe * clusters / sqrt(rows)` (equal to the old behavior for one segment); default nprobe became 512,
   chosen from a sweep at 10M: 128 gave recall 0.928, 256 gave 0.969, 384 gave 0.981, 512 gave 0.994, and the
   rerank factor made no difference between 64 and 256. Selective filters use exact reads when that is cheaper.
2. **Storage cache** (`bf-storage`). Planning no longer holds the async lock across disk reads, disk
   lookups run in parallel batches outside the disk index lock, blocks verify with CRC-32 instead of SHA-256,
   reads use exact-size buffers (no 2x retained capacity), per-block path allocations and per-block metric
   updates are gone, and eviction maintenance runs once per fetch. Warm ANN at 10M went from 1,239 ms to
   about 255 ms at the same recall, of which about 3x came from these changes.
3. **Decoded-metadata accounting.** Entries are charged at resident size instead of 16x encoded size, and the
   default budget is one eighth of memory (512 MiB to 4 GiB). Warm BM25 p50 went from 4,454 ms to 82 ms
   without raising any flag.
4. **Ingest.** Findings with evidence: write commits are serialized under the state lock and dominated by the
   9 MB WAL PUT (append about 1.4 s per commit, of about 2.3 s total); an idle indexer tick deep-cloned the
   whole unindexed tail under the same lock; the pre-commit catch-up LIST cost about 0.6 s per commit; GC
   deleted retired objects one at a time, stalling indexing for minutes once the retention window passed
   (2,500 deletes); and compaction runs in the same call as the flush, so a long compaction backs up the tail
   (the loader hit 800 backpressure 429s over the run). Fixes: decide before cloning, skip the catch-up when
   the state is fresh (the conditional create still detects other writers), concurrent GC deletes, text
   tokenization in parallel, ANN and text builds overlapped on blocking workers, concurrent index uploads,
   multipart uploads for files of 32 MiB and up, concurrent segment builds, and a pipelined loader
   (`--inflight`). The loader's own bandwidth was about 1,400 docs/s against a local store.
5. Smaller: the key store refreshes every 300 s when auth is disabled (it LISTed every 5 s), optional
   `coalesce_gap_blocks`, new env knobs `BF_INDEX_BYTES`, `BF_ANN_RERANK`, `BF_BUILD_CONCURRENCY`,
   `BF_RETENTION_SECS`, and commit-stage and segment-build metrics.

### Ingest and storage

| Measurement | Iteration 0 (10M) | Iteration 1 (4.86M) |
| --- | ---: | ---: |
| End-to-end rate | 500 docs/s | about 600 docs/s average (about 900 docs/s peak; 360 to 650 while compacting) |
| Backpressure 429s | 1,279 | 799 |
| R2 bytes per doc after GC | 5,653 (1M rehearsal) | 5,689 (27.65 GB over 70 segments) |
| Server peak scope memory | 10.6 GiB RSS | 13.9 GiB scope with a 4 GiB decoded cache and 1 GiB unindexed limit, 7 to 8 GiB with 1 GiB and 384 MiB |

Ingest was still bound by serial commits and compaction, not by CPU (the VM was 85 to 98 percent idle): see
the ranked bottlenecks in `WORKER_REPORT.md`. The run was resumed several times to hot-swap fixes, so the
averages include restarts. Post-GC inventory: docs 11.95 GB, ANN 10.79 GB, full text 4.45 GB, filters
0.33 GB, ids 0.06 GB.

### Query matrix (strong consistency, 32 held-out queries, `top_k` 10)

Cold means a fresh process and an empty disk cache for each of 8 queries at concurrency 1. Requests and
bytes are R2 operations per query from process metrics. Warm queries make one HEAD and no other request.

| Query | Cold p50 / p90 / p99 ms | Cold R2 requests/q | Cold MB/q | Warm c1 p50 / p90 / p99 ms | Warm QPS c1 / c8 / c32 | Recall@10 |
| --- | --- | ---: | ---: | --- | --- | ---: |
| ANN | 27,690 / 30,719 / 33,393 | 5,067 | 419 | 321 / 384 / 390 | 3.00 / 3.73 / 3.49 | 0.984 |
| BM25 | 46,485 / 50,236 / 52,851 | 1,902 | 857 | 82 / 129 / 237 | 10.65 / 18.90 / 17.93 | |
| Hybrid | 70,570 / 74,992 / 77,012 | 6,752 | 1,185 | 371 / 483 / 579 | 2.67 / 3.14 / 3.17 | |
| Selective ANN | 41,255 / 46,195 / 46,653 | 7,305 | 600 | 391 / 478 / 493 | 2.53 / 3.03 / 2.82 | 0.994 |

Recall is against the loader's exact truth for the loaded rows (computed at the last shard checkpoint, so it
misses about 1 percent of rows loaded after it; treat as a lower bound). Peak RSS: ANN 2.3 GiB, BM25
3.7 GiB, hybrid 5.3 GiB, selective 1.6 GiB.

Sequential read waves per cold query: ANN has three ranged-read stages (index metadata, ids, and live masks
together; probed-cluster codes; candidate vectors), asserted by `cold_ann_has_three_read_stages_...` in
`bf-engine/tests/read_cache.rs`. BM25 and hybrid have the same shape (directory, posting blocks, documents)
by construction; they were not independently counted at scale.

### Before and after

| Metric | Iteration 0 (10M, 60 segments) | Iteration 1 (4.86M, 70 segments) |
| --- | ---: | ---: |
| Cold ANN p50 | 44,644 ms | 27,690 ms |
| Cold ANN R2 requests/query | about 30,000 | 5,067 |
| Cold hybrid R2 requests/query (c32) | about 20,200 | 6,752 |
| Warm ANN p50 (c1) | 1,239 ms | 321 ms |
| Warm BM25 p50 (c1) | 4,454 ms | 82 ms |
| Warm BM25 QPS (c1) | 0.22 | 10.65 |
| ANN recall@10 | 0.997 | 0.984 (0.994 at 10M-tuned nprobe 512 on the selective set) |

Not comparable one-to-one (half the data), but the cold request counts fall far faster than data size, and the
warm BM25 fix is a pure accounting change.

### Remaining bottlenecks

Cold queries still read 400 MB to 1.2 GB (index metadata and probed codes) and are latency-bound by round
trips: 5,000 or more GETs per query. Warm ANN QPS does not scale with concurrency (3.0 at c1, 3.5 at c32)
because one query already uses all cores for about 2 core-seconds of code scanning. See `WORKER_REPORT.md`.

## Iteration 2 (100K docs)

Scope change: from this round on, scale runs use **100,000 documents** (the first 100K English Wikipedia passages,
1024-dim f16, the same 32 held-out queries), until speeds are down. The namespace holds 8 segment directories and
about 1.18 GB in the bucket (including WAL objects not yet collected). Memory budgets are scaled down with the data:
block cache 256 MiB (the 4.86M run used 8 GiB over 27.6 GB, about 30 percent; here 256 MiB over about 0.9 GB of
segment data) and decoded-metadata cache 128 MiB (4 GiB at 4.86M, scaled by data size). Strong consistency, cold
means a fresh process and an empty disk cache per query (8 queries), warm is the same 32 queries repeated.
"Before" is the iteration 1 code (commit `bb8db09`) run on the same 100K namespace.

### Diagnosis (measured, then fixed)

1. **What the 419 MB per cold ANN query at 4.86M was** (read trace, `BF_TRACE_READS=1`): 323 MB from the 70 ANN
   files (about 130 MB index metadata, mostly 4 KB centers and per-row checksums, plus about 186 MB of probed
   codes in 4,000 requests), 56 MB of whole ID indexes, 32 MB of manifest (namespace-wide term document
   frequencies, read in full by every cold query), 7 MB of rerank vectors, 3 MB of live masks. Probes covered 23
   percent of all clusters because every segment is clustered independently and probes scale with the number of
   clusters. In the old layout codes and vectors alternated per cluster, so probed codes could never coalesce.
   The cold timeline was serial: manifest, then open (8 s at 4.86M), then codes (9 s), then rerank.
2. **Warm ANN was not scan-bound.** A first 100K baseline looked broken (warm ANN 1.1 s, 15 GB of RSS under hybrid
   load). The cause was a harness error: the loader stopped before the indexer drained, leaving a 30K-document
   unindexed tail that every query cloned and scanned exhaustively. The loader now waits until the tail is empty.
   With the tail drained, warm ANN is about 75 ms, of which about 70 ms is the strong-consistency HEAD of the next
   WAL object; query execution itself is 23 ms. At 100K the warm floor is one R2 round trip.
3. ANN recall at 100K, by sweep over 32 queries: nprobe 16, 32, 64, 128 gave 0.925, 0.966, 0.981, 0.997;
   rerank factors 8, 16, 32 gave identical recall. The old defaults (512, 64) scanned every cluster.

### What changed

| Change | Effect |
| --- | --- |
| Index format flags (`bf-vector`): centers stored rotated, 8-byte code headers, all codes contiguous before all vectors. Old indexes still read (centers rotated at open) | smaller codes (144 to 136 B per row), probed clusters adjacent in the file |
| Probed code blocks and candidate vectors are read with gap merging (64 KB for codes, 16 KB for vectors) | cold ANN 1,021 to 238 requests |
| ANN scan: rotate the query once, quantize the residual to 8 bit planes, score with popcounts (was a 256-entry f64 table per 8 dimensions, rebuilt per cluster) | per-cluster setup and per-row scoring both cheaper; 4 to 8 ms per 60 clusters warm |
| Defaults: nprobe 512 to 64, rerank factor 64 to 32 | cold bytes halved; recall 0.97 at 100K |
| Manifest payloads over 64 KB are zstd-compressed (plain JSON still readable) | cold manifest download 2.4 MB at 100K, 32 MB at 4.86M, now compressed |
| Fetch concurrency 64 to 512, wider segment open concurrency | request-latency-bound phases overlap more |
| Diagnostics: `BF_TRACE_READS=1` (one line per backend read) and `BF_TRACE_STAGES=1` | used for the numbers above |

### Inner loops

| Loop | Change | Cold ANN req / MB / p50 | Warm ANN p50 |
| --- | --- | --- | --- |
| 0 | Iteration 1 code, drained namespace | 1,021 / 31.2 / 2,503 ms | 87 ms |
| 1 | New binary on the old (v1) files: popcount scan, wide concurrency | 998 / 31.7 / 2,300 ms | 92 ms |
| 2 | Fresh load in the new format, nprobe 64, rerank 16, merged reads | 170 / 15.7 / 2,079 ms | 74 ms |
| 3 | Rerank 32, compressed manifest (final) | 238 / 16.1 / 1,839 ms | 81 ms |

### Results at 100K (strong, c1 unless noted)

| Query | Cold p50 / p90 / p99 ms | Cold R2 req/q | Cold MB/q | Warm p50 / p90 / p99 ms | QPS c1 / c8 / c32 | Recall@10 |
| --- | --- | ---: | ---: | --- | --- | ---: |
| ANN | 1,839 / 2,161 / 2,216 | 238 | 16.1 | 81 / 132 / 306 | 10.3 / 63.4 / 114.0 | 0.966 |
| BM25 | 1,640 / 3,087 / 4,099 | 38 | 24.4 | 68 / 111 / 176 | 12.3 / 76.5 / 100.0 | |
| Hybrid | 2,710 / 2,824 / 2,827 | 264 | 38.6 | 73 / 134 / 194 | 11.0 / 83.5 / 105.8 | |
| Selective ANN | 2,458 / 3,018 / 3,699 | 234 | 7.9 | 72 / 114 / 130 | 12.3 / 61.2 / 109.9 | 1.000 |

Before (iteration 1 code, same data): ANN cold 2,503 / 7,633 / 9,494 ms, 1,021 requests, 31.2 MB, warm c8 QPS 42.3,
recall 1.000; BM25 cold 1,773 ms, 42 requests, 25.7 MB; hybrid cold 2,964 ms, 1,050 requests, 53.4 MB; selective
cold 2,466 ms, 259 requests, 9.4 MB. Cold ANN reads 16.1 MB, which is about 1.4 percent of the 1.18 GB namespace
(about 5 percent of the 335 MB ANN data). A cold ANN query opens 5 indexed segments plus a flat one and scans about
120 of about 700 clusters. Sequential waves: manifest chain (list, slot, payload, WAL list; 3 to 4 serial round
trips, about 0.65 s), open (index metadata, IDs, live masks, in parallel), codes, rerank vectors, then document
hydration: 6 stages, over the 3-wave target. Ingest: 660 to 830 docs/s (the earlier baseline load ran 560),
0 backpressure 429s. Targets met: cold ANN requests under 500, cold ANN p50 under 2 s, warm p50 under 100 ms.
Not met: waves, cold BM25 and hybrid bytes, warm p50 below the HEAD round trip.

## Iteration 3 (100K docs)

Same 100K namespace, strong consistency, memory budgets scaled with the data (256 MiB block cache, 128 MiB decoded
metadata), cold means a fresh process and an empty disk cache (8 queries), as in iteration 2. The R2 round trip was
about 25 ms slower during the final matrix than during iteration 2 (warm p50 100 ms against 75 to 81 ms, which is
the strong-consistency HEAD), and a cold query has five to six dependent round trips, so the final cold numbers
understate the code gain: single cold runs taken earlier in the day on the same data gave ANN 1.1 to 1.3 s, BM25
0.9 s, hybrid 1.5 to 1.9 s, and selective 1.2 s.

### What changed, and the evidence

Cold ANN timeline before (read trace): manifest list, slot, payload (867 KB, 290 ms), WAL probe, then open, then the
flat segment's exact search (370 ms, serialized before the cluster scan), scan, rerank, hydrate.

| Change | Effect |
| --- | --- |
| Manifest slot holds the manifest itself (JSON, or zstd above 64 KB); old slots with a payload pointer still read | one fewer dependent GET |
| Namespace-wide term statistics leave the manifest: an out-of-line object (an FST from term to document frequency per field, plus counts). BM25 queries look up only their own terms in the downloaded bytes; the manifest is 13 KB instead of 867 KB | manifest read 290 ms to 90 ms; no term-map decode or clone per query; non-text queries never read it |
| WAL tail probing in doubling windows, not one slot at a time | a tail of n entries costs about log2(n) round trips |
| A strong snapshot that adopts a newer manifest opens the query's segment files (vector index, text index, IDs, live masks, docs directory, term statistics) while it reads the WAL tail | the open wave overlaps the tail probe |
| Text segment format v2: postings, then per-term block directories, then lengths and the term dictionary. The dictionary maps a term to its directory record, and a query reads only its terms' records (v1 segments still read) | cold BM25 24.4 MB to 7.5 MB, the 7 MB to 17 MB of eager decode per segment is gone |
| Flat-segment exact search runs concurrently with the clustered scan (was serialized) | about 370 ms off cold ANN |
| Hybrid subqueries run concurrently (were sequential) | cold hybrid 2.7 s to 2.2 s (1.5 s on a quiet link) |
| Document hydration reads columns and segments concurrently; ANN prefetches the best candidates' documents during rerank; the docs directory is opened in the open wave when the query projects attributes or filters | selective cold 2.9 s to 1.2 s in a single-run trace (hydration was 1.7 s of serial 80 ms reads) |

### Inner loops (single cold query traces, request count / MB / ms)

| Loop | Change | ANN | BM25 | Hybrid | Selective |
| --- | --- | --- | --- | --- | --- |
| 0 | Iteration 2 main (v1 files) | 238 / 16.1 / 2,225 | 38 / 24.4 / 1,406 | 264 / 38.6 / 2,710 | 234 / 7.9 / 2,458 |
| 1 | Exact and scan overlap, concurrent hybrid, open overlapped with the WAL tail (old files) | 251 / 16.3 / 1,669 | 48 / 24.3 / 1,576 | | |
| 2 | Lazy text format v2, inline manifest, statistics object as JSON (fresh load) | 238 / 15.9 / 1,751 | 69 / 8.2 / 1,420 | 292 / 23.0 / 1,477 | 240 / 7.2 / 3,005 |
| 3 | Statistics as an FST, concurrent hydration, namespace drained | 275 / 16.5 / 1,084 | 43 / 7.5 / 890 | 301 / 22.8 / 1,514 | 233 / 7.9 / 1,214 |

(Loops 2 and the first selective figure ran on an undrained WAL tail; the drained namespace is loop 3.)

### Results at 100K, final matrix (strong, c1 unless noted)

| Query | Cold p50 ms | Cold GETs/q | Cold MB/q | Warm p50 / p90 / p99 ms | QPS c1 / c8 / c32 | Recall@10 |
| --- | ---: | ---: | ---: | --- | --- | ---: |
| ANN | 1,937 (was 1,839) | 240 (238) | 16 (16.1) | 100 / 101 / 221 | 10.0 / 46.2 / 56.1 | 0.988 (0.966) |
| BM25 | 1,415 (1,640) | 46 (38) | 7 (24.4) | 100 / 164 / 299 | 8.7 / 47.2 / 93.8 | |
| Hybrid | 2,167 (2,710) | 274 (264) | 22 (38.6) | 100 / 126 / 177 | 10.2 / 52.9 / 51.7 | |
| Selective ANN | 1,616 (2,458) | 239 (234) | 8 (7.9) | 99 / 105 / 221 | 9.8 / 52.0 / 107.0 | 1.000 |

Ingest 100K docs at 616 to 655 docs/s with 0 backpressure 429s; peak server RSS 2.6 GB. Warm QPS at c8 is lower than in
iteration 2 (63 to 83) for the same reason as the warm p50: R2 round trip time during this run.

Sequential waves for a cold ANN query that returns ids only: list, slot, (WAL tail with segment open), (flat exact
with cluster scan), rerank, which is five; with attributes there is a sixth (hydration), partly served from the
speculative prefetch. The target of three needs either a pointer that skips the listing or a persisted open-hint,
neither of which exists yet.

Eventual consistency, warm (32 queries, c1, repeated): ANN 35 ms per query (28.7 QPS) against about 100 ms strong; BM25
9 ms (114 QPS). The strong figure is the HEAD of the next WAL object, so the eventual figure is the execution floor.

### BM25 score parity

Scores run 1 to 3 percent below turbopuffer's for the same ranking. `bench/scale/bm25_idf_probe.py` isolates it. For a
single-term query the score is idf times a tf-part, and the ratio of the two systems' scores is constant across
documents with different term frequencies and lengths, so average document length, k1, and b agree. What differs is
the idf, by a near-constant additive offset: for 40 percent of terms turbopuffer's idf is exactly +0.169 above ours
(rare terms, df 1 to 15, and common terms alike), which is ln((N'+1)/(N+1)) for an N' about 18.5 percent above the
100,000 documents the namespace holds. The rest of the terms sit below that offset or above it (a few at +0.2 to +0.3),
which is consistent with turbopuffer counting a different set of documents for those terms (the 200 held-out passages
are not enough). The recorded 3-document compat scenarios match to 1e-5, so the formula is the same; the difference
appears only at scale. This looks like an artifact of the reference namespace (statistics counting documents that
were superseded or not yet compacted), not a blowfish bug, and the ordering is the same (top-1 agreement 1.000), so no
scoring change was made. Confirming it needs a freshly loaded reference namespace, which this round was not
permitted to create.

## Compatibility validation

The 121-case compatibility suite passes in both normal mode and forced indexing
mode (`BF_INDEX_ROWS=1`, `BF_INDEX_AGE_MS=1`, and `BF_INDEX_INTERVAL_MS=50`), against
an isolated local file-store server built from commit `736162f`. No oracle or
production namespaces were used for these checks.

## Iteration 4 (100K docs): the namespace ANN directory

Same dataset and 200 holdouts, `scale/` wiped and reloaded; block cache 256 MiB and decoded metadata 128 MiB (scaled
down from the 10M settings so cache pressure resembles 10M); cold means a fresh process and an empty disk cache (8
queries); strong consistency. Implements `docs/design/ann-directory.md`.

### What changed

| Change | Effect |
| --- | --- |
| The indexer writes one `ann-dir-*.bfd` object per publication (manifest field `ann_dirs`, additive): per leaf cluster, an int8-quantized rotated center for ranking, code and vector byte ranges, checksums, and a blob (exact center, row set, row checksums) read only for probed clusters. About 2.8 MB at 100K | cold ANN opens no segment index: the test over 20 segments sees zero reads of segment index metadata |
| A directory is pinned in the decoded cache and loaded in the snapshot wave, next to the WAL tail | ranking needs no further wave |
| Probed clusters: codes, blobs, and docs footers are read in one wave from directory byte ranges | one wave, requests grow with probes, not segments |
| Error-bounded reranking: only candidates whose lower bound (estimate minus about 2 sigma) can beat the k-th best upper bound are reranked; `ann_rerank_factor` stays as a cap | rerank pool 301 (cap hit) to 93 per query; requests 216 to 129; recall unchanged (0.984) |
| Vector range merge gap 16 KB to 64 KB | fewer rerank requests for a few more bytes |
| Hydration reads the docs `id` column (no ID index); the best candidates' document blocks are prefetched during rerank, including exactly scanned flat segments | ids-only and attribute queries both finish in 3 waves in the test |
| Ingest: no change in code, but the run loaded at 798 to 920 docs/s (iteration 3: 616 to 655), 0 backpressure 429s | |

Directory use needs: no filter, no tail shadowing, and a directory that lists exactly the manifest's segments
(otherwise the per-segment path runs). Filtered ANN still uses the per-segment path (phase 2: per-cluster filter
summaries). `BF_ANN_DIRECTORY=0` forces the old path for A/B runs.

### Results (strong, c1 unless noted; same session A/B for ANN)

| Query | Cold p50 ms | Cold GETs/q | Cold MB/q | Warm p50 / p90 / p99 ms | QPS c1 / c8 / c32 | Recall@10 |
| --- | ---: | ---: | ---: | --- | --- | ---: |
| ANN, directory | 923 (881 in the A/B run) | 129 | 14 | 61 / 104 / 112 | 14.6 / 81 / 114 | 0.984 |
| ANN, per-segment path, same session | 1,160 | 230 | 16 | 57 / 90 / 144 | 15.3 / 95 / 129 | 0.984 |
| ANN, iteration 3 recorded | 1,937 | 240 | 16 | 100 / 101 / 221 | 10.0 / 46 / 56 | 0.988 |
| BM25 | 725 (1,415) | 39 | 7 | 62 / 90 / 106 | 15.3 / 85 / 157 | |
| Hybrid | 1,080 (2,167) | 168 (274) | 21 | 62 / 119 / 184 | 13.5 / 88 / 100 | |
| Selective ANN | 1,172 (1,616) | 222 | 9 | 69 / 115 / 173 | 12.8 / 94 / 113 | 1.000 |

R2 round trips were faster this session than in iteration 3 (warm p50 about 60 ms against 100 ms), so compare the
same-session ANN rows: the directory is 24 percent faster cold than the per-segment path and halves cold requests.
The target of 30 percent below iteration 3 is met against the recorded figure (52 percent) but only 24 percent
in an A/B run, because this 100K namespace has only 8 segments, where the old open wave was cheap. The directory's win
grows with the segment count.

Waves, cold ANN (test `cold_ann_over_twenty_segments_uses_the_directory`, 20 segments, 10 ms per request): 3 for ids
only and 3 with attributes, counting the snapshot wave with the directory, 33 to 40 requests, no segment index read.

Parity against turbopuffer is recorded in the milestone table below. The per-segment path scored identically on this
reloaded namespace (ANN overlap 0.962), so the small difference from earlier milestones is the reload, not the directory.

### Remaining

- Filtered ANN uses the per-segment path (222 requests, 8 MB).
- The flat segment (about 2K rows) is read whole, 4 MB per cold query.
- Rerank is still 80 sparse ranges per query; storing full vectors with the codes would turn them into one read per cluster.
- Directory centers are int8, 1 KB per cluster: 30 MB at 10M. A 1-bit code would cut that to 130 B.

## Iteration 5 (100K docs): one-bit directory centers and incremental publication

Same dataset and 200 holdouts, `scale/` wiped and reloaded; block cache 256 MiB and decoded metadata 128 MiB (scaled
down from the 10M settings so cache pressure resembles 10M); cold means a fresh process and an empty disk cache (8
queries); strong consistency; nprobe 64, rerank cap 32 x k.

### What changed

| Change | Why (evidence) | Effect |
| --- | --- | --- |
| Directory centers are RaBitQ one-bit codes of the offset from the segment mean (196 B per leaf), not int8 (about 1.1 KB resident per leaf) | the int8 directory would be about 30 MB at 10M, pinned in memory and read whole on every cold start | final 100K directory 139 KB (about 700 leaves); projected 4.4 MB at 10M (about 22K leaves); recall@10 unchanged at 0.984 |
| Each segment writes a fragment (`annfrag:` file) with its directory section and cluster blobs; a publication reads only new fragments and copies the other sections from the previous directory | iteration 4 reopened every segment's metadata on each publication, so ingest cost grew with segment count | test over 60 publications: zero segment-index reads, at most two fragment reads and one directory read per publication; the directory holds no blobs and is read whole |
| Exactly scanned (flat) segments prefetch their vectors in parallel with the blobs, as concurrent 1 MiB reads | the 4 MB flat-segment read ran after the blob read, a second serial wave inside the scan stage (about 230 ms) | the flat read is a cache hit (3 ms) by the time the scan needs it |

Scaling test (`directory_cost_and_publication_do_not_grow_with_segment_count`, 10 ms per request): at 6 segments cold
ANN takes 3 waves and 14 requests; at 60 segments 3 waves and 64 requests (ten times the rows, so about 3x the
probes). Waves do not grow with segments. The existing 20-segment test still sees 3 waves and zero segment-index reads.

### Results (strong consistency)

| Query | Cold p50 / p90 / p99 ms | Cold GETs/q | Cold MB/q | Warm c1 p50 / p90 / p99 ms | QPS c1 / c8 / c32 | Recall@10 |
| --- | --- | ---: | ---: | --- | --- | ---: |
| ANN | 1,044 / 1,681 / 2,142 | 138 | 13.4 | 59 / 88 / 114 | 15.1 / 101 / 124 | 0.984 |
| BM25 | 943 / 1,429 / 1,663 | 39 | 7.0 | 58 / 92 / 160 | 15.2 / 110 / 158 | |
| Hybrid | 1,023 / 1,141 / 1,161 | 178 | 20.5 | 60 / 71 / 119 | 16.3 / 83 / 107 | |
| Selective ANN | 1,120 / 1,260 / 1,301 | 229 | 7.8 | 58 / 106 / 148 | 14.4 / 97 / 172 | 1.000 |

Iteration 4 (recorded): ANN cold p50 923 (881 in its A/B run), 129 GETs, 14 MB, warm 61 ms, recall 0.984. Cold ANN
over three runs of this iteration's code was 829, 1,044, and (an earlier build) 1,088 ms, so R2 round-trip variance
between sessions is about as large as any difference here; the iteration's gain is structural (directory size,
publication cost), not a 100K latency win. Cold ANN is 3 sequential waves on R2 as well (stage trace: snapshot with the
directory about 200 ms, scan about 430 ms, rerank about 270 ms). Ingest: 100K in 122 s (816 docs/s), 0 backpressure
429s, server peak RSS 2.9 GiB. Bucket after load: 1.2 GB including WAL objects and retired files awaiting GC; fragments
add about 55 B per document (5.5 MB at 100K). Parity: milestone 3 row below.

### Remaining

- Filtered ANN (selective, broad) still opens every segment's index and ID index: 229 requests. Needs per-cluster
  filter summaries.
- Rerank is about 120 sparse ranges per query in one wave. Interleaving vectors with codes would read every probed
  row's vector (about 48 MB at 100K), so it is the wrong trade; a smaller pool or merged ranges are the levers.
- BM25 block-max skipping is not done; BM25 cost still grows with posting-list length.

## Iteration 6 (1M docs): filtered ANN through the directory, row-to-cluster maps, and a cold-read fix

First round at 1M documents (the first 1M English Wikipedia passages, the same 200 held-out passages, 1024-dim f16;
`scale/` wiped and loaded fresh; exact truth in `bench/scale/data/oracle-1m.npz`). Scale-down for cache pressure: block
cache 1 GiB and decoded metadata 512 MiB (about a tenth of the 10M settings), nprobe 64, rerank cap 32 x k, strong
consistency, cold means a fresh process with an empty disk cache (8 queries per type). R2 and VM latency moved by as much
as 40 percent between sessions in this round (the same binary measured cold ANN p50 at 1,065 and 1,520 ms), so the table
gives two full sessions of the final binary, and the baseline is iteration 5's code on the same dataset and cache sizes.

### What grew from 100K to 1M (iteration 5 code, evidence)

| Query | 100K cold p50, GETs, MB | 1M cold p50, GETs, MB | Cause |
| --- | --- | --- | --- |
| ANN | 1,044 ms, 138, 13 | 2,000 ms, 402, 24 | probes grow with sqrt(N): about 180 clusters, each its own request, plus the rerank ranges |
| BM25 | 943 ms, 39, 7 | 2,371 ms, 113, 47 | per-segment whole ID indexes (10.7 MB) and length arrays (28 MB) are read whole |
| Hybrid | 1,023 ms, 178, 20 | 2,404 ms, 518, 71 | the sum of both |
| Selective ANN | 1,120 ms, 229, 8 | 3,800 ms, 719, 118 | the filtered path opened every segment (index header plus ID index) and, because selectivity inflated the probe count, scanned nearly every cluster's codes |

Trace of one cold ANN query at 1M (`BF_TRACE_READS=1`): 433 backend reads, of which about 93 cluster blobs, about 300
code and rerank ranges, and 38 document footers. Individual requests took 200 to 300 ms each instead of 45 to 90 ms,
because hundreds were in flight together. The wave structure did not grow (manifest, then directory with the WAL probe,
then scan, then rerank); the cost grew through request count and through the next item.

**The largest cold cost was not the network.** The cache wrote every fetched 4 KB block to the disk tier with an awaited
`spawn_blocking` call before returning the read. A cold 2 MB read finished its network transfer in about 150 ms and then
spent 400 to 900 ms writing 500 block files, one at a time. In a cold BM25 trace the backend reads all finished by 650 ms
and the next stage started at 1,450 ms. Disk writes now run behind the read (bounded: at most 16 queued batches, so a cold
cache cannot buffer the whole transfer in memory), and tests call `Storage::flush_cache()` where they inspect the disk.
Without that bound, a 200K namespace that fell back to the per-segment path grew the server to 15 GB under concurrent
queries (see the open items).

### What changed

| Change | Evidence | Effect (1M, cold) |
| --- | --- | --- |
| Disk-tier writes run behind reads (bounded queue) | the trace above: network done at 650 ms, stage started at 1,450 ms | cold ANN 2,000 to about 1,100 ms; BM25 2,371 to about 1,250 ms, with no change in requests or bytes |
| Filtered ANN uses the directory (`ann_dir::search` takes per-segment allowed-row bitmaps), reading no segment index and no ID index | selective was 719 requests and 118 MB; its warm-up opened every segment | filter bitmaps and the directory load in the snapshot wave; first version (estimated counts) 330 requests, 20 MB, 1,102 ms but recall 0.803 |
| Fragments carry a row-to-cluster map (u16 per row, `BFFRG002`/`BFDIR003`): exact allowed rows per cluster, and each allowed row's vector location, without reading blobs | an estimate from row fractions gave selective recall 0.80 (needs about every matching row at 0.4 percent selectivity); exact counts keep 1.000 | selective cold p50 3,800 to about 1,450 ms, 719 to 330 requests (blob route) or 1,885 requests and 42 MB (exact route, below), recall 1.000 |
| A filter whose allowed set is at most `exact_fetch_rows` (8,192) ranks exactly those rows' vectors (`PartialIndex::exact_at`); a larger set scans the nearest clusters' codes with exact per-cluster counts and does not widen the probe count | recall must reach turbopuffer's 0.998 at selectivity 0.4 percent (4,181 of 1M rows) | the exact route reads about 4,000 sparse 2 KB ranges merged to about 1,900 requests; see the open items |
| `BF_IVF_CLUSTER_FACTOR` (default 1, so unchanged) makes leaf clusters N times larger | 200K experiment: factor 2 cut cold ANN requests 250 to 200 and MB 18 to 14, recall 0.950 to 0.944, cold p50 1,254 vs 1,161 ms (4 queries, noise) | left at 1: the recall cost is small but not free, and 1M recall is only 0.006 above turbopuffer |
| Diagnostics: `ANN snapshot` and `DIRECTORY` stage lines (`BF_TRACE_STAGES=1`), warm-task timings | found the shadowed-tail fall-back below | none |

Rejected after measuring: multipart upload for 12 MiB and larger WAL payloads (A/B at 120K docs, 16 writes in flight:
1,223 docs/s without, 1,182 with; commit append 3.8 s vs 3.2 s per commit), so ingest is limited by the VM's upload
bandwidth to R2 (about 10 to 12 MB/s), not by per-request latency. The change was reverted.

### Results at 1M (strong consistency)

Baseline is iteration 5's code on the same data; "session 2" is the final binary measured when R2 was quiet, "session 1"
the same binary when it was not.

| Query | Baseline cold p50 / p90 / p99 ms | Final cold p50 / p90 / p99 ms (session 2; session 1 p50) | Cold GETs/q | Cold MB/q | Warm c1 p50 / p90 / p99 ms | QPS c1 / c8 / c32 | Recall@10 |
| --- | --- | --- | ---: | ---: | --- | --- | ---: |
| ANN | 2,000 / 2,355 / 2,681 (406 GETs, 24 MB) | 1,065 / 1,257 / 1,268 (1,520) | 434 | 25 | 48 / 74 / 100 | 18.7 / 60.9 / 66.7 | 0.931 baseline, 0.944 final (32 queries); 0.942 over 200 |
| BM25 | 2,371 / 2,712 / 2,897 (116 GETs, 47 MB) | 1,240 / 1,428 / 1,562 (1,790) | 122 | 49 | 52 / 86 / 152 | 16.5 / 109.6 / 155.0 | |
| Hybrid | 2,404 / 2,983 / 3,444 (518 GETs, 71 MB) | 1,478 / 1,576 / 1,619 (2,702) | 553 | 74 | 53 / 84 / 202 | 15.4 / 51.3 / 52.6 | |
| Selective ANN | 3,800 / 4,310 / 4,961 (722 GETs, 118 MB) | 1,623 / 2,053 / 2,081 (1,443) | 1,888 | 42 | 131 / 193 / 225 (session 1: 48 / 64 / 90) | 7.1 / 47.1 / 86.5 (session 1: 19.4 / 70.1 / 88.3) | 1.000 |

Baseline warm: ANN 53 ms (QPS 17.5 / 70.3 / 69.7), BM25 51 ms (16.9 / 113 / 135), hybrid 65 ms (14.6 / 44.7 / 51.0), selective
59 ms (15.7 / 28.6 / 29.3). Cold waves (sequential read stages, from the read trace): ANN 4 (manifest; directory with the
WAL probe; blobs, codes, and document footers; rerank), BM25 4 (manifest; index headers, ID indexes, lengths, and text
statistics; term records; postings), hybrid the larger of the two plus statistics, selective about 5 (manifest; directory;
row-to-cluster maps; vectors; hydration). Cold ANN meets the target of under 500 requests but not 3 waves. Ingest of 1M:
1,695 s (590 docs/s) with 2 writes in flight, against 1,351 s (740 docs/s) with 16, 0 backpressure 429s both times;
indexing rewrote 2.09M rows (baseline) and 1.61M rows (final) of segments; the bucket held 12.4 GB after the load
(12.4 KB per document, including retired files awaiting GC; the documents file holds the vector column as well as the
index file's full vectors). The loader's write rate fell from about 1,980 docs/s at 80K rows to about 400 docs/s at 900K rows (WAL
append of 24 MB per group commit at the VM's upload bandwidth, plus `prepare` and ID-index lookups growing with N).

### Blowfish against turbopuffer, by scale

turbopuffer numbers are from "turbopuffer alone at 1M and 10M" below (hot or warm: it cannot be forced cold). Blowfish
warm is a local process, read-only, with the scaled-down caches described above, strong consistency (one R2 HEAD per
query), measured at one request at a time; blowfish cold is a fresh process and an empty disk cache.

| Scale | Query | blowfish cold p50 / p90 (ms) | blowfish warm p50 / p90 / p99 (ms) | turbopuffer client p50 / p90 / p99 (ms) | turbopuffer `server_total_ms` p50 | recall@10 blowfish / turbopuffer |
| --- | --- | --- | --- | --- | --- | --- |
| 100K | ANN | 1,044 / 1,681 (iteration 5) | 53 / 72 / 114 | 73 / 81 / 123 | 15 | 0.984 / 0.963 |
| 1M | ANN | 1,065 / 1,257 | 51 / 77 / 114 | 65 / 72 / 87 | 13 | 0.942 / 0.938 |
| 1M | BM25 | 1,240 / 1,428 | 56 / 76 / 146 | 64 / 69 / 81 | 13 | |
| 1M | Hybrid | 1,478 / 1,576 | 57 / 81 / 184 | 66 / 72 / 100 | 13 | |
| 1M | Selective ANN | 1,623 / 2,053 | 51 / 76 / 141 | 66 / 71 / 106 | 13 | 1.000 / 0.998 |
| 1M | Broad ANN | not measured cold | 62 / 75 / 120 | 66 / 70 / 99 | 13 | 0.973 / 0.977 |
| 10M | (turbopuffer alone) ANN | | | 150 / 277 / 492 | 88 | 0.943 |

Kyle's target for the climb is turbopuffer's published 10M cold numbers plus 40 percent: p50 at most about 1,224 ms and
p90 about 1,700 ms. At 1M, cold ANN p50 is 1,065 ms (p90 1,257) when R2 is quiet and 1,520 ms in the noisy session; BM25
is 1,240 / 1,428; hybrid and selective are above the p50 target (1,478 and 1,623).

### Parity at 1M against `blowfish-oracle-wiki1m` (milestone 4, 200 queries per type)

| Query | overlap@10 | top-1 agree | recall@10 blowfish / turbopuffer | blowfish client p50 / p90 / p99 (ms) | turbopuffer client p50 / p90 / p99 (ms) |
| --- | ---: | ---: | --- | --- | --- |
| ANN | 0.929 | 0.945 | 0.942 / 0.938 | 51 / 77 / 114 | 65 / 72 / 87 |
| BM25 | 0.990 | 0.995 | | 56 / 76 / 146 | 64 / 69 / 81 |
| Hybrid (RRF) | 0.969 | 0.910 | | 57 / 81 / 184 | 66 / 72 / 100 |
| Selective ANN | 0.998 | 1.000 | 1.000 / 0.998 | 51 / 76 / 141 | 66 / 71 / 106 |
| Broad ANN | 0.966 | 0.970 | 0.973 / 0.977 | 62 / 75 / 120 | 66 / 70 / 99 |

Broad ANN is 0.004 below turbopuffer. Before this round's last change (code scans widen the probe count in proportion to
how few rows the filter leaves, at most 3 times) it was 0.941; the change costs requests only on filtered queries that
take the code route. An earlier measurement in the same session, with the caches at the same size but R2 busy, gave
blowfish client p50 130 to 140 ms for every type, because 200 distinct queries do not fit the 1 GiB block cache at 1M
documents. Both sides' numbers are one session.

### Open items found in this round

- **Any unindexed tail upsert disables the directory.** `NamespaceState::shadowed` marks every tail ID, whether or not it
  replaces a segment row, and `ann::search` only takes the directory path when it is empty. A 200K namespace with 58K
  unindexed docs answered ANN in 2 to 12 s and, at 8 concurrent queries, grew the server to 15 GB. Mitigation: only
  shadow IDs that exist in a segment (a bloom check on each ID index), or search the directory with the tail's IDs
  removed by lookup.
- **BM25 bytes grow linearly with N**: ID indexes (10.7 MB at 1M) and length arrays (28 MB) are read whole per segment,
  and corpus statistics 5.9 MB. At 10M that is about 500 MB per cold BM25 query.
- **Selective filters read about 1,900 requests** at 0.4 percent selectivity (exact route over 4,181 rows). At 10M that is
  about 20,000 requests. Per-cluster filter summaries, or a copy of the filtered set's vectors, would replace it.
- **Ingest is bounded by upload bytes per document**: the WAL alone was 3.0 GB for 1M documents, and segment builds
  covered 1.6M rows. Store vectors once and compact less often.

## Iteration 7 (1M docs): ingest that no longer stalls, and an unindexed tail that keeps the directory

Second round at 1M (same 1M passages, 200 held-out queries, `scale/` wiped and reloaded with the final binary; truth from
the loader, parity against `blowfish-oracle-wiki1m` and `bench/scale/data/oracle-1m.npz`). Same cache sizes as iteration 6
(1 GiB block cache, 512 MiB decoded metadata), nprobe 64, rerank cap 32 x k, strong consistency, cold means a fresh process
with an empty disk cache (8 queries per type). Two full matrix sessions of the final binary are given, because R2 latency
moved by up to 40 percent between sessions.

### Where ingest time went (measured, then fixed)

A local file-store load of 450K rows (so no network limit) with stage timers in the indexer
(`BF_TRACE_STAGES=1` prints `idx ...` lines) and the commit-stage histograms showed the writers, not the builders, were
the limit:

| Stage (per cycle or commit, 300K to 450K rows) | Before | After |
| --- | --- | --- |
| Indexer `state.clone()` under the namespace lock (deep copy of every tail document) | 1.2 to 1.6 s | 0.05 s |
| `prepare` per commit (clones the tail, then diffs every tail document against the batch) | 650 ms average | about 390 ms (the rest is normalizing 2,000-document batches) |
| Final `catch_up` after publication (rebuild from the manifest, re-download the tail) with writes locked out | 1 to 5.5 s | 0 (adopts its own checkpoint in memory) |
| Text statistics (`adjust`) after the build, serial | 0.6 to 1.7 s | overlapped with the builds |
| Flush and compaction builds | serial | concurrent |
| Local ingest, 450K rows | about 2,100 docs/s, falling | 3,650 docs/s |

On R2 the remaining limit is bytes per document through one WAL stream. A flush cycle also downloaded the whole tail again
after every publication, and garbage collection (every 60 s) replayed the whole WAL from scratch: 3.1 GB of GETs at 560K
rows; 5.1 GB by the end of the 1M load, and 1.5 GB with the fixes.

### What changed

1. **Tail documents are shared** (`NamespaceState::docs` holds `Arc<Document>`), so cloning a state copies pointers.
2. **Writes read only the rows they name**: `for_writes` builds the write's working state from the request's IDs (tail rows
   by ID, segment rows by lookup) instead of cloning the whole tail, and `prepare` compares only touched rows.
3. **Indexer cycle**: flush build, compaction build, and the tail's text statistics run concurrently; after publishing, the
   node adopts its own checkpoint in memory (`NamespaceState::fast_forward`, tail IDs trimmed by their last-write sequence)
   instead of replaying the WAL; a republished manifest with the same checkpoint (garbage collection, epoch claims) no longer
   discards the resident tail; garbage collection starts from the resident state.
4. **Bulk loads build big segments**: while writes are still arriving, the tail accumulates up to `bulk_flush_rows`
   (100,000, `BF_BULK_FLUSH_ROWS`) or `bulk_flush_age` (60 s) before a segment is built, and compaction waits for writes
   to pause (up to 64 segments). Ignored when `index_age` is under a second (forced-indexing tests).
5. **WAL payloads of 8 MiB or more upload as concurrent 5 MiB parts** (the key is unique per attempt).
6. **An unindexed tail keeps the ANN directory path.** Shadowed rows are dropped by ID from the winners (asking for
   `k + max(2k, 20)` winners), with a fall back to the per-segment path only if shadowed rows leave fewer than k while the
   pool was full. Filtered queries skip the per-segment shadow lookup (`snapshot::allowed_with`). Test:
   `unindexed_tail_keeps_the_directory_path` (new rows, overwrites, and deletes of the query's own nearest neighbours; no
   `.bfi` read; at most 4 waves).
7. **BM25 hits for big segments take their IDs from the `id` column of the documents file** (winning rows only) instead of
   reading the whole ID index, for ID indexes over 1 MiB. Smaller ones are still read whole, because the ranged read costs
   one more sequential wave.

### Ingest of 1M (batch 2,000, 16 writes in flight, loader in a 6 GiB scope, server in a 16 GiB scope)

| | Iteration 6 | Iteration 7 |
| --- | --- | --- |
| Loader wall time, 1M rows | 1,351 s | 834 s |
| docs/s over the whole load | 740 | 1,199 |
| docs/s at about 160K rows, then over the last 100K rows | 1,980, then 400 | 1,480, then about 1,150 |
| Fully indexed (including compaction) after the load starts | 1,351 s | 1,247 s |
| 429s | 0 | 0 |
| Server peak RSS | not captured (scope 11 GB with page cache) | about 3.1 GB |
| Segment rows built per row ingested | 1.6 to 2.1 | about 1.0 at load end, then compaction |
| WAL | 3.0 GB | 3.0 GB (3.0 KB per document) |
| Live segment bytes | 12.4 KB per document in the bucket | 5.4 KB per document of live segments; 13.2 GB bucket before garbage collection |

Intermediate runs on the way (same 1M load): 1,109 docs/s after items 1 to 3 alone (fully indexed 1,360 s), and 1,155
docs/s after adding items 4 and 5. The load is now limited by upload bytes (the WAL commit is one serialized stream, and
the indexer shares the link), not by the indexer. The 1,500 docs/s target is not met on this VM's link to R2.

### Results at 1M (strong consistency; cold = fresh process, 8 queries)

| | ANN | BM25 | Hybrid | Selective |
| --- | --- | --- | --- | --- |
| Iteration 6 cold p50 / p90 / p99 ms (session 2) | 1,065 / 1,257 / 1,268 | 1,240 / 1,428 / 1,562 | 1,478 / 1,576 / 1,619 | 1,623 / 2,053 / 2,081 |
| Iteration 7 cold, session 1 | 1,410 / 1,722 / 1,922 | 1,854 / 2,962 / 3,275 | 1,525 / 1,631 / 1,757 | 1,768 / 2,944 / 4,687 |
| Iteration 7 cold, session 2 | 1,118 / 1,233 / 1,250 | 1,212 / 1,274 / 1,347 | 1,652 / 2,779 / 2,945 | 1,699 / 3,039 / 3,533 |
| Cold GETs / MB (before to after) | 434 / 25 to 375 / 23.5 | 122 / 49 to 126 / 38 | 553 / 74 to 493 / 61 | 1,888 / 42 to 1,949 / 43 |
| Warm c1 p50 / p90 / p99 ms (session 2) | 59 / 77 / 126 | 61 / 77 / 154 | 52 / 79 / 146 | 60 / 96 / 187 |
| QPS c1 / c8 / c32 (session 2) | 15.6 / 70.3 / 78.7 | 15.2 / 65.5 / 133.5 | 16.4 / 32.6 / 36.3 | 13.8 / 86.8 / 108.4 |
| Recall@10 | 0.966 (200 queries) | | | 1.000 |

The same session also re-measured iteration 6's namespace before it was wiped: cold ANN p50 1,168 ms (431 GETs, 25 MB) and BM25
1,255 ms (119 GETs, 49 MB), within noise of session 2 of iteration 7. Cold p50 is therefore flat for ANN and BM25 in a quiet
session (1,118 and 1,212 ms against a target of about 1,224 ms), and noisy session 1 (1,410 and 1,854 ms) shows the 40 percent
spread. Hybrid and selective are above the target in both sessions. Waves were not re-measured (the wave tests pass unchanged), and requests fell about 14 percent
for ANN because a 1M namespace now has 11 larger segments instead of 14 (compaction no longer runs during the load).

### Parity at 1M against `blowfish-oracle-wiki1m` (200 queries per type)

| | ANN | BM25 | Hybrid | Selective | Broad |
| --- | --- | --- | --- | --- | --- |
| Overlap@10 with turbopuffer | 0.924 | 0.990 | 0.965 | 0.999 | 0.966 |
| Recall@10, blowfish vs turbopuffer | 0.949 vs 0.939 | | | 1.000 vs 0.999 | 0.978 vs 0.977 |
| Warm client p50 ms, blowfish vs turbopuffer | 56 vs 68 | 57 vs 66 | 57 vs 67 | 60 vs 68 | 64 vs 67 |

Blowfish's recall is at or above turbopuffer's at every size measured. Broad filter recall, which was 0.004 short at 1M in
iteration 6, is now 0.001 above.

### Not done (and why)

- **Vectors are still stored twice per segment** (documents file and ANN file): 2.1 of the 5.9 KB per document. Reading them
  back from the ANN file (hydration and compaction) was not started.
- **BM25 bytes** are 38 MB per cold query, not under 10 MB: the per-segment text dictionaries (about 28 MB) and corpus
  statistics (5.9 MB, a single FST) are still read whole, and they need a chunked format in `bf-text`.
- **Selective filters** still need about 1,950 requests (exact route over 4,181 rows).
- **Cold ANN** is still 4 waves and about 375 requests; `BF_IVF_CLUSTER_FACTOR` was not re-evaluated.

## turbopuffer reference (100K)

The real turbopuffer holds the same 100K documents (the first 100K English Wikipedia passages, 1024-dim f16, the same
200 held-out passages excluded) in one long-lived namespace, `blowfish-oracle-wiki100k`. It was loaded with
`crates/bf-engine/examples/scale_load.py` pointed at turbopuffer, using the same schema as the blowfish load (the
loader now sends the vector attribute as `{"type": "[1024]f16", "ann": true}`, which both services accept; the real
service requires `ann`). The dataset never changes, so every speed milestone reuses it.

Run `bench/scale/oracle_compare.py` (see its docstring) with `TPUF_URL` set to the turbopuffer base URL. It runs 200
held-out queries per type (ANN, BM25, hybrid ANN plus BM25 with RRF, selective filtered ANN, and broad filtered ANN,
all `top_k` 10, strong consistency) once against each side, alternating per query. Ground truth for recall is the exact
cosine top-10 computed by the loader (checked in as `bench/scale/data/oracle-100k.npz`); BM25 and hybrid have no exact
truth, so they only have parity numbers. Overlap divides by the larger result size, because a rare BM25 term can match
fewer than 10 documents.

Reading the numbers: turbopuffer requests go through a proxy and over the internet, so its client latency includes
that path, while `server_total_ms` does not. turbopuffer cannot be forced cold, so the table records the cache
temperature it reports. Blowfish was warmed with one unmeasured pass and is a local process (the 100K `scale/`
namespace, read-only, 256 MiB block cache, 128 MiB decoded-metadata cache, default nprobe 64 and rerank 32), so its
client latency has no network path to the client but includes the strong-consistency check against R2.

### Milestone 0: current main (`e12178f`), 2026-11-16

Parity, blowfish vs turbopuffer (200 queries per type):

| Query | overlap@10 | min overlap | top-1 agree | same-rank@10 | mean rel score diff | recall@10 blowfish | recall@10 turbopuffer |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ANN | 0.963 | 0.7 | 0.970 | 0.848 | 0.0000 | 0.980 | 0.963 |
| BM25 | 0.982 | 0.7 | 1.000 | 0.875 | 0.0167 | | |
| Hybrid (RRF) | 0.973 | 0.6 | 0.955 | 0.823 | 0.0200 | | |
| Selective ANN | 0.998 | 0.9 | 1.000 | 0.995 | 0.0000 | 1.000 | 0.998 |
| Broad ANN | 0.992 | 0.8 | 1.000 | 0.963 | 0.0000 | 0.995 | 0.996 |

Latency, ms p50 / p90 / p99, one request at a time:

| Query | blowfish client | turbopuffer client | turbopuffer `server_total_ms` | turbopuffer cache temperature |
| --- | --- | --- | --- | --- |
| ANN | 76 / 120 / 179 | 73 / 81 / 123 | 15 / 21 / 41 | 200 hot |
| BM25 | 73 / 110 / 191 | 70 / 89 / 115 | 14 / 32 / 59 | 200 hot |
| Hybrid | 78 / 117 / 261 | 74 / 97 / 139 | 16 / 34 / 54 | 200 hot |
| Selective ANN | 74 / 123 / 232 | 70 / 80 / 123 | 14 / 19 / 26 | 200 hot |
| Broad ANN | 74 / 110 / 166 | 72 / 83 / 158 | 15 / 23 / 38 | 200 hot |

Notes:

- ANN set differences are approximation noise on both sides: both miss about 2 to 4 percent of the exact top-10, and
  the ids they miss differ. Same-rank is low only because one missing id shifts every later position; $dist values
  of shared ids are identical.
- BM25 orders results the same way (top-1 agreement 1.000), but blowfish scores run about 1.5 to 2 percent below
  turbopuffer for most queries. The likely cause is corpus statistics (average document length or IDF); the default
  tokenizer settings match. The hybrid score diff is that gap passing through RRF plus rank shifts.
- Blowfish's warm client p50 is within a few ms of turbopuffer's client p50, but its tail is longer (p99 166 to 261 ms
  against 115 to 158 ms), and turbopuffer's own server time is only about 15 ms, so the floor for blowfish is the
  network round trip to R2 for the strong-consistency check.

### turbopuffer alone at 1M and 10M, 2026-11-16

Namespaces `blowfish-oracle-wiki1m` and `blowfish-oracle-wiki10m` hold the first 1M and 10M passages (the same 200
held-out passages excluded; same schema, 1024-dim f16). Loaded with `scale_load.py` (batch 2000, 4 writes in flight,
f16, no 429s): 1M in 780 s (1,282 docs/s), 10M in 6,467 s (1,546 docs/s, 45.0 GB raw). Measured with
`oracle_compare.py --tpuf-only --data bench/scale/data/oracle-<size>.npz` (built by `make_oracle_truth.py` from the
loader's exact truth), 200 queries per type, one pass, one request at a time, no blowfish side.

| Scale | Query | recall@10 |
| --- | --- | ---: |
| 100K | ANN | 0.963 |
| 100K | Selective ANN | 0.998 |
| 100K | Broad ANN | 0.996 |
| 1M | ANN | 0.938 |
| 1M | Selective ANN | 0.998 |
| 1M | Broad ANN | 0.977 |
| 10M | ANN | 0.943 |
| 10M | Selective ANN | 0.993 |
| 10M | Broad ANN | 0.971 |

Latency, ms p50 / p90 / p99 (100K rows are from Milestone 0 above):

| Scale | Query | turbopuffer client | turbopuffer `server_total_ms` | cache temperature |
| --- | --- | --- | --- | --- |
| 100K | ANN | 73 / 81 / 123 | 15 / 21 / 41 | 200 hot |
| 100K | BM25 | 70 / 89 / 115 | 14 / 32 / 59 | 200 hot |
| 100K | Hybrid | 74 / 97 / 139 | 16 / 34 / 54 | 200 hot |
| 100K | Selective ANN | 70 / 80 / 123 | 14 / 19 / 26 | 200 hot |
| 100K | Broad ANN | 72 / 83 / 158 | 15 / 23 / 38 | 200 hot |
| 1M | ANN | 68 / 77 / 104 | 12 / 16 / 25 | 200 hot |
| 1M | BM25 | 67 / 78 / 96 | 12 / 17 / 30 | 200 hot |
| 1M | Hybrid | 69 / 89 / 101 | 12 / 16 / 24 | 200 hot |
| 1M | Selective ANN | 69 / 102 / 152 | 12 / 23 / 83 | 200 hot |
| 1M | Broad ANN | 68 / 93 / 101 | 12 / 15 / 19 | 200 hot |
| 10M | ANN | 150 / 277 / 492 | 88 / 203 / 390 | 182 warm, 17 hot, 1 cold |
| 10M | BM25 | 125 / 231 / 368 | 65 / 176 / 303 | 109 hot, 87 warm, 4 cold |
| 10M | Hybrid | 108 / 214 / 349 | 53 / 159 / 271 | 114 hot, 84 warm, 2 cold |
| 10M | Selective ANN | 238 / 444 / 1027 | 172 / 361 / 923 | 148 warm, 51 hot, 1 cold |
| 10M | Broad ANN | 169 / 352 / 539 | 110 / 289 / 435 | 165 warm, 35 hot |

Cold-ish samples: one query per type right after each load finished, before any measured pass (client ms /
`server_total_ms` / temperature).

| Scale | ANN | BM25 | Hybrid | Selective ANN | Broad ANN |
| --- | --- | --- | --- | --- | --- |
| 1M | 129 / 11 / hot | 67 / 13 / hot | 83 / 8 / hot | 99 / 32 / hot | 101 / 21 / hot |
| 10M | 518 / 405 / warm | 321 / 267 / warm | 311 / 207 / hot | 551 / 448 / warm | 753 / 650 / warm |

Reading: at 1M turbopuffer is flat with 100K (server about 12 ms, always hot, so client latency is the network path).
At 10M the first pass is not hot: ANN is mostly warm and the server time p50 rises to 52 to 172 ms with p99 up to
923 ms (selective ANN is the slowest), so the tail grows 5 to 10 times. ANN recall@10 stays near 0.94 (0.963, 0.938,
0.943); filtered recall holds. `bench/scale/bm25_idf_tpuf_only.py` estimates the BM25 idf offset from turbopuffer
alone (approximate, about +/-0.05 per term): the constant +0.169 plateau seen at 100K does not appear at 1M or 10M.
Median offsets over 40 title terms are +0.035 at 1M and +0.049 at 10M, against about +0.13 at 100K with the same
method, and the large offsets (+0.33 at 1M) belong to the rarest terms (document frequency 2 to 7). So the 100K
offset is not a fixed "18.5 percent more documents" factor that scales with N.

### Per-milestone rows

Add one row per milestone with the same 200 queries (blowfish client p50 / p90 / p99, then overlap@10 against
turbopuffer). turbopuffer's numbers do not change between milestones except for its own variance.

| Milestone | Commit | ANN | BM25 | Hybrid | Selective ANN | Broad ANN |
| --- | --- | --- | --- | --- | --- | --- |
| 0: iteration 2 main | `e12178f` | 76 / 120 / 179 ms, 0.963 | 73 / 110 / 191 ms, 0.982 | 78 / 117 / 261 ms, 0.973 | 74 / 123 / 232 ms, 0.998 | 74 / 110 / 166 ms, 0.992 |
| 1: iteration 3 | `73d8110` | 87 / 116 / 155 ms, 0.965 | 90 / 129 / 201 ms, 0.982 | 100 / 133 / 299 ms, 0.972 | 98 / 136 / 216 ms, 0.998 | 100 / 131 / 234 ms, 0.991 |
| 2: iteration 4 (ANN directory) | `f1a7db3`+ | 68 / 91 / 191 ms, 0.962 | 66 / 99 / 198 ms, 0.982 | 65 / 90 / 173 ms, 0.971 | 64 / 88 / 134 ms, 0.998 | 57 / 74 / 156 ms, 0.991 |
| 3: iteration 5 (one-bit directory, fragments) | `af04e66`+ | 53 / 72 / 114 ms, 0.968 | 61 / 86 / 193 ms, 0.982 | 60 / 87 / 165 ms, 0.975 | 54 / 78 / 137 ms, 0.998 | 61 / 86 / 106 ms, 0.993 |

### Milestone 4 at 1M (iteration 6)

The milestone table above is at 100K. The 1M rows are the "Parity at 1M" table in the iteration 6 section: overlap@10
ANN 0.929, BM25 0.990, hybrid 0.969, selective 0.998, broad 0.966; recall@10 ANN 0.942 (turbopuffer 0.938), selective
1.000 (0.998), broad 0.973 (0.977).

## Iteration 8 (1M docs): BM25 bytes that no longer grow with the data

Third round at 1M (same 1M passages, 200 held-out queries; `scale/` wiped and reloaded twice, the second time with the
final text format; truth from the loader, parity against `blowfish-oracle-wiki1m` and `bench/scale/data/oracle-1m.npz`).
Same settings as iteration 7 (1 GiB block cache, nprobe 64, rerank cap 32 x k, strong consistency, cold means a fresh process
with an empty disk cache, 8 queries per type). R2 latency in this round was noisy (cold ANN, with the same 374 requests,
ranged from 1,472 ms to 2,656 ms between two sessions an hour apart), so both final-layout sessions are given.

### What changed

1. **Chunked text dictionaries (`bf-text` segment version 3).** Each segment's FST dictionary is cut into chunks of 2,048
   terms, each its own FST with a crc32, plus an index (first term, offset, length, crc) in the metadata tail. A query reads
   only the chunks that hold its terms. Document lengths are bit-packed per 128-row block (about 1 byte per row, was 4).
   Evidence before the change (one cold BM25 query, 15 segments, `BF_TRACE_READS`): 119 FTS reads of 5.9 MB (mostly the
   whole-segment lengths and dictionaries), 2.3 MB of whole ID indexes, 1.1 MB of docs, 0.6 MB of live masks, 0.13 MB of
   statistics.
2. **Chunked corpus statistics (`BFSTAT02`).** FST chunks of 4,096 terms; the chunk index (first term, offset, length, crc
   per chunk, 8.7 KB at 1M) rides in the manifest, so it is in the snapshot wave and a query reads only the chunks that hold
   its terms. The legacy whole-object format still reads.
3. **Snapshot-wave prefetch of query terms.** The open hint keeps each BM25 clause; the warm-up tokenizes it with the
   manifest's schema, fetches the statistics chunks, and per segment opens the text index and runs the term lookups
   (dictionary chunks and term records). Hybrid queries no longer wait for statistics before the ANN half.
4. **ID indexes over 128 KiB** (was 1 MiB) are not read whole by BM25 queries: hits take IDs from the docs file's `id` column.
5. **Compaction ends bulk loads in a few large segments.** A tier that cannot reach a full group of four under the 200K-row
   cap now merges its pair, and when no tier is full the smallest sizeable segments (at least an eighth of the cap) merge
   across tiers. The first reload ended with 15 segments of 40K to 100K rows (the old tier rule never merged them), which
   cost 488 cold ANN requests and recall 0.934; after the new rule: 9 segments (2K, 20K, 22K, 112K, 144K, 166K, 166K, 172K, 196K).

Tests: `chunked_dictionary_reads_only_the_query_terms_chunks` (bf-text; terms across chunk boundaries, prefixes spanning
chunks, bytes read under a tenth of the object), `cold_bm25_reads_term_chunks_not_whole_dictionaries` (bf-engine; six
segments of 9,000 terms each, bytes read about 2 percent of the FTS and statistics bytes and independent of how many
segments hold other terms), `out_of_line_text_statistics_round_trip_and_restrict_to_query_terms` (three chunks), and
`compaction_merges_pairs_that_cannot_grow_to_a_full_group`.

### Cold queries at 1M

| | Iteration 7 (session 2) | Iteration 8, session A | Iteration 8, session B |
| --- | --- | --- | --- |
| ANN p50 / p90 / p99 ms | 1,118 / 1,233 / 1,250 | 2,656 / 3,885 / 4,138 | 1,472 / 1,954 / 2,614 |
| BM25 p50 / p90 / p99 ms | 1,212 / 1,274 / 1,347 | 1,666 / 2,613 / 3,469 | 1,329 / 1,599 / 1,764 |
| Hybrid p50 / p90 / p99 ms | 1,652 / 2,779 / 2,945 | 1,605 / 1,776 / 1,835 | 1,736 / 2,047 / 2,158 |
| Selective p50 / p90 / p99 ms | 1,699 / 3,039 / 3,533 | 2,474 / 3,098 / 3,287 | 2,070 / 2,691 / 3,246 |
| GETs / MB: ANN | 375 / 23.5 | 374 / 22.6 | 374 / 22.6 |
| GETs / MB: BM25 | 126 / 38 | 144 / 6.5 | 144 / 6.5 |
| GETs / MB: hybrid | 493 / 61 | 507 / 27.9 | 507 / 27.9 |
| GETs / MB: selective | 1,949 / 43 | 1,899 / 41.9 | 1,899 / 41.9 |
| Waves (sweep of one traced cold query): ANN, BM25, hybrid, selective | 4, n/a, n/a, n/a | 4, 3, 5, 5 | |

BM25 cold bytes fell 83 percent (38 MB to 6.5 MB, under the 10 MB target) and hybrid bytes 54 percent. Latency did not
follow in the noisy sessions (the same-session iteration 7 baseline was not available: R2 moved by a factor of two between
sessions); with requests flat and bytes down, the remaining cold time is round trips, not transfer.

### Warm, recall, and parity

| | Iteration 7 (session 2) | Iteration 8, session B |
| --- | --- | --- |
| Warm c1 p50 ms: ANN, BM25, hybrid, selective | 59, 61, 52, 60 | 113, 96, 71, 77 |
| QPS c1 / c8 / c32: ANN | 15.6 / 70.3 / 78.7 | 8.6 / 48.0 / 54.5 |
| QPS c1 / c8 / c32: BM25 | 15.2 / 65.5 / 133.5 | 10.7 / 53.4 / 74.1 |
| Recall@10 vs loader truth: ANN, selective | 0.949, 1.000 | 0.944, 1.000 |
| Recall@10 vs oracle truth: ANN (tpuf), selective (tpuf), broad (tpuf) | 0.949 (0.939), 1.000 (0.999), 0.978 (0.977) | 0.952 (0.938), 1.000 (0.998), 0.974 (0.977) |
| Overlap with tpuf: ANN, BM25, hybrid, selective, broad | 0.924, 0.990, 0.965, 0.999, 0.966 | 0.935, 0.990, 0.971, 0.998, 0.963 |

Warm latency was slower this round on the same code paths (the warm query makes one HEAD to R2, so it tracks the network);
I did not isolate it from the session noise.

### Ingest of 1M (batch 2,000, 16 writes in flight)

| | Iteration 7 | Iteration 8 |
| --- | --- | --- |
| Loader wall time, docs/s | 834 s, 1,199 | 903 s, 1,107 (first load: 863 s, 1,158) |
| 429s | 0 | 0 |
| Server peak RSS | about 3.1 GB | 5.4 GB (the first load of this round peaked at 9.6 GB; not investigated) |
| Segments when the load ends, before compaction | not recorded | 15 |

Ingest did not improve: it is bound by bytes per document through the WAL (item 4 of the brief, not started).

### What is left

See `WORKER_REPORT.md` of this round for the ranked list. In short: selective filters still read about 1,900 ranges in one
wave (3 s); cold ANN is still 374 requests in 4 waves; per-segment lengths are still read whole at open; the indexer still
rewrites the whole statistics object per publication; vectors are still stored twice.

## Iteration 9 (1M docs): paying R2's tail latency, not its median

Goal (from the brief): cold p50 within 40 percent of turbopuffer's published 10M numbers (p50 874 ms, p90 1,214 ms), so
p50 at most about 1,224 ms and p90 at most about 1,700 ms. The namespace is iteration 8's 1M load (9 segments), reused
unchanged: nothing in this round changes a format.

### The R2 floor, measured from this VM (`crates/bf-storage/examples/r2_floor.rs`)

The tool uses the same `object_store` client blowfish uses (HTTP/1.1, one connection per concurrent request) against one
317 MB object. One R2 request from this VM, warm connection, sequential:

| Size | p50 ms | p90 | p99 |
| ---: | ---: | ---: | ---: |
| 4 KB | 72 | 110 | 309 |
| 64 KB | 72 | 98 | 210 |
| 1 MB | 96 | 190 | 242 |
| 4 MB | 130 | 325 | 651 |

A wave of N parallel ranged GETs (wall time of the slowest, median of 5 waves; warm pool unless noted):

| N | 4 KB objects | 64 KB | 256 KB | Fresh connections, 4 KB | Fresh, 64 KB |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 77 | 129 | 92 | 142 | 167 |
| 8 | 149 | 151 | 179 | 349 | 223 |
| 32 | 174 | 210 | 194 | 317 | 246 |
| 128 | 355 | 369 | 401 | 484 | 388 |
| 256 | 291 | 374 | 720 | 1,052 | 520 |
| 1,024 | 553 | 983 | 17,925 | 1,311 | 2,161 |

What the floor says:

- A single request is about 70 to 80 ms whatever its size up to 64 KB. A wave of N is the slowest of N draws, so the wave
  time is the tail of the latency distribution: about 150 ms at N=8, 175 to 290 at 32, 300 to 700 at 256, and 550 ms to
  over 2 s at 1,024. The coordinator's diagnosis (we pay the tail) holds.
- **Connections matter as much as the tail.** The client speaks HTTP/1.1, so a wave of N needs N connections, and a new TLS
  connection costs about 65 ms. A cold process pays that for every request of its first big wave: a 256-request wave takes
  about 1.05 s on fresh connections and 0.3 to 0.4 s on warm ones. `object_store` keeps idle connections for 90 s by default,
  so any idle server goes cold again. HTTP/2 (`with_allow_http2`) did not beat a warm HTTP/1.1 pool (256 x 4 KB: 537 ms
  against 291 ms) and was not adopted.
- **Bytes cost real time too: the VM-to-R2 path tops out around 60 to 180 MB/s.** Waves of 1 MB objects take 3 s at N=128
  (128 MB) and 9.5 s at N=256, and 4 MB x 128 takes 8.6 s. Merging ranges across a wide gap is therefore not free. The
  A/B below (cold ANN, `BF_READ_GAP_KB`) shows it: gap 64 KB (current) 1,013 ms p50 with 356 requests and 22 MB, gap 256 KB
  1,118 ms with 229 requests and 40 MB, gap 1 MB 1,481 ms with 153 requests and 90 MB. A marginal request costs about
  1 ms in a warm wave and a marginal MB about 10 ms, so the break-even gap is near 100 KB, which is about where it is.

### What changed

1. **Warm connection pool** (`StorageConfig::warm_connections`, default 256 for S3, `BF_WARM_CONNECTIONS`): at startup and
   every 45 s, 256 parallel HEADs of a missing key keep that many idle connections open, and the pool's idle timeout is
   10 minutes. A long-running server keeps connections after any big wave anyway; this makes a restart or an idle period
   not cost a TLS handshake per request. Cost: about 5.7 HEADs per second, about 15M a month, which is about 5 dollars of R2
   class B operations. The A/B runs wait 1.5 to 4 s after start for the pool to fill, as a warmed server would have.
2. **Hedged ranged reads** (`bf-storage/src/hedge.rs`, `BF_HEDGE_BUDGET`, default 0.1): a block-cache fetch of at most
   1 MiB that has not answered by the p90 of the last 512 fetches (at least 60 ms; 250 ms before 24 samples) gets one duplicate
   request, and the first answer wins. A token bucket earns 0.1 token per fetch (capacity 64), so extra requests stay at
   or under 10 percent. Metric: `blowfish_object_store_hedges_total`. Tests: a stalled first attempt is beaten, the budget
   caps hedges, and large or disabled reads are never hedged.
3. **Hybrid queries start their ANN half without waiting for BM25 term warm-up** (`snapshot.rs`): the snapshot wave used to
   join the dictionary-chunk and term-record prefetch for every segment before the query began, so the ANN probes of a
   hybrid query started about 400 ms after the directory had arrived. The text half of the warm-up now continues in the
   background (its reads are single-flighted with the query's own), and the snapshot waits only for the directory work.
   Cold hybrid went from 4 waves (5 in some traces) to 2.3.
4. Harness: `bench/scale/ab.py` (same-session A/B: alternates arms query by query, each on a fresh server process with an empty
   disk cache, rotating the order), `waves.py` (waves from `BF_TRACE_READS`), `parity.sh` (recall and parity against the
   oracle), and `r2_floor.rs`. `BF_READ_GAP_KB` overrides every merge gap for experiments.

### Cold queries at 1M, same-session A/B (fresh process and empty disk cache per query, alternating arms)

Base is iteration 8's binary; final has all three changes. n is the number of queries per arm. R2 was about 20 percent slower in
this session than in the exploratory runs, so compare rows within a table, not across tables.

| Query (n) | Base p50 / p90 / p99 ms | Final p50 / p90 / p99 ms | Requests | MB | Waves base, final |
| --- | --- | --- | ---: | ---: | --- |
| ANN (24) | 1,392 / 1,950 / 2,146 | 1,278 / 1,593 / 2,503 | 339 | 22.0 | 4.1, 4.1 |
| BM25 (24) | 1,069 / 1,816 / 3,020 | 869 / 1,594 / 1,994 | 310 | 9.2 | 3.4, 3.2 |
| Hybrid (24) | 1,635 / 2,714 / 3,298 | 1,188 / 1,752 / 2,138 | 635 | 29.8 | 4.0, 2.3 |
| Selective (16) | 1,958 / 2,642 / 2,949 | 1,973 / 2,600 / 2,731; with 1,024 warm connections 1,429 / 1,940 / 2,390 | 1,904 | 41.9 | 4.1, 4.1 |

Earlier smaller A/B blocks in the same afternoon (n=12 to 16) gave the same ordering with faster R2: ANN 1,162 to 1,013 ms
(base to warm pool plus hedging; the pool alone 1,107 and hedging alone was the part that cut p90 from 2,045 to 1,240),
BM25 1,089 to 1,010, hybrid 1,845 to 1,584 before the hybrid change and 1,684 to 1,177 after it.

Where the time goes in a cold ANN query (one traced query, fresh process, warm pool): manifest 60 to 100 ms, then WAL probes
and the directory 120 to 170 ms, then the probe wave (about 250 requests: 150 code ranges, 90 cluster blobs, 9 docs
footers; 20 MB) 400 ms, then the rerank wave (80 to 190 requests, 1 to 3 MB) 200 to 280 ms. Without the warm pool the probe
wave took 795 ms.

### Selective filters: what did not work

Selective is one wave of 1,900 requests (4,000 matching rows, each a separate 2 KB vector). Fetching only the allowed rows of
the nearest clusters, to cut requests, loses recall faster than it saves time, because clusters hold about 300 rows and
the winners are spread across many of them: covering 50 percent of the allowed rows (1,025 requests) gave recall@10 0.990
and 25 percent (663 requests) gave 0.964, against 1.000 for all of them (turbopuffer 0.998 here). Latency at 50 percent was
1,754 ms against 2,147 ms (n=12). Not adopted: the recall bar is 0.993. Widening merge gaps does not help (same byte cost
as for ANN). A larger warm pool does: 1,024 connections took p50 from 2,677 (128) to 2,404 (512) to 2,010 ms (1,024) in one block,
and 1,973 to 1,429 ms in the final block.

### Recall and parity at 1M (200 queries per type, against `blowfish-oracle-wiki1m`)

| | Overlap with turbopuffer | Recall@10 blowfish | Recall@10 turbopuffer |
| --- | ---: | ---: | ---: |
| ANN | 0.935 | 0.952 | 0.938 |
| BM25 | 0.990 | | |
| Hybrid | 0.971 | | |
| Selective | 0.998 | 1.000 | 0.998 |
| Broad | 0.963 | 0.974 | 0.977 |

Identical to iteration 8: nothing in this round changes what is read or ranked.

### Verdict against the goal

Met at 1M in this session: BM25 p50 869 ms and p90 1,594 ms, hybrid p50 1,188 ms and p90 1,752 ms (p90 just over 1,700).
Close: ANN p50 1,278 ms (goal 1,224) and p90 1,593 ms. Not met: selective (p50 1,973 ms, 1,429 ms with a 1,024-connection
pool). The request count per wave did not fall (ANN 339 requests): the floor measurements say bytes cost more than requests
past a gap of about 100 KB, so the way to fewer requests is a layout in which the cluster blob and the codes of a probe
are one range, and nearby clusters are stored near each other, which needs a reload. That is the first item for the next round.
Warm latency and QPS were not re-measured this round.

## Iteration 10 (10M docs): pipelined WAL ingest, and what 10M costs

Goal unchanged: cold p50 at most about 1,224 ms or p90 at most about 1,700 ms (turbopuffer's published 10M cold numbers are
p50 874 ms and p90 1,214 ms), recall at least turbopuffer's measured 10M recall. This round loaded the full 10M real passages
(same dataset, 200 held-out queries, truth `bench/scale/data/oracle-10m.npz`) after wiping `scale/`.

### What changed

1. **WAL pipeline** (`bf-wal` `Wal::stage` and `Wal::commit`, `commit_worker` in `bf-engine`, `BF_WAL_PIPELINE`, default 8).
   The coordinator measured the old ingest at 14 MB/s of upload with the server at 0.6 cores: each group's payload was a single PUT,
   and the next group could not start until the previous slot committed. Now up to 8 groups upload their payloads at once, each as its
   own object under a unique key, and the small slot records commit strictly in order. A writer is acknowledged only after its own
   slot commits. Writes are evaluated against the groups still in flight (`snapshot::for_writes` takes an overlay of their IDs and
   schema), so conditions, `$ref_new`, patches, and deletes see them. A failed slot fails every later in-flight group: after a lost
   race the writers are queued again after a resync, after a storage error they get the error, and the abandoned payloads are
   deleted (orphans from a crash are reclaimed by the existing `prune_payloads`). Deletes, copies, and filter writes drain the
   pipeline first. Tests (`crates/bf-engine/tests/wal_pipeline.rs`): ordering with reversed upload completion, conditional writes
   across in-flight groups, a failed slot, two racing writers, and orphan reclaim.
2. **Adaptive connection warmer** (`bf-storage`): the warm pool follows 1.25 times the largest wave of parallel reads seen in the last
   10 minutes (starts at 256, cap 1,024, floor 16 after an idle window, `BF_WARM_CONNECTIONS` fixes it, `BF_WARM_FLOOR`).
3. **ANN directory hint**: each publication also writes the directory to a fixed key, and a cold open reads it in parallel with
   the manifest (verified against the manifest's directory key). A unit test shows one wave fewer. At 10M it did not show in the
   A/B (waves 4.6 against 4.1 is within the spread), because the wave count there is dominated by the probe and rerank waves.

### Ingest

`scale/inner/` (200K docs, same loader, 16 writes in flight, same R2): old binary 175 s (1,152 docs/s, server 0.64 cores, peak RSS
2.5 GB); pipelined binary 98 s (2,040 docs/s over the run, which includes the startup and drain, server 0.75 cores, 3.1 GB). A
500K run on the new binary held about 1,860 docs/s from 100K to 470K rows while another job was compiling on the same VM.
**The target of 3,000 docs/s was not met, and the next limiter is the VM-to-R2 upload path**: a plain parallel PUT test (12 MB
objects) reaches 4.8 MB/s with one stream and tops out at 13.9 MB/s aggregate with 4, 8, or 16 streams. Each document uploads about
3 KB of WAL plus 5.6 KB of live segments (more with rewrites), so the ceiling is about 1.7K docs/s. The commit worker is about
20 percent busy (prepare 0.2 s, slot commit 0.27 s, replay 0.04 s per group of two requests).

Full 10M load (batch 2,000, 16 in flight, loader in a 6 GiB scope, server in a 16 GiB scope): rows 0 to 1M at 1,710 docs/s, 1 to 2M at
1,380, 2 to 5M at 1,250, 5 to 7M at 1,160; 7.83M rows at 6,446 s (1,215 docs/s over that stretch) with 1,717 429s. Then the
**server's memory became the limiter**: RSS reached the 14 GB scope mark and stalled (a throttled process at `MemoryHigh` makes
no progress), and finishing took six server restarts and about 18,000 seconds of pauses, so the whole-run wall time of 24,780 s
(403 docs/s) says nothing about throughput. Peak RSS 16.1 GB. Final: 10,000,000 documents in 70 segments (the last 2M rows
were loaded with `BF_MAX_SEGMENT_ROWS=100000` to avoid the large merges below), 3,039 429s, 5,635 live bytes per document
(ANN 2.2 KB, documents 2.5 KB, text 0.9 KB); the bucket held 69.5 GB before garbage collection (WAL 6.7 GB not yet reclaimed).

### Memory grows with the namespace (top bottleneck)

Server RSS at 0.5M, 1M, 1.5M, 2.5M, 3.5M, 5M, 7M rows: 4.0, 5.9, 7.0, 7.9, 9.4, 8.8, 11.2 GB, a sawtooth with a floor rising about 1.3 GB per
million rows, and spikes to 13 to 15 GB when a compaction reads its source segments (every compaction of two to four 100K-row
segments went to 13 to 15 GB). A restarted server at 9.6M rows reached 15 GB within a minute of its first compaction. At the
7.09M mark `smaps` showed 11.4 GB anonymous: 4.7 GB in the main heap and 6.4 GB in other anonymous mappings (thread arenas and large
allocations), 20 MB file-backed. Known contributors: the 1 GiB block cache (full), the decoded-metadata cache (budget 4.2 GiB,
charged at resident size), the unindexed tail held as documents (base64 f32 vectors, about 8 KB per row, up to the 2 GB
backpressure limit), and compaction and flush working sets. There is no heap profiler on this VM, so the split is inferred from
budgets and mapping sizes, not measured: the next iteration should run a heap profile (dhat or jemalloc) at 2M rows first.

### Cold queries at 10M, same-session A/B (fresh process and empty disk cache per query, alternating arms)

Base is this round's starting binary, new has the three changes. Both read the same 10M namespace (70 segments).

| Query (n) | Base p50 / p90 / p99 ms | New p50 / p90 / p99 ms | Requests | MB | Waves (base, new) |
| --- | --- | --- | ---: | ---: | --- |
| ANN (12) | 2,947 / 4,043 / 4,404 | 2,999 / 4,379 / 4,612 | 1,115 | 68.9 | 4.1, 4.6 |
| BM25 (8) | 4,991 / 7,607 / 8,424 | 4,384 / 5,816 / 6,448 | 1,196 | 61.5 | 3.8, 3.4 |
| Hybrid (8) | 3,877 / 4,799 / 5,604 | 4,158 / 4,769 / 5,647 | 2,241 | 119.9 | 2.4, 2.9 |
| Selective (8) | 7,285 / 8,532 / 10,658 | 6,564 / 8,707 / 10,725 | 2,603 | 268.0 | 4.0, 4.2 |

There is no measurable difference between the arms: the changes in this round do not touch the cold read path at 10M. What grew
from 1M (iteration 9: ANN 339 requests and 22 MB, BM25 310 and 9.2 MB, hybrid 635 and 29.8 MB, selective 1,904 and 41.9 MB) is
the number of probed clusters and the number of segments: ANN 3.3 times the requests and 3.1 times the bytes, BM25 3.9 and 6.7
times, hybrid 3.5 and 4.0 times, and selective 1.4 times the requests but 6.4 times the bytes (the filtered path now scans codes
rather than fetching exact rows, because 42K rows match, which is over the exact-fetch limit). Cold latency is about 2.3 to 4.5
times the 1M figures and 2.4 to 7.5 times the goal.

### Hot (warm) latency, recall, and parity at 10M (`oracle_compare.py`, 200 queries per type, one request at a time, after one warm-up pass)

| | ANN | BM25 | Hybrid | Selective |
| --- | ---: | ---: | ---: | ---: |
| blowfish hot client p50 / p90 / p99 ms | 104 / 147 / 204 | 128 / 266 / 483 | 153 / 265 / 377 | 201 / 232 / 316 |
| turbopuffer client p50 / p90 / p99 ms (measured, this session) | 123 / 279 / 442 | 70 / 192 / 389 | 67 / 74 / 121 | 117 / 134 / 188 |
| turbopuffer `server_total_ms` p50 / p90 | 43 / 223 | 15 / 137 | 13 / 18 | 24 / 27 |
| turbopuffer published cold p50 / p90 ms | 874 / 1,214 | | | |
| blowfish cold p50 / p90 ms (A/B above) | 2,999 / 4,379 | 4,384 / 5,816 | 4,158 / 4,769 | 6,564 / 8,707 |
| Overlap@10 with turbopuffer | 0.887 | 0.985 | 0.935 | 0.962 |
| Recall@10 blowfish (default nprobe 64) | 0.886 | | | 0.968 |
| Recall@10 turbopuffer | 0.943 | | | 0.993 |
| Recall@10 blowfish with `BF_ANN_NPROBE=160` (turbopuffer 0.943 and 0.993) | 0.947 | | | 0.992 |
| Hot client p50 / p90 ms with nprobe 160 | 164 / 197 | | | 446 / 518 |

Recall at 10M is below turbopuffer's with the default 64 probes (ANN 0.886 against 0.943, selective 0.968 against 0.993), which
the earlier sizes did not show (0.952 at 1M). 160 probes restores it (0.947 and 0.992) at a hot-latency cost; its cold cost was not
measured and will be larger than the table above. turbopuffer's published numbers are cold p50 874 ms and p90 1,214 ms; it cannot
be forced cold, so its measured column is hot or warm (ANN 98 hot and 102 warm of 200).

### Verdict

Not met: no query type is near the goal at 10M (cold p50 3.0 to 6.6 s against 1.22 s). Ingest improved 1.7 to 1.8 times but is now
bound by the upload path and, past about 5M rows, by server memory.

### Ranked bottlenecks for the next iteration

1. **Memory that grows with rows and compaction working sets** (above): blocks the load at about 7M rows on a 14 GB scope. Profile
   the heap, stream compaction (read, merge, and build per cluster without holding every source row), and cap the tail's in-memory
   form.
2. **Cold requests and bytes scale with the data**: ANN 1,115 requests and 69 MB, BM25 1,196 and 61 MB, selective 2,603 and 268 MB.
   Needs the layout change from iteration 9's report (vectors next to codes, clusters ordered by proximity, so a probe set is a few
   ranges), a smaller probe set that keeps recall (the 160-probe recall fix makes it worse), and fewer, larger segments
   (70 segments against about 9 at 1M).
3. **Selective filters at 10M** scan codes over about 3 times the probed clusters (268 MB per query): a per-cluster allowed-row index
   would cut that.
4. **Upload bandwidth**: 14 MB/s aggregate to R2 from this VM caps ingest at about 1.7K docs/s at 8.6 KB uploaded per document;
   fewer bytes per document (vectors are stored twice per segment) is the only lever.
5. Not done this round: the selective-filter structure, the cold layout change, the manifest and directory overlap beyond the hint,
   and the 160-probe cold cost.

## Against turbopuffer's published numbers

Measured 2026-11-17 on the benchmark VM (8 vCPU, 32 GB, in Los Angeles; nothing else heavy running), against a
Cloudflare R2 bucket whose location hint is WNAM. The binary is `main` at the end of perf iteration 10. Published numbers are
turbopuffer's, from https://turbopuffer.com/docs/performance, checked 2026-11-16. Ratios are blowfish over turbopuffer. All
latencies are client-side, in ms, with strong consistency in every request.

- **Cold** is a fresh blowfish process with an empty memory and disk cache, one query, repeated 100 times (`bench/scale/ab.py`, in
  its fresh-process mode, with a 1.5 s settle for the connection warmer).
- **Warm** is two unmeasured passes over the query set (one for BM25), then 1,000 queries at concurrency 1 on a keep-alive
  connection (`bench/scale/warm.py`).
- The **network-adjusted estimate** is an estimate, shown next to the measurement and never instead of it. It is the measured
  number minus waves times (our request RTT minus 15 ms), where 15 ms is an assumed in-region RTT, our request RTT is the 4 KB
  GET p50 below, and waves is the mean number of sequential rounds of parallel reads in a cold query (from `BF_TRACE_READS`).
  A warm strong query is one wave: a check of the WAL tail. It ignores bandwidth, so it understates the cold ANN rows.

### The network, and where this VM is

| Measurement (this VM to the R2 S3 endpoint, 200 samples) | p50 | p90 |
| --- | ---: | ---: |
| TCP connect | 6.2 ms | 8.5 ms |
| TLS handshake | 26.4 ms | 35.0 ms |
| GET of 4 KB (warm connection) | 83.5 ms | 129.8 ms |
| PUT of 200 B to 500 KB, one object (30 samples per size) | 260 to 304 ms | 346 to 499 ms |

The VM is in Los Angeles (IP geolocation and the Cloudflare edge it reaches is LAX); the bucket reports `WNAM`. Connect time is
small, so the edge is near. The 83 ms is R2's own time to first byte for a small object, which is far above an in-region object
store, and a PUT costs about three times a GET. This is the difference we cannot remove from this VM. A same-region test needs a
VM in the bucket's region (or an object store there that we do not run on R2), and would need a new bucket and VM, so it was not
done. turbopuffer measures in-region next to its storage.

### Vector query: 10M docs, strong consistency

blowfish is the existing 10M, 1024-dimension Wikipedia namespace (70 segments, f16 vectors, about 5.6 KB per document). turbopuffer's
published run is 768 dimensions, so our vectors are 33 percent larger: this is a conservative upper bound. Recall@10 is against exact
cosine truth for 200 held-out queries (`bench/scale/data/oracle-10m.npz`). turbopuffer measured 0.943 on the same queries.
The 160-probe row is the apples-to-apples one, because it matches turbopuffer's accuracy.

| Cold (n=100) | p50 | p90 | p99 | Recall@10 | Requests, MB | Waves |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| turbopuffer published | 874 | 1,214 | 1,686 | | | |
| blowfish, 160 probes | 5,175 | 6,528 | 8,456 | 0.952 | 2,093, 137 | 4.5 |
| ratio, 160 probes | 5.9x | 5.4x | 5.0x | | | |
| blowfish, 64 probes (default) | 2,790 | 3,862 | 5,353 | 0.886 | 1,103, 67 | 4.4 |
| ratio, 64 probes | 3.2x | 3.2x | 3.2x | | | |
| network-adjusted estimate, 160 probes | 4,870 | 6,222 | | | | |
| network-adjusted estimate, 64 probes | 2,491 | 3,563 | | | | |

| Warm (n=1,000, c=1) | p50 | p90 | p99 | Recall@10 |
| --- | ---: | ---: | ---: | ---: |
| turbopuffer published | 14 | 17 | 27 | |
| blowfish, 160 probes | 136 | 168 | 280 | 0.9465 |
| ratio, 160 probes | 9.7x | 9.9x | 10.4x | |
| blowfish, 64 probes (default) | 88 | 128 | 199 | 0.8855 |
| ratio, 64 probes | 6.3x | 7.5x | 7.4x | |
| network-adjusted estimate, 160 probes (one wave) | 68 | 100 | | |
| network-adjusted estimate, 64 probes (one wave) | 20 | 60 | | |

Neither cold row is within 10 percent of the perf goal (cold p50 1,224 ms or p90 1,700 ms). The adjustment barely moves the cold
rows: 4 to 5 waves of 68 ms is only 300 ms, and the cold query moves 67 to 137 MB and 1,100 to 2,100 requests over 70 segments, so
it is bound by bytes, request count, and compaction, not RTT. The 160-probe cold cost is about 1.9 times the 64-probe cost,
in line with its 2x bytes and requests.

### BM25 full-text: 1M docs, strong consistency

MS MARCO passages (the first 1,000,000 of `Tevatron/msmarco-passage-corpus`, text only, loaded with `bench/scale/msmarco_load.py`),
one `text` attribute with full-text search and default tokenizer settings, no vectors. The corpus is 335 MB of text (335 bytes
per passage on average; turbopuffer's is about 300 MB), and `approx_logical_bytes` reports 358 MB. Queries are 1,000 MS MARCO dev
queries (seeded sample; many of their relevant passages lie outside the first 1M, which does not change the latency work). Top 10.

| BM25 | p50 | p90 | p99 | Requests, MB | Waves |
| --- | ---: | ---: | ---: | ---: | ---: |
| turbopuffer published, cold | 316 | 381 | 559 | | |
| blowfish, cold (n=100) | 1,570 | 1,885 | 2,268 | 198, 17.6 | 3.3 |
| ratio, cold | 5.0x | 4.9x | 4.1x | | |
| network-adjusted estimate, cold | 1,346 | 1,661 | | | |
| turbopuffer published, warm | 13 | 18 | 29 | | |
| blowfish, warm (n=1,000, c=1) | 77 | 108 | 178 | | |
| ratio, warm | 5.9x | 6.0x | 6.1x | | |
| network-adjusted estimate, warm (one wave) | 9 | 40 | | | |

### Upsert: about 512 KB batches

Each batch is 68 rows (about 506 KB of JSON): 30 to 70 pseudo-random words of text, a category, and a 768-dimension f32 vector as a JSON
array. 300 sequential writes (`bench/scale/upsert_bench.py`) into a fresh namespace, with the indexer running normally, each
acknowledged once its WAL slot is committed in R2. No 429s. The namespace and its objects were removed afterward.

| Upsert (client latency until acknowledged) | p50 | p90 | p99 |
| --- | ---: | ---: | ---: |
| turbopuffer published | 165 | 248 | 850 |
| blowfish, run 2 (300 writes) | 792 | 1,531 | 2,713 |
| ratio | 4.8x | 6.2x | 3.2x |
| blowfish, run 1 (300 writes, a slower R2 hour) | 2,114 | 2,563 | 3,250 |
| network-adjusted estimate, run 2, GET RTT (2 PUT waves) | 656 | | |
| network-adjusted estimate, run 2, PUT RTT of 270 ms | 282 | | |

A write is two sequential PUTs: the payload, then the conditional slot record. The brief's formula, with the 83 ms GET RTT, leaves
656 ms. The PUT RTT is the better per-request figure for this benchmark, because the PUT p50 here is 260 to 304 ms whatever the
size, and the estimate then leaves 282 ms. Run 1 and run 2 differ by 2.7x on the same code: R2 write latency moves a lot
between sessions, so treat the upsert numbers as a range (792 to 2,114 ms p50), not a point. A 60-write run on the same binary had p50 810.

### Differences that remain

1. **Network:** we cross the Internet to R2 (83 ms for a 4 KB GET, 270 ms for a PUT), while turbopuffer measures in-region. Not removable
   from this VM. The adjusted estimates above assume 15 ms in-region.
2. **Vector dimension:** 1024 against 768 (33 percent more bytes). The 768-dimension reload was skipped, on instruction.
3. **Layout:** the 10M namespace has 70 segments, because its last 2M rows were loaded with `BF_MAX_SEGMENT_ROWS=100000` and never
   compacted. A compacted namespace would read fewer, larger objects.
4. **Warm consistency check:** every strong query pays one R2 round trip to check the WAL tail, which dominates our warm numbers
   (77 to 136 ms against about 14 ms). turbopuffer does not have to cross a network for it.
5. **Corpus:** MS MARCO at 335 bytes per passage, not the 300 MB turbopuffer used; the queries are MS MARCO dev queries. The upsert rows
   are synthetic.
6. **Recall:** at the 64-probe default we are below turbopuffer (0.886 against 0.943). The apples-to-apples row is 160 probes (0.947 warm,
   0.952 cold; the two differ only in which queries were drawn and the cold run's 100 against 1,000).
7. **Sampling:** 100 cold queries each (the cold ANN arms alternated in one session, which is the only way to compare R2 at different times),
   1,000 warm queries (the 200-query ANN set repeats five times). R2 latency varies by 2x or more between sessions, as the two upsert runs show.
8. **Cache and process:** cold means an empty 8 GiB memory cache and an empty disk cache, but a settled warm connection pool; the server
   is one process on the VM, and no other heavy job ran during any measurement.

## Iteration 11 (10M docs): block fragments, fewer requests per cold query

Goal unchanged: cold vector search p50 at most about 1,224 ms or p90 at most about 1,700 ms, with recall at least turbopuffer's
(ANN 0.943, selective 0.993). Same 10M, 1,024-dimension namespace (70 segments, f16 vectors), same 200 held-out queries and truth
(`bench/scale/data/oracle-10m.npz`); nothing was reloaded. Cold means a fresh process and an empty disk and memory cache per query,
arms alternating query by query (`bench/scale/ab.py`).

### Diagnosis: where one cold ANN query goes (query 3, before this round, `BF_TRACE_READS`)

| Stage (wave) | 64 probes: requests, MB | 160 probes: requests, MB |
| --- | ---: | ---: |
| Directory hint (5.35 MB) and manifest | 2, 5.4 | 2, 5.4 |
| WAL tail | 2, 0 | 2, 0 |
| Scan wave, codes from the segments' index objects (`.bfv`) | 502, 48.5 | 1,178, 107.9 |
| Scan wave, cluster blobs from the fragments (`.bff`: exact center, row set, row checksums) | 424, 8.7 | 768, 27.3 |
| Scan wave, documents' footers of every probed segment (`.bfc`, 70 segments) | 70, 10.5 | 70, 10.5 |
| Rerank vectors (about 105 candidates after the error bound; 2 KB each) | 48, 0.1 | 35, 0.1 |
| Documents of the top candidates (speculative, then the winners) | 21, 0.7 | 21, 0.4 |
| Total | 1,069, 75.1 | 2,076, 153.1 |

Why the count scales with probes times segments: the probe count is `nprobe` times `clusters / sqrt(rows)` (8.4 here, from 26.5K
leaf clusters in 70 segments), so 64 probes mean about 540 probed clusters (245K rows, 2.45 percent) and 160 mean about 1,350
(583K rows, 5.8 percent). Each probed cluster cost two reads, a blob from the fragment and its codes from the index object, and
clusters sit in arbitrary order inside a segment, so only about 1 in 20 of them merged with a neighbor. The rerank is not the
problem (about 105 candidates survive the error bound at both probe counts, so it stays about 50 small reads). The wave of
requests is. Its time is set by two things: the number of connections it needs beyond the 256 the warmer opens (each extra one
pays a TLS handshake, and a connection that has only carried HEADs starts TCP slow start on its first large range), and the
slowest request. `BF_WARM_CONNECTIONS=1024` alone took the old binary from 2,329 to 1,746 ms p50 at 64 probes (10 queries), which
is how much of the old wave was connection setup.

Things measured and not adopted (warm server, 200 queries unless noted):

- Widening the error bound (`BF_ANN_BOUND`, a temporary knob) from 1 to 2 and 3 times grew the rerank pool from 108 to 273 and 308
  and left recall at 0.8855 in all three. The error-bounded pruning is lossless, and the recall loss at 64 probes is routing (clusters
  not probed), not quantization.
- Exact (f32) cluster centers instead of the directory's one-bit centers (`BF_ANN_DIRECTORY=0`) give 0.897 against 0.8855 at 64
  probes: routing noise is worth about 1 point.
- Rows scanned per query: 129K at 32 probes (recall 0.8155), 245K at 64 (0.8855), 583K at 160 (0.9465).
- A cluster-distance ratio cutoff instead of a fixed probe count (clamped to a third to three times the count): recall fell to 0.77
  to 0.78. In 1,024 dimensions the distances to the nearest clusters are too close together to tell them apart by ratio.
- Shorter codes (the first 128, 256, or 512 of the 1,024 sign bits, on a real 200K-row segment, 60 queries): the relative error of
  the estimate grows about 3 times at 256 bits (standard deviation 0.143 of `|o| |q|` against 0.047 at 1,024), and keeping 9.9 of the
  true top 10 needs a rerank pool of about 1,300 candidates per 66K rows (against about 46 at 1,024 bits). The extra rerank reads
  cost more than the 4 times smaller scan saves.
- Coarser clusters (fewer, bigger blocks per request), top-10 coverage at 2.5 percent of rows scanned, one 200K-row segment: 0.86 at
  448 clusters, 0.83 at 224, 0.77 at 133. Recall falls faster than the request count.
- Hedge budget 0.1 against 0.3, merge gap 0, 16, and 64 KB: no difference outside the noise (12 queries each).

### What changed

1. **Block fragments** (`annfrag2:<attribute>`, directory `ann2:<attribute>`; format in `docs/design/ann-directory.md`). A cluster's
   block is its blob followed by its codes, so a probe is one ranged read, and blocks are ordered along a nearest-neighbor tour
   of the segment's cluster centers so probed neighbors merge: on a real segment, 8 probed clusters make 4.4 ranges against 7.8
   in cluster-ID order, with no gap bytes. The plain fragments and directory stay, so older binaries and filtered queries are
   unchanged. New segments get both fragments; older ones get the block fragment from the next indexing pass.
2. **Scan prefetch.** A lone unfiltered ANN query starts the scan's reads (and the flat segments' exact reads) in the snapshot wave,
   as soon as the directory hint is loaded and in parallel with the WAL tail read, instead of after it. Skipped when a resident
   checkpoint lets the query search speculatively.
3. **No documents' footers in the scan wave** (70 requests, 10.5 MB at 70 segments), except when 8 or fewer segments are probed. With
   dozens of segments, warming the footers of the nearest 32 clusters' segments was slower in an A/B (1,386 against 1,294 ms p50
   at 64 probes, 14 queries).
4. **Reranks off the CPU workers.** They are a few small reads, and holding one of the 8 worker permits through each read's latency
   queued the segments (the rerank wave fell from about 600 to about 450 ms).
5. **The scan wave starts every segment at once** (it used to run 48 at a time, a second batch of segments at 70).

The 10M namespace was upgraded in place, once, by a writable server with compaction disabled (`BF_MAX_SEGMENT_ROWS=1`): 70 block
fragments (1.53 GB, about 22 MB per segment) and the `ann2` directory, in 540 s, peak resident 7.5 GB, with the plain objects
untouched. The old binary was measured against the upgraded manifest in every A/B below and answers as before.

### Cold queries at 10M, same-session A/B (30 queries per arm, 200-query truth, `top_k` 10)

| Arm | p50 ms | p90 | p99 | mean | Requests | MB | Waves | Recall@10 (these 30) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 probes, before | 2,421 | 3,291 | 3,507 | 2,561 | 1,112 | 67.6 | 4.6 | 0.870 |
| 64 probes, after | 1,502 | 1,834 | 2,947 | 1,559 | 462 | 52.9 | 3.5 | 0.870 |
| 160 probes, before | 5,422 | 6,318 | 10,909 | 5,182 | 2,104 | 137.9 | 4.5 | 0.950 |
| 160 probes, after | 3,280 | 8,293 | 20,352 | 4,907 | 879 | 106.5 | 3.9 | 0.950 |
| 160 probes, after, `BF_WARM_CONNECTIONS=1024` | 3,718 | 5,887 | 7,498 | 4,003 | 879 | 106.5 | 4.5 | 0.950 |
| 160 probes, after, repeat in a second session | 3,256 | 5,077 | 6,576 | 3,467 | 879 | 106.5 | 3.7 | 0.950 |

Recall on the full 200 queries (warm server, which is the same code path): 0.8855 at 64 probes and 0.9465 at 160, unchanged (the
scan reads the same clusters). The selective (filtered) path is untouched: it still uses the plain directory, and its recall was not
re-measured. The 160-probe tail is R2's: 4 of the 60 "after" queries took 7.8 to 22.8 seconds with the same request counts (the
"before" arm had 2 of 30 above 9.7 seconds), and a smaller 1 MiB cap on merged reads (hedgeable) did not change it. The
first-session 160-probe medians (3,280 against 5,422 ms) and the 64-probe medians (1,502 against 2,421 ms) are the comparison;
the 160-probe p90 and p99 move with R2's stalls from session to session.

One cold query (query 3) after the change, from the trace: 64 probes make 461 requests and 60.2 MB (366 block reads, 47 MB; 52
vector reads; 39 documents' footers and rows), 4 waves, 1,693 ms; 160 probes make 780 requests and 122.3 MB (698 block reads, 109
MB), 4 waves.

### Verdict

Neither stop rule is met. At 64 probes the cold p50 is 1,502 ms (goal 1,224), but recall is 0.8855 (turbopuffer 0.943). At 160 probes
recall is 0.9465 and the cold p50 is about 3.3 s. Against the iteration 10 numbers, p50 fell 38 percent at 64 probes (2,421 to
1,502 ms) and 40 percent at 160 (5,422 to 3,280 ms), requests fell 58 and 58 percent, and bytes fell 22 and 23 percent.

### What is left

- **Bytes.** 160 probes scan 583K rows at 136 bytes each (codes: 128 bytes of sign bits and 8 of corrections), about 80 MB of the
  106 MB, and recall is routing-limited, so shorter codes (above) and a wider error bound do not help. What would: better routing
  (a probe plan that spends fewer rows for the same recall, such as spilling each row into two clusters, or a second ranking from the
  exact centers that the block reads already carry), or a cross-segment layout that merges clusters of different segments that
  are near each other, so a probe is one read whatever the segment count.
- **The final wave** (vector reads, then documents' footer and rows): about 340 to 450 ms, two dependent hops on the documents. An
  `id`-only query could read the id column's footer range instead of the 66 to 80 KB footer.
- **The pre-scan wave**: the directory hint (5.35 MB, about 150 to 290 ms) and the manifest LIST and GET (about 190 ms) run side by
  side, and the scan cannot start before the manifest names the segments. A smaller directory (the entry table is 1.8 MB of ranges and
  checksums that could be derived) would shave tens of milliseconds.
- **Connections and stalls.** A wave of 800 requests opens about 550 connections beyond the warmed 256, and R2 stalls a few of them for
  seconds. Opening the pool at 1,024 (the cap it already grows to) fixed some of the tail in one session and none in another.
- 1M and 100K were not loaded in R2 for this round. The cold-read unit tests (`crates/bf-engine/tests/read_cache.rs`: waves and
  requests for 20 and 60 segments) pass with unchanged limits.

## Iteration 12 (10M docs): a namespace-wide recluster, scanning a fifth of the rows

Same 10M, 1,024-dimension namespace (70 segments), same 200 queries and truth. Goal unchanged: cold p50 at most 1,224 ms or p90 at
most 1,700 ms, with ANN recall@10 at least 0.943. **Met on both latency rules at 0.945 recall** (full 200 queries, warm).

### Offline: recall against rows scanned (1M, exact truth, exact centers, 200 queries)

The first 1M passages, truth recomputed by brute force. "Rows to 0.943" is the mean rows scanned when the nearest clusters are probed
until recall@10 first reaches 0.943.

| Layout | Rows to 0.943 | Notes |
| --- | ---: | --- |
| (a) per-segment clusters (7 segments, sqrt(n) each), global ranking of all centers | 56K | the current layout. (b), a global ranking with no per-segment scaling, is the same curve: the planner already ranks all centers together, and the scaling only sets the probe count |
| (c) one global k-means, about 1,000 rows per cluster | 55K | |
| (c) global, about 300 rows | 39K | |
| (c) global, about 100 rows | 16K | |
| (d) global 100 rows, each row in its top 2 clusters | 17K | counting duplicated rows. Gain 1.0 to 1.13 times (300 rows: 35K): killed, under 1.3 times |
| (e) 20 spatial slabs, about 100 rows per cluster | 23K | slabs are a hard coarse partition |
| (e) 20 slabs, about 200 rows | 29K | the shipped layout |
| (e) 8 slabs, about 100 rows | 23K | |

Merged byte runs per query at 0.943 (blocks in tree order, a 136-byte code per row, gap 0, 256 KB, 1 MB; at 1M rows, so the gaps cover
more of the layout than at 10M): 20 slabs of 100 rows need 194 probes (123, 30, and 15 runs); 200 rows, 131 probes (80, 25, and 13
runs); 374 rows, 119 probes (67, 28, and 15 runs; 48K rows); the 7 per-segment layout needs 131 probes (98, 52, and 28 runs).
The 300K-row kill rule for 100-row leaves is not close (16K to 23K at 1M).

Why a global layout wins more at 10M than at 1M: the per-segment layout needs clusters from every segment that holds a neighbor, so
its probes grow with the segment count (1,500 clusters, 583K rows at 160 probes). A slab layout's probes grow with the neighborhood.

### What was built

- `Engine::recluster` (`recluster.rs`, `examples/recluster.rs`): copies every live row's vector once into a local spill file indexed by
  global row number (segment base plus segment-local row), trains 32 coarse cells on a 100K sample (8 Lloyd iterations), assigns every
  row, then builds each cell as a slab with the ordinary index builder (about 200 rows per leaf), its block fragment, and a block
  directory over the slabs. A slab's row IDs are global row numbers, so a winner maps back to its segment and row by a 70-entry base
  table, with no lookup object. Documents, attributes, and the per-segment indexes and directories stay where they are.
- `Manifest::ann_slabs` (`SlabSet`): slabs, covered segments (with bases and live generations), and the directory. Older binaries ignore
  the field and answer from the segments as before. GC protects the slab files and retires them when a covered segment is gone.
- Queries: an unfiltered ANN query answers from the slabs while they cover exactly the live segments (same planner, scan, bound-pruned
  rerank, and hydrate code, with winners mapped back). A delete after the build is a mask over global rows. A tail upsert or delete
  works as before (shadowed IDs). The snapshot wave fetches the slab directory's hint beside the manifest and prefetches the scan.
  Filtered queries, and any namespace that gained a segment since the build, use the per-segment path (`BF_ANN_SLABS=0` forces it).
- The 10M namespace was reclustered additively: 32 slabs, 49,986 leaves, a 9.9 MB directory, 2,004 s (copy 273 s, partition 37 s,
  slab builds the rest), peak resident 16.9 GB (above the 12 GB target: the largest slab, 465K rows, builds in f32 with a second copy
  for training; a lower `max_slab_rows` cuts it). The temporary spill file is 20 GB and is deleted at the end.
- A bench-only `POST /_bench/drop_caches` in the scale server (loopback, no auth, not in the public router) empties the block, disk,
  and decoded caches and the resident namespace state: the cache-cold, process-warm measurement.

### Recall, warm, full 200 queries

| `BF_ANN_NPROBE` (slab leaves probed, x15.8) | 8 (126) | 16 (253) | 24 (380) | 32 (506) | 40 (632) | 48 (759) | 64 (1,011) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| recall@10 | 0.8365 | 0.882 | 0.9135 | 0.930 | 0.9415 | 0.953 | 0.9615 |

Rows scanned at 0.943 (about 42 probes, 660 leaves) are about 130K, against 583K for the per-segment layout at 160 probes.

### Cold, same-session A/B, 30 queries per arm (`bench/scale/ab.py`, fresh process, empty caches)

| Arm | p50 ms | p90 | p99 | Requests | MB | Waves | Recall@10 (these 30; full 200) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| per-segment, 160 probes (before) | 2,916 | 4,358 | 23,116 | 880 | 116.5 | 3.5 | 0.950 (0.9465) |
| per-segment, 64 probes | 1,342 | 1,720 | 2,782 | 463 | 62.9 | 3.4 | 0.870 (0.8855) |
| **slabs, 42 probes (after)** | **1,082** | **1,592** | 2,283 | 383 | 47.1 | 3.2 | 0.927 (about 0.945) |

Cache-cold, process-warm (one running server, `drop_caches` before each query; 30 queries; its own session, so only compare within the
row pair): slabs at 42 probes 954 ms p50 and 1,262 ms p90 (440 requests, 47.1 MB); per-segment at 160 probes 4,306 and 7,001 ms
(1,000 requests, 116.5 MB). Both numbers are reported beside each other; the fresh-process number is the conservative one.

Recall on these 30 queries is below the full-set recall for the slab arm (0.927 against about 0.945): 42 probes is the full-set
interpolation between 0.9415 (40) and 0.953 (48), and 30 queries are a noisy sample.

### What is left

- Two-level directory: a cold query reads the 9.9 MB slab directory plus the manifest (about 5 MB of it is the per-segment directory
  hint, still fetched). The slab directory could be a top level in the manifest and per-slab sections.
- The final wave's two dependent document hops (about 340 to 450 ms) are untouched. Putting each leaf's documents' bytes beside its
  block (research hypothesis 2) would make rerank and hydrate one wave.
- New segments flushed after a recluster make the slabs stale (any uncovered live segment): queries fall back to the per-segment path
  until the next recluster. Serving covered slabs plus the new segments in one query, and an indexer-triggered recluster, are not built.
- The builder's peak memory (16.9 GB) and a slab split that ignores locality for cells above `max_slab_rows`.
- Filtered ANN does not use the slabs.

### Iteration 12: publication run

Date 2026-11-17, commit 1901a41 (main), no engine changes. Same 10M, 1,024-dimension namespace (70 segments, 32 slabs from round 12),
read-only, on the 8 vCPU benchmark VM against Cloudflare R2. Queries are the first 50 of the 200 held-out queries in `ann-apples.json`
(exact top-10 truth). Cold arms use `bench/scale/ab.py` in one session: a fresh server process and an empty memory and disk cache for
every query, arms rotated query by query, 1.5 s for the connection warmer. One binary, only `BF_ANN_NPROBE` differs.

Network floor for the session (`r2_rtt.py`, 200 samples, warm connection): 4 KB ranged GET p50 50.0 ms, p90 64.8 ms; TCP connect
p50 6.2 ms; TLS handshake p50 24.5 ms.

#### Cold, fresh process, 50 queries per arm

| `BF_ANN_NPROBE` | p50 ms | p90 ms | p99 ms | Requests | MB | Recall@10 (these 50) | Recall@10 (all 200, warm) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 40 | 1,075 | 1,345 | 3,043 | 357 | 45.6 | 0.934 | 0.9415 |
| 48 | 1,111 | 1,311 | 1,584 | 404 | 50.7 | 0.944 | 0.953 |
| 64 | 1,326 | 1,757 | 2,228 | 495 | 60.7 | 0.950 | 0.9615 |

The all-200 recall comes from a warm server per setting (`warm.py`, one pass over the 200 queries after one warm-up pass); it matches
the round 12 table at all three settings. With n = 50, p99 is the second-slowest query, so it is noisy: the 3,043 ms at 40 probes is
one or two slow object-store responses, not a property of the setting.

#### Cache-cold, process-warm, 48 probes

One running server (the resident namespace state stays only until `POST /_bench/drop_caches`, which empties the block, disk, and
decoded caches and that state before every query), 50 queries, in its own session.

| p50 ms | p90 ms | p99 ms | Recall@10 (these 50) |
| ---: | ---: | ---: | ---: |
| 1,176 | 1,677 | 3,584 | 0.944 |

Here the drop-caches p50 is not lower than the fresh-process p50 at the same setting (1,111 ms): the two runs are different sessions
minutes apart, and the gap is inside session-to-session R2 variation. Treat the two as the same cold cost.

Warm for reference (same servers, 200 queries, concurrency 1): p50 76, 78, and 101 ms at 40, 48, and 64 probes.

## In-region on AWS (v0.1.12)

Setup: AWS `c7gd` instances in `us-west-2` reading a same-region S3 bucket, running `main` at `1901a41` (round 12, which is the
`v0.1.12` code). All rows were measured on `c7gd.2xlarge` (8 vCPU, 16 GB) unless the instance-size table says otherwise. The 10M
English Wikipedia namespace (1,024-dim `f16` vectors, 70 segments, and 32 spatial slabs from `recluster`) was copied from R2 to S3
unchanged; BM25 uses 1M MS MARCO passages. Every query uses strong consistency. Upserts were not measured in-region.

"Fresh" starts a new read-only process with an empty cache for each query (50 queries per arm, rotated in the same session).
"Cache-cold" runs against a warm process with caches dropped before each query (`POST /_bench/drop_caches`). `Recall@10` is
measured over all 200 held-out queries from a warm 1,000-query run.

### Network floor (instance to S3)

| Measurement | p50 | p90 |
| --- | ---: | ---: |
| TCP connect | 1.6 ms | 2.2 ms |
| TLS handshake | 26.9 ms | 27.7 ms |
| 4 KB `GET` | 24.5 ms | 30.5 ms |
| 1 MB `GET` | 30.2 ms | 36.1 ms |

### 10M vector (`c7gd.2xlarge`)

Cold `p50 / p90` in milliseconds (blowfish vectors are 1,024-dim `f16`; turbopuffer's published benchmark uses 768-dim):

| Arm | Fresh (ms) | Gap vs. published | Cache-cold (ms) | Gap vs. published | Recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| turbopuffer published | 874 / 1,214 | | | | 0.943 |
| Slabs, 40 probes | 642 / 806 | -27% / -34% | 558 / 699 | -36% / -42% | 0.9415 |
| Slabs, 48 probes | 662 / 991 | -24% / -18% | 574 / 652 | -34% / -46% | 0.953 |
| Slabs, 64 probes | 773 / 969 | -12% / -20% | 630 / 746 | -28% / -39% | 0.9615 |
| Per-segment, 160 probes (slabs off) | 1,079 / 1,336 | +23% / +10% | 924 / 1,071 | +6% / -12% | 0.9465 |

Tail latency and object-store I/O per cold query:

| Arm | p99 fresh (ms) | p99 cache-cold (ms) | Requests | MB | Waves |
| --- | ---: | ---: | ---: | ---: | ---: |
| Slabs, 40 probes | 1,078 | 1,038 | 357 | 45.6 | 3.4 |
| Slabs, 48 probes | 1,454 | 816 | 404 | 50.7 | 3.5 |
| Slabs, 64 probes | 1,080 | 872 | 495 | 60.7 | 3.6 |
| Per-segment, 160 probes (slabs off) | 1,543 | 2,371 | 844 | 115.2 | 3.2 |

Warm, 1,000 queries at concurrency 1 (`p50 / p90 / p99`, ms; turbopuffer published warm is `14 / 17 ms` `p50 / p90`):

| Arm | Warm p50 / p90 / p99 (ms) |
| --- | ---: |
| Slabs, 40 probes | 67.5 / 80.9 / 96.0 |
| Slabs, 48 probes | 70.7 / 84.1 / 99.0 |
| Slabs, 64 probes | 84.0 / 101.9 / 117.8 |
| Per-segment, 160 probes (slabs off) | 96.9 / 106.2 / 112.3 |

### BM25, 1M MS MARCO passages (`c7gd.2xlarge`)

| Mode | turbopuffer published (p50 / p90) | blowfish in-region (p50 / p90) | Gap vs. published |
| --- | ---: | ---: | ---: |
| Cold, fresh process | 316 / 381 ms | 453 / 569 ms | +43% / +49% |
| Cold, cache-cold (`drop_caches`) | | 420 / 512 ms | +33% / +34% |
| Warm (round 11 code) | 13 / 18 ms | 17.1 / 34.5 ms | |

- Each cold BM25 query makes 156 requests and reads 17.1 MB.
- Against the published cold numbers, no gap remains on vector; on BM25, about 4 sequential waves at 25–30 ms per round trip, plus
  CPU, dominate the remaining gap. Wave count, rather than bytes, sets the floor because only 17.1 MB moves.

### Per-stage breakdown: one cache-cold 10M vector query (slabs, 48 probes)

The query took 477 ms in-process (`308 ms` snapshot total and `169 ms` execution total) and 660 ms of CPU summed across threads:

| Stage | Requests | Bytes | Time |
| --- | ---: | ---: | ---: |
| Wave 0: two ANN directory hints | | 15.3 MB | 100 ms |
| Wave 1: WAL tail | 2 | | 23 ms |
| Wave 2: fragments (scan prefetch) | 361 | 36.8 MB | 181 ms |
| Wave 3: rerank vectors plus two dependent document hops | 67 | | 123 ms |
| Scan CPU | | | 34 ms |
| Rerank and hydrate in-process | | | 162 ms |

About 70% of query wall time is waiting on S3, and the 15.3 MB directory-hint read in wave 0 is the clearest single target. For
comparison, one cache-cold per-segment query at 160 probes took 899 ms wall time and 1,690 ms of CPU; its fragment wave alone moved
113 MB across 705 requests and took 550 ms.

### Instance sizes

`p50 / p90` in milliseconds (except warm `p50` and swap pages out):

| Size | Slab 40 fresh | Slab 48 cache-cold | Slab 64 fresh | BM25 cold fresh | Warm p50 (40) | Swap pages out |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `c7gd.2xlarge` (8 vCPU, 16 GB) | 642 / 806 | 574 / 652 | 773 / 969 | 453 / 569 | 67.5 | 0 |
| `c7gd.xlarge` (4 vCPU, 8 GB) | 566 / 730 | 650 / 845 | 673 / 976 | 501 / 682 | 71.3 | 0 |
| `c7gd.large` (2 vCPU, 4 GB) | 728 / 961 | 661 / 781 | 857 / 1,152 | 509 / 720 | 114 | 8 |

Verdict:
- `c7gd.xlarge` is the smallest size that holds the `c7gd.2xlarge` cold numbers; differences are within session noise.
- `c7gd.large` works (1 GiB block cache, 256 MB decoded cache, and an NVMe swap file that was barely touched) and still beats
  turbopuffer's published cold vector `p50` and `p90` at 40 and 48 probes, though it is 15–20% slower on fresh `p50` and 1.7x
  slower warm because it has 2 vCPUs.

### S3 copy incident

A writable loader server indexes, compacts, and garbage-collects every namespace under its prefix. Loading MS MARCO under the same
prefix forked the S3 copy of the 10M namespace. The namespace was restored from R2 and verified key by key, and
`bench/scale/msmarco_load.py` now warns about this.
## Iteration 13 (BM25, 1M)

Goal: cold BM25 on 1M MS MARCO passages within 25 percent of turbopuffer's published cold numbers (p50 316 ms, p90 381 ms), so
p50 at most 395 ms or p90 at most 476 ms, with the same result quality. Namespace: the first 1,000,000 passages, 9 text segments,
under the R2 prefix `scale/bm25/` (`msmarco_load.py`, loaded by a server that sees only that prefix). Queries: the 1,000 dev
queries of the apples run. BM25 settings are the schema defaults (word_v4, no stemming, no stopword removal, k1 1.2, b 0.75, k3 8).
Truth: `bf-text/examples/exact_bm25.rs` scores every document for every query with the engine's formula (namespace-wide
statistics), and `bench/scale/bm25_overlap.py` compares top 10 (ties at the 10th score count as matches).

### Where the milliseconds went (one fresh-process cold query, before; `bench/scale/cold_trace.py`, `timeline.py`)

Query "is my 401k an ira", 5 segments, a fast-R2 hour (a 4 KB GET is 50 to 60 ms), 123 reads, 14.9 MB, 812 ms in the server:

| Step (sequential hop) | ms |
| --- | ---: |
| LIST of `manifest/` (not in the read trace; it precedes the first read) | about 50 |
| GET manifest | 52 |
| Open each segment's text index (192 KB tail read: lengths and dictionary index) | 80 |
| Dictionary chunks (and the corpus-statistics chunks, WAL tail, live masks, docs footers in the same wave) | 60 to 130 |
| Term records (block directories) | 90 |
| Posting blocks of every term (25 reads, 11.5 MB, so "is", "my", and "an" are read whole) | 185, then 43 of decode and scoring |
| Hydration of the top 10, one segment after another (31 reads in a stagger of about 50 ms per segment) | 130 |

That is seven dependent round trips, and the network, not CPU, is most of it: CPU between waves was 10 to 45 ms in total. The
1,570 ms of the earlier table is the same chain at 150 to 250 ms per hop (R2 tails; the same query measured 2,076 ms right after
an idle hour, 830 ms minutes later). Fresh process and cache-cold with a warm process differ by under 10 percent once the process
has been up a few seconds; the 1.5 s settle for the connection warmer is enough. Compaction to one text segment was tried (cap
1.2M rows gave 4 segments of 700K rows at most) and did not help: p50 700 against 635 ms for 9 segments (n=40), because the
lengths and dictionary reads grow with the segment.

### What changed

1. **Window-pruned posting reads (`bf-text`).** A prefetching search used to read every block of every query term. Now short exact
   terms (at most 4 blocks) are read whole and give exact per-window bounds, long terms give one block bound per window, and blocks
   are fetched in two parallel batches: the 24 highest-bound 128-row windows first, then every window whose bound can still reach
   the heap's k-th score. A window is skipped only when its bound is below that score, so results are unchanged.
   Bytes per cold query fell from 14.7 to 4.3 MB.
2. **Segment format version 4: records next to their postings.** Each term's record (block directory) is stored immediately before
   its blocks, and phrase positions moved to one region after all postings. One read returns a short term's record and postings, so
   the dictionary-to-record-to-postings chain lost a hop for rare terms, and a long term's blocks are contiguous.
3. **Text headers in the manifest.** A version 4 segment's footer and dictionary chunk index ride in its manifest entry
   (`SegmentFile.text_header`), so a cold query reads the dictionary chunks with no open read first; the document lengths load in
   parallel with the dictionary. Version 1 to 3 segments still open and query as before.
4. **Manifest copy at a fixed key.** The indexer writes `ns/<ns>/manifest-latest` (the sequence and the manifest bytes) after each
   publication. A cold reader fetches it while it lists `manifest/`, and uses it only when it is the newest listed generation, so the
   LIST and the GET share one round trip, and a stale or missing copy falls back to the old path.
5. **Segments run concurrently.** `prepare_scalar` handled segments one after another, hydration included (a stagger of about 50 ms per
   segment). Each segment now builds its own scores and documents, and the parts merge.

### Results (R2, fresh process and empty disk cache per query, arms alternating; this VM to R2)

| Cold BM25 at 1M | p50 / p90 / p99 ms | Reads | MB | Waves | overlap@10 |
| --- | --- | ---: | ---: | ---: | ---: |
| turbopuffer published | 316 / 381 / 559 | | | | |
| before (iteration 12 binary), fresh process, n=60 | 839 / 1,080 / 1,425 | 146 | 14.7 | 3.7 | 1.000 |
| after, fresh process, same session, n=60 | 673 / 923 / 1,167 | 186 | 4.3 | 3.1 | 1.000 |
| after, fresh process, a second session, n=40 | 635 / 716 / 1,850 | 188 | 4.3 | 3.3 | 1.000 |
| before, cache-cold with a warm process, n=100 | 825 / 1,204 / 1,331 | | | | 1.000 |
| after, cache-cold with a warm process, same session, n=100 | 634 / 977 / 1,112 | | | | 1.000 |

Overlap@10 against the exhaustive truth over all 1,000 queries (warm server, the new binary): 1.0000, every query exact. The
requests rose (more, smaller reads across 9 segments) and the waves are counted by `waves.py` (a wave starts when no earlier read is
still running; the true dependent chain is longer than that count).

The best single cold query after the change (one traced fresh process, "al baik food houston", 9 segments): manifest copy 0 to 50 ms;
dictionary chunks, lengths, WAL probes, live masks, and statistics chunks 58 to 180 ms; term records with the short postings
149 to 262 ms; the long terms' pruned blocks 266 to 394 ms; hydration of the winners 309 to 474 ms (overlapping, per segment). The
server reported 474 ms.

### Verdict and what is left

Not met from this VM: 635 to 673 ms p50 over the internet against 395 ms, and R2's per-hop tail (50 to 120 ms for the slowest
of about 60 parallel requests) is most of the remaining time. The chain is now manifest copy, dictionary, records plus short
postings, long-term blocks, hydration: five hops, from seven. Projected in-region (about 25 ms per hop, 150 ms of
non-overlapped CPU and transfer): 260 to 330 ms, which would meet the goal; the coordinator's in-region run is the confirmation.
Ranked next steps:

1. Fold the record read into the dictionary read for short terms (inline records in small dictionary chunks, or a hot-term table in the
   manifest-adjacent object): one hop fewer.
2. Store each posting block's document lengths with the block, so no lengths read is needed (192 KB per 200K rows, about 40 percent of
   the bytes in the busiest wave), and run-compress the live-row bitmaps (118 KB each, about 1 MB per query, for a few hundred bytes).
3. One namespace-wide text dictionary, so requests do not scale with segment count (186 reads over 9 segments).
4. Hydrate only the global winners (the per-segment top 10 is hydrated, up to 50 rows for 10 results).

## Iteration 14 (memory): where 10M's memory goes, and the first fixes

Goal: load and recluster 10M documents (1,024-dimension f16) on a c7gd.large (2 vCPU, 4 GB, no swap) with RSS under about 3.5 GB. Before this
round a 10M load reached 13 to 16 GB and `recluster` 16.9 GB. Status: **target not met, not validated at 10M on c7gd.large** (the weekly
quota ran out first). Measured on a c7gd.2xlarge in-region (us-west-2), jemalloc heap profiles (`--features heap-profile`,
`_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19`, dumps every 10 to 20 s, read with `jeprof`) on a 1M-row load and one 1M-row recluster.

### Heap profile (live bytes at the allocation site, at the peak dump)

| Phase | Peak live heap | Top sites |
| --- | ---: | --- |
| Steady ingest (WAL commit into the tail), 100K-row flush cycle | 2.1 to 2.5 GB | `commit_front` / `branch::replay` tail documents 660 to 720 MB (a 5.5 KB base64 vector plus text per row); segment build 400 to 820 MB (`decode_vector` f32 copies 340 MB, `cluster::prepare` 340 MB, `build_text` 180 MB) |
| Segment build, 100K rows | about 1.7 KB x 4 per row | the build holds the input f32 vectors, a second rounded f32 copy (`stored`), a third normalized copy (`training`), the blocks, and then the whole blob again |
| Compaction, two 100K-row segments (200K rows) | 5.7 to 6.3 GB (32 KB per row) | `segment::documents` re-materializes every document 2.1 GB (vectors as base64 strings); `build` 0.8 GB (`decode_vector` 700 MB, `prepare` 684 MB); `build_text` 240 MB; text statistics `hydrate` / `full_stats` 240 MB (grows with the namespace) |
| Idle after the load | 3.2 GB | the in-memory block cache, filled by the compaction reads (`read_batch_with_stats`, 2.7 GB with a 4 GiB budget) |
| Recluster, 1M rows in 3 slabs (largest 527K) | 10.3 GB (8.4 GB without the profiler) | the copy pass fills the block cache 2.1 GB; slab build `vectors` 2.1 GB, `stored` 2.1 GB, `training` 2.1 GB, blocks 1.1 GB, blob 1.1 GB (about 16 KB per slab row) |

RSS split: with jemalloc and 1 s decay, RSS was within 1 to 6 percent of the live heap (resident 2,566 vs allocated 2,413 MB at a spike), so
**fragmentation and retention are small once the allocator purges**: the memory is live data. At the 1M compaction peak: 2.1 GB materialized
documents, about 1.5 GB build copies, 0.25 GB text statistics, and the block cache whatever its budget allows (1 GiB on a 4 GB box).
The 1M ingest floor with the starting code and jemalloc was 2.8 GB peak (earlier glibc runs: 5.9 GB at 1M).

### What changed

1. **jemalloc** (`bf-alloc`) with background purge threads, 4 arenas, and 1 s dirty and muzzy decay. Statistics are in `/metrics`
   (`blowfish_alloc_bytes{kind}`) and `BF_MEM_LOG_SECS`; the heap profiler is the `heap-profile` feature.
2. **Size by memory**: segment rows (limit / 80,000, 25K to 200K), bulk flush rows, build concurrency (1 below 8 GiB), unindexed tail bytes (limit / 16, so
   256 MiB on 4 GB), and recluster `max_slab_rows` (limit / 40,000) follow the host or cgroup memory limit (read from the process's own cgroup).
3. **Builder takes ownership** (`IndexBuilder::build_owned`): one fewer f32 copy of the input (4 KB per row) in segment builds and recluster slabs.

### Measured after (c7gd.2xlarge host, process in a 4 GiB cgroup with swap off, 1 GiB block cache, 2 rayon threads)

| Test | Before | After |
| --- | ---: | ---: |
| 1M ingest, peak RSS (cap 4 GiB) | 2.8 GB (8 GB box, no cap) | 3.4 GB, 7,084 docs/s, no OOM (3 retries on 429) |
| 1M recluster, peak RSS | 8.4 GB (10.3 GB profiled), 58 s | **2.5 GB** with 15 slabs of at most 100K rows (largest cell 137K split in two), 196 s on 2 threads |
| 3M ingest in a 4 GiB cgroup | not run | **OOM-killed at 1.56M rows** (4.05 GB RSS at 4,110 docs/s, 350 retries): floor grew 1.0 to 1.9 GB between 0.5M and 1.5M rows, with compaction spikes of +2 GB |

Recluster memory no longer depends on the row count (builder about 1.3 GB per 100K-row slab, the block cache, and about 10 bytes per row of
assignment arrays), so a 10M recluster should fit; that was not run. Ingest does not fit yet: the namespace-size term is
the text statistics and the compaction working set (below).

### BM25 cold at 1M MS MARCO (round 13 code, c7gd.2xlarge in-region, 50 queries, `ab.py`)

| Arm | p50 / p90 / p99 ms | Reads | MB | Waves | overlap@10 vs exact BM25 (1,000 queries) |
| --- | --- | ---: | ---: | ---: | ---: |
| turbopuffer published | 316 / 381 / 559 | | | | |
| fresh process | 380 / 526 / 659 | 194 | 4.4 | 3.4 | 1.0000 |
| cache-cold (`--cachecold`) | 301 / 369 / 429 | 194 | 4.4 | 3.5 | 1.0000 |

Target (p50 at most 395 or p90 at most 476): met on p50 in both arms and on p90 for cache-cold; fresh-process p90 is 526.

### What is left

- **Compaction working set**: stream compaction (read, merge, and write a segment's documents block by block instead of `segment::documents` for the whole merge),
  and keep vectors f16 or off-heap during compaction. 32 KB per row now.
- **Text statistics and the tail**: the corpus statistics that the indexer hydrates grow with the namespace (240 MB at 1M, so about 2.4 GB at 10M); and the
  tail holds base64 f32 documents (7 KB per row).
- **Builder**: `training` (normalized f32 copy) and `stored` are still full f32; build from f16 and normalize on the fly (target 6 KB per row).
- **Block cache pollution**: compaction and recluster reads should bypass the memory block cache.
- **Validation**: 10M load and recluster on a c7gd.large, cold ANN at 48 probes, and recall@10 were not run.

## Iteration 15 (memory): 10M load and recluster on a c7gd.large

Target: ingest 10M documents (1,024-dim f16 Wikipedia) and run `recluster` on a c7gd.large (2 vCPU, 3.8 GiB usable, swap off) with no OOM and
peak RSS of at most 3.5 GB. **Met**: ingest, index builds, and compaction peaked at 2.26 GB (10-second samples of VmHWM; 2.09 GB in the 5-second
allocator log), and recluster at 1.76 GB. The Python loader ran on the same machine (about 0.5 GB), and MemAvailable never fell under 0.97 GB. Block
cache 0.5 GiB while loading (1 GiB for the query runs), decoded cache at its default of 512 MiB, server in a 3.5 GiB cgroup.

### What changed

1. **Packed builder.** `IndexBuilder::build_packed` takes rows at the stored precision (f16 bytes) and normalizes on the fly for training, assignment, and
   quantization; the vector bytes in the blob are the packed input copied once. Peak is the packed input plus the output blob (about 4.3 KB per row at
   1,024 dims in f16) instead of four f32 copies; the blob is byte-identical to the old builder's (checked with hashes on three configurations).
2. **Compaction keeps vectors packed.** A compaction reads each source segment block by block, takes the vector column as packed bytes straight from the
   column blocks, and reads the other columns without the vector; no base64 string is ever materialized (documents were about 10 KB per row).
3. **Text statistics merged by streaming.** The indexer no longer hydrates the per-term map (3 to 4 copies, 240 MB per 1M rows). It streams the previous
   chunked FST object, applies the batch's sorted term deltas, and writes new chunks (`text::merge_text_stats`, tested to be byte-equal to a full
   rewrite); a publication that changes no text statistics keeps the object.
4. **Scans bypass the block cache.** `RangeReader::bypass_cache` reads through the caches without admitting blocks; compaction, text statistics reads,
   and recluster reads use it.
5. **Flat BM25 postings in the segment builder** (`bf-text`): a posting is 8 bytes plus its positions instead of about 48 bytes and a heap allocation
   (the old layout was about 17 KB per Wikipedia row and dominated a compaction). The segment bytes are identical (hash-checked).
6. **Recluster.** The coarse-partition sample is `cells * 600` rows (19,200 at 32 cells) instead of 100,000 f32 rows, slabs are built from the spill file
   as packed bytes, and `max_slab_rows` is memory / 12,000 (330,000 on this machine).

### 10M on c7gd.large (swap off)

| Phase | Peak RSS | Time |
| --- | ---: | ---: |
| Ingest, index builds, and compaction (whole load; the 2,000-second windows peak between 1.90 and 2.09 GB) | 2.09 GB (2.26 GB VmHWM) | 11,568 s, 864 docs/s average |
| Index drain after the last write | 1.87 GB | |
| Idle after the load (caches) | 1.33 GB | |
| Recluster, vector copy (10M rows) | 0.59 GB | 272 s |
| Recluster, assignment | 0.57 GB | 133 s |
| Recluster, slab builds (45 slabs, largest cell 449,930 rows split in two) | 1.76 GB | 4,287 s |

Ingest rate fell from about 3,200 docs/s at 0.5M rows to about 1,500 at 3M and 300 near 9M: every publication rewrites the text statistics object and
checks every segment's ID index, and the indexer wrote 19.0M segment rows for 10M ingested (1.9 times). On the same instance and loader, the previous
code ingested 1M rows at 3,392 and 1,900 docs/s in two runs and this code at 2,290 and 3,800 (round-to-round noise from the host and downloads is
larger than the difference); build time per row was 185 and 174 microseconds. The previous code cannot load this namespace in this memory at all.

### Cold ANN at 48 probes, 10M, c7gd.large (50 queries; recall over all 200 oracle queries)

The new namespace's exact top 10 (the loader's own truth) matches `oracle-10m.npz` as a set for all 200 queries (198 equal in order, 2 tie
swaps), so the same ground truth applies. 1 GiB block cache, 256 MiB decoded cache, 2 rayon threads.

| Arm | p50 / p90 / p99 ms | Requests, MB, waves | Recall@10 |
| --- | --- | --- | ---: |
| c7gd.2xlarge, slabs (iteration 11 report) | 662 / 991 (fresh) | 404, 50.7, 3.5 | 0.953 |
| c7gd.large, fresh process | 769 / 973 / 1,384 | 436, 53.5, 3.2 | 0.9545 (200 queries; 0.952 on the 50 timed) |
| c7gd.large, cache-cold (`--cachecold`) | 623 / 740 / 935 | 436, 53.5, 3.2 | 0.952 |
| c7gd.large, warm, 1,000 queries | 81.8 / 96.0 / 121.9 | | 0.9545 |

### Not done and what is left

- The per-row cost of a compaction is still about 12 KB (non-vector documents, BM25 build state, packed vectors, and the blob); text, filter, and
  trigram builds still hold a segment's non-vector documents rather than streaming them.
- The unindexed tail still holds base64 f32 documents (bounded by bytes, not compacted).
- Ingest throughput decays with namespace size (above): a separate bloom object per segment (so a write batch need not load every segment's ID
  index) and a larger bulk flush would cut that.

## Iteration 16 (ingest): throughput that stays flat as the namespace grows

Round 15 made 10M fit in 4 GB, but ingest slowed as the namespace grew (about 3,200 docs/s at 0.5M rows, 300 near 9M on a c7gd.large). This round
finds out why and makes it flat. Target, on a c7gd.xlarge (4 vCPU, 8 GB, swap off, server in a 3.5 GiB cgroup, the Python loader on the same
machine): ingest docs/s at 9M to 10M rows within 25% of the rate at 1M, and peak RSS under 3.5 GB. **Met**: 4,540 docs/s over 9M to 10M against 4,187 to
4,683 at 0.5M to 1M (an average of 4,501 over the 10M, against 2,126 before), and a 2.51 GB peak (VmHWM) on the xlarge. The 10M load takes 2,222 s
instead of 4,704 s, and the namespace is fully indexed 7 s after the last write.

### Where ingest time went (profile first)

`BF_TRACE_STAGES=1` prints the time of each stage of an indexing cycle, and `perf record --call-graph dwarf` on the server (loader on the same box)
gives the CPU view. Before: the loader is not the bottleneck (the loader meets a 429 about once per batch (4,575 retries over 5,000 batches), so the server is the
limit; the Python loader averages about a third of a core, though it reaches two cores while it checks truth for a batch). The server's time per cycle:

| Rows | rows/cycle | builds + compaction read | directories | text stats | claim, publish, other | s/cycle |
|---|---:|---:|---:|---:|---:|---:|
| 1.0M | 23,256 | 2.6 | 0.34 | 0.33 | 0.41 | 3.6 |
| 3.0M | 21,277 | 7.3 | 0.58 | 0.95 | 0.64 | 9.5 |
| 5.0M | 20,000 | 5.4 | 0.69 | 1.26 | 0.74 | 8.1 |
| 7.0M | 20,833 | 5.4 | 0.82 | 1.60 | 0.81 | 8.6 |
| 9.0M | 21,277 | 5.3 | 1.12 | 1.87 | 1.04 | 9.3 |
| 10.0M | 26,316 | 4.7 | 1.20 | 1.96 | 1.08 | 8.9 |

- **Compaction rewrites.** Up to 64 segments (about 1.3M rows) the load only flushes; after that every cycle also merges, so rows are built 1.9 times and
  the rate halves, from about 4,900 to about 2,300 docs/s between 1M and 2M rows. This is the biggest term and does not depend on the namespace's size after that.
- **Text statistics** (the whole object is read and rewritten on every flush): 0.33 s per cycle at 1M, 1.96 s at 10M. perf at 9.6M shows
  `merge_text_stats` at 13.6% of CPU.
- **ANN directories** (both layouts, plus their hints, are written on every publication): 0.34 s to 1.2 s. **Claim, publish, and the live-bitmap pass**: 0.4 s to 1.1 s.
- **ID checks.** Every write batch and every publication asks every segment whether it holds the IDs. A segment's Bloom filter (10 bits per ID) passes 1.7%
  of absent IDs, so a 2,000-row batch always passed in every segment and the segment's whole ID list (35 bytes per ID decoded, 350 MB at 10M against a 512 MiB cache) was
  read and parsed. At 10M on the xlarge this sets in at 9M: the rate falls from about 2,050 to 1,300 and then 920 docs/s over the last 1M (`lookup_many`
  is 1.2% of CPU at 9.6M and the cache churns); on the 2-vCPU large it is the collapse to 300 docs/s.
- **CPU.** `nk_sqeuclidean_f32_sve` (k-means training and assignment in segment builds) is 37% of cycles at 0.65M, 45% at 2.2M and 5.3M, and 27% at 9.6M;
  zstd (text segment blocks) is 8 to 11%, BM25 `add_document` 3%, RaBitQ encode and transform 4%.
  WAL commit and S3 waits do not show: the S3 client (`aws_lc` SHA-256) is 1.4%.

### What changed

1. **Per-segment ID filter** (`idfilter.bff`, new file in each segment). The segment's smallest and largest ID plus a Bloom filter at 24 bits per ID with 16 probes
   (false-positive rate about 1e-5, 3 bytes per row; the ID list `ids.bfi` is 35). `lookup_many` first takes only the wanted IDs inside the segment's range
   (a range query on the sorted set, so a bulk load's sequential IDs never touch an old segment), tests them against the filter, and opens `ids.bfi` only
   if one passes. Segments built earlier have no filter and take the old path (tested by deleting the filters from a manifest; a 10M namespace loaded by the
   previous binary accepted patches and deletes under the new one). Layout: `BFIF0001`, probe count, a JSON `[min, max]`, then the bits.
2. **Bulk loads flush big and compact later.** The unindexed limit is now memory / 8 (448 MiB in a 3.5 GiB cgroup; it was /16) and a bulk load flushes at three quarters of it
   (`BF_BULK_FLUSH_BYTES`), so a flush builds a segment of about `max_segment_rows` (43,750 on this machine). While writes arrive, compaction waits until a namespace holds 1,024
   segments (`BF_DEFER_COMPACTION_SEGMENTS`, was 64). Each row is built once during the load (the rewrite factor goes from 1.9 to 1.0); the merges run when writes pause,
   and segments already at the cap do not merge at all.
3. **The text statistics merge runs beside the builds**, not after them. It needs only the tail's statistics and the previous object, so it is one more future in
   the `try_join` with the flush and compaction builds; it decodes and encodes its chunks in parallel (`rayon`, 32 chunks at a time; the output is byte-identical
   to the sequential merge, checked on a 300,000-term object).
4. **The two ANN directory layouts are written at the same time.**

### Ingest at 10M, c7gd.xlarge (3.5 GiB cgroup, loader on the same machine)

Docs/s over each 0.5M rows, same loader, same instance type, one run each:

| Rows | Before, xlarge docs/s | s | After, xlarge docs/s | s | After, c7gd.large docs/s | s |
|---|---:|---:|---:|---:|---:|---:|
| 0.5M | 4,959 | 101 | 4,683 | 107 | 3,281 | 153 |
| 1.0M | 4,718 | 207 | 4,187 | 227 | 3,090 | 315 |
| 1.5M | 2,989 | 374 | 4,431 | 339 | 3,093 | 476 |
| 2.0M | 2,077 | 615 | 4,530 | 450 | 2,966 | 645 |
| 2.5M | 1,974 | 868 | 4,512 | 561 | 3,113 | 806 |
| 3.0M | 2,434 | 1,074 | 4,517 | 671 | 3,038 | 970 |
| 3.5M | 2,507 | 1,273 | 4,427 | 784 | 2,920 | 1,141 |
| 4.0M | 2,416 | 1,480 | 4,358 | 899 | 3,017 | 1,307 |
| 4.5M | 2,256 | 1,702 | 4,583 | 1,008 | 2,996 | 1,474 |
| 5.0M | 2,472 | 1,904 | 4,310 | 1,124 | 2,841 | 1,650 |
| 5.5M | 2,236 | 2,128 | 4,497 | 1,235 | 2,966 | 1,819 |
| 6.0M | 2,077 | 2,368 | 4,326 | 1,351 | 2,847 | 1,994 |
| 6.5M | 2,188 | 2,597 | 4,658 | 1,458 | 2,851 | 2,170 |
| 7.0M | 2,412 | 2,804 | 4,472 | 1,570 | 2,855 | 2,345 |
| 7.5M | 2,032 | 3,050 | 4,730 | 1,676 | 2,842 | 2,521 |
| 8.0M | 2,048 | 3,294 | 4,538 | 1,786 | 2,717 | 2,705 |
| 8.5M | 2,038 | 3,540 | 4,783 | 1,890 | 2,708 | 2,889 |
| 9.0M | 2,077 | 3,781 | 4,639 | 1,998 | 2,814 | 3,067 |
| 9.5M | 1,306 | 4,163 | 4,315 | 2,114 | 2,796 | 3,246 |
| 10.0M | 921 | 4,704 | 4,639 | 2,222 | 2,744 | 3,427 |
| total | 2,126 avg | 4,704 | 4,501 avg | 2,222 | 2,918 avg | 3,427 |

The rate over a window moves by about 5% between runs on this instance (a first run of the same binary measured 5,165 at 1M, and a build running at
low priority on the same machine in that run cost 15% from 7M on, so its curve is not used). Per cycle, after:

| Rows | Before: rows/cycle | builds + compaction read | directories | text stats | claim, publish, other | s/cycle | After: rows/cycle | builds | directories | text stats | claim, publish, other | s/cycle |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1.0M | 23,256 | 2.6 | 0.34 | 0.33 | 0.41 | 3.6 | 37,037 | 4.5 | 0.16 | 0.00 | 0.67 | 5.4 |
| 2.0M | 18,868 | 5.5 | 0.53 | 0.70 | 0.63 | 7.4 | 31,250 | 4.5 | 0.27 | 0.00 | 0.53 | 5.3 |
| 3.0M | 21,277 | 7.3 | 0.58 | 0.95 | 0.64 | 9.5 | 29,412 | 4.5 | 0.26 | 0.00 | 0.69 | 5.4 |
| 5.0M | 20,000 | 5.4 | 0.69 | 1.26 | 0.74 | 8.1 | 34,483 | 5.0 | 0.46 | 0.00 | 0.62 | 6.1 |
| 7.0M | 20,833 | 5.4 | 0.82 | 1.60 | 0.81 | 8.6 | 35,714 | 5.3 | 0.51 | 0.00 | 0.58 | 6.4 |
| 9.0M | 21,277 | 5.3 | 1.12 | 1.87 | 1.04 | 9.3 | 40,000 | 6.0 | 0.64 | 0.00 | 0.63 | 7.3 |
| 10.0M | 26,316 | 4.7 | 1.20 | 1.96 | 1.08 | 8.9 | 38,462 | 5.9 | 0.68 | 0.00 | 0.79 | 7.4 |

On the c7gd.large (2 vCPU, 3.8 GiB, server in a 3.5 GiB cgroup, loader on the same machine, 2 rayon threads) the same binary loads 10M in 3,427 s
(2,918 docs/s on average) where round 15 took 11,568 s (864): 3,281 docs/s over the first 0.5M and 2,744 over the last, against about 3,200 falling to 300 before.
Peak server RSS is 2.27 GB in the 5-second allocator log (VmHWM 2.31 GB), and the machine's MemAvailable never fell under 1.09 GB.
On the xlarge the peak is 2.38 GB in the log (VmHWM 2.51 GB), reached about ten minutes in and flat after that: the larger tail (448 MiB instead of 224 MiB) costs about 0.25 GB over round 15,
and the ID lists no longer fill the decoded cache.

### Validation on the new namespace (xlarge)

- `scripts/compat_both.sh`: 121/121 in both modes (run on the x86 development VM). `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, and `cargo test -p bf-engine` pass.
  New tests: the filter's format (round trip, false-positive rate, string IDs, corrupt bytes), a 300,000-term statistics merge equal to a full rewrite, and
  an engine test that replaces, patches, and deletes rows spread over ten segments and then repeats it with the filters stripped from the manifest.
- The exact top 10 the loader computed for this namespace matches `oracle-10m.npz` as a set for all 200 queries (198 equal in order), so the same ground truth applies.
- `recluster` (32 cells, `max_slab_rows` 330,000 as on the large): 47 slabs (largest cell 493,055 rows), 49,976 clusters, 10.0 MB directory, 2,967 s on 4 vCPU (copy 135 s,
  assignment 65 s, slabs 2,767 s; round 15 took 4,692 s on 2 vCPU), peak RSS 1.2 GB.
- Cold ANN at 48 probes, 10M (1 GiB block cache, 256 MiB decoded cache; recall@10 over the 200 oracle queries):

| Arm | p50 / p90 / p99 ms | Requests, MB, waves | Recall@10 |
| --- | --- | --- | ---: |
| c7gd.large, fresh process (round 15) | 769 / 973 / 1,384 | 436, 53.5, 3.2 | 0.9545 |
| c7gd.large, cache-cold (round 15) | 623 / 740 / 935 | 436, 53.5, 3.2 | 0.9545 |
| c7gd.xlarge, fresh process, 2 rayon threads (50 queries) | 751 / 966 / 1,079 | 443, 53.9, 3.4 | 0.934 on these 50 |
| c7gd.xlarge, fresh process, 4 rayon threads (50 queries) | 745 / 1,181 / 1,469 | 443, 53.9, 3.4 | 0.934 on these 50 |
| c7gd.xlarge, cache-cold, 4 rayon threads (200 queries) | 600 / 751 / 929 | 445, 53.7, 3.3 | 0.956 |

Latency and recall have not regressed (the first 50 queries score 0.934 against 0.956 over all 200 in every arm).

### Projection for a 100M load

Measured per-cycle costs that grow with the namespace (after, xlarge): directories 0.16 s at 1M to 0.68 s at 10M (about 0.06 s per million rows), claim and publish 0.8 to 1.1 s
(about 0.015 s per million). The text statistics merge is hidden behind the builds at 10M; it is a serial 1.96 s there before this round, and grows with the vocabulary.
- **If the rate stays flat** at 4,500 docs/s, 100M takes about 22,000 s (6.2 hours) on this instance, about 9.5 hours at the large's 2,900.
- **With the growth above continuing** (directories about 6 s, claim and publish about 2 s, and a statistics merge of several seconds per cycle on a 40,000-row flush,
  which then no longer hides behind a 6 s build), a cycle is 14 to 20 s, the rate 2,000 to 2,800 docs/s, and the load 10 to 14 hours.
- A bigger machine helps twice: segments and flushes scale with memory (`max_segment_rows` is memory / 80,000, so 200,000 rows on 16 GB), which divides the per-cycle terms by five, and
  the builds get more cores.
- At 100M the segment count is about 2,300, so the deferral limit of 1,024 would start merging at 45M rows on this machine; raise it with `BF_DEFER_COMPACTION_SEGMENTS`.

### Not done and what is left

- The text statistics object, both ANN directories, and the manifest are still rewritten in full on every publication, so those terms stay linear in the namespace. Next: a log-structured
  statistics object (small additive deltas, folded geometrically), an incremental directory, and manifest delta records.
- Compaction is deferred, not cheaper: merging two IVF segments still re-trains k-means (37 to 45% of build CPU). A merge that keeps the source clusters, or reuses their centroids, would make the merges after a load cheap.
- Builds are CPU-bound (k-means, zstd on text blocks) and the loader's truth computation takes cores from the server on a small machine; a lean loader (no truth) would raise the numbers here.
- The unindexed tail still holds base64 f32 documents (7 KB per row), which sets the flush size on a small machine; holding it packed would double the flush.

## 100M: Wikipedia embeddings, in-region and over the internet

A 100M-row load of the Cohere Wikipedia (2023-11, multilingual v3, 1,024-dim, f16) embeddings, then `recluster`, then cold and warm ANN queries.
Rows: the first 41M or so are English, exactly as in the 10M runs (`oracle-10m.npz` stays valid as a subset check), then German, French, Spanish, and
Russian, in that order, until 100M (the loader's `LANGUAGES` list; ids are language index times 10^9 plus shard times 100,000 plus the row, so
English ids are unchanged). The 200 held-out queries are English. Truth is exact top-10 cosine, kept by the loader while it streams
(`bench/scale/data/oracle-100m.npz`), and spot-checked against brute force on nine sampled shards across all five languages (834,543 rows, 200 queries;
no row outside the truth scores above a query's tenth-best truth score).

Machine: one on-demand c7gd.2xlarge (8 vCPU, 16 GB, 441 GB local NVMe) in the same region as the bucket. Server in a 10 GiB cgroup, loader in a 4 GiB
cgroup on the same box (`bench/scale/big/start_load.sh`).

### Load

| Rows | Docs/s over the last 5M | Seconds |
|---|---:|---:|
| 5M | 4,042 | 1,254 |
| 10M | 6,418 | 2,034 |
| 20M | 6,297 | 3,582 |
| 30M | 6,555 | 6,182 |
| 50M | 6,378 | 9,359 |
| 70M | 5,777 | 12,881 |
| 90M | 5,265 | 16,609 |
| 95M | 3,760 | 17,939 |
| 100M | 1,995 | 20,445 |

100M rows in 20,445 s (5.7 h), 4,891 docs/s on average, with 525 backpressure retries. Peak server RSS 6.8 GB (7.1 GB VmHWM); minimum MemAvailable on the
box 8.5 GB. The 10M mark came at 2,034 s, so the projection from it was 5.7 h flat (the stop line was 30 h). The rate holds at 6,000 to 6,600 docs/s to 50M and
then declines (5,300 at 90M; 2,000 over the last 5M; I did not profile that decline). Between 21M and 25M rows the load lost about 17 minutes to a
bug found and fixed here (below).

**Bug found and fixed: the WAL payload GC re-read the whole log serially.** One hour into the load (the retention window), `collect_garbage` calls
`prune_payloads(through)`, which loaded the record of every WAL sequence from 1 to `through` one GET at a time, every pass (every 60 s), inside the loop that
also runs indexing. At 22M to 25M rows (about 12,000 writes) a pass took 30 to 230 s with the server idle, and the loader stalled on backpressure: the rate fell from
6,300 to 1,600 docs/s. The fix: `Manifest::pruned_through` remembers how far the log is reclaimed, a pass reads only `(pruned_through, through]`, and the
reads and deletes run 32 at a time (`Wal::prune_payloads_from`, with a test). After a server restart on the new binary the rate returned to 6,800 docs/s.
The change is additive (older manifests read `pruned_through` as 0 and prune once from the top).

### Recluster

| Step | Time | Peak RSS |
|---|---:|---:|
| Copy 100M rows to the local spill (about 200 GB on NVMe) | 684 s | |
| Assign rows to 32 coarse cells (largest 7,283,025) | 1,010 s (cumulative) | |
| 160 slab builds (about 30 s each) | 6,248 s total | 6.0 GB (VmHWM 6.4 GB) |

160 slabs, 499,920 clusters (200 rows per cluster), directory 98.7 MB (10M: 47 slabs, 49,976 clusters, 10.0 MB). The 10M recluster took 2,967 s on 4 vCPU.
After recluster the `big/` prefix holds 945 GB in 74,124 objects (slabs 238 GB, segments 683 GB including retired ones, WAL payloads 19 GB).

### Query: c7gd.2xlarge in the same region as the bucket, 100M

`ab.py`, 50 queries each; recall@10 for the cold rows is over those 50; warm is 1,000 queries at concurrency 1 over the 200 oracle queries. 8 GiB block cache,
default decoded cache, 6 rayon threads (the same setup as the 10M in-region table).

| Arm | p50 / p90 ms | p99 ms | Requests, MB, waves | Recall@10 |
|---|---|---:|---|---:|
| 48 probes, fresh process | 3,105 / 4,051 | 6,002 | 1,186, 294.0, 4.1 | 0.950 |
| 48 probes, cache-cold | 2,698 / 3,071 | 3,691 | 1,186, 294.0, 4.2 | 0.950 |
| 48 probes, warm | 333 / 378 | 410 | | 0.941 (200 queries) |
| 64 probes, fresh process | 2,899 / 3,806 | 7,210 | 1,510, 326.8, 4.1 | 0.958 |
| 64 probes, cache-cold | 2,687 / 2,936 | 4,632 | 1,510, 326.8, 4.2 | 0.958 |
| 64 probes, warm | 387 / 434 | 480 | | 0.9555 (200 queries) |

### Query: over the internet (this VM, outside AWS), 100M, 48 probes

A read-only server on a 32 GB x86 VM against the same prefix. Network floor to the bucket from here: TCP connect 33.9 / 38.4 ms (p50 / p90), TLS
handshake 55.8 / 60.6, 4 KB GET 56.7 / 71.3, 1 MB GET 65.9 / 76.1 (in-region: 1.6, 26.9, 24.5, 30.2).

| Arm | p50 / p90 ms | p99 ms | Requests, MB, waves | Recall@10 |
|---|---|---:|---|---:|
| fresh process | 6,399 / 8,929 | 26,901 | 1,186, 294.0, 3.5 | 0.950 |
| cache-cold | 8,231 / 21,575 | 37,372 | 1,186, 294.0, 3.5 | 0.950 |
| warm, 1,000 queries | 448 / 513 | 565 | | 0.941 |

The cold numbers are bandwidth-bound (294 MB a query through this VM's link, with a long tail) and noisy; read them as an order of magnitude. The 64-probe
arm was not run over the internet (each cold query moves about 300 MB through the bucket's egress).

### 10M to 100M (in-region, c7gd.2xlarge, 48 probes)

| | 10M | 100M | Ratio |
|---|---:|---:|---:|
| Cold, fresh p50 / p90 ms | 662 / 991 | 3,105 / 4,051 | 4.7x / 4.1x |
| Cold, cache-cold p50 / p90 ms | 574 / 652 | 2,698 / 3,071 | 4.7x / 4.7x |
| Warm p50 / p90 ms | 70.7 / 84.1 | 333 / 378 | 4.7x / 4.5x |
| Requests, MB per cold query | 404, 50.7 | 1,186, 294.0 | 2.9x, 5.8x |
| Waves | 3.5 | 4.1 | |
| Recall@10 (48 probes) | 0.953 | 0.941 (0.950 on the cold 50) | |
| Recall@10 (64 probes) | 0.9615 | 0.9555 | |
| Slabs, clusters, directory | 47, 49,976, 10.0 MB | 160, 499,920, 98.7 MB | 3.4x, 10x, 9.9x |

Per-stage timeline of one cache-cold query, 48 probes (2,694 ms):
- wave 0, the two ANN directory hints: 2 requests, 185.7 MB, 1,093 ms (10M: 15.3 MB, 100 ms);
- wave 1, the WAL tail: 2 requests, 94 ms;
- wave 2, scan fragments: 1,191 requests, 115.1 MB, 753 ms (10M: 361 requests, 36.8 MB, 181 ms);
- wave 3, rerank vectors and documents: 51 requests, 1.8 MB, 318 ms.

What this says: cold and warm both grow 4 to 5x for 10x the data, much better than linear but far above the 10M numbers.
- The directory hint is the main cold term. It grows with the cluster count (10x), and a cold query reads both layouts' hints (186 MB, 63% of the bytes and 40% of the time).
  A directory that is read in pieces (a coarse level first), or only the layout in use, is the clear next step.
- The scan wave reads 3x the bytes and makes 3.3x the requests: 48 probes over 160 slabs touch more slabs and larger clusters (the recluster cells are no finer than at 10M).
- Warm queries cost 4.7x because a probe scans larger slabs, and recall is a little lower than at 10M for the same probe count (0.941 against 0.953 at 48), so 64 probes
  (0.9555) is the like-for-like setting. Turbopuffer publishes no 100M number to compare with.
