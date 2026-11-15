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
https://hop.rip/ritt2

https://developers.cloudflare.com/r2/platform/limits/
https://hop.rip/32da7

https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3
https://hop.rip/a2ant

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

## Compatibility validation

The 121-case compatibility suite passes in both normal mode and forced indexing
mode (`BF_INDEX_ROWS=1`, `BF_INDEX_AGE_MS=1`, and `BF_INDEX_INTERVAL_MS=50`), against
an isolated local file-store server built from commit `736162f`. No oracle or
production namespaces were used for these checks.
