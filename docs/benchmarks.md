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

### Per-milestone rows

Add one row per milestone with the same 200 queries (blowfish client p50 / p90 / p99, then overlap@10 against
turbopuffer). turbopuffer's numbers do not change between milestones except for its own variance.

| Milestone | Commit | ANN | BM25 | Hybrid | Selective ANN | Broad ANN |
| --- | --- | --- | --- | --- | --- | --- |
| 0: iteration 2 main | `e12178f` | 76 / 120 / 179 ms, 0.963 | 73 / 110 / 191 ms, 0.982 | 78 / 117 / 261 ms, 0.973 | 74 / 123 / 232 ms, 0.998 | 74 / 110 / 166 ms, 0.992 |
| 1: iteration 3 | `73d8110` | 87 / 116 / 155 ms, 0.965 | 90 / 129 / 201 ms, 0.982 | 100 / 133 / 299 ms, 0.972 | 98 / 136 / 216 ms, 0.998 | 100 / 131 / 234 ms, 0.991 |
