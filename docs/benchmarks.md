# Scale benchmark log

## Current numbers

Chronological lab notebook (iterations 0 through 18, plus 100M runs). For the concise summary and reproduction guide, see [`benchmarks/README.md`](benchmarks/README.md); for estimated infrastructure and benchmark costs, see [`cost-estimates.md`](cost-estimates.md).

Latest in-region AWS S3 numbers (archived `v0.1.14`, strong consistency,
1,024-dimensional `f16` Cohere Wikipedia embeddings; see [v0.1.14 in-region](#v0114-in-region)
for upgrade timings, full comparison tables, memory limits, and cost):

- **10M vector, 48 probes, recall@10 `0.9525`:** 625.5 / 924.3 ms fresh, 454.3 / 533.6 ms cache-cold, and 18.2 / 25.7 ms warm; 216.1 queries/s at 16 clients. At 64 probes, warm is 20.3 / 30.7 ms at recall `0.9615`. Published turbopuffer (768-dimensional): 874 / 1,214 ms cold and 14 / 17 ms warm.
- **100M vector, 64 probes, recall@10 `0.9540`:** 1,251.6 / 1,429.1 ms fresh, 1,149.3 / 1,341.7 ms cache-cold, and 150.0 / 228.5 ms warm under a 10 GiB cgroup. The 8 GiB payload-cache warm prewarm OOMed; reported 100M warm arms use 7 GiB, and are slower than prior documented results. A separate 12 GiB diagnostic is recorded below. Published turbopuffer has no 100M result.
- **1M BM25, MS MARCO, exact overlap@10 `1.0000`:** 249.4 / 449.1 ms fresh, 206.5 / 261.2 ms cache-cold, and 11.2 / 14.2 ms warm. Published turbopuffer: 316 / 381 ms cold and 13 / 18 ms warm.
- **Actual 4 GB query check, c7gd.large, 48 probes:** 670.4 / 833.3 ms fresh and 536.3 / 637.1 ms cache-cold, swap off. This rerun measured queries; historical 10M ingest and recluster validations remain [Iteration 15](#iteration-15-memory-10m-load-and-recluster-on-a-c7gdlarge), [Iteration 16](#iteration-16-ingest-throughput-that-stays-flat-as-the-namespace-grows), and [Iteration 18](#memory-check-10m-load-and-recluster-4-gib-cgroup-swap-off-2-cpus).
- **One machine, BEIR NQ (2.68M x 1,024 f16, local disk; [Iteration 19](#iteration-19-beir-natural-questions-on-one-machine)):**
  reclustered at 64 probes, recall@10 `0.955`, warm `8.3 / 18.1 ms` p50 / p99, and `444 q/s` at 16 clients on 6 pinned
  vCPUs (was `28.7 / 732 ms` and `27 q/s`). As loaded at default settings, recall@10 is `0.960` (was `0.907`). On an
  [Apple M4 at v0.1.15](#apple-m4-at-v0115-blowfish-qdrant-and-lancedb): `1.3–1.4 / 1.9–2.0 ms` and
  `1,814 q/s` at 64 probes (recall@10 `0.961`).
  [Iteration 20](#iteration-20-fewer-cycles-per-row-and-better-placed-rows-beir-nq-one-machine): `573 q/s` at recall@10
  `0.954` reclustered (54 probes, against main's `395 q/s` at `0.955` in the same interleaved run), `0.960` at the
  default 64 probes, and `387 q/s` as loaded (main `269`).
  [Iteration 22](#iteration-22-the-fixed-costs-of-a-warm-vector-query-beir-nq-one-machine): 40% fewer instructions and
  20% fewer cycles a query at the same recall@10 (`0.961`), and `574 q/s` against main's `509` on a shared VM.
  [Automatic recluster](#automatic-recluster-rounds-r1-and-r2-beir-nq-one-machine): the server reclusters NQ by itself
  in `231 s` after the load, at recall@10 `0.962` and `587 q/s`. With 5% of rows rewritten afterwards, a query costs
  `20.9 M` instructions against `15.5 M` fully covered.
- **My own hosted-service measurements (10M, caches cannot be forced cold externally):** turbopuffer (server-reported) `"cold"`-labelled after 5 min idle about `670 / 1,120 ms` (one deep-cold sample after 2 days idle `2,051 ms`; at 100M, still `"warm"` at `201 ms` after a day idle); Amazon S3 Vectors recall@10 `0.894`, warm `60 / 278 ms`, `"cold"` after 30 min idle `178 / 276 ms`.

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
the idf, by a near-constant additive offset: for 40 percent of terms turbopuffer's idf is exactly +0.169 above blowfish's
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
  over 2 s at 1,024. The coordinator's diagnosis (queries pay the tail) holds.
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
  number minus waves times (the request RTT minus 15 ms), where 15 ms is an assumed in-region RTT, the request RTT is the 4 KB
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
store, and a PUT costs about three times a GET. This is the difference I cannot remove from this VM. A same-region test needs a
VM in the bucket's region (or an object store there that I do not run on R2), and would need a new bucket and VM, so it was not
done. turbopuffer measures in-region next to its storage.

### Vector query: 10M docs, strong consistency

blowfish is the existing 10M, 1024-dimension Wikipedia namespace (70 segments, f16 vectors, about 5.6 KB per document). turbopuffer's
published run is 768 dimensions, so blowfish's vectors are 33 percent larger: this is a conservative upper bound. Recall@10 is against exact
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

1. **Network:** blowfish crosses the Internet to R2 (83 ms for a 4 KB GET, 270 ms for a PUT), while turbopuffer measures in-region. Not removable
   from this VM. The adjusted estimates above assume 15 ms in-region.
2. **Vector dimension:** 1024 against 768 (33 percent more bytes). The 768-dimension reload was skipped, on instruction.
3. **Layout:** the 10M namespace has 70 segments, because its last 2M rows were loaded with `BF_MAX_SEGMENT_ROWS=100000` and never
   compacted. A compacted namespace would read fewer, larger objects.
4. **Warm consistency check:** every strong query pays one R2 round trip to check the WAL tail, which dominates blowfish's warm numbers
   (77 to 136 ms against about 14 ms). turbopuffer does not have to cross a network for it.
5. **Corpus:** MS MARCO at 335 bytes per passage, not the 300 MB turbopuffer used; the queries are MS MARCO dev queries. The upsert rows
   are synthetic.
6. **Recall:** at the 64-probe default blowfish is below turbopuffer (0.886 against 0.943). The apples-to-apples row is 160 probes (0.947 warm,
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
| c7gd.2xlarge, slabs (v0.1.12 / iteration 12 report) | 662 / 991 (fresh) | 404, 50.7, 3.5 | 0.953 |
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

### Query: over the internet (this VM, outside AWS), 100M, 48 probes (pre-Iteration-17 code, not remeasured)

Measured before Iterations 17 and 18 (without the paged slab directory, compact manifest copy, or parallel slab scans), and not remeasured since. A read-only server on a 32 GB x86 VM against the same prefix. Network floor to the bucket from here: TCP connect 33.9 / 38.4 ms (p50 / p90), TLS
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

## Iteration 17 (100M): cold and warm ANN

Target at 100M (in-region on a c7gd.2xlarge): recall@10 of at least 0.95, cache-cold p50 of 1,200 ms or less, fresh-process p50 of 1,500 ms or less,
and warm p50 of 150 ms or less. Performance at 10M must not regress. These runs use the same machine, namespace (`wikipedia-100m`, 160 slabs, 499,920
clusters), workload, and measurement code as the 100M section above. In the tables below, `"main"` is pre-iteration-17 code and `"this branch"` is iteration 17 (now merged), measured in the same session.

### Where the time went (main, one cache-cold query at 64 probes, 3,049 ms)

- **Wave 0 (both ANN directory hints, slab and block layouts):** 2 requests, 185.7 MB, 1,281 ms.
- **Manifest listing:** 2,760 manifest objects require three sequential list pages (about 260 ms) before anything else is known.
- **Wave 2 (scan):** 1,402 requests, 131.3 MB, 682 ms.
- **Wave 4 (rerank vectors and documents):** 127 requests, 3.3 MB, 427 ms.
- **Between waves:** about 100 ms of CPU, because the scan waited until the last read completed. On a warm query, six clones of a namespace state holding
  1,787 segment records also cost about 10 ms each.

In this bucket, small ranged reads of objects that are not hot take 100 to 125 ms at p50 (measured with boto3 from the same instance using random 8 KB
ranges across many objects; a single hot object answered in 43 ms). Every serial wave therefore adds about 100 ms plus its tail, regardless of size. The
changes below cut bytes and waves; I cannot change S3 latency itself.

### What changed

- **Paged slab directory.** `recluster` (and the new `repage` example for previously reclustered namespaces) writes a paged copy of the slab directory
  beside the flat one. It starts with a resident header (4.0 MB at 100M) containing the segment table with each slab's object sizes, a one-bit code per
  cell, and a page table. One page per cell follows, each holding about 24 neighboring clusters (21,000 pages, 104 MB total). Cells come from two-level
  k-means over the approximate cluster centers and are ordered along a nearest-neighbor tour, so the pages a query needs sit together and merge into about
  100 requests. A query ranks the cells from the header, fetches the nearest pages until they hold 16 times the clusters it will probe (about 13 MB for 48
  probes, 18 MB for 64), and ranks only those clusters. At a multiple of 8, recall fell by 0.5 percentage points (0.005); at 16, it matches the flat directory at 48 probes and
  reaches 0.9540 against 0.9555 at 64 (table below). `repage` took 39 s at 100M without rebuilding any slabs.
- **Only the layout in use.** Cold queries previously downloaded both the slab and block layout hints (186 MB). They now request the paged, slab, and
  block hints together in that order of preference and drop the lower-priority requests once a higher-priority hint answers.
- **Manifest.** A cold open reads the fixed-key manifest copy and then lists only manifests after it (one request) instead of listing all 2,760. The
  object-store wrapper previously did not forward `list_with_offset`, so the first version still listed everything; that is now fixed and tested.
- **Speculation from the hint.** Because the hint header lists every slab object and its size, directory-page and scan reads start while the manifest is
  still in flight. The manifest confirms the layout afterward; a stale hint costs only the bytes.
- **Scan while reads arrive.** Prefetching scans each slab's clusters as its reads complete and hands the candidates to the query, so reranking starts
  immediately after the last read. The scan memo is exact because a scan is a pure function of the immutable index object, the query, and the probed
  clusters; it is skipped whenever a row mask applies.
- **Cheaper namespace state.** State clones share the checkpoint's segment list and slab sets via copy-on-write `Arc`, so the six clones per query no
  longer copy 1,787 segment records each. The probe plan partitions the nearest few thousand entries instead of sorting all of them, and per-segment entry
  slices replace a filter over all entries.
- **Warm queries skip the scan prefetch** when the directory pages are already resident.

New environment-only settings (in `docs/configuration.md`): `BF_ANN_PAGED`, `BF_ANN_PAGE_MARGIN`, `BF_ANN_PAGE_KAPPA`, `BF_ANN_PAGE_ENTRIES`,
`BF_ANN_PAGED_MIN_ENTRIES`, `BF_ANN_SCAN_MEMO`, and `BF_ANN_SPECULATE`.

### Results: c7gd.2xlarge, same region as the bucket, 100M

Measured with `ab.py` using an 8 GiB block cache, the default decoded cache, and 6 rayon threads. Cold arms run 50 queries each (fresh process: 50 for 48
probes, 30 for main at 64 probes); warm arms run 1,000 queries at concurrency 1. Recall@10 is over those 50 queries for cold rows and over all 200 oracle
queries for warm rows.

| Arm | p50 / p90 ms | p99 ms | Requests, MB, waves | Recall@10 |
|---|---|---:|---|---:|
| 48 probes, main, cache-cold | 2,478 / 2,863 | 3,433 | 1,186, 294.0, 4.1 | 0.950 |
| 48 probes, this branch, cache-cold | 949 / 1,131 | 1,253 | 1,295, 126.0, 4.5 | 0.950 |
| 48 probes, main, fresh process | 2,914 / 3,606 | 4,220 | 1,186, 294.0, 4.2 | 0.950 |
| 48 probes, this branch, fresh process | 1,047 / 1,223 | 1,520 | 1,295, 126.0, 4.3 | 0.950 |
| 48 probes, main, warm | 323.1 / 366.3 | 398.3 | | 0.9410 |
| 48 probes, this branch, warm | 123.5 / 142.9 | 159.9 | | 0.9410 |
| 64 probes, main, cache-cold | 2,677 / 2,953 | 4,096 | 1,510, 326.8, 4.2 | 0.958 |
| 64 probes, this branch, cache-cold | 1,164 / 1,348 | 1,614 | 1,637, 163.0, 4.5 | 0.958 |
| 64 probes, main, fresh process | 3,366 / 4,111 | 5,557 | 1,512, 326.5, 4.2 | 0.957 |
| 64 probes, this branch, fresh process | 1,228 / 1,411 | 1,813 | 1,637, 163.0, 4.4 | 0.958 |
| 64 probes, main, warm | 378.2 / 425.4 | 476.7 | | 0.9555 |
| 64 probes, this branch, warm | 168.3 / 201.1 | 249.0 | | 0.9540 |

At 64 probes (the setting that reaches 0.95 recall): cache-cold p50 is 1,164 ms (target 1,200, met), fresh-process p50 is 1,228 ms (target 1,500, met),
and warm p50 is 168 ms (target 150, **not met**). At 48 probes, recall is 0.941 and warm p50 is 123.5 ms. Cold p99 sits well above p50 because each wave
waits for its slowest request out of about a thousand.

### Per-stage timeline, one cache-cold query at 64 probes (traced runs; slightly slower than the table)

| Wave | main | This branch |
|---|---|---|
| Hints and manifest | 2 requests, 185.7 MB, 1,281 ms, then the WAL tail 91 ms | 1 request, 4.0 MB, 96 ms |
| Directory pages | (inside the hints) | 121 requests, 17.9 MB, 178 ms (starts at 96 ms, overlaps the manifest) |
| Scan | 1,402 requests, 131.3 MB, 682 ms (starts at 1,419 ms) | 1,404 requests, 131.6 MB, 699 ms (starts at 299 ms) |
| Gap before the rerank | about 350 ms (scan CPU, ranking 500,000 entries, state clones) | 35 ms |
| Rerank vectors and documents | 127 requests, 3.3 MB, 427 ms | 127 requests, 3.3 MB, 308 ms |
| Total | 3,049 ms | 1,380 ms |

The remaining time is in the scan (130 MB in about 650 ms across 1,400 requests of about 90 KB, limited by the slowest request) and the final wave (a
hundred small reads across different objects, limited by S3's 100 ms tail). Neither shrinks without fewer bytes per query or fewer waves.

### Recall against the page margin (64 probes, 200 oracle queries, warm runs)

| Page margin | Entries read (about) | Recall@10 | Warm p50 ms |
|---|---:|---:|---:|
| 12 | 38,000 | 0.9505 | 157 |
| 16 (default) | 51,000 | 0.9540 | 168 |
| 24 | 77,000 | 0.9550 | 189 |
| flat directory (main) | 500,000 | 0.9555 | 378 |

The margin 12 and 24 recall rows come from 400-query runs, and the margin 16 (default) row comes from the 1,000-query run; the margin 12 warm p50 comes from an earlier 1,000-query run before the probe plan was optimized. At 48 probes, margins of 12 and 16 both give 0.9410, matching the flat directory's recall.

### Tried, no gain

- **Hedge budget 0.4:** cache-cold p50 was 1,297 ms against 1,383 ms on the same build, within S3 noise.
- **Merge gaps (64, 128, 256, and 512 KB):** fewer requests, more bytes. Latency hit 1,275 ms at 128 KB, got worse above that, and did not improve when
  combined with hedging.
- **jemalloc 30 s dirty-page decay** (`_RJEM_MALLOC_CONF=background_thread:true,narenas:4,dirty_decay_ms:30000,muzzy_decay_ms:30000`): cuts about 14 ms
  from a warm query (the kernel spends 10 to 15 percent of a warm query faulting in freshly allocated read buffers), but retains freed memory longer than
  the memory targets allow, so it is not the default. Over 1,000 warm queries at 64 probes, it measured 155.7 / 180.0 ms, and 148.2 / 169.7 ms when paired
  with `RAYON_NUM_THREADS=8` instead of 6 (recall 0.954 in both). That meets the 150 ms warm target for a query-only node.

### No regression at 10M (in-region, c7gd.2xlarge, `scale/ns/wikipedia-10m`, read-only, 48 probes, same session, 50 cold queries)

The 10M namespace's directory (49,976 clusters) is below the paging threshold, so it takes the old path plus the manifest, hint, and state-clone changes.

| Arm | p50 / p90 ms | p99 ms | Requests, MB, waves | Recall@10 |
|---|---|---:|---|---:|
| main, cache-cold | 665 / 911 | 1,151 | 405, 50.7, 3.3 | 0.944 |
| this branch, cache-cold | 561 / 683 | 795 | 406, 50.2, 3.3 | 0.944 |
| main, fresh process | 703 / 938 | 1,063 | 405, 50.7, 3.7 | 0.944 |
| this branch, fresh process | 691 / 914 | 1,111 | 406, 50.1, 3.6 | 0.944 |
| main, warm | 47.4 / 59.0 | 73.1 | | 0.953 |
| this branch, warm | 43.2 / 55.8 | 71.7 | | 0.953 |

### Not done and what is left

- **Warm at 64 probes is 168 ms against the 150 ms target.** Roughly 10 to 15 percent of a warm query's CPU is kernel time from allocating and freeing
  about 130 MB of read buffers, and about 13 percent is the scan itself (`PlaneQuery::scan`). A longer jemalloc decay plus 8 rayon threads closes the gap
  (148.2 ms, above). The follow-up is to reuse read buffers or make those settings the default for `--read-only` servers.
- **Finer slabs.** A recluster with 100-row clusters (so each probe reads less and recall per probe rises) was not built, because the cold targets were
  met without it and 100M requires another 3 to 4 hour recluster. It remains the way to cut scan bytes, which now make up most of a cold query.
- **Manifest at 100M.** Every cold query reads the full 3.4 MB JSON manifest (about 50 ms). Splitting it is the next cold-start reduction.
- **Existing 100M data.** The 100M namespace was not reclustered again; it carries the paged copy added by `repage`.


## Iteration 18 (100M): warm ANN under 150 ms, and a small cold manifest read

Target: at 100M, 64 probes, default settings, warm p50 of 150 ms or less (it was 160 to 168 ms) without breaking the 10M memory target, and a small cold
manifest read that older namespaces and older binaries still work with. Same machine (c7gd.2xlarge, in the bucket's region), namespace
(`wikipedia-100m`), workload, and `ab.py` as iteration 17. In the tables below, `"main"` is iteration 17 code and `"branch"` is iteration 18 (now merged), measured in the same session.

### Where the warm time went

Warm queries cost 322 ms of CPU each but only 157 ms of wall time, so about two of the eight cores were busy. A per-task trace of one query (`BF_TRACE_STAGES`
now also prints a `TASK` line per slab) showed why. The scan stage launched one per-slab task per probed slab (about 70 of the 160 slabs at 64 probes) from one polled future (`buffer_unordered`). On a warm query
every read is a block-cache hit that finishes without yielding, so each task's copies, checksums, and decodes ran one after another on a single thread
(task start times spread from 38 ms to 94 ms after the query began), and only the final scan reached other threads.

| Stage (median, warm, 64 probes) | main | branch |
|---|---:|---:|
| Directory pages, rank, and plan | 4 ms | 4 ms |
| Scan (read, decode, and score 790,000 rows in 70 slab tasks) | 109 ms | 42 ms |
| Rerank | 21 ms | 23 ms |
| Whole query (trace on) | 154 ms | 89 ms |

### What changed

- **Slab scan tasks run on the runtime.** Each task is spawned (inside the query's read context, and cancelled together on the first error), so the
  per-slab reads and decodes use every core. Results are unchanged (same recall).
- **Adaptive allocator decay.** jemalloc's page decay is 1 s while a write, a segment build or compaction, or a `recluster` is in flight and for 15 s after,
  and 30 s while only queries run (`BF_BUSY_DECAY_MS`, `BF_IDLE_DECAY_MS`, `BF_DECAY_LINGER_MS`). The 1 s setting that keeps ingest inside a 4 GB machine is
  therefore unchanged during loads. On its own it did not move the warm p50 in this session (table below): the earlier 13 ms gain did not reproduce once
  the scan was the bottleneck, so it stays as a memory-neutral default rather than a speedup.
- **Rayon pool.** The default pool is already one thread per available CPU (rayon honors cgroup quotas). The benchmark scripts had been forcing 6; they no
  longer do. Eight threads against six made no difference here either.
- **Cheaper `PlaneQuery` construction.** The residual is quantized to bytes in one pass and transposed into bit planes 64 values at a time, with no
  intermediate vector. This showed as 8 percent of CPU in the profile (the scan itself is 26 percent).
- **Compact manifest copy for cold ANN queries.** At 100M the manifest is 9.8 MB of JSON (3.4 MB compressed), and 6.6 MB of that is the per-segment text
  headers (the dictionary chunk index of each full-text segment), which only BM25 and full-text filters read. The fixed-key copy (`manifest-latest`) is now
  written without them (`root: true`) when the manifest is larger than 256 KB and the compact form is under half the size: 375 KB at 100M. A cold ANN query
  reads that copy; a text query, a writer, and the indexer read the full manifest slot (a state that holds the compact copy replaces it before it
  builds anything, so a publish never drops a header, and a test checks that). Manifest slots are unchanged, so older binaries read these namespaces
  as before (a segment without a header reads its own footer). Older namespaces keep their full copy until the next publication; `examples/readercopy.rs`
  rewrites it (it took one PUT on the 100M namespace).

### Warm, 100M (1,000 queries, concurrency 1; recall over the 200 oracle queries)

| Arm | p50 / p90 ms | p99 ms | Recall@10 |
|---|---|---:|---:|
| 64 probes, main | 163.0 / 195.3 | 254.4 | 0.9540 |
| 64 probes, branch | **99.7 / 162.8** | 243.5 | 0.9540 |
| 48 probes, main | 119.6 / 138.0 | 156.5 | 0.9410 |
| 48 probes, branch | **72.6 / 90.4** | 119.4 | 0.9410 |

Before the scan change, with the allocator and thread settings alone (64 probes, a different part of the same session):

| Arm | p50 / p90 ms |
|---|---|
| main, 6 rayon threads | 158.7 / 190.1 |
| main, default threads | 162.0 / 193.6 |
| branch (adaptive decay), default threads | 161.4 / 193.0 |
| branch (adaptive decay), 6 threads | 163.0 / 197.4 |

### Cold, 100M (50 queries per arm; "fresh" is a new process per query, "cache-cold" drops caches on a warm process)

With the full manifest copy (the namespace as iteration 17 left it; the branch has the scan change only):

| Arm | p50 / p90 ms | p99 ms | MB | Waves | Recall |
|---|---|---:|---:|---:|---:|
| 64 probes, main, fresh | 1,345 / 1,529 | 1,674 | 163.0 | 4.0 | 0.958 |
| 64 probes, branch, fresh | 1,352 / 1,652 | 2,097 | 163.0 | 4.2 | 0.958 |
| 64 probes, main, cache-cold | 1,250 / 1,430 | 1,619 | 163.0 | 4.3 | 0.958 |
| 64 probes, branch, cache-cold | 1,260 / 1,392 | 1,591 | 163.0 | 4.5 | 0.958 |
| 48 probes, main, fresh | 1,085 / 1,319 | 1,466 | 126.0 | 4.1 | 0.950 |
| 48 probes, branch, fresh | 1,036 / 1,364 | 1,695 | 126.0 | 4.1 | 0.950 |
| 48 probes, main, cache-cold | 1,080 / 1,227 | 1,429 | 126.0 | 4.4 | 0.950 |
| 48 probes, branch, cache-cold | 1,069 / 1,243 | 1,673 | 126.0 | 4.5 | 0.950 |

With the compact reader copy (the 100M namespace rewritten; "full manifest" is the branch with `BF_READER_COPY=0`, which probes `manifest-latest` and then reads the 3.4 MB full slot):

| Arm | p50 / p90 ms | p99 ms | MB | Waves | Recall |
|---|---|---:|---:|---:|---:|
| 64 probes, compact copy, fresh | 1,243 / 1,370 | 1,540 | 159.9 | 4.5 | 0.958 |
| 64 probes, full manifest, fresh | 1,230 / 1,513 | 3,767 | 163.3 | 3.9 | 0.958 |
| 64 probes, compact copy, cache-cold | 1,201 / 1,391 | 1,500 | 159.9 | 4.3 | 0.958 |
| 64 probes, full manifest, cache-cold | 1,219 / 1,511 | 1,601 | 163.3 | 4.4 | 0.958 |
| 48 probes, compact copy, fresh | 1,058 / 1,253 | 1,972 | 122.9 | 4.3 | 0.950 |
| 48 probes, full manifest, fresh | 1,059 / 1,337 | 5,902 | 126.3 | 3.9 | 0.950 |
| 48 probes, compact copy, cache-cold | 1,128 / 1,341 | 1,584 | 122.9 | 4.7 | 0.950 |
| 48 probes, full manifest, cache-cold | 1,075 / 1,347 | 1,629 | 126.3 | 4.1 | 0.950 |

Reading it: the manifest read is 375 KB instead of 3.4 MB (about 3.0 MB smaller; total query bytes dropped by 3.4 MB because `BF_READER_COPY=0` still probes the 375 KB `manifest-latest` copy before fetching the 3.4 MB full slot) and the compact copy's p90 is 140 ms lower at 64 probes fresh, but
the p50 does not move beyond the run-to-run spread (S3 first-byte latency, not the transfer, dominates a manifest that small). The cold targets are
not regressed. This session's cold numbers are 40 to 100 ms above iteration 17's (1,164 and 1,228 ms) for the same code, which is S3 variance between
sessions; only the same-session pairs are comparable.

### 10M no-regression check (`scale/ns/wikipedia-10m`, read-only, 48 probes, which has no compact copy)

| Arm | p50 / p90 ms | p99 ms | MB | Recall@10 |
|---|---|---:|---:|---:|
| main, fresh | 703 / 890 | 1,322 | 49.6 | 0.944 |
| branch, fresh | 680 / 907 | 1,173 | 50.2 | 0.944 |
| main, cache-cold | 539 / 686 | 828 | 50.4 | 0.944 |
| branch, cache-cold | 533 / 656 | 755 | 50.3 | 0.944 |
| main, warm | 42.2 / 54.7 | 70.9 | | 0.953 |
| branch, warm | 36.6 / 50.8 | 70.5 | | 0.953 |

### Known gaps

- The `ab.py` fresh-process runs sometimes failed before printing a table (a client timeout or a reset connection): 5 of 10 runs of 50 queries, at
  both 100M and 10M, and each succeeded on retry. Twenty-five fresh-process queries run directly against the branch all succeeded with the server alive, so
  I did not reproduce a server fault and did not find the cause (the harness restarts a server on the same port between arms). The cache-cold and warm runs
  never failed.
- A cold text query on a namespace with a compact copy reads the manifest slot after the listing, one round trip more than before (not measured; the BM25
  namespaces here are small and have no compact copy).
- Writes are not measured against the adaptive decay beyond the memory check below.

### Memory check (10M load and recluster, 4 GiB cgroup, swap off, 2 CPUs)

The server ran in a systemd cgroup with `MemoryMax=4G`, `MemorySwapMax=0`, and two CPUs (`AllowedCPUs=0-1`) on the c7gd.2xlarge, with the loader in a
separate 2 GiB cgroup on two other CPUs. Defaults throughout (memory sizing follows the cgroup limit), fresh namespace under `mem/` (deleted afterward).
RSS is sampled every 5 s from `/proc`; the cgroup figure includes page cache, which the kernel reclaims, so it sits at the limit without anything being killed.

| Phase | Branch | main (same setup) |
|---|---|---|
| Ingest 10M rows (f16, 1,024 dimensions), seconds | 2,839 (3,522 docs/s) | 2,804 (3,566 docs/s) |
| Ingest peak RSS / VmHWM | 2,931 / 3,016 MB | 2,961 / 3,035 MB |
| Indexing to idle after the last write | 270 s | 270 s |
| Recluster (32 slabs, 49,985 clusters), seconds | 2,972 | not run |
| Recluster peak RSS / VmHWM | 1,538 / 2,357 MB | not run |
| OOM kills | 0 | 0 |

Both are under the 3.5 GB target and equal within noise, so the adaptive decay costs no memory: at the peak jemalloc's resident bytes were within 6 percent of
its live bytes (2,980 against 2,811 MB). The ingest peak here (3.0 GB) is higher than the 2.3 GB measured on a c7gd.large in iteration 16, for both binaries; I did not find
out why (the cgroup setup, the loader's pace, and the host are different). The recluster peak is below the ingest peak.

## turbopuffer cold samples, measured by me (10M and 1M)

turbopuffer can't be forced cold, so `bench/scale/tpuf_cold.py` waits between queries and records the
`cache_temperature` and `server_total_ms` turbopuffer reports (server-side, so the network to its region is excluded).
Namespaces `blowfish-oracle-wiki10m` and `blowfish-oracle-wiki1m` (the same 1,024-dim Wikipedia rows and oracle queries
as blowfish's 10M and 1M runs), 2026-11-19.

| Sample | 10M `server_total_ms` | 1M `server_total_ms` |
| --- | ---: | ---: |
| First query after about 2 days idle (one sample each) | 2,051 | 1,374 |
| Queries after 5 minutes idle, all reported `cold` (22 each), p50 / p90 | 670 / 1,120 | 380 / 699 |
| A different query right after each of those, also reported `cold`, p50 / p90 | 748 / 849 (first 18) | 363 / 507 (first 18) |
| The same namespace once its data is cached (reported `hot`) | 13 to 24 | 14 to 30 |

Every query that touched data turbopuffer hadn't cached was reported `cold`, even right after other queries, so its
"cold" label is per query, not per namespace. Its published 10M cold p50 (874 ms) matches the 5-minute-idle samples; the
2-day-idle sample is about 2.3 times that.

## turbopuffer at 100M, measured by me

turbopuffer publishes no 100M number, so I loaded the same 100M rows into it and measured it myself. These are my
measurements from one VM, over the internet, with turbopuffer's reported `server_total_ms` as the
in-region equivalent. They are not turbopuffer's published figures.

### Load

Namespace `blowfish-oracle-wiki100m`, loaded with `scale_load.py --vector-type f16` in the same order as
the blowfish 100M load (English, then de, fr, es, ru), so ids match `oracle-100m.npz`. 8 writes in flight, batches of 2,000.

| | |
|---|---:|
| Rows | 100,000,000 |
| Load time | 53,000 s (14.7 h), 1,887 docs/s on average (2,100 to 2,400 early on) |
| Billable bytes written (`billable_logical_bytes_written`) | 450.4 GB (4.5 GB per 1M rows) |
| Raw bytes sent | 451.5 GB |
| Namespace size reported by turbopuffer (`approx_logical_bytes`) | 246.8 GB |
| 429 retries | 4,846 (and 64 transport retries; one 502 stopped the loader, and I added a 5xx retry and resumed from the checkpoint) |
| Time from the last write to "up-to-date" | about 2 minutes |

Cost: at the list rate of $2/GB the billable bytes come to about $901. With turbopuffer's batch discount the bill is likely
lower, about $450 to $901; I have not seen the invoice.

### First query after indexing

Three ANN queries sent right after the index reported `up-to-date` (the first is not a deep-cold sample: the namespace had
just been written and indexed).

| Query | Client ms | `server_total_ms` | Cache temperature | Cache hit ratio | Recall@10 |
|---|---:|---:|---|---:|---:|
| 1 | 2,085 | 1,966 | cold | 0.15 | 1.0 |
| 2 | 1,433 | 1,327 | cold | 0.60 | 0.9 |
| 3 | 730 | 624 | cold | 0.66 | 1.0 |

### Idle-gap samples

`tpuf_cold.py` sends one ANN query after each idle gap, doubling the gap until turbopuffer reports "cold". It never did for
the probe query at 100M: I stopped it after the 19,200 s gap, so these are idle-gap samples, not cold p50 and p90. The last row
is one separate sample after the namespace sat untouched for a day, and it still came back warm.

| Idle gap | `server_total_ms` | Client ms | Cache temperature | Recall@10 |
|---:|---:|---:|---|---:|
| 0 s (warm-up) | 14 | 132 | hot | 1.0 |
| 300 s | 16 | 334 | hot | 0.9 |
| 600 s | 44 | 373 | hot | 1.0 |
| 1,200 s | 131 | 441 | warm | 1.0 |
| 2,400 s | 145 | 435 | warm | 0.7 |
| 4,800 s | 170 | 459 | warm | 1.0 |
| 9,600 s | 202 | 482 | hot | 1.0 |
| 19,200 s | 503 | 782 | warm | 0.9 |
| 1 day | 201 | 454 | warm | 1.0 |

The follow-up query after each probe (a different query, no idle) came back cold twice (after the 300 s and 600 s gaps:
842 and 542 ms server time) because its clusters had not been read yet, so "cold" depends on the query as well as the idle time.
The probe never reported cold, and server time grew with the idle gap (16 ms at 300 s, 503 ms at 19,200 s).

### Recall and warm latency

`oracle_compare.py --tpuf-only` with `oracle-100m.npz`, 200 English queries per type, one pass, one request at a time (ms p50 / p90 / p99):

| Query | Recall@10 | Client | `server_total_ms` | Cache temperature |
|---|---:|---|---|---|
| ANN | 0.947 | 310 / 525 / 636 | 232 / 426 / 535 | 14 hot, 186 warm |
| BM25 | | 215 / 430 / 805 | 159 / 373 / 750 | 11 cold, 12 hot, 177 warm |
| Hybrid | | 90 / 119 / 157 | 32 / 63 / 80 | 200 hot |
| Selective ANN | 0.995 | 445 / 682 / 1,218 | 358 / 590 / 1,110 | 4 hot, 196 warm |
| Broad ANN | 0.962 | 148 / 311 / 507 | 88 / 256 / 449 | 59 hot, 141 warm |

Then 1,000 ANN queries at concurrency 1 (`tpuf_warm.py`, after one pass over the 200 queries; all 1,000 reported hot):

| | p50 | p90 | p99 |
|---|---:|---:|---:|
| Client ms | 70.2 | 75.6 | 115.6 |
| `server_total_ms` | 14 | 19 | 53.1 |

Recall@10 over those 1,000 was 0.9465.

### Beside blowfish at 100M

Blowfish numbers are the current ones, from the iteration 18 section above (c7gd.2xlarge in the bucket's region; 48 probes, 64 probes in brackets).
turbopuffer's are the measurements on this page, with server time as the in-region equivalent. Neither is a cold p50 and p90 for turbopuffer.

| | blowfish | turbopuffer |
|---|---|---|
| ANN recall@10 | 0.941 (0.954) | 0.947 (first pass), 0.9465 (warm) |
| Warm ANN p50 / p90 ms | 72.6 / 90.4 (99.7 / 162.8) | 14 / 19 server, 70 / 76 client (over the internet) |
| First pass over 200 queries, ANN p50 / p90 ms | not measured | 232 / 426 server, 310 / 525 client |
| After the load, first query | not measured | 1,966 ms server (cold) |
| Idle 19,200 s | not measured | 503 ms server (warm) |
| Idle 1 day | not measured | 201 ms server (warm) |
| Cold, cache-cold p50 / p90 ms | 1,069 / 1,243 (1,260 / 1,392) | not measured (the namespace did not go cold in the idle-gap run) |

At 10M turbopuffer's first-pass ANN had server time 88 / 203 ms; at 100M it is 232 / 426 ms, about 2.6 and 2.1 times.

## Iteration 19: BEIR Natural Questions on one machine

An outside benchmark, "Blowfish on BEIR Natural Questions" by Kerry Kurian (2026-11-20), measured blowfish 0.1.13 on
one machine against FAISS, Qdrant, and LanceDB. It found three problems. First, throughput flattened at about 50 to
65 q/s on 8 CPUs, even though one query took about 12 ms. Second, warm p99 was several hundred ms. Third, recall@10 as
loaded was 0.899 at the default 64 probes. I reproduced its numbers here, fixed the causes, and measured Qdrant and
LanceDB on the same machine, then on a Mac.

**Workload.** The BEIR NQ test split has 2,681,468 passages, 3,452 queries, and 4,201 qrels. The vectors are
precomputed Cohere embed-english-v3 embeddings (1,024 dimensions), stored as f16, with cosine distance. Storage is a
local directory (`file://`), with no S3. Each document carries id, vector, title, and text (BM25 on text). The harness
is in `bench/nq/`, and its README shows how to reproduce every number below.

- **Recall@10** is measured against the exact top 10 (brute force over the f16 vectors). F1@10, nDCG@10, and
  recall@100 are measured against the qrels, using top_k 100 requests.
- **Warm latency** is client-timed over keep-alive HTTP: one warm-up pass, then 1,000 queries at concurrency 1.
- **Throughput** uses 2,000 queries with pre-encoded bodies and one process with one connection per client.

**Machine.** This VM has 8 vCPUs and 31 GB of RAM. The 8 vCPUs share 4 L2 caches, so they are probably 4 physical
cores with SMT. The server runs pinned to CPUs 0 to 5 (about 3 cores), in a 12 GB cgroup with swap off. Clients are
pinned to CPUs 6 and 7. Kerry's run had 8 CPUs, so its absolute numbers are higher than my baseline. Every engine below
had the same limits. The block cache is 3 GiB, a quarter of 12 GB as in Kerry's run, and the disk cache is off.

### Before: blowfish 0.1.13 (main at 80bed53) on this VM

Quality matched Kerry's within 0.01. Latency and throughput had the same shape: a flat throughput curve and a p99 of
several hundred ms.

| Config | Recall@10 (Kerry) | p50 / p95 / p99 ms (Kerry) | q/s at 1 / 4 / 16 (Kerry) |
|---|---:|---|---|
| As loaded, 64 probes (default) | 0.907 (0.899) | 23.5 / 250 / 620 | 24.6 / 26.4 / 27.7 |
| As loaded, 160 probes | 0.958 (0.953) | 40.2 / 65.9 / 420 (14.7 / 113 / 358) | 13.0 / 21.8 / 18.2 (46 / 68 / 64) |
| Reclustered, 48 probes | 0.941 | 24.1 / 42.9 / 485 | 18.1 / 20.2 / 23.3 |
| Reclustered, 64 probes | 0.954 (0.954) | 28.7 / 457 / 732 (11.6 / 31.5 / 378) | 19.8 / 23.8 / 27.1 (31 / 37 / 49) |
| Reclustered, 64 probes, 8 GiB cache | | 25.1 / 77.0 / 558 (10.9 / - / 235) | 19.9 / 23.8 / 31.0 (45 / 63 / 63) |
| Reclustered, 64 probes, unpinned | | 28.7 / 156 / 647 | 17.1 / 23.5 / 21.3 |

### Where the time went, and what changed

Each change below is its own commit, with the numbers in its message (reclustered, 64 probes, measured at the time).

1. **The tail came from the recall sampler (8020d46).** The default 1% sampler ran an exhaustive search over 5.5 GB of
   vectors about every 100 queries. It ran on every core and through the block cache, which evicted the blocks that warm
   queries reuse. Now one comparison runs at a time, at most once a minute (`BF_RECALL_SAMPLE_INTERVAL_MS`), one segment
   at a time, and without filling the cache. Warm p50/p95/p99 went from 28.7/457/732 to 24.2/37.3/47.1 ms, and q/s at
   16 clients from 27 to 73.
2. **Rerank vectors were read through 1 MiB cache blocks (07a5c8d).** That was about 15 MB per query, which churned the
   cache. They now go through 16 KiB blocks (`BF_SPARSE_BLOCK_BYTES`). p99 went from 47 to 40 ms.
3. **AVX-512 for the RaBitQ scan and query planes (51f19db, 68982e0), and popcount ranking of the directory
   (50d3ef5).** The scan uses `VPOPCNTQ`, and the bit planes use `VPTESTMB`, bit-identical to the scalar path
   (`BF_NO_AVX512` turns this off). Directory ranking fell from 11% to 1% of CPU. On arm64, the same scan uses NEON
   `CNT` (786e6df), also bit-identical and tested on an M4. The kernel went from 58 to 19 ns per code there.
4. **A lock that every warm read took (b91345e).** A read batch planned its fetches under a store-wide lock, even when
   the cache answered all of it. At 16 clients, a warm cluster read waited 21 ms. Fully cached batches now skip the
   lock. q/s at 16 clients went from 97 to 164.
5. **Per-query work that warm queries repeated:**
   - Decoded directory clusters are now cached across queries, and each code block's CRC is checked once (e543658).
     Server CPU per query went from 28 to 21 ms.
   - Stub clusters no longer allocate (87444e7, 54775de).
   - The vector rank key is the query's f32 bits, not its JSON (fada966).
   - Decoded clusters live in a sharded CLOCK cache instead of moka (37b1820).
   - Results hydrate from cached decoded column blocks (b1bca3c). More importantly, they hydrate only the projected
     attributes (b9bd2bc): `include_attributes: ["id"]` had parsed every title and text.
6. **Fan-out under load (0c80a65).** One query spreads its slab tasks over the thread pool. Under load, that
   oversubscribed the CPUs. Once more queries are in flight than half the CPUs (`BF_FANOUT_QUERIES`), each query runs
   its slab work on its own thread. A single query, such as a warm 100M query, still fans out.
7. **The scan stops early when it can (ad28a98).** It keeps the k smallest upper bounds and drops rows that the
   error-bounded rerank would drop anyway. Results are the same (tested for k from 1 to 50). q/s at 16 clients went
   from about 380 to 435.
8. **As-loaded recall at default settings (c8a9415, 034284f).** The probe count scales with the square root of the
   rows, which fits one clustering of all rows, as `recluster`'s slabs have. A namespace as loaded has one independent
   clustering per segment (25 here), so the same probe count finds fewer neighbours. When the probe count is left at its
   default, an unreclustered layout now probes `min(S^0.3, 4)` times as many clusters. S is the effective number of
   equal segments: (sum of rows)² / sum of rows². NQ gets 2.6 times the probes. If you set a probe count, it is used as
   given. Reclustered layouts, including the 10M and 100M namespaces measured earlier, are unchanged. The cost is that
   rows scanned and cold bytes grow by the same factor. For a 10M namespace as loaded with 70 segments, I estimate
   3.5 times the clusters: about 710,000 rows and 91 MB of codes per cold query, against 200,000 rows and 26 MB. That
   is an estimate from the code, not a measurement. At 100 segments or more, the multiplier is capped at 4 times.
   Reclustering removes the cost.

Dropped: reciprocal divisions in the scan (no gain), and concurrency limits and fan-out thresholds (no effect). I
also tried 4-bit query quantization instead of 8-bit. It cut CPU by about 8% with recall@10 unchanged, but recall@100
went from 0.9282 to 0.9275, so I left it out.

The final profile at 16 clients: the RaBitQ scan, its callback, and dot products are about 42% of server CPU.
Building the query planes is 5%, and nothing else is over 2%. In isolation, the scan costs 18.7 ns per row. That is
about the hardware cost of a 1,024-bit code with 8-bit query planes, so further gains have to come from scanning fewer
rows.

### After: final code, fresh load, same VM

I deleted the store, loaded again with the final binary, and measured each configuration. The load took 287 s (9,346
rows/s, against 7,665 before), with 3.4 GB peak RSS. `recluster` took 162 s (32 slabs, 13,391 clusters).

| Config | Recall@10 | F1@10 | nDCG@10 | Recall@100 | p50 / p95 / p99 ms | q/s at 1 / 4 / 16 |
|---|---:|---:|---:|---:|---|---|
| As loaded, default (64 probes, scaled 2.6 times) | **0.960** | 0.168 | 0.604 | 0.929 | 9.5 / 16.0 / 27.5 | 117.9 / 195.4 / 258.6 |
| As loaded, 64 probes set explicitly | 0.907 | 0.161 | 0.582 | 0.887 | 5.3 / 8.2 / 9.9 | 144.9 / 302.5 / 484.6 |
| As loaded, 160 probes | 0.961 | 0.168 | 0.604 | 0.930 | 9.1 / 15.6 / 27.8 | 115.6 / 203.8 / 274.5 |
| Reclustered, 48 probes | 0.944 | 0.167 | 0.603 | 0.922 | 5.6 / 8.6 / 10.7 | 150.3 / 326.1 / 480.0 |
| Reclustered, 64 probes (default) | **0.955** | 0.168 | 0.606 | 0.929 | **8.3 / 13.3 / 18.1** | 111.3 / 311.4 / **444.2** |
| Reclustered, 64 probes, 8 GiB cache | 0.955 | | | | 8.0 / 12.1 / 14.5 | 121.1 / 272.0 / 388.5 |
| Reclustered, 64 probes, unpinned | 0.955 | | | | 7.5 / 12.8 / 15.0 | 110.0 / 297.7 / 510.2 |

The 16-client rate varies by about 10 to 15% from run to run on this VM, so I compared changes in interleaved A/B
runs, using server CPU per query as the steadier metric. Against the baseline:

- reclustered at 64 probes is 16 times the throughput at 16 clients (27.1 to 444.2 q/s);
- p99 fell from 732 to 18.1 ms, and p50 from 28.7 to 8.3 ms;
- as loaded at default settings, recall@10 went from 0.907 to 0.960, with no probe choice by the user.

### Qdrant and LanceDB on the same VM

These ran with the same limits: CPUs 0 to 5 and 12 GB, the same clients, and the same queries.

- **Qdrant 1.20** ran over HTTP in Docker, with HNSW m=16, ef_construct=100, f16 vectors in RAM, and one segment.
- **LanceDB 0.41** ran in-process, as in Kerry's run, with IVF-PQ (1,600 partitions, 128 sub-vectors) and refine 10.
  Its client threads share its six CPUs, and it has no HTTP hop.

Both widen their search with the limit, so I measured their recall@10 at limit 10. The latency and q/s rows use
top 10, and so do blowfish's.

| Engine, setting | Recall@10 | p50 / p95 / p99 ms | q/s at 1 / 4 / 16 |
|---|---:|---|---|
| blowfish, reclustered, 64 probes | 0.955 | 8.3 / 13.3 / 18.1 | 111 / 311 / 444 |
| Qdrant, hnsw_ef 64 | 0.951 | 1.3 / 2.7 / 3.9 | 688 / 1,812 / 2,565 to 3,164 |
| Qdrant, hnsw_ef 128 | 0.969 | 2.1 / 3.6 / 4.7 | 476 / 1,233 / 2,014 to 2,167 |
| LanceDB, 120 probes | 0.956 | 48.4 / 62.9 / 68.8 | 19.4 / 68.1 / 77.9 |
| LanceDB, 160 probes | 0.967 | 62.9 / 79.5 / 85.5 | 15.6 / 48.9 / 54.2 |

At recall@10 of about 0.95, blowfish serves about 5.7 times LanceDB's throughput at 16 clients, and about one sixth of
Qdrant's. Qdrant keeps an HNSW graph and every vector in RAM, which costs about 6 GB here. blowfish reads RaBitQ codes
and exact vectors through a block cache over object storage, so the same server also works cold from S3.

### On a Mac: blowfish, Qdrant, and LanceDB (Apple M4)

I reran all three engines on a Mac mini: an Apple M4 (10 cores) with 16 GB of RAM, running macOS 26. I ran one engine
at a time, deleted its data before starting the next, and loaded vectors only (id and vector) for all three.

- **blowfish:** a native release build of the final branch, without `local-models`, on `file://` storage. The block
  cache was 4 GiB, and the RaBitQ scan used the NEON path.
- **Qdrant 1.20.0:** the official macOS arm64 binary, with the same collection settings as on the VM.
- **LanceDB 0.41:** via `uv`, in-process, with the same index.

macOS cannot pin CPUs, so the servers and the client processes shared all 10 cores. The rates below therefore include
the clients' own CPU. Peak RSS is from `/usr/bin/time -l`. Recall@10 is measured at limit 10 for Qdrant and LanceDB, as
on the VM.

The as-loaded layout here had 97 segments (main's had 102), against 25 on the VM. I did not look into why; the
vectors-only load and the Mac's memory-based segment sizing are the likely differences. At default settings, that
many segments gets the capped 4 times the probes.

**blowfish, before (main) and after (this branch):**

| Config | Recall@10 | p50 / p95 / p99 ms | q/s at 1 / 4 / 16 | Peak RSS |
|---|---:|---|---|---:|
| main, as loaded, default | 0.896 | 8.6 / 84.2 / 145.5 | 70.0 / 97.9 / 83.4 | 5.9 GB |
| main, reclustered, 64 probes | 0.953 | 14.3 / 204 / 656 | 24.8 / 29.5 / 38.1 | 5.9 GB |
| main, reclustered, 48 probes | 0.940 | 10.3 / 123 / 565 | 28.5 / 25.1 / 34.3 | 6.0 GB |
| branch, as loaded, default (4 times the probes) | **0.968** | 6.9 / 7.8 / 8.8 | 142.9 / 183.2 / 246.3 | 5.3 GB |
| branch, reclustered, 64 probes | **0.952** | **2.8 / 3.7 / 4.1** | 327.1 / 748.3 / **869.7** | 5.4 GB |
| branch, reclustered, 48 probes | 0.940 | 2.5 / 3.3 / 3.6 | 375.4 / 879.7 / 1,097.0 | 5.4 GB |

Load and recluster:
- main: load 129 s (1.9 GB peak RSS); `recluster` 360 s (0.9 GB).
- branch: load 139 s (1.9 GB); `recluster` 334 s (0.7 GB).

main needed `ulimit -n 10240` to run at all; see below.

**All three engines on the Mac:**

| Engine, setting | Recall@10 | p50 / p95 / p99 ms | q/s at 1 / 4 / 16 | Peak RSS | Build |
|---|---:|---|---|---:|---|
| blowfish, reclustered, 64 probes | 0.952 | 2.8 / 3.7 / 4.1 | 327 / 748 / 870 | 5.4 GB (4 GiB cache) | load 139 s, `recluster` 334 s |
| blowfish, reclustered, 48 probes | 0.940 | 2.5 / 3.3 / 3.6 | 375 / 880 / 1,097 | 5.4 GB | |
| Qdrant, hnsw_ef 64 | 0.952 | 0.4 / 0.6 / 0.7 | 2,028 / 5,556 / 7,572 | 16.1 GB (includes mapped files) | upload and index 616 s |
| Qdrant, hnsw_ef 128 | 0.970 | 0.7 / 0.9 / 1.0 | 1,396 / 3,561 / 5,517 | | |
| LanceDB, 80 probes | 0.935 | 12.9 / 13.2 / 13.4 | 78 / 226 / 394 | build 9.9 GB, queries 0.6 GB | write 19 s, index 55 s |
| LanceDB, 120 probes | 0.955 | 18.4 / 18.9 / 19.1 | 55 / 160 / 281 | | |

At recall@10 of about 0.95, blowfish on the M4 serves about 3.1 times LanceDB's throughput at 16 clients (870 against
281 q/s) and about 11% of Qdrant's (7,572). On the VM the LanceDB gap was 5.7 times, but on the Mac LanceDB's clients
are in-process and no core is reserved for blowfish's HTTP clients, so the Mac numbers favor it. Qdrant's lead comes
from HNSW over vectors held in memory: a query computes a few thousand distances, where blowfish's IVF scan reads
about 118,000 one-bit codes. The cost is memory: Qdrant's footprint was the whole Mac,
including its mapped storage. blowfish's 5.4 GB is its 4 GiB block cache plus the decoded caches.

**macOS open-file limit.** The first Mac run failed every query on the as-loaded layout with a 503 "segment is
unavailable or corrupt". macOS starts processes with a soft limit of 256 open files, and a query on a namespace with
about a hundred segments on `file://` storage opened more than that at once. blowfish now raises its soft limit to
the hard limit at startup (824c5e7): 256 to 61,440 on this Mac. Linux distributions usually start at 1,024, which
this load did not reach.

### Not done and what is left

- The scan is now most of the CPU and runs near its per-row cost. The next step is fewer rows per query: a better
  first-stage estimate (for example, multi-bit RaBitQ codes) or a smaller rerank target.
- As-loaded recall comes from probing more clusters. Recluster automatically when it pays off is still an open
  decision in `SPEC.md`.
- The decoded caches could hold up to 1.5 times `BF_DECODED_CACHE_BYTES`. [Iteration 20](#iteration-20-fewer-cycles-per-row-and-better-placed-rows-beir-nq-one-machine)
  fixed that.

## Iteration 20: fewer cycles per row and better-placed rows (BEIR NQ, one machine)

Same workload, VM, and limits as [Iteration 19](#iteration-19-beir-natural-questions-on-one-machine): 2.68M f16
vectors at 1,024 dimensions, the server pinned to 6 vCPUs in a 12 GB cgroup, a 3 GiB block cache, and clients on the
other 2 vCPUs. The goal was more q/s at 16 clients at recall@10 of at least 0.95, both reclustered and as loaded.

### Results

Each layout was built by its own binary: main's load and `recluster`, and a fresh load and `recluster` with the branch.
The quality and latency columns come from one run each. The q/s column is the mean of two interleaved rounds of all five
configurations (16 clients, after two warm-up passes).

| Config | Recall@10 | Recall@100 (qrels) | p50 / p95 / p99 ms | q/s at 16 (rounds) |
|---|---:|---:|---|---|
| main, reclustered, 64 probes | 0.955 | 0.928 | 9.8 / 14.8 / 17.4 | 395 (411, 379) |
| branch, reclustered, 54 probes | 0.954 | 0.927 | 4.8 / 7.7 / 11.7 | **573** (589, 557) |
| branch, reclustered, 64 probes (default) | **0.960** | 0.933 | 4.9 / 7.3 / 9.6 | 538 (572, 505) |
| main, as loaded, default | 0.958 | 0.924 | 9.6 / 16.1 / 27.3 | 269 (260, 278) |
| branch, as loaded, default | 0.958 | 0.925 | 7.2 / 13.0 / 28.9 | **387** (371, 402) |

At the same recall, reclustered throughput rose by 45% and as-loaded throughput by 44%. At the default 64 probes, the
branch's reclustered layout has recall@10 of 0.960, up from 0.955, and still serves 36% more q/s. This is short of the
1,000 q/s stretch target.

### What changed

- **Fast-scan code layout.** A new index stores each cluster's one-bit RaBitQ codes transposed in blocks of 32 rows.
  Next to them it keeps four `f32` factors per row, so the estimate needs no division or square root: `|r|^2`,
  `|r| / alpha`, the error scale, and `<b, c - g> / sqrt(D)`.
  - A query builds one table per index: 16 entries for each group of 4 dimensions. The table holds the query's offset
    from the index's mean center `g`, quantized to 7 bits. A byte shuffle then looks up 4 dimensions of 64 rows at once
    (`vpshufb` on AVX-512BW, `TBL` on NEON, and a scalar loop elsewhere, all with the same integer sums).
  - Per row, that is about 3 times faster than the bit-plane popcount scan on this VM: 5 against 24 ns for the kernel
    with its bookkeeping. On an Apple M4 it is 5 times faster (5.4 against 26.5 ns).
- **Table rounding is in the error bound.** The 7-bit tables round once per group, and the resulting error grows with
  the query's distance from `g`, not from the row's cluster center. On NQ it is about 1% of the RaBitQ error. On tight
  clusters (a query that nearly duplicates a row), it reordered the near-ties and cost a cold query an extra read wave.
  The bound now includes it. Clusters where it is more than a tenth of the RaBitQ term rescore their candidates with a
  second table of the rounding residuals, which has about 100 times less error than the coarse table.
- **The cut is applied inside the scan.** A block reports a bitmask of the rows whose lower bound beats the current
  k-th upper bound, so most rows never reach the per-row bookkeeping (+7%).
- **Smaller costs.** These changes removed:
  - Copies from the block cache (a reader backed by it now gets ranges unmerged).
  - A second query rotation per segment (the directory search rotates once).
  - Rebuilding the RaBitQ sign vectors for every partial index (they are cached).
  - Per-value f16 conversion (now a block at a time).
  - A scalar query-to-center distance (now SIMD).
- **Cross-cell leaf assignment in `recluster`.** Each cell's leaves were clustered on that cell's rows alone. On NQ, a
  row's own cluster was its nearest center only 80% of the time, and the losses sat at cell boundaries. A coverage
  analysis showed this: the clusters a query probes held about the recall measured, and adaptive probing or exact
  center ranking did not help. Now every row joins its nearest leaf among its four nearest cells, and the leaf centers
  are recomputed once. Recall@10 at 64 probes went from 0.955 to 0.960, and 54 probes now match the old 64.
- **The decoded cache budget is honored.** `BF_DECODED_CACHE_BYTES` is now split into two thirds for metadata, a
  quarter for clusters, and a twelfth for columns. Before, the parts could total 1.5 times the setting.

### Cost and limits of the cross-cell assignment

| Layout | Per-cell `recluster` | Cross-cell `recluster` | Peak RSS (per-cell / cross-cell) |
|---|---|---|---|
| NQ, 32 cells (about 420 leaves a cell) | 158 s | 240 s | not measured |
| NQ, 8 cells (about 1,700 leaves a cell, the size of a 10M layout's cells) | 93 s | 508 s | 1.95 / 2.04 GB |

Each cell's leaves are trained with one-level k-means, and every row is compared with about 16 times the square root of
its cell's leaves. The cost therefore grows faster than the rows. The cross-cell assignment runs by default only when
the largest cell has at most 1,024 leaves (about 6.5M rows at the default 32 cells). The published 10M and 100M layouts
keep the per-cell assignment and the same build time. `BF_RECLUSTER_GLOBAL=1` forces the cross-cell assignment, and `0`
turns it off.

Forcing it at 10M would take about 5 times the per-cell job, judging by the 8-cell run. At 100M, with about 15,600 leaves
a cell, the one-level training alone makes it impractical. It would also hold about 2 GB of leaf centers, plus about
1.2 GB of per-row assignment arrays. Making it scale needs two-level leaf training, as in the per-slab builder.

### Checks

- Old indexes still work: the branch reading main's reclustered store gives recall@10 of 0.9548, the same as main.
- `cargo test --workspace`, clippy, and the compat suite (121/121) pass. The `bf-vector` and `bf-engine` tests also
  pass on an Apple M4 (NEON).
- The `bench` ladder (768 dimensions, 64 probes, against main) gives the same recall at 10K, 50K, and 200K rows
  (1.0, 1.0, and 0.94). Warm ANN p50 was 1.0 / 1.7–1.8 / 5.4 ms against main's 1.1 / 1.9–2.8 / 5.9 ms, and cold
  ANN was within run-to-run spread.

### Tried and dropped

- **Finer leaves (100 rows each, 26,797 clusters).** They gave more recall per scanned row (0.955 at 38 probes), but
  center ranking doubled. At matched recall the layout was 3 to 4% slower.
- **Refining every block near the cut with the fine table,** or rescoring every surviving candidate. Both kept
  recall but cost 16 to 20% of throughput on NQ, where the rounding barely matters.
- **Adaptive, distance-threshold probing** (offline analysis). It covered no more of the true top 10 per row read than
  a fixed probe count did.

### What is left

- Center ranking and per-index table setup are now a few percent each. A finer IVF pays only with a cheaper center
  search, such as a small index over the centers.
- Training the cross-cell assignment two-level would carry its recall gain to 10M and 100M layouts.

## Iteration 21: one recluster assignment at every size, and a lighter query path (BEIR NQ, one machine)

This used the same workload, VM, and limits as [Iteration 20](#iteration-20-fewer-cycles-per-row-and-better-placed-rows-beir-nq-one-machine).
I had three goals: profile the 16-client query path and cut its top costs, make the cross-cell leaf assignment affordable
at 10M- and 100M-sized cells so it can be the only `recluster` assignment, and keep the small ladder and old indexes
unchanged. Per SPEC decision 25, I also removed every setting that only chose a slower path (see "Removed settings"
in [configuration](configuration.md)).

### Results

"Before" is main at the start of this round's final pass, which includes the reindex and format-fence work. Each
binary built its own reclustered layout (32 cells, the default 64 probes). The as-loaded rows share one store. Quality
and 1-client latency come from one run each. q/s is the mean of three interleaved rounds of two passes each (16
clients, after a warm-up pass). I turned the recall sampler off for the q/s passes, for the reason given below.

| Config | Recall@10 | Recall@100 (qrels) | p50 / p95 / p99 ms | q/s at 16 | Server CPU per query |
|---|---:|---:|---|---:|---:|
| main, reclustered | 0.961 | 0.932 | 5.3 / 7.7 / 9.6 | 580 | 7.7 ms |
| branch, reclustered | 0.961 | 0.933 | 5.3 / 8.2 / 9.7 | **616** | 7.3 ms |
| main, as loaded | 0.956 | 0.927 | 7.2 / 13.3 / 27.9 | 392 | 12.1 ms |
| branch, as loaded | 0.956 | 0.927 | **5.2 / 7.3 / 9.3** | 400 | 11.8 ms |

Reclustered throughput rose by 6% at the same recall, which is just past this VM's noise. As loaded, q/s is unchanged,
but 1-client tail latency fell by two thirds. This is far from the 1,000 q/s stretch target. See "What is left".

The recall sampler does one exhaustive, single-core scan of the namespace at most once a minute. Whether that scan
lands inside a 10-second measured pass decides that pass's q/s. With the sampler on, the same as-loaded comparison
read 388 against 360 q/s in one set of rounds. With it off, it read 392 against 400.

### Recluster cost

| Layout | main | branch | Recall@10 at 64 probes (main / branch) |
|---|---|---|---|
| NQ, 32 cells (about 420 leaves a cell) | 253 s, 0.69 GB | **138 s**, 0.69 GB | 0.961 / 0.961 |
| NQ, 8 cells (about 1,700 leaves a cell, a 10M layout's cells) | 216 s, 2.60 GB (per-cell) | 230 s, 2.66 GB | 0.950 / **0.961** |
| NQ, 8 cells at 21 rows a leaf (about 16,000 leaves a cell, a 100M layout's cells) | not run | 939 s, 3.22 GB | not measured |

I measured the 32-cell rows on the quiet VM. The 8-cell rows come from one session under load from other builds.
At 8 cells, main uses its per-cell assignment because cross-cell would have taken about 515 s. The branch's
cross-cell assignment there costs 6% more time than main's per-cell one, and it raises recall@10 from 0.950 to 0.961.

**At 100M.** The 100M-shaped run spent 81 s training leaves and about 240 s on each of its two assignment passes. Each
row was compared with about 4,080 centers. The assignment cost per row depends on the shape (cells, leaves, and
groups), not on the row count, so 100M rows would take about 480 s × 37 ≈ 5 hours of assignment on these 8 vCPUs, or
about 1.25 hours on 32. The last 100M recluster took 1.7 hours in all. The leaf centers would need about 2 GB of
memory. Dropping the refine pass halves the assignment time, at a cost of 0.003 recall@10 on NQ. A graph over leaf
centers (`docs/design/graph-index.md`) could replace the group stage.

### What changed

- **Two-level leaf training in `recluster`.** A cell with more than 1,024 leaves trains sqrt(k) parents and then the
  children under each parent, in parallel. A smaller cell still trains one-level, which gives 0.003 more recall@10 on NQ
  at 32 cells. Each cell's leaves are grouped: under their parent, or by a small k-means for a one-level cell.
- **Blocked cross-cell assignment.** A row compares the groups of its 4 nearest cells, then the leaves of its 12 nearest
  groups. Rows are counting-sorted by nearest group and processed 4 at a time over the union of their candidates, with
  a 4-row dot-product kernel (`|r|^2 + |c|^2 - 2<r, c>`). Neighbors in that order share leaves, which stay in cache.
  At 8 cells a row is compared with about 1,230 centers, and the run takes 230 s against about 515 s for the
  iteration 20 assignment.
- **Cross-cell is the only assignment.** I removed the per-cell path, `BF_RECLUSTER_GLOBAL`, `BF_RECLUSTER_GROUPS`, and
  `BF_RECLUSTER_REFINE`. The group count (12) and the single refine round are now constants.
- **f32 distance kernels.** numkong's f32 squared Euclidean took 300 to 700 ns per 1,024-dimension pair here. A 16-lane
  AVX-512 kernel (with AVX2 and portable fallbacks) takes about 65 ns. It now serves k-means, the query-to-center
  distances, and rerank. Cosine rerank computes its two norms and the dot product in one pass.
- **A query's state clone no longer copies the text stats header.** It is shared behind an `Arc`.
- **Rerank checks a whole cluster's CRC once** instead of once per row as well.
- **foldhash for the decoded-cluster cache and per-query cluster sets.** SipHash ran twice per lookup (shard and map),
  and a warm query makes about 500 lookups.

### Profile (16 clients, reclustered, sampler off)

| Area | Share of CPU |
|---|---:|
| Fast-scan kernel (`block_sums_avx512`) | 11.6% |
| Fast-scan bookkeeping (`FastQuery::scan`) | 8.5% |
| Per-index table setup (`FastQuery::new`) | 2.7% |
| RaBitQ center ranking (`signed_dot_avx512`, directory sections) | 5.4% |
| Query-to-center distances | 2.5% |
| JSON request parsing (`serde_json` through `serde_path_to_error`) | about 5% |
| `memcpy` | about 5% |
| Cache lookups and hashing (before foldhash) | about 4% |

A warm query probes about 524 clusters (64 probes, scaled by clusters over sqrt(rows)) and scans about 105,000 rows.
Before this round, the profile also showed two other costs: about 9% from cloning the text stats header, and the numkong
distances. I removed both. Scanning is now the largest cost by far, so more q/s at this recall needs fewer clusters per
query, not cheaper bookkeeping.

### Checks

- `cargo fmt`, workspace clippy with warnings denied, and `cargo test --workspace` (252 passed) all pass.
  `read_cache::cold_ann_has_three_read_stages` passed in every run.
- The compat suite passes 121/121.
- Old indexes: the branch serves main's as-loaded store with the same recall (0.956).
- The `bench` ladder (768 dimensions, 64 probes, against main) gives the same recall at 10K, 50K, and 200K rows
  (1.0, 1.0, and 0.94). Warm ANN p50 was 1.3 / 2.8 / 4.9 ms against main's 1.5 / 3.1 / 5.3 ms. Cold ANN (9.5 / 10.0 /
  8.0 ms against 9.1 / 10.0 / 13.6) was within run-to-run spread.

### Tried and dropped

- **Locality order alone, or 8-row blocks.** Sorting rows did not speed up the assignment without blocking, and 8-row
  blocks made the candidate union too large. 4-row blocks with balanced groups worked.
- **Final means without a second assignment.** This lost 0.003 recall@10, so the refine pass stays.
- **Two-level training for every cell.** At 32 cells it gave 0.9577 recall@10 against main's 0.961, and 20 group probes
  did not recover it (0.9584). Small cells train one-level.

### What is left

- Fewer clusters per query. A graph over leaf centers (`docs/design/graph-index.md`) is the next round.
- A typed parser for `rank_by` vectors would remove most of the 5% spent on JSON parsing.
- At 100M, the assignment would need a cheaper group stage or no refine pass to stay near the old build time.
## BM25 round: fewer cold reads and exact bounds (MS MARCO 1M, one machine)

I measured the first 1,000,000 MS MARCO passages locally, with the 1,000 checked-in dev queries,
strong consistency, default BM25 parameters, and exhaustive top-10 truth from
`bf-text/examples/exact_bm25.rs`. The baseline is the existing main release binary (SHA-256
`dfec618d9fddd80279ad3968460a0896c6ea7341f39b8328d4e10aec972329c3`). Both stores contain the
same documents in six segments, after the same final compaction. This run uses no cloud bucket.

I restricted servers, clients, and profiling to CPUs 6–7 at nice 10 on the shared 8-vCPU VM.
Another worker also used the VM, including a CPU-intensive recluster run and builds; timings
are sensitive to that contention. Builds use nice 19 and two jobs. The memory cache is 1 GiB, with 4,000-byte blocks, 512-block
coalescing, and fetch concurrency 512, matching the earlier scale harness. Warm measurements
follow one complete workload pass. Cold measurements use a fresh process per query for the
first 60 queries, through a read-only local S3 facade with a fixed 25 ms delay per request.

### What changed

- **Dictionary bundles, text format 5.** Each bundle contains an FST, term directories, and the
  posting payloads for terms with at most 32 blocks. The reader slices these bytes directly,
  even when its `RangeRead` has no cache. Bundles have at most 128 terms and an estimated 32 KiB
  payload budget; large directories occupy their own bundle. Query dictionaries and directories
  arrive together, and rare terms need no separate posting fetch. Large terms keep exact window
  bounds and two posting batches when needed.
- **Less repeated CPU work.** A sampled baseline profile attributed about 57% of CPU samples to
  BM25 search and another 8% to posting decode. Scoring now precomputes term weights and caches
  exact f64 saturation factors by average length, k1, and b. Both preserve the reference operation
  order. Scoring binary-searches each row window's posting slice, uses dense block cache slots,
  and removes redundant allowed-row membership probes for fully live segments.
  Readers retain at most 8 MiB of decoded sparse posting payloads and 8,192 blocks, with
  conservative memory allowances charged to the engine cache. Sparse term scores are summed
  at the same row before taking window maxima. Dense terms skip rows whose remaining bound
  cannot reach the current heap threshold, with a rounding margin covering all query terms.
  Scores and query answers are never cached.
- **Compact winner IDs.** A checksummed row-to-ID map uses numeric strides or string prefix
  compression and a fixed 128 KiB admission budget. ID-only BM25 fetches admitted maps with
  opening metadata, avoiding document footers and late winner-ID reads. The six local maps
  total 246 bytes. Larger maps record a 17-byte marker and keep ranged ID reads. New segments
  write the map, and ordinary indexer publications backfill older segments once. The existing
  segment-file map carries an optional `text_keys` role, with no manifest schema change.
- **Automatic upgrades.** The background indexer rebuilds one older text field from immutable
  document columns per fenced publication and retires its old object for normal GC. Checkpoint
  comparison now notices changed text object keys even when segment IDs and live masks stay the
  same; otherwise repeated passes could rebuild the same field. Versions 1–4 remain readable
  during upgrades. New writes always use version 5, with no layout-selection setting.

The manifest schema is unchanged, so its version and the bucket fence do not increase. Text
footers carry version 5 through the existing manifest header representation. The writer repacks
its intermediate output into one copy of each posting and directory. A reserved FST region
breaks the dependency between dictionary size and its absolute directory offsets.

### Results

| Metric | Before | After |
| --- | ---: | ---: |
| Cold reads/query, mean | 189.0 | 117.2 |
| Cold MB/query, mean | 4.350 | 3.606 |
| Cold waves/query, mean | 3.25 | 3.02 |
| RTT-only model, mean | 81.3 ms | 75.4 ms |
| RTT-only model, p50 / p90 | 75 / 100 ms | 75 / 100 ms |
| Fresh-process local facade, p50 / p90 | 457.3 / 669.6 ms | 254.5 / 352.7 ms |
| Warm c1, p50 / p90 | 45.5 / 99.8 ms | 18.4 / 44.1 ms |
| Warm c16, p50 / p90 | 658.5 / 827.4 ms | 326.3 / 441.3 ms |
| overlap@10, all 1,000 queries | 1.000 | 1.000 |
| Live text index bytes | 730,399,461 | 743,799,785 |
| Compact ID-map bytes | — | 246 |

The local facade's p90 fell 47%, warm c1 p50 fell 59%, and warm c16 p50 fell 50%. Reads fell
38%, and bytes fell 17%. Live text indexes grew 1.8%. Both warm client counts match
exhaustive top-10 truth on every query, and all 60 cold queries also match.

The final binary SHA-256 is
`34d713d68ab02f02c07e680351fdbfda34b16d1ae4e306d48de3918a4d3e9b97`.

The targets remain open. Warm c1 on this shared two-CPU allocation is above 13/18 ms, and the
mean wave count remains above two. I did not measure AWS latency in this round. The in-region
fresh-process p90 below 381 ms and warm 13/18 ms targets require the coordinator's confirmation;
these local timings do not establish a win over the published turbopuffer numbers.

Formatting, clippy for `bf-text` and `bf-engine` with all targets and warnings denied, and all
136 tests in those crates passed. I disabled test debug symbols and incremental compilation
to keep artifacts within the shared disk budget. Compat passed 121/121 in normal and forced
modes, with 4/4 SDK retry checks and the TypeScript smoke and retry checks in each mode.

### Interpreting the cold model

`bench/scale/local_s3.py` serves an immutable local file store over a read-only S3 facade.
`bench/scale/bm25_local.py cold` starts a fresh server per query and counts `BF_TRACE_READS` with
exactly the idle-gap rule used by `waves.py` and `timeline.py`: a wave starts when every earlier
read has finished. Overlapping independent reads can hide dependencies, so this count and
`waves × 25 ms` are lower bounds, not an estimate of total AWS latency. The measured facade
latency also includes local CPU, Python HTTP handling, and transfer; it does not reproduce S3's
latency distribution, throttling, TLS setup, or the c7gd CPU. The coordinator must confirm the
fresh-process p90 target in-region. The observed wave histogram is six queries at two waves,
47 at three, and seven at four. Removing independent footer reads can expose idle gaps that
those reads previously bridged; this coarse count can rise even as latency and bytes fall.
The first query falls from 101 reads and three waves in the baseline to 42 reads and two waves,
with no document-file reads on the new path. Broad posting batches remain the main extra dependency.

The first diagnostic pass used the production CLI's default 1 MiB cache blocks. It fetched
roughly 48–88 MB on individual cold queries and was discarded. The reported runs use the earlier
scale harness's 4,000-byte blocks. I also discarded a warm pass that overlapped exhaustive truth
generation, and reran the baseline after matching the final segment count.

An early bundle prototype duplicated original records and postings. It measured 28.7/66.9 ms
warm c1 and 412/619 ms through the facade, with 169 reads, 4.35 MB, and 3.12 waves. Its text
indexes grew from 730 MB to 969 MB. I discarded that layout: the final writer has one copy of
each payload, colocates moderately sized lists too, and uses row-aligned sparse bounds.

### Reproduction

Build the server and exact scorer with the throttled Cargo commands, and use
`bench/scale/msmarco_load.py` to load a dedicated local `file://` store. Keep its data and outputs
under `.scratch/`, and stop only the exact load-server PID. Let indexing, compaction, and automatic
text upgrades finish before measuring. Run the baseline against its own version-4 store;
an older binary cannot query the new text format.

```sh
source ~/.cargo/env
nice -n 19 cargo build -j 2 --release -p blowfish --bin blowfish
nice -n 19 cargo build -j 2 --release -p bf-text --example exact_bm25
# STORE is an already indexed local directory under .scratch/.
timeout 1800 taskset -c 6-7 nice -n 10 uv run bench/scale/bm25_local.py warm \
  --binary target/release/blowfish --store "$STORE" > .scratch/warm.jsonl
timeout 1800 taskset -c 6-7 nice -n 10 uv run bench/scale/bm25_local.py cold \
  --binary target/release/blowfish --store "$STORE" --queries 60 > .scratch/cold.jsonl
uv run python bench/scale/bm25_overlap.py .scratch/truth.json .scratch/warm.jsonl
```

The local runner writes JSON summaries to stderr, emits per-query JSONL to stdout, and removes
its temporary server logs and child processes on exit. Its warm output labels c1 and c16 rows
with `clients`; use each group separately when comparing timings. The exhaustive scorer takes
the original corpus JSONL on stdin and the checked-in workload as its argument. Delete the
corpus, stores, results, and other run artifacts after recording the measurements.

## Iteration 22: the fixed costs of a warm vector query (BEIR NQ, one machine)

This used the same workload, VM, and limits as [Iteration 21](#iteration-21-one-recluster-assignment-at-every-size-and-a-lighter-query-path-beir-nq-one-machine):
the NQ store reclustered by main (32 cells, 13,391 leaves), the server pinned to CPUs 0-5, and 16 clients on CPUs 6-7.
The goal was 1,000 q/s at recall@10 0.95 or more, against a design target of 5 vCPU-ms a query. I worked on the costs
every query pays whatever it probes: request parsing, scan bookkeeping, center ranking, setup per slab, and hashing.
There are no format changes.

### Results

Another worker's builds shared the VM, so the load average was 7 to 13 throughout and the server was never saturated.
Wall-clock q/s moved by up to 15% between passes of the same binary. I report hardware counters from `perf stat` on the
server process, divided by the queries served, which repeat within 0.2% (instructions) and about 3% (cycles). Each row is
the mean of two interleaved rounds of two passes, after four warm-up passes.

| Config | Recall@10 | Recall@100 (qrels) | Instructions per query | Cycles per query | q/s at 16 |
|---|---:|---:|---:|---:|---:|
| main, 64 probes | 0.961 | 0.937 | 25.5 M | 26.6 M | 509 |
| branch, 64 probes (default) | 0.961 | 0.937 | **15.3 M** | **21.4 M** | 574 |
| branch, 48 probes | 0.951 | 0.930 | 13.3 M | 17.6 M | 657 |

The branch returns the same results as main (identical recall and nDCG). Instructions per query fell by 40%, and
cycles by 20%. The cycle saving is smaller because the work that remains, the fast-scan kernel and memory traffic, runs
at a lower IPC. Across all of this round's runs, branch q/s at 64 probes ranged from 533 to 697, and main's from 430 to 613.
1-client latency at 64 probes was 4.3 / 7.8 / 11.2 ms p50 / p95 / p99 (iteration 21: 5.3 / 8.2 / 9.7 ms).

**The gate is not met.** At 64 probes a query costs about 7 ms of CPU here (21.4 M cycles), so 6 saturated vCPUs top out
near 850 q/s. At 48 probes (recall@10 0.951) it costs about 5.9 ms, or about 1,000 q/s with 6 vCPUs to itself, which this
shared VM never gave the server. 5 vCPU-ms a query needs about 25% fewer cycles than the branch spends at 48 probes.

### Recall against probes

Reclustered NQ, branch (the same at main):

| Probes | 40 | 44 | 48 | 52 | 56 | 60 | 64 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Recall@10 | 0.944 | 0.948 | 0.951 | 0.954 | 0.957 | 0.959 | 0.961 |
| Recall@100 (qrels) | 0.927 | 0.929 | 0.930 | 0.933 | 0.935 | 0.936 | 0.937 |

I kept the default at 64. 48 probes clears 0.95 on NQ, but the 100M layout measured 0.941 at 48 and 0.954 at 64
([Iteration 18](#iteration-18-100m-warm-ann-under-150-ms-and-a-small-cold-manifest-read)), and the 200K `bench` ladder
gives 0.94 at 64. Lowering it needs recall at 10M- and 100M-sized layouts first.

### What changed

- **A typed `rank_by` reader.** A plain `["vector", "ANN", [numbers]]` is read straight into `f32`s. Everything else
  is rebuilt as a JSON value and goes through the old parser, and a test checks both readers agree on results and errors.
  The re-serialized form prints each number in its shortest `f32` form, as before.
- **Query bodies without path tracking.** axum's `Json` runs every value through `serde_path_to_error`. A body that
  parses is now read with `serde_json` directly, and only a body that fails is parsed again through `Json`, so every
  rejection is unchanged.
- **Fast-scan bookkeeping.** The per-block distance and bound math runs 32 lanes at a time in AVX-512 code (the build
  target is baseline x86-64, so the hot functions are compiled twice and chosen at run time). A cluster's quantization
  factors are checked once, with its CRC, instead of on every block of every scan. Factor loads no longer call `memcpy`.
- **Center ranking in one kernel.** A directory section's center dots run in one AVX-512 loop with the query's bit
  planes held in registers (for 512 and 1,024 padded dimensions), bit-identical to the per-center dot.
- **Probe picking on integers.** Candidates are `u64` keys (the distance in total order, then the entry), and each
  probed segment takes its entries from the picked indices instead of filtering all of its entries through a hash set.
- **Per-slab setup.** Each directory section builds its stub list once instead of on every query, row counts read the
  checksum count instead of walking the row bitmap, and `BF_TRACE_STAGES` is read once.
- **The scan memo key.** Every probed slab hashed the whole 1,024-value query vector with SipHash, and took the memo's
  lock, even on warm queries where the memo is empty. The vector is now hashed once a query, with foldhash, and an
  empty memo is skipped. Vector rank keys write their hex digits directly instead of through `format!`.

### Profile (16 clients, reclustered, 64 probes)

This profile is of the branch just before the center-ranking kernel, which then cut another 2.9 M instructions a query.

| Area | Share of CPU |
|---|---:|
| Fast-scan kernel (`block_sums_avx512`) | 14.8% |
| Fast-scan bookkeeping (`FastQuery::scan`) | 6.0% |
| Center ranking (dots, then distances) | 8.8% |
| JSON request parsing (the vector's numbers) | 4.2% |
| `memcpy` (mostly block-cache reads that cross blocks) | about 4% |
| Block-cache and decoded-cluster lookups | about 3% |
| Rerank distances (`l2_squared`) | 2.9% |
| Per-query table setup (`FastQuery::new`) | 1.7% |
| Scan memo key (SipHash of the vector, per slab; now removed) | 1.4% |
| Vector rank key (`format!` per value; now removed) | 1.3% |
| Per-slab index stubs (build and drop) | 1.4% |

Against iteration 21's profile, JSON parsing fell from about 5% (plus `serde_path_to_error`) to 4.2%, bookkeeping from
8.5% to 6.0%, and table setup from 2.7% to 1.7%. The kernel's share grew because the total shrank; its cost did not change.

### Checks

- `cargo fmt`, workspace clippy with warnings denied, and `cargo test --workspace` (256 passed, 4 ignored) all pass,
  including `read_cache::cold_ann_has_three_read_stages`. It failed once in a workspace run while an NQ server was also
  running on the VM, and passed in 7 runs alone and in the final workspace run.
- The compat suite passes 121/121.
- Old indexes: every number above is the branch serving a store that main built and reclustered.
- The `bench` ladder (768 dimensions, 64 probes, against main) gives the same recall at 10K, 50K, and 200K rows
  (1.0, 1.0, and 0.94). With 300 queries each, warm ANN p50 was 0.67 / 1.61 / 3.65 ms against main's 0.83 / 2.50 / 4.04
  ms, and p99 was no higher. The default 20-query runs were within their spread (cold open 0.6 to 3.7 ms on both).

### Tried and dropped

- **Two blocks per fast-scan kernel call**, sharing each table load. It was slower in isolation (8.5 against 6.1 ns a
  row): the two blocks' accumulators did not fit in registers alongside the tables.
- **An `f32` array `map` for factor loads** compiled to a call per block. A plain loop is vectorized.

### What is left

- The fast-scan kernel is now the largest cost (about 1 ms a query at 64 probes). Fewer clusters per query, from a
  graph over leaf centers (`docs/design/graph-index.md`), is the next lever for both q/s and recall.
- JSON parsing of the 1,024 numbers is still about 4%. Base64 vectors would skip it.
- Each probed slab still builds and drops an index of stubs (about 1.4%), and cache lookups cost about 3%.

## Round D1: one ANN object per segment and slab (2026-11-23)

I compared parent `587b8fe` with D1 (`c4d1878`) on three local namespaces, each with 200,000 rows and
1,024-dimensional f16 vectors. The synthetic namespace has one final segment; its reclustered copy adds
six slabs and 998 leaves. The NQ slice uses the first 200,000 corpus embeddings and the first 128 held-out
queries prepared by `bench/nq`, with the harness's short synthetic text and bucket attributes. I measured
ANN recall against exact neighbors within the slice; the full 2.68M corpus and qrels scoring remain unmeasured.

I used release builds with debug information and LTO disabled on a shared x86-64 VM, pinned every client
and engine process to CPUs 6–7, and limited Rayon and Tokio to two workers. Another CPU study shared those
cores, so I used four alternating old/new rounds for latency and instruction counts. I restored isolated
format-2 controls from retained pre-upgrade checkpoints and immutable objects; the upgraded prefixes kept
their format-3 fences. Both source snapshots used the same updated `bench.rs` input harness.

### Live indexed footprint

I summed manifest-referenced segment files, live masks, ANN directories, slabs, and text statistics.
These are logical object payload bytes; I exclude WAL history, manifests, retired objects awaiting GC,
and the storage layer's 24-byte create envelope. The remaining `.bff` object is the ID filter.

| Namespace | ANN objects, before → after | ANN bytes/row, before → after | Indexed bytes/row, before → after | Indexed reduction |
|---|---:|---:|---:|---:|
| Synthetic, as loaded | 3 → 1 | 2,396.02 → 2,215.33 | 4,602.05 → 4,420.90 | 3.94% |
| NQ slice, as loaded | 3 → 1 | 2,393.65 → 2,214.15 | 4,599.68 → 4,419.72 | 3.91% |
| Synthetic, reclustered | 15 → 7 | 4,808.50 → 4,446.04 | 7,015.65 → 6,652.72 | 5.17% |

I removed the duplicate ANN codes and cluster blobs in this round. Document vectors and the additive
slab vector copies remain for D2 and D4; the larger disk-footprint target depends on those rounds.

### Query parity and latency

I used `scale_query` with 48 probes, `top_k: 10`, strong consistency, and only IDs in the projection.
Each alternating run warmed 256 queries and timed the same 256: 128 unfiltered and 128 with
`bucket Eq 3` (one sixteenth of the rows). Warm values below are medians of four run percentiles.
Cold values are medians of ten paired queries, each in a fresh process with empty blowfish caches.
The OS page cache was retained; these are local file measurements, not S3 cold measurements.

| Namespace / query | Warm p50 ms, before → after | Warm p99 ms, before → after | Cold p50 ms, before → after |
|---|---:|---:|---:|
| Synthetic, as loaded, ann | 2.64 → 2.61 | 14.87 → 14.15 | 30.89 → 33.14 |
| Synthetic, as loaded, filtered | 2.59 → 2.55 | 15.48 → 12.76 | 47.15 → 34.36 |
| NQ slice, as loaded, ann | 1.48 → 1.47 | 8.47 → 9.06 | 30.33 → 27.83 |
| NQ slice, as loaded, filtered | 2.67 → 2.82 | 12.74 → 14.15 | 65.35 → 64.28 |
| Synthetic, reclustered, ann | 3.27 → 2.83 | 15.75 → 14.84 | 51.41 → 43.67 |
| Synthetic, reclustered, filtered | 2.74 → 2.34 | 11.88 → 12.07 | 54.75 → 38.95 |

I found no latency regression outside the observed spread. For example, synthetic unfiltered warm p50
ranged from 2.46 to 2.91 ms before and 2.04 to 3.08 ms after; its cold samples ranged from 24.52 to
35.51 ms before and 24.86 to 43.26 ms after. Shared-core scheduling makes the tail percentiles noisy.

I compared 3,072 paired timed responses and 60 additional paired cold responses: every ID list matched,
and every distance matched within 1e-6. The 20-query recall@10 checks also remained identical: synthetic
as loaded `0.985`, NQ `0.985`, and reclustered synthetic `1.000`; filtered recall was `0.965`, `0.975`,
and `0.965`, respectively. Unit tests additionally compare the stored codes and vector bytes directly.

### Instructions and cold reads

I ran `perf stat` over each old/new arm. Instructions per query include process setup, the 256-query
warm-up pass, the 256 timed queries, diagnostics, and result serialization, divided by 512. Each workload
is half filtered and half unfiltered ANN. The table reports median and full range across four runs.

| Namespace | Old M instructions/query [min–max] | New M instructions/query [min–max] | Median change |
|---|---:|---:|---:|
| Synthetic, as loaded | 11.276 [11.253–12.185] | 11.297 [11.264–12.162] | +0.18% |
| NQ slice, as loaded | 8.860 [8.819–9.687] | 8.873 [8.825–9.731] | +0.15% |
| Synthetic, reclustered | 12.287 [12.233–13.105] | 12.232 [12.221–13.276] | -0.45% |

I recorded backend requests and transferred bytes for the ten cold queries per case. These include
snapshot requests and search reads, with decimal MB. Repeating the first query four times gave exactly
the same request count and byte total in every arm.

| Namespace / query | Mean requests, before → after | Mean MB read, before → after | Byte change |
|---|---:|---:|---:|
| Synthetic, as loaded, ann | 32.5 → 31.2 | 26.55 → 28.15 | +6.0% |
| Synthetic, as loaded, filtered | 46.0 → 43.6 | 32.07 → 24.32 | -24.2% |
| NQ slice, as loaded, ann | 32.2 → 31.7 | 22.18 → 22.05 | -0.6% |
| NQ slice, as loaded, filtered | 67.2 → 65.5 | 42.87 → 38.81 | -9.5% |
| Synthetic, reclustered, ann | 36.1 → 32.2 | 32.29 → 33.67 | +4.3% |
| Synthetic, reclustered, filtered | 46.0 → 43.6 | 32.07 → 24.32 | -24.2% |

All six cases use fewer backend requests. The synthetic unfiltered cases read more bytes; I infer
coalescing gaps and cache-block alignment after moving vectors into tour order account for that tradeoff.
The NQ unfiltered mean is nearly unchanged, and filtered reads shrink substantially. I keep this byte
increase visible as a tradeoff; the local latency spread does not establish an S3 speedup.

### Upgrade

I ran `blowfish reindex` on each old prefix while pinned to CPUs 6–7, without queries running during
its timed interval. The reported write bytes cover ANN objects; directory and manifest writes are extra.

| Namespace | Segment indexes + slabs rewritten | ANN bytes written | Wall seconds | Peak RSS MiB |
|---|---:|---:|---:|---:|
| Synthetic, as loaded | 1 + 0 | 443,066,848 | 3.11 | 911 |
| NQ slice, as loaded | 1 + 0 | 442,829,696 | 4.80 | 878 |
| Synthetic, reclustered | 1 + 6 | 889,207,104 | 10.19 | 875 |

A second run rewrote zero objects and published nothing in all three cases. The parent binary refused
every upgraded prefix with `BucketFormat { required: 3, supported: 2 }`. A CLI dry run left an isolated
format-2 prefix unchanged. Concurrent writes, partial slab batches, and resident generation refresh are
covered separately by Rust tests. The follow-up below checks queries during conversion, including mixed
segment and slab sets.

### Queries during the automatic upgrade

I followed up with queries at 0%, 50%, and 100% conversion. I loaded each 200K-row namespace in four
50K-row batches; the indexer combined them into two ANN segments. I also measured each namespace
reclustered into six slabs, with 997 leaves. I used a 60K-row cap for NQ; synthetic data produced the
same six slabs at both the 50K and 60K caps used in its two passes. That gives actual halfway checkpoints:
one of two segment indexes, or three of six slabs, converted. I constructed legacy-format fixtures from
the same trained clusters and vectors, then used the real fenced reindex publisher, one object per batch.

I ran two complete passes per input on CPUs 6–7, with two Tokio workers and two Rayon workers. At each
checkpoint, a new engine warmed all 128 requests and then timed them: 64 unfiltered ANN queries and
64 with `bucket Eq 3`. I used 48 probes, a rerank factor of 32, `top_k: 10`, and IDs in the projection.
Recall uses exact neighbors for the first 16 queries of each kind. These are separate query and segment
layouts from the completed-layout comparison above; I compare their migration stages with each other.

I report the median of the two run percentiles below. These are local warm timings. A new engine shares
the storage cache, and I retained the OS page cache, so I do not label its first query as cold.

| Namespace / query | 0% p50 / p90 ms | 50% p50 / p90 ms | 100% p50 / p90 ms | Median ANN rows, every stage | Recall@10, every stage |
|---|---:|---:|---:|---:|---:|
| Synthetic, segments, ANN | 2.71 / 5.20 | 2.21 / 3.14 | 2.22 / 3.38 | 43,733 | 0.86250 |
| Synthetic, segments, filtered | 2.39 / 5.95 | 2.07 / 3.23 | 2.10 / 3.49 | 14,453 | 0.90000 |
| Synthetic, slabs, ANN | 1.79 / 2.43 | 2.25 / 2.91 | 2.50 / 3.82 | 21,816 | 0.99375 |
| Synthetic, slabs, filtered | 1.76 / 2.50 | 2.27 / 3.28 | 2.65 / 4.28 | 14,453 | 0.90000 |
| NQ, segments, ANN | 1.27 / 2.08 | 1.09 / 1.75 | 1.00 / 1.48 | 28,836 | 0.96875 |
| NQ, segments, filtered | 2.77 / 3.55 | 2.48 / 3.33 | 2.24 / 2.87 | 82,943 | 1.00000 |
| NQ, slabs, ANN | 1.31 / 2.24 | 1.56 / 3.00 | 1.77 / 3.30 | 24,297 | 0.97500 |
| NQ, slabs, filtered | 2.60 / 3.50 | 3.18 / 5.25 | 3.51 / 5.55 | 82,943 | 1.00000 |

Every timed query recorded zero `document_scan_rows`: none of the 3,072 timed responses required a
scan of document vectors. All ID lists matched their zero-converted baseline. The minimum, median, and
maximum `ann_rows_scanned` also matched at every checkpoint and on both passes. I count rows in selected
leaf code blocks or exact ANN ranges, including filtered-out rows in a fetched code block.

Mixed slab p50 increased about 26% on synthetic data and 20% on NQ relative to zero converted. The
filtered rows in those same slab runs still use the unchanged, fully converted segment indexes; their
p50 increased about 29% and 22%, respectively. I therefore cannot attribute the timing drift to the
mixed format alone. The table retains that variation, including p90, while the probe work and recall
remain unchanged. I measured no S3 cold queries or 100M queries in this follow-up. The small directories
here are flat; regression tests separately force paged directories through every slab conversion batch.

The original failure happened when any segment lacked `annv2:`, or a partial batch left a directory
whose immutable keys no longer covered its segments or slabs. I now rebuild directories across mixed
objects. Before the first publication, upgrade-only readers use an existing old directory or assemble
resident leaf metadata without writing storage. Legacy code flags and membership maps feed the same
global leaf selection and rerank as current objects; migration does not require document-vector scans.

I keep the reproduction harness in `crates/bf-engine/src/reindex_migration_tests.rs`. I build it with:

```sh
source ~/.cargo/env
CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=false \
  nice -n 19 cargo test --release -p bf-engine --lib migration_200k_benchmark --no-run -j 2
```

I set `TEST_BINARY` to the test executable printed by Cargo, then run:

```sh
RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2 taskset -c 6,7 timeout 1800 \
  "$TEST_BINARY" migration_200k_benchmark --ignored --nocapture
```

For NQ, I additionally set `D1_CORPUS_F16` to raw little-endian f16 corpus bytes and `D1_QUERIES_F32`
to the first 64 query vectors as raw f32. The preparation harness's `corpus.f16` is a NumPy file, so
I strip its NumPy header before exporting the first 200K rows. I keep inputs and logs under `.scratch/`;
the test removes its disposable object store when it finishes.

### Reproduction notes

I use separate source snapshots and build directories for the parent and D1, copy the updated
`crates/bf-engine/examples/bench.rs` into the parent snapshot, and save a consistent old-prefix copy
before running the upgrade. Every data directory and log belongs under `.scratch/` and is removed after
I copy the useful results here. The 200K input command is:

```sh
source ~/.cargo/env
CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=false \
  nice -n 19 cargo build --release -p bf-engine --example bench --example scale_query -j 2
RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2 taskset -c 6,7 timeout 1800 \
  target/release/examples/bench --url "file://$PWD/.scratch/d1/store" \
  --rows 200000 --dims 1024 --batch 25000 --queries 128 --recall-queries 20 \
  --nprobe 48 --vector-type f16
```

For NQ, I export the first 200K rows from the harness's NumPy `corpus.f16` file to raw little-endian f16,
and the first 128 `queries.npy` rows to raw f32. I add `--corpus-f16 RAW_CORPUS --queries-f32 RAW_QUERIES`.
The indexed-byte and ANN-object totals are included in the harness output. For the alternating runs,
I supply `scale_query` a read-only storage JSON and a workload JSON containing 128 held-out vectors,
each once unfiltered and once with `bucket Eq 3`, and run four old/new rounds:

```sh
BF_ANN_NPROBE=48 RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2 \
  "$PERF" stat -x ";" --no-big-num -e instructions,cycles -o "$RUN/counters.txt" -- \
  taskset -c 6,7 timeout 180 "$SCALE_QUERY" storage.json workloads.json NAMESPACE 1 warmup
```

I use the installed `perf` executable directly when its kernel-version wrapper cannot find it. A
single-request workload without `warmup` produces the cold backend request and byte diagnostics.
I validate with strict clippy and the tests for the five changed crates (224 passed, 4 ignored),
121/121 compatibility scenarios in both normal and forced indexing modes, both TypeScript smoke runs,
and 9/9 Python compatibility unit tests.

## Automatic recluster, rounds R1 and R2 (BEIR NQ, one machine)

This round makes the recluster automatic (`docs/design/auto-recluster.md`, rounds R1 and R2): the server builds a base
tier of slabs in the background and installs it in its own indexer publication. Unfiltered ANN runs one plan over the
tier and the segments written since. Same VM and harness as
[Iteration 22](#iteration-22-the-fixed-costs-of-a-warm-vector-query-beir-nq-one-machine): server pinned to CPUs 0-5 in
a 12 GB scope, clients on CPUs 6-7, 64 probes, and other workers' builds sharing the VM (load average 4 to 15), so I
report hardware counters per query next to wall-clock q/s.

### Load, then the job, unattended

I loaded all 2,681,468 rows into a fresh store (`load.py`, four requests in flight) and did nothing else.

- **Load:** 268.7 s (9,979 rows/s). Compaction was pending at every publication of the load, so the trigger deferred
  31 times (`reason="compaction"`) and no build competed with ingest.
- **Trigger:** about a minute after the last write, the trigger saw 2,681,468 rows in 27 segments and no tier
  (gain 4.2) and started a base build.
- **Build:** 231 s on half of the six cores: vectors copied at 15 s, cells at 21 s, 13,393 leaves trained at 64 s,
  rows assigned at 104 s, refined at 172 s, 32 slabs planned at 193 s and written by 231 s. The job sized slabs at
  536,870 rows from the memory budget. Server RSS during the build was at most 791 MB (sampled every 20 s); the
  server's peak, 3.4 GB, was during the load.
- **Queries afterwards:**

| Layout | Recall@10 | Recall@100 (qrels) | nDCG@10 | p50 / p95 / p99 ms (1 client) | q/s at 1 / 4 / 16 |
|---|---:|---:|---:|---|---|
| main, reclustered by hand (Iteration 22) | 0.961 | 0.937 | | 4.3 / 7.8 / 11.2 | 574 at 16 |
| automatic tier | **0.962** | 0.934 | 0.608 | 4.4 / 7.8 / 10.9 | 181 / 307 / **587** |

The automatic tier answers like main's hand-built one. With four warm-up passes, a query costs 15.5 M instructions
and 19.9 M cycles (Iteration 22: 15.3 M and 21.4 M).

An earlier run of the same load, with a different flush timing, built at 175,000 rows (168 s) and again at 1,595,000
(257 s) during the load, then deferred for the bulk load and never evaluated again: a deferral marked the manifest
generation as evaluated, and no more publications came. A deferred or failed evaluation now comes due again after a
minute (ten for a failure).

### Writes after the tier

I rewrote the last 5% of rows (134,073 rows, titles changed so the upserts are not no-ops) and measured instructions and
cycles per query at 16 clients (four warm-up passes, then four measured; the q/s columns are from `run.sh`).

| Layout | Instructions / query | Cycles / query | Recall@10 | q/s at 16 |
|---|---:|---:|---:|---:|
| tier covers every row | 15.5 M | 19.9 M | 0.962 | 587 |
| 5% of covered rows deleted (tier masked, nothing outside it) | 16.3 M | 21.5 M | | |
| 5% rewritten, first R1 plan (two warm-up passes) | 35.3 M | 36.7 M | 0.962 | 420 |
| 5% rewritten, this round | **20.9 M** | 28.1 M | 0.959 | 511 |

The first R1 plan searched the rows outside the tier through the namespace directory with covered segments skipped,
but it still ranked every covered segment's centers, rebuilt the tier's live-row mask on every query, and probed the
134,073 rows as if they were a namespace of their own: about a third of the tier's scanned rows for 5% of the data.
Now covered segments' sections are left out of the center ranking, the mask is built once per live generation, and
the search beside a tier probes `sqrt(rows outside / all rows)` of a standalone probe, the share one clustering of all
rows would give them. Recall@10 went from 0.960 to 0.959 with that last change.

The 5.4 M instructions still over a fully covered query are the second search's fixed costs (five more segments, a
second rerank pool, and plan work over the whole directory). R3's delta tier replaces that search.

### The rebuild trigger

I then rewrote 25.4% of rows (681,468, another title change) in 105 s. The trigger deferred for the bulk load at every
minute for five minutes (`reason="bulk_load"`, covered 2,681,468, outside 681,468, gain 3.6), then rebuilt the base
over every live row in 287 s. The build added about 150 MB to the 6.5 GB the server held after the write load (its peak,
9.2 GB, was during the writes, not the build).

### Crash safety

`crates/bf-engine/tests/auto_recluster.rs` injects failures into the object store:

- A build that dies after uploading two slab objects leaves the namespace answering from its current tier. GC (retention
  zero) keeps the dead build's objects while its record is fresh, and deletes them as orphans once it is stale. The
  installed tier's objects stay, and the spill directory is empty.
- An install publication that fails, before or after writing (the ambiguous case), is retried. GC at retention zero then
  keeps every slab the manifest references, and a fresh engine answers from the tier.

Both found bugs before they passed: the build-record listing used a partial path segment and found no records, and GC
deleted fresh records at retention zero.

I also killed the server with `kill -9` during a rebuild on NQ (a 25% rewrite, 681,468 rows), three times, and
restarted it on the same store:

| Kill point | Slabs uploaded | After restart | Referenced tier objects | Restarted build |
|---|---:|---|---|---|
| Assignment, 90 s in | 0 | same top 3 at once | 33 of 33 | installed in 227 s |
| 215 s in (a slow VM, still before uploads) | 0 | same top 3 at once | 33 of 33 | installed in 260 s |
| After the last slab, before install | 32 | same top 3 at once | 33 of 33 | installed in 245 s |

In every case the old tier kept answering, and no referenced object was missing before or after the new install. The
killed build's 32 uploaded slabs stayed in the bucket under their fresh record, for GC to reclaim once the record is
stale. A failed or ambiguous install publication is covered by the fault test above, not by a real kill.

## Automatic recluster, round R3 (BEIR NQ, one machine)

Round R3 adds the delta tier, the merge and masked-rows rebuilds, the persistent bulk-load guard, and build-record
cleanup (`docs/design/auto-recluster.md`). Same VM and harness as rounds R1 and R2: the server is pinned to CPUs 0-5,
the clients to CPUs 6-7, 64 probes, and other workers share the VM. Instruction counts here are user-space
instructions per query from `perf stat` over 4,000 one-client queries (two passes), after two warm-up passes. To
compare layouts under the same load, I ran two servers at once on hard-linked copies of one store and alternated the
measurements between them.

### The gate: 5% of rows rewritten

The fully covered store is a base over all 2,681,468 rows. The other copy has its last 134,073 rows rewritten (new
titles, same vectors), which the server answers from a delta built on the base's coarse cells (6 to 11 s with 656
leaves in one slab; a base build takes about 5 minutes). Recall is against the exact top 10 over 3,452 queries.

| Layout | Instructions / query | Recall@10 | Recall@100 (qrels) | p50 / p95 ms |
|---|---:|---:|---:|---|
| base over every row | 16.03 M | 0.961 | 0.934 | 4.5 / 8.8 |
| 5% of rows deleted (base masked, nothing outside it) | 16.80 M (+4.8%) | | | |
| 5% rewritten, side search (rounds R1 and R2) | +36% (16 clients) | 0.960 | | |
| 5% rewritten, base and delta | **17.84 M (+11.3%)** | **0.960** | **0.934** | 4.7 / 7.4 |

The gate asked for at most 10% more instructions at equal recall. This lands at 11.3% (17.78 to 17.95 M over five
runs, against 16.03 to 16.11 M), with recall@10 down 0.001 and recall@100 unchanged. Of the 1.8 M extra instructions,
0.8 M is the 5% of base rows the rewrite masked, which any update pays until a rebuild (the masked-only copy shows
it). The base probes 5.3% more leaves to find the same live rows. The other 1.0 M is the delta: 26 more leaves (about
5% more scanned rows) and a second plan, rerank, and hydration.

The plan took four tries, and the first three are worth recording:

- **One list over both tiers' leaves:** a delta's leaves hold as many rows as the base's over a sparser sample, so
  their centers sit nearer every query, and the walk spent up to 446 of about 550 probes on the delta's 656 leaves.
  The base lost recall: 0.955 against 0.961.
- **Delta leaves as dense as the base's** (about 10 rows each): 13,391 more centers to rank cost 45% more
  instructions per query, and recall still fell to 0.959.
- **One row budget split by share:** the base got 95% of the probe and lost recall on its own rows (0.956 against
  0.959).
- **Now:** the base probes as it would alone, the delta `sqrt(its share)` of what it would alone (the rule rows
  outside the tiers already use), and the two exchange the k-th smallest upper bound before reranking and the k-th
  exact distance before hydrating. The base's own rows come back at the same recall as with no delta (0.9592 on the
  first 1,000 queries in both). The delta's rows, 2% of the true top 10, come back at 0.918.

### Merges, masked rows, and the bulk-load guard

- **Masked rebuild:** after rewriting 440,000 more rows on the base and delta, the masked rows reached 574,073 (20% of
  the tiers' 2,815,541 rows). The guard deferred the build each minute for five minutes, then the job rebuilt the
  base on the old coarse cells (`reuses_cells=true`) in 373 s, and the delta was dropped.
- **Bulk-load guard across restarts:** every load above set `bulk_load_at` in the manifest, and builds waited for
  five minutes after the last bulk flush. `a_bulk_load_flush_is_kept_in_the_manifest_so_a_restart_still_waits`
  checks that a restarted server still waits.
- **Build records:** after every install on NQ, the build's record was gone from `indexer/`. The job deletes it
  whether its tier was installed or discarded.

### Continuous writes

The design's test is 10M rows plus 1,000 upserts a second for an hour. I ran it on NQ (2,681,468 rows) for 25 minutes,
starting from a fresh base: the writer upserts random existing rows (same vector, new title), and once a minute or two
a probe measures recall@10 on 300 queries and their latency, one at a time, while the writes go on. At this rate the
writer replaces the whole namespace every 45 minutes, four times the design's rate for its size.

| Minutes | Delta rows | Rows outside the tiers | Recall@10 | p50 / p90 ms |
|---:|---:|---:|---:|---|
| 0 | 0 | 0 | 0.962 | 48 / 109 |
| 3 | 120,752 | 8,487 | 0.955 | 101 / 323 |
| 10 | 483,390 | 32,539 | 0.951 | 88 / 344 |
| 15 (base rebuild running) | 605,582 | 174,566 | 0.947 | 302 / 630 |
| 23 (base rebuild running) | 605,582 | 651,300 | 0.945 | 284 / 470 |
| 25 (new base, then a delta) | 599,022 | 48,350 | 0.947 | 56 / 411 |

- **Writes:** 1,500,000 upserts, none rejected.
- **Deltas:** every two to three minutes, in 6 to 55 s, kept the rows outside the tiers at or under 5.4% (143,521 rows
  at most).
- **Masked rebuilds:** once the delta was built and a fifth of the tiers' rows were masked, the job rebuilt the base,
  in 11.3 minutes under the writes. Only one build runs at a time, so the rows outside the tiers grew to 651,300 (24%)
  before it installed. The delta then covered them within a minute, and the next masked rebuild started straight
  away: at this rate the base is always being rebuilt.
- **Latency:** not within 10% of a fresh base (4.5 ms). A traced query during the writes spent 10 ms in the ANN plan
  over the tiers and the segments outside them, and about 470 ms reading the 63,245 rows not yet indexed (the
  strongly consistent WAL tail), so the tail dominates, not the tiers.
- **Recall@10** stayed between 0.945 and 0.962.

Two problems turned up in earlier runs and are fixed. The trigger asked whether compaction had merges pending with
`force`, which lists merges the indexer defers while a long write tail is pending, so under steady writes every
evaluation deferred, and the rows outside the base reached 300,000 in six minutes with no build. It now waits only
for merges the indexer would run. A masked rebuild also came ahead of a due delta, so updates stayed outside the
tiers for a whole base build. A delta now goes first.

### Crash safety

I killed the server (`kill -9`) 2 s into a delta build on NQ, after one build record was written and before the slab
upload. The installed base and old delta kept answering at once on restart (recall@10 on base rows 0.957 over 300
queries, with the rewritten rows searched beside the tiers). The restarted build installed in 11 s and deleted its own
record. The killed build's record stays until it is stale, and GC then reclaims its objects, as in round R2's tests.

## Rounds D2 and D3: vector-free documents and compressed text

I measured document-v2 on 200,000 synthetic rows and all 2,681,468 BEIR NQ passages, using the
`bench/nq` prepared Cohere vectors, query vectors, titles, and text. Both workloads use 1,024-dimensional
f16 vectors, cosine distance, 100,000-row segments, a `bucket = id % 16` filter, and full-text indexing.
I build persisted segment fixtures with the server's canonical schema, including its default title
filter index: two segments for synthetic data and 27 for NQ. I also measure the synthetic namespace
reclustered into six slabs with leaves targeted at 200 rows.

I compare the post-D1 layout with document-v2: the two arms reference identical trained `.bfa` objects,
filter indexes, full-text indexes, live masks, and row ordinals. Test-only code reconstructs document-v1
and its original JSON-encoded ID Bloom filter from those same stored vectors. Query trials use one
binary against alternating immutable document/ID sets. Compaction additionally uses the actual
pre-D2 code at `6eaa244`, built with the same release settings, for its old arm.

I pin every benchmark to CPUs 6–7, with two Tokio workers and two Rayon workers, and build at
`nice -n 19`, with two Cargo jobs, release debug info disabled, and LTO disabled. Other work shares
the VM. These are local-file engine measurements. Cold samples start a fresh process with empty
blowfish caches; I retain the OS page cache. I make no S3 cold-latency or BM25 quality claim here.

### Active footprint

I sum manifest-referenced segment file sizes, live masks, and ANN directories, including slabs when
present. WAL objects, manifest history, unreferenced fixture files, and retained old objects are excluded.
GB and MB below are decimal.

| Namespace | Indexed GB, before → after | Bytes/row, before → after | Reduction |
|---|---:|---:|---:|
| Synthetic, as loaded | 1.081 → 0.577 | 5,406 → 2,887 | 46.6% |
| NQ, as loaded | 15.942 → 9.602 | 5,945 → 3,581 | 39.8% |
| Synthetic, reclustered | 1.527 → 1.024 | 7,637 → 5,118 | 33.0% |

NQ lands at 3,581 bytes/row as loaded. The reclustered synthetic namespace still keeps 444.60 MB of
segment ANN objects beside 446.09 MB of slab ANN objects. The design's 3,600-byte reclustered target
still needs D4's retirement of covered segment ANN objects and its lineage support.

| Namespace | `docs.bfc` MB, before → after | String block payload MB, before → after | String compression |
|---|---:|---:|---:|
| Synthetic | 506.32 → 3.27 | 94.56 → 1.31 | 72.26× |
| NQ | 6866.73 → 537.51 | 1343.01 → 508.12 | 2.64× |

Synthetic text is deliberately repetitive; its ratio is unrepresentative of natural documents. NQ's
text column alone compresses by 2.53×, close to the design's assumed 2.5×. Each removed dense vector
column saves 2,049 payload bytes/row at this dimension and precision. Removing `ids.bfi`'s Bloom
filter saves another 3.78 bytes/row in its existing JSON encoding. The separate 24-bit `idfilter.bff`
remains unchanged; tests make the ID object unreadable and verify that absent-ID checks never open it.

### Warm and cold queries

I alternate four old/new pairs. Each arm warms 64 held-out vectors across four query kinds, then times
the same 256 requests: ANN with `include_attributes: false`, ANN filtered by `bucket Eq 3`, vector
projection in base64, and title/text projection. I use 48 probes, a rerank factor of 32, `top_k: 10`,
and strong consistency. Warm values are medians of the four run percentiles; brackets give the full
range of the four p50 values. Cold values are medians of ten paired requests per kind, each in its own
fresh process. I repeat default float-vector projection separately with two alternating old/new pairs.

The existing planner retains scalar columns internally for `include_attributes: false`, while an
explicit `["id"]` projection narrows them. These measurements include that scalar-prefetch work;
compressed text contributes to the unprojected query's cold-read and CPU changes. ANN codes and
search kernels are unchanged.

| Namespace / query | Warm p50 ms [min–max], before → after | Warm p99 ms, before → after | Cold p50 ms, before → after |
|---|---:|---:|---:|
| Synthetic / ANN | 3.64 [2.82–4.78] → 3.55 [3.46–3.73] | 9.81 → 12.35 | 77.51 → 54.22 |
| Synthetic / filtered ANN | 3.61 [2.81–4.19] → 3.43 [2.81–3.68] | 10.11 → 12.42 | 96.12 → 62.83 |
| Synthetic / vectors, base64 | 3.72 [2.86–5.00] → 4.16 [3.27–4.68] | 12.75 → 11.13 | 178.88 → 46.86 |
| Synthetic / title + text | 3.40 [2.61–4.63] → 3.45 [2.73–3.79] | 12.19 → 12.61 | 79.93 → 52.76 |
| NQ / ANN | 9.27 [7.21–9.58] → 9.39 [8.22–10.19] | 16.25 → 21.26 | 285.29 → 263.08 |
| NQ / filtered ANN | 21.12 [17.50–23.95] → 22.02 [19.56–24.61] | 35.21 → 40.66 | 600.76 → 619.52 |
| NQ / vectors, base64 | 9.40 [7.34–10.37] → 10.18 [8.40–11.33] | 26.52 → 37.69 | 295.78 → 265.03 |
| NQ / title + text | 9.21 [7.01–9.83] → 8.99 [7.78–10.73] | 18.43 → 17.45 | 275.69 → 266.44 |
| Synthetic, slabs / ANN | 3.03 [2.59–3.18] → 3.38 [3.10–3.53] | 8.79 → 8.38 | 78.06 → 44.41 |
| Synthetic, slabs / filtered ANN | 3.13 [2.38–3.62] → 3.53 [3.05–3.78] | 8.40 → 8.72 | 90.38 → 62.65 |
| Synthetic, slabs / vectors, base64 | 2.96 [2.43–3.80] → 3.82 [3.52–4.41] | 13.62 → 11.67 | 157.94 → 48.32 |
| Synthetic, slabs / title + text | 2.91 [2.08–3.31] → 3.19 [2.95–3.72] | 8.98 → 7.81 | 74.77 → 42.74 |

| Float-vector projection | Warm p50 ms [min–max], before → after | Warm p99 ms, before → after | Cold p50 ms, before → after |
|---|---:|---:|---:|
| Synthetic | 3.62 [3.50–3.74] → 3.79 [3.58–4.00] | 10.07 → 12.02 | 165.54 → 44.22 |
| NQ | 9.59 [9.08–10.10] → 10.10 [9.02–11.18] | 17.67 → 25.39 | 342.77 → 318.63 |

Warm vector projection adds some work to fetch, check, and encode individual ANN rows instead of
cloning a decoded document-column value. Its measured p50 rises modestly, within the observed run
ranges; tail values are noisy. I do not establish a warm wall-clock speedup. Cold synthetic vector
projection improves substantially, while NQ's cold time remains dominated by its ANN reads.

I compare 6,656 timed responses and 280 paired cold responses across these trials: every returned ID,
distance, text attribute, and vector value matches. Base64 vector strings match exactly, and every
query records zero document-scan rows. ANN scan counts also match throughout the alternating warm
trials. Separate tests compare serialized projections at f16, f32, and i8 precision, cover float and base64
responses, and run indexed queries
at 0%, 50%, and 100% of fenced document conversion. I make no new recall claim from these format-only
trials; both arms share the same codes, centers, memberships, and vectors.

### Instructions and backend reads

I run `perf stat` over every arm. Counts include process setup, request construction, the warm-up pass,
the timed pass, and result serialization, divided by both passes' query count. They include the initial
column hydration cost and do not isolate steady-state warm-query CPU. Mixed runs divide by 512, and
float-only runs divide by 128. Values below are median M instructions/query, with the full run range.

| Workload | Before, M instructions/query [min–max] | After, M instructions/query [min–max] |
|---|---:|---:|
| Synthetic, mixed | 45.50 [45.47–47.19] | 15.79 [15.74–15.84] |
| NQ, mixed | 101.29 [97.85–106.25] | 62.26 [61.45–63.47] |
| Synthetic, slabs, mixed | 43.96 [43.86–44.34] | 15.84 [15.81–15.93] |
| Synthetic, float vectors | 147.36 [145.13–149.59] | 24.73 [24.70–24.75] |
| NQ, float vectors | 228.83 [225.74–231.93] | 49.33 [49.12–49.54] |

I also collect physical backend requests and transferred bytes for every fresh-process cold query.
The table gives means across ten queries per arm; snapshot and search reads are both included.

| Namespace / query | Requests, before → after | MB read, before → after |
|---|---:|---:|
| Synthetic / ANN | 55.8 → 35.3 | 67.90 → 31.99 |
| Synthetic / filtered ANN | 89.1 → 65.5 | 71.50 → 34.54 |
| Synthetic / vectors, base64 | 64.9 → 35.3 | 83.53 → 31.99 |
| Synthetic / title + text | 55.8 → 35.3 | 67.90 → 31.99 |
| NQ / ANN | 193.3 → 182.5 | 302.95 → 288.09 |
| NQ / filtered ANN | 335.7 → 317.6 | 559.19 → 534.69 |
| NQ / vectors, base64 | 182.6 → 176.3 | 295.93 → 279.70 |
| NQ / title + text | 193.3 → 182.5 | 302.95 → 288.09 |
| Synthetic, slabs / ANN | 55.5 → 33.4 | 66.81 → 30.99 |
| Synthetic, slabs / filtered ANN | 89.1 → 65.5 | 71.50 → 34.54 |
| Synthetic, slabs / vectors, base64 | 63.0 → 51.5 | 80.57 → 35.98 |
| Synthetic, slabs / title + text | 55.6 → 33.4 | 66.82 → 30.99 |

### Compaction and upgrade

I compact the first two 100,000-row segments of each corpus into one segment. The old arm reads
vectors from document-v1 and writes the original layout using `6eaa244`; the new arm reads vectors
from ANN leaves and writes document-v2. Four alternating pairs give the wall-time median and full
range below. The final two pairs additionally collect whole-process `perf stat` instruction counts.
Uploads include the compacted segment's indexes and documents; no manifest is published for these
isolated compaction outputs.

| Corpus / 200K-row compaction | Seconds [min–max], before → after | G instructions, before → after | MB written, before → after |
|---|---:|---:|---:|
| Synthetic | 12.81 [12.06–13.69] → 11.96 [11.34–12.17] | 115.69 → 113.66 | 1079.36 → 575.55 |
| NQ slice | 21.08 [20.44–22.21] → 21.42 [19.70–25.41] | 139.13 → 142.53 | 1186.67 → 709.39 |

NQ compaction writes 40.2% fewer bytes and uses 2.4% more instructions. Its later paired wall timings
reverse the difference seen in the first two pairs, so I retain the full spread and make no NQ
compaction speedup claim. Compaction selection, tier coverage, lineage, and retirement policy are unchanged.

I run the real fenced upgrade on the as-loaded legacy document sets. ANN is already current, so the
rewrite bytes below are documents and IDs. Directory and manifest writes are extra. Each segment gets
one publication after its epoch claim. The upgrader batches up to 64 source column ranges through its
bypassing reader and checks every source block before compression.

| Namespace | Document/ID pairs | MB rewritten | Seconds | Peak RSS MiB |
|---|---:|---:|---:|---:|
| Synthetic, 200K | 2 | 4.560 | 0.373 | 49.4 |
| NQ, 2,681,468 | 27 | 557.853 | 17.138 | 206.1 |

Both second runs rewrite zero objects, write zero bytes, and publish nothing. Tests cover stale
conversions, concurrent publication, an upgrade without writes, and failures before publication or
after a durable write whose verification GET also fails. Referenced objects survive that ambiguous
case. Old document and ID objects remain protected for normal GC retention.

### Reproduction

The tracked harness is `crates/bf-engine/src/document_benchmark.rs`. I build it with:

```sh
source ~/.cargo/env
CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=false \
  nice -n 19 cargo test --release -p bf-engine --lib document_footprint_benchmark --no-run -j 2
```

I set `D2_TEST_BINARY` to Cargo's printed executable, and use a disposable directory under `.scratch/`:

```sh
export D2_RUN="$PWD/.scratch/d2-repro"
export RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2
D2_ACTION=build taskset -c 6,7 timeout 1800 \
  "$D2_TEST_BINARY" document_footprint_benchmark --ignored --nocapture --test-threads=1
```

`build` writes canonical document-v2 segments, saves `current.json`, reconstructs the exact legacy
column and Bloom encodings, saves `legacy.json`, and selects the legacy manifest. For NQ I additionally
set `D2_ROWS=2681468`, `D2_CORPUS` to the harness's NumPy `corpus.f16`, `D2_QUERIES_F32` to `queries.npy`,
and `D2_ATTRS` to JSONL objects containing `title` and `text`, in the same parquet row order. The reader
accepts NumPy headers or raw packed arrays; I export attributes from the original parquet using `uv`
and `pyarrow`, without re-encoding vector values.

I select an arm with `D2_ACTION=legacy` or `current`, then invoke `D2_ACTION=query D2_WARMUP=1` under
`perf stat -x ';' --no-big-num -e instructions,cycles`, alternating four old/new pairs. Every invocation
uses `taskset -c 6,7 timeout 1800` and the same ignored-test arguments shown above. For cold requests I
omit `D2_WARMUP`, set `D2_QUERIES=1`, choose `D2_QUERY_OFFSET=0..9`, and select `D2_KIND=ann`, `filtered`,
`vector`, or `text`; each request gets a fresh process. Float-only trials additionally use
`D2_KIND=vector D2_VECTOR_ENCODING=float`, with two alternating old/new pairs.

`D2_ACTION=compact` reads and rebuilds the first two source segments; its matching baseline calls
`compaction_rows`, `merge_packed`, and `build_with` from the pre-D2 source snapshot. `D2_ACTION=upgrade`
runs the public fenced upgrader. `D2_ACTION=recluster` builds a synthetic tier and updates both saved
format fixtures to reference it. I collect RSS with `/usr/bin/time -v`, and remove every disposable
store, source snapshot, build directory, input copy, and log after retaining these results.


## Round IN1: text upgrades and update memory (2026-11-23)

### Text upgrades

I measure 960,000 synthetic text documents in 16 immutable 60,000-row segments, with 100 tokens per
row and a 10,007-term vocabulary. Every source field uses text format 4 and lacks `text_keys`. Each
segment is larger than half the 100,000-row cap, so no compaction runs during the upgrade. I discard
an earlier fixture whose smaller segments mixed compaction into the measurement.

I compare the original indexer from `12e58da` with direct format-4 repacking, using the same fixture,
format-5 bundler, checksum validation, and test harness. I pin both arms to CPUs 6–7, use two Rayon
workers and two Tokio workers, and build at nice 19 with two Cargo jobs, release debug information,
and LTO disabled. These are local-file measurements on a shared VM, with the OS page cache retained.
I use A/B/A/B order, resetting to the identical immutable source manifest before every arm.

| Metric | Rebuild from documents | Repack postings |
| --- | ---: | ---: |
| Timed upgrade seconds, median [range] | 62.73 [58.11–67.34] | 4.81 [4.28–5.34] |
| Fenced data publications | 32 | 1 |
| Whole-process CPU seconds, median | 64.94 | 7.66 |
| Whole-process instructions, billions | 277.98 | 13.09 |
| Peak RSS, median MiB | 1,985 | 1,009 |
| Exact output text bytes | 932,481,488 | 932,481,488 |
| overlap@10, 32 verification queries | 1.000 | 1.000 |

I get 13.0× wall throughput and 8.5× fields per CPU-hour, exceeding the 5× target. Instructions fall
21.2×. CPU time and instruction counts include startup and a final checksum scan over every output
field, with query verification disabled in those timed arms. RSS includes that scan too. I separately
compare all 932,481,488 payload bytes between complete old and new publications: every byte matches.
Another run compares every top-10 ID and score against the format-4 source on 32 queries. Unit tests
also preserve missing and empty fields, phrase positions, long postings, short postings, and lengths.

A sampled profile of the original pass puts 41.3% of its sampled cycles in `SegmentBuilder::add_document`.
The old path decodes document columns, tokenizes them, rebuilds postings, and publishes each field
and each ID backfill separately. I now copy existing compressed postings and positions through the
format-5 bundler, process fields sequentially within each segment, and run segments concurrently
under an estimated memory budget. Up to 32 segments and their ID maps share one publication.
Formats 1–3 retain the document rebuild solely for automatic migration. I add no layout setting,
format version, or bucket-fence generation.

I keep the reproduction harness in `crates/bf-engine/src/ingest_benchmark.rs`. I compile the original
and current engines with the same harness and the test-only `bf-text/legacy-fixtures` feature, then
use its `build-text` and `upgrade` actions against a disposable `IN1_RUN` under `.scratch/`. I set
`IN1_SEGMENTS=16`, `IN1_SEGMENT_ROWS=60000`, `RAYON_NUM_THREADS=2`, and `TOKIO_WORKER_THREADS=2`.
Every invocation uses `taskset -c 6,7 timeout 1200`; timed arms additionally use `/usr/bin/time -v`
and `perf stat -e instructions,cycles`. I set `IN1_VERIFY=0` for timing and omit it for the separate
query comparison. The original and current arms reference identical source objects; uploaded outputs
remain available until I finish the byte comparison and remove the disposable store.


### Update memory diagnosis

I reproduce the memory growth with 300,000 NQ rows through the engine, using 1,024-dimensional f16
vectors, original titles and text, 1,000-row requests, and 100,000-row flushes. The original engine
peaks at 1,390 MiB for inserts and 3,378 MiB for overwrites. These diagnostic runs use host-sized
defaults, rather than the 12 GiB scope used for the HTTP comparison below.

I enable `bf-alloc/heap-profile` and collect jemalloc profiles during updates. One live-heap snapshot
attributes 59.6% of its bytes to old-document projections and 48.5% to their packed-vector reads;
these cumulative stacks overlap. The write preparer hydrates old rows even for unconditional
replacements, and the indexer later materializes every overwritten row of a source segment to
subtract statistics. Internal documents use f32 base64 for dense vectors: a 1,024-dimensional f16
vector expands from 2,048 stored bytes to a 5,464-byte string, before other document allocations,
packed read buffers, and cache admission. That work scales with overwritten rows and source segments.

I skip stored-row hydration for unconditional upserts. Patches, deletes, and conditional writes still
read the rows they need, including earlier in-flight groups, and UUID normalization stays shared
between both ID sets. I accumulate overwritten-row statistics in 4,096-row chunks, bypassing cache
admission. A current dense ANN membership map proves that every row has its float vector, so I count
its exact canonical JSON width from dimensions and the encoded field name, without fetching vectors.
Sparse, flat, i8, and older layouts use the ordinary decoder within the same row bound. Metadata
statistics use this bounded scan too. I move added and removed term maps into the merge worker instead
of cloning them, and use the merge result to decide whether to retain the previous statistics object.

I compare exact row counts, logical byte totals, and text statistics with materialized documents,
including dense, sparse, legacy, and deleted-row subsets. A regression makes old document and ANN
objects unavailable and verifies an unconditional replacement succeeds; other checks retain patch
conditions, conditional upserts, and metadata parity before and after indexing. One storage tradeoff
remains: an unconditional replacement of an indexed row can now append an identical value to the WAL.
Rows already in the tail retain their no-op comparison. Wire-level replacement semantics are unchanged.


### Resident tail adoption

After removing old-row projections, I profile the remaining update peak. A 681K-row HTTP-run snapshot
attributes 84.2% of live allocations to documents created by WAL replay, including 828 MiB from
checkpoint refresh. Maintenance can replace segment files without advancing the indexed WAL
checkpoint. The existing adoption helper rejects an equal checkpoint, so compaction-only publications
fall back to rebuilding the resident overlay from stored WAL payloads. That duplicates large buffers
and stalls writers while the namespace lock is held.

I adopt both advancing and equal checkpoints from a complete resident WAL history, retaining the
later document Arcs and trimming only writes covered by the checkpoint. I reject older publication
generations, gaps, copy overlays, and checkpoints beyond the locally known sequence. Capped reads
retain their storage-replay path and cap enforcement. Adoption also carries the compact-reader flag,
so a later text request still opens full headers. This changes checkpoint adoption, rather than
compaction selection or tiers.

I verify Arc identity across a maintenance file replacement, exact trimming across an advancing
checkpoint, rejection of stale generations and copy histories, capped snapshots, and actual query
results. A counted object-store regression confirms a changed document-file key triggers zero reads
of the already resident WAL payload. All 167 engine/text tests and compatibility in both modes pass.


### NQ HTTP comparison

I compare `12e58da` with the final memory implementation at `09f3fb1` in two alternating old/new pairs.
I load the first 681,468 prepared NQ passages into a fresh local prefix per arm, using their original
Cohere vectors, titles, and text, then overwrite every existing row with the same vector and text and
an appended title suffix. The source namespace is this 681K-row slice, rather than the full 2.68M rows
from the original report. I use the real HTTP server and `bench/nq/load.py`, 5,000-row columnar
requests, four clients, default server tuning, two Tokio workers, and two Rayon workers.

I pin servers, clients, and profiling to CPUs 6–7 inside a 12 GiB scope. Both release builds have heap
profiling and debug information enabled, with LTO disabled, and use nice 19 and two build jobs. I set
jemalloc sampling to 2 MiB, log allocator statistics once per second, and retain the OS page cache.
I finish other local checks before the measured arms; other workers still share the VM.

After each phase I wait, with a timeout, for the immutable manifest's WAL checkpoint to equal the WAL
head and its row count to equal 681,468. Acknowledgement rates come from the client; indexed rates
include that drain. RSS comes from the server's `/proc` high-water mark, sampled after each phase.
The overwrite high-water mark is cumulative over both phases. I report the median and full range
of two runs per implementation.

| Metric | Before | After |
| --- | ---: | ---: |
| Insert acknowledgement rows/s | 5,181 [4,955–5,407] | 5,735 [5,602–5,867] |
| Insert rows/s through full indexing | 4,601 [4,368–4,833] | 5,033 [4,903–5,163] |
| Insert peak RSS, GiB | 2.90 [2.89–2.91] | 2.78 [2.59–2.97] |
| Overwrite acknowledgement rows/s | 3,590 [3,160–4,019] | 3,940 [3,877–4,003] |
| Overwrite rows/s through full indexing | 3,149 [2,839–3,459] | 3,382 [3,357–3,407] |
| Peak RSS after overwrites, GiB | 6.40 [6.26–6.53] | 3.14 [3.09–3.19] |
| RSS immediately after overwrite drain, GiB | 4.52 [3.91–5.14] | 0.67 [0.64–0.70] |
| Whole-server instructions, trillions | 3.123 [2.949–3.296] | 3.136 [3.076–3.197] |
| Whole-server CPU seconds | 614.62 [577.87–651.37] | 616.48 [594.71–638.25] |

I reduce the median overwrite peak by 51%, from 2.20 times the insert peak to 1.13 times it. Insert
acknowledgement throughput is 10.7% higher in this local sample. The overwrite-throughput ranges and
combined CPU counters overlap, so these runs establish the memory improvement without establishing
a general CPU-throughput gain. Whole-server counters cover both phases, drains, startup, and the
projection checks. Default background scheduling produces 9–14 initial segments and 7–16 segments
after overwrites; I keep compaction policy unchanged throughout the comparison.

I check IDs 0, 100,000, and 681,467 after each measured overwrite phase: every title has the suffix,
and every returned base64 vector matches its original stored f16 bytes exactly. All four arms finish
at 681,468 indexed rows. I make no full-namespace, 100M, or in-region S3 claim from this slice.

I reproduce the server builds with:

```sh
source ~/.cargo/env
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_LTO=false CARGO_INCREMENTAL=0 \
  nice -n 19 cargo build --release -p blowfish --features heap-profile -j 2
```

I run each server against a fresh `.scratch/` prefix in a `MemoryMax=12G` user scope, with
`taskset -c 6,7`, `RAYON_NUM_THREADS=2`, `TOKIO_WORKER_THREADS=2`, `BF_MEM_LOG_SECS=1`,
`BF_HEAP_DUMP_SECS=30`, a scratch heap directory, and
`_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:21`. I collect server instructions and cycles with
`perf stat -e instructions,cycles`. The client commands use the prepared data directory and an
isolated localhost port:

```sh
taskset -c 6,7 timeout 1800 uv run --project bench/nq python bench/nq/load.py DATA \
  --base-url http://127.0.0.1:PORT --namespace in1 --rows 681468 --batch 5000 --inflight 4
taskset -c 6,7 timeout 1800 uv run --project bench/nq python bench/nq/load.py DATA \
  --base-url http://127.0.0.1:PORT --namespace in1 --rows 681468 --batch 5000 --inflight 4 \
  --title-suffix ' IN1 updated'
```

I wait for the checkpoint after each command, capture the server high-water mark, verify projections,
and stop its exact PID. I delete each disposable store after its arm, and remove inputs, profiles,
logs, build snapshots, and scratch output after preserving these numbers.

### Failed upgrade groups

I also wait for every launched segment job when a text conversion fails. Blocking conversion work
cannot be cancelled by dropping its future; returning early could let a retry spend the same budget
while the previous job still runs. A regression checks that a failed group drains its launched peer,
leaves uploaded objects for GC, and adopts no partial results. This error-path correction does not
change the normal NQ workload above, whose text objects and ID maps are already current.

## BM25 round 2: exact MaxScore and hybrid concurrency

I reload the first 1,000,000 MS MARCO passages and use the same 1,000 checked-in dev queries,
strong consistency, default BM25 parameters, and exhaustive top-10 truth as the previous BM25
round. Both arms read one immutable format-5 file store, compacted into six segments of 105,000,
160,000, 200,000, 200,000, 200,000, and 135,000 rows. There is no reindex or layout difference
between the measured arms. The baseline is `3f2c120`; the implementation is `94c2c77`, `2e30d1d`, and `dba02d0`.

I build both arms with the production release profile: thin LTO, four codegen units, and debug
level 1. Builds use nice 19 and two jobs, and finish before measurement. I pin servers, clients,
and perf to CPUs 6–7 at nice 10, with two Rayon workers and two Tokio workers. The block cache
is 1 GiB, with 4,000-byte blocks, 512-block coalescing, and fetch concurrency 512. I keep the OS
page cache. This is a local measurement on a shared VM, with a reserved quiet window.

Each warm arm starts a fresh server, makes one unmeasured pass, and then measures all 1,000 queries
at one and 16 clients over keep-alive HTTP. I run A/B/A/B, attaching `perf stat -e instructions,cycles`
only during each measured pass. Counts cover the server's threads and exclude the client. I use
instruction counts as the primary speed evidence and paired wall times as supporting evidence.
The tables give the median of two runs; I also report the individual warm p50/p90 pairs.

### Diagnosis and changes

The initial `perf record -g` profiles at one and 16 clients attribute about 37% of samples to the
search loop and 8% to posting decode. Long queries with many common words dominate the tail.
A posting frame can span many row windows: broadcasting its best posting to every window makes
unrelated rows look competitive, even when sparse terms identify only a few useful candidates.

- I keep query terms in canonical order and address frames and windows directly. Posting batches
  use sorted vectors, avoiding repeated tree lookups and directory searches. Auxiliary window
  metadata is bounded, including for broad prefix expansion.
- I use block-max MaxScore to generate candidates from essential terms. Low-impact common terms
  score only those candidates, using row seeks when a frame has few competitive rows. Strict
  comparisons retain score ties, and final additions keep the reference f32 order.
- I cache normalization maxima at the actual row windows represented in decoded dense frames.
  Windows without a posting contribute zero. These bounds are independent of IDF and query
  frequency, and reset with average length, k1, or b. Admission is capped at 8 MiB per reader,
  with a conservative 16 MiB cache charge. Short decoded lists accumulate their scores once.
- I retain validated dense frames in an 8 MiB CLOCK cache, with a separate 8 MiB allowance
  protecting sparse lists. Sparse partial bounds use f32 storage and a conservative rounding
  allowance for both accumulation stages. Actual scores retain the reference f32 operation order.
- I share a monotone score floor across a query's disjoint live segments. A segment publishes only
  after finding k real hits; other segments can then reject strictly lower bounds. I also apply
  the engine's existing fanout rule to BM25, reducing worker handoffs under concurrent load.
- I run independent multi-query branches together, and let RRF's ANN branch start while text
  statistics load. Explicit `include_attributes: ["id"]`, `[]`, and `false` use compact IDs and
  avoid document-column reads in both opening metadata and result hydration.

The engine selects this plan automatically. Segment formats and bucket fence generation stay
unchanged. The text crate adds `SearchThreshold::new(k)` and `search_prefetched_shared` for merging
exact top-k contributions across disjoint live segments.

### Results

| Metric | Baseline | New |
| --- | ---: | ---: |
| Warm c1, p50 / p90 | 18.4 / 42.4 ms | 11.3 / 21.8 ms |
| Warm c16, p50 / p90 | 311.4 / 393.9 ms | 149.2 / 236.8 ms |
| Warm c1 throughput | 44.8 queries/s | 75.8 queries/s |
| Warm c16 throughput | 50.2 queries/s | 99.9 queries/s |
| Server instructions/query, c1 | 118.31 million | 32.41 million |
| Server instructions/query, c16 | 118.58 million | 30.60 million |
| Peak server RSS after c16 | 582 MiB | 640 MiB |
| Fresh-process facade, p50 / p90 | 257.6 / 372.0 ms | 250.9 / 365.7 ms |
| Cold reads/query, mean | 115.47 | 113.71 |
| Cold MB/query, mean | 3.636 | 3.624 |
| Cold waves/query, mean | 2.98 | 2.99 |
| overlap@10, all 1,000 queries | 1.000 | 1.000 |

Server instruction counts fall 72.6% at one client and 74.2% at 16 clients. Median throughput
at 16 clients is 1.99× the baseline. The extra decoded data costs about 58 MiB of peak RSS in
this workload. Cold latency stays within the observed variation, with essentially flat bytes
and waves.

Wall times vary substantially on this VM. I retain the paired observations below instead of
presenting the fastest pass as a reproducible machine limit. The summary above is the median
of each pair, and the instruction counts are the stronger evidence for the change.

| Client count | A1 p50 / p90 | B1 p50 / p90 | A2 p50 / p90 | B2 p50 / p90 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 14.5 / 33.7 ms | 9.9 / 19.1 ms | 22.2 / 51.1 ms | 12.7 / 24.4 ms |
| 16 | 292.6 / 364.1 ms | 135.8 / 211.2 ms | 330.1 / 423.8 ms | 162.5 / 262.5 ms |

Cold p90 spans 363–381 ms in the baseline and 359–373 ms in the new arm. Warm p50 is below
13 ms in both repeats; **the 18 ms warm p90 target remains open**. I do not establish an
in-region latency result from this local facade.

The worst long, common-word queries remain expensive. Query 672 falls from 149.7 to 76.0 ms,
query 769 from 101.8 to 44.7 ms, and query 439 ("what is the gas called that they give you at
the dentist") from 89.5 to 53.1 ms, using each query's median across the two c1 passes.
Final sampled profiles still spend about 30–33% in search and 6% initializing frame-cache
slots; decode falls to about 3%. Avoiding dense-frame probes before window pruning is a
remaining profiling lead.

All 12,240 measured and profiled BM25 responses, including both client counts and all cold
repeats, match the exhaustive top-10 IDs and baseline scores exactly. There are 1,000 unique
queries. The baseline binary SHA-256 is
`c4bccdcbe01117f9cd8943255f98eb0736ae83fd6208cf6783949370e688611e`; the final binary is
`ffc75d69c3f893f432e5ffe6ba8c669e69ac2e9ab38f0d0b1cf5a627c801827f`.


### Hybrid measurement

I load 100,000 BEIR NQ rows with their 1,024-dimensional f16 embeddings, titles, and text, and use
the first 200 aligned test query vectors and texts. I measure BM25 and ANN separately, a multi-query
returning both lists, and a multi-query with server-side RRF at limit 10. Each subquery requests
ten IDs. These runs use the same cache, CPU, strong-consistency, warm-up, and A/B/A/B procedure.
The cold RRF samples use the first 30 queries and a fresh process per query through the same
25 ms facade.

| Warm NQ workload | Baseline p50 / p90 | New p50 / p90 | Instructions/query, before → after |
| --- | ---: | ---: | ---: |
| BM25 | 7.0 / 10.8 ms | 4.8 / 7.4 ms | 15.27 → 6.59 million |
| ANN | 9.1 / 16.9 ms | 8.8 / 18.2 ms | 13.31 → 12.36 million |
| Two-list multi-query | 14.8 / 22.6 ms | 12.0 / 15.8 ms | 27.05 → 18.46 million |
| Multi-query with RRF | 12.3 / 18.8 ms | 10.5 / 14.0 ms | 27.11 → 18.40 million |

The two-list hybrid uses 31.8% fewer instructions, and RRF uses 32.1% fewer. ANN is a control;
I change no vector index, tier, or distance kernel, and its wall-time movement is inconsistent.
Both hybrid forms return the same IDs and scores in every repeat. I independently fuse the
two lists with rank constant 60 and verify the server's RRF result, including every cold sample.

| Fresh-process NQ RRF through the facade | Baseline | New |
| --- | ---: | ---: |
| p50 / p90 | 335.1 / 391.5 ms | 302.8 / 366.0 ms |
| Reads/query, mean | 233.05 | 192.93 |
| MB/query, mean | 12.467 | 10.443 |
| Waves/query, mean | 2.75 | 2.37 |

The concurrent statistics read and ID-only projection remove a serial dependency and document
reads. Cold RRF reads fall 17.2%, and bytes fall 16.2%. These figures apply to this 100K NQ
fixture; I do not extrapolate them to the 1M MS MARCO workload.


### Validation and reproduction

Formatting, clippy with all targets and warnings denied, and all 175 tests in `bf-text` and
`bf-engine` pass; three benchmark tests remain ignored. Regressions cover long posting lists,
normalization changes, broad prefixes, concurrent shared thresholds, live-row masks, ties,
indexed/tail hybrid results, exhaustive RRF parity, protected cache budgets, rounded sparse
partials, and ID-only reads. Compatibility passes 121/121
in normal and forced indexing modes, plus 4/4 SDK retry checks and TypeScript smoke and retry checks
in each mode.

I prepare the disposable MS MARCO store and exhaustive truth using the preceding BM25 round's
loader and scorer. Both binaries use the same completed format-5 store. I keep each binary
separately, and repeat these commands in A/B/A/B order with distinct output names. `RUN` and
`STORE` are owned directories under `.scratch/`, and `PERF` names an installed perf executable.

```sh
source ~/.cargo/env
nice -n 19 cargo build --release -p blowfish -j 2
# BINARY selects one separately built production-profile arm.
timeout 1800 taskset -c 6,7 nice -n 10 uv run bench/scale/bm25_local.py warm \
  --binary "$BINARY" --store "$STORE" --perf "$PERF" --profile-dir "$RUN/stat" \
  > "$RUN/warm.jsonl" 2> "$RUN/warm.log"
timeout 1800 taskset -c 6,7 nice -n 10 uv run bench/scale/bm25_local.py warm \
  --binary "$BINARY" --store "$STORE" --perf "$PERF" --perf-record \
  --profile-dir "$RUN/stacks" > "$RUN/profile.jsonl" 2> "$RUN/profile.log"
timeout 360 taskset -c 6,7 nice -n 10 uv run bench/scale/bm25_local.py cold \
  --binary "$BINARY" --store "$STORE" --queries 60 > "$RUN/cold.jsonl" 2> "$RUN/cold.log"
uv run python bench/scale/bm25_overlap.py "$RUN/truth.json" "$RUN/warm.jsonl"
# DATA is prepared with bench/nq, and NQ_STORE contains the first 100,000 indexed rows.
timeout 120 uv run bench/scale/nq_hybrid_workload.py "$DATA" "$RUN" --queries 200
timeout 180 taskset -c 6,7 nice -n 10 uv run bench/scale/bm25_local.py warm \
  --binary "$BINARY" --store "$NQ_STORE" --namespace nq --queries 200 --clients 1 \
  --workload "$RUN/nq-rrf.json" --perf "$PERF" --profile-dir "$RUN/rrf-stat" \
  > "$RUN/rrf.jsonl" 2> "$RUN/rrf.log"
```

The harness terminates its exact server and profiler PIDs, and removes temporary server logs.
I remove input copies, disposable stores, profiles, and build snapshots after preserving these
numbers. The facade models fixed dependent round trips and local transfer; I have not measured
this revision against an in-region S3 bucket or hosted turbopuffer.

## BM25 round 3: fewer posting probes, and a Mac baseline

I load the first 1,000,000 MS MARCO passages on a 10-core Apple M4 Mac mini (16 GiB RAM, macOS 26.2) into one immutable format-5 store of six segments (200,000, 200,000, 5,000, 200,000, 200,000, and 195,000 rows) and run the 1,000 checked-in dev queries at strong consistency against exhaustive top-10 truth. Both arms use the production release profile, two Rayon and two Tokio workers, a 1 GiB cache (4,000-byte blocks), and a retained OS page cache, changing only scalar Rust in `bf-text` with formats, public signatures, and the bucket fence unchanged. The baseline is `e49c509` (`v0.1.15`); the implementation is `3b5f06f`, `08130a9`, and `155b007`.

### Changes

- I probe dense decoded frames after window selection, skipping hashes, clones, retention, and CLOCK refreshes on rejected windows while keeping early sparse probes.
- I cache query-independent normalization summaries by term and row window inside the 8 MiB admission budget, invalidating on new frames or `k1`, `b`, and average-length changes, with an empty-window prefix sentinel.
- I prefetch postings in one row-order sweep across selected windows, keeping descending-bound window order and canonical term addition order in scoring.
- I skip redundant posting binary searches via directory endpoints, searching only remaining suffixes.

### Results

| Metric | Baseline | New |
| --- | ---: | ---: |
| M4 warm c1, p50 / p90 | 1.702 / 3.096 ms | 1.468 / 3.001 ms |
| M4 warm c16 throughput | 505.8 queries/s | 520.3 queries/s |
| M4 cold 25 ms facade (60q), p50 / p90 | 206.3 / 245.4 ms | 206.2 / 240.6 ms |
| VM instructions/query (200q), c1 | 28.870 million | 26.558 million |
| VM instructions/query (200q), c16 | 26.851 million | 25.278 million |

- Across two M4 A/B/A/B sequences (four warm passes per arm), c1 p50 drops 13.7%, p90 drops 3.1%, and c16 throughput rises 2.9% (`24.61 / 56.36` to `23.96 / 55.07 ms`). Cold I/O stays flat across one 60-query A/B/A/B pass at `116.88–116.97` reads, `3.598–3.599 MB`, and `3.417–3.442` waves.
- On the VM (CPUs 6–7, nice 10, 200-query A/B/A/B under `perf stat` on the copied store), median server instructions fall 8.0% at c1 and 5.9% at c16. I omit VM wall times because other work was running; VM claims are instruction counts.
- All 17,840 checked responses (16,240 M4 and 1,600 VM) match exhaustive top-10 IDs and `f32` score bits (`overlap@10` `1.0000`).

### Limits

- On the M4, the baseline already beat turbopuffer's published warm 13 / 18 ms, so I make no claim of crossing that threshold, comparing on equal hardware, or bringing the earlier 21.8 ms p90 (measured on two shared VM CPUs) below 18 ms.
- Posting-slice searches, candidate generation, and scoring still dominate long, common-word queries, and some M4 c1 tail medians rise slightly (query 672: `16.13` to `16.83 ms`; 182: `10.52` to `10.87 ms`), though 769 drops from `5.84` to `5.41 ms`.
- The 200-query VM pass is not comparable to round 2's 1,000-query mean, and candidate c1 counts vary (`27.509` vs. `25.606` million) with segment-floor ordering under contention.
- Other applications stayed open on the unpinned Mac, and the fixed 25 ms/request S3 facade makes no in-region cold claim.

## Apple M4 at v0.1.15: blowfish, Qdrant, and LanceDB

I reran the vectors-only NQ comparison on a 10-core Apple M4 Mac mini with 16 GiB of RAM and macOS 26.2.
I built blowfish from tag `v0.1.15` (`e49c509`), in the native release profile, without `local-models`.
I used Qdrant 1.20.0's official macOS arm64 binary and LanceDB 0.41.0, the same versions as the
[Iteration 19 Mac run](#on-a-mac-blowfish-qdrant-and-lancedb-apple-m4).

I kept that run's limits: all 10 cores shared by engine and clients, no CPU pinning, no process memory cap,
and a 4 GiB blowfish block cache with its disk cache disabled. macOS swap stayed enabled, and the desktop's
background applications remained running. I ran one engine at a time, on local disk, and deleted its data
before starting the next.

The workload is the same 2,681,468 NQ passages, 3,452 queries, and 1,024-dimensional Cohere embed-english-v3
vectors stored as f16, with cosine distance. Every engine gets id and vector only. I use `bench/nq/` and
measure recall@10 against its brute-force exact truth at limit 10 for all three engines. Warm latency is
1,000 queries at one client after one 1,000-query warm-up pass. Throughput is the first 2,000 queries, with
pre-encoded HTTP request bodies for blowfish and Qdrant, at 1, 4, and 16 clients. LanceDB runs in-process
with client threads. I added p90 output to both measurement scripts.

### Query results

| Engine, setting | Recall@10 | p50 / p90 / p99 ms, 1 client | q/s at 16, mean (range) |
|---|---:|---|---|
| blowfish v0.1.15, 64 probes (default) | 0.9611 | 1.3–1.4 / 1.6 / 1.9–2.0 | 1,814 (1,801–1,827) |
| blowfish v0.1.15, 48 probes | 0.9512 | 1.2 / 1.4 / 1.7–1.8 | 2,101 (2,075–2,128) |
| Qdrant 1.20.0, hnsw_ef 64 | 0.9497 | 0.5 / 0.6 / 0.7 | 7,752 (7,481–8,023) |
| Qdrant 1.20.0, hnsw_ef 128 | 0.9696 | 0.7 / 0.9 / 1.0 | 5,451 (5,444–5,459) |
| LanceDB 0.41.0, 80 probes, refine 10 | 0.9249 | 13.0–13.2 / 13.3–13.5 / 13.9–14.5 | 393 (390–396) |
| LanceDB 0.41.0, 120 probes, refine 10 | 0.9489 | 18.6–18.7 / 19.0–19.1 / 19.8–19.9 | 278 (276–279) |

I measured blowfish in 64, 48, 64, and 48 probe order, restarting the process and warming it each time.
At 64 probes, the two 16-client passes gave 1,827.0 and 1,801.1 q/s; at 48, 2,074.6 and 2,127.9 q/s.
I also repeated both Qdrant settings and both LanceDB settings. The table gives the range of each latency
percentile across two passes and the mean throughput, with its range. I ran quality in the first blowfish
and Qdrant pair, and in both LanceDB passes; LanceDB's recall repeated exactly. Iteration 19 scored blowfish
at limit 100; Qdrant and LanceDB were scored at limit 10 then too.

Qdrant uses HNSW m=16, ef_construct=100, and f16 vectors in RAM. I kept the optimizer target of one segment
and max_segment_size=100,000,000. When I measured, it reported green status, all 2,681,468 vectors indexed,
an empty update queue, and **two physical segments**. LanceDB uses IVF-PQ with 1,600 partitions,
128 sub-vectors, and refine factor 10, at 80 and 120 probes. Its newly built index has lower recall than
Iteration 19's, so I report this run's measured quality alongside its speed.

At recall around 0.95, blowfish at 48 probes serves about **7.6 times** LanceDB's throughput at 120 probes
(2,101 against 278 q/s), and about **27%** of Qdrant's at ef 64 (7,752 q/s). The default 64 probes gives
higher recall, 0.9611, at about 1,814 q/s. The algorithms and client paths differ: LanceDB pays no HTTP or
JSON cost, and Qdrant uses HNSW over RAM-resident vectors.

### Load, automatic recluster, and memory

I loaded blowfish through the HTTP API with `load.py --vectors-only`: 5,000-row batches, four requests
in flight, and default indexing, compaction, and automatic recluster settings. I waited for the tier
covering all rows to be installed and indexing to become idle before measuring queries.

The HTTP load took **89.9 s** (29,822 rows/s). The automatic job installed a 225,000-row tier in 11.1 s,
then a 545,000-row tier in 105.9 s, starting both during ingest. The bulk-load guard permits another build
when uncovered rows exceed covered rows, so these intermediate tiers are part of the default behavior.
The final build covered all 2,681,468 rows in 19 segments, with 32 slabs and 13,392 leaves, and took
**580.9 s**. It started about 35 s after the last write and installed about 616 s after it; from the first
write to the final tier was about **706 s**. Vector copying took 191 s of that final build. These times
overlap, so I do not add the intermediate builds to the load time.

| Engine | Load or write | Index or recluster | Peak RSS, decimal GB |
|---|---|---|---|
| blowfish | 89.9 s | 11.1 s and 105.9 s intermediate builds; 580.9 s final build | 2.73 load/build; 2.69–3.34 queries |
| Qdrant | 343 s | 270 s | 14.73 across load, index, and queries |
| LanceDB | 18 s | 56 s | 8.82 build; 0.60–0.61 queries |

I measured process maxima with `/usr/bin/time -l`. One-second RSS samples during blowfish's final full-corpus
build peaked at 1.54 GB; its whole load/build process peaked at 2.73 GB. Qdrant's process maximum includes
indexing, while its query RSS samples peaked at 5.89 GB. Qdrant's indexing time includes the harness's
30-second readiness polling. The old blowfish quality pass requested more results and could fill more
cache, so the RSS difference from Iteration 19 is not a controlled memory comparison.

Against the historical M4 run, default-probe throughput rises from 870 to about **1,814 q/s**, with recall
rising from 0.952 to 0.961. The 1-client p50 falls from 2.8 to 1.3–1.4 ms, and p99 from 4.1 to 1.9–2.0 ms.
At 48 probes I now measure recall 0.951 rather than 0.940. I rebuilt the indexes, so this comparison
includes layout and build differences.

Time to the final tier is longer: about 706 s from the first write here, against the old 139 s load plus
334 s manual recluster. The automatic job uses half the cores and made intermediate tiers while this
load ran; I have not isolated those effects from the other changes since Iteration 19. I would use the
query improvement and the time to a final tier together when comparing releases. All of these numbers
are warm, with local-disk storage on a shared Mac.

## v0.1.14 in-region

Rerun begun on 2026-11-23, using only the archived `v0.1.14` release
(`e6fbc8fcd806fdf16efc75dd6b82a14fa145835b`) for Rust sources and binaries.
Copying, upgrades, GC, all required query measurements, and AWS cleanup are complete.
This run measures existing data after the release's automatic format upgrades,
without retraining, reclustering, or changing segment and slab geometry.

### Checkpoint copy and upgrade method

The original deployment prefixes remain untouched. A server-side S3 copy selected
the latest full manifest, its reader copy, referenced live files, matching ANN
hints, and deployment format markers. Retired segments and checkpointed WAL were
excluded; no namespace had an uncheckpointed WAL tail. Source stability was
checked again after each copy. This is a quiescent benchmark checkpoint copy;
production backup guidance remains in `operations.md`.

The destination preserves deployment-relative keys under `v014/scale` and
`v014/big`. All writable opens and upgrades use those copies.

| Namespace | Copied objects | Copied bytes | Copy wall seconds | Segments / slabs |
| --- | ---: | ---: | ---: | ---: |
| Wikipedia 10M | 632 | 81,713,485,245 | 73.540 | 70 / 32 |
| MS MARCO 1M | 27 | 1,133,003,880 | 3.101 | 5 / 0 |
| Wikipedia 100M | 16,414 | 838,308,354,467 | 783.738 | 1,787 / 160 |
| Total | 17,073 | 921,154,843,592 | | |

The release-tag `blowfish reindex` CLI converted vector indexes first. A writable
release-tag benchmark server then upgraded text indexes and backfilled text ID
maps. The copied text indexes were format 3 at 10M and MS MARCO, and format 4
at 100M; all reached format 5. Upgrades used a 10 GiB cgroup, no swap, a 1 GiB
storage cache, a 512 MiB
decoded-cache budget, six Rayon threads, one builder, and a segment-row cap of
one to prevent compaction. One-day retention preserved rewrite inventory before
normal garbage collection. Neither original deployment prefix was opened by a
writable process.

| Namespace | Vector wall seconds | Vector peak RSS bytes | Index/fragment bytes written | Converted segment indexes / slabs |
| --- | ---: | ---: | ---: | ---: |
| Wikipedia 10M | 385.710 | 3,368,763,392 | 48,153,920,416 | 69 / 32 |
| MS MARCO 1M | 0.501, no-op | Not sampled meaningfully | 0 | 0 / 0 |
| Wikipedia 100M | 9,379.929 | 5,494,411,264 | 483,798,730,044 | 1,761 / 160 |

One 10M segment and 26 100M segments have flat indexes that do not require the
fast-scan conversion. The 100M CLI completed 364 publications, with no superseded
work. External wall time includes CLI startup and final reporting; the CLI's
internal vector times were 385.2 seconds and 9,378.8 seconds, respectively.

The shared writable server completed the five MS MARCO text fields and ID maps
after 96.327 seconds and the 70 Wikipedia 10M fields and maps after 742.119
seconds. Corresponding whole-process RSS high-water bounds were 1,899,151,360
and 2,720,260,096 bytes. These elapsed times begin at the same server start;
work alternated between namespaces, and RSS is a shared-process bound. New
stored objects totaled 734,155,049 bytes for MS MARCO and 57,806,052,764 bytes
for 10M. This inventory counts keys absent from the initial copy at their final
stored size; the CLI byte column counts index and fragment output only.

The first spot instance was terminated by AWS for lack of capacity after 6.69
hours, with all vector conversions and 782 of 1,787 text fields durably committed.
A sequential on-demand replacement reuses byte-identical release binaries.
The copied namespace retained all 100M rows, segments, and slabs. No query
measurements had run before the interruption. Text timings below identify
the interruption and include its downtime.

The 100M text phase completed at 03:48 UTC on 2026-11-24: about **11 h 33 m**
end to end, including the spot interruption and recovery, or **11 h 24 m** of
estimated active episodes. The resumed episode took **7 h 59 m 38 s**. The origin of
the interrupted episode is reconstructed from paired clock samples, with a
combined timing uncertainty of about 45 seconds. Maximum observed whole-process
RSS across both text writers was **2,808,823,808 bytes**. All 1,787 fields reached
text format 5, all ID maps were present, and all 100M rows, 1,787 segments, and
160 slabs were preserved.

The 100M upgrade created **21,360 new stored objects totaling 840,226,569,560
bytes**. By suffix, this includes 181,075,547,430 bytes of JSON metadata,
67,850,049,052 bytes of directory files, 107,501,991,227 bytes of text indexes,
and the rewritten vector indexes and fragments. This inventory counts keys absent
from the initial copy, including lease records and newly introduced fixed keys,
once at their final stored size. It measures additional stored objects rather
than cumulative PUT traffic. Inventory includes the portable
24-byte object headers; the CLI's vector byte counter measures logical payloads.
Repeated writes of fixed-key hints and reader copies are outside this new-key
inventory. One extra text-index object from the interrupted attempt was left
unreferenced and subsequently reclaimed.

Before GC, the full 100M manifest was **97,125,315 bytes of JSON**, or
**46,542,105 encoded bytes**, versus the original 11,291,307 bytes of JSON. The
indexer publishes a field conversion and its ID map separately, each with a
fenced claim and commit. At this geometry, those steps require at least 7,148
full manifest-slot writes. Repeated manifest work is a plausible contributor to
migration time; its CPU share was not profiled. The 100M source also has 25.5
times as many segments as the 10M source, so these upgrade times are not a
constant-geometry scaling comparison.

Normal release-tag GC then reclaimed **705,945,354,747 bytes** from the copied
roots, taking 22.5 seconds for the shared scale root and 78.4 seconds for the
100M root. No unreferenced reclaimable objects remained in the scanned data,
slab, legacy-payload, and lease prefixes. Retirement records for objects omitted
from the checkpoint copy can remain in the manifest ledger. Permanent manifest
history is retained. The resulting copy has **27,202 current objects totaling
1,113,976,881,229 bytes**, approximately **$23.86/month** at the checked S3
Standard rate of $0.023/GiB-month. This is an estimate for current listed
objects; IAM denies independent bucket-versioning and object-version inventory
reads, while newly written object HEAD responses contain no version ID.

| Namespace | New keys before GC | New-key bytes before GC | Current objects after GC | Current bytes after GC |
| --- | ---: | ---: | ---: | ---: |
| Wikipedia 10M | 965 | 57,806,052,764 | 1,061 | 82,945,842,525 |
| MS MARCO 1M | 43 | 734,155,049 | 57 | 1,148,811,811 |
| Wikipedia 100M | 21,360 | 840,226,569,560 | 26,082 | 1,029,882,226,805 |

Namespace rows exclude two deployment format markers totaling 88 bytes.

### Measurement method and initial network floor

The main host is an actual `c7gd.2xlarge` in the S3 bucket's region. Queries use
strong consistency, `top_k=10`, 4,000-byte cache blocks, 512-block coalescing,
512 concurrent fetches, an 8 GiB storage-cache budget for cold arms and 10M warm arms, a 512 MiB
decoded-cache budget, eight Rayon threads, and the existing ANN oracle workloads. Eight threads
match the full-host default used by the latest iteration-18 comparison; the
earlier in-region briefs forced six. The 100M server
is constrained to a 10 GiB cgroup with swap disabled. No maintenance runs during
query measurements.

The first 100M warm prewarm at the historical 8 GiB payload-cache setting was
OOM-killed under the 10 GiB cgroup. At the kill, the server alone had
10,605,543,424 bytes of anonymous RSS; the cgroup also included the Python client.
A payload-cache budget is not a whole-process RSS cap. The 100M warm reruns use
**7 GiB of payload cache under the same 10 GiB limit**, with all other query
settings preserved. These warm results therefore include a cache-budget change
relative to the historical comparison. The cold arms retain the 8 GiB setting.
No timing or recall result is reported for the failed prewarm.

Each cold arm uses 50 distinct queries. Fresh mode starts a new read-only server
and empty application disk cache for every query. Cache-cold mode drops memory,
disk, decoded metadata, and namespace caches on a warm process before each
query. Operating-system caches are not explicitly flushed. Warm c1 runs use
1,000 requests after a complete workload prewarm, and recall uses all 200 ANN
oracle queries. The 10M throughput runs use 16 clients and 1,000 timed requests
after prewarming. Warm throughput excludes prewarming; fresh-run wall time also
includes process lifecycle and is not reported as query throughput.

BM25 uses the existing 1,000-query MS MARCO workload. Independent exact top-10
answers were generated with the release-tag `bf-text` exhaustive scorer over the
first million source documents, taking 111.879 seconds; all queries had nonempty
answer sets. Overlap is the strict ID intersection divided by ten.

| 4 KB S3 GET, 200 requests | p50 ms | p90 ms |
| --- | ---: | ---: |
| Prior in-region run | 24.5 | 30.5 |
| v0.1.14 run, before upgrades | 25.0 | 31.2 |
| v0.1.14 replacement, before resumed upgrades | 25.9 | 32.4 |

All main-host query measurements use the replacement. Its 1 MB GET floor was
36.5 / 58.2 ms, versus 107.3 / 127.9 ms on the first host; the similar 4 KB
floor does not imply identical transfer behavior. The older documented in-region
1 MB floor was 30.2 / 36.1 ms. Separate-session comparisons include S3 and host
variance as well as release differences.

Historical comparisons use the documented v0.1.13-era results, with their
actual provenance retained. There is no v0.1.13-era 10M 64-probe result in this
notebook; the older 64-probe table is v0.1.12. The historical MS MARCO warm result
is round 11. These are separate-session comparisons, not an interleaved A/B.
Published turbopuffer 10M results use 768-dimensional vectors, whereas these
Wikipedia vectors have 1,024 dimensions at `f16` precision. The published 10M
recall@10 is 0.943; this run reports its own recall over the full 200-query oracle.


### Query results

Latency pairs are p50 / p90 in milliseconds. Historical ANN results are the
v0.1.13-era iteration-18 measurements. BM25 cold comes from the round-14
in-region check, BM25 warm is round 11, and the prior actual-4-GB result is
iteration 15. Their original provenance is preserved above and in the earlier
sections; these are not newly rerun v0.1.13 controls.

### 10M vector

| Probes and mode | turbopuffer published p50 / p90 ms | v0.1.13-era documented p50 / p90 ms | v0.1.14 p50 / p90 ms |
| --- | ---: | ---: | ---: |
| 48, fresh process | 874 / 1,214 | 680 / 907 | 625.5 / 924.3 |
| 48, cache-cold | 874 / 1,214 | 533 / 656 | 454.3 / 533.6 |
| 48, warm c1 | 14 / 17 | 36.6 / 50.8 | 18.2 / 25.7 |
| 64, fresh process | 874 / 1,214 | Not reported | 561.3 / 734.2 |
| 64, cache-cold | 874 / 1,214 | Not reported | 477.6 / 533.1 |
| 64, warm c1 | 14 / 17 | Not reported | 20.3 / 30.7 |

| Probes | Prior recall@10 | v0.1.14 recall@10 (200 queries) | v0.1.14 warm c1 queries/s |
| --- | ---: | ---: | ---: |
| 48 | 0.953 (rounded) | 0.9525 | 51.60 |
| 64 | Not reported | 0.9615 | 42.54 |

### 100M vector

v0.1.14 cold rows use 8 GiB of payload cache; warm rows use 7 GiB after the
8 GiB prewarm failed. All required rows use the 10 GiB cgroup.

| Probes and mode | turbopuffer published p50 / p90 ms | v0.1.13-era documented p50 / p90 ms | v0.1.14 p50 / p90 ms |
| --- | ---: | ---: | ---: |
| 48, fresh process | Not published | 1,058 / 1,253 | 1,163.8 / 1,435.5 |
| 48, cache-cold | Not published | 1,128 / 1,341 | 1,034.9 / 1,201.2 |
| 48, warm c1 | Not published | 72.6 / 90.4 | 112.1 / 164.6 |
| 64, fresh process | Not published | 1,243 / 1,370 | 1,251.6 / 1,429.1 |
| 64, cache-cold | Not published | 1,201 / 1,391 | 1,149.3 / 1,341.7 |
| 64, warm c1 | Not published | 99.7 / 162.8 | 150.0 / 228.5 |

| Probes | Prior recall@10 | v0.1.14 recall@10 (200 queries) | v0.1.14 warm c1 queries/s |
| --- | ---: | ---: | ---: |
| 48 | 0.9410 | 0.9410 | 8.29 |
| 64 | 0.9540 | 0.9540 | 6.14 |

### 10M throughput at 16 clients

| Probes | turbopuffer published | Prior in-region | v0.1.14 queries/s | v0.1.14 p50 / p90 ms |
| --- | --- | --- | ---: | ---: |
| 48 | Not published | Not reported | 216.14 | 72.3 / 86.9 |
| 64 | Not published | Not reported | 171.97 | 91.0 / 106.7 |

### MS MARCO BM25 1M

| Mode | turbopuffer published p50 / p90 ms | v0.1.13-era documented p50 / p90 ms | v0.1.14 p50 / p90 ms |
| --- | ---: | ---: | ---: |
| fresh process | 316 / 381 | 380 / 526 | 249.4 / 449.1 |
| cache-cold | 316 / 381 | 301 / 369 | 206.5 / 261.2 |
| warm c1 | 13 / 18 | 17.1 / 34.5 (round 11) | 11.2 / 14.2 |

Overlap@10 versus exact: 1.0000

### 10M on an actual 4 GB c7gd.large

| Probes and mode | turbopuffer published p50 / p90 ms | Prior 4 GB in-region p50 / p90 ms | v0.1.14 p50 / p90 ms |
| --- | ---: | ---: | ---: |
| 48, fresh process | 874 / 1,214 | 769 / 973 | 670.4 / 833.3 |
| 48, cache-cold | 874 / 1,214 | 623 / 740 | 536.3 / 637.1 |
| 64, fresh process | 874 / 1,214 | Not reported | 649.0 / 750.9 |
| 64, cache-cold | 874 / 1,214 | Not reported | 568.0 / 709.3 |

The actual `c7gd.large` check used two Rayon threads, a 1 GiB payload cache,
a 256 MiB decoded-cache budget, a 3.5 GiB cgroup, and no swap. Hardware exposed
3,897,240 KiB of RAM. Separate 200-query checks gave recall@10
`0.9525` at 48 probes and `0.9615` at 64 probes.
The maximum sampled server high-water RSS across its six arms was
1,860,599,808 bytes.
The old small-host comparison used 47 slabs; this preserved source has 32.
This rerun validates queries on 4 GB, not a new ingest or recluster run.

### Separate memory diagnostic, outside the required 10 GiB profile

After the required runs, one bounded 100M 48-probe warm control restored the
8 GiB payload cache under a **12 GiB cgroup**, with the same 200-query prewarm,
1,000 timed requests, decoded cache, and eight Rayon threads. It measured
**44.8 / 56.9 ms**, 21.52 queries/s, recall@10 `0.9410`,
and 11,246,977,024 bytes of server high-water RSS. It is separate from
the required 10 GiB results and does not establish that the 8 GiB cache fits
that limit: its measured server RSS alone exceeds 10 GiB. Restoring cache
capacity and memory headroom materially changed warm latency in this session.
This control helps distinguish the memory constraint from a release comparison;
a profiler would still be needed to attribute CPU and allocation costs.

### Interpretation, spend, and cleanup

At 10M and 48 probes, warm latency fell from the documented 36.6 / 50.8 ms to
18.2 / 25.7 ms, with full-set recall `0.9525`, versus the documented rounded
value of `0.953`.
Cold improvements vary by mode; 10M fresh p90 and 100M fresh latency did not
improve in this session. At 100M, the historical 8 GiB warm profile could not
complete under this run's 10 GiB cgroup, and the 7 GiB fallback was slower than
the documented prior warm results. The distinct memory diagnostic above should
not replace those constrained results. BM25 cache-cold and warm improved, with
exact overlap maintained. Separate-session variance and cache-budget differences
prevent attributing every change to the release alone.

| Run cost component | Estimated USD |
| --- | ---: |
| EC2 compute, all three sequential instances | 4.011 |
| Root EBS and public IPv4 time | 0.131 |
| Copied storage, peak-listed-size time bound | 0.852 |
| S3 request reserve, included once in this estimate | 5.000 |
| Egress reserve | 1.000 |
| Total including reserves | **10.994** |

Instance time was 6.690 hours on the interrupted
spot `c7gd.2xlarge`, 8.797 hours on its on-demand
replacement, and 0.191 hours on `c7gd.large`.
Checked on-demand rates were $0.3629/hour and $0.0907/hour, respectively;
the spot rate was about $0.12/hour. The storage bound applies the observed
peak of 1,819,922,235,976 bytes over the entire 15.732-hour
run. These are estimates, not a reconciled AWS invoice: S3 multipart request
amplification and exact billed request counts were not independently available.
The final estimate expands the egress reserve to $1.00 to cover binary,
manifest, and checkpoint-hint transfers whose exact bill was not reconciled.
The conservative projection, including that larger allowance, stayed below $16,
the $35 stop threshold, and the $40 approved budget. The retained copy's
approximately $23.86/month is future storage, separate from this run estimate.

EC2 describe calls confirmed all owned instances terminated, with no active
`Project=blowfish` instances or remaining tagged volumes. The owned key pair and
security group were deleted; the interrupted spot request is closed. No incomplete
uploads remain under `v014/`. Its retained count and bytes match the post-GC
inventory above. Final read-only checkpoint checks matched all three originals,
including selected live-object sizes and ETags and deployment format markers.
Neither original prefix nor the production process on this VM was modified.

## Round AR3: compaction through the tiers (lineage), and deltas beside a base build (2026-11-24)

NQ (2,681,468 rows, 768 dims in f16, cosine) on one shared 8-CPU VM: server on CPUs 0 to 5, clients on 4 and 5, local
file store. Other workers were running benchmarks at the same time (load average 10 to 15), so latencies are noisy.
The gate uses instruction counts (`perf stat -e instructions:u` on the server over 2,000 one-client queries, after
2,000 warm-up queries).

### State before compaction

v0.1.15 loaded NQ, built its base, then took 10 minutes of upserts at 1,000 rows a second (random existing rows, same
vector, new title). Since AR1 it never compacts covered segments: 51 segments, a base over 31 of them, a delta over
19 flush segments (625,447 rows), and one uncovered segment. Both sides below start from copies of that store, with
the write tail flushed by v0.1.15.

### Gate: before and after compaction

- **Before:** this branch, read-only, on the v0.1.15 store (format 3, nothing compacted).
- **After:** this branch, writable, until no manifest was published for 60 seconds, then read-only. It upgraded the
  bucket to format 4 and merged the delta's 19 flush segments with lineage: 51 segments became 33, with 25 lineage
  entries. Both tiers kept their directory keys, so nothing was rebuilt.

| Run | Instructions per query, before | After | Recall@10, both |
|---|---:|---:|---:|
| 1 (before, then after) | 23.17M | 23.09M | 0.9521 |
| 2 (after, then before) | 32.65M | 23.05M | 0.9521 |
| 3 | 44.19M | 42.90M | 0.9521 |
| 4 | 30.31M | 38.58M | 0.9521 |

On the least-contended runs (1 and 2), after is 0.5% below before. Runs 3 and 4 ran with the VM at load average 15:
counts rose on both sides, because idle runtime threads spin more under contention. Recall is identical in every run. A
read-only server in front of an unindexed 46,681-row write tail spent 3.2G instructions and 344
ms p50 per query, nearly all decoding and scoring the tail's base64 vectors. That is the WAL tail path, not lineage.

### Continuous writes

On the compacted store, the branch writable, with 10 minutes of upserts at a target of 1,000 rows a second (506,900
sent, about 850 a second on the loaded VM, none rejected):
- The masked-rows rebuild the trigger wanted at startup ran for 19 minutes (the VM was loaded). Beside it, three
  deltas over the rows written meanwhile built in 42 to 338 seconds and installed, each replacing the previous one.
  The rows outside the tiers therefore stayed at a few flush segments, not the whole run's writes. In AR2 they reached
  24% during the rebuild.
- After the writes stopped, compaction merged the beside-delta's flush segments twice, with lineage.
- The new base installed over its 33 segments and kept the delta built beside it (2 tiers). Lineage went back to
  zero, because the new base covers the merged targets itself.
- No tier was retired as stale, none was discarded, and with retention at 60 seconds, GC kept every object the final
  manifest names.

### Crash points

Each was run on a fresh copy of the v0.1.15 store, with `kill -9`, then a restart:

| Killed at | State left | After restart |
|---|---|---|
| The first lineage map written, before its manifest | 4 maps, no lineage in the manifest | Recall@10 0.955 over 200 queries. The merges re-ran (25 entries, same tiers). GC deleted the 4 orphan maps, and every named object exists |
| The first manifest with lineage | 4 entries published | Recall@10 0.955, no exhaustive rows. Compaction finished (25 entries, same tiers), and every named object exists |

The unit and integration tests cover the same points with injected store failures. Those tests also cover a failure
before and after the manifest write and a merge of a merged segment.

### D4

Not done in this round. The covered segments' ANN objects total 6.0 GB on NQ (about 2,240 bytes per covered row),
which is what D4 would save. `disk-footprint.md` lists what retiring them needs.

## Round X1: the resident ceiling and warm query path

Measured on 2026-11-24 on a dedicated c7g.2xlarge (eight Graviton3 vCPUs, 16 GB,
Linux, Rust 1.100.0, and GCC 14), against the unchanged `b663eb2` server. This is a
paired ARM measurement, separate from the Apple M4 results above. The corpus is the
same persisted NQ tier used in Q1: 2,681,468 f16 vectors of 1,024 dimensions, 32 slabs,
and 13,395 leaves. All 3,452 queries request ten IDs with strong consistency.

The non-shipped `bf-engine` example `resident_ceiling` loads validated code, center,
and exact-vector arrays, and runs each query synchronously to completion on an OS
thread. Each thread owns reusable scratch. There is no Tokio, range reader, block
cache, or allocation in a warmed query. The probe scaling, directory order,
per-slab running bounds, fine rescoring, rerank cap, f32 query, and f16-to-f32 exact
rerank match the server. Every top-ten ID list matches the server at both settings.

The harness replays the full query set three times after warming every query on
every worker. Linux hardware counters count user instructions on the query threads.
The server uses sixteen keep-alive HTTP clients, pre-encoded JSON arrays, a 4 GiB
memory block cache, no disk cache, no indexing, and no recall sampling. Its two
independent passes each warm and measure the full query set. `perf stat` counts
all server threads after warm-up; counter coverage is 100%.

| Probes | Recall@10, both paths | Resident instructions/query | HTTP instructions/query, two passes | Resident q/s, one / eight threads | HTTP q/s, two passes |
|---:|---:|---:|---:|---:|---:|
| 48 | 0.9490151 | 22.2856 M | 25.7217 / 25.7220 M | 357 / 2,695 | 1,248 / 1,256 |
| 64 | 0.9598204 | 25.9276 M | 29.6851 / 29.6794 M | 294 / 2,264 | 1,062 / 1,096 |

| Probes | HTTP p50 / p90, first pass (ms) | HTTP p50 / p90, second pass (ms) |
|---:|---:|---:|
| 48 | 12.13 / 17.14 | 12.02 / 17.12 |
| 64 | 14.33 / 20.08 | 13.85 / 19.27 |

The instruction budget outside the resident pipeline is 3.44 M/query (13.36%) at 48
probes and 3.75 M/query (12.65%) at 64. The roughly twofold throughput gap is larger
than that instruction ratio. Server task-clock is 4.95 / 5.67 CPU-ms per query;
eight times the resident wall duration gives an upper bound of 2.97 / 3.53 CPU-ms
per query for its eight workers. This points to execution, scheduling, and data
locality as well as retired instructions. The AVX-512 profile's cost split should
not be applied directly to this NEON machine.

Resident payloads occupy 5.994 GB, including every exact vector; peak process RSS
is 6.25 GB. Server peak RSS is 3.64–3.74 GB. The server reports a logical cache-hit
ratio of 1.000 and no exhaustive tail in every retained pass. The resident example
is a ceiling experiment, not a proposal to retain every exact vector in the server.

The allocation check and the ordinary-reader comparison run on both ARM and x86.
They cover cosine and squared Euclidean distances, zero vectors, f16 and f32
storage, corrupt admission, reused rotation, center ranking, and scan estimates.
One premise from the study reports did not survive a source check: serde's
flattened `extra` field buffers unknown fields; known `rank_by` fields still use
typed deserialization. No request-deserializer rewrite is justified by that premise.

The harness is reproducible with a local, fully indexed, single-tier NQ store and
a headerless little-endian f32 query file:

```sh
cargo run --release -p bf-engine --example resident_ceiling -- \
  "$STORE" nq "$QUERIES_F32" 8 48 3
```

It requires Linux user instruction counters, uint document IDs, and an unmerged
covered-segment ID index. Its resident APIs live in `bf-vector`; it adds no HTTP
endpoint, configuration flag, or persisted format.

### Compiled warm tiers

The real query path now admits an immutable compiled view on a tier generation's
second query. Ordinary reads supply validated leaf codes and exact-vector rows;
admission issues no extra I/O. The decoded cache reserves the complete code and
metadata capacity before admission, including a bounded sparse exact-vector
cache. Generation keys and immutable references keep replacement atomic for
readers. Eviction drops the cache's reference while in-flight queries finish.

A compiled hit performs rotation, center ranking, probe selection, bounded scan,
candidate pruning, and exact reranking on the query's existing compute thread,
with reusable thread-local scratch. The small winner list then enters the same
hydration, lineage, ID ordering, projection, snapshot, and WAL-tail logic as the
ordinary path. Masks, filters, joint-tier plans, missing resident data, paged
directories, legacy formats, and exact-all plans retain the ordinary reader.
The `blowfish_ann_compiled_queries_total` metric counts completed compiled plans.

Vector score-map keys now retain each query float's bits in binary. Empty score
maps skip key construction, and disabled recall sampling skips UUID generation.
HTTP parsing and serialization are unchanged.

The original Spot instance was reclaimed after the initial implementation runs.
Final comparisons use a dedicated replacement c7g.2xlarge, with the same corpus,
toolchain, settings, and sixteen clients for every arm. Each process warms two
complete query epochs, then measures all 3,452 queries. Baseline and candidate
are paired in both orders; a separate control contains the compiled tier without
the score-key and disabled-sampling changes. Release builds invalidate the changed
crates between source snapshots so Cargo cannot reuse another arm's artifacts.

| Probes | Baseline instructions/query, two passes | Final instructions/query, two passes | Baseline q/s, two passes | Final q/s, two passes |
|---:|---:|---:|---:|---:|
| 48 | 25.7034 / 25.6970 M | 21.5091 / 21.5088 M | 1,148 / 1,176 | 1,526 / 1,551 |
| 64 | 29.6650 / 29.6694 M | 24.9513 / 24.9521 M | 1,036 / 1,018 | 1,310 / 1,347 |

Mean user instructions fall 16.3% at 48 probes and 15.9% at 64; mean throughput
increases 32.4% and 29.4%. The compiled-only control takes 21.6975 / 25.1402 M
instructions at 48 / 64 probes, so the score-key and sampling changes remove a
further 0.189 M instructions/query. Their separate throughput difference is within
run-to-run variation; the instruction saving is the reason to retain them.

| Probes | Baseline p50 / p90, two passes (ms) | Final p50 / p90, two passes (ms) |
|---:|---:|---:|
| 48 | 13.20 / 18.91; 12.92 / 18.38 | 9.91 / 14.53; 9.70 / 14.31 |
| 64 | 14.63 / 20.68; 14.90 / 20.93 | 11.63 / 16.79; 11.31 / 16.33 |

All 3,452 ordered top-ten ID lists match across baseline, control, and final builds
at both settings. Recall remains 0.9490151 / 0.9598204. Every measured final query
uses the compiled plan, has a logical cache-hit ratio of 1.000, and reads no
exhaustive tail. Final peak RSS is 4.64–4.66 GB versus baseline 3.68–3.83 GB; the
improvement retains about 0.9 GB more resident data within the cache reservation.

Workspace formatting, full-workspace ARM all-target clippy with warnings denied, and tests
pass (336 tests, zero failures). Engine/vector all-target clippy, tests, and
doctests also pass on x86 (198 tests, zero failures). Tests include allocation-free
scoring, checksums, ties, concurrent admission, cache bounds, cache eviction,
projections, WAL-tail changes, indexed masks, filters, and exact-all fallback.

### Cold gate and remaining budget

The cold gate alternates AB/BA for 30 matching queries per probe setting, starting
a fresh server process for each arm. It uses the standard local S3 facade with
25 ms per-read delay, eight runtime workers, no disk cache, no indexing, no recall
sampling, and connection warming disabled. All ordered ID lists match.

| Probes | Baseline p50 / p90 (ms) | Final p50 / p90 (ms) | Mean reads/query, both | Mean bytes/query, both |
|---:|---:|---:|---:|---:|
| 48 | 250.71 / 315.83 | 255.68 / 316.66 | 96.47 | 113.20 MB |
| 64 | 273.33 / 338.26 | 277.64 / 326.21 | 104.53 | 138.23 MB |

The median paired latency changes are +0.67 / +0.22 ms, with paired-bootstrap
95% intervals of [-2.21, +3.20] / [-4.21, +2.45] ms (20,000 resamples). Mean paired
changes are -0.08 / -4.33 ms. This sample detects no cold regression and does not
rule out a small latency change. Every query pair has identical read and byte
counts; the first query retains the ordinary plan.

On the replacement machine, the final eight-thread resident harness reaches
2,670 / 2,256 q/s and 22.4591 / 26.1451 M instructions/query at 48 / 64 probes.
Its full ID lists again match the server. The server reaches about 58% of that
throughput. Different specializations of the shared scoring code produce
different instruction sequences: the server now retires fewer instructions than
the harness while taking longer. The harness is a throughput ceiling experiment,
not a strict instruction lower bound.

A final 15,000-query, 48-probe all-cycle profile attributes 79.3% of sampled cycles
to vector functions, including 30.2% to NEON block sums and 18.3% to plane dots.
Kernel symbols account for 2.5%, libc copies for 3.4%, moka for 2.2%, and jemalloc
for 0.9%. Eight samples were lost out of roughly 17,000. The warm server averages
3.85 CPU-ms/query and uses about 5.9 CPUs during the measured HTTP interval; the
resident run's eight-worker duration bounds its CPU cost by 3.00 ms/query.
These measurements suggest that the remaining wall-time gap includes request
work and pipeline utilization, beyond the count of retired instructions. They
do not establish a throughput win over Qdrant.

After merging main `52525a4`, paired ABBA runs on an owned c7g.2xlarge
(Rust 1.100.0, Clang 19/22) used a newly built NQ tier: 32 slabs, 13,392 leaves,
and all 3,452 queries, with the same sixteen clients and cache settings above.
At 48 / 64 probes, mean warm throughput rose from 1,234 / 1,101 to
1,619 / 1,405 q/s (**+31.2% / +27.6%**); instructions/query fell from
23.125 / 26.782 M to 20.781 / 24.068 M (**10.1%** at both settings).
Every ordered top-ten list matched, recall stayed 0.9516802 / 0.9618192, and every
measured merged query used the compiled path. Two 30-pair fresh-process cold
ABBA checks per setting, using the same 25 ms S3 facade, detected no regression:
pooled p50/p90 changed from 208.7/245.3 to 200.2/239.5 ms at 48 probes, and
205.4/246.3 to 207.8/236.2 ms at 64. Median paired changes were -4.36 / +1.31 ms,
with query-clustered bootstrap 95% intervals [-12.44, +2.22] / [-4.52, +5.63] ms;
all cold lists matched. The first 64-probe p50 increase did not reproduce.

## Round Q1: warm vector query cost

This round starts from `6dcde67` (v0.1.15). The unchanged-query profile uses `b899d0b`, which
adds microsecond diagnostics without changing the wire response or query algorithm. The target is
at least 1.5× warm throughput at recall@10 0.95 and 0.96, with no cold-S3 regression. **That gate
is not met by the VM results below.** The M4 was reserved for another worker, and the coordinator directed this round to finish its VM gates;
M4 throughput is unmeasured.

### Dataset and measurement

I loaded all 2,681,468 NQ passages as 1,024-dimensional f16 cosine vectors. The corpus, queries,
truth, and qrels have the same four hashes as the v0.1.15 M4 run. The automatic tier contains
32 slabs and 13,395 leaves. Preparation retries left 125,000 superseded source row versions and
46 covered source segments; the slabs contain exactly 2,681,468 live rows, and no WAL tail is
searched. This independently built layout differs from the M4 comparison, so its recall differs
slightly too. Every binary comparison below uses the same unchanged store.

The VM server and clients use CPUs 6–7, a nice-19 server, two Tokio workers, two Rayon workers,
a 4 GiB raw block cache, no disk cache, no recall sampling, and read-only mode with indexing off.
Clients send pre-encoded JSON float arrays over keep-alive connections, requesting the top 10 and
only IDs. Hardware counters attach to the exact owned server PID after warming the measured
queries. Instruction counts are user-space instructions. Other workers share the VM, so wall-clock
q/s and stage timers cannot establish a throughput improvement. No production process is signaled.

### The unchanged query

Two alternating passes at each probe count, each with 1,000 warm-up and 1,000 measured queries
at 16 clients:

| Probes | Instructions/query, passes | Cycles/query, passes | Recall@10, first 1,000 |
|---:|---:|---:|---:|
| 48 | 13.1499 / 13.1318 M | 18.7558 / 18.2749 M | 0.9511 |
| 64 | 15.1948 / 15.1868 M | 22.5791 / 20.2938 M | 0.9625 |

All retained passes have cache-hit ratio 1.000 and exhaustive-search count zero. In 100 serial
traced queries after 1,000 warm-ups, the mean scan visits 393 / 524 leaves, scans 82,820 / 110,114
rows, emits 5,748 / 7,168 candidates, and exactly reranks 54.10 / 54.87 rows at 48 / 64 probes.

| Stage | Median microseconds, 48 / 64 |
|---|---:|
| HTTP body collection / typed JSON decode | 6 / 8; 65 / 70 |
| Snapshot/state access, overlapping search | 207 / 162 |
| Center ranking, including rotation and dispatch | 437 / 514 |
| Probe planning | 209 / 259 |
| Per-slab task construction | 92 / 101 |
| Cluster reads, install, and scan wall stage | 3,205 / 4,611 |
| Sum of slab scan calls | 3,298 / 4,371 |
| Global bounds, vector reads, rerank, and speculative hydration | 662 / 748 |
| Sum of rerank cache-read calls / exact CPU calls | 191 / 196; 66 / 67 |
| Final hydration / response JSON serialization | 36 / 37; 14 / 16 |
| Search execution | 5,179 / 6,982 |

These are scheduler-sensitive wall timers. Medians do not add, parallel call sums differ from
elapsed stages, and snapshot checking overlaps search. Scan-call medians are about 40 ns per row;
means rise to 61.5 / 50.6 ns with preemption. Keep-alive amortizes TCP accept across requests.

A separate hot 64-probe `perf record` pass at 199 Hz has no reported lost samples, cache-hit
ratio 1.000, and no tail. Approximate self-sample shares of all cycles: fast-scan kernel 12.3%;
scan setup, bookkeeping, and candidate emission 5.6%; center ranking 6.1%; exact query-to-leaf-center
distances 1.5%; JSON decoding 7.4%; allocator 4.9%; raw and decoded cache lookups 2.8%; kernel
13.6%; and exact rerank symbols about 0.4%, small enough for sampling noise. About 7% of samples
are unresolved libc addresses. They are not attributed to a particular copy routine.

### Shared partial-index metadata

A directory query previously allocated and initialized a complete cluster table for every slab,
then installed a few selected payloads. Partial indexes now share immutable ranges and retain only
their installed payloads. Ordinary fully opened indexes retain dense tables. Original cluster IDs,
encoded objects, read ranges, and HTTP responses are preserved. The resident-byte estimate charges
the shared table to the existing decoded-cache budget.

| Probes | Baseline instructions/query, ABAB | Shared metadata instructions/query, ABAB | Saving |
|---:|---:|---:|---:|
| 48 | 13.1441 / 13.1374 M | 12.3756 / 12.3567 M | 5.9% |
| 64 | 15.1982 / 15.1997 M | 14.3571 / 14.3725 M | 5.5% |

All 4,000 top-10 ID lists match exactly. Peak RSS is roughly unchanged, or slightly lower; the
shared VM does not establish a cycle or throughput improvement.

Fresh-process cold queries use `bench/scale/local_s3.py`, 25 ms per request, the same store, and
the production 1 MiB block size. Thirty queries per binary alternate ABBA at query granularity.
At 64 probes, baseline p50/p90 is 482.90/637.16 ms; shared metadata is 475.65/588.48 ms. All IDs
match. Mean requests are 101.93/102.13, bytes 139.10/139.53 MB, and idle-gap waves 3.067/3.133;
speculative document-read scheduling accounts for small read-count differences. This does not
model real S3 transfer throughput or tails, and 30 samples cannot establish p99 behavior. An earlier
10-query comparison overlapped other owned query sweeps and is discarded for latency attribution.

### Sharing candidate bounds between slabs

After a slab scans, its k-th smallest candidate upper bound is a valid cut for the other slabs:
the global k-th upper bound cannot be larger than one subset's. A per-query relaxed atomic passes
that cut to slabs that have not scanned yet. Scan reads still start concurrently. Each scan also
applies its final local bound after fine rescoring, removing candidates accepted under earlier,
looser cuts before the global merge. The local filter alone saves only about 0.2%; passing the
cut between slabs gives the useful additional saving. There is no new setting or stored format.

The rotated bounded-scan Rust API gains an `upper_bound` argument, infinity until another slab has
k witnesses. The engine is its only existing caller. Regression tests compare local and shared
survivor sets against bounding a complete scan, reject invalid bounds, and check that independent
partial indexes share no installed payloads.

Combined shared metadata and candidate bounds, two ABAB rounds with 1,000 warm-up and 1,000
measured queries at 16 clients:

| Probes | Baseline instructions/query | Combined instructions/query | Saving | Baseline cycles/query | Combined cycles/query |
|---:|---:|---:|---:|---:|---:|
| 48 | 13.1336 / 13.1380 M | 11.4780 / 11.5023 M | 12.5% | 17.9741 / 19.2160 M | 16.7857 / 15.0430 M |
| 64 | 15.1824 / 15.1967 M | 13.2610 / 13.2478 M | 12.7% | 21.9068 / 20.0412 M | 18.1330 / 17.1201 M |

All 4,000 top-10 ID lists are identical, cache-hit ratio is 1.000, and no tail is searched. These
instruction savings do not establish a proportional M4 throughput gain. The requested 1.5× gate
remains unmet.

In 100 traced queries at 16 clients, the combined path scans the same 82,820 / 110,114 rows
at 48 / 64 probes, but returns 431 / 493 slab candidates instead of 5,748 / 7,168. Exact rerank
remains 54.10 / 54.87 rows. The retained saving comes from shared metadata and candidate handling.

A final fresh-process cold ABBA comparison uses 30 queries per binary at each probe count,
the same default 1 MiB cache blocks, and the 25 ms local S3 facade:

| Probes | Baseline p50/p90, ms | Combined p50/p90, ms | Baseline/combined requests | Baseline/combined MB | Baseline/combined waves |
|---:|---:|---:|---:|---:|---:|
| 48 | 388.70/544.00 | 382.19/523.55 | 94.07/94.10 | 114.50/114.57 | 3.100/3.100 |
| 64 | 436.83/528.02 | 426.22/518.11 | 102.10/101.90 | 139.46/139.02 | 3.067/3.033 |

All 60 cold top-10 ID lists match. No median or p90 cold regression is observed. Small request
count differences come from speculative document reads; no scan-read dependency is added. The
facade and sample-size limitations above still apply.

### Recall and rejected experiments

Full 3,452-query results against the exact top 10, four clients, after warming all queries.
All 31,068 top-10 ID lists match between binaries. These full quality sweeps were not interleaved;
the VM wall rates reflect changing shared load and **do not measure a speedup**.

| Probes | Recall@10, both | Baseline instructions/query | Combined instructions/query | Baseline VM q/s | Combined VM q/s |
|---:|---:|---:|---:|---:|---:|
| 32 | 0.932706 | 10.9547 M | 9.6623 M | 173.1 | 267.2 |
| 40 | 0.942439 | 12.0532 M | 10.5922 M | 146.4 | 186.6 |
| 48 | 0.949015 | 13.1146 M | 11.4877 M | 186.7 | 176.4 |
| 52 | 0.951825 | 13.6584 M | 11.9327 M | 84.8 | 165.0 |
| 56 | 0.954635 | 14.1542 M | 12.3858 M | 204.4 | 153.8 |
| 64 | 0.959820 | 15.1649 M | 13.2692 M | 187.0 | 194.7 |
| 68 | 0.962196 | 15.6785 M | 13.7019 M | 87.9 | 207.5 |
| 80 | 0.966628 | 17.1395 M | 14.9867 M | 208.6 | 181.4 |
| 96 | 0.971060 | 19.0511 M | 16.6872 M | 200.6 | 144.1 |

I did not lower the default probe count. On this layout the measured qualifying points are 52
and 68 probes; their baseline costs are 13.6584 and 15.6785 M instructions/query.

- Caching owned code blocks beside decoded clusters saves 3.3–3.6% instructions, with identical
  results, but adds roughly 0.4 GB RSS and has no stable cycle gain. It is set aside.
- Ranking only eight nearby slabs at 64 probes costs 11.599 M instructions/query, but full recall
  falls to 0.928158. It is rejected.
- Rebuilding through the existing recluster API with 64-row leaves takes 1,324.49 seconds and
  produces 32 slabs and 41,881 leaves. With shared metadata, 24/28/32/40 probes give recall
  0.943279/0.949363/0.953998/0.960892 at 16.3465/17.1787/17.9653/19.5305 M instructions/query.
  At 32 probes, 100 traced queries scan 59,187 rows across 819 leaves: fewer rows, but more than
  twice as many leaf setups as default 48. Hot self samples attribute 13.4% to center ranking,
  4.4% to exact query-to-leaf-center distances, 3.9% to probe selection, and about 4% to decoded
  cluster lookups. The blocked scan still costs 14.5%. Finer leaves fail the complete-query gate.
- Transposing resident center codes into 32-row lookup blocks changes center quantization slightly.
  Default-layout recall becomes 0.949044/0.959994 at 48/64 probes, but costs rise to
  13.6184/15.5983 M instructions/query, despite including the shared-metadata saving. Extra table
  setup per slab outweighs the new kernel on this AVX-512 VM, so the prototype is rejected.

Formatting, three-crate all-target clippy with warnings denied, and all three crates' tests pass:
217 passed, six ignored, and zero failed across 44 suites. The final test run uses one test thread
and two Tokio/Rayon workers. An earlier run overlapping compilation failed two 50 ms read-wave
assertions; both pass unchanged in the serial run, consistent with scheduling sensitivity in the
fixture. No assertions were relaxed. `bench/nq/profile.py` preserves the owned-process warm
profiling procedure; see `bench/nq/README.md` for reproduction. All run artifacts are removed
after recording these measurements.

## Memory budgets: blowfish and Qdrant on an Apple M4

I find that **the ranking changes with available RAM**. With hard 2 and 4 GiB query budgets,
blowfish at 48 probes (recall 0.9497) gives **710 and 1,799 q/s**, against Qdrant ef 64
with vectors on disk (0.9492) at **83 and 224 q/s**. Mapping Qdrant's graph too improves it to
**96 and 712 q/s**, at slightly lower recall, 0.9453. At 8 and 12 GiB, Qdrant's closest-recall
mode reaches **5,616 and 5,702 q/s**, against blowfish's **1,805 and 1,795 q/s**: about 3.1 times
faster. Blowfish benefits from a larger small cache, then plateaus; Qdrant recovers sharply once
its roughly 5 GiB query working set fits. The old 14.73 decimal GB Qdrant maximum included
index construction, rather than measuring query memory alone.

### Workload and controls

I use the same 10-core Apple M4 Mac mini, 16 GiB RAM, macOS 26.2, blowfish `v0.1.15`
(`e49c509`, release profile, without `local-models`), and Qdrant 1.20.0 as the preceding section.
The data is again 2,681,468 NQ passages, 3,452 queries, 1,024-dimensional f16 vectors, cosine distance,
and id/vector only. I verify all four input SHA-256 hashes against that run:

```text
b1f1fb5d2d49dad476e3f0419ae3ea2975f907ddff94e447133e868ba31be185  corpus.f16
841a621c983f0299d60873ee976990806f5d01e20116516f7bdc9842f4be2aaf  queries.npy
fa470968ca6bf216a02ff0995881b07df055d06ca5a5780c7edb16769fb3c1b9  truth.npy
60764fc0c70c0e35204c36905c40cb3299721ae97f15d235ada83af92f510e64  qrels.json
```

Every warm row has two fresh-process repeats. In **each repeat**, I first measure recall@10 against
exact truth over all 3,452 queries at limit 10, then warm 1,000 queries and time 1,000 at one client.
I run the same 2,000 pre-encoded HTTP queries at 1, 4, and 16 clients, in that order, and report 16-client
throughput. Tables give the range of each latency percentile, and mean throughput with its range.
All 10 cores are shared by server and clients, without CPU pinning. I run one engine at a time,
wait for complete indexing, and remove its data before the next engine. Storage is local disk, rather
than S3. Index creation is uncapped; these budgets apply to querying an existing index.

Blowfish uses a fresh automatic tier covering all rows in 23 segments, 32 slabs, and 13,392 leaves.
I keep that store fixed across its native profiles and Linux budgets. Its new layout differs from
the preceding run, so I report quality again rather than reusing the previous recall.
I disable the background exhaustive recall sampler
(`BLOWFISH_RECALL_SAMPLE_RATE=0`) throughout this sweep: the external quality pass already checks every
query in every repeat, and a periodic exhaustive job would fall inside only the longer, low-memory
measurements. The engine source and query algorithm are unchanged. Its disk cache is disabled.
Qdrant uses f16, HNSW m=16, ef_construct=100, optimizer target one segment, and
max_segment_size=100,000,000. Both placements finish green with every vector indexed and two physical
segments. I build each placement separately, so their measured recalls differ; I keep those values
visible rather than treating ef as a quality guarantee.

### Enforced query budgets

I run both engines in Docker on an arm64 Linux guest, with Colima 0.10.3 and a run-local Lima 2.3.0
built by `bench/nq/build_uncached_lima.sh`. Standard VZ backing files are host-cached: a guest cgroup
alone can leave another copy of the dataset in the Mac's page cache. The helper switches VZ to
`DiskImageCachingModeUncached`. Before the first boot, I APFS-clone and replace the stopped VM's root
and data disk files to discard previously cached inodes. The installed Lima remains unchanged.

Each fresh server gets `--memory=2g --memory-swap=2g`, or the corresponding 4, 8, or 12 GiB pair.
I verify cgroup v2, active memory and swap controllers, and `memory.swap.max=0`; the server cannot swap
inside its cgroup. The VM gets 10 vCPUs and one GiB above the engine limit: 3, 5, 9, or 13 GiB.
The OS and client share that extra GiB; the client runs in a separate container outside the engine's
cgroup. I use a Linux release build of the same blowfish tag and the `qdrant/qdrant:v1.20.0` image.
Engine files live on a guest ext4 Docker volume, without host bind mounts. The virtual data disk
is 26 GiB for Qdrant and expands to 36 GiB for blowfish's larger retained store. I restart the VM between
budgets, and sync and drop the guest page cache before each fresh server, so earlier containers'
pages cannot escape the next container's accounting.

For blowfish, the block cache is 1, 3, 7, or 11 GiB, with a 512 MiB decoded cache in every case.
These are cache capacities, rather than reservations; the cgroup also includes other allocations and
file cache. Qdrant's first placement has vectors `on_disk=true` and HNSW `on_disk=false`; its second
has both `on_disk=true`. I read process peak RSS from `/proc/1/status` (`VmHWM`), and total peak memory
from cgroup `memory.peak`, including charged file cache and kernel memory. Each table cell shows the
maximum across the two repeats, in GiB. Tiny cgroup overshoots at the page-accounting boundary round
to the configured limit.

I hash Qdrant's binary vector and graph files against their native reference before and after each
VM sweep, and compare blowfish's entire store before and after its sweep. macOS swap stays enabled,
and the existing desktop applications remain running. Across the 48 capped repeats,
the system-wide counters record **30.8 MiB of swap-ins and zero swap-outs**, with at most 7.72 MiB
swapped in during one repeat. These counters include desktop activity. The capped servers record
no OOM events, and all 48 exit with status zero.
The absolute rates below describe virtualized Linux on the M4; I compare both engines within this
setup, and show native Mac calibration separately.

At recall around 0.95:

| Engine, setting | Limit GiB | Peak RSS / cgroup GiB | Recall@10 | p50 / p90 / p99 ms | q/s at 16, mean (range) |
|---|---:|---|---:|---|---|
| blowfish, 48 probes | 2 | 1.62 / 2.00 | 0.9497 | 3.1–3.2 / 4.9–5.1 / 9.0–10.2 | 710 (696–724) |
| Qdrant, vectors on disk, ef 64 | 2 | 1.96 / 2.00 | 0.9492 | 25.2–26.9 / 34.2–38.1 / 43.1–56.3 | 83 (80–86) |
| Qdrant, vectors + HNSW on disk, ef 64 | 2 | 1.96 / 2.00 | 0.9453 | 22.7–23.1 / 30.7–31.3 / 40.1–41.8 | 96 (85–108) |
| blowfish, 48 probes | 4 | 3.54 / 4.00 | 0.9497 | 1.8 / 2.1 / 2.6–2.7 | 1,799 (1,785–1,814) |
| Qdrant, vectors on disk, ef 64 | 4 | 3.96 / 4.00 | 0.9492 | 4.8–5.6 / 17.2–18.4 / 23.0–24.0 | 224 (212–236) |
| Qdrant, vectors + HNSW on disk, ef 64 | 4 | 3.96 / 4.00 | 0.9453 | 4.0–4.2 / 14.0–14.2 / 18.4–19.5 | 712 (708–717) |
| blowfish, 48 probes | 8 | 3.52 / 6.41 | 0.9497 | 1.8 / 2.1 / 2.5–2.7 | 1,805 (1,792–1,818) |
| Qdrant, vectors on disk, ef 64 | 8 | 4.82 / 4.89 | 0.9492 | 0.6 / 0.7–0.9 / 0.8–1.2 | 5,616 (5,482–5,751) |
| Qdrant, vectors + HNSW on disk, ef 64 | 8 | 4.79 / 4.87 | 0.9453 | 0.6 / 0.7 / 0.8 | 5,752 (5,692–5,811) |
| blowfish, 48 probes | 12 | 3.53 / 6.42 | 0.9497 | 1.8 / 2.1 / 2.6–2.7 | 1,795 (1,790–1,800) |
| Qdrant, vectors on disk, ef 64 | 12 | 4.81 / 4.89 | 0.9492 | 0.6 / 0.7 / 0.8 | 5,702 (5,653–5,751) |
| Qdrant, vectors + HNSW on disk, ef 64 | 12 | 4.79 / 4.87 | 0.9453 | 0.6 / 0.7 / 0.8 | 5,707 (5,676–5,739) |

At the requested higher search settings (their measured recalls differ):

| Engine, setting | Limit GiB | Peak RSS / cgroup GiB | Recall@10 | p50 / p90 / p99 ms | q/s at 16, mean (range) |
|---|---:|---|---:|---|---|
| blowfish, 64 probes | 2 | 1.67 / 2.00 | 0.9602 | 3.3–3.4 / 5.2–5.4 / 9.8–10.8 | 581 (561–600) |
| Qdrant, vectors on disk, ef 128 | 2 | 1.96 / 2.00 | 0.9707 | 48.8–51.2 / 60.5–63.6 / 71.6–77.9 | 44 (43–44) |
| Qdrant, vectors + HNSW on disk, ef 128 | 2 | 1.96 / 2.00 | 0.9680 | 44.7–45.1 / 55.1–56.0 / 65.7–67.5 | 56 (56) |
| blowfish, 64 probes | 4 | 3.54 / 4.00 | 0.9602 | 1.9 / 2.3 / 2.8–2.9 | 1,554 (1,549–1,560) |
| Qdrant, vectors on disk, ef 128 | 4 | 3.97 / 4.00 | 0.9707 | 8.1–8.6 / 27.2–27.5 / 36.9–38.7 | 131 (131–132) |
| Qdrant, vectors + HNSW on disk, ef 128 | 4 | 3.97 / 4.00 | 0.9680 | 7.3–7.4 / 21.2–21.6 / 27.4–28.2 | 169 (168–170) |
| blowfish, 64 probes | 8 | 3.52 / 6.42 | 0.9602 | 1.9–2.0 / 2.3–2.4 / 2.7–2.9 | 1,519 (1,485–1,552) |
| Qdrant, vectors on disk, ef 128 | 8 | 5.19 / 5.26 | 0.9707 | 0.8–0.9 / 1.1 / 1.2 | 3,998 (3,995–4,001) |
| Qdrant, vectors + HNSW on disk, ef 128 | 8 | 5.18 / 5.25 | 0.9680 | 0.8 / 1.1 / 1.2 | 4,152 (4,104–4,201) |
| blowfish, 64 probes | 12 | 3.54 / 6.43 | 0.9602 | 1.9–2.0 / 2.3 / 2.8 | 1,553 (1,537–1,569) |
| Qdrant, vectors on disk, ef 128 | 12 | 5.19 / 5.26 | 0.9707 | 0.8–0.9 / 1.1 / 1.2 | 4,034 (4,033–4,035) |
| Qdrant, vectors + HNSW on disk, ef 128 | 12 | 5.18 / 5.25 | 0.9680 | 0.8–0.9 / 1.1 / 1.2 | 4,077 (4,061–4,092) |

At the 8 GiB limit, blowfish at 48 probes peaks at **3.52 GiB RSS but 6.41 GiB charged memory**,
while vector-on-disk Qdrant ef 64 peaks at **4.82 GiB RSS and 4.89 GiB charged memory**.
Blowfish has file cache outside its process mappings in addition to its application cache;
Qdrant's memory-mapped file pages appear in RSS. Lower process RSS alone does not establish
lower total query memory. The tables use total charged memory to expose that difference.

### Native Mac calibration and a fresh-process pass

These rows have no process memory cap. I vary blowfish's block-cache capacity through 1, 3, 7, and
11 GiB with a 512 MiB decoded cache, and add its original 4 GiB block / 2 GiB decoded default.
Peak RSS comes from `/usr/bin/time -l` around each fresh query process, including its quality,
warm-up, latency, and throughput work. A larger configured cache only uses memory if the workload
fills it; it cannot by itself establish a matched memory budget on macOS.

| Engine, cache or placement | Peak RSS GiB | Recall@10 | p50 / p90 / p99 ms | q/s at 16, mean (range) |
|---|---:|---:|---|---|
| blowfish, block 1 / decoded 0.5 GiB, 64 probes | 1.57 | 0.9602 | 1.9 / 3.0–3.1 / 5.6–6.0 | 1,005 (980–1,030) |
| blowfish, block 1 / decoded 0.5 GiB, 48 probes | 1.54 | 0.9497 | 1.7–1.8 / 2.6–2.9 / 4.8–5.5 | 1,159 (1,080–1,238) |
| blowfish, block 3 / decoded 0.5 GiB, 64 probes | 3.21 | 0.9602 | 1.4 / 1.7–1.8 / 2.1–2.2 | 1,833 (1,823–1,842) |
| blowfish, block 3 / decoded 0.5 GiB, 48 probes | 3.05 | 0.9497 | 1.3 / 1.5 / 1.9–2.0 | 2,129 (2,114–2,143) |
| blowfish, block 7 / decoded 0.5 GiB, 64 probes | 3.10 | 0.9602 | 1.4 / 1.7 / 2.1 | 1,816 (1,783–1,849) |
| blowfish, block 7 / decoded 0.5 GiB, 48 probes | 3.08 | 0.9497 | 1.3 / 1.5–1.6 / 1.9–2.0 | 2,145 (2,141–2,149) |
| blowfish, block 11 / decoded 0.5 GiB, 64 probes | 3.07 | 0.9602 | 1.4 / 1.7–1.8 / 2.1–2.2 | 1,814 (1,794–1,834) |
| blowfish, block 11 / decoded 0.5 GiB, 48 probes | 3.12 | 0.9497 | 1.3 / 1.6 / 1.9–2.0 | 2,138 (2,129–2,148) |
| blowfish, block 4 / decoded 2 GiB (default), 64 probes | 3.16 | 0.9602 | 1.3 / 1.6 / 1.9–2.0 | 1,888 (1,886–1,890) |
| blowfish, block 4 / decoded 2 GiB (default), 48 probes | 3.14 | 0.9497 | 1.2 / 1.5 / 1.8–1.9 | 2,165 (2,111–2,219) |
| Qdrant, vectors on disk, ef 64 | 5.37 | 0.9492 | 0.4 / 0.6 / 0.6–0.7 | 6,063 (5,827–6,299) |
| Qdrant, vectors on disk, ef 128 | 5.45 | 0.9707 | 0.7 / 0.9 / 1.0 | 4,855 (4,831–4,879) |
| Qdrant, vectors + HNSW on disk, ef 64 | 5.37 | 0.9453 | 0.4 / 0.6 / 0.7 | 7,535 (7,360–7,709) |
| Qdrant, vectors + HNSW on disk, ef 128 | 5.45 | 0.9680 | 0.7 / 0.8 / 0.9 | 5,500 (5,483–5,517) |

At 48 probes, increasing the native block cache from 1 to 3 GiB raises mean throughput from
**1,159 to 2,129 q/s**; 7 and 11 GiB give **2,145 and 2,138 q/s**. Their peak RSS remains near
3.1 GiB. The original default gives 2,165 q/s. This workload stops benefiting materially from
larger cache capacities once the hot data fits; the 7 and 11 GiB cache capacities do not
cause equally large native allocations.

For the separate default 64-probe pass, I start a fresh process with empty application caches,
retain the OS file cache, and immediately time 1,000 queries without quality or warm-up first.
The **first query takes 51.8 ms**; the entire pass has **2.3 / 3.9 / 7.7 ms** p50 / p90 / p99,
2.8 ms mean, and **1.83 GiB** peak RSS. Later queries warm the application, so the pass percentiles
are not 1,000 independently cold queries. This is process startup on local disk, rather than
a cold-device or cold-S3 measurement.

The historical all-RAM Qdrant ef 64 row remains **7,752 q/s** (7,481–8,023), recall **0.9497**,
and **0.5 / 0.6 / 0.7 ms** p50 / p90 / p99. Its query RSS samples peaked at **5.49 GiB**
(5.89 decimal GB); its **14.73 decimal GB** maximum included load and indexing. Those are different
measurements. I reuse the row without rerunning it, and do not plot its build peak as query memory or
compare its native rate directly with a capped Linux row.

### Limits and reproduction

The query sequence, cache history, and independent index builds matter. A complete quality pass
precedes every warm repeat; these are neither random-query throughput estimates nor cold-read S3
measurements. Latency uses 1,000 sequential queries, while throughput uses 2,000 with concurrency;
a fast median can coexist with severe later cache churn. Two repeats show variability, rather than
a confidence interval. A 4-to-8 GiB change brackets a transition; it does not locate the exact
working-set threshold. The VM limit includes the server's charged page cache, but excludes the guest
OS, client, VM overhead, and desktop applications. Host swap and virtualization remain limitations.

`bench/nq/qdrant.py --vectors-on-disk` selects the first Qdrant placement; adding
`--hnsw-on-disk` selects the second. `bench/nq/measure.py --parts quality,latency,throughput
--clients 1,4,16` runs each warm repeat, with `NQ_QUALITY_K=10`, the selected engine, and its search
setting. `--parts latency --first-pass` skips latency warm-up for the separate fresh-process pass.
The harness now reports a failed or killed throughput worker, and reaps its peers. Its three
regressions pass, along with formatting, full-workspace clippy, and all 324 Rust tests. The runtime
helper is syntax-checked and exercised by the actual signed build, VM boots, and memory sweeps.

## Round T1: smaller phrase positions, and BM25 kill tests

Text format 6 reduces live MS MARCO text indexes by 2.14×, with identical results.
The 1.3× BM25 cost target remains open. The seed, document-order, and bound
experiments did not pass their instruction gates, so their implementations stay
out of the production path.

I use the first 1,000,000 MS MARCO passages, the checked-in 1,000 dev queries,
default BM25 parameters, and exhaustive truth from `bf-text/examples/exact_bm25.rs`.
Measurements run on one disposable c7g.2xlarge (8 Graviton vCPUs, 16 GiB RAM).
Both arms use the production release profile. The server has two Rayon and two
Tokio workers, a 1 GiB memory cache, 4,000-byte blocks, 512-block coalescing,
and fetch concurrency 512. Builds and loads finish before measured query passes.
This is local disk and a fixed-delay S3 facade on EC2, with no real S3 latency claim.

### Footprint and format

The first kill test uses five consecutive 200,000-row text segments, with positions
enabled. It inspects every block before selecting a codec.

| Region | Format 5 bytes |
| --- | ---: |
| Phrase positions | 453,410,160 |
| Packed postings | 168,369,046 |
| Term records | 82,327,828 |
| Packed lengths | 950,355 |
| Dictionary bundles, padding, chunk index, and footers | 26,638,311 |
| Total | 731,695,700 |

Positions occupy 62.0% of the index. All 56,676,270 occurrences have element zero.
Unsigned delta varints take 56,699,677 bytes; one scalar/array flag per block brings
that to 58,032,680 bytes, 7.81× smaller than raw pairs. Total text bytes fall to
336,318,220, a 2.18× reduction. A complete decode comparison checks every occurrence,
not just phrases or samples.

A separate HTTP-loaded namespace contains six compacted segments of 125,000,
200,000, 165,000, 200,000, 200,000, and 110,000 rows. Its existing background upgrade
moves all six text fields from format 5 to format 6. Live text bytes fall from
743,516,484 to 348,199,345 (2.14×). The other live segment files are unchanged:
documents 126,088,846 bytes, filters 52,991,262, ID filters 3,000,163, ID indexes
6,889,704, and compact text keys 222. Retired files remain eligible for ordinary
retention and GC; these figures count live references.

Format 6 keeps the footer and 52-byte block-directory shape. Position blocks have
a one-byte flag, followed by unsigned LEB128 position deltas. Array blocks also
encode element deltas. Coordinates restart per document, and positions restart
when the element changes. Gaps, repetitions, and array boundaries are lossless.
BM25 does not read the position stream.

Formats 4 and 5 transcode directly without retokenizing documents. The background
upgrade recognizes `TextHeader::is_current`; manifest and bucket versions are
unchanged. The current reader can serve older files while an upgrade runs. Unknown versions
fail, new writes always use format 6, and there is no downgrade switch.

The binary `ids.bfi` hypothesis is not implemented. On this integer-ID corpus,
those files use 6.89 bytes/row rather than the estimated 35. Eliminating every ID-index
byte would save less than 1% of format-5 live segment bytes, and ID-only BM25 already has
222 bytes of stride-encoded text keys. A separate UUID/string-ID study is needed
before changing the engine's ID format.

### Build cost

The Spot host was reclaimed after the query checks. I repeated the build gate on
one replacement c7g.2xlarge, with both arms paired on that machine. Two A/B/A/B
builds parse, tokenize, build, and write the same five 200,000-row segments.
Separate binary hashes and every emitted segment footer are checked (5 before,
6 after). An initial pass that reused Cargo artifacts after archive extraction
was discarded; these runs follow a forced rebuild.

| Build metric, median of two runs | Format 5 | Format 6 |
| --- | ---: | ---: |
| Parse, tokenize, build, and write | 34.124 s | 33.818 s |
| Within `SegmentBuilder::finish` | 4.438 s | 4.370 s |

Individual full-build times are 34.611 and 33.637 s before, versus 34.460 and
33.176 s after. The codec passes the ≤1.05× build-time gate.

### CPU hypotheses

These kill tests call `bf-text` directly over immutable memory-backed segments.
They search the five segments sequentially with the existing shared threshold,
warm all 1,000 queries once, and count only the measured pass with perf's control
FIFO. They exclude HTTP and engine work, and are not throughput comparisons with
the earlier Apple M4 results. Counts are the median of two measured passes.

| Variant | Million instructions/query | Reduction factor |
| --- | ---: | ---: |
| Baseline | 20.32 | 1.00× |
| Exact kth score supplied free | 14.91 | 1.36× |
| Best 64 rows per term, free seed scoring | 17.75 | 1.14× |
| Best 64 rows per term per segment, free seed scoring | 15.92 | 1.28× |
| Recursive document bisection prototype | 18.66 | 1.09× |
| Experimental window-span cache | 19.60 | 1.04× |
| Unlimited exact window bounds plus span cache | 18.61 | 1.09× |
| Bisection, unlimited exact bounds, and span cache | 16.37 | 1.24× |

The larger seed union contains 1,656 candidate rows/query on average. Its 1.28×
ceiling already misses 1.3× before charging selection, scoring, or I/O, so no seed
lists are written. The ideal floor clears 1.3× by too little to rescue these lists.

The bisection prototype uses four log-odds refinement passes per balanced split,
recursing to at most 128 rows and omitting unique or twice-occurring graph edges.
Partitioning takes 2.38–2.44 seconds per 200,000-row segment. Its compressed text
files grow from 336,318,220 to 342,407,182 bytes, and instructions fall only 9%.
This prototype misses both the byte and instruction gates; it is not a general
claim about other graph-partitioning implementations.

Unlimited exact per-window maxima are a stronger bound than one joint `(tf, len)`
pair per block. Even their combined ceiling misses 1.3×, so no bound metadata or
row permutation is added to format 6. Persisting a single winning pair would also
need to account for changed namespace average length, since that can change which
posting wins.

### Committed codec: HTTP checks

The baseline production source is `b663eb2` (the test-only `c3eafdd` has identical
production code). The codec is `3e49a4a`; corpus-backed kill and round-trip tests
are in `5f13376`. Both binaries use Rust 1.100.0 and Clang 21.1.8 for native
dependencies. Warm A/B/A/B passes start fresh servers, warm the full workload,
and then measure 1,000 queries at each client count. Perf attaches only to the
server during the measured pass. Cold A/B/A/B uses 60 queries and a fresh process
per query, through the read-only facade with 25 ms/request. The OS page cache is
retained. Tables give the median of each arm's two passes.

| Metric | Format 5 | Format 6 |
| --- | ---: | ---: |
| Live text index bytes | 743,516,484 | 348,199,345 |
| All live segment bytes | 932,486,681 | 537,169,542 |
| Warm c1 p50 / p90 | 3.31 / 5.55 ms | 3.28 / 5.51 ms |
| Warm c16 p50 / p90 | 26.47 / 52.14 ms | 25.41 / 48.54 ms |
| Warm c16 queries/s | 499.3 | 517.3 |
| Server instructions/query, c1 | 27.851 million | 27.811 million |
| Server instructions/query, c16 | 27.037 million | 26.698 million |
| Fresh-process facade p50 / p90 | 165.9 / 232.5 ms | 170.4 / 231.0 ms |
| Cold reads/query | 113.78 | 113.86 |
| Cold MB/query | 3.600 | 3.617 |
| Cold waves/query | 3.15 | 3.15 |
| Peak server RSS, median after c16 | 616.1 MiB | 616.4 MiB |
| overlap@10, all 1,000 dev queries | 1.000 | 1.000 |

BM25 cost is essentially unchanged. Cold p50 is 2.7% higher, p90 is 0.7% lower,
and reads, bytes, and waves are essentially flat. Individual cold p50/p90 pairs
are 161.2/237.7 and 170.6/227.4 ms before, versus 178.0/234.1 and 162.8/227.9 ms
after. I do not claim a BM25 speedup from position compression.

For phrase checks, I use the first four whitespace-delimited words of each of the
first 60 passages, with `ContainsTokenSequence`, BM25 over the same words, and
ID-only projection. This is a separate correctness and position-I/O workload,
not a claim about the MS MARCO relevance distribution.

| Cold phrase metric, 60 queries, A/B/A/B | Format 5 | Format 6 |
| --- | ---: | ---: |
| p50 / p90 | 363.2 / 560.2 ms | 355.1 / 540.8 ms |
| Reads/query | 553.82 | 395.00 |
| MB/query | 6.128 | 5.001 |

Phrase p90 stays below the 5% regression gate; reads fall 28.7%, and bytes fall
18.4%. These are whole-query numbers, including dictionaries, postings, and
manifest reads. Fixed 4,000-byte cache blocks limit the benefit from the 7.81×
smaller position payload.

All **8,480 final HTTP responses** pass strict ordered-ID and f32 score-bit
comparisons: 8,240 BM25 responses against exhaustive truth, and 240 phrase responses
against one another. The complete 1,000-query offline result also matches the
exhaustive IDs and score bits. Codec properties cover arbitrary u32 coordinates,
arrays, scalar fields, repetitions, truncation, overflow, and noncanonical varints.
Migration tests cover formats 4 and 5, missing and empty fields, long and short
posting lists, header-only opens, and byte-identical fresh builds and upgrades.

### Reproduction and limits

Prepare the corpus with the same source and settings as
`bench/scale/msmarco_load.py`. Keep corpus, binaries, stores, and output in an
owned `.scratch/` directory, and remove it after preserving measurements. The
ignored tests in `bf-text/src/experiments.rs` provide `prepare_msmarco`,
`inspect_msmarco`, `upgrade_msmarco`, `lossless_msmarco`, `seeds_msmarco`,
`reorder_msmarco`, and `query_msmarco`. They take `BF_TEXT_EXPERIMENT` as their
scratch root, with `corpus.jsonl` and `workload.json`. Use the baseline revision's
prepare test for format-5 input, and the current revision for transcoding and
round-trip validation. The query test accepts `BF_TEXT_IDEAL_FLOOR`,
`BF_TEXT_SEED_FLOOR`, and `BF_TEXT_PERFECT_BOUNDS` for the optimistic ceilings.
Perf's enable/disable FIFO excludes preparation and warm-up from instruction counts.

For the HTTP runs, use `bench/scale/bm25_local.py warm` with `--queries 1000
--clients 1 16`, and `cold` with `--queries 60`, selecting each separately built
binary and its corresponding immutable store. Supply `--namespace msmarco` and
an owned `--profile-dir` under `.scratch/` when using perf. Repeat A/B/A/B, and
compare every response to the exhaustive scorer using strict IDs and score bits.
The published timings are not Apple M4 timings, in-region S3 latency, or a new
comparison with hosted turbopuffer.

## Round C1: overlapping cold ANN reads

Measured on 2026-11-24, on one spot `c7gd.2xlarge` in AWS us-west-2, the bucket's region,
using the preserved release-upgraded Wikipedia namespaces. Every server was
read-only. No source objects, manifests, or layouts were changed.

The control is commit `b663eb2`, with one instrumentation-only counter added to
count backend read attempts when they start. Counting only completed operations
misses requests canceled by hedging. Both arms use that counter; GET totals below
include canceled attempts and missing-key GETs, and exclude HEADs.

Each comparison alternates arm order for the same held-out query on the same
instance. Cache-cold arms keep separate processes and connections, and clear
memory, disk, decoded metadata, and namespace handles before each query. Fresh
arms start a process and an empty application cache for every query, with the
same 1.5-second connection-warmer settle in both arms. Operating-system caches
are retained. Requests use strong consistency, 64 probes, `top_k=10`, and the
existing exact-truth oracle workloads. Settings are eight Rayon threads, 4,000-byte
blocks, 512-block coalescing, 512 concurrent fetches, an 8 GiB payload cache, and a
512 MiB decoded cache. The toolchain is Rust 1.100.0 with GCC 14 on aarch64.

A separate 200-request-per-size S3 check measured 4 KB GET p50 / p90 of
**24.5 / 31.0 ms**, and 1 MB GETs of **30.8 / 37.9 ms**. S3 latency changed during
the session; compare each candidate with its paired control, rather than comparing
absolute candidate times across trials.

### What changed

The old snapshot prefetch awaited the entire scan, and reranking awaited the
slowest slab. The replacement reserves coalesced reads before starting independent
four-leaf scan batches, prioritizes nearby leaves, and starts a bounded prefetch
of at most 80 vectors and 30 documents once 40% of the scan batches finish.
The final probe set, error bound, rerank cap, distance calculation, and hydration
remain the same. Every selected leaf is scanned; no time-based cutoff is enabled.

Keeping coalescing was necessary: the first streaming prototype roughly doubled
completed GETs. `RangeReader::read_batch_notifying` lets leaf consumers share the
whole batch's reserved requests without waiting for an unrelated range to finish.
A memory-residency check keeps resident queries on the per-slab path. Streaming
applies to large, unfiltered slab searches without a shared cross-tier plan;
These comparisons used a temporary `BF_ANN_STREAM_SCAN=0` override to restore the previous scheduling; it was removed at merge, so streaming is the only cold slab path and later comparisons use a baseline binary.

### Rejected experiments

| Paired trial | Control p50 / p90 ms | Candidate p50 / p90 ms | Result |
| --- | ---: | ---: | --- |
| 35 ms hedge seed and floor, 20 cache-cold queries | 503.9 / 599.3 | 488.3 / 631.6 | 1.03× p50 and worse p90; removed |
| Streaming without reserved coalescing, 20 queries | 503.9 / 599.3 | 402.1 / 559.9 | Completed GETs rose from 562.5 to 1,154.4; removed |
| Coalesced streaming, 30 speculative vectors and documents, 200 queries | 482.7 / 609.3 | 375.3 / 499.3 | 1.286× p50 misses 1.30×; not kept as the final setting |
| Distant-half cutoff, 2% overfetch, and 35 ms hedging, 200 queries | 482.7 / 609.3 | 380.4 / 475.5 | Slower than streaming alone; removed |
| 80 speculative vectors and documents, 50 queries | 501.4 / 713.5 | 365.1 / 479.6 | GET attempts rose 18.8%, above the 15% limit; removed |

The cutoff waited for the nearest half and at least 95% of batches, then allowed
`max(1.8 × median completion time, 80 ms)` before canceling distant stragglers.
Its recall was 0.9630 against 0.9615, but it did not improve the latency target.
An earlier handoff at 20% completion also missed the target in its 50-query screen.
Co-probed leaf repacking was not attempted; this round makes no format change.

The split speculation limits first passed a 200-query cache-cold run:
581.6 / 720.0 ms became 443.7 / 582.1 ms, with recall 0.9615 on both arms and
GET attempts 588.6 → 655.0 (+11.3%). A 50-query fresh-process run measured
648.7 / 840.0 → 472.3 / 734.9 ms, with 11.8% more GETs. The final default build
was then rebuilt and checked separately below.

### Final default build

Latency pairs are p50 / p90 in milliseconds. Cache-cold rows use all 200 oracle
queries per arm, and fresh rows use the first 50. Recall below is over all 200
cache-cold queries, against the existing independently computed exact top-10.
The 100M paired harness, including both servers and the client, runs in a 10 GiB
cgroup with swap disabled; payload-cache budgets remain 8 GiB per server.

| Corpus and mode | Control p50 / p90 ms | Streaming p50 / p90 ms | Mean GET attempts, control → streaming |
| --- | ---: | ---: | ---: |
| 10M, cache-cold | 554.1 / 701.2 | 424.7 / 543.7 | 586.5 → 653.7 |
| 10M, fresh process | 617.1 / 951.9 | 478.3 / 623.7 | 575.0 → 644.3 |
| 100M, cache-cold | 1,027.9 / 1,165.7 | 715.1 / 907.0 | 2,211.5 → 2,247.5 |
| 100M, fresh process | 1,116.6 / 1,418.1 | 807.1 / 1,062.4 | 2,181.5 → 2,233.6 |

10M cache-cold p50 improves **1.3045×**, with p90 lower, recall unchanged at
**0.9615**, and **11.5%** more GET attempts. The independent default-build rerun
therefore meets the required 1.30× / 0.001 recall / 15% GET gate. Fresh-process
GET growth is 12.1%, and its p90 also improves.

At 100M, cache-cold p50 improves **1.4374×**, p90 is lower, recall remains
**0.9540**, and GET growth is **1.6%**. This improves on the control, though it
falls just short of the advisor's separate 1.45× aspiration for 100M.

All **500 final query pairs** (200 cache-cold and 50 fresh at each size) returned
identical IDs and distances. After timing, a fan-out/admission guard was added to
preserve the existing per-slab policy for busy processes and cache-bypass reads.
Its predicates are true in this single-query, cache-admitting configuration; the
normal and forced-fallback paths both passed the final regression checks.

### Reproduction

`crates/bf-engine/examples/cold_ab.py` preserves the paired harness. Supply the
private storage URL, namespace, region, and optional profile/endpoint in a local
config file; the runner enforces read-only storage. For a comparison within the
current binary:

```sh
source ~/.cargo/env
CARGO_TARGET_DIR="$PWD/.scratch/c1/target" cargo build --release \
  --manifest-path crates/bf-engine/examples/scale-server/Cargo.toml
uv run crates/bf-engine/examples/cold_ab.py \
  --config bench/scale/config.local \
  --binary .scratch/c1/target/release/blowfish-scale-server \
  --arms '{"control":{"BINARY":"BASELINE_BINARY"},"stream":{}}' \
  --scratch .scratch/c1 --label cachecold --queries 200
```

Use `--mode fresh --queries 50` and a new label for fresh-process timing. Use
`--oracle 100m` with a config pointing at the copied 100M namespace for the larger
workload. The recorded control above used the original build with the attempt
counter backported, rather than the environment override. Summarize results and
remove the run directory afterward.

### Validation, spend, and cleanup

`cargo fmt --check`, `cargo clippy -p bf-engine -p bf-storage --all-targets -- -D warnings`,
and `cargo test -p bf-engine -p bf-storage` passed on a separate Graviton checks
host: **197 tests passed**, four existing tests remained ignored, and none failed.
The 100K-row current-format regression also passed separately with
`BF_FANOUT_QUERIES=0`, exercising the per-slab fallback. It covers cold/warm result
equality, projection, and indexed deletes. Storage tests cover coalescing,
dispatch priority, independent early completion, cancellation, and residency.
The reusable Python harness passed an AST check and its `--help` invocation.

| EC2 work | Instance type | Instance-hours | Estimated compute USD |
| --- | --- | ---: | ---: |
| Paired query measurements, spot | c7gd.2xlarge | 0.975 | 0.1163 |
| Checks interrupted by Spot capacity reclamation | c7gd.2xlarge | 0.092 | 0.0103 |
| Completed checks, on-demand | c7g.2xlarge | 0.394 | 0.1143 |
| Total | | **1.461** | **0.2409** |

Compute uses the observed regional Spot rates and the AWS on-demand price;
EBS and S3 request charges are additional. No S3 data was added. All instances were
terminated, and the temporary key pair and security group were deleted. Local run artifacts
were removed after recording these results. Workspace and SDK compatibility
checks remain the coordinator's merge checks.

## Round I2: binary vectors in the WAL (2026-11-24)

I test whether replacing the WAL's f32-base64 vectors with a binary section at the declared
precision improves acknowledged ingest by at least 1.30×, preserves replayed values, and avoids a
write-p90 increase. The starting source is `d79c6757`.

### Method and kill test

All measurements run on one disposable c7g.2xlarge Spot instance in us-west-2 (8 vCPU, 16 GiB RAM,
swap off), against Amazon S3 in that region. The server uses CPUs 0–5, six Tokio workers, and six
Rayon workers; the client uses CPUs 6–7. Both binaries use the same Rust release profile, with
`CARGO_PROFILE_RELEASE_LTO=false` and `CARGO_PROFILE_RELEASE_DEBUG=1`.

The input is the first 500,000 BEIR NQ rows: 1,024-dimensional f16 vectors, original titles, and
original text. I prepare identical 1,000-row JSON request bodies once, outside timing. They carry
f32-base64 vectors, as the official SDK does. Four clients write into a fresh namespace in a fresh
S3 prefix for every arm. Timing includes the first schema-bearing write, request-body reads,
backpressure retries, and the last durable acknowledgment. Write percentiles include retry delays.
No query traffic runs during ingest.

Both arms use a 512 MiB memory cache, no disk cache, a 100,000-row or 256 MiB indexing threshold,
a 30-second indexing age, a 256 MiB bulk-flush threshold, and a 384 MiB unindexed-WAL limit. The
WAL pipeline remains eight; background indexing, compaction, and automatic reclustering stay on.

The initial profiled baseline takes 181.91 s (2,749 acknowledged rows/s), with write p50/p90
669.5/1,378.9 ms and 44 backpressure retries. A 99 Hz `perf record` captures 36,696 samples with
no lost samples. Vector-distance work in `l2_squared_f32` accounts for 28.98% of sampled cycles
on Tokio workers; text posting construction accounts for 5.68%. The WAL staging stack accounts for
9.64% inclusively, including 8.77% in `encode_payload`. The main JSON string-escaping sample accounts
for 1.33%. Compression samples include both WAL payloads and segment blocks, so I do not attribute
all compression time to the WAL. The profiled load is excluded from the paired timing
comparison.

### Codec experiment

The disposable prototype replaces only the external WAL payload codec. It writes zstd-compressed
JSON metadata followed by little-endian binary vector records. Dense f16 values occupy two bytes
per dimension, and dense f32 values occupy four. Metadata borrows documents without cloning their
large vector strings. Small inline commits keep their existing representation. Public `WalEntry`
and `WriteBatch` values, HTTP bodies, acknowledgment durability, indexing, and query code stay the
same.

The prototype uses payload version 4 because versions 2 and 3 identify commit-slot encodings.
It reads version-1 payloads automatically until checkpointing and normal GC remove them. There is
one payload writer. Before timing, prototype tests pass for mixed old/new logs, conflicts, retries,
pruning, malformed binary sections, missing/null fields, dense f16/f32 values, arrays, sparse vectors,
multi-vectors, and every finite f16 bit pattern. Values that cannot round-trip through their declared precision stay in
JSON instead of being silently rounded by the codec.

I run unprofiled arms in candidate/baseline/baseline/candidate order:

| Arm | Seconds | Acknowledged rows/s | Write p50 / p90 ms | Backpressure retries |
| --- | ---: | ---: | ---: | ---: |
| Candidate 1 | 169.55 | 2,949 | 393.3 / 812.6 | 56 |
| Baseline 1 | 125.27 | 3,991 | 678.6 / 883.3 | 28 |
| Baseline 2 | 125.41 | 3,987 | 687.8 / 926.4 | 28 |
| Candidate 2 | 114.80 | 4,355 | 398.0 / 740.3 | 44 |

The retained WAL payload objects shrink from 1,548,985,444 and 1,548,985,398 bytes in the
baselines to 1,041,784,805 and 1,041,756,063 bytes in the candidates: about 3,098 versus 2,084 B/row,
a 32.7% reduction. These are compressed object payload sizes, including the storage identity header;
commit slots and segment objects are excluded.

Fresh-process replay with the prototype reader checks baseline 1 and candidate 1 independently
against the original input. Both reproduce every ID exactly once, all 512,000,000 vector coordinates
bit-for-bit, and every title and text. They contain 321 and 338 WAL slots, respectively. The five
prototype codec tests also pass; they are removed with the unadopted codec.

**Rejected.** The baselines agree within 0.11%. The candidate ranges from 0.74× to 1.09× their
throughput, and neither run reaches 1.30×. Write p90 improves, but acknowledged ingest does not
meet the gate. Faster individual commits do not remove background indexing work or backpressure:
the candidate receives 44–56 retry responses, compared with 28 in both baselines. The difference
between uncompressed base64 size and binary size is not an ingest-speed prediction.

I remove the experimental codec instead of adding a format or a setting for it. Production WAL
payload version 1, commit slots, bucket fencing, and public APIs remain unchanged. No upgrade is
needed. The retained HTTP driver and replay checker support future ingest experiments.

### Reproduction and cleanup

`crates/bf-wal/benches/http_ingest.py` prepares the fixed request bodies and measures acknowledged
HTTP ingest. Run it with `uv run --with numpy python`, using `prepare BODIES --corpus CORPUS.npy
--attrs ATTRS.jsonl`, then `load BODIES --clients 4` against each fresh server. The corpus is a
little-endian f16 NumPy matrix; attributes are JSONL objects containing `title` and `text` in corpus
row order. Put every input, body directory, store, and log below the run's `.scratch/` directory.

`crates/bf-wal/examples/replay_check.rs` checks retained WAL payloads independently of the writer's
resident state. Set `I2_URL` to the disposable S3 prefix, `I2_CORPUS` to that matrix, and `I2_ATTRS`
to that JSONL file, then run `cargo run --release -p bf-wal --example replay_check`. The checker
requires every source ID exactly once, compares all 1,024 f32 bit patterns with the original f16
values expanded to f32, and compares every title and text. It fails on missing or duplicate rows.

All disposable S3 prefixes are deleted and verified empty. The instance, key pair, and security
group are removed. The instance ran for about 0.50 hours; the observed Spot price was $0.1074/hour,
or about $0.05 in compute, excluding EBS and S3 requests. Local run inputs, logs, and source archives
are removed after retaining these results. Final `cargo fmt --check`,
`cargo clippy -p bf-wal --all-targets -- -D warnings`, and all 13 `cargo test -p bf-wal` tests pass.

## Round B4: impact ordering, and dense BM25 scoring

The tested impact-ordered TAAT path fails the CPU and cold-block gates. The
row-aligned NEON scorer also fails the gate when combined with the existing
reader's planner, caches, and posting reads. No production algorithm or text
format changes ship from this round; text format 6 and automatic upgrades remain
unchanged.

The workload is T1's first 1,000,000 MS MARCO passages, five consecutive 200,000-row
segments, and all 1,000 checked-in dev queries, with default BM25 parameters and
limit 10. The offline example was `crates/bf-text/examples/impact_bm25.rs` (removed after the round; it is in commit 11a4738); its
adjacent README documents preparation, counter control, and each variant.
Final measurements use an owned on-demand c7g.2xlarge, pinned to one core, with
one complete warm-up before each measured pass, on Rust 1.100.0. Perf's control
FIFOs exclude preparation and warm-up. All arms use the release profile and sequential segments with a shared
query threshold. These are paired ARM instruction/cycle measurements, with no
HTTP, S3 latency, or Apple M4 throughput claim.

| Offline variant | Million instructions/query | Million cycles/query |
| --- | ---: | ---: |
| Existing reader | 17.832 | 11.975 |
| Impact TAAT | 127.684 | 111.551 |
| Impact, free exact block maxima | 127.105 | 112.032 |
| Dense, optimistic NEON | 13.653 | 7.291 |
| Dense, optimistic cached scalar | 13.056 | 7.132 |
| Dense, existing planner, NEON | 17.023 | 10.453 |
| Dense, existing planner, cached scalar | 16.927 | 10.331 |

Counts are medians of two measured passes per variant, except the free exact
block-maxima diagnostic, which has one. The integrated NEON variant reduces
instructions by **1.047×** and cycles by **1.146×**, missing both the 15.6M
instruction limit and the 1.3× cycle gate. Cached scalar in the same planner is
slightly cheaper, at **1.053×** fewer instructions and **1.159×** fewer cycles.
The optimistic standalone NEON result passes its absolute instruction and cycle
gates, but the scalar control is cheaper still; that result cannot be attributed
to SIMD or substituted for the integrated gate.

The final embedded reader's paired baseline differs from T1's historical 20.32M
instructions/query. Compare variants inside this binary; embedding the reader,
changing the offline driver, or changing release code generation is not a shipped
production improvement. Both integrated variants also miss the absolute 15.6M gate.

Resetting decoded postings and saturation/bound caches before each segment/query
gives **3,928,538** required baseline posting blocks across the 1,000 queries.
Impact order needs **6,863,734** (**1.747×** baseline), and free exact block maxima
still need **6,815,551** (**1.735×**). Neither meets the ≤50% cold-block gate.
These are logical payload-block counts, with no GET coalescing or latency claim.

Repacking every long list, including terms outside the queries, adds **13,186,336**
packed posting bytes to the **336,318,220**-byte format-6 text index: **3.921%**.
This estimates the posting codec delta with the current bitpacking implementation;
it does not include changes to dictionary reservation sizes or validate an emitted
format-7 file. Footprint is not the reason this candidate is rejected.

The impact prototype sorts long lists (df > 4,096 or more than 32 blocks) by
build-time segment-normalized saturation, keeps rows sorted within each
128-posting block, and leaves shorter lists in row order. Runtime suffix maxima
of independent max-TF/min-length bounds remain conservative when namespace
statistics change. A priority queue selects the next term block. Per-row term
contributions preserve canonical f32 addition, and an indexed top-k heap tracks
increasing lower scores.

The proposed sum of remaining term bounds is only a bound on **untouched** rows.
A touched row can retain a missing contribution, and a current winner needs its
exact score before it can be returned. The prototype also checks unresolved
partials and requires winners to have exact scores. An absent term can force
exhaustion of its list to establish that missing contribution as zero. This
experiment supplies no free candidate completion through a separate row index.
The exact sorted-access implementation is rejected; this does not establish a
ceiling for impact layouts with a separately charged candidate-completion index.

The dense experiment transposes long query terms into row-aligned u32 frequencies.
The NEON kernel uses two f64 lanes, keeps the reference arithmetic order, converts
each term's contribution to f32, and adds terms in dictionary order. Sparse
candidate windows retain the cached scalar scorer. The standalone planner gets
unlimited exact window maxima and a fully prepared frequency cache. Its cached
scalar control isolates the kernel from the planner/cache advantage.

The integrated control retains the existing planner, bounded decoded cache,
window-bound cache, posting reads, and shared threshold, changing only the final
scoring loop to consult the prepared frequencies. Preparation of that additional
frequency cache is free in this test. It does not model a persisted row-aligned
representation or establish that all possible format-7 layouts fail. Its measured
cost still misses the requested gate, so the optimistic standalone result is
insufficient evidence to ship a format bump.

The example tests compare randomized corpora with exhaustive scoring, covering
missing terms, repeated query terms, changed namespace averages, and score ties.
A native ARM test checks SIMD score bits against the existing scorer for zero
frequencies, frequencies and lengths beyond the saturation cache, odd batch
lengths, and multiple query frequencies. The full-corpus checks compare ordered
IDs and f32 score bits, rather than overlap alone.

The final crate gate passes: formatting, clippy for all bf-text targets, 39 library
and integration tests, and 14 example tests. Seven historical corpus experiments
remain ignored in each relevant test binary. The independent exhaustive utility
checks all 1,000 queries and 10,000 ordered hits, with every f32 score bit equal;
every prototype pass asserts against that validated baseline. No HTTP, SDK, or
full-workspace validation is claimed for these example-only changes.

A Spot c7g.2xlarge was reclaimed during an incomplete repeat. Incomplete counters
are discarded; the final table comes entirely from the replacement on-demand
host. No S3 objects are created, and both hosts and their temporary access
resources are cleaned up at the end of the round.

## Round RR1: recluster readiness and spill ownership (2026-11-24)

NQ's full tier became ready **257.5 seconds after the last load acknowledgment**, versus **756.7 seconds**
before RR1: **2.94 times sooner**. There were no intermediate recluster builds during loading. The final build
fell from **426.4 to 255.0 seconds**, or **1.67 times faster**, with equal or better measured recall.
The requested **2 times faster full build was not achieved**; leaf assignment remains the largest cost.

### Matched setup

These numbers isolate RR1 through `5a34b2d` against `b663eb2`, before integrating later main changes. The baseline
has only spill cleanup and the same stage timers added; its trigger, training, and assignment algorithms are
unchanged. Both use native release builds with the repository's thin LTO profile, four codegen units, no native
Rust CPU flags, and Clang 19 for C dependencies.

One dedicated Graviton3 `c7gd.2xlarge`: eight cores, 16 GiB RAM, local NVMe, and a fresh local file store per arm.
The server and HTTP loader share CPUs 0 to 7, at nice 19, with no competing compilation or benchmark. The default
recluster pool has four threads. Both servers have a 4 GiB payload cache, no disk cache, and automatic recall
sampling disabled. No S3 bucket or paid oracle is used.

The corpus is the NQ vectors used by the Mac and Q1 runs: 2,681,468 rows, 1,024 dimensions in f16, cosine, and
3,452 queries with exact ground truth. `bench/nq/load.py --vectors-only` sends 5,000-row HTTP batches with four
requests in flight. A sampler sends at most one serial top-10 ANN request a second, with only IDs returned,
using a fixed sequence of query vectors and a persistent connection. Every 20 seconds, a separate diagnostic
compares one query against exact search on the same live snapshot. These sparse load-time recall samples do
not replace the full quality gate below. Expected 404 responses before the first write creates the namespace
are excluded from query samples.

The last acknowledgment is approximated by the loader's final status write. Readiness is the full-row-count
tier installation in the server log, polled every 250 ms and checked against namespace metadata. Latencies
cover requests started during the load, including time spent competing with ingest and the exact diagnostics.
This is one paired run, with independently generated segment IDs and layouts; it does not give confidence
intervals or establish Mac performance.

### Load and readiness

| Measurement | Before | RR1 |
|---|---:|---:|
| HTTP load, seconds | 190.5 | 176.5 |
| Intermediate builds started during the load | 5 | 0 |
| Last acknowledgment to full-build start, seconds | 330.3 | 2.5 |
| Full build, seconds | 426.4 | 255.0 |
| Last acknowledgment to full-tier installation, seconds | 756.7 | 257.5 |
| Query p50 during load, ms | 563.8 | 408.7 |
| Query p90 during load, ms | 1,623.9 | 1,589.2 |
| Successful timed queries during load | 160 | 153 |
| Query errors | 0 | 0 |
| Snapshot-exact recall samples | 8, all 1.0 | 7, all 1.0 |
| Server CPU through readiness, seconds | 3,377.6 | 1,813.3 |

The baseline installed intermediate tiers over 160K, 75K, 440K, 410K, and 950K rows. Four installed during the
load, and the fifth shortly afterward. Together they rebuilt 2.035M rows before the final 2.681M-row build.
Its doubling exception starts work during a load, but its five-minute history can still hold the final build
once uncovered rows fall just below covered rows.

RR1 uses that history only to recognize bulk ingestion. A classified load holds first bases, merges, and deltas
while writes arrived in the last two seconds or the tail is unindexed. It retries after two seconds, without
requiring another publication. The first base then takes priority over remaining compaction, protecting its
sources before its background task can yield. After installation, format-4 lineage lets those segments compact.
Queries continue searching uncovered segments and any existing tiers throughout the load. There is no setting
that selects the previous behavior.

### Full-build profile

Times below are seconds, summed over the final full build's stage events. Uploads overlap the next slab's CPU
work. Spill service times sum work across concurrent readers or writers and are already inside their parent
stages; they must not be added to build wall time. "Upload" here means publication to the local object store,
not a network or S3 upload measurement.

| Stage | Before | RR1 |
|---|---:|---:|
| Copy source vectors to spill | 20.193 | 13.557 |
| Spill write service time, inside copy | 17.835 | 11.277 |
| Coarse k-means | 1.211 | 1.156 |
| Coarse assignment | 6.788 | 6.456 |
| Leaf k-means | 116.099 | 21.948 |
| Leaf assignment, both passes | 253.871 | 183.309 |
| Center refinement | 2.560 | 2.547 |
| Read slab vectors from spill | 3.649 | 3.861 |
| RaBitQ encoding and ANN serialization | 21.172 | 21.359 |
| Upload, overlapping | 2.249 | 2.437 |
| Random spill read service time, inside earlier stages | 31.039 | 39.129 |
| Directory construction | 0.033 | 0.016 |

Lloyd training and coarse assignment use bounded batches of 128 rows against packed centers. Seeds, samples,
iteration counts, near-cell counts, group probes, and the refinement pass stay unchanged. Center accumulation
stays in input order in f64. Leaf assignment keeps direct four-row dot products, with independent FMA chains
on NEON, AVX2, and AVX-512. Whole source clusters are written contiguously to the spill; a four-byte address
per global row preserves identity without scattering one small write per vector. This changes only temporary
spill layout, with no persistent format change.

Larger assignment blocks were counterproductive: a 16-row experiment compared 1,549 centers per row, versus
812 with four rows, and its first assignment took 144.2 seconds, versus 91.2. It was stopped and discarded.
Packing selected leaf centers into matrices was also slower in a separate prototype. Batched near-group
selection completed a full build in 250.5 seconds, only 1.8% below the selected implementation in a separate
layout, and was not retained. These controls explain why the final path uses matrix batches for training and
direct four-row scoring for leaves; they do not demonstrate the missing 2 times build gain.

### Recall and memory

Separate read-only servers run the full 3,452-query quality gate, top 10, with explicit probes:

| Probes | Before recall@10 | RR1 recall@10 | Difference |
|---|---:|---:|---:|
| 48 | 0.9494 | 0.9501 | +0.0007 |
| 64 | 0.9602 | 0.9603 | +0.0001 |

Both are within 0.002 of the baseline. RSS is sampled every 250 ms; the process high-water mark is read from
`VmHWM`. GB below are decimal. The build-only sample uses current `VmRSS` between full-build start and install,
so an earlier ingest or exact-diagnostic peak is not mistaken for build memory.

| Server memory measurement | Before | RR1 |
|---|---:|---:|
| Sampled full-build peak RSS, GB | 6.884 | 6.964 |
| Process high-water RSS over load, diagnostics, and build, GB | 9.543 | 9.817 |

The full-build sampled peak rose 1.2%, with the same cache and slab budgets. This 16 GiB-host profile includes
a configured 4 GiB cache and exact diagnostics, so it is not a matched comparison with the historical Mac's
approximately 2.7 GB load/build peak. No full-precision corpus or corpus-sized distance matrix is materialized.

### Spill recovery

Each build holds an exclusive ownership lock in its own process-named directory. A drop guard unlinks its
spill on success, failure, cancellation, shutdown, or panic unwinding, even while a blocking worker has an open
descriptor. Startup and new builds recover only unlocked abandoned directories. A lock on the root directory
serializes creation and sweeping, including the interval before the ownership file exists.

Tests cover cancellation, stopped engines, panic unwinding, and a separate live process surviving a sweep,
followed by recovery after that process is killed. Legacy loose spills have no ownership lock and are left
alone; remove those only after the old processes using their directory have stopped.

## Round B5: clustered block-max pruning, short posting tails, and cold fan-out

B5 tests three separate hypotheses before changing the production layout. The
short-list codec passes its serialized-byte gate; the clustered block-max
prototype fails its CPU and block-count gates. Cold fan-out measurements and the
final production checks are described below.

### Corpus and paired CPU protocol

The first 1,000,000 passages from the loader's MS MARCO stream form five consecutive
200,000-row, format-6 text segments. The workload is the checked-in 1,000 dev
queries. All counters use one owned, on-demand c7g.2xlarge, the release profile,
and CPU 3. The baseline and BMP are in the same executable. Each invocation first
computes reference results, warms its full workload, and then uses perf's control
and acknowledgment FIFOs to count only the second pass. Builds, indexing, and
other measurements finish before the counter passes.

An independent `exact_bm25` pass verifies all 1,000 queries and 10,000 ordered
hits, including every f32 score bit. The codec and BMP also compare against those
validated production results. This is a text-library experiment, without HTTP,
engine, or S3 work in its instruction counts.

### Clustered block-max pruning: rejected

Documents sort by their rarest term, second-rarest term, length, and original row,
then form 64-document blocks. Term IDs follow lexical order. Each block records
upward-rounded u8 saturation maxima per term. At query time, contributions scatter
into block bounds in canonical f32 term order. Blocks sort by decreasing bound
across all five segments, and scoring stops only when the next bound is strictly
below the top-k floor. Original rows decide score ties.

| Workload | Baseline M instructions/query | BMP M instructions/query | BMP evaluated blocks/query |
| --- | ---: | ---: | ---: |
| Original queries, top 10 | 20.581 | 45.786 | 990.773 |
| First retained token only, top 10 | 3.431 | 6.474 | 10.905 |
| Original queries, top 1,000 | 73.701 | 250.587 | 6,601.686 |

The top-10 values are medians of two complete invocations per arm. The two edge
cases each use one complete invocation per arm. Every workload preserves ordered
rows and score bits, including the retained top-1,000 results.

The main workload fails the ≤10.1M instruction and ≤45-block gates by wide
margins. BMP executes 2.22× more instructions, scores 63,409 rows/query, and costs
47.529M cycles/query versus 10.664M baseline. First-token queries still cost
1.89× more instructions despite visiting few blocks. At top 1,000, BMP scores
422,508 rows/query, versus 53,023 baseline, and costs 3.40× more instructions.

These are deliberately favorable bounds on this proposal: the forward lists are
already decoded, normalization bounds are rebuilt for the current corpus outside
counters, and block ordering is global. The lexical sort does not cluster joint
query relevance tightly enough. Estimated codec bytes are 255,174,577, including
retained position payloads; that is not a serialized BMP format or a validated
phrase-lookup directory. No BMP layout, row permutation, or query option ships.

The experiment adapts BMP to BM25; the original paper evaluates learned sparse
retrieval, so its speedups are not a BM25 prediction:
https://www.antoniomallia.it/uploads/SIGIR24.pdf

### Actual-length posting tails: format 7

| Five-segment text footprint | Format 6 | Format 7 |
| --- | ---: | ---: |
| Total serialized bytes | 336,318,220 | 198,829,726 |
| Posting payload bytes | 168,369,046 | 64,533,360 |
| Term directory bytes | 82,327,828 | 48,686,905 |
| Phrase payload bytes | 58,032,680 | 58,032,680 |

Actual serialization saves 40.9%, or 1.69×, and passes the ≤258.7 MB gate.
The initial 199,172,186-byte accounting estimate conservatively retained the
original dictionary overhead and pre-transcode posting-size fields. Actual
serialization saves another 330,575 directory bytes and 11,885 overhead bytes.

Full 128-row blocks keep their existing SIMD bitpacking. Partial blocks store
only their actual rows as unsigned LEB128 gaps and frequencies, with their first
row in the directory. Variable-width block records retain independent posting
and position CRCs. Per-term base addresses plus frame sizes determine offsets;
document frequency determines block counts and the final block's row count.
The footer, dictionary-bundle structure, independent phrase stream, and
namespace-wide scoring arithmetic are unchanged.

Migration validates every old posting and phrase frame. Corpus validation checks
all 1,333,003 blocks, 56,676,270 phrase occurrences, document lengths, statistics,
and score bits. Position payloads are byte-identical. Unit and property tests also
cover arrays, missing fields, partial-block boundary values, corrupt metadata,
truncation, overflow, duplicates, and trailing bytes. Format-4 and format-5
migration tests continue to pass.

New writes use format 7. Formats 4, 5, and 6 transcode directly without reading
or tokenizing document columns. The existing `TextHeader::is_current` and fenced
background upgrade path select the new layout; no manifest schema change or
layout-selection option is added.

### Production codec controls

A separate HTTP-loaded checkpoint finishes compaction into five live segments.
The ordinary background upgrader converts all five from format 6 to format 7 in
4.08 seconds. Live text bytes fall from 336,384,495 to 198,814,582. Non-text segment
fields, live masks, file references, namespace statistics, and the indexed WAL
sequence stay identical. The engine's existing two upgrade integration tests pass.
Sixty HTTP phrase queries also preserve complete ordered rows and score bits.

A paired single build per arm parses, tokenizes, builds, and writes the same first
million passages. Format 6 takes 34.222 seconds; format 7 takes 35.715 seconds
(4.4% longer). Time inside `finish` rises from 4.478 to 6.186 seconds. The additional
validation and repacking cost is charged. Both emitted versions and every file's
size are checked. A stale-artifact build after switching source roots was discarded;
the reported format-7 run follows a package clean and forced compilation.

One final-binary warm pass per arm uses the original five format-6 files and their
upgrades: 20.714M versus 20.665M instructions/query, and 10.605M versus 10.842M
cycles/query. Both read the same 663.77 posting blocks and score the same 5,365
rows/query. This is essentially flat warm CPU, not a BM25 speedup claim.

### Cold facade protocol

Cold tests use the first 60 dev queries, the same stopped five-segment checkpoint,
and one fresh server process with an empty disk cache per query. Arm order
alternates by query. Each server has two Rayon and two Tokio workers, 1 GiB of
memory cache, 4 GiB of disk cache, 4,000-byte blocks, coalescing and fetch limits of
512, and 128 warm connections. Each local S3 facade delays requests by 25 ms.
The OS page cache is retained. This models dependent reads and local transfer;
it is not an in-region S3 measurement. GET totals include both `get` and
`get_range`; bytes include both operations too.

| Production codec, 60 paired queries | Format 6 | Format 7 |
| --- | ---: | ---: |
| Facade p50 / p90, ms | 156.676 / 209.140 | 154.780 / 208.918 |
| GETs/query | 109.83 | 109.82 |
| MB/query | 3.442 | 3.037 |
| Mean traced waves/query | 3.50 | 3.37 |

Every response row is identical. The codec adds no cold read wave, reduces bytes,
and leaves latency effectively unchanged. Current baselines differ from T1's
historical six-segment checkpoint, so the table is the relevant paired control.

### Cold term packing: rejected

The offline packer stores corpus DF, per-segment block directories, and short
postings in global lexical groups. Long postings and phrase data keep their old
objects and offsets. The experimental statistics index locates each group directly
from the manifest. This is a fresh-process, single-field checkpoint prototype,
with no production publication or update lifecycle.

A 128-term grouping expands the compressed manifest to 275,598 bytes, triggering
the existing compact-reader-copy rule and an additional metadata wave. Grouping
256 terms reduces it to 225,629 bytes, below that threshold. Both layouts reduce
GETs, but the first prototype also mistakenly waited for packed term data before
issuing independent document-length reads. The final candidate overlaps those
length reads with term prefetch; the final gate uses this corrected version.

| Corrected 256-term packing, 60 paired queries | Baseline | Packed |
| --- | ---: | ---: |
| Facade p50 / p90, ms | 159.653 / 217.248 | 153.009 / 201.016 |
| GETs/query | 109.42 | 84.37 |
| MB/query | 3.438 | 3.143 |

Complete response rows match for all 60 queries. The 1.043× p50 improvement misses
the ≤131 ms gate, even though p90, GETs, and bytes improve. Reducing requests does
not remove the remaining posting-read/scoring work and dependent query stages.
No cross-segment pack object, engine hook, or production format ships. The disposable
packer, temporary patch, facade driver, and block-max prototype were removed after
the round; they remain in commit 6383ed1 (`crates/bf-text/examples/b5`).

The format-7 codec is the only shipped change. The 1.3× BM25 CPU target and the
cold-packing latency target remain open. All native bf-text checks, exhaustive
truth, full corpus losslessness, automatic engine upgrades, and HTTP phrase checks
pass. The two existing engine upgrade integration tests also pass. No full-workspace,
SDK, M4, ANN, or real-S3 run is claimed.

## Round G3: disposable graphs for hot tiers

> The prototype code is not on main: it is in commit 285ea88 (branch history), and round G4 turns it into an automatic, default path with no feature flag. The measurements below are from that prototype.

These are disposable experiments and a feature-gated query-path prototype. Default production
queries, manifests, S3 layouts, and cold-path behavior are unchanged. The harness is
`crates/bf-vector/examples/hot_graph.rs`, with reproducible commands in `examples/g3/README.md`.

### Shared-VM baseline and packet kill test

BEIR NQ, 2,681,468 x 1,024 f16 vectors, cosine, top 10, and all 3,452 test queries. Recall is
against exact cosine top 10. These runs used two query threads on two x86-64 vCPUs of an AMD
EPYC 9354P. Other experiments shared the machine, so wall-clock q/s is not retained as throughput
evidence. Perf counters cover only the resident query region, including normalization, query setup,
traversal, scan, exact rerank, and recall accounting. Loading, construction, and encoding are excluded.

The IVF fixture copies the current 32-slab, 13,395-leaf base tier. Its 2,806,468 ordinal slots include
superseded covered rows; only 2,681,468 rows are indexed. Mapping tier ordinals through covered
segments' ID lists is necessary before comparing with corpus truth. The baseline uses the production
directory ranking, scaled probe count, resident code scan, per-slab top-10 bounds, global top-10 upper
bound, 320-row rerank cap, and direct exact scoring over the same f16 corpus as the graphs. It excludes
HTTP, hydration, cache lookups, and read planning. The strict qualifying baseline points are 50 probes
at 0.95 and 66 at 0.96; the requested 48/64 points fall just short of those thresholds in this harness.

The packet variant greedily forms spatial packets of up to 32 rows within existing IVF leaves,
then builds an exact-center HNSW using `hnsw_rs 0.3.5` (MIT OR Apache-2.0), M=16, base degree up to
32, and construction ef=100. Its 90,316 packets are navigated by minimum residual one-bit fast-scan
row score, using the production transform and scorer. It keeps the best 80 reached rows for exact
f16 rerank. A query caches each packet score, so upper/base-layer revisits do not scan it twice.
Row counts are logical corpus rows; the SIMD kernel also processes padded lanes.

| Variant | Probes / ef | Recall@10 | Unique packets / query | Rows scored / query | M instructions / query |
|---|---:|---:|---:|---:|---:|
| ivf | 48 | 0.949015 | — | 83,135 | 9.160 |
| ivf | 50 | 0.950637 | — | 86,668 | 9.350 |
| ivf | 64 | 0.959820 | — | 110,402 | 10.717 |
| ivf | 66 | 0.961095 | — | 113,745 | 10.891 |
| packet32 | 64 | 0.838934 | 737.2 | 22,377 | 4.416 |
| packet32 | 256 | 0.926043 | 2019.9 | 61,122 | 8.988 |
| packet32 | 480 | 0.951912 | 3335.7 | 100,764 | 13.721 |
| packet32 | 512 | 0.953882 | 3515.0 | 106,159 | 14.365 |
| packet32 | 640 | 0.960718 | 4212.5 | 127,145 | 16.891 |
| packet32 | 1024 | 0.973001 | 6168.6 | 185,937 | 24.008 |
| packet32 | 2048 | 0.985429 | 10819.2 | 325,454 | 41.137 |

**Kill the packet variant.** At the qualifying 0.95 and 0.96 points (ef=480 and 640), it needs
47% and 55% more instructions than the qualifying IVF baselines, rather than at least 50% fewer.
It also scores more rows at both thresholds. The predicted walk of 300–500 packets at 0.96 does not
materialize: the measured 0.960718 point visits 4,213 packets and scores 127,145 rows. This result
rejects this packet topology and scorer, without claiming that all possible packet graphs fail.

Packet preparation, including loading the IVF fixture, took 142 s, and its center HNSW took 127 s,
while sharing the two CPUs with other study work. The harness keeps the entire f16 corpus resident;
its approximately 5.9 GiB query RSS is not a sparse-rerank-cache memory result. Eight-bit encoding
of the corpus took 55.6 s, including writing the scratch codes. Full row-graph construction was
moved to an isolated Graviton instance to avoid contention on the shared VM; its paired results follow.

### Isolated Graviton kill tests

Measurement snapshot: query code through `29676ab` (upstream `a195183`), plus G3's corrected
scheduler and bounded-builder examples. X1's compiled warm tier, RR1, and B5 landed afterward;
their merges are integration-tested here, and their performance is not included in these tables.


Full NQ on a dedicated c7g.8xlarge: 32 physical Graviton3 cores, 64 GiB nominal RAM, Linux,
Rust 1.100, NumKong 7.8.7, and Clang 22. Unsupported SME compile targets were disabled; NEON
and SVE remained enabled. `hnsw_rs` 0.3.5 is MIT OR Apache-2.0. The row graph uses M=16,
construction ef=100, original f16 cosine distances, and a shuffled insertion order. Both row
scorers reuse identical links. Layer sampling and parallel insertion are nondeterministic.

The following are **corrected** all-core medians of three runs over 110,464 queries each
(32 passes over all 3,452 queries). One reusable state and a strided stream per worker avoid
identical repeat-aligned streams and use every worker on non-power-of-two pools. Earlier rates
from the coarse-chunk loop are superseded. Counters cover query setup, scoring, heaps, exact
rerank, and recall accounting, excluding loading and construction.

| Variant | Recall@10 | Row scores/query | Instructions/query | 32-core q/s, median (range) |
|---|---:|---:|---:|---:|
| Resident IVF, 50 probes | 0.950637 | 86,668 | 20.362M | 9,992 (9,989–9,992) |
| Row graph, f16, ef112 | 0.952462 | 1,998 | 3.105M | 24,668 (24,664–24,776) |
| Same graph, rotated SQ8, ef104, rerank30 | 0.950174 | 1,886 | 1.262M | 44,610 (44,531–44,964) |
| Resident IVF, 66 probes | 0.961095 | 113,745 | 23.745M | 8,304 (8,278–8,306) |
| Row graph, f16, ef160 | 0.961790 | 2,639 | 4.106M | 18,528 (18,506–18,621) |
| Same graph, rotated SQ8, ef160, rerank30 | 0.961472 | 2,638 | 1.550M | 32,060 (31,834–32,153) |

Both row variants survive the 2x gate. SQ8 is 4.46x/3.86x faster than the qualifying IVF
points, with about 16.1x/15.3x fewer instructions. F16 is 2.47x/2.23x faster. Row-score counts
are not equal-cost operations: IVF uses one-bit fast scan, while the row graph uses full f16
or eight-bit dot products and graph bookkeeping. The Q1-era main-library IVF implementation
is included in this baseline; its output recall and selected row counts are unchanged.

The packet graph still fails on ARM: ef480 reaches 0.951912 at 100,764 scored rows and about
20.66M instructions; ef640 reaches 0.960718 at 127,145 rows and about 25.74M instructions.
Corrected 32-core throughput is 5,557/4,428 q/s, below IVF at both points. It does not deliver a 2x saving.

The HNSW builder plus link extraction took 205.98 s; corpus loading, construction, teardown,
and scratch serialization took 215.29 s, with 13.00 GiB peak RSS. These are warm local-input
measurements, not S3 promotion timings. Native query RSS is about 6.31 GiB for f16 and 8.88 GiB
for SQ8 because the kill-test harness retains the entire f16 corpus in both cases.

### Corrected M4 native results

The same EC2-built graph, original NQ corpus, and all-query truth were measured on an otherwise
reserved 10-core M4. These are three-run medians with the corrected strided scheduler. macOS
query-region task instructions and cycles come from `RUSAGE_INFO_V4`, excluding load/build.
Native rates exclude HTTP, namespace snapshots, storage readers, and hydration.

| Variant | Recall@10 | M4 q/s, median (range) | Instructions/query |
|---|---:|---:|---:|
| F16, ef112 | 0.952462 | 9,141 (9,133–9,176) | 2.846M |
| SQ8, ef104, rerank30 | 0.950174 | 15,142 (15,089–15,172) | 1.622M |
| F16, ef160 | 0.961790 | 6,871 (6,865–6,872) | 3.767M |
| SQ8, ef160, rerank30 | 0.961472 | 11,160 (11,042–11,188) | 2.126M |

These imported-graph measurements do not demonstrate construction on the M4. They establish a
promising search core, rather than a shipped-path Qdrant win.

### Real query path: the Qdrant target is not met

The prototype is compiled only with `experimental-hot-graph`. It routes through the real
`Engine::query`, source-tier placement, live masks, normal sparse exact rerank, projected rows,
WAL shadowing, and delta merge. NQ was prepared afresh through normal writes, indexing, and
reclustering. The M4 comparison imports the trusted EC2-built graph over corpus IDs, translates
those IDs into the fresh tier's coordinates, and opens its exact-rerank metadata. The full f16
corpus is released; graph, SQ8, source metadata, and row-location capacity total 3.62 GB (3.37 GiB).

Paired M4 HTTP results use 16 persistent clients, top-10 requests, all-query quality, warm-up,
and 110,464 timed queries per run. The table reports three-run medians. Qdrant is the official
1.20.0 release, cosine, f16 in RAM, M=16, ef_construct=100, no payloads, and one indexed segment
plus an empty write segment. A fresh Qdrant build needed different ef values from the earlier
reference; actual recall, rather than nominal ef, sets the comparison.

| Engine and budget | Recall@10 | M4 HTTP q/s, median (range) | Server instructions/query | Warm process RSS |
|---|---:|---:|---:|---:|
| Graph ef104, rerank30, 4 GiB range-cache cap | 0.950174 | 5,472 (5,247–5,509) | 3.771M | 5.15–5.19 GiB |
| Qdrant ef72 | 0.950377 | 8,502 (8,494–8,554) | 2.023M | 5.55 GiB |
| Graph ef160, rerank30, 4 GiB range-cache cap | 0.961472 | 5,136 (5,128–5,171) | 4.267M | 5.18–5.21 GiB |
| Qdrant ef104 | 0.961124 | 6,976 (6,892–7,039) | 2.335M | 5.55 GiB |

The graph is 36%/26% slower at nearby recall. Its native core gains do not survive roughly
2.15M additional server instructions per query on this machine. Server task counters exclude
client work. A paired 24-server-core Graviton check also loses: graph ef104 achieves 11,267 q/s
at 0.950174 versus Qdrant ef66's 14,097 at 0.950261. Graph ef160 achieves 9,912 at 0.961472.
The Graviton profile attributes 27.7% of cycles to i8 dots and 22.2% to graph traversal; JSON,
exact distance, copies, cache operations, scheduling, and the surrounding engine account for the
rest. Shared-wrapper optimization belongs to the subsequent Q2 study.

### Sparse-cache budget curve

All points use the same graph and exact rerank30 through the existing range cache. These are
single budget-sweep runs; the repeated comparison above is the final Qdrant gate. RSS includes
the process's retained graph and working cache, rather than the configured cap alone.

| Range-cache cap / sparse block | M4 q/s at 0.950174 / 0.961472 | M4 RSS | Graviton q/s at the same recalls |
|---|---:|---:|---:|
| No admission / 16 KiB | 2,622 / 2,505 | 4.12–4.13 GiB | 3,614 / 3,519 |
| 256 MiB / 16 KiB | 2,371 / 2,321 | 4.40 GiB | 1,533 / 1,531 |
| 256 MiB / 2 KiB | not measured | not measured | 807 / 812 |
| 1 GiB / 2 KiB | 4,799 / 4,753 | 4.92–4.93 GiB | 10,586 / 9,448 |
| 2 GiB / 16 KiB | 5,282 / 5,220 | 5.12–5.14 GiB | 11,267 / 9,912 |
| 4 GiB / 16 KiB | 5,332 / 5,228 | 5.58–5.62 GiB | 11,546 / 10,183 |

Small-cache admission and eviction can cost more than bypassing admission, even over warm local
files. A smaller block alone does not solve the shared-pool cliff. These runs repeatedly warm the
same test queries; they do not establish an unseen-query miss rate. RSS excludes some OS file
cache and is not a cgroup limit or total charged-memory result. No total-memory win is claimed.

### Bounded Vamana construction

The proposed distance-ratio boundary rule first needed calibration. Over normalized empirical
means of the existing 32 spatial slabs, a ratio of 1.15 duplicates 74.17% of rows, exceeding the
50% overlap ceiling. Ratios 1.03, 1.05, and 1.10 yield 23.65%, 36.09%, and 59.09%. This
prerequisite probe took 9.93 s and 52.9 MiB peak RSS.

`stitch_graph` implements two-pass Vamana (R=32, build L=64, alpha=1 then 1.2), reciprocal robust
pruning, and robust-pruned stitching. It holds one partition in RAM, spills other partitions,
and stores global adjacency in a scratch file. The final variant also encodes SQ8 per partition
and writes it in corpus-ID order, so no full-corpus encoding buffer is hidden outside the memory
measurement. Its streamed codes match a full ARM encoding byte-for-byte; cross-ISA codebooks
are not assumed bit-identical. Source slabs and spill files are warm local files in these runs.

| Boundary ratio | Extra row copies | Build time | Peak build RSS | Search result |
|---|---:|---:|---:|---|
| 1.03 | 23.65% | 295.36 s, adjacency only | 0.738 GiB | 0.930243 at 2,400 scores; 0.951333 needs 4,133 |
| 1.07 | 46.50% | 349.96 s, including SQ8 | 0.887 GiB | Four medoid entries: 0.953505 at 2,370 scores; 0.959849 at 2,826; 0.963760 at 3,268 |

The lower-overlap graph misses the 0.01 recall-gap gate relative to monolithic HNSW. The higher-
overlap graph recovers the quality/cost curve, stays below 1 GiB, and reaches both recall targets,
but misses the proposed 70 s construction target by 5x. Four medoid entries save roughly 32 scores
and barely alter recall. This establishes bounded construction feasibility, with slower build and
search than the monolithic graph; it does not demonstrate a bounded M4 constructor or a Qdrant win.
Local spill capacity, CPU amortization, foreground headroom, and better topology/layout remain
shipping work. No adjacency is added to `.bfa`, and no graph sidecar is published to S3.

### Outcome and validation

Keep the row-graph search core as an experimental hot-tier candidate; reject this packet variant.
Do not enable the current graph in the shipped server. The real-path Qdrant gate fails at both
recall targets. The proposed automatic lifecycle, budget reservations, generation checks, delta
handling, filtering, and optional persistence are documented in `docs/design/hot-tier-graph.md`.
The prototype uses explicit promotion and one process-global entry; automatic heat admission,
backend-scoped ownership, eviction, cancellation, and compact production storage are not implemented.

Tests cover budget refusal, source extraction, original row coordinates, live masks, WAL deletes
and upserts, uncovered segments, multiple tiers, filtered and zero-query fallback, exact rerank,
streamed construction primitives, and independent worker scheduling. Default and feature-enabled
crate gates are run before committing. A scheduling-sensitive cold-wave assertion failed once
under the parallel VM suite, then passed in isolation and in the serial default suite; no assertion was weakened.
The feature-enabled suite also passes. Results were retained before removing experimental data,
processes, staging objects, and cloud resources.

## Round C2: cold routing hints and corpus-scaled leaves

Tooling: `crates/bf-engine/examples/recluster_copy.rs` (an owned-copy upgrade and recluster example) and `cold_ab.py`.

## Routing data in the first response: killed

Owned in-region c7gd.2xlarge, 60 paired cache-cold queries, oracle indices 0–59,
64 probes, alternating arm order, and the existing C1 HTTP harness. Each arm
used 8 GiB payload memory, 512 MiB decoded metadata, 4,000-byte cache blocks,
512-request concurrency, and eight Rayon threads. Only the paged-directory
object bytes were supplied from a preloaded immutable local buffer at the
`paged_view` read boundary in the ideal arm. Header retrieval, page decoding,
page selection, assembly, cold flags, slab scans, reranks, and hydration remained
unchanged. Every query dropped the ordinary caches, and the buffer was loaded
outside the timed samples. The temporary hook is not in the commit.

| Arm | p50 ms | p90 ms | Recall@10 | Mean physical GET attempts | Mean rows scanned |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 688.313 | 816.881 | 0.951667 | 2,208.917 | 809,325.017 |
| Instant directory pages | 606.413 | 752.530 | 0.951667 | 1,796.233 | 809,325.017 |

All 60 pairs returned identical ordered IDs and distances. The ideal arm saves
81.9 ms at p50 (1.135x), but misses the required 550 ms cutoff by 56.4 ms.
It removes approximately 413 GET attempts and 12.30 MB per query; the remaining
roughly 1,796 GETs still dominate the opportunity. Per the brief, stop here:
there is no real-header implementation or new on-disk format to review.

### Corpus-scaled leaves: not qualified after confirmation

Target 640 rows per leaf, close to `0.2 * sqrt(10,000,000) = 632.46`.
The original 10M checkpoint contained legacy ANN objects, so the current
recluster builder initially rejected it. The owned copy was automatically
upgraded without retraining: 70 segment indexes, 32 slabs, and 70 document
objects, in 1,048.414 s. The unchanged-center, upgraded control contained 531
live objects and 55,798,359,779 bytes, copied in 50.911 s. The original copy had
704 live objects and 82,530,563,146 bytes. Existing prefixes stayed read-only.

The candidate rebuilt all 10M rows in 1,249.571 s: 15,611 clusters, 55 slabs,
and a 3,303,367-byte directory. The candidate used 32 coarse cells and a
250,000-row slab cap. The first complete, alternating cache-cold replay used
all 200 oracle queries:

| Arm | Probes | p50 ms | p90 ms | Recall@10 | Mean GET attempts | Mean planned rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original 200-row layout | 64 | 424.606 | 550.839 | 0.9615 | 660.590 | 245,659.015 |
| Upgraded 200-row control | 64 | 453.871 | 607.047 | 0.9615 | 629.975 | 245,659.015 |
| 640-row leaves | 64 | 313.577 | 466.450 | 0.9660 | 341.005 | 208,031.490 |
| 640-row leaves | 80 | 291.833 | 414.793 | 0.9730 | 369.635 | 258,297.005 |
| 640-row leaves | 96 | 311.282 | 445.323 | 0.9775 | 392.370 | 308,491.730 |

The original and upgraded controls returned identical ordered IDs and distances
for all 200 pairs. At 64 probes, the candidate is 1.354x faster than the original
and 1.447x faster than the upgraded control, with better recall and 0.847x rows.
It misses the GET gate: 1.937x fewer than the original, and 1.847x fewer than the
upgraded control. This does not hit a kill criterion. A follow-up tested 48 and
56 probes to spend the excess recall margin on fewer reads, using another 200
alternating cache-cold queries:

| Arm | Probes | p50 ms | p90 ms | Recall@10 | Mean GET attempts | Mean planned rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Upgraded control | 64 | 411.606 | 535.424 | 0.9615 | 629.675 | 245,659.015 |
| 640-row leaves | 48 | 288.569 | 448.719 | 0.9560 | 300.510 | 157,424.260 |
| 640-row leaves | 56 | 278.146 | 392.645 | 0.9620 | 312.360 | 183,033.660 |

48 probes loses too much recall. 56 probes provisionally clears every gate:
1.480x p50, 2.016x fewer GET attempts, 0.745x planned rows, and slightly better
recall. Gates treat recall tolerance as loss of at most 0.001; this candidate
also satisfies an absolute 0.001 tolerance.

An independent confirmation removed the other candidate settings and replayed
all 200 queries against both controls on the same instance:

| Arm | Probes | p50 ms | p90 ms | Recall@10 | Mean GET attempts | Mean planned rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original 200-row layout | 64 | 387.348 | 459.114 | 0.9615 | 657.620 | 245,659.015 |
| Upgraded 200-row control | 64 | 410.016 | 537.414 | 0.9615 | 625.705 | 245,659.015 |
| 640-row leaves | 56 | 301.129 | 396.045 | 0.9620 | 325.635 | 183,033.660 |

Against the original control, confirmation gives **1.286x p50**, below the
required 1.30x, and **2.0195x fewer GET attempts**. Against the upgraded control,
it gives **1.3616x p50**, but **1.9215x fewer GET attempts**, below the required
2.0x. Quality and rows reproduce exactly: all 200 candidate ID/distance lists
match between tuning and confirmation, and both controls match on all 200
queries in each replay. Candidate recall is 1,924/2,000 exact neighbors versus
1,923/2,000 for both controls. Planned rows are 0.745x the control.

**Do not promote or publish the provisional pass as a reproducible win.**
Neither prescribed hard-kill condition is hit: p50 gains exceed 1.2x, and
matched recall needs fewer rows. This is promising but unqualified by the full
acceptance gate. The existing leaf-size policy remains unchanged. Candidate
GET attempts rose 4.25% between tuning and confirmation; mean issued hedges rose
from 18.84 to 28.56, while results and planned rows stayed identical. The narrow
GET margin is insufficient for promotion. Confirmation fetched 40.83 MB/query
for the candidate, versus 62.02 MB for the original and 67.16 MB for the upgraded
control. Across the four measured replays, physical GET attempts totaled
1,289,325, excluding warmups and preparation.

The candidate recomputes centers and assignments with the current builder,
and its slab cap produces 55 slabs versus 32 in the preserved control. A fresh
200-row build under identical options would isolate leaf size from historical
builder changes and slab partitioning. The results compare rebuilt layouts.
No 100M large-leaf rebuild or production-policy change was attempted. A later
round would need more GET headroom and the same-builder control before changing
the default. No edits in other crates are required by the retained tooling.

### What was built and its interface

- `crates/bf-engine/examples/cold_ab.py` accepts a per-arm `CONFIG` path in the
  arms JSON, requires matching namespace names, and continues to force read-only
  storage. Arms can override `BF_ANN_NPROBE`. Results and summaries include
  `ann_rows_scanned`, obtained from trace records; builds without those records
  report the field as unavailable.
- `ann_dir` emits `PLAN rows=N` under the existing `BF_TRACE_STAGES` switch.
  Query behavior and the wire API are unchanged.
- `recluster_copy storage.json namespace cluster_rows scratch_dir [--upgrade-only]`
  upgrades and rebuilds an owned benchmark copy. It restricts writes to simple
  S3 `tmp/<owner>/...` prefixes, rejects traversal and URL escapes, requires a
  `.scratch/` directory, and disables background indexing. `--upgrade-only`
  prepares a lossless control before the candidate rebuild. It uses existing
  `Engine::reindex` and `Engine::recluster` APIs, with 32 coarse cells and a
  250,000-row slab cap. JSON output includes rows, slabs, clusters, directory
  bytes, seconds, and the requested leaf size.

### Validation

All checks ran on a separate owned c7g.2xlarge, without measurement contention:
`cargo fmt --check`, `cargo clippy -p bf-engine --all-targets -- -D warnings`,
`cargo test -p bf-engine` (183 passed, four existing tests ignored), and
`cargo test -p bf-engine --example recluster_copy` (one passed). The new guard
test rejects existing scale prefixes, traversal, escapes, empty owners, and
non-S3 URLs. The Python harness passed `--help` and an AST parse through `uv`.
The checked diff contains no private settings, credentials, or hostnames.

The checks host was terminated immediately after verification. The measured
results and all gate decisions are recorded above before deleting scratch data.

## Round B6: exact random-access completion, and bitmap score bands

Both offline kill tests fail. B6 ships no production code, public API, dependency,
query option, or text format. The failed prototypes are removed from the final
source tree; implementation, tests, and reproduction notes remain in `eae8ece`
and `83c6574` (`crates/bf-text/examples/b6`).

### Corpus and paired protocol

The first 1,000,000 passages from `msmarco_load.py`'s stream form five consecutive
200,000-row, format-7 segments, built by `experiments::prepare_msmarco`. The workload
is all 1,000 checked-in MS MARCO dev queries, with default BM25 parameters and
limit 10. An independent `exact_bm25` run validates all 10,000 ordered hit IDs and
f32 score bits. Every warm and measured candidate pass compares against those
validated production results.

All arms compile natively on one owned, on-demand c7g.2xlarge, use the same final
release executable, and run pinned to CPU 3. Builds, indexing, and truth generation
finish before counters start. Each invocation verifies truth, warms the complete
workload, and counts the next pass using B5's perf control/ack FIFOs. Values below
are medians of two complete invocations per arm. Counters measure instructions and
cycles in user mode, without multiplexing. Two hardware-counter reads per query
provide p90 cycles, and their cost is included in the outer counters.

| Arm | M instructions/query | M cycles/query | p90 M cycles/query |
| --- | ---: | ---: | ---: |
| Existing reader | 18.230 | 11.485 | 28.102 |
| Impact threshold algorithm, with forward TF completion | 16.658 | 16.780 | 44.483 |
| Four scalar bitmap bands | 19.163 | 11.146 | 26.105 |
| Eight scalar bitmap bands | 19.083 | 11.739 | 28.197 |
| Four bands, direct bitmap intersections | 19.433 | 11.501 | 27.398 |
| Eight bands, direct bitmap intersections | 20.402 | 12.061 | 28.745 |

The final executable's SHA-256 is
`f06618000d28167b2b0727d23644f9c63030e281756291059da0402afe5d54ef`.
The paired baseline differs from B5's historical 20.581M instructions/query;
comparisons use this executable's baseline. No improvement is inferred from a
changed driver or release code generation.

### Threshold algorithm with forward TF: rejected

Query-term postings sort by exact saturation under the current namespace
normalization, and form 128-row impact blocks. BitPacker4x packs their absolute
row IDs. Each surfaced candidate is completed once from its document's forward
TF slice, scored in canonical term order, and inserted into a heap shared across
segments. Summing the remaining term maxima in canonical f32 order bounds
untouched documents; strict termination retains score ties.

The forward index stores row offsets and lexical term-ID deltas. A low bit folds
in the common tf=1; other TFs use unsigned LEB128. Completion decodes the actual
compressed bytes during queries. Saturation-sorted impact blocks and exact maxima
are prepared outside counters, giving this warm experiment favorable conditions.
Only workload terms need experimental impact lists; the forward index includes
**every term in every document**.

| Forward TF encoding | Bytes, including row offsets |
| --- | ---: |
| Lexical term-ID deltas, used for measured completion | 100,100,908 |
| Frequency-ranked IDs, also including the ID mapping | 77,773,764 |

Both measured encodings miss the 65 MB footprint gate. The corpus contains
39.714 distinct terms/document, rather than the proposed roughly 35. The ranked
alternative is a footprint diagnostic; its query completion is not measured.

The algorithm reads 114.140 impact blocks/query, but completes **13,331.462
candidates/query**, far above the proposal's 400–1,200. Reduced sorted access
does not make completion cheap enough. Instructions exceed the 12.0M kill gate,
mean cycles rise 1.46×, and p90 cycles rise **1.583×**, exceeding the 1.2× kill
gate. Random access resolves the missing-contribution correctness problem, but
the tested completion index fails both the CPU and footprint requirements.

### Frequent-term bitmap bands: rejected

The namespace's 32 most frequent terms receive four or eight disjoint bitmaps of
upward-rounded saturation bounds. During normal window selection, the prototype
combines those frequent-term row bounds with exact sparse partials and other dense
terms' window maxima. Empty windows are removed before posting prefetch; surviving
rows retain the original scorer, bounded decoded caches, and shared threshold.
No query-truth floor is supplied.

Eight scalar bands reduce posting reads from **663.770 to 495.275 blocks/query**
(25.4%), and fully scored rows from **5,364.998 to 4,117.256/query**. Four bands
read 509.794 blocks and score 4,163.172 rows/query. Bound evaluation costs more
instructions than the saved posting/scoring work. Four bands improve p90 cycles,
but still fail both absolute mean gates.

A second implementation directly intersects the disjoint bitmap bands, pruning
recursive intersections with suffix score bounds. It uses the sparse window
maximum to avoid per-row bound accumulation. Those looser bounds leave
654.372 blocks/query with four bands, and 642.218 with eight. Direct bitmap
combination also fails: all bitmap variants exceed **15.9M instructions** and
**8.34M cycles/query**. No bitmap layout or production hook ships.

### Validation and limits

Native ARM formatting, `cargo clippy -p bf-text --all-targets -- -D warnings`,
and `cargo test -p bf-text` pass: **42 tests**, with eight corpus experiments
ignored. The historical example adds its own checks: **16 pass**, with eight
embedded corpus experiments ignored. New tests compare forward decoding and
reference score bits, and exercise repeated terms, missing terms, ties, changed
namespace averages, and both bitmap combinations. A 12K-row case forces frequent
terms through the bitmap pruning path, and verifies that rows are actually pruned.
Final bf-text checks pass again after removing the prototypes.

This is an optimistic warm library experiment. Normalization-specific impact
order, maxima, and bands are prepared outside counters; actual impact decoding,
forward decoding, and query-bound evaluation are charged. There is no HTTP,
engine, cold-I/O, SDK, M4, or real-S3 measurement. No format publication or update
lifecycle is implemented. The existing BM25 CPU target remains open.

## Round Q2: warm query dispatch after X1

Measured on 2026-11-24 on one owned Spot c7g.4xlarge. Eight Graviton3 CPUs run
the server, and the other eight run sixteen independent keep-alive clients.
Both binaries use Rust 1.100.0, Clang 19, and the LLVM 22 linker. The baseline is
X1's `3e12797`; its production source is identical to merged main `ab4172d`.
The candidate is built on `ab4172d`. This is a paired ARM server experiment,
not an Apple M4 or Qdrant comparison.

The standard `bench/nq/load.py` loader builds a new store from the prepared NQ
data, and measurements wait for the final automatic recluster. Its immutable
single tier contains 2,681,468 f16 vectors, 1,024 dimensions, 32 slabs, and
13,391 leaves. This layout differs slightly from X1's historical store; compare
arms within this round. Each process warms all 3,452 queries three times, then
measures four full epochs. Requests use strong consistency, top ten, and ID-only
projection. Raw and decoded cache budgets are each 4 GiB, with no disk cache,
indexing, or recall sampling. Linux counters cover all server threads, including
kernel work, unless explicitly marked user-only below.

### Retained dispatcher and admission policy

Warm plans run their first poll on a fixed, CPU-sized Rayon pool. A resident plan
returns its result directly through a oneshot. If the plan yields, the same pinned
future resumes through the existing blocking-pool/runtime continuation. It keeps
its I/O, locks, scratch, and execution state. Dispatch adds no separate cache-only
execution probe and does not restart the planner on a miss. Cold namespace snapshots retain the original path.
Metrics distinguish `ready`, `yielded`, and `cold` dispatches through
`blowfish_query_cpu_dispatch_total`.

The default admits `max(8, 2 * available_parallelism())` query drivers. Admission
counts drivers waiting for I/O or a CPU worker, rather than compute threads alone.
The fixed CPU pool limits simultaneous resident computation. The existing
`BF_QUERY_CONCURRENCY` resource cap remains available: the proposed exact-one-
permit-per-core rule lost throughput on four cores, and admission has latency and
throughput tradeoffs. No new setting, public method, wire type, or format is added.

The dispatcher-only gate orders baseline/candidate/candidate/baseline at each probe
setting; the separate cache-retention candidate is measured below.
Each table cell reports the median of four epochs for each independent process.
All ordered top-ten rows, including distances, match the unchanged baseline in
every epoch. Recall remains 0.9501738123 / 0.9610950174 at 48 / 64 probes.

| Probes | Baseline q/s, two passes | Final q/s, two passes | Baseline / final instructions/query, two passes |
|---:|---:|---:|---:|
| 48 | 1,832 / 1,837 | 2,153 / 2,156 | 20.9632 / 20.9675 M; 20.9611 / 20.9668 M |
| 64 | 1,585 / 1,599 | 1,859 / 1,861 | 24.2283 / 24.2384 M; 24.2286 / 24.2365 M |

| Probes | Baseline p50 / p90, two passes (ms) | Final p50 / p90, two passes (ms) |
|---:|---:|---:|
| 48 | 7.83 / 11.70; 7.78 / 11.50 | 6.96 / 10.19; 7.02 / 10.32 |
| 64 | 9.11 / 13.76; 9.00 / 13.36 | 8.09 / 11.73; 8.10 / 11.67 |

Mean process-median throughput rises **17.4% / 16.8%**. The **1.30× warm gate is
not met**, and this round establishes no win over Qdrant on the M4. The simpler
positive change is retained under the brief's smaller-win allowance. Instructions
are effectively unchanged. At 48 probes, context switches fall from 6.25–6.48 to
5.09–5.10 per query, and migrations from 2.03–2.08 to 0.68–0.73. Cycles move only
from 8.49–8.51 M to 8.42–8.47 M. The main gain is better delivery of CPU work,
rather than a large reduction in executed instructions.

### Isolated arms and kill results

Each arm below was compared with unchanged X1 on the same store and CPU masks.
These are screening results, rather than substitutes for the final paired table.

| Arm | 48 / 64 probe q/s | Decision |
|---|---:|---|
| Typical unchanged X1 | 1,830–1,850 / 1,590–1,600 | Control |
| Synchronous resident work on Tokio reactors | 445 / not retained | Reject; p90 rises to 39.47 ms at 48 probes |
| Published immutable namespace snapshots | 1,863 / 1,545 | Reject; +1.9% / -3.3%, with negligible instruction savings |
| Specialized resident CPU pool and projection, sixteen permits | 2,222 / 1,916 | Reject duplicate planner/projection logic; generic continuation is simpler |
| Generic continuation, eight permits | 2,057 / 1,797 | Positive dispatch control |
| Generic continuation, sixteen permits | 2,158 / 1,865 | Retain, subject to final gates |
| CLOCK replacement for the sparse exact-vector cache | 980 / 816 | Reject; compiled-hit admission collapses |
| Shared request ownership instead of the two request clones | 1,848 / 1,588 | Reject; approximately 0.004 M instructions saved, with no reliable throughput gain |
| TCP_NODELAY on the API listener | 1,834 / 1,600 | Reject; within paired variation |

The source audit finds three namespace mutex acquisitions, three namespace-state
clones, and two query-request clones on the usual speculative warm strong path.
Their existence does not establish a large cost: published snapshots and request
sharing fail to produce a consistent gain. The HTTP route already serializes a
typed response; it does not round-trip the entire response through a JSON value.
Row attribute maps remain dynamic. The existing empty-state validation pass in
`indexed::execute` also remains; safely splitting validation from evaluation
would require preserving all multi-query and error semantics. These paths are
unchanged; the specialized bypass was not retained.

At four server cores, changing eight permits to four reduced throughput from
994 / 879 to 936 / 813 q/s at 48 / 64 probes. At ten cores, changing eight to ten
increased throughput from about 2,177 / 1,869 to 2,302 / 1,972, while slightly
increasing p90. Sixteen permits on the eight-core unchanged server improve
throughput to 1,970 / 1,711 but worsen p90 to 12.93 / 14.98 ms. Admission alone
does not account for the dispatcher's latency improvement.

A cancellation-safe coalescer assigned each strong caller a generation after the
check already in flight, preserving the required arrival ordering. Its unit tests
and concurrent acknowledged-write/read stress passed. File-backed warm throughput
improved only about 2–3%. With sixteen clients, one cached row, and a controlled
10 ms WAL HEAD delay, independent checks achieve 1,390 q/s and 12.04 ms p90;
coalescing achieves 955 q/s and 23.38 ms. Yielding before the next generation
does not repair that latency regression. The coalescer is removed. Single-writer
storage mode alone does not prove that an engine owns the WAL head: another engine
can share the storage, and read-only replicas still need checks. Safe skipping
would require shared storage-level ownership information, outside this change.

Four-query groups replayed from the real sixteen-client arrival order share
11.44% / 14.31% of selected leaf-code bytes at 48 / 64 probes. Perfect elimination
of repeated loads yields only 1.129× / 1.167× even if loading accounts for all
query cost. Queue delay alone has p90 2.96 / 3.35 ms. This kills the load-reuse-only
case before implementing a scheduler or kernel. It does not bound independent
arithmetic improvements from a new batched kernel; no four-query scoring kernel
is shipped or claimed to pass the 1.30× batch gate.

### Request shapes, cold queries, and fixed cost

A disposable 10K-row namespace adds scalar `category` and `label` attributes to
the same 1,024-dimensional corpus vectors. All complete rows and distances match
for top ten IDs, top ten projected attributes, top 100 projected attributes, and
scalar ordering. Screening throughput increases from 3,776–4,020 to 4,192 q/s,
3,715–3,799 to 3,995, 1,077–1,116 to 1,241, and 1,189–1,205 to 1,369,
respectively. These are real indexed queries, including scoring and projection;
the scalar-order case is not a zero-work wrapper benchmark. Recall against the
full corpus is not meaningful for this smaller namespace.

The final shape comparison uses 96 seeded cases, repeated three times per binary:
80 successes and 16 expected errors. Warm final responses match for filters,
eventual consistency, grouping, RRF, scalar ordering, vector/base64 projection,
scalar attributes, offset, invalid limits, and unknown attributes. The Rust suite
adds sixteen readers checking 4,096 strong reads against 128 acknowledged writes.

The dispatcher-only cold gate alternates AB/BA on 30 queries per probe setting,
starting a fresh
server for every request. The standard local S3 facade adds 25 ms per backend read.
It models fixed read latency, not real S3 tail behavior. All ordered rows and
distances match.

| Probes | Baseline p50 / p90 (ms) | Final p50 / p90 (ms) | Paired median change, 95% bootstrap interval (ms) |
|---:|---:|---:|---:|
| 48 | 204.52 / 249.48 | 201.99 / 212.66 | -0.20 [-3.79, +2.74] |
| 64 | 205.91 / 263.88 | 205.69 / 215.45 | -1.33 [-3.28, +2.79] |

Twenty thousand paired resamples detect no cold latency regression; this small
sample does not rule out a small change. Mean backend reads are 134.93 / 128.00 at
48 probes and 134.97 / 136.90 at 64, baseline / final; mean transferred bytes are
126.81 / 125.14 MB and 147.86 / 147.74 MB. Asynchronous read timing makes these
counts variable; read-count identity is not claimed. Cold queries retain the
ordinary planner and the original blocking-pool dispatch.

The fixed-cost control distinguishes a warm block cache from a completed compiled
plan. On 10K rows, 512 distinct queries thrash X1's exact-vector cache, whose byte
budget equals the much smaller code payload. The ordinary rerank remains visible
in the instruction profile. Native scoring takes 2.8669 M user instructions and
0.8383 M user cycles/query; HTTP takes approximately 6.89 M total instructions.
That difference includes fallback scoring and cache work, so it is not a fixed
HTTP tax. Every native ordered ID list matches HTTP, including the full NQ corpus.

A repeated-first-vector control fits the sparse cache and isolates a genuinely
compiled resident working set. Its counters cover 100% of runtime; user and
kernel events are collected separately to avoid PMU multiplexing. Native scoring
runs 100 epochs, and HTTP counters use twelve 512-request epochs after warming.
All ordered IDs match. The repeated-vector fixture's full-corpus recall is not a
quality measurement.

| Repeated-vector path, 48 probes | User instructions/query | Kernel instructions/query | User / kernel cycles/query | q/s |
|---|---:|---:|---:|---:|
| Native scorer, eight workers | 2.3168 M | not counted | 0.6553 M / not counted | 31,620 |
| X1 HTTP, sixteen clients | 3.0713 M | 0.0981 M | 1.2514 / 0.1651 M | 9,016 |
| Dispatch-only HTTP, sixteen clients | 3.1035 M | 0.0959 M | 1.2320 / 0.1418 M | 9,876 |

The native/HTTP instruction difference in this control is about 0.85–0.88 M,
including attached-thread kernel work. It is not a universal 2.15 M constant.
The native and server scorers also have different specializations, so subtracting
their counters is not a strict phase decomposition. Neither this ARM IVF result
nor the older full-NQ subtraction invalidates the separate M4 graph measurement.

Instruction sampling uses a period of 500,003 user instructions, DWARF stacks,
and zero lost samples. The following mutually exclusive function-family groups
are estimates, not instrumented phase boundaries. Allocator leaves take priority;
inlined/shared validation, projection, and synchronization can remain in the
engine or unresolved groups. The standalone profile summarizer preserves these
rules for reproduction.

| Named function family | X1, 512 distinct vectors | X1, repeated vector | Dispatch-only, repeated vector |
|---|---:|---:|---:|
| JSON input | 6.12% | 13.31% | 13.08% |
| Named validation helpers | 0.02% | 0.06% | 0.03% |
| Other engine and planning | 2.09% | 3.91% | 2.44% |
| Snapshot and consistency | 0.52% | 0.84% | 0.85% |
| Hydration and projection | 1.34% | 1.83% | 1.07% |
| JSON output | 0.24% | 0.57% | 0.52% |
| Allocator | 1.91% | 1.09% | 1.02% |
| CPU and runtime dispatch | 0.25% | 0.46% | 1.40% |
| HTTP and async plumbing | 0.85% | 1.95% | 1.98% |
| Cache and range reader | 8.95% | 3.81% | 2.54% |
| Vector search | 73.17% | 70.19% | 73.23% |
| Shared or unresolved | 4.53% | 1.98% | 1.83% |

In the distinct-vector profile, the largest exclusive functions are `block_sums`
(15.59%), `distance_unchecked` (15.39%), ordinary `rerank_recorded` (9.26%),
`PlaneQuery::reset` (4.94%), JSON `parse_number` (4.85%), memcpy (4.52%), and
`extend_f16` (4.46%). The cache/fallback path is the large difference. Float parsing
alone costs approximately 0.33 M user instructions/query in this case: making it
twice as fast would save roughly 0.16 M, about 2.4% of user instructions. A perfect
parser cannot remove the remaining vector, cache, kernel, or scheduling costs.
This is a CPU-cost estimate, not a measured throughput improvement.

The retained cache fix reserves every exact-vector payload, descriptor, and cache
entry when that full reservation plus code and metadata fits the existing decoded
budget. It otherwise retains X1's code-sized sparse reservation. It does not fetch
extra rows. A regression warms 32 distinct, nonzero queries on a small tier and
requires every supported plan to remain compiled on the next pass; zero-vector
and oversized-tie fallback correctness checks remain separate.

The isolated policy comparison uses the same dispatcher and sixteen permits on
both sides. With 512 distinct queries, the small namespace improves from
**4,067 to 8,163 q/s (2.007×)**, p50 3.31→1.72 ms, p90 5.57→2.70 ms, instructions
6.9217→3.7714 M/query (-45.5%), and cycles 3.7854→1.7911 M. All complete rows and
distances match. The larger reservation stays inside the configured cache budget;
it trades otherwise unused budget for retaining the observed exact working set.

A clean final baseline/candidate/candidate/baseline gate, after the independent
prepared-data transfer finished, measures the dispatcher and cache policy together:

| Probes | X1 q/s, two passes | Final q/s, two passes | X1 / final instructions/query, two passes |
|---:|---:|---:|---:|
| 48 | 1,828 / 1,835 | 2,148 / 2,146 | 20.9615 / 20.8774 M; 20.9588 / 20.8757 M |
| 64 | 1,580 / 1,587 | 1,889 / 1,882 | 24.2281 / 24.1134 M; 24.2297 / 24.1152 M |

| Probes | X1 p50 / p90, two passes (ms) | Final p50 / p90, two passes (ms) |
|---:|---:|---:|
| 48 | 7.89 / 11.82; 7.87 / 11.67 | 6.94 / 10.25; 6.92 / 10.12 |
| 64 | 9.16 / 13.42; 9.12 / 13.35 | 7.99 / 11.61; 7.95 / 11.67 |

Final throughput increases **17.2% / 19.1%**, with every ordered row and distance
identical and recall unchanged. The **1.30× gate still fails**. Full NQ's complete
exact-vector reservation exceeds 4 GiB, so it keeps the sparse policy. The small
instruction differences between separately linked binaries are not credited to
the small-tier cache fix. A second 96-case shape comparison again gives 80 matching
successes and 16 matching expected errors.

The corrected varied-query resident control takes 3.6641 M user instructions and
0.1046 M kernel instructions/query, versus 2.8669 M user instructions in the native
scorer. User / kernel cycles are 1.6361 / 0.1652 M, versus 0.8383 M user cycles
natively. All counters have 100% coverage, and native ordered IDs match. The
profile run measures 7,954 q/s before sampling; sampling overhead is excluded from
the throughput claim above. No sampled ordinary `rerank_recorded` work remains.

| Corrected varied-query function family | User instruction samples |
|---|---:|
| Vector search | 75.82% |
| JSON input | 11.14% |
| Cache and range reader | 3.02% |
| Other engine and planning | 2.11% |
| HTTP and async plumbing | 1.55% |
| CPU and runtime dispatch | 1.30% |
| Hydration and projection | 1.10% |
| Allocator | 0.86% |
| Snapshot and consistency | 0.76% |
| JSON output | 0.44% |
| Named validation helpers | 0.04% |
| Shared or unresolved | 1.85% |

`parse_number` now represents 9.00% of user instructions, still approximately
0.33 M/query. The roughly 0.90 M native/HTTP difference includes the wrapper,
cache access, specialization differences, and attached-thread kernel work. This
explains why a cache-missing small control overstates the fixed tax; it does not
establish an index-independent constant. Both the repeated-vector and repaired
varied-vector controls are useful alongside the full-corpus result.

The final first 30-query cold sample has p50 204.76 / 202.32 ms and p90
218.27 / 229.03 ms at 48 probes, baseline / candidate. At 64 probes, these are
207.65 / 210.27 ms and 219.37 / 275.78 ms. Paired median changes are -1.51 ms
[-4.55, +2.23] and +1.37 ms [-3.01, +4.67]. Paired p90-change intervals are wide:
[-21.90, +71.92] / [-50.16, +70.80] ms. Because the point tail worsens despite
stable medians, the cold check is expanded to 60 query pairs per probe setting.

The isolated 60-pair confirmation completes after the scoped checks, with no
concurrent build, test, transfer, or profiling load. All ordered rows and distances
match. Median changes remain within uncertainty, and no cold regression is
detected. The p90 interval at 64 probes remains broad; this sample does not prove
a tight bound on cold tails.

| Probes | Baseline p50 / p90 (ms) | Final p50 / p90 (ms) | Paired median change, 95% interval (ms) | Paired p90 change, 95% interval (ms) |
|---:|---:|---:|---:|---:|
| 48 | 204.76 / 251.38 | 203.64 / 212.51 | +0.34 [-2.80, +1.51] | -38.87 [-61.76, -4.15] |
| 64 | 206.89 / 219.06 | 207.50 / 219.08 | +0.16 [-1.37, +2.78] | +0.02 [-41.14, +18.96] |

A final attached-thread kernel instruction profile uses period 50,003, frame-pointer
stacks, 10,395 samples, and zero lost samples. Rounded leaf-symbol families assign
approximately 15.2% to network functions, 24.2% to scheduling and shared
synchronization, 6.3% to filesystem/metadata functions, and 54.2% to shared or
unresolved work. These estimates are normalized after rounding. The largest named
leaves are `arch_local_irq_enable` (19.59%), `try_to_wake_up` (9.13%),
`__arch_copy_to_user` (7.23%), `el0_svc` (6.32%), and `__sched_yield` (4.23%).
This is CPU attribution within attached server threads, not an off-CPU wait
breakdown or whole-host network/softirq accounting.

### Reproduction and checks

`crates/blowfish/benches/server_tax.py` imports the standard NQ HTTP adapter and
request builder. It records full rows, per-request latency, arrivals, and per-epoch
hardware counters. Its default run measures the complete query set; `--count`,
`--namespace`, `--attrs`, and `--top-k` select the small controls. Use the same
server CPU affinity, frozen store, cache settings, and binary build settings for
both arms, and alternate AB/BA. Outputs must stay under `.scratch`.

```sh
uv run --project bench/nq python crates/blowfish/benches/server_tax.py \
  --data "$DATA" --pid "$SERVER_PID" --out .scratch/q2/arm.json
uv run --project bench/nq python crates/blowfish/benches/server_tax_verify.py \
  --mode cold --store "$STORE" --data "$DATA" --baseline "$BASELINE" \
  --candidate "$CANDIDATE" --out-dir .scratch/q2/cold
cargo run --release -p bf-engine --example resident_ceiling -- \
  "$STORE" small "$QUERIES_512_F32" 8 48 100 .scratch/q2/native.ids
```

The verification script also supports `--mode shapes`. For the fixed-cost control,
use `--namespace small --count 512 --perf-events instructions:u,cycles:u`, repeat
with kernel-only events, and use `--same-query` for the sparse-cache-fitting
control. Record a warmed process with the appropriate Linux perf privileges:

```sh
sudo perf record -e instructions:u -c 500003 --call-graph dwarf,8192 \
  -p "$SERVER_PID" -o .scratch/q2/profile.data
sudo perf script -i .scratch/q2/profile.data > .scratch/q2/cache-final.stacks
uv run --no-project python crates/blowfish/benches/server_tax_profile.py .scratch/q2
```

Save stacks as `baseline-x1.stacks`, `final.stacks`, or `cache-final.stacks`
alongside matching counter files. The summarizer reports user-space attribution
separately from kernel counters. The `server_tax_fixture`
example creates the small namespace only under `.scratch`; `leaf_reuse` computes
the code-byte reuse and queue-delay screening bound from recorded arrivals.
`strong_head_latency_benchmark` is an explicitly ignored, reproducible 10 ms
consistency-check control. The resident ceiling reports user-only instructions
and cycles around the warmed query loop.

Final formatting, scoped all-target clippy with warnings denied, and scoped tests
pass on ARM: 210 passed, zero failures, and five explicit historical/controlled
ignores. SHA-256 checks confirm that every changed Rust source file matches the
checked source. Python syntax checks, undefined-name/import checks, full-corpus
comparisons, seeded request-shape comparisons, and the isolated cold confirmation
also pass. No full-workspace or M4 validation is claimed.

The owned c7g.4xlarge, its root volume, its temporary key pair, and its security
group are removed after measurements. No S3 objects or paid oracle namespaces are
created. Scratch results are discarded after the retained numbers enter this
record.

## Round RR2: recluster scheduling under bulk loads

## Retained changes

- `d208436`: remember each namespace generation's advertised backpressure retry deadline on its
  handle. Both pipelined writes and barrier writes record 429s. Deadlines use a monotonic clock,
  include whole-second Retry-After rounding and the ordinary two-second quiet allowance, and only
  extend when another 429 arrives. Reclustering waits for outstanding retry deadlines or a WAL at
  its configured backpressure limit. Checkpoint refreshes preserve the hint; namespace recreation
  does not inherit it.
- `00e63f6`: compaction and reclustering use the same activity predicate. Compaction stays deferred
  across retry pauses, including when the tail has been fully indexed. Forced compaction, protected
  sources, and the existing segment-count cap retain their behavior.
- `2aa772a`: track queued write and delete operations through their final reply, including staged
  WAL work. The activity token belongs to the operation, so caller cancellation does not release
  it prematurely. The ordinary quiet interval starts at actual operation completion. This extra
  signal applies only to recently classified bulk namespaces, preserving ordinary steady-write
  behavior. A private operation pattern in snapshot hydration was updated for the token.

The first single-assignment trial exposed an incomplete base over 1,830,000 rows while clients were
still loading. A traced repeat received four 15.9–16.4-second retry delays. With the correction, the
first base covered all 2,681,468 rows. A fixed five-second debounce would not have covered those waits.
The preliminary incomplete trial is excluded from comparisons.

The eight-client control exposed a second cause without any 429s: a first base over only 730,000
rows. Namespace `updated_at` is captured during write preparation, before WAL serialization, upload,
and commit. Its p50/p90 write latency was 2.49/4.30 seconds, so a two-second test of that timestamp
was not a truthful quiet interval. Queued activity and completion-based quiet fix this without changing
wire timestamps. The final eight-client repeat starts one first base over all 2,681,468 rows.

Public APIs and persistent formats are unchanged. The retry hint and the extra private tier-selection
argument are internal to bf-engine. There are no dependency or bf-vector changes.

### Paired measurements

One owned Spot c7gd.2xlarge: eight Graviton3 cores, 16 GiB RAM, and local NVMe. Native Rust 1.100.0,
Clang 19 C compilation, and Clang 22 linking with compiler-rt; no native Rust CPU flags. All compilation
and tests ran on the instance, with no compilation competing with timed loads or builds.

Full NQ: 2,681,468 f16 vectors, 1,024 dimensions, cosine, and all 3,452 quality queries at top_k 10.
The supplied prepared data was copied read-only. I2 prepares 5,000-row request bodies once, and timing
includes body reads, writes, and durable acknowledgments. Every run uses a fresh local store, a 4 GiB
payload cache, no disk cache, and cleared OS page caches before loading. Global Rayon, Tokio, and the
recluster pool each use four threads on the eight-core host. Server and loader use nice 19.

No foreground queries compete with timed ingest or building. Quality sweeps run afterward in separate
server processes. Absolute times and RSS therefore differ from RR1's query-interleaved run; the paired
comparisons here are the evidence. Readiness is sampled every 250 ms after the loader's final return.
A scratch I2 copy adds retry timestamps without changing request bodies or retry behavior.

| Arm | Clients | I2 load, s | Final acknowledgment to full tier, s | Full build, s | Recall@10, 48 | Recall@10, 64 |
|---|---:|---:|---:|---:|---:|---:|
| Original main | 4 | 168.062 | 222.688 | 217.715 | 0.951130 | 0.961298 |
| Single assignment, 64 samples | 4 | 161.030 | 161.525 | 156.579 | 0.949276 | 0.959994 |
| Single assignment, 96 samples | 4 | 156.464 | 187.868 | 179.203 | 0.949594 | 0.959762 |
| Single assignment, 112 samples | 4 | 168.576 | 200.556 | 194.251 | 0.948667 | 0.958575 |
| Retry/compaction fixes, A (pre-final) | 4 | 161.360 | 224.119 | 218.596 | 0.950521 | 0.961530 |
| Retry/compaction fixes, B (pre-final) | 8 | 184.099 | 265.828 | 215.392 | 0.951072 | 0.961559 |
| All three fixes, A | 4 | 149.575 | 231.872 | 225.384 | 0.950116 | 0.960747 |
| All three fixes, B | 8 | 169.756 | 219.450 | 214.197 | 0.949594 | 0.959907 |

The acceptance gate is a loss of at most 0.001 against the paired original main at **both** probe counts,
plus readiness ≤198 seconds. The 64-sample arm loses 0.001854 / 0.001304; 96 loses 0.001535 at both;
112 loses 0.002462 / 0.002723 and also misses time. None ships. Historical RR1 thresholds alone would
have accepted the 64-sample arm. Later unchanged-algorithm controls also expose variation at this
scale, so these strict gate failures are conservative rejections, not proof that every observed loss
is caused by single assignment. No sampling change is shipped.

| Arm | Leaf training, s | Leaf assignment total, s | Post-acknowledgment RSS peak, GB | Whole-process HWM, GB |
|---|---:|---:|---:|---:|
| Original main | 21.354 | 153.911 (two passes) | 1.395 | 2.296 |
| 64 samples | 38.111 | 75.471 | 1.944 | 4.131 |
| 96 samples | 62.375 | 73.140 | 3.321 | 3.554 |
| 112 samples | 72.734 | 78.015 | 2.295 | 4.201 |
| Retry/compaction fixes, A | 20.878 | 153.677 (two passes) | 0.919 | 4.191 |
| All three fixes, A | 21.800 | 158.638 (two passes) | 2.286 | 4.079 |
| All three fixes, B | 20.595 | 149.164 (two passes) | 1.563 | 2.667 |

RSS sampling includes the last indexer drain between acknowledgment and build start. Different
asynchronous flush and compaction layouts affect peak memory; these are not isolated training-RSS deltas.

The pre-final retry/compaction-only four-client load has ten 15–17-second retries, versus zero in
the original main control.
It improves acknowledged throughput from 15,955 to 16,618 rows/s (1.042x), with write p90 1,960.7 to
1,277.5 ms. This is one end-to-end pair, not a confidence interval. Post-acknowledgment readiness is
essentially unchanged and misses 198 seconds. The first base uses 30 source segments, versus the
original control's more fragmented layout, and covers the full corpus.

The final four-client load improves acknowledged throughput from 15,955 to 17,927 rows/s (1.124x),
and write p90 from 1,960.7 to 807.5 ms, with twelve 15–17-second retries. Its first base covers the full
corpus over 24 segments. Readiness is 231.872 seconds versus original main's 222.688 seconds; this
round does **not** claim a readiness speedup for the four-client profile or meet 198 seconds.

At eight clients, actual queued-write tracking removes the 730K intermediate base and its later merge.
Within that same eight-client profile, load improves 184.099 to 169.756 seconds, and full-tier readiness
265.828 to 219.450 seconds. Both runs have zero 429s. The final run's only first build covers the full
corpus over 100 segments. The pre-final row's 215.392-second build is its final merge, excluding the
75.294-second intermediate base; its readiness includes that scheduling penalty.

The four- and eight-client runs within each phase use **identical binaries and unchanged clustering**.
Eight clients deliberately change batching/retry behavior for spread testing; eight-versus-four-client
throughput is not a claimed compaction effect. These are individual end-to-end measurements, not
confidence intervals.

#### Control recall spread

- The pre-final unchanged-algorithm pair differs by 0.0005504 / 0.0000290 (19 / 1 neighbor hits).
- The final unchanged-algorithm pair differs by 0.0005214 / 0.0008401 (18 / 29 hits).
- Across original main and all four unchanged-algorithm scheduler controls, the range is
  0.0015353 / 0.0016512 (53 / 57 hits).
- Excluding the pre-final eight-client run that trained coarse cells on a 730K prefix, the four
  full-corpus-first-base controls still span 0.0015353 / 0.0016222 (53 / 56 hits).

This confirms that fresh-load layout variability reaches or exceeds the 0.001 gate even when clustering
code is unchanged. In particular, final four-client recall at 48 probes loses 35 hits against original
main: 0.0010139, narrowly outside the strict gate. Final eight-client losses are 0.0015353 / 0.0013905.
These are reported rather than labelled passes. A reliable sub-0.001 algorithm comparison needs stable
input ordering/sampling or a frozen-layout paired control; the current one-run gate is too close to
its observed end-to-end variation. The 198-second performance target remains unmet independently.

### Other kill results

1. **Exact group-outer strips:** preserving RR1 assignments requires preserving the four-row unions.
   Removing union bloat changes the actual search space. A pass-one runner-up/top-16 bound also does
   not cover centers newly admitted by refined group routing. The prototype retains those semantics,
   uses SGEMM as a conservative rejection filter, and recomputes possible winners with the original
   four-row dot arithmetic. A 4,096-row kernel control takes 0.2024 s with direct dots versus 0.2270 s
   with strips (0.892x); 32,768 rows take 1.8612 versus 2.1191 s (0.878x), allowing 4,096 gathered rows
   per matrix call. Both return identical assignments. Shapes are 1,024 dimensions, 13,400 leaves,
   640 groups, and 40-group unions. These are synthetic kernel controls, not full-NQ leaf-stage
   measurements. This implementation fails its prerequisite; it does not prove a ceiling for every
   possible group-outer implementation. The prototype is discarded.
2. **Free coarse routing:** 32 zero centers and free balanced assignments remove all segment training
   and distance assignment while retaining cluster-level encoding parallelism. Full-NQ I2 load is
   146.417 s, 18,314 rows/s, zero retries, and 1,954.4 ms write p90: only 1.148x original main, below
   1.3x. The final build is stopped after the failed ingest prerequisite; full-tier time and quality
   are not measured for this arm. Adaptive batching means this is an experimental optimistic arm,
   not a mathematical bound across every batching policy. No coarse-routing change ships.
3. **Ingest-time staging:** RR1's copy and coarse stages sum to 21.169 s, not the advisor's assumed
   70 s. Current paired main spends 11.436 + 0.167 + 3.614 = 15.217 s. Perfectly hiding all three
   leaves 207.471 s readiness, still above 198 s, before reconciliation or ingest overhead. This
   rejects staging alone by its critical-path bound; combinations with other changes remain open.
4. **Explicit ARM L2:** 0.1549 s existing versus 0.1437 s explicit NEON (1.078x). The existing lane
   loop already compiles efficiently on ARM. This isolated gain does not justify another kernel;
   the prototype is discarded.

### Validation and limitations

Native commands before each commit:

```sh
cargo fmt --all
cargo clippy --locked -p bf-engine -p bf-vector --all-targets -- -D warnings
cargo test --locked -p bf-engine -p bf-vector -- --test-threads=2
```

All pass: the final run has **225 passed, zero failed, and seven ignored**, including doctests.
Regressions cover a fully indexed ten-second retry gap against a 21-second advertised wait,
expiry, shorter and longer subsequent retries, namespace recreation, the exact WAL threshold, real
429s from both commit paths, checkpoint refresh, compaction expiry, forced compaction, the segment cap,
protected sources, queued work after caller cancellation, actual completion-based quiet, and ordinary
steady-write classification. Full-workspace and official-SDK compatibility gates remain coordinator work.

Retry deadlines are node-local scheduling hints. Persisted bulk history and WAL checks provide the
existing restart/cross-node fallback; these deadlines themselves are not propagated across nodes or
restarts. They do not affect durable write ordering or manifest fencing.

Independent loads produce different flush layouts and segment-derived global row ordering. Coarse
sampling, leaf sampling, and four-row candidate unions depend on that ordering. A hash of **document
IDs**, rather than the segment-derived global row number, is the useful stable key. Coarse sampling
alone would not make the whole build layout-invariant: leaf samples and within-group block order also
need attention. No sampling change ships. Reading stable IDs and preserving bounded memory/I/O need
measurement before such a change.

The coordinator should update PROGRESS.md, benchmark/design documentation, and run workspace plus
both-mode SDK compatibility checks before merging. No additional crate change is required for the three
scheduling fixes. Cross-node persistence of retry hints would require broader metadata work.

## Round C3: automatic leaf populations and matched probes

A small tier keeps **200-row leaves**. A tier with at least **10 million live vector rows** builds
**640-row leaves**. This is a conservative, two-regime policy: no intermediate population is
promoted. The 320-row NQ experiment failed the warm gate. Larger tiers stay capped at 640
until they have their own measurements. There is no new configuration setting or format
version, and existing tiers keep their layout until their next normal rebuild.

The planner reads the stored leaf population, rather than assuming every tier has the new
layout. The default quality budget remains 64 for 200-row leaves and becomes **70** for
640-row leaves, before the existing cluster-count, live-row, and delta-share adjustments.
The asynchronous, paged, and compiled planners share this calculation. C2's 56-probe point
matched the historical 200-row layout; it did not match a fresh build from current main.

### Controls and protocol

Baseline source: `4552a00`. All copies were owned, and existing checkpoints were read-only.
The 10M comparison used two fresh builds from the same upgraded checkpoint, with 32 coarse
cells, a 250,000-row slab cap, eight builder threads, and the same main builder. Both fresh
layouts contain 55 slabs. This removes the historical 32-versus-55-slab confound.

Cold qualification runs use an owned c7gd.4xlarge in us-west-2: 16 Graviton3 cores, 32 GiB
RAM, and local NVMe. Every query drops application caches while retaining the process and
connections. Arms alternate AB/BA over all 200 exact-truth queries. Each arm has 8 GiB raw
cache, 512 MiB decoded metadata, a 48 GiB NVMe cache, 4,000-byte blocks, 512-request fetch
concurrency, and eight Tokio/Rayon workers. No build or test runs on the measurement host.
GET counts include started hedges; bytes count completed reads. Temporal read waves come
from trace overlap and are distinct from the dependency-wave regression tests.

NQ warm tests use a separate c7g.4xlarge, with server cores 0–7 and client cores 8–15.
The frozen local copies contain all 2,681,468 f16 vectors, 1,024 dimensions, and 3,452 query
vectors with exact truth. Requests ask for ten IDs, use strong consistency, and are encoded
before measurement. Sixteen keep-alive client processes run every query. Each arm gets
4 GiB raw cache, 4 GiB decoded metadata, and no disk cache. Independent server processes
run in forward/reverse order, with two complete warm epochs before measurement. The final
confirmation uses four measured epochs per process. Cache hit ratio, tail rows, population,
compiled-query counts, and ordered IDs are checked alongside throughput.

The initial Spot measurement host was interrupted before its 10M rebuild finished. Its
completed owned checkpoints survived; unfinished builds restarted on an on-demand host.
No interrupted build time is reported as a completed result.

### Why NQ retains fine leaves

A 320-row NQ build had 8,365 leaves, 32 slabs, and a 1,780,580-byte directory. At matched
full-workload recall, it needed quality budgets of 60 and 80 in the old planner's units,
against main's 48 and 64. Simply reducing probes as in the 10M experiment lost recall.

| Layout | Quality target | Recall@10, all 3,452 queries | Mean warm q/s |
| --- | ---: | ---: | ---: |
| Main, 200-row leaves | 0.95 | 0.951970 | 1,811.4 |
| 320-row leaves | 0.95 | 0.951188 | 1,590.2 |
| Main, 200-row leaves | 0.96 | 0.961674 | 1,551.4 |
| 320-row leaves | 0.96 | 0.962572 | 1,331.5 |

These are four measured epochs per arm, from two processes, after two warm epochs each.
The 320-row layout loses **12.2% / 14.2%** throughput. Its compiled path answers about
3,240 of 3,452 queries per epoch, against all 3,452 for main; missing exact rows in the
bounded sparse-vector cache cause the remaining fallbacks. That explains part of the
regression, without establishing that it is the only CPU cost.

On the paired first-200-query cold workload, 320-row leaves also lost recall: **0.9570
versus 0.9635**, despite the small full-workload recall difference. Planned rows grew from
111,343 to 136,900. This layout is rejected. Retaining 200-row leaves preserves the complete
warm plan and ordered answers, including the hard queries in that cold slice.

The final retained-200 policy passes the warm gate at both recall targets. A longer initial
confirmation showed a 0.92% high-target throughput drop. The compiled tier now caches its
immutable row total and nonempty leaf positions once, rather than recovering them through
two strided metadata walks per query. The added word per leaf is charged to its decoded-cache
reservation. The final source was then rebuilt and replayed independently:

| Layout | Target | Recall@10 | Mean warm q/s | Epoch q/s range | Peak RSS, GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Main, 200 rows | 0.95 | 0.951970 | 1,881.4 | 1,845–1,897 | 4.74 |
| C3, 200 rows | 0.95 | 0.951970 | 1,907.2 | 1,885–1,936 | 3.64 |
| Main, 200 rows | 0.96 | 0.961674 | 1,624.2 | 1,594–1,646 | 3.82 |
| C3, 200 rows | 0.96 | 0.961674 | 1,638.0 | 1,623–1,666 | 3.91 |

This is eight measured epochs per arm, from two independent processes. Mean throughput
increases **1.37% / 0.85%** at the two targets; the small gains are within the epoch ranges.
Every measured epoch has cache hit ratio 1.0, no exhaustive tail, the correct population,
and all 3,452 queries on the compiled path. All ordered IDs match main at each target,
across all 32 measured epochs. RSS variation is reported without attributing it to the
small metadata cache.

### The 10M control changed the probe calibration

The historical original and its lossless upgrade both recall **0.9615** at a 64-unit quality
budget. A fresh 200-row build recalls **0.9690**. The 640-row build at C2's effective 56
recalls **0.9620**, so it fails the fresh-control tolerance by 0.007. Comparing only with
the historical layout would have hidden this loss.

The NVMe calibration replay found **0.9700** at effective 70 for 640-row leaves, against
0.9690 for the paired fresh control. It reduced p50/p90 from **405.6 / 513.2 ms** to
**357.8 / 461.8 ms**, GET attempts from 622.28 to 352.34, and completed bytes from
58.27 to 47.56 MB. Planned rows increased **3.15%**, from 220,243 to 227,174. The selected
budget is the lowest measured coarse budget that matches the fresh control's recall.
A higher 73.5-unit tuning point recalled 0.9710, but scanned 237,927 rows; it is not the
default. Multi-arm replays now reverse alternate full rotations to balance every pair's
order; two-arm confirmation retains strict AB/BA order.

The final-source, independent two-arm confirmation gives:

| Layout | p50 / p90, ms | Recall@10 | GET attempts | Completed MB | Planned rows | Read waves |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fresh 200 rows, 64 units | 377.57 / 465.56 | 0.9690 | 620.555 | 58.404 | 220,243 | 3.205 |
| 640 rows, effective 70 | 325.12 / 413.34 | 0.9700 | 354.915 | 47.646 | 227,174 | 3.240 |

The selected layout cuts p50 **13.9%**, p90 **11.2%**, GET attempts **42.8%**, and completed
bytes **18.4%**. Recall improves 0.0010; planned rows rise 3.15%, which is the measured cost
of matching the fresh control's higher recall. An earlier independent production-policy
replay also improved p50/p90, from 390.52 / 486.59 ms to 354.43 / 456.02 ms, with the same
0.9690 / 0.9700 recalls. All coarse plans, ordered IDs, and distances match exactly between
that replay and the final source's 200-query confirmation.

The final paired NQ cold confirmation, retaining the 200-row layout, gives:

| Reader | p50 / p90, ms | Recall@10 | GET attempts | Completed MB | Planned rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| Main | 246.29 / 357.35 | 0.9635 | 382.580 | 28.088 | 111,343 |
| C3 | 242.60 / 326.67 | 0.9635 | 381.985 | 26.952 | 111,343 |

All 200 ordered ID lists, distances, and planned row counts match exactly. p50 falls 1.50%,
p90 falls 8.59%, and completed bytes fall 4.04%. Temporal read waves average 3.160 / 3.180.
The earlier independent pair had p50 235.83 / 234.70 ms and identical recall, but p90
307.26 / 315.08 ms; the small NQ timing differences vary between runs. The retained layout
and recall remain unchanged, while redundant routing bytes are avoided.

### Build cost and readiness

| Corpus | Target leaf rows | Recluster/job seconds | Peak RSS, GiB |
| --- | ---: | ---: | ---: |
| NQ, main automatic build | 200 | 127.49 | 1.04 |
| NQ, rejected candidate | 320 | 101.77 | 0.78 |
| 10M, fresh main builder | 200 | 627.37 | 2.02 |
| 10M, coarse leaves | 640 | 479.43 | 1.84 |

NQ's automatic-job timer includes installation. The standalone rebuilds returned only
after their tiers were installed: process wall times were 108.04 seconds for NQ/320,
638.27 seconds for 10M/200, and 488.07 seconds for 10M/640, including the current-format
survey before building. HTTP queries could continue against the previous layout during
the build. The NQ control RSS is the monitored writer's VmHWM; the standalone figures
come from `/usr/bin/time -v`.

The fresh 10M directories contain 49,985 versus 15,611 leaves, and 10,040,671 versus
3,303,367 bytes. Coarse leaves reduce build time by 23.6% on this same host. These are
16-core-host, eight-builder-thread results, not a direct speedup over C2's smaller host.
The existing automatic slab cap continues to follow the engine's memory budget.

### The upgraded-copy finding

The lossless upgrade preserves 32 slabs and every center. Its directory is 9,938,808 bytes,
against 9,938,840 before upgrade. The extra read is a **5,352,604-byte segment-directory
hint**, which reindex creates. The cold-open code raced that hint with the slab hints.
It could finish before the larger flat slab hint, downloading a full directory that the
query did not use. The extra hint object and its unnecessary request are confirmed; total
transferred bytes also depend on cancellation timing. C2's 20–30 ms upgraded-copy slowdown did not reproduce
consistently in C3, so it is not presented as a fixed penalty.

The fix gates segment speculation on either a manifest proving there is no slab tier,
or both slab hints being absent. Paged and flat slab hints retain their priority. Filtered
queries still request the segment hint directly. Coordination state is allocated only for
a cold, unfiltered open. Delaying the segment hint until all slab requests finish added an
untiered read wave and was rejected by the existing tests; the manifest notification keeps
those queries within their original three-wave budget.

On the same upgraded 200-row prefix, the final 200-pair replay reports:

| Reader | p50 / p90, ms | Recall@10 | GET attempts | Completed MB | Read waves |
| --- | ---: | ---: | ---: | ---: | ---: |
| Main | 350.37 / 459.69 | 0.9615 | 612.075 | 67.206 | 3.325 |
| C3 | 359.53 / 461.63 | 0.9615 | 613.045 | 62.168 | 3.225 |

All 200 ordered ID lists, distances, and planned row counts match exactly. Mean routing
bytes fall from 15.05 to 9.99 MB, and total completed bytes fall 7.50%. Started GET attempts
are slightly higher because hedges vary; eliminating one hint does not guarantee fewer
attempts elsewhere. Median time outside the instrumented ANN stages is 117.53 / 117.90 ms.

This replay has **2.61% higher p50 and 0.42% higher p90** after the fix. An earlier NVMe
pair also showed +1.08% / +5.66%, while the initial GP3 diagnostic showed a reduction from
405.82 / 513.22 ms to 366.50 / 450.86 ms. The byte waste is fixed, but a latency gain is
not established. Original-versus-upgraded main-reader diagnostics also reversed C2's
latency ordering. There is no evidence that 32-versus-55 slab packing caused the lossless
upgrade penalty: the upgraded tier retained its original 32 slabs and identical answers.

### 100M projection and follow-up

No 100M rebuild was attempted under this round's total $5 cap. At the capped 640 rows,
100M would have about **156,250 leaves**, versus about 500,000 at 200 rows. At 1,024
dimensions, the measured directory bytes per leaf project to roughly **33 MB**. The
70-unit quality mapping implies about **1,094 nominal leaf probes and 700,000 nominal
rows**, versus 3,200 and 640,000 for a 200-row tier at 64 units. Actual selected populations,
coalesced GETs, recall, and latency are unmeasured. In particular, this is not a recall claim.

Assignment uses neighboring parent groups, rather than comparing every row with every
leaf. A rough work model is `N * sqrt(N / leaf_rows)` at fixed coarse-cell count; 10× rows
would mean about 31.6× assignment work at fixed leaf population. Large coarse cells also
switch to two-level training above 1,024 leaves, so extrapolating one build duration would
be unreliable. A paired 100M legacy upgrade, 200-row control, and 640-row rebuild would
require several host-hours per arm and would risk this round's remaining budget.

Follow up with an owned 100M copy, a fresh same-builder control, all 200 independent oracle
queries, repeated cache-cold pairs, warm throughput, installation time, and peak RSS. Check
the training transition and recall before changing the cap or its probe mapping. Intermediate
populations need their own matched-recall warm and cold evidence before the 10M threshold
is lowered. Other embedding models and dimensions have not been calibrated in this round.
The warm throughput gate is NQ; 10M and 100M warm parity are not established here.

### Reproduction and validation

`crates/bf-engine/examples/recluster_copy.rs` accepts `auto` as the leaf target. Numeric
targets remain available for controlled experiments; writes are restricted to owned `tmp/`
S3 copies, and scratch must be under `.scratch/`. `ReclusterReport::cluster_rows` records
the selected target. Automatic background jobs pass zero internally to select from their
live population. No leaf-size configuration or forced-rewrite trigger was added.

`cold_ab.py --nq-data DATA` uses prepared NQ queries and truth. Its per-query records include
GET attempts, completed bytes, planned rows, read waves, bytes by object kind, and stage
service times. `warm_ab.py` takes private arm configs and supports `--epochs`; its server
storage is always read-only. Delete outputs after retaining the results.

Final policy source is `836ce91`, with the hint coordination in `a828c4e` and the paired
harness improvements in `9277654` / `f4028dc`. Local and EC2 source hashes match. Native
ARM `cargo fmt --check`, `cargo clippy -p bf-engine -p bf-vector --all-targets -- -D warnings`,
`cargo test -p bf-engine -p bf-vector`, and the `recluster_copy` example test pass: **229
passed, zero failed, and seven existing ignored tests**. Both release examples build.
Tests cover automatic target selection, legacy probe invariance, coarse budgeting,
compiled/reference equality with masks and eviction, missing hints, and the original
untiered three-wave limit. Both Python harnesses pass Ruff.

Measurements and builds used about 0.54 interrupted Spot c7gd.4xlarge hours, 1.90 Spot
c7g.4xlarge hours, and 1.47 on-demand c7gd.4xlarge hours. The conservative total estimate
is **$3–4**, below the $5 cap, allowing $1/hour for the on-demand host plus request and
storage charges; this is an estimate, not a billing audit. All owned instances terminated,
their tagged key pairs and security groups were deleted, and their EBS roots were removed.
Cleanup deleted 5,239 current objects totaling 409.8 GB from the owned temporary prefix;
zero current objects and zero multipart uploads were verified. Existing prefixes were
read-only. Local run artifacts were removed after retaining these measurements.

## Round MEM2: fast local reads under a memory limit

The final candidate passes the NQ memory gates after merging Q2 and RR2 through
`a649aa5`: 3.59× baseline throughput at 2 GiB, 1.16× at 4 GiB, 1.37× at 8 GiB,
and 1.32× at 12 GiB, with identical ordered IDs and distances. This section measures
IVF; the G3 graph prototype is outside this round's scope.

### What changed

On Unix, immutable `file://` range readers map the original files and return exact
slices, removing the heap block copy in front of the OS page cache. Small reads and
explicitly sparse vector reads use a random-access mapping; ordinary reads containing
a range of at least 64 KiB use a normal mapping with read-ahead. Both views share the
same physical file pages. Sparse hints remain random even when their requested block
size equals the configured main block size. Published files must remain immutable.
Deleting a key invalidates both cached views; already-returned bytes remain valid.

The cache retains at most 16 GiB of weighted virtual-file references and budgets 512
cached descriptors, with 60-second idle expiry. These are reference budgets, rather
than resident-page reservations. Caller-held bytes, including higher-level cache
entries, can keep an unlinked inode alive until dropped. Mapped diagnostics describe
logical returned ranges, not physical page faults. Query statistics conservatively
count mapped bytes as misses because OS residency is unknown. Full mutable operations
and S3 reads keep their ordinary backends.

The effective heap block budget is zero for mapped sources. IVF's streaming seed-and-scan
plan is disabled at zero retention: otherwise completed seed buffers can be discarded
before scan readers request them again. The decoded-cache default changes from
`limit / 8` to `limit / 4`, retaining the 512 MiB floor, 4 GiB ceiling, and existing
override. At 2/4/8/12 GiB, its capacities are 0.5/1/2/3 GiB. X1 already reserves codes
and a bounded row-exact vector pool together; this lets the NQ tier fit at 8 and 12 GiB.
The small-budget regression now runs before live masks bypass compiled admission and
checks that the same rejection marker survives repeated queries.

### Final Graviton budget sweep

One dedicated c7g.4xlarge, 16 vCPUs, 32 GiB RAM, and gp3 storage ran the two merged
release builds serially. These warm local-store rates have no hard cgroup cap. Each arm
checks all 3,452 queries, warms 1,000 queries, times 1,000 serial queries, and runs two
2,000-query throughput passes at 16 clients. Tokio/Rayon have 12 threads, query/search
admission is 64/12, and Q2's query workers follow the 16 available CPUs. Disk cache is
disabled. Main blocks are 1 MiB; sparse blocks are 16 KiB. The decoded budget is fixed
at 512 MiB here, isolating the storage policy. Numbers are mean q/s across the two passes.

| Raw block budget | Merged baseline | Final mapped views |
|---|---:|---:|
| None | 204 | 2,765 |
| 256 MiB | 380 | 2,819 |
| 1 GiB | 1,047 | 2,855 |
| 2 GiB | 2,193 | 2,791 |
| 4 GiB | 2,582 | 2,812 |

No tested mapped budget is slower than raw zero; these settings use the same effective
mapped policy. Their means vary by about 3%. At decoded 2 GiB, the final candidate
reaches 3,345 q/s with raw zero and 3,404 q/s with raw 4 GiB, 1.32× the final baseline's
best 4 GiB rate. Its Graviton serial p50 is 3.18–3.19 ms, versus about 2.2 ms for mapped
views at decoded 512 MiB: compiled search trades serial latency for throughput here.

The initial post-X1 isolation baseline (`ab4172d`) gave 233/403/1,068/2,081/2,407 q/s
at raw zero/256 MiB/1/2/4 GiB with 16 KiB sparse blocks. At 2 KiB sparse blocks, it gave
200/231/409/2,140/2,236 q/s. The severe graph-specific cache cliff was not reproduced
on IVF, but smaller sparse blocks were worse at small budgets. Increasing only that
baseline's decoded budget to 2 GiB gave about 3,200 q/s. A separate 1.5 GiB decoded
control, the old default under a 12 GiB limit, gave 2,426 q/s and no compiled queries:
its general decoded pool is only half the total, so the NQ reservation still did not fit.

### Matched M4 memory limits

The same 2,681,468 NQ f16 cosine vectors, 1,024 dimensions, and 3,452 exact-truth queries
are used throughout. The immutable IVF index was rebuilt with 32 recluster cells, 200
rows per leaf, and at most 250,000 rows per slab; its recall differs slightly from MEM1's
index. Inputs have the MEM1 SHA-256 hashes. The query snapshot excludes checkpointed WAL
history and manifest-retired objects, retaining every active object. Its full 294-file,
12,127,019,472-byte hash matches before and after the blowfish sweep.

Apple M4's ten cores are shared by server and clients. The disposable arm64 Linux VM
uses run-local Lima 2.3.0 with uncached VZ disks, guest ext4 data, no host data bind mount,
and 3/5/9/13 GiB guest RAM for 2/4/8/12 GiB server caps. Cgroup v2 includes allocations
and file cache, with swap disabled. The VM restarts between budgets, guest caches are
synced and dropped before every server, and previously cached backing-file inodes are
replaced with APFS clones while stopped. Query/search admission is 8/8, with ten runtime
and Rayon threads. Baseline uses the MEM1 cache profile: raw `limit - 1 GiB`, decoded
512 MiB, and disk zero. Candidate uses the new decoded default and mapped file pages.

Each table row has two fresh-process repeats: full external quality, 1,000 warm-up queries,
1,000 serial latency queries, and 2,000 pre-encoded queries at each of 1, 4, and 16 clients.
Within each budget, the order is baseline/candidate/candidate/baseline. Recall@10 is
0.950261 for every blowfish row, and every ordered ID and distance matches the baseline.
Peak RSS and charged memory are maxima across both repeats. RSS can count alias mappings;
cgroup charged memory counts their physical pages once. Small accounting overshoots round
to the limit. Throughput is the 16-client mean and range.

| Engine | Limit GiB | Peak RSS / charged GiB | Recall@10 | p50 / p90 / p99 ms | q/s, mean (range) |
|---|---:|---|---:|---|---|
| blowfish baseline, 48 probes | 2 | 1.59 / 2.00 | 0.950261 | 3.1 / 4.9–5.0 / 9.2–9.3 | 622 (619–625) |
| blowfish MEM2, 48 probes | 2 | 1.93 / 1.57 | 0.950261 | 1.5–1.6 / 1.8 / 2.0–2.1 | 2,235 (2,210–2,260) |
| blowfish baseline, 48 probes | 4 | 3.45 / 4.00 | 0.950261 | 1.7 / 2.0 / 2.5–2.6 | 1,987 (1,978–1,995) |
| blowfish MEM2, 48 probes | 4 | 1.96 / 1.60 | 0.950261 | 1.5 / 1.7 / 1.9–2.0 | 2,305 (2,298–2,312) |
| blowfish baseline, 48 probes | 8 | 3.47 / 6.36 | 0.950261 | 1.7 / 2.0–2.1 / 2.5–2.6 | 1,974 (1,958–1,990) |
| blowfish MEM2, 48 probes | 8 | 2.79 / 2.44 | 0.950261 | 1.5 / 1.6 / 1.8 | 2,702 (2,688–2,715) |
| blowfish baseline, 48 probes | 12 | 3.47 / 6.36 | 0.950261 | 1.7 / 2.0 / 2.5 | 1,959 (1,931–1,988) |
| blowfish MEM2, 48 probes | 12 | 2.80 / 2.44 | 0.950261 | 1.5 / 1.6 / 1.8 | 2,592 (2,516–2,668) |

All 16 blowfish cgroups have zero OOM events and zero swap. Benchmark servers are
terminated after measurements; exit 137 after Docker's stop deadline is not an OOM
(the OOM flag and cgroup counters are checked separately). Across these runs, host
counters record 106 16-KiB swap-ins (1.66 MiB) and no swap-outs; those counters include
desktop applications. The kernel logs of the measured runs are clean.

Qdrant 1.20.0 was rebuilt on the same NQ inputs: f16 vectors on disk, HNSW in RAM,
`m=16`, `ef_construct=100`, and query `ef=64`. It completed all 2,681,468 vectors
with two physical segments, despite a target of one. These are comparison rows at
slightly lower recall, not an equal-recall win for blowfish.

| Engine | Limit GiB | Peak RSS / charged GiB | Recall@10 | p50 / p90 / p99 ms | q/s, mean (range) |
|---|---:|---|---:|---|---|
| Qdrant, vectors on disk, ef 64 | 8 | 4.81 / 4.89 | 0.943569 | 0.5 / 0.7 / 0.8 | 6,023 (5,996–6,050) |
| Qdrant, vectors on disk, ef 64 | 12 | 4.81 / 4.89 | 0.943569 | 0.5 / 0.7 / 0.8 | 6,011 (5,972–6,051) |

Each Qdrant row uses the same quality, warm-up, latency, and throughput protocol,
with two fresh processes and uncached-runtime query VMs. Its full stored-file manifest
matches before and after all four repeats: 41 files and 5,878,957,996 bytes. All repeats
have identical ordered results, zero OOM events, zero server swap, and clean kernel logs.
Qdrant remains faster once its working set fits; MEM2 improves blowfish's plateau but
does not close that gap.

An uncached-runtime bulk-loading attempt stalled and was discarded. Construction
instead used the stock cached VZ runtime, ten GiB guest RAM, and an uncapped server;
the loader and integrity helpers had separate caps. After construction, both backing
files were synced, stopped, and cloned to fresh inodes before switching back to the
uncached runtime for every measured query. No loading or construction time is included
in throughput. The final blowfish store also matches its full before/after manifest:
294 files and 12,127,019,472 bytes.

### Cold checks and rejected controls

Final merged disk-cold ABBA empties all engine caches and Linux page cache before every
query on Graviton, raw 1 GiB, and decoded 512 MiB. Four ten-query passes give 20 samples
per arm: pooled baseline p50/p90 1,067.7/1,512.1 ms, final candidate 331.2/598.8 ms.
Every returned ID and distance matches. Large reads need bulk I/O: an earlier all-random
mapping improved small-budget warm performance but regressed this disk-cold test to
about 1.3 s p50, so it is not the final policy.

A process-warm S3 ABBA uses a read-only facade delayed 25 ms per request and clears all
application caches before each query. At raw zero, four 100-query passes give 200 samples per arm: pooled baseline p50/p90 176.0/198.6 ms, candidate 170.7/195.4 ms, with identical results. Shorter 20-query ABBA checks at raw 256 MiB and 4 GiB give p50 174.8/178.1 ms and 175.9/175.4 ms for baseline/candidate; their p90 values are 226.5/209.8 ms and 221.7/207.2 ms. Positive-budget S3 uses the same read policy in both builds. The facade models dependent
request latency, not production S3 bandwidth, tails, or throttling.

Rejected measurements were retained as controls, rather than shipped:

- Unadvised mappings passed native warm tests but failed the 2 GiB M4 gate: 343 q/s and
  p50/p90 9.7/23.9 ms, versus baseline 630 q/s and 2.9/4.8 ms. Sparse read-ahead was
  the cliff. Random sparse views fix it; normal bulk views avoid serial cold-page faults.
- Globally shrinking main blocks to 16 KiB reduced initial IVF throughput to 122 q/s
  without raw cache, 215 q/s at 256 MiB, and 760 q/s at 1 GiB. It is not retained.
- Amortized flight-table cleanup did not give a consistent gain. Additional ghost-list,
  storage row-cache, and protected-LRU prototypes were not performance-qualified and
  are not retained. X1's existing row-exact cache remains the compiled vector pool.
- `posix_fadvise(DONTNEED)` after actual disk-block promotion, under a 2 GiB Graviton
  cgroup with raw 256 MiB, decoded 512 MiB, and disk 6 GiB, improved a first pair from
  45.1 to 78.8 q/s but regressed serial p50/p90 from 21.7/39.1 to 150.0/362.8 ms.
  Its 1,000-query quality prefix matched. Remaining arms were killed. Remote S3's
  disk-cache policy remains unchanged.

Two discarded staging attempts and one comparator-loading attempt hit guest-kernel or
runtime failures during bulk I/O, before comparator measurements. Preparation retries
are excluded. Utilities use no bridge/NAT, payloads and binaries are hash-checked, and
all measured VMs use the uncached runtime. Rust formatting, storage/engine all-target
clippy with warnings denied, and both crates' full tests pass on the merged source.

## Round BW1: exact BM25 during steady writes

The measured cause is query-time reconstruction of full-vocabulary checkpoint
statistics, followed by repeated analysis of shadowed and unindexed documents.
BW1 reuses immutable document term counts and fetches checkpoint frequencies only
for query terms. Commits perform no additional tokenization or index maintenance.

### Protocol

One owned, on-demand c7g.4xlarge in us-west-2, Ubuntu 24.04 arm64, Rust 1.100.0,
and Clang 19. The server runs on CPUs 0–5, and clients run on CPUs 6–7. Every
arm starts from an identical local-store copy of the first 1,000,000 passages in
`msmarco_load.py`'s stream. The block cache is 2 GiB; decoded-cache and indexing
settings retain their defaults. No S3 latency is measured.

The workload is `bench/scale/data/bm25-msmarco.json`, strong consistency, top ten,
and IDs only. W1's `steady_writes.py` now accepts this text-only workload and
corpus. Seed 17 selects random 100-row upsert batches at 1,000 rows/s. Each write
uses the original passage plus a batch-specific suffix, changing BM25 statistics.
Seed 29 permutes query order. Paired runs pace queries at one per second, use
120-second windows with 30 seconds of warm-up, and run in A/B/B/A order. These
runs compare the same query schedule, without letting the faster binary cover a
different part of the workload. Closed-loop runs separately measure a saturated
single client. Builds and independent reference generation finish before timing.

A load must drain its WAL tail before making the store copies. In this run,
metadata reported `index.status = "up-to-date"` while 375,000 load rows remained
unindexed. The drain gate is a query's `performance.exhaustive_search_count = 0`;
`["id", "asc"]`, with limit one, checks it without BM25 analysis. The frozen
snapshot checker also explicitly checkpoints the initial load.

### Initial measurement and cause

The initial closed-loop idle result is **2.615/5.127 ms p50/p90**, versus
**517.969/703.266 ms** at 1,000 upserts/s. All 120,000 offered rows are
acknowledged, with zero rejections. The measured tail median/maximum is
2,797/5,683 rows. This is well outside the stop condition of twice idle latency.
The idle HTTP warm-up has already drained the initial load tail.

### Implementation

- `bf-text::DocumentTerms` retains exact lengths and term frequencies without
  positions. `PreparedQuery` prepares query weights once and accumulates f32
  contributions in the reference scorer's lexical order, including repeated
  terms and prefix matches.
- Engine queries fetch only needed checkpoint frequencies, including terms in
  computed BM25 projections. Counts and total lengths remain integers through
  subtraction and addition; reconstructing a length from a rounded average
  would introduce an avoidable score-bit risk.
- Immutable WAL summaries use document allocation identities, weak references,
  and complete analysis configurations. Shadow summaries use scoped immutable
  document-object keys, row ordinals, and configurations. Only cache misses
  hydrate and analyze shadow rows.
- A sharded CLOCK pool receives one eighth of the existing decoded-cache budget,
  taken from the general metadata pool. Total configured cache capacity does
  not increase. Eviction remains bounded under sustained replacement; commits
  maintain no new index or statistics structure.

### Paired result

All four arms execute the **same 90 measured query IDs, in the same order**.
The first A/B pair enables stage traces; the second B/A pair disables them.

| Arm | Queries | BM25 p50 ms | BM25 p90 ms | Tail median / maximum |
| --- | ---: | ---: | ---: | ---: |
| A1, existing reader | 90 | 541.867 | 723.110 | 2,995.5 / 6,278 |
| B1, cached counts | 90 | 57.228 | 84.534 | 3,095.5 / 6,877 |
| B2, cached counts | 90 | 56.540 | 80.202 | 3,194 / 6,677 |
| A2, existing reader | 90 | 536.329 | 718.113 | 2,994.5 / 6,378 |

The medians of the run percentiles are **539.098/720.611 → 56.884/82.368 ms**:
**9.48× faster p50, and 8.75× faster p90**. Both candidate runs pass the
100/180 ms latency gates, despite slightly larger measured tails.

Each arm acknowledges all 120,000 offered rows, with zero rejections, sustaining
1,000 upserts/s. Median write-service p50/p90 changes from 1.841/2.462 to
1.896/2.550 ms, increases of 3.0%/3.6%. Observed acknowledged throughput has
zero regression at the offered rate. Maximum ingest capacity remains unmeasured.

Separate closed-loop candidate runs give **2.691/5.074 ms idle**, and
**21.839/43.294 ms under writes**, with 3,612 measured queries in the latter
90-second measured window, about 40.1 queries/s from one client. Its tail
median/maximum is 2,000/6,976 rows. All **720,000 offered measurement rows** across
the initial, paired, and closed-loop write runs are acknowledged. The paired table
is the controlled speedup comparison; the closed-loop observations use different
query arrival rates and sample counts.

The HTTP baseline is `a649aa5` plus opt-in stage timing; candidate production code
is `238373b`. Their executable SHA-256 values are respectively
`22deabc60c7d220f54c330f400292e1921c5d99174c3f4e7cb29af7af2767418` and
`26d64844fff5cd0e2314e856b607cd202cc0dd33ae4c20a15bfaefc9f02f25ce`.

### Per-query work breakdown

`BF_TRACE_STAGES` groups observations by completed HTTP query. Each column below
covers 120 tail-bearing requests from A1 or B1, including warm-up.

| Component | Before p50 / p90 ms | After p50 / p90 ms |
| --- | ---: | ---: |
| Global statistics, including the first analysis pass | 487.354 / 633.502 | 41.606 / 60.044 |
| Checkpoint statistics, inside global statistics | 235.638 / 250.263 | 0.092 / 1.833 |
| Shadow-row statistics, inside global statistics | 115.732 / 197.213 | 25.166 / 39.248 |
| Tail statistics analysis, inside global statistics | 96.200 / 173.060 | 14.326 / 20.535 |
| Tail analysis or summary lookup for scoring | 10.269 / 18.333 | 1.448 / 2.779 |
| Tail scoring | 16.468 / 30.053 | 3.007 / 7.177 |
| Segment scoring and hydration, summed worker elapsed time | 16.227 / 34.982 | 15.920 / 34.582 |
| Final result merge | 0.030 / 0.034 | 0.028 / 0.032 |

The global-statistics rows overlap by definition. Segment worker durations can
also overlap. These are per-request work observations; percentiles do not sum to
wall latency, and include discarded speculative work when present. The full
vocabulary decode disappears from queries; fresh shadow and tail analysis remains
on first use. A mutable WAL inverted index is unnecessary to pass this round's
gates. The remaining tail scan is exact and proportional to the bounded tail.

### Exactness and checks

The opt-in engine test `text_benchmark::bm25_serialized_msmarco_snapshots` captures
four states: the fully indexed million passages, 5,000 overwrites, overlapping
replacements plus 300 deletes and null fields, and the next index publication.
Every state remains alive while later publications complete. Its serialized live
text corpus feeds the independent `exact_bm25` executable, with null fields
excluded from field statistics. Independent reference passes run concurrently,
outside every timed measurement.

All **4,000 query results match**, including **40,000 ordered IDs and their f32
score bits**, with zero differences. The two tail-bearing states have 5,000 and 4,800 live tail documents;
the second has 5,100 shadowed IDs. Unit coverage also exercises cache eviction,
zero budgets, immutable replacement and copy-on-write identities, changed analysis
settings, scalar and array fields, missing and empty values, repeated and prefix
terms, filters, multi-field scores, computed BM25 projections, and publication.

Formatting, scoped Clippy with warnings denied, and both complete crate suites
pass. No bucket format, wire response, dependency, or write-path change ships.
The cache pool redistribution can affect metadata residency; ANN, cold-S3, 4 GiB,
full-workspace, and official-SDK integration measurements remain coordinator work.

### Rare-term-pair side experiment: rejected

The offline fixture is B6's five consecutive 200,000-row format-7 segments over
the same first million passages, all 1,000 MS MARCO queries, default parameters,
and top ten. An independent exhaustive pass validates all 10,000 ordered IDs and
score bits before counters run. Every warm and counted pass checks those results.

For each query, the prototype intersects the two least-frequent term postings,
fully scores matches in canonical term order, and keeps ten hits in a bounded
heap. Query weights are prepared once. It skips the normal reader only when the
tenth score strictly exceeds both missing-term score bounds. Bounds sum the
actual per-term maximum contributions in lexical order with upward rounding;
those resident maxima and all query-term posting lists are prepared outside
counters, making this an optimistic warm test. Failed certificates pay for the
ordinary reader, including all fallback work.

One native release executable runs pinned to CPU 3. Perf control/ack FIFOs exclude
fixture loading, truth generation, and complete workload warm-up. Counters cover
cycles and instructions in user mode, without multiplexing. A/B/B/A medians use
two counted passes per main arm; the wider gate has one diagnostic pass.

| Arm | M instructions/query | M cycles/query | Attempts / certificates per 1,000 queries |
| --- | ---: | ---: | ---: |
| Existing reader | 20.622 | 12.855 | 0 / 0 |
| Rare pair, at most 16,384 combined postings | 20.806 | 13.059 | 871 / 12 |
| Rare pair, at most 65,536 combined postings | 20.888 | 13.102 | 999 / 13 |

The main candidate fully scores 23,522 intersections across the workload; the
wider gate scores 35,114. Only 1.2–1.3% of queries certify. Mean cycles increase
about 1.6% at the selective gate and miss the **8.83M cycles/query** requirement.
This executable's paired baseline differs from B6's historical 11.485M; no gain
is inferred from that difference. All counted results remain exact.

The failed prototype is removed from the final tree. Its complete implementation
and corpus checks remain in `45f50da`, following the initial driver in `781f311`.
No rare-pair production hook, query option, index, or format ships.

### Reproduction

Use an owned run directory under `.scratch/`, a disposable fully indexed local
store copy per arm, and a trap that deletes the run directory when finished. The
MS MARCO corpus input is JSONL with numeric `id` and string `text`. With the server
pinned to CPUs 0–5, the matched client command is:

```sh
uv run --with numpy crates/bf-engine/bench/steady_writes.py \
  --corpus "$BW1_RUN/corpus.jsonl" \
  --workload bench/scale/data/bm25-msmarco.json \
  --namespace msmarco --base-url http://127.0.0.1:8300 \
  --query-count 1000 --rate 1000 --batch 100 --bm25-hz 1 \
  --seconds 120 --warmup 30 --tag paired-a1 --out "$BW1_RUN/results.jsonl"
```

Omit `--bm25-hz` for closed-loop requests, and use `--rate 0` for idle. For the
frozen checker, build `bf-text`'s `exact_bm25` example, copy it beneath the owned
scratch directory, and set `BW1_REFERENCE` and `BW1_STORE` to its absolute path and
a disposable local-store copy. Run the ignored engine test in release mode. It
writes reference JSON next to the disposable store; remove that output with the
run directory. Historical rare-pair reproduction uses `BF_TEXT_EXPERIMENT`,
`BF_TEXT_PERF_CONTROL`, and `BF_TEXT_PERF_ACK` with the driver in `45f50da`.
Set `BF_TEXT_RARE_PAIR=1` for candidate passes, and select the combined-posting
limit with `BF_TEXT_PAIR_LIMIT=16384` or `65536`; leave the first variable unset
for baseline passes. These variables belong only to the historical experiment.

## Round Q3: warm HTTP stalls

The attribution baseline is main `ddbb7bb`, including Q2's CPU dispatcher and
small-tier cache policy; the final integrated-main gate is recorded below. Measurements use one owned c7g.4xlarge, eight server CPUs, eight separate
client CPUs, and sixteen keep-alive clients. NQ is downloaded from its public
source, prepared with `bench/nq/prepare.py`, and loaded with `bench/nq/load.py`.
Both arms use the same frozen automatic tier, build flags, cache budgets, query
sequence, and host. Warm gates use the complete 3,452-query sequence at 48 and
64 probes, with three warming epochs and four measured epochs per process.

The resident ceiling can collect additional raw, user-only hardware events around
only its warmed loop with `BF_BENCH_EVENTS=name:hex-config,...`. Its JSON includes
counts per query and actual counter scheduling coverage. This is a benchmark-only
control, rather than a server setting. For example, Neoverse V1 frontend and
backend stall-cycle events are `stall_frontend:23` and `stall_backend:24`.
`server_tax.py --query-repeats N` repeats the selected sequence to lengthen HTTP
counter and cycle samples without replacing distinct vectors with one vector.
Use separate small event groups to avoid multiplexing, and report kernel work
separately from user work. Stalled-cycle counts are distinct from slot-based
top-down percentages.

A parser-only kill test compares Sonic 0.5.12 with the existing `serde_json`
deserializer over every NQ request and eighteen numeric boundary cases. All NQ
requests agree, but `1.0000000596046449` changes both a numeric filter value and
the resulting ANN f32 component. A wholesale parser replacement is rejected:
faster parsing must preserve the existing query bits and acceptance behavior.
No parser dependency or production parser change is retained.

### Pure compaction must preserve the resident plan

A covered-segment compaction can move every row without deleting one. The old
`tier_mask` returns `Some(full_bitmap)` in that case, despite its documented
mask-free contract, and the bitmap disables the compiled tier. The fix checks
coverage of every original row range and caches `None` under the existing
immutable live-object and lineage-map key. Real deletions retain their masks.
The regression case checks reuse of the compiled generation through compaction,
repeated cached proofs, unindexed updates, indexed deletes, projections, and eviction.

The new NQ tier has 32 slabs and 13,393 leaves. Its paired recall is
0.9484067207 / 0.9590092700 at 48 / 64 probes. Its layout differs from Q2's;
compare arms within this round. A copy of the first complete publication receives
one ordinary covered-segment compaction, with no writes or tier rebuild. The
native reference keeps the original publication's ID mapping and identical tier
payloads. Every ordered row and distance in the following ABBA matches both the
other arm and the first-publication reference.

| Probes | Main q/s, two processes | Mask fix q/s, two processes | Main / fix p90, two processes (ms) |
|---:|---:|---:|---|
| 48 | 1,489 / 1,486 | 1,953 / 1,955 | 14.80 / 11.22; 14.83 / 11.16 |
| 64 | 1,296 / 1,350 | 1,686 / 1,669 | 16.86 / 12.98; 16.21 / 13.05 |

Mean process-median throughput rises **31.4% / 26.8%**. User instructions fall
25.556→22.639 M / 29.331→26.110 M per query, while user cycles fall
12.079→9.063 M / 13.774→10.708 M. Context switches fall
9.53→5.17 / 8.88→5.43 per query. These counts have 100% scheduling coverage.
An already unmasked first publication has no corresponding work reduction;
its separate process controls vary by roughly 1–3%, and are not credited as a win.

The first 40-pair cold check uses fresh processes and the standard 25 ms loopback
S3 facade. Rows and distances agree. Paired median changes are +2.25 ms
[-1.29, +3.85] / -2.09 ms [-3.87, +0.61]. The 48-probe p90 point worsens by
42.97 ms, with a wide [-37.22, +60.12] ms interval, so this sample does **not**
clear the cold-tail gate. A larger confirmation is required before the final result.

### Match the scorer before attributing the HTTP difference

The packed resident harness and the server use different scorer representations.
The new ignored `warm_ann::benchmark::compiled_kernel_profile` control warms through
ordinary engine queries, retains that exact `WarmTier` and sparse vector cache,
and runs its scorer on eight OS threads. Timed loops exclude snapshots, hydration,
HTTP, and worker dispatch. Both controls report user-only counters, with zero
missing compiled payloads and 100% counter coverage.

| Dataset, 48 probes | Packed native instructions / cycles (M/query) | Exact compiled scorer instructions / cycles (M/query) | Packed / compiled q/s |
|---|---:|---:|---:|
| 10K control | 3.348 / 1.073 | 3.377 / 1.310 | 18,682 / 15,191 |
| Full NQ | 22.247 / 8.099 | 21.716 / 8.031 | 2,546 / 2,562 |

The representation difference affects the small control, but does not explain the
full-NQ HTTP gap. The full-NQ server takes about 22.64 M user instructions and
9.3 M user cycles/query, at roughly 1,900–2,000 q/s. Fixing a presumed packed-array
gap on full NQ would target the wrong cause.

`BF_BENCH_COMPILED` points to a JSON control file under `.scratch`, with `store`,
`namespace`, `queries` (headerless little-endian f32), `dims`, `probes`, `threads`,
and `repeats`. Run the explicitly ignored test in release mode. Both scorer
harnesses support `BF_BENCH_PROFILE_STOP`: after warming, the Unix process emits
`NATIVE_PROFILE_READY`, stops itself, and resumes on `SIGCONT`. Attach a profiler
before resuming to exclude startup and warming from function-cycle samples.
With two raw events, the instruction counter is omitted and reported as `null`,
so cycles plus the two events fit this host's three available PMU counters.

Initial larger event groups multiplexed and were discarded. The corrected groups
have 100% coverage. L3 refill events read zero on this host and are not interpreted
as evidence of no L3 misses.

Formatting, scoped all-target Clippy with warnings denied, and scoped tests pass:
223 passed, zero failed, and six explicit ignores, including the new diagnostic.
The large synthetic fixture uses a separate 8 MiB test-driver stack; runtime
workers retain their normal stacks. No production API, wire type, format, or
setting changes in the mask fix.

### Stall attribution

Corrected user-only PMU groups give the following counts per query. The small
control uses 10,000 rows; full NQ uses all 2,681,468 rows. These compare the packed
native control with the ordinary HTTP server at 48 probes, rather than claiming
that subtracting the two isolates one function. Each raw group has 100% scheduling
coverage. Frontend, backend, and memory stall events overlap and are not additive.

| Event/query | Small native | Small HTTP | NQ native | NQ HTTP |
|---|---:|---:|---:|---:|
| Frontend stalled cycles | 18,905 | 255,946 | 80,153 | 433,319 |
| Backend stalled cycles | 369,437 | 819,902 | 3,310,533 | 4,074,237 |
| Memory stalled cycles | 37,857 | 323,704 | 814,382 | 1,203,757 |
| L1D refills | 8,843 | 20,587 | 102,643 | 110,994 |
| L2D refills | 1,326 | 15,709 | 16,376 | 37,016 |
| L1I refills | 196 | 16,421 | 1,039 | 20,625 |
| Data TLB walks | 160 | 1,436 | 6,974 | 9,543 |
| Instruction TLB walks | 0.28 | 166 | 2.83 | 365 |
| Branch mispredictions | 2,337 | 6,649 | 8,947 | 10,269 |
| Frontend stalled slots | 1.562 M | 4.004 M | 10.660 M | 13.980 M |
| Backend stalled slots | 3.370 M | 7.142 M | 30.107 M | 36.144 M |

The excess is consistent with a wider instruction working set, instruction-TLB
activity, and cache disruption around HTTP work and dispatch. It is particularly
large for the short small-tier scorer. On full NQ, most cycles still belong to
scoring. Warm-only function-cycle samples have zero lost samples. The exact
compiled control spends 35.74% in `block_sums`, 19.24% in `PlaneQuery::dot`,
7.60% in its scan, 6.55% in exact L2, 6.26% in `FastQuery::reset`, 5.55% in
`PlaneQuery::reset`, and 3.78% in memcpy. Full-NQ HTTP spends respectively
31.18%, 17.54%, 5.78%, 5.75%, 5.71%, 5.05%, and 3.82% there. Its number parser
is 0.98%; the small HTTP parser is 4.61%. These are cycle shares, not instruction
shares, and symbol inlining differs between binaries.

Scheduler traces expose a second cost that user-cycle counts cannot capture.
Cropped to the interval with active query workers, the 27,616-query NQ trace has
95.32 aggregate CPU-seconds and 6.96 runnable-wait seconds on query workers;
Tokio runtime workers add 7.35 CPU-seconds and **34.17 runnable-wait seconds**.
There are 9,475 query-worker wakes, including 3,334 from another CPU, and 74,659
runtime-worker wakes, including 42,379 from another CPU. Query/runtime threads
migrate 3,326 / 19,857 times. The 24,576-query small trace has 5.12 / 5.20
query/runtime runnable-wait seconds, and 4,127 / 6,053 migrations. These are
aggregate scheduler measurements, not a sum of one query's critical-path stages;
several threads can wait simultaneously.

Pinning query workers to individual CPUs loses roughly 3–4% throughput and worsens
p90. A one-thread Tokio screen has no consistent win. Deferring the slab-state
clone until a compiled miss saves a little work, but its ABBA wins 2.1% at 48
probes and loses 2.2% at 64 probes, so it is discarded. No affinity, reactor-thread
setting, or clone-path change is retained.

The expanded 80-pair cold confirmation for the mask fix follows the inconclusive
40-pair sample above. At 48 / 64 probes, main p50 is 262.60 / 265.40 ms and the fix
is 260.93 / 266.71 ms. Paired median changes are -0.82 ms [-2.80, +2.18] /
-1.15 ms [-3.93, +1.41]. P90 improves 294.57→274.67 / 286.55→280.74 ms;
paired p90 changes are -19.89 ms [-50.77, +3.61] / -5.81 ms [-43.69, +37.77].
Rows and distances agree. This confirmation detects no cold regression; its broad
p90 intervals still limit the precision of any claimed cold benefit.

Applying Arm's Neoverse V1 `Topdown_L1` formulas (Telemetry Specification, issue 02,
section 5.1) gives the approximate slot shares below. Backend slots are divided by
eight times cycles; frontend slots also subtract the specified branch-mispredict
correction. Retiring and bad speculation use the retired/speculated operation
ratio. Events come from separate non-multiplexed passes over identical query
sequences, so the four estimates need not sum to exactly 100% (observed totals
99.1–100.4%). They are not the overlapping stalled-cycle ratios above.

https://documentation-service.arm.com/static/66f715817243ed16264336b6

| Dataset and path | Frontend bound | Backend bound | Retiring | Bad speculation |
|---|---:|---:|---:|---:|
| Small native | 17.3% | 39.1% | 40.3% | 3.3% |
| Small HTTP | 23.8% | 44.9% | 27.2% | 4.5% |
| NQ native | 16.0% | 46.4% | 34.9% | 1.7% |
| NQ HTTP | 18.3% | 48.6% | 32.0% | 1.4% |

### Leave scheduling capacity for completions

For `N` available CPUs, the CPU pool uses `N - 1` workers when `N >= 8`, and `N`
workers otherwise. The minimum remains one; query admission remains
`max(8, 2 * N)`. Workers remain schedulable on every allowed CPU. This leaves some
capacity for WAL-check completions and response delivery under sustained compute
load, while preserving the existing worker count on small hosts.

On the unmasked first publication, the initial four-epoch ABBA gains 2.1% q/s at
48 probes but loses 1.2% at 64, with p90 improving 10.4% / 9.6%. The longer
confirmation uses twelve measured full-query epochs per process:

| Probes | Main / candidate q/s | Change | Main / candidate p90 (ms) | User cycles/query, main / candidate |
|---:|---:|---:|---:|---:|
| 48 | 1,894 / 1,994 | +5.3% | 11.52 / 10.08 | 9.325 / 8.976 M |
| 64 | 1,682 / 1,696 | +0.8% | 13.00 / 11.65 | 10.603 / 10.661 M |

The throughput gain at 64 probes is small relative to process variation. The
repeatable result is the lower p90 at both probe counts. Instructions remain
22.628→22.616 M / 26.110→26.098 M; this is a scheduling change, rather than less
scoring work. Migrations fall 0.899→0.485 / 0.845→0.440 per query. Context switches
are 5.017→4.954 / 5.397→5.154 per query. All counts have 100% scheduling coverage.

With one covered-segment compaction, the combined mask and worker-pool candidate's
four-epoch ABBA gives:

| Probes | Main / candidate q/s | Change | Main / candidate p90 (ms) | User cycles/query, main / candidate |
|---:|---:|---:|---:|---:|
| 48 | 1,489 / 1,948 | +30.8% | 14.77 / 10.33 | 12.059 / 9.123 M |
| 64 | 1,336 / 1,722 | +28.9% | 16.40 / 11.52 | 13.610 / 10.464 M |

Every ordered row and distance matches the original reference. The separate
96-case response-shape comparison passes: 80 successes and sixteen expected
errors, including repeated requests against both binaries. Scoped checks pass
again: 223 tests, zero failures, and six explicit ignores.

### Wall-time stages

A separate diagnostic build replaces per-request stderr output with aggregate
histograms. The sample subtracts a metrics snapshot after warming from another
after four measured epochs, giving exactly 2,048 small / 13,808 NQ samples for
each stage. It uses the combined mask and seven-worker candidate at 48 probes.
These are diagnostic timings, rather than additional throughput gate results.
The earlier stderr trace slowed NQ to about 1,509 q/s and was discarded for
quantitative wall attribution; aggregate instrumentation runs at about 1,934 q/s.

| Mean stage (µs) | Small tier | NQ |
|---|---:|---:|
| Body collection | 10 | 6 |
| JSON decode | 63 | 65 |
| Search-state clone | 1.3 | 2.2 |
| CPU-worker queue | 292 | 3,051 |
| CPU-worker poll, including scoring and result assembly | 671 | 3,473 |
| Worker completion to Tokio continuation | 191 | 544 |
| Search execution, including dispatch | 1,160 | 7,077 |
| Snapshot check, overlapping search | 862 | 766 |
| Complete engine call | 1,561 | 7,499 |
| Response encoding | 7.5 | 13.3 |
| Client request to fully received response | 2,259 | 8,183 |

Queue, poll, and continuation nearly sum to search execution. Snapshot time
**overlaps** search and must not be added to it. The complete engine call includes
other state-lock acquisition, context setup, and finalization. Another roughly
0.6 ms remains outside the measured engine, parser, collector, and encoder stages:
this includes routing, scheduling before the extractor runs, response delivery,
and client transport. Connections are reused, so accept cost is amortized over
an epoch; the body timer starts when the extractor is polled, rather than when
bytes reach the socket. These are per-stage means over complete query cohorts,
not a reconstruction of each request's unobserved network timeline.

Snapshot cloning and encoding are small here. Memcpy's cycle share also includes
scoring scratch and rerank work, so charging it all to query-vector copies would
misidentify the cause. The fixed pool's queue and the Tokio continuation explain
substantial wall time that is absent from an isolated scorer's user-cycle count.
All additional timing hooks remain scratch-only experiment code.

### Direct-response ceiling and bounded candidate

The scratch-only numeric-ID ceiling loads one flat ID table before measurement,
skips document hydration and generic result evaluation, and emits rows directly
into a JSON byte buffer. Against the seven-worker candidate, its ABBA gains
1.04% / 3.58% q/s at 48 / 64 probes, with p90 down 0.75% / 2.24% and instructions
down about 1.1%. Every ordered row and distance matches.

A bounded version learns numeric IDs from ordinary winner reads, allocates pages
lazily, reserves their complete upper bound, and falls back for strings, unknown
IDs, and `u64::MAX`. Its narrow, normally validated ID-only ANN shortcut preserves
the existing response type and serializer. The corrected eight-epoch ABBA gains
**2.37% / 1.58%** q/s, p90 improves 1.83% / 0.98%, and instructions fall
0.49% / 0.40%. This misses the coordinator's two-probe 2% retention gate, so the
cache, assembly shortcut, and candidate-only tests are discarded. The initial
bounded screen did not match the benchmark's `["id"]` projection and is excluded
from the decision. No new production ID table or response path is shipped.

The long small control repeats 512 distinct queries 64 times per epoch. It gains
0.69% q/s, improves p90 1.50%, and leaves user cycles effectively unchanged
(1.988 M/query in both arms). The repeat-aware baseline verifier accepts either
one original epoch or the complete repeated sequence, and still compares every
ordered row and distance. Its first run exposed the old verifier's sequence-length
mismatch; that failed measurement is discarded.

A remaining miscellaneous cycle cost is Crossbeam epoch advancement. In the
full-NQ HTTP sample, 1,456 of 2,095 samples whose leaf is `try_advance` have
identifiable vector-cache lookup or housekeeping callers; 97 identify document
columns. The exact compiled scorer has only 75 such samples in its comparable
warm-only profile. Stack recovery is incomplete for some samples, and these are
sample counts rather than a new percentage estimate. This points to sparse-vector
cache maintenance as follow-up work; bypassing document hydration alone does not
remove it.

### Final gate on integrated main

The attribution, prototypes, and initial gates above use task-start main
`ddbb7bb`. The final gate rebuilds both arms against **`aba5c96`**, incorporating
RR2, C3, MEM2, BW1, and MD1. The coordinator approved closing on this pinned
comparison after the later predicate-cache change landed. That combination will
receive its own coordinator gate. The original NQ publication and its ordinary
compaction remain frozen; upgrading the code does not rebuild either tier.

Both release binaries use the same toolchain and frame-pointer flags. Unpacking
source with preserved old mtimes initially let Cargo reuse the baseline artifact;
that was caught by comparing binary hashes before measurement. Cleaning the two
release packages forced a distinct candidate build. The merged source passes
formatting, scoped all-target Clippy with warnings denied, and scoped tests:
**235 passed, zero failed, and eight explicit ignores**. The compiled-tier
regression retains the upstream 60-row and 640-row cases and the rejection-marker
coverage, alongside Q3's compaction and mutation cases.

Final warm ABBA, sixteen clients, three warming epochs, and four measured epochs
per process:

| Publication | Probes | Main / Q3 q/s | Change | Main / Q3 p90 (ms) | User cycles/query, main / Q3 |
|---|---:|---:|---:|---:|---:|
| First complete tier | 48 | 2,016 / 2,077 | +3.0% | 10.78 / 9.87 | 8.723 / 8.548 M |
| First complete tier | 64 | 1,731 / 1,770 | +2.3% | 12.61 / 11.29 | 10.340 / 10.170 M |
| After pure compaction | 48 | 1,671 / 2,040 | +22.1% | 13.14 / 10.04 | 10.507 / 8.702 M |
| After pure compaction | 64 | 1,467 / 1,745 | +19.0% | 15.00 / 11.51 | 12.184 / 10.347 M |

All ordered rows and distances match both arms and the original publication
reference. Recall remains 0.9484067207 / 0.9590092700. After compaction, user
instructions fall 24.901→22.307 M / 28.571→25.749 M; context switches fall
9.212→4.817 / 9.205→4.925, and migrations fall 1.079→0.424 / 1.012→0.358 per
query. Counter scheduling coverage is 100%.

The four-core confirmation uses four CPU workers in both arms. Its q/s changes
are -0.96% / -2.09%, with p90 +0.37% / +2.10%. These small-host point differences
remain an uncertainty; there is no claimed four-core gain. The earlier task-start
four-core screen was -2.7% / -3.3%. The sixteen-core task-start control is flat /
+1.1% q/s, with p90 -2.2% / -3.6%; its clients share CPUs 8–15, so it is a topology
control rather than the isolated-client gate. The worker rule preserves one and
two workers on one- and two-CPU hosts. No M4, larger-host, or simultaneous-ingest
performance claim is made.

The final 96-case response-shape check again gives 80 successes, sixteen expected
errors, and identical responses. Final cold confirmation uses eighty ABBA query
pairs at each probe count, fresh processes, and the standard 25 ms S3 facade:

| Probes | Main / Q3 p50 (ms) | Main / Q3 p90 (ms) | Paired median change, 95% CI (ms) | Paired p90 change, 95% CI (ms) |
|---:|---:|---:|---:|---:|
| 48 | 262.32 / 261.94 | 298.56 / 288.24 | -1.39 [-3.37, +1.47] | -10.32 [-49.60, +41.68] |
| 64 | 266.36 / 265.83 | 301.98 / 280.50 | +0.01 [-2.44, +1.64] | -21.48 [-53.68, +12.98] |

Rows and distances agree. This detects no cold regression; broad tail intervals
limit any claim of a cold benefit. The preceding sixty-pair combined-candidate
screen had higher p90 points (+11.88 / +19.90 ms), with intervals spanning zero;
it is superseded by this confirmation, rather than treated as evidence of a win.

A separate four-epoch counter control on the first publication collects user
cycles, kernel cycles, and user instructions together, within the host's three
available PMU counters. The native reference uses eight scorer threads; the HTTP
candidate uses seven compute workers plus its runtime. Native kernel counting
starts after the warmed-ready stop and ends when the timed result is emitted,
excluding teardown of the multi-gigabyte resident arrays. The initial through-exit
native kernel samples are discarded because teardown inflated them.

| Dataset | Native user / kernel cycles (M/query) | Main HTTP user / kernel cycles | Q3 HTTP user / kernel cycles |
|---|---:|---:|---:|
| Small, 48 probes | 1.073 / 0.001 | 1.997 / 0.182 | 1.991 / 0.178 |
| NQ, 48 probes | 8.213 / 0.008 | 8.700 / 0.232 | 8.430 / 0.193 |
| NQ, 64 probes | 9.550 / 0.009 | 10.039 / 0.235 | 10.378 / 0.216 |

The scorer is unchanged by Q3, so its native result is a common before/after
reference. User instructions are 3.337 / 4.162 / 4.156 M for small native / main
HTTP / Q3 HTTP; 22.216 / 22.300 / 22.285 M for NQ48; and
25.813 / 25.786 / 25.756 M for NQ64. Counter coverage is 100% in both the internal
native counters and the external kernel interval. The independent NQ64 user-cycle
point varies by 3.4% in the opposite direction from the warm ABBA's 1.6% decrease;
this is not evidence of a repeatable unmasked CPU-cycle win at that probe count.
Use the paired warm gates above for throughput and latency conclusions. The small
HTTP/native cycle difference remains substantial.

## Round BW2: shadow statistics from indexed postings

At 1,000 text-changing upserts/s, paired BM25 p50 falls **49.011 → 29.734 ms**
(**1.65×**). All 4,000 frozen queries retain exact score bits, write-service p90
increases 2.62%, every offered write is acknowledged, and idle latency holds.

The first reproduction on the BW2 host gives **49.493/69.522 ms p50/p90** under
1,000 text-changing upserts/s, with all 120,000 offered rows acknowledged. Shadow
correction remains the largest component: **22.378/29.261 ms**, against
14.379/18.592 ms for tail-statistics analysis, and 17.118/33.163 ms of summed
segment scoring and hydration. Checkpoint-statistics lookup is 0.085/0.130 ms.
These are the 90 measured requests in the first baseline run. Nested statistics
and overlapping segment-worker durations are not additive. BW1's historical
56.884 ms median is context; the comparisons below use fresh, paired BW2 runs.

### Implementation

The correction reads exact normalization lengths from each text index and counts
query-term postings inside the bitmap of indexed rows replaced or deleted by the
WAL. It first intersects that bitmap with the published live mask, preventing a
second subtraction of rows already absent from checkpoint statistics. Length
zero denotes a missing/null field; present empty fields, including text emptied
by stopword removal, already have length one. No document-column read is needed
to distinguish them.

`SegmentReader::document_frequencies` counts exact terms in input order, including
repetitions and absent terms. It reads intersecting posting blocks in batches,
uses the smaller of the selected-row set and posting list within each block,
and takes counts directly from metadata for full masks or gap-free blocks.
Positions remain lazy. Existing posting and dictionary caches retain their
existing limits.

The same snapshot-local bitmap excludes shadowed hits during segment search.
It travels through the query's read context to its search workers, and leaves
scope with that query. It is never cached across snapshots. Idle queries retain
the existing checkpoint-statistics path. WAL term summaries still use BW1's
bounded cache; shadow-document summaries are removed. Commits perform no new
analysis or index maintenance. No dependency, wire response, or persistent
format changes.

### Paired protocol

One owned, on-demand c7g.4xlarge in us-west-2, Ubuntu 24.04 arm64, Rust 1.100.0,
and Clang 19. Both timed release binaries use the same build settings. Server
CPUs are 0–5, client CPUs are 6–7, the block cache is 2 GiB, and disk caching is
disabled. Decoded-cache and indexing settings retain their defaults.

The store contains the first million passages from the same MS MARCO stream
as BW1. Its initial six segments contain 125,000, 200,000, 175,000, 200,000,
200,000, and 100,000 rows. Loading drains the exhaustive tail to zero before
copying the checkpoint. Each arm starts from an identical disposable copy.
Every HTTP run prewarms all 1,000 queries, then runs for 120 seconds. Query
percentiles exclude the first 30 seconds; write-service percentiles cover all
1,200 requests in each write-loaded arm. The query and write seeds, text-changing
100-row batches, strong consistency, top ten, and IDs-only projection are BW1's settings.
Queries run at one Hz, and writes at 1,000 rows/s. Write-loaded arms run A/B/B/A;
idle controls run separately in A/B/B/A order. Builds and independent reference
generation finish before candidate timing. The first write-loaded pair enables
stage traces; the second disables them.

Baseline source is `aba5c96`; candidate source is `a8b9201`. Executable SHA-256:

- Baseline: `d871681449ef2a48b6f15e8ac64ed87f4fca205b5c564a85b226b2866b59b983`.
- Candidate: `31ef5e3eede0aebbf3462617b3ea74e8b2993cc0c1b0036ce57ce75620b8e102`.

The million-row corpus JSONL SHA-256 is
`c9d3f3493900d156cf1cfaf514a81a8cbea6e1f02a266bc8ce73165148ce42d6`;
the checked-in workload SHA-256 is
`3c8f1b03e4d8293ec9c45f53e8d1f94bfe4bd5cbb23d1e31623df655baf9f472`.

### Stage confirmation

The traced first pair contains the same 90 measured query IDs, in the same order.
Per-request stage percentiles are:

| Component | Baseline p50 / p90 ms | Candidate p50 / p90 ms |
| --- | ---: | ---: |
| Global statistics | 36.836 / 48.103 | 19.301 / 25.901 |
| Checkpoint statistics, inside global statistics | 0.085 / 0.130 | 0.071 / 0.121 |
| Shadow statistics, inside global statistics | 22.378 / 29.261 | 6.340 / 10.387 |
| Tail-statistics analysis, inside global statistics | 14.379 / 18.592 | 13.077 / 16.846 |
| Segment scoring and hydration, summed worker elapsed time | 17.118 / 33.163 | 11.097 / 24.749 |
| Tail analysis or summary lookup for scoring | 1.272 / 2.256 | 1.181 / 2.073 |
| Tail scoring | 3.372 / 6.544 | 2.332 / 5.027 |
| Final merge | 0.022 / 0.024 | 0.021 / 0.023 |

Shadow correction falls **71.7% at p50**. Reusing its bitmap also removes repeated
ID lookups from segment search. Tail-statistics analysis is now the largest
statistics component. The measured tail medians in this pair are 3,046.5 and
2,945 rows. Nested and overlapping durations remain non-additive; these figures
include the actual publication and cache behavior of each arm.

### Exactness and checks

BW1's serialized-snapshot checker passes all **4,000 queries**, including all
**40,000 ordered IDs and f32 score bits**, against the independently compiled
exhaustive `exact_bm25` executable. The four retained states cover the fully
indexed million passages, 5,000 overwrites, overlapping updates plus deletes and
nulls, and the next index publication. Old snapshots remain queryable after the
later publication. Reference generation and checking take 155.57 seconds,
outside all candidate timing.

Formatting, scoped Clippy with warnings denied, and
`cargo test -p bf-engine -p bf-text` pass: **252 tests, zero failures, and 15 expected ignored tests**.
New coverage checks raw document presence against masked frequencies, sparse
and dense masks, repeated and absent terms, missing and empty fields, lazy reads,
packed tails, and text formats 4–7. An engine test removes the document-column
files after computing its reference, clears storage caches, and verifies exact IDs-only BM25 with zero
and positive decoded-cache budgets. Existing tests retain coverage of arrays,
prefixes, filters, computed scores, changed analysis settings, and publications.
The local and EC2 changed-source hashes match.

The frozen checker uses the same `BW1_STORE` and `BW1_REFERENCE` variables as the
previous round. Its independent reference executable SHA-256 is
`b3b1bf4d1cb659bc5b35ba4017a723957dbfd7488543ddc98aebc7731f85bfad`.
ARM test linking uses Clang 22, LLD 19, and LLVM 22 compiler builtins; both timed
HTTP binaries use Clang 19's normal linker.

### Write-loaded result

All four arms execute the same **90 measured queries**. Their ordered query-index
list has SHA-256
`d7c5fdacd887c880c0e518841ac904d3a4b77dc83379b18122ce8e2f9143ed59`.

| Arm | BM25 p50 / p90 ms | Write-service p50 / p90 ms | Tail median / maximum | Acknowledged / offered rows |
| --- | ---: | ---: | ---: | ---: |
| A1, baseline, traced | 49.493 / 69.522 | 1.241 / 1.796 | 3,046.5 / 5,686 | 120,000 / 120,000 |
| B1, candidate, traced | 30.125 / 41.286 | 1.284 / 1.863 | 2,945 / 5,685 | 120,000 / 120,000 |
| B2, candidate | 29.342 / 40.220 | 1.296 / 1.862 | 2,945.5 / 5,589 | 120,000 / 120,000 |
| A2, baseline | 48.530 / 70.535 | 1.278 / 1.834 | 2,894.5 / 5,880 | 120,000 / 120,000 |

Medians of the two run percentiles are **49.011/70.029 → 29.734/40.753 ms**:
**1.65× faster p50, and 1.72× faster p90**. Both candidate repetitions beat
44 ms p50. Median write-service p50/p90 changes from **1.260/1.815** to
**1.290/1.862 ms**, increases of **2.43%/2.62%**, within the 5% p90 allowance.
Every arm acknowledges every offered row, with zero rejections: **480,000 of
480,000** in total. This establishes parity at the offered rate; maximum write
capacity is not measured.

### Idle control and reproduction

All idle arms use the same query schedule, initial checkpoint, prewarm, and
90-query measured slice as the write-loaded arms. Every query has zero exhaustive
tail rows.

| Arm | BM25 p50 / p90 ms |
| --- | ---: |
| A1, baseline | 2.501 / 4.441 |
| B1, candidate | 2.451 / 4.344 |
| B2, candidate | 2.377 / 4.139 |
| A2, baseline | 2.496 / 4.171 |

Median run percentiles are **2.498/4.306 → 2.414/4.242 ms**. Idle BM25 does not
regress on this control. Together with the frozen exactness check, acknowledged
writes, and write-service allowance, the paired runs pass all BW2 gates.

Use BW1's reproduction command with the sources above, an owned scratch directory,
`--bm25-hz 1`, `--seconds 120`, `--warmup 30`, and a fresh fully checkpointed store
copy per arm. Use `--rate 1000` for write-loaded arms and `--rate 0` for idle.
Both source builds, reference generation, and frozen checking must finish before
timing. This round measures a warm local store; cold S3, broader ANN, small-memory,
full-workspace, and official-SDK integration remain coordinator validation.

The owned c7g.4xlarge ran for **0.93 hours**. Estimated cost is **about $0.56**,
including compute at BW1's $0.58/hour rate and the 120 GiB gp3 root volume.
The instance is terminated; its root volume, tagged key pair, and security group
are confirmed absent. No S3 objects or prefixes were created. Owned scratch
artifacts were removed after retaining these results.
