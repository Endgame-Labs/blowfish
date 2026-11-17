# Benchmarks

This page summarizes blowfish's measured query and upsert latency against turbopuffer's published numbers, along with the network round-trip profile, 1M multi-workload breakdown, recall and result-set parity, ingest throughput, known engineering gaps, and reproduction commands.

For the complete chronological engineering log across iterations 0 through 10 and the full apples-to-apples run notes, see [`../benchmarks.md`](../benchmarks.md).

## Against turbopuffer's published numbers

Client-side `p50 / p90` latency in milliseconds (with strong consistency on every query), compared against turbopuffer's [published benchmarks](https://turbopuffer.com/docs/performance):

| Benchmark | turbopuffer published (p50 / p90) | blowfish measured (p50 / p90) | Ratio (p50 / p90) |
| --- | ---: | ---: | ---: |
| Vector, 10M docs, cold (`BF_ANN_NPROBE=160`) | 874 / 1,214 ms | 5,175 / 6,528 ms | 5.9x / 5.4x |
| Vector, 10M docs, warm (`BF_ANN_NPROBE=160`) | 14 / 17 ms | 136 / 168 ms | 9.7x / 9.9x |
| BM25, 1M docs, cold | 316 / 381 ms | 1,570 / 1,885 ms | 5.0x / 4.9x |
| BM25, 1M docs, warm | 13 / 18 ms | 77 / 108 ms | 5.9x / 6.0x |
| Upsert, 512 KB batch | 165 / 248 ms | 792 / 1,531 ms | 4.8x / 6.2x |

Today blowfish is about 5–6x slower than turbopuffer on cold queries and 6–10x slower on warm queries, at matching recall. Most of the cold gap is engineering work in progress: fewer requests and fewer bytes per cold query.

### Vector search: 10M documents, strong consistency

| Setting | Cold p50 / p90 (ms) | Warm p50 / p90 (ms) |
| --- | ---: | ---: |
| turbopuffer published (768-dim, in-region) | 874 / 1,214 | 14 / 17 |
| blowfish measured, 160 probes (1,024-dim) | 5,175 / 6,528 | 136 / 168 |
| blowfish measured, 64 probes (default, 1,024-dim) | 2,790 / 3,862 | 88 / 128 |
| blowfish network-adjusted estimate, 160 probes | 4,870 / 6,222 | 68 / 100 |
| blowfish network-adjusted estimate, 64 probes | 2,491 / 3,563 | 20 / 60 |

| blowfish 10M vector configuration | Cold recall@10 | Warm recall@10 | Cold requests / query | Cold MB / query | Sequential waves |
| --- | ---: | ---: | ---: | ---: | ---: |
| `BF_ANN_NPROBE=160` (recall-matched) | 0.952 | 0.947 | 2,093 | 137 | 4.5 |
| Default (`nprobe=64`) | 0.886 | 0.886 | 1,103 | 67 | 4.4 |

- **Setup and recall:** blowfish's 10M Wikipedia namespace uses 1,024-dim `f16` vectors across 70 segments (turbopuffer's published run uses 768-dim `f16` vectors, so blowfish's vectors are 33% larger per row). Against exact cosine top-10 ground truth on the same 200 held-out queries, turbopuffer's measured recall@10 is **0.943**; `BF_ANN_NPROBE=160` is the apples-to-apples setting because it matches that recall (`0.952` over the 100 cold queries and `0.947` over the 1,000 warm queries). At the default 64 probes, latency roughly halves, but recall@10 drops to **0.886**.
- **Sampling:** cold runs `n = 100` queries per arm (interleaving the 64-probe and 160-probe arms in a single session on fresh server processes with empty memory and disk caches); warm runs `n = 1,000` queries at concurrency 1 after two unmeasured warm-up passes.
- **Why network adjustment barely moves cold vector:** 4.4–4.5 sequential waves at ~68.5 ms of excess round-trip latency account for ~300 ms, while the query transfers 67–137 MB across 1,103–2,093 requests. Cold vector latency is bound by bytes, request count, and segment count rather than round-trip time. Network-adjusted rows are estimates only, never replacements for the measured numbers.

### BM25 full-text search: 1M documents, strong consistency

| Setting | Cold p50 / p90 (ms) | Warm p50 / p90 (ms) |
| --- | ---: | ---: |
| turbopuffer published (~300 MB text, in-region) | 316 / 381 | 13 / 18 |
| blowfish measured (335 MB text) | 1,570 / 1,885 | 77 / 108 |
| blowfish network-adjusted estimate | 1,346 / 1,661 | 9 / 40 |

- **Setup:** the first 1,000,000 passages of [`Tevatron/msmarco-passage-corpus`](https://huggingface.co/datasets/Tevatron/msmarco-passage-corpus), text only, with one `text` attribute (`full_text_search: true`, default tokenizer) and no vectors. Total raw text is **335 MB** (335 bytes per passage on average; `approx_logical_bytes` reports 358 MB). Queries are 1,000 seeded MS MARCO dev queries (`top_k = 10`).
- **Diagnostics and sampling:** cold (`n = 100` fresh-process queries) averages **198 object-store requests** and **17.6 MB** across **3.3 sequential waves**. Warm (`n = 1,000` queries at concurrency 1 after one warm-up pass) performs one strong-consistency check against the bucket per query. Network-adjusted figures are estimates only.

### Upsert: ~512 KB batches

| Run | p50 (ms) | p90 (ms) |
| --- | ---: | ---: |
| turbopuffer published | 165 | 248 |
| blowfish measured, run 2 (300 writes) | 792 | 1,531 |
| blowfish measured, run 1 (300 writes, slower bucket session) | 2,114 | 2,563 |

- **Setup and session variance:** each batch contains 68 synthetic rows (about 506 KB of JSON: 30–70 pseudo-random words of text, a category string, and a 768-dim `f32` vector as a JSON array), sent as 300 sequential writes into a fresh namespace with the background indexer running and acknowledged once durable in object storage (zero `429` retries). Two identical 300-write runs differed by 2.7x (`792 ms` vs. `2,114 ms` `p50`) due to bucket write-latency variance across hours, so treat upsert `p50` as roughly **0.8 to 2.1 s**.
- **Write path and network-adjusted estimate:** each sequential write executes two `PUT`s (the WAL payload, then the conditional slot record). As a `p50` estimate only on run 2, replacing two ~270 ms `PUT` round trips with 15 ms in-region `PUT`s leaves ~282 ms (~656 ms if adjusted using the 83.5 ms `GET` RTT).

## Network and environment

- **Compute host:** an 8-vCPU x86-64 VM (AMD EPYC 9354P, Zen 4) in Los Angeles with 32 GiB RAM and local NVMe, running no other heavy workloads during measurements. The benchmark server runs inside a `systemd` scope with `MemoryHigh=14G`, `MemoryMax=16G`, 6 Rayon worker threads, and an 8 GiB memory block cache (`4,000`-byte blocks).
- **Object-storage path:** blowfish reaches an R2 bucket (location hint `WNAM`) over the public internet, whereas turbopuffer measures in-region next to object storage. Direct measurements from this VM (`bench/scale/r2_rtt.py` and single-object `PUT` probes) show:

| Operation (VM in Los Angeles to R2 endpoint) | Samples | p50 (ms) | p90 (ms) |
| --- | ---: | ---: | ---: |
| TCP connect | 200 | 6.2 | 8.5 |
| TLS handshake | 200 | 26.4 | 35.0 |
| 4 KB ranged `GET` (warm connection) | 200 | 83.5 | 129.8 |
| Single-object `PUT` (200 B to 500 KB) | 30 / size | 260–304 | 346–499 |

- **Connection, tail, and bandwidth behavior:**
  - TCP connect takes ~6 ms (nearby network edge), so the ~84 ms 4 KB `GET` `p50` is object-store time to first byte, and a small-to-medium `PUT` costs roughly three times a `GET`.
  - A parallel wave of `N` ranged `GET`s waits on the slowest request in the wave (`crates/bf-storage/examples/r2_floor.rs`: median wave wall time on warm HTTP/1.1 connections rises from ~174 ms at `N = 32` to ~291–374 ms at `N = 256` and ~553–983 ms at `N = 1,024`; hedged reads cap the slowest tail outliers within a 10% extra-request budget).
  - Aggregate upload bandwidth from this VM to the bucket tops out at **13.9 MB/s** (~14 MB/s) across 4–16 parallel streams, and download throughput tops out around 60–180 MB/s.
  - Bucket round-trip latency shifts by up to 2x between sessions an hour apart, so cold code and flag comparisons interleave arms query by query in the same session (`bench/scale/ab.py`).
- **Network-adjusted estimate formula:** `measured_ms - waves * (83.5 ms - 15 ms)`, where `15 ms` is an assumed in-region round trip, `83.5 ms` is the measured warm 4 KB `GET` `p50`, and `waves` is the mean number of sequential rounds of parallel reads per query from `BF_TRACE_READS` (`1` wave for a warm strong-consistency WAL tail check). Because this subtracts round-trip time only and ignores bandwidth, it is reported strictly as an illustrative estimate alongside measured numbers, never instead of them.

## Methodology

### Datasets and schemas

- **Wikipedia 1M and 10M (vector, hybrid, filtered vector, and parity):** [`CohereLabs/wikipedia-2023-11-embed-multilingual-v3`](https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3) (English subset, revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`), streamed from 100,000-row Parquet shards (1M = first 1M passages; 10M = first 10M passages, 45.0 GB raw uncompressed source).
  - **Vectors:** 1,024-dimensional embeddings stored as `[1024]f16` with `cosine_distance`. Every source element is verified to be losslessly representable in IEEE-754 `f16` before upload, discarding no source precision.
  - **Attributes:** `text` (`string`, `full_text_search: true`), `title` (`string`, stored unindexed), `title_initial` (lowercased first character of `title`, filterable), and `text_length` (Unicode character count of `text`, filterable).
  - **Held-out queries and exact ground truth:** 200 passages evenly spaced across the first 100,000 source rows (`0, 500, 1000, ..., 99500`) are excluded from ingestion. Their embeddings serve as ANN query vectors, and their article titles serve as Wikipedia BM25 queries. Exact cosine top-10 ground truth is computed over every ingested row (`bench/scale/data/oracle-1m.npz` and `bench/scale/data/oracle-10m.npz`), including separate exact top-10 sets for the filtered workloads.
- **MS MARCO 1M (headline BM25 comparison):** first 1,000,000 passages of [`Tevatron/msmarco-passage-corpus`](https://huggingface.co/datasets/Tevatron/msmarco-passage-corpus), text-only (`full_text_search: true`), 335 MB of text, queried with 1,000 seeded MS MARCO dev queries (`BeIR/msmarco` joined to `BeIR/msmarco-qrels` `dev.tsv`).
- **Synthetic 512 KB batches (headline upsert comparison):** 68 rows per request (~506 KB JSON) with unique integer IDs, 30–70 words of full-text-indexed `text`, `category`, and normalized `[768]f32` cosine vectors.

### Query types on the Wikipedia dataset

Every query requests `top_k = 10` (or `limit = 10` for `multi_query`) with **strong consistency**:

- **Vector (ANN):** `["vector", "ANN", query_vector]`
- **BM25:** `["text", "BM25", query_title]`
- **Hybrid (BM25 + vector, RRF):** `multi_query` combining the ANN and BM25 subqueries (`top_k = 10` each), fused with Reciprocal Rank Fusion (`rerank_by: ["RRF"]`, `limit: 10`)
- **Selective-filter vector:** ANN with `["title_initial", "Eq", "z"]` (matches ~0.4% of the corpus: 4,181 rows at 1M and ~42,000 rows at 10M)
- **Broad-filter vector (parity runs):** ANN with `["text_length", "Gte", 300]`

### Cold vs. warm definitions

- **Cold (`bench/scale/ab.py`):** each measured cold query runs at concurrency 1 against a **freshly started, read-only server process** with an **empty 8 GiB memory cache and a newly created, empty local disk cache**. After startup, the harness waits 1.5 s for the HTTP connection warmer to establish idle TLS connections to the bucket (matching an already-running node serving a cold namespace), then times one query. Every manifest, cluster directory, posting-list chunk, quantized code block, and candidate document is fetched over the network from object storage.
- **Warm (`bench/scale/warm.py`):** a read-only server process completes unmeasured warm-up passes over the query set (two passes for 10M vector, one pass for 1M BM25), then runs 1,000 measured queries sequentially at concurrency 1 over a keep-alive HTTP connection. Active index metadata and data blocks reside in local memory and NVMe caches, while strong consistency still issues one bucket round trip per query to check the WAL tail.

## 1M Wikipedia query breakdown

Cold measurements across all four query types on the 1M English Wikipedia namespace (9 segments, strong consistency, same-session interleaved runs with `n = 24` queries per type and `n = 16` for selective):

| Query type | Cold p50 / p90 (ms) | Cold R2 requests / query | Cold MB / query | Sequential waves |
| --- | ---: | ---: | ---: | ---: |
| Vector (ANN) | 1,278 / 1,593 | 339 | 22.0 | 4.1 |
| BM25 | 869 / 1,594 | 310 | 9.2 | 3.2 |
| Hybrid (BM25 + vector, RRF) | 1,188 / 1,752 | 635 | 29.8 | 2.3 |
| Selective-filter vector (`title_initial == "z"`) | 1,973 / 2,600 | 1,904 | 41.9 | 4.1 |

With a 1,024-connection warm pool (instead of the initial 256-connection pool), selective-filter cold latency at 1M drops to **1,429 / 1,940 ms**. Warm strong-consistency queries at 1M take **about 50–60 ms at `p50`** (concurrency 1) from this VM, of which roughly 55 ms is the strong-consistency `HEAD` check against R2.

## Recall and turbopuffer parity

Live turbopuffer namespaces cannot be forced cold externally, so head-to-head runs against a live turbopuffer namespace loaded with the identical Wikipedia documents and schema (`bench/scale/oracle_compare.py`) are used strictly to evaluate **exact `recall@10`** against ground truth and **result-set parity (`overlap@10`, fraction of shared IDs in the top 10)** over all **200 held-out queries per workload**. BM25 and hybrid RRF have no independent exact ground truth, so they appear in the `overlap@10` parity tables only.

### 1M documents

Exact `recall@10` against cosine ground truth:

| Query type | blowfish | turbopuffer |
| --- | ---: | ---: |
| Vector (ANN) | 0.952 | 0.938 |
| Selective-filter vector (`title_initial == "z"`) | 1.000 | 0.998 |
| Broad-filter vector (`text_length >= 300`) | 0.974 | 0.977 |

Result-set parity with turbopuffer:

| Query type | `overlap@10` with turbopuffer |
| --- | ---: |
| Vector (ANN) | 0.935 |
| BM25 | 0.990 |
| Hybrid (BM25 + vector, RRF) | 0.971 |
| Selective-filter vector (`title_initial == "z"`) | 0.998 |
| Broad-filter vector (`text_length >= 300`) | 0.963 |

### 10M documents

Exact `recall@10` against cosine ground truth:

| Query type | blowfish (`nprobe=64`) | blowfish (`BF_ANN_NPROBE=160`) | turbopuffer |
| --- | ---: | ---: | ---: |
| Vector (ANN) | 0.886 | 0.947 | 0.943 |
| Selective-filter vector (`title_initial == "z"`) | 0.968 | 0.992 | 0.993 |

Result-set parity with turbopuffer (measured at `nprobe = 64`):

| Query type | `overlap@10` with turbopuffer |
| --- | ---: |
| Vector (ANN) | 0.887 |
| BM25 | 0.985 |
| Hybrid (BM25 + vector, RRF) | 0.935 |
| Selective-filter vector (`title_initial == "z"`) | 0.962 |

At 10M documents, the default `nprobe = 64` drops below turbopuffer's recall (`0.886` vs. `0.943` on ANN, and `0.968` vs. `0.993` on selective-filter ANN). Setting `BF_ANN_NPROBE=160` restores recall parity (`0.947` across all 200 queries, and `0.992` on selective-filter ANN).

## Ingest and storage footprint

- **Ingest rate:** about **2,000 docs/s** on pipelined WAL code (`BF_WAL_PIPELINE=8`, 2,000 rows per batch, 16 concurrent writes in flight: 2,040 docs/s measured end-to-end on 200K docs, ~1,860 docs/s sustained on 500K, and ~1,710 docs/s over the first 1M rows of the 10M load). Throughput on this VM is capped by its **~14 MB/s** aggregate upload link to the bucket: each document uploads ~3.0 KB of compressed WAL payload plus ~5.6 KB of segment files, while the server's commit worker is ~20% utilized.
- **10M memory behavior:** during the full 10M Wikipedia load inside a 16 GiB `systemd` scope (`MemoryHigh=14G`), resident memory rose by roughly 1.3 GB per million rows and spiked to 14–16 GB during multi-segment compactions past ~7M rows, requiring server restarts and capping segment size (`BF_MAX_SEGMENT_ROWS=100000`) for the final 2M rows (leaving 70 segments).
- **Retained storage size:** after compaction and garbage collection, the 10M Wikipedia namespace occupies **~5.6 KB per document** (~56.3 GB across 10M rows: ~2.5 KB/doc in columnar `.bfc` document files, ~2.2 KB/doc in `.bfv` ANN index files, and ~0.9 KB/doc across `.bft` full-text, `.bfx` filter, and `.bfi` ID indexes).

## Known gaps

- **Selective filters issue ~1,900 object-store requests per cold query at 1M** (fetching ~4,181 individual 2 KB vectors on the exact-fetch path when matching rows <= 8,192), and scan **~268 MB across ~2,600 requests at 10M** (where ~42,000 matching rows exceed the exact-fetch cap and fall back to widened RaBitQ code scans without a per-cluster allowed-row index).
- **The cold vector layout is not contiguous yet:** cluster metadata blobs, RaBitQ 1-bit codes, and full-precision rerank vectors live in separate regions across clusters that are not ordered by spatial proximity, leaving cold ANN at ~339 requests / 22 MB at 1M, ~1,103 requests / 67 MB at 10M with 64 probes, and ~2,093 requests / 137 MB at 10M with 160 probes.
- **Vectors are stored twice per segment:** once in the columnar document store (`.bfc`) for attribute projection and compaction, and once in the ANN index file (`.bfv`) for full-precision reranking, nearly doubling segment upload bytes and bucket footprint.
- **Ingest and compaction memory growth above ~5M rows:** compaction currently materializes bounded segment inputs in memory rather than streaming cluster-by-cluster, causing 13–15 GB RSS spikes near 7–10M rows on a 16 GiB budget.
- **Default probe scaling at 10M:** default `nprobe = 64` under-probes at 10M (`0.886` recall@10); raising `BF_ANN_NPROBE=160` recovers `0.947–0.952` recall@10 at roughly twice the cold bytes and requests (`5,175 / 6,528 ms` cold vs. `2,790 / 3,862 ms`).

## How to reproduce

The scale and apples-to-apples harness lives in [`bench/scale/`](../../bench/scale/). Private bucket endpoints, profiles, and namespace names stay in `bench/scale/config.local`, which is git-ignored; all build outputs, caches, downloaded shards, and logs stay under `.scratch/scale/` (`SCALE_SCRATCH`).

1. **Create `bench/scale/config.local` from [`bench/scale/config.example.json`](../../bench/scale/config.example.json) and build the benchmark server:**

   ```bash
   cp bench/scale/config.example.json bench/scale/config.local
   # Edit bench/scale/config.local with storage_url (s3://...), endpoint, region, profile, and namespace.
   bench/scale/build.sh
   ```

   `build.sh` compiles `blowfish-scale-server` (`crates/bf-engine/examples/scale-server`) into `.scratch/scale/target/release/`, wrapping the unmodified `blowfish::router` with `EngineConfig::from_env()` tuning knobs.

2. **Measure object-store round-trip latency (`r2_rtt.py`):**

   Point `r2_rtt.py` at `bench/scale/config.local` and any existing object key of at least 4 KB in the bucket to measure `p50 / p90` TCP connect, TLS handshake, and warm 4 KB ranged `GET` times:

   ```bash
   uv run bench/scale/r2_rtt.py bench/scale/config.local <object-key-at-least-4kb> --n 200
   ```

3. **Generate the apples-to-apples query workloads (`apples_workloads.py`):**

   Writes `ann-apples.json` (200 held-out 1,024-dim strong ANN queries with exact top-10 ground truth from `bench/scale/data/oracle-10m.npz`) and `bm25-msmarco.json` (1,000 seeded MS MARCO dev queries, strong BM25 `top_k = 10`, also checked in under `bench/scale/data/`):

   ```bash
   uv run bench/scale/apples_workloads.py --msmarco-queries 1000 --out bench/scale/data
   ```

4. **Load datasets (`load.sh` for Wikipedia 1M/10M, `msmarco_load.py` for MS MARCO 1M) and benchmark upserts (`upsert_bench.py`):**

   ```bash
   mkdir -p .scratch/scale/cache
   python3 bench/scale/mkconfig.py bench/scale/config.local .scratch/scale/storage.json .scratch/scale/cache
   BF_TRACE_STAGES=1 bench/scale/serve.sh .scratch/scale/storage.json > .scratch/scale/server.log 2>&1 &

   # Load 1M or 10M Wikipedia passages (uses the namespace in config.local)
   bench/scale/load.sh 1000000 --inflight 16
   bench/scale/wait_idle.sh .scratch/scale/server.log

   # Stream 1M MS MARCO text passages into a dedicated namespace (no local shard files kept)
   uv run bench/scale/msmarco_load.py --base-url http://127.0.0.1:8300 --namespace msmarco-1m --rows 1000000
   bench/scale/wait_idle.sh .scratch/scale/server.log

   # Sequential ~512 KB batch upsert benchmark (300 writes; --delete removes the test namespace afterward)
   uv run bench/scale/upsert_bench.py http://127.0.0.1:8300 upsert-bench --writes 300 --target-kb 512 --delete
   ```

   Stop the writable server before running read-only query benchmarks (`systemctl --user stop scale-server.scope`).

5. **Measure cold latency (`ab.py`) and warm latency (`warm.py`):**

   `ab.py` starts a fresh read-only server process with an empty memory and disk cache for every query (waiting 1.5 s for the connection warmer), rotating arms query by query:

   ```bash
   # Cold 10M vector: interleave default 64 probes and recall-matched 160 probes (n = 100 each)
   uv run --with numpy python bench/scale/ab.py --kind ann --queries 100 \
     --workload bench/scale/data/ann-apples.json --namespace wikipedia-10m \
     --arm nprobe64=.scratch/scale/target/release/blowfish-scale-server \
     --arm nprobe160=.scratch/scale/target/release/blowfish-scale-server:BF_ANN_NPROBE=160

   # Cold 1M MS MARCO BM25 (n = 100)
   uv run --with numpy python bench/scale/ab.py --kind bm25 --queries 100 \
     --workload bench/scale/data/bm25-msmarco.json --namespace msmarco-1m \
     --arm blowfish=.scratch/scale/target/release/blowfish-scale-server
   ```

   For warm concurrency-1 measurements, start a read-only server on `127.0.0.1:8300` and run `warm.py` (uses a keep-alive HTTP connection after unmeasured warm-up passes):

   ```bash
   python3 bench/scale/mkconfig.py bench/scale/config.local .scratch/scale/ro.json .scratch/scale/cache \
     --memory-gib 8 --read-only
   BF_ANN_NPROBE=160 bench/scale/serve.sh .scratch/scale/ro.json > .scratch/scale/ro.log 2>&1 &

   # Warm 10M vector (2 warm-up passes, then 1,000 queries; also reports recall@10)
   uv run bench/scale/warm.py bench/scale/data/ann-apples.json http://127.0.0.1:8300 wikipedia-10m \
     --queries 1000 --warmup-passes 2

   # Warm 1M MS MARCO BM25 (1 warm-up pass, then 1,000 queries)
   uv run bench/scale/warm.py bench/scale/data/bm25-msmarco.json http://127.0.0.1:8300 msmarco-1m \
     --queries 1000 --warmup-passes 1

   systemctl --user stop scale-server.scope
   ```

   For the 1M Wikipedia multi-workload matrix (`ann,bm25,hybrid,selective`), run:

   ```bash
   uv run --with numpy python bench/scale/matrix.py --cold-queries 8 --kinds ann,bm25,hybrid,selective
   ```

6. **Optional: measure recall and parity against a live turbopuffer namespace (`parity.sh`):**

   If you have loaded the same Wikipedia rows into a turbopuffer test namespace (`tpuf_namespace` in `bench/scale/config.local`), run:

   ```bash
   ORACLE_DATA=oracle-1m.npz TPUF_URL=https://<region>.turbopuffer.com \
     bench/scale/parity.sh .scratch/scale/target/release/blowfish-scale-server \
     ann,bm25,hybrid,selective,broad .scratch/scale/out/parity-1m.json
   ```

7. **Clean up:** delete your test prefix in the object-storage bucket (and any paid turbopuffer test namespace), then remove `.scratch/scale`.
