# Benchmarks

This page summarizes blowfish's measured query latency, recall, result-set parity with turbopuffer, and ingest throughput at 1M and 10M documents, along with the test methodology, caveats, known engineering gaps, and reproduction steps.

For the complete chronological engineering log across iterations 0 through 10 (including R2 latency-floor probes, storage byte inventories, cache tuning, and experiments that were rejected), see [`../benchmarks.md`](../benchmarks.md).

## Headline comparison

Comparison against turbopuffer's [published cold query latency](https://turbopuffer.com/docs/performance):

| Workload | turbopuffer published | blowfish measured |
| --- | --- | --- |
| Vector search, cold, p50 / p90 | 874 / 1,214 ms (10M docs, 768-dim, in-region) | 10M: 3,000 / 4,380 ms (1024-dim); 1M: 1,278 / 1,593 ms |
| BM25, 1M docs, cold, p50 / p90 | 316 / 381 ms | 869 / 1,594 ms |

blowfish is slower than turbopuffer on cold queries today. At 10M documents, cold vector search is about 3.4x slower at p50: a cold ANN query issues about 1,115 object-store requests and reads about 69 MB across 70 segments. Reducing cold request count and bytes transferred is the primary open performance gap.

## Methodology

### Dataset and schema

- **Source dataset:** [`CohereLabs/wikipedia-2023-11-embed-multilingual-v3`](https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3) (English subset, revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`), streamed from 100,000-row Parquet shards (1M = first 1M passages; 10M = first 10M passages, 45.0 GB raw uncompressed source).
- **Vectors:** 1,024-dimensional embeddings stored as `[1024]f16` with `cosine_distance`. Every source element in the dataset is verified to be losslessly representable in IEEE-754 `f16` before upload, so no source precision is discarded.
- **Attributes:** `text` (`string`, `full_text_search: true`), `title` (`string`, stored unindexed), `title_initial` (lowercased first character of `title`, filterable), and `text_length` (Unicode character count of `text`, filterable).
- **Held-out queries and exact ground truth:** 200 passages evenly spaced across the first 100,000 source rows (`0, 500, 1000, ..., 99500`) are excluded from ingestion. Their embeddings serve as ANN query vectors, and their article titles serve as BM25 queries. During ingestion, the loader computes exact cosine top-10 ground truth over every ingested row (`bench/scale/data/oracle-1m.npz` and `bench/scale/data/oracle-10m.npz`), plus separate exact top-10 sets for the two filtered workloads.

### Query types

Every query requests `top_k = 10` (or `limit = 10` for `multi_query`) with **strong consistency**:

- **Vector (ANN):** `["vector", "ANN", query_vector]`
- **BM25:** `["text", "BM25", query_title]`
- **Hybrid (BM25 + vector, RRF):** `multi_query` combining the ANN and BM25 subqueries (`top_k = 10` each), fused with Reciprocal Rank Fusion (`rerank_by: ["RRF"]`, `limit: 10`)
- **Selective-filter vector:** ANN with `["title_initial", "Eq", "z"]` (matches ~0.4% of the corpus: 4,181 rows at 1M and ~42,000 rows at 10M)
- **Broad-filter vector (parity runs):** ANN with `["text_length", "Gte", 300]`

### Cold vs. warm definitions

- **Cold:** each measured cold query runs at concurrency 1 against a **freshly started, read-only server process** with a **newly created, empty local disk cache**. After startup, the harness waits briefly (1.5 s) for the HTTP connection warmer to establish idle TLS connections to R2 (matching an already-running node serving a cold namespace), then times one query. Every manifest, cluster directory, posting-list chunk, quantized code block, and candidate document must be fetched over the network from object storage.
- **Warm:** the server process has already completed an unmeasured warm-up pass over the query set, so active index metadata and data blocks reside in the local memory and NVMe caches. Because consistency is `strong`, every warm query still performs one `HEAD` request against object storage to confirm no newer WAL slot has been published.

## Environment and network caveats

- **Compute host:** 8 x86-64 vCPUs (AMD EPYC 9354P, Zen 4), 32 GiB RAM, local NVMe. The benchmark server runs inside a `systemd` scope with `MemoryHigh=14G`, `MemoryMax=16G`, 6 Rayon worker threads, and an 8 GiB memory block cache (`4,000`-byte blocks).
- **Object storage over the public internet:** blowfish reaches **Cloudflare R2 Standard** over the internet rather than colocated in-region storage. Direct floor measurements (`crates/bf-storage/examples/r2_floor.rs`) from this VM show:
  - a single sequential 4–64 KB `GET` takes **72 ms at p50**, **98–110 ms at p90**, and **210–309 ms at p99**;
  - a parallel wave of `N` ranged `GET`s waits on the slowest request in the wave (~174 ms at `N = 32`, ~291–374 ms at `N = 256`, and ~553–983 ms at `N = 1,024` on warm connections; hedged reads cap the worst tail outliers at <=10% extra requests);
  - aggregate upload bandwidth from this VM to R2 tops out at **13.9 MB/s** (~14 MB/s) across 4–16 parallel streams, and download throughput tops out around 60–180 MB/s;
  - R2 round-trip latency from this VM shifts by 40–100% between sessions an hour apart, so code comparisons use interleaved query-by-query A/B runs in the same session (`bench/scale/ab.py`).
- **Like-for-like differences vs. turbopuffer:** turbopuffer's published cold benchmarks run in-region next to object storage on 768-dimensional vectors, whereas blowfish's measurements above cross the internet to R2 on 1,024-dimensional `f16` vectors. Live turbopuffer namespaces cannot be forced cold externally, so live head-to-head runs (`bench/scale/oracle_compare.py`) measure warm/hot turbopuffer caches and are used to evaluate recall and result-set parity rather than cold latency. A stricter in-region comparison on matched hardware and dimensions is planned.

## Query latency

### 1M documents (9 segments, strong consistency)

Cold measurements from same-session interleaved runs (`n = 24` queries per type, `n = 16` for selective):

| Query type | Cold p50 / p90 / p99 (ms) | Cold R2 requests / query | Cold MB / query | Sequential waves |
| --- | ---: | ---: | ---: | ---: |
| Vector (ANN) | 1,278 / 1,593 / 2,503 | 339 | 22.0 | 4.1 |
| BM25 | 869 / 1,594 / 1,994 | 310 | 9.2 | 3.2 |
| Hybrid (BM25 + vector, RRF) | 1,188 / 1,752 / 2,138 | 635 | 29.8 | 2.3 |
| Selective-filter vector (`title_initial == "z"`) | 1,973 / 2,600 / 2,731 | 1,904 | 41.9 | 4.1 |

With a 1,024-connection warm pool (instead of the initial 256-connection pool), selective-filter cold latency at 1M drops to **1,429 / 1,940 / 2,390 ms**.

Warm strong-consistency queries at 1M take **about 50–60 ms at p50** (concurrency 1) from this VM, of which roughly 55 ms is the single strong-consistency `HEAD` check against R2.

### 10M documents (70 segments, strong consistency)

Cold measurements from Iteration 10 (`n = 12` for ANN, `n = 8` for BM25, hybrid, and selective), alongside warm single-concurrency client latency across all 200 held-out queries after one warm-up pass:

| Query type | Cold p50 / p90 / p99 (ms) | Cold R2 requests / query | Cold MB / query | Sequential waves | Warm p50 / p90 / p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Vector (ANN, default `nprobe=64`) | 2,999 / 4,379 / 4,612 | 1,115 | 68.9 | 4.6 | 104 / 147 / 204 |
| BM25 | 4,384 / 5,816 / 6,448 | 1,196 | 61.5 | 3.4 | 128 / 266 / 483 |
| Hybrid (BM25 + vector, RRF) | 4,158 / 4,769 / 5,647 | 2,241 | 119.9 | 2.9 | 153 / 265 / 377 |
| Selective-filter vector (`title_initial == "z"`) | 6,564 / 8,707 / 10,725 | 2,603 | 268.0 | 4.2 | 201 / 232 / 316 |

With `BF_ANN_NPROBE=160` (needed at 10M to match turbopuffer's recall; see below), warm p50 / p90 is **164 / 197 ms** for ANN and **446 / 518 ms** for selective-filter ANN; cold latency at `nprobe = 160` has not yet been measured and will exceed the 64-probe cold numbers above.

## Recall and turbopuffer parity

Result-set parity (`overlap@10`, fraction of shared IDs in the top 10) and exact `recall@10` against ground truth are measured over all **200 held-out queries per query type**, comparing blowfish against a live turbopuffer namespace loaded with the identical documents and schema (`bench/scale/oracle_compare.py`). BM25 and hybrid RRF have no independent exact ground truth, so only parity with turbopuffer is reported for them.

### 1M documents

| Query type | `overlap@10` with turbopuffer | Recall@10 (blowfish) | Recall@10 (turbopuffer) |
| --- | ---: | ---: | ---: |
| Vector (ANN) | 0.935 | 0.952 | 0.938 |
| BM25 | 0.990 | — | — |
| Hybrid (BM25 + vector, RRF) | 0.971 | — | — |
| Selective-filter vector | 0.998 | 1.000 | 0.998 |
| Broad-filter vector (`text_length >= 300`) | 0.963 | 0.974 | 0.977 |

### 10M documents

| Query type | `overlap@10` with turbopuffer (`nprobe=64`) | Recall@10 (blowfish, `nprobe=64`) | Recall@10 (blowfish, `BF_ANN_NPROBE=160`) | Recall@10 (turbopuffer) |
| --- | ---: | ---: | ---: | ---: |
| Vector (ANN) | 0.887 | 0.886 | 0.947 | 0.943 |
| BM25 | 0.985 | — | — | — |
| Hybrid (BM25 + vector, RRF) | 0.935 | — | — | — |
| Selective-filter vector | 0.962 | 0.968 | 0.992 | 0.993 |

At 10M documents, the default `nprobe = 64` drops below turbopuffer's recall (0.886 vs. 0.943 on ANN, and 0.968 vs. 0.993 on selective-filter ANN). Setting `BF_ANN_NPROBE=160` restores recall parity (0.947 and 0.992).

## Ingest and storage footprint

- **Ingest rate:** about **2,000 docs/s** on current pipelined WAL code (`BF_WAL_PIPELINE=8`, 2,000 rows per batch, 16 concurrent writes in flight: 2,040 docs/s measured end-to-end on 200K docs, ~1,860 docs/s sustained on 500K, and ~1,710 docs/s over the first 1M rows of the 10M load). Throughput on this machine is capped by its **~14 MB/s** aggregate upload link to R2: each document uploads ~3.0 KB of compressed WAL payload plus ~5.6 KB of segment files, while the server's commit worker is ~20% utilized.
- **10M memory behavior:** during the full 10M load inside a 16 GiB `systemd` scope (`MemoryHigh=14G`), resident memory rose by roughly 1.3 GB per million rows and spiked to 14–16 GB during multi-segment compactions past ~7M rows, requiring server restarts and capping segment size (`BF_MAX_SEGMENT_ROWS=100000`) for the final 2M rows.
- **Retained storage size:** after compaction and garbage collection, the 10M namespace occupies **~5.6 KB per document** (~56.3 GB across 10M rows: ~2.5 KB/doc in columnar `.bfc` document files, ~2.2 KB/doc in `.bfv` ANN index files, and ~0.9 KB/doc across `.bft` full-text, `.bfx` filter, and `.bfi` ID indexes).

## Known gaps

- **Selective filters issue ~1,900 object-store requests per cold query at 1M** (fetching ~4,181 individual 2 KB vectors on the exact-fetch path when matching rows <= 8,192), and scan **~268 MB across ~2,600 requests at 10M** (where ~42,000 matching rows exceed the exact-fetch cap and fall back to widened RaBitQ code scans without a per-cluster allowed-row index).
- **The cold vector layout is not contiguous yet:** cluster metadata blobs, RaBitQ 1-bit codes, and full-precision rerank vectors live in separate regions across clusters that are not ordered by spatial proximity, leaving cold ANN at ~339 requests / 22 MB at 1M and ~1,115 requests / 69 MB at 10M.
- **Vectors are stored twice per segment:** once in the columnar document store (`.bfc`) for attribute projection and compaction, and once in the ANN index file (`.bfv`) for full-precision reranking, nearly doubling segment upload bytes and bucket footprint.
- **Ingest and compaction memory growth above ~5M rows:** compaction currently materializes bounded segment inputs in memory rather than streaming cluster-by-cluster, causing 13–15 GB RSS spikes near 7–10M rows on a 16 GiB budget.
- **Default probe scaling at 10M:** default `nprobe = 64` under-probes at 10M (0.886 recall@10); raising `BF_ANN_NPROBE=160` recovers 0.947 recall@10 at the cost of reading more clusters per query.

## How to reproduce

The scale harness lives in [`bench/scale/`](../../bench/scale/). Private bucket endpoints, profiles, and namespace names stay in `bench/scale/config.local`, which is git-ignored; all build outputs, caches, downloaded shards, and logs stay under `.scratch/scale/` (`SCALE_SCRATCH`).

1. **Create `bench/scale/config.local` from [`bench/scale/config.example.json`](../../bench/scale/config.example.json):**

   ```bash
   cp bench/scale/config.example.json bench/scale/config.local
   ```

   Edit `bench/scale/config.local` to point `storage_url` (`s3://...`), `endpoint`, `region`, and `profile` at an isolated test prefix in your bucket (or supply `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the environment).

2. **Build the standalone benchmark server (`crates/bf-engine/examples/scale-server`):**

   ```bash
   bench/scale/build.sh
   ```

   This compiles `blowfish-scale-server` in release mode into `.scratch/scale/target/release/`, wrapping the unmodified `blowfish::router` with `EngineConfig::from_env()` tuning knobs.

3. **Start the server and load 1M (or 10M) Wikipedia passages:**

   ```bash
   mkdir -p .scratch/scale/cache
   python3 bench/scale/mkconfig.py bench/scale/config.local .scratch/scale/storage.json .scratch/scale/cache

   BF_TRACE_STAGES=1 bench/scale/serve.sh .scratch/scale/storage.json > .scratch/scale/server.log 2>&1 &
   bench/scale/load.sh 1000000 --inflight 16
   bench/scale/wait_idle.sh .scratch/scale/server.log
   ```

   `load.sh` streams Parquet shards from Hugging Face into `.scratch/scale/load/`, deletes each shard after upload, and writes exact cosine ground truth to `.scratch/scale/load/truth.npz` (precomputed truth for the first 100K, 1M, and 10M rows is also checked into `bench/scale/data/`). Stop the writable server once `wait_idle.sh` returns (`systemctl --user stop scale-server.scope`).

4. **Measure cold and warm query latency:**

   ```bash
   # Warm (concurrency 1, 8, and 32) and cold (concurrency 1, fresh process + empty cache) matrix
   uv run --with numpy python bench/scale/matrix.py --cold-queries 8 --kinds ann,bm25,hybrid,selective

   # Interleaved same-session cold A/B across binaries or environment variables
   uv run --with numpy python bench/scale/ab.py --kind ann --queries 24 \
     --arm default=.scratch/scale/target/release/blowfish-scale-server \
     --arm nprobe160=.scratch/scale/target/release/blowfish-scale-server:BF_ANN_NPROBE=160
   ```

5. **Optional: measure recall and parity against a live turbopuffer namespace:**

   If you have loaded the same rows into a turbopuffer test namespace (`tpuf_namespace` in `bench/scale/config.local`), run:

   ```bash
   ORACLE_DATA=oracle-1m.npz TPUF_URL=https://gcp-us-central1.turbopuffer.com \
     bench/scale/parity.sh .scratch/scale/target/release/blowfish-scale-server \
     ann,bm25,hybrid,selective,broad .scratch/scale/out/parity-1m.json
   ```

6. **Clean up:** delete your test prefix in the object-storage bucket (and any paid turbopuffer test namespace), then remove `.scratch/scale`.
