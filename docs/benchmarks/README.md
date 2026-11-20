# Benchmarks

This page summarizes blowfish's measured query and ingest performance against turbopuffer's published numbers, along with 100M in-region results, my own measurements of turbopuffer and Amazon S3 Vectors, the network round-trip profile, 1M multi-workload breakdown, recall and result-set parity, known engineering gaps, and reproduction commands.

For the complete chronological engineering log across iterations 0 through 18, the 100M runs, the apples-to-apples R2 runs, and the AWS in-region runs, see [`../benchmarks.md`](../benchmarks.md).

## In-region on AWS S3 (current `main`, beyond `v0.1.12`)

Measured on `c7gd.2xlarge` (8 vCPU, 16 GB) in `us-west-2` reading a same-region S3 bucket on current `main` (which adds memory-sized defaults, flat ingest, paged slab directories, compact manifest reader copies, parallel slab scans, and BM25 posting pruning on top of `v0.1.12`). Vector benchmarks use 1,024-dim `f16` Cohere Wikipedia embeddings in the namespace-wide `recluster` slab layout; BM25 uses 1M MS MARCO passages (335 MB of text). Client-side `p50 / p90` latency in milliseconds with strong consistency on queries, compared against turbopuffer's [published benchmarks](https://turbopuffer.com/docs/performance):

| Benchmark | turbopuffer published | blowfish in-region |
| --- | --- | --- |
| Vector, 10M, cold (48 probes, recall@10 0.953 vs. turbopuffer's 0.943) | 874 / 1,214 ms | 680 / 907 ms fresh (533 / 656 ms cache-cold) |
| Vector, 10M, warm (48 probes) | 14 / 17 ms | 36.6 / 50.8 ms |
| BM25, 1M, cold (`overlap@10` 1.000 vs. exact BM25) | 316 / 381 ms | 380 / 526 ms fresh (301 / 369 ms cache-cold) |
| BM25, 1M, warm | 13 / 18 ms | 17.1 / 34.5 ms |

- **10M vector across runs:** on `c7gd.2xlarge` at 48 probes (recall@10 `0.953–0.956`), cache-cold runs range from `533–600 / 650–750 ms` (latest run `533 / 656 ms`), fresh-process runs range from `680–690 / 907–966 ms` (latest run `680 / 907 ms`), and warm latency dropped from `70.7 / 84.1 ms` (`v0.1.12`) to `36.6 / 50.8 ms` after spawning per-slab scan tasks in parallel.
- **1M BM25:** cold cache-cold latency (`301 / 369 ms`) now matches turbopuffer's published `316 / 381 ms`, while fresh-process latency is `380 / 526 ms` (down from `453 / 569 ms` in `v0.1.12`). Warm BM25 (`17.1 / 34.5 ms`) was measured earlier on `c7gd.2xlarge` and has not been remeasured since.
- **Single-batch upserts:** not measured in-region; see the over-the-internet R2 section below for 512 KB batch latency, and [Ingest and storage footprint](#ingest-and-storage-footprint) for in-region bulk ingest throughput (`~4,500 docs/s` at 10M on `c7gd.xlarge` and `4,891 docs/s` at 100M on `c7gd.2xlarge`).

### 100M vector in-region (`c7gd.2xlarge`)

100M rows of 1,024-dim `f16` Cohere Wikipedia embeddings across five languages (English, German, French, Spanish, and Russian), 160 spatial slabs, a paged slab directory (written by `recluster` or `examples/repage.rs`), and a compact manifest reader copy (375 KB instead of 3.4 MB, written on publish or by `examples/readercopy.rs`). Strong consistency, `c7gd.2xlarge` in the same region as S3; turbopuffer publishes no 100M number:

| Setting | Cache-cold p50 / p90 (ms) | Fresh process p50 / p90 (ms) | Warm p50 / p90 (ms) | Recall@10 |
| --- | ---: | ---: | ---: | ---: |
| 64 probes (latest run) | 1,201 / 1,391 | 1,243 / 1,370 | 99.7 / 162.8 | 0.954 |
| 48 probes (latest run) | 1,128 / 1,341 | 1,058 / 1,253 | 72.6 / 90.4 | 0.941–0.950 |

- **Range across recent 100M runs (64 probes):** cache-cold `1,164–1,201 / 1,348–1,391 ms`; fresh process `1,228–1,243 / 1,370–1,411 ms`.
- **Cold I/O, load, and recluster:** each cold 64-probe query reads about `160 MB` (down from `326.8 MB` before the paged slab directory). Loading 100M rows took **5.7 h** (`20,445 s`, **4,891 docs/s** average); `recluster` took **1.7 h** (`6,248 s`, 160 slabs, 499,920 clusters).
- **Over the internet (pre-paged-directory code, not remeasured):** measured before the paged slab directory and parallel slab scans from an 8-vCPU VM outside AWS at 48 probes (`294 MB` per cold query), fresh process was `6,399 / 8,929 ms` and warm was `448 / 513 ms`.

### My own measurements of hosted services (10M)

I also loaded the same 10M 1,024-dim `f16` Wikipedia dataset into live turbopuffer and Amazon S3 Vectors indexes to measure recall and idle-gap latency (`bench/scale/tpuf_cold.py` and `bench/s3vectors/measure.py`). Because caches cannot be forced cold externally on either hosted service, their idle-gap latencies are not directly comparable to blowfish's fresh-process or cache-cold measurements:

| Service (10M, 1,024-dim `f16`) | Idle / "cold" p50 / p90 (ms) | Warm p50 / p90 (ms) | Recall@10 | Caveat |
| --- | ---: | ---: | ---: | --- |
| turbopuffer (server-reported) | ~670 / 1,120 (5 min idle) | 88 / 203 (first-pass warm) | 0.943 | Caches cannot be dropped; one deep-cold sample after 2 days idle measured 2,051 ms (100M load in progress) |
| Amazon S3 Vectors (client-side, in-region) | 178 / 276 (30 min idle) | 60 / 278 | 0.894 | Caches cannot be dropped; lower recall@10 than blowfish (0.953–0.956) and turbopuffer (0.943) |

### 10M vector: probe sweep and layout comparison (`v0.1.12` baseline)

Cold `p50 / p90` in milliseconds on `c7gd.2xlarge` across the 32-slab `recluster` layout (`BF_ANN_NPROBE` = 40, 48, and 64) and the 70-segment per-segment baseline (`BF_ANN_SLABS=0`, 160 probes), from the `v0.1.12` in-region sweep (warm slab latency has since dropped to `36.6 / 50.8 ms` at 48 probes and cache-cold to `533 / 656 ms`):

| Arm | Fresh p50 / p90 (ms) | Gap vs. published | Cache-cold p50 / p90 (ms) | Gap vs. published | Recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| turbopuffer published (768-dim) | 874 / 1,214 | — | — | — | 0.943 |
| Slabs, 40 probes | 642 / 806 | -27% / -34% | 558 / 699 | -36% / -42% | 0.9415 |
| Slabs, 48 probes | 662 / 991 | -24% / -18% | 574 / 652 | -34% / -46% | 0.953 |
| Slabs, 64 probes | 773 / 969 | -12% / -20% | 630 / 746 | -28% / -39% | 0.9615 |
| Per-segment, 160 probes (slabs off) | 1,079 / 1,336 | +23% / +10% | 924 / 1,071 | +6% / -12% | 0.9465 |

Tail latency, object-store I/O per cold query, and warm latency (`1,000` queries at concurrency 1, `v0.1.12` baseline):

| Arm | Fresh p99 (ms) | Cache-cold p99 (ms) | Requests | MB | Waves | Warm p50 / p90 / p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Slabs, 40 probes | 1,078 | 1,038 | 357 | 45.6 | 3.4 | 67.5 / 80.9 / 96.0 |
| Slabs, 48 probes | 1,454 | 816 | 404 | 50.7 | 3.5 | 70.7 / 84.1 / 99.0 |
| Slabs, 64 probes | 1,080 | 872 | 495 | 60.7 | 3.6 | 84.0 / 101.9 / 117.8 |
| Per-segment, 160 probes (slabs off) | 1,543 | 2,371 | 844 | 115.2 | 3.2 | 96.9 / 106.2 / 112.3 |

- **Vectors and sampling:** vectors are 1,024-dim `f16` (while turbopuffer's published benchmark uses 768-dim). "Fresh" starts a new server process with an empty cache per query (50 queries per arm, rotated in the same session). "Cache-cold" runs against a warm server process with caches dropped before each query via `POST /_bench/drop_caches` (turbopuffer's definition of cold). `Recall@10` is measured over all 200 held-out queries from a warm 1,000-query run.
- **Per-stage breakdown (one cache-cold 48-probe slab query, `v0.1.12`):** 477 ms in-process (`308 ms` snapshot total and `169 ms` execution total) and 660 ms of CPU summed across threads. Wave 0 read the two ANN directory hints (`15.3 MB`, `100 ms`), wave 1 read the WAL tail (`2` requests, `23 ms`), wave 2 prefetched scan fragments (`361` requests, `36.8 MB`, `181 ms`), and wave 3 read rerank vectors plus two dependent document hops (`67` requests, `123 ms`), with `34 ms` of scan CPU and `162 ms` of in-process rerank and hydration. Current `main` drops lower-priority directory hints once the active layout answers and spawns per-slab scan tasks in parallel.
- **Limits of the `recluster` layout today:** it covers unfiltered ANN only; writes made after the last `recluster` fall back to the per-segment path until the next one; and `recluster` is a manual step. Building the 10M layout previously peaked at 16.9 GB RSS in `v0.1.12`, but with packed `f16` builds and memory-sized defaults it now peaks at **1.76–2.36 GB RSS** and fits on a 4 GB `c7gd.large` with swap off.

### 1M BM25 (MS MARCO)

Measured on `c7gd.2xlarge` in `us-west-2` with window-pruned posting reads, segment format v4, manifest text headers, and fixed-key `manifest-latest`:

| Mode | turbopuffer published (p50 / p90) | blowfish in-region (p50 / p90) | `overlap@10` vs. exact BM25 |
| --- | ---: | ---: | ---: |
| Cold, fresh process | 316 / 381 ms | 380 / 526 ms | 1.000 |
| Cold, cache-cold (`POST /_bench/drop_caches`) | 316 / 381 ms | 301 / 369 ms | 1.000 |
| Warm (earlier measurement) | 13 / 18 ms | 17.1 / 34.5 ms | 1.000 |

- **Cold I/O:** each cold BM25 query makes `194` requests and reads `4.4 MB` (down from `17.1 MB` in `v0.1.12`), across about `3.4–3.5` waves.

### Network floor and instance sizing (`us-west-2`)

Instance-to-S3 round-trip floor in `us-west-2`:

| Measurement | p50 | p90 |
| --- | ---: | ---: |
| TCP connect | 1.6 ms | 2.2 ms |
| TLS handshake | 26.9 ms | 27.7 ms |
| 4 KB `GET` | 24.5 ms | 30.5 ms |
| 1 MB `GET` | 30.2 ms | 36.1 ms |

Smallest-instance 10M results (memory-sized defaults, jemalloc, packed `f16` builds, **swap off**):

- **`c7gd.large` (2 vCPU, 4 GB, swap off):** holds a full 10M ingest plus `recluster` at **2.3–2.9 GB** peak RSS (2.26–2.31 GB on a bare `c7gd.large`; 2.93–3.02 GB in a 4 GiB cgroup check). On `c7gd.large` at 48 probes (recall@10 `0.9545`), 10M vector search measures **769 / 973 ms** fresh, **623 / 740 ms** cache-cold, and **81.8 / 96.0 ms** warm.
- **`c7gd.xlarge` (4 vCPU, 8 GB):** holds flat **~4,500 docs/s** (`4,501 docs/s` average) across a full 10M ingest at **2.51 GB** peak RSS (`VmHWM`), with 48-probe cache-cold 10M vector search at **600 / 751 ms** (recall@10 `0.956`).
- **`c7gd.2xlarge` (8 vCPU, 16 GB):** ingests 100M rows at **4,891 docs/s** (5.7 h) and serves 10M vector at **680 / 907 ms** fresh, **533 / 656 ms** cache-cold, and **36.6 / 50.8 ms** warm.

Historical `v0.1.12` cross-size comparison (`p50 / p90` in ms, except warm `p50` and swap pages out):

| Instance size | Slab 40 fresh | Slab 48 cache-cold | Slab 64 fresh | BM25 cold fresh | Warm p50 (40) | Swap pages out |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `c7gd.2xlarge` (8 vCPU, 16 GB) | 642 / 806 | 574 / 652 | 773 / 969 | 453 / 569 | 67.5 | 0 |
| `c7gd.xlarge` (4 vCPU, 8 GB) | 566 / 730 | 650 / 845 | 673 / 976 | 501 / 682 | 71.3 | 0 |
| `c7gd.large` (2 vCPU, 4 GB) | 728 / 961 | 661 / 781 | 857 / 1,152 | 509 / 720 | 114 | 8 |

## Over the internet to Cloudflare R2 (version 0.1.12)

Iteration 12 added `recluster`: a manual compaction step that regroups every vector in a namespace into 32 spatial slabs of about 200-row clusters, instead of clustering each segment on its own.

Cold 10M vector search (1,024-dim `f16`, strong consistency, fresh server process with empty memory and disk caches for every query, over the internet from Los Angeles to R2; 50 queries per slab arm rotated query by query in one session where a warm 4 KB `GET` was 50.0 ms p50 and 64.8 ms p90):

| Layout and setting | Cold p50 / p90 / p99 (ms) | Requests | MB per query | Recall@10 (200 queries) |
| --- | ---: | ---: | ---: | ---: |
| Per-segment baseline, 160 probes (`n = 30`, prior session) | 2,916 / 4,358 / 23,116 | 880 | 116.5 | 0.9465 |
| Namespace-wide slabs, 40 probes (`BF_ANN_NPROBE=40`) | 1,075 / 1,345 / 3,043 | 357 | 45.6 | 0.9415 |
| Namespace-wide slabs, 48 probes (`BF_ANN_NPROBE=48`) | 1,111 / 1,311 / 1,584 | 404 | 50.7 | 0.953 |
| Namespace-wide slabs, 64 probes (`BF_ANN_NPROBE=64`) | 1,326 / 1,757 / 2,228 | 495 | 60.7 | 0.9615 |

- **Against turbopuffer over the internet:** turbopuffer publishes **874 / 1,214 ms** (p50 / p90) for cold 10M vector queries (768-dim, in-region) and measures **0.943** recall@10 on the same 10M dataset and 200 queries. At 48 probes (`0.953` recall@10), blowfish over the internet to R2 measures **1,111 / 1,311 ms** (about 1.27x at p50 and 1.08x at p90).
- **Tail note and warm reference:** with `n = 50`, p99 is the second-slowest query, so individual object-store stalls move p99 (such as 3,043 ms at 40 probes, and 23,116 ms at `n = 30` on the per-segment baseline). Warm concurrency-1 p50 on the same slab servers over all 200 queries is **76, 78, and 101 ms** at 40, 48, and 64 probes.

The R2 sections below predate iteration 12 (except where noted) and retain the BM25, upsert, network, 1M multi-workload, and parity measurements.

## Over the internet to Cloudflare R2 (iteration 10 layout)

Client-side `p50 / p90` latency in milliseconds over the internet to R2 (with strong consistency on every query), compared against turbopuffer's [published benchmarks](https://turbopuffer.com/docs/performance):

| Benchmark | turbopuffer published (p50 / p90) | blowfish measured on R2 (p50 / p90) | Ratio (p50 / p90) |
| --- | ---: | ---: | ---: |
| Vector, 10M docs, cold (`BF_ANN_NPROBE=160`) | 874 / 1,214 ms | 5,175 / 6,528 ms | 5.9x / 5.4x |
| Vector, 10M docs, warm (`BF_ANN_NPROBE=160`) | 14 / 17 ms | 136 / 168 ms | 9.7x / 9.9x |
| BM25, 1M docs, cold | 316 / 381 ms | 1,570 / 1,885 ms | 5.0x / 4.9x |
| BM25, 1M docs, warm | 13 / 18 ms | 77 / 108 ms | 5.9x / 6.0x |
| Upsert, 512 KB batch | 165 / 248 ms | 792 / 1,531 ms | 4.8x / 6.2x |

On the iteration 10 per-segment layout over the internet to R2, blowfish was about 5–6x slower than turbopuffer on cold queries and 6–10x slower on warm queries, at matching recall.

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

## Network and environment (R2 runs)

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

- **Cold, fresh process (`bench/scale/ab.py`):** each measured cold query runs at concurrency 1 against a **freshly started, read-only server process** with an **empty 8 GiB memory cache and a newly created, empty local disk cache**. After startup, the harness waits 1.5 s for the HTTP connection warmer to establish idle TLS connections to the bucket (matching an already-running node serving a cold namespace), then times one query. Every manifest, cluster directory, posting-list chunk, quantized code block, and candidate document is fetched over the network from object storage.
- **Cache-cold, process-warm (`bench/scale/ab.py --cachecold`):** one long-lived read-only server process keeps its S3/R2 connections warm while `POST /_bench/drop_caches` empties memory, disk, decoded-metadata, and namespace-handle caches before every measured query (turbopuffer's definition of cold).
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

- **In-region ingest rate (AWS S3, current `main`):** with per-segment ID filters, bulk-load compaction deferral, memory-sized defaults, jemalloc, and packed `f16` segment builds:
  - **10M on `c7gd.xlarge` (4 vCPU, 8 GB, server in a 3.5 GiB cgroup):** flat at about **4,500 docs/s** (`4,501 docs/s` average, `2,222 s` total, `4,540 docs/s` over 9M–10M), peak RSS `2.51 GB` (`VmHWM`).
  - **10M on `c7gd.large` (2 vCPU, 4 GB, swap off):** **2,918 docs/s** average (`3,427 s` total), peak RSS **2.3–2.9 GB** across runs (including a 4 GiB cgroup check with swap off), with `recluster` peaking at `1.76–2.36 GB`.
  - **100M on `c7gd.2xlarge` (8 vCPU, 16 GB):** **4,891 docs/s** average (`20,445 s`, **5.7 h** total), peak RSS `6.8 GB` (`7.1 GB` `VmHWM`), followed by a `1.7 h` (`6,248 s`) `recluster` into 160 slabs (`6.0 GB` peak RSS).
- **Over-the-internet R2 ingest (historical, iteration 10):** about **2,000 docs/s** (`BF_WAL_PIPELINE=8`, 2,000 rows per batch, 16 concurrent writes in flight: `2,040 docs/s` on 200K docs, `~1,860 docs/s` on 500K, and `~1,710 docs/s` over the first 1M of 10M), capped by the VM's `~14 MB/s` aggregate upload link to R2.
- **Retained storage size:** after compaction and garbage collection, the 10M Wikipedia namespace occupies **~5.6 KB per document** (`~56.3 GB` across 10M rows: `~2.5 KB/doc` in columnar `.bfc` document files, `~2.2 KB/doc` in `.bfv` ANN index files, and `~0.9 KB/doc` across `.bft` full-text, `.bfx` filter, and `.bfi` ID indexes).

## Known gaps

- **Selective filters issue ~1,900 object-store requests per cold query at 1M** (fetching ~4,181 individual 2 KB vectors on the exact-fetch path when matching rows <= 8,192), and scan **~268 MB across ~2,600 requests at 10M** (where ~42,000 matching rows exceed the exact-fetch cap and fall back to widened RaBitQ code scans without a per-cluster allowed-row index).
- **`recluster` is manual and covers unfiltered ANN only:** writes flushed into new segments after a `recluster` fall back to the per-segment layout until the next manual `recluster`, and filtered ANN does not use slabs. On the per-segment layout (before `recluster`), default `nprobe = 64` under-probes at 10M (`0.886` recall@10; `BF_ANN_NPROBE=160` restores `0.947–0.952`).
- **Vectors are stored twice per segment:** once in the columnar document store (`.bfc`) for attribute projection and compaction, and once in the ANN index file (`.bfv`) for full-precision reranking, nearly doubling segment upload bytes and bucket footprint.
- **Full rewrites per publication at very large scale:** the text statistics object, ANN directories, and manifest are still rewritten in full on each publication, which adds linear per-cycle work past tens of millions of rows.

## How to reproduce

The scale and apples-to-apples harness lives in [`bench/scale/`](../../bench/scale/), the 100M workload scripts live in [`bench/scale/big/`](../../bench/scale/big/), and the Amazon S3 Vectors comparison scripts live in [`bench/s3vectors/`](../../bench/s3vectors/). Private bucket endpoints, profiles, and namespace names stay in `bench/scale/config.local`, which is git-ignored; all build outputs, caches, downloaded shards, and logs stay under `.scratch/scale/` (`SCALE_SCRATCH`).

For large reclustered namespaces, three helper examples live in `crates/bf-engine/examples/`:

- `cargo run --release -p bf-engine --example recluster -- storage.json NAMESPACE ATTRIBUTE SCRATCH_DIR` builds the namespace-wide spatial slab layout (and writes a paged slab directory automatically at 100,000+ clusters).
- `cargo run --release -p bf-engine --example repage -- storage.json NAMESPACE ATTRIBUTE` adds a paged slab directory to a previously reclustered namespace without rebuilding slabs.
- `cargo run --release -p bf-engine --example readercopy -- storage.json NAMESPACE` rewrites `manifest-latest` in compact form (omitting text headers) for a namespace published before compact copies existed.

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

   Stop the writable server before running read-only query benchmarks (`systemctl --user stop scale-server.scope`). Because a writable server indexes, compacts, and collects garbage in every namespace under its storage prefix, point writable loader servers at an isolated prefix holding only the namespace being loaded when preserving an existing benchmark namespace.

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
