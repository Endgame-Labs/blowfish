# blowfish

[![CI](https://github.com/endgame-labs/blowfish/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/endgame-labs/blowfish/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/endgame-labs/blowfish)](https://github.com/endgame-labs/blowfish/releases/latest)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust 2024](https://img.shields.io/badge/rust-2024_edition-orange.svg?logo=rust)](https://www.rust-lang.org)
[![turbopuffer API compatible](https://img.shields.io/badge/turbopuffer_API-compatible-6f42c1.svg)](compat/README.md)

blowfish is a self-hostable search engine that implements the [turbopuffer](https://turbopuffer.com) HTTP API in a single Rust binary. Point it at any S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, or SeaweedFS) or a local directory; the bucket is the only source of truth, with no separate database and no dedicated WAL disk. The official turbopuffer SDKs work against it unchanged by setting the base URL and API key.

> **Status: Pre-1.0.** The storage format may still change between releases, so do not store data you cannot re-ingest. Hardening (admission control, soak testing, and failure testing) is ongoing.

## Quickstart

Start blowfish locally with Docker (stores data in a Docker volume on `127.0.0.1:8000`, with auth disabled for local development):

```bash
docker compose up --build --wait blowfish
curl --fail http://127.0.0.1:8000/
```

Point the official Python SDK at blowfish, **unset `TURBOPUFFER_REGION`**, write a document, and run an approximate nearest-neighbor (ANN) query:

```bash
export TURBOPUFFER_BASE_URL=http://127.0.0.1:8000
unset TURBOPUFFER_REGION
export TURBOPUFFER_API_KEY=local-development

uv run --with turbopuffer python - <<'PY'
from turbopuffer import Turbopuffer

client = Turbopuffer()
ns = client.namespace("hello-fish")
ns.write(
    distance_metric="cosine_distance",
    upsert_rows=[{"id": 1, "vector": [1.0, 0.0], "message": "hello fish"}],
)
print(ns.query(rank_by=("vector", "ANN", [1.0, 0.0]), top_k=1, include_attributes=True))
PY
```

## Benchmarks

Client-side `p50 / p90` latency in milliseconds (strong consistency on queries), compared against turbopuffer's [published numbers](https://turbopuffer.com/docs/performance).

### In-region (AWS S3)

Measured on `c7gd.2xlarge` in `us-west-2` reading a same-region S3 bucket, with the `v0.1.14` release (2026-11-24). Cold numbers are shown both for a fresh server process with empty caches and for a warm process with caches dropped before each query (turbopuffer's definition of cold):

| Benchmark | turbopuffer published | blowfish in-region |
| --- | --- | --- |
| Vector, 10M, cold (48 probes, recall@10 0.953 vs. turbopuffer's 0.943) | 874 / 1,214 ms | 626 / 924 ms fresh (454 / 534 ms cache-cold) |
| Vector, 10M, warm (48 probes) | 14 / 17 ms | 18.2 / 25.7 ms |
| BM25, 1M, cold (`overlap@10` 1.000 vs. exact BM25) | 316 / 381 ms | 249 / 449 ms fresh (207 / 261 ms cache-cold) |
| BM25, 1M, warm | 13 / 18 ms | 11.2 / 14.2 ms |

- **100M vector (in-region, `c7gd.2xlarge`):** with `v0.1.14` in a 10 GiB cgroup, 64 probes measure **1,149 / 1,342 ms** cache-cold and **1,252 / 1,429 ms** fresh, at the same recall@10 (`0.954`); warm at 48 probes is **44.8 / 56.9 ms** with an 8 GiB cache in a 12 GiB cgroup. Earlier (`v0.1.13`-era) measurements: 100M rows of 1,024-dim `f16` Wikipedia embeddings (160 slabs, paged slab directory, and compact manifest copy) at 64 probes and recall@10 `0.954` take **1,201 / 1,391 ms** cache-cold, **1,243 / 1,370 ms** with a fresh process, and **99.7 / 162.8 ms** warm (**72.6 / 90.4 ms** warm at 48 probes, recall@10 `0.941–0.950`). Each cold query reads about 160 MB. Loading 100M rows took 5.7 h at **4,891 docs/s**; `recluster` took 1.7 h. turbopuffer publishes no 100M number.
- **Memory and instance sizing:** a 4 GB `c7gd.large` (swap off) holds a 10M load plus `recluster` at **2.3–2.9 GB** peak RSS; on `c7gd.large`, cold 10M vector search is **769 / 973 ms** fresh and **623 / 740 ms** cache-cold. On `c7gd.xlarge` (4 vCPU, 8 GB), 10M ingest stays flat at about **4,500 docs/s**.
- **Network floor:** a 4 KB `GET` to S3 is 25–26 ms p50.
- **10M at 16 clients:** 216 queries/s at 48 probes (72 / 87 ms).
- **On a 4 GB `c7gd.large` with `v0.1.14`:** cold 10M vector search is **670 / 833 ms** fresh and **536 / 637 ms** cache-cold at 48 probes.
- **Vectors and layout:** vectors are 1,024-dim `f16` Cohere Wikipedia embeddings (while turbopuffer's published benchmark uses 768-dim), served from the namespace-wide ANN slab layout built by `recluster` (about 200 rows per cluster; unfiltered ANN only). The server builds it on its own in the background, and rows written after it are searched beside it. Across runs, 10M recall@10 at 48 probes is `0.953–0.956`.
- **BM25:** 1M MS MARCO passages (335 MB of text), `overlap@10` `1.000` against exact BM25.
- **Single-batch upserts:** not measured in-region; see the over-the-internet table below for 512 KB batch latency.

### Over the internet (Cloudflare R2)

Measured from an 8-vCPU VM in Los Angeles reaching Cloudflare R2 over the public internet (fresh server process and empty caches per cold query):

| Benchmark | turbopuffer published | blowfish over the internet (R2) |
| --- | --- | --- |
| Vector, 10M, cold (48 probes, recall@10 0.953 vs. turbopuffer's 0.943) | 874 / 1,214 ms | 1,111 / 1,311 ms |
| Vector, 10M, warm (160 probes, per-segment layout) | 14 / 17 ms | 136 / 168 ms |
| BM25, 1M, cold | 316 / 381 ms | 1,570 / 1,885 ms |
| BM25, 1M, warm | 13 / 18 ms | 77 / 108 ms |
| Upsert, 512 KB batch | 165 / 248 ms | 792 / 1,531 ms |

- **Network floor:** a warm 4 KB `GET` to R2 was about 50 ms p50 in the cold-vector session, and about 84 ms p50 (`PUT` about 260 to 300 ms p50) in the earlier session covering BM25, warm queries, and upserts. Warm latency over the internet is mostly one strong-consistency round trip to the bucket.
- **Cold vector cost:** a cold 48-probe query makes about 400 requests and reads about 51 MB (down from about 880 requests, 117 MB, and 2,916 / 4,358 ms on the per-segment layout at 160 probes).
- **Upsert variance:** each batch is 68 rows (about 506 KB) over 300 sequential writes; a second identical R2 session measured **2,114 / 2,563 ms**, so treat R2 upsert p50 as roughly **792 to 2,114 ms**.

I also measured exact BM25 locally on 1M MS MARCO passages, restricted to two CPUs on a shared VM.
The new scorer uses **73% fewer server instructions** at one client and **74% fewer** at 16 clients,
with `overlap@10` **1.000**. Across two A/B repeats, warm p50/p90 ranges are **9.9–12.7 / 19.1–24.4 ms**;
16-client throughput is **91–109 queries/s**, against **47–53** before. Peak server RSS rises about
58 MiB. A fresh-process local S3 facade with 25 ms per request measures a median **251/366 ms**, against
**258/372 ms** before. This is a local round-trip model, and I have not confirmed this revision in-region.
On a 10-core Apple M4, the same store and queries measure warm p50/p90 **1.47/3.00 ms** and
**520 queries/s** at 16 clients, so the two shared VM CPUs, not the scorer, set the VM's p90.
Details and reproduction: [BM25 measurements](docs/benchmarks.md#bm25-round-2-exact-maxscore-and-hybrid-concurrency)
and [BM25 round 3](docs/benchmarks.md#bm25-round-3-fewer-posting-probes-and-a-mac-baseline).

### One machine: blowfish, Qdrant, and LanceDB (BEIR NQ)

On a 10-core Apple M4 Mac mini, I loaded the same 2.68M BEIR NQ passages (1,024-dim `f16`, cosine) into
each engine, one engine at a time, on local disk. blowfish v0.1.15 ran with defaults and reclustered by itself.
Latency is one client; throughput is 16 clients.

| Engine, setting | Recall@10 | p50 / p99 | q/s at 16 clients | Peak RSS: build / queries |
| --- | ---: | --- | ---: | ---: |
| blowfish v0.1.15, 64 probes (default) | 0.961 | 1.4 / 2.0 ms | 1,814 | 2.7 / 2.7–3.3 GB |
| blowfish v0.1.15, 48 probes | 0.951 | 1.2 / 1.8 ms | 2,101 | 2.7 / 2.7–3.3 GB |
| Qdrant 1.20.0, HNSW `ef=64` | 0.950 | 0.5 / 0.7 ms | 7,752 | 14.7 / 5.9 GB |
| LanceDB 0.41.0, IVF-PQ, 120 probes | 0.949 | 18.7 / 19.9 ms | 278 | 8.8 / 0.6 GB |

Qdrant keeps its HNSW graph and vectors in RAM and is about 3.7 times faster at the same recall. blowfish
scans one-bit codes and reads exact vectors through its object-storage cache, so the same layout serves
cold from S3. Details: [Apple M4 at v0.1.15](docs/benchmarks.md#apple-m4-at-v0115-blowfish-qdrant-and-lancedb).

#### Under a memory limit

To test whether Qdrant's lead depends on RAM, I ran the same 2.68M BEIR NQ workload (1,024-dim `f16`,
16 clients) on the same Mac inside a Linux VM with a cgroup cap on RAM plus page cache, swap off, and an
uncached VM disk. Engines ran one at a time, with two fresh processes per row:

| Engine | Recall@10 | 2 GiB | 4 GiB | 8 GiB | 12 GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| blowfish v0.1.15, 48 probes | 0.950 | 710 q/s | 1,799 | 1,805 | 1,795 |
| Qdrant 1.20.0, `ef=64`, vectors on disk | 0.949 | 83 | 224 | 5,616 | 5,702 |
| Qdrant 1.20.0, `ef=64`, vectors and HNSW on disk | 0.945 | 96 | 712 | 5,752 | 5,707 |

The ranking flips with available RAM: with 2 or 4 GiB, blowfish is 2.5 to 8.5 times faster; with 8 GiB
or more, Qdrant is about 3.1 times faster. At 2 GiB, p50 latency is 3.1–3.2 ms for blowfish vs.
22.7–26.9 ms for Qdrant; at 4 GiB, it is 1.8 ms vs. 4.0–5.6 ms. blowfish uses about 3.5 GB RSS and does
not get faster beyond about 4 GiB (natively on macOS, 1/3/7/11 GiB of block cache gave
1,159/2,129/2,145/2,138 q/s). Qdrant needs roughly 5 GB to hold its graph and vectors; below that, it
reads from disk on each hop. Details:
[Matched memory limits](docs/benchmarks.md#memory-budgets-blowfish-and-qdrant-on-an-apple-m4).

### My own measurements of hosted services (10M)

I also measured turbopuffer and Amazon S3 Vectors on the same 10M 1,024-dim `f16` Wikipedia dataset. Because caches cannot be forced cold externally on either hosted service, their idle-gap latency is not directly comparable to blowfish's cold numbers:

- **turbopuffer (server-reported):** queries labelled `"cold"` after 5 minutes idle measured about **670 / 1,120 ms** (`p50 / p90`); a single deep-cold sample after 2 days idle measured **2,051 ms**.
- **Amazon S3 Vectors:** recall@10 **0.894**, warm **60 / 278 ms**, and **178 / 276 ms** after 30 minutes idle.

Methodology, probe sweeps, network RTT tables, 1M breakdown, parity numbers, known gaps, and reproduction scripts: [`docs/benchmarks/README.md`](docs/benchmarks/README.md).

## Why blowfish (and when to use turbopuffer instead)

Use blowfish to:

- develop, test, and run CI against the turbopuffer API without a turbopuffer account or network access;
- self-host modest workloads, or keep search indexes inside your own cloud or on-premises bucket.

For production at scale, I recommend [turbopuffer](https://turbopuffer.com) itself. Because the APIs match, moving from blowfish to turbopuffer is a base-URL and API-key change.

blowfish is an independent open-source project and is not affiliated with or endorsed by turbopuffer.

## Install

### Docker

Run on a local volume for development (port `8000`, auth off):

```bash
git clone https://github.com/endgame-labs/blowfish.git
cd blowfish
docker compose up --build --wait blowfish
```

The image builds on x86_64 and arm64 (including Apple Silicon); CI builds and smoke-tests both.

To test against a local S3-compatible store, run the `s3` profile, which starts SeaweedFS and a `blowfish-s3` container on port `8001`:

```bash
docker compose --profile s3 up --build --wait blowfish-s3
curl --fail http://127.0.0.1:8001/_blowfish/v1/status
```

### Cargo (development)

With a stable Rust toolchain installed, on Linux or macOS (CI tests both, including Apple Silicon):

```bash
cargo run -p blowfish -- serve --storage-url file:///tmp/blowfish --listen 127.0.0.1:8000
```

### Binary from source

Build a release binary:

```bash
cargo build --release -p blowfish
./target/release/blowfish serve --storage-url file:///tmp/blowfish --listen 127.0.0.1:8000
```

To compile in-process `llama.cpp` GGUF support for local embedding and reranking (requires `cmake`, `clang`, and `libclang`):

```bash
# Portable x86-64 build with AVX2 kernels:
CFLAGS="-march=x86-64-v3" CXXFLAGS="-march=x86-64-v3" \
  cargo build --release -p blowfish --features local-models

# Or tuned for the build machine's own CPU:
cargo build --release -p blowfish --features local-models-native
```

For S3-compatible storage, pass `--storage-url s3://bucket/prefix` along with `--storage-endpoint`, `--storage-region`, and standard AWS credentials (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`), or copy [`blowfish.example.toml`](blowfish.example.toml) and run `blowfish serve --config blowfish.toml`. See [`docs/configuration.md`](docs/configuration.md).

## Using the SDKs

All official turbopuffer SDKs speak the same wire protocol. Configure three environment variables (or equivalent client constructor arguments):

1. Set `TURBOPUFFER_BASE_URL` to your blowfish address (for example, `http://127.0.0.1:8000`). Routes are absolute, so serving blowfish under a path prefix requires a reverse proxy that strips the prefix.
2. Set `TURBOPUFFER_API_KEY` to your blowfish `bf_...` key, or any nonempty string when server auth is disabled (the SDKs reject an empty key client-side).
3. **Unset `TURBOPUFFER_REGION`.** Official SDKs validate that a region is only set when the base URL contains a `{region}` template placeholder, and throw an error before sending any request if both a plain `TURBOPUFFER_BASE_URL` and `TURBOPUFFER_REGION` are present.

On `429` backpressure and `503` transient storage errors, blowfish returns `Retry-After`, `retry-after-ms` (estimated from the indexer's drain rate for WAL backpressure), and `x-should-retry` headers so the official SDKs retry with appropriate backoff.

### Python: BM25 and hybrid search

```python
from turbopuffer import Turbopuffer

# Ensure TURBOPUFFER_REGION is unset in the environment.
client = Turbopuffer(
    base_url="http://127.0.0.1:8000",
    api_key="local-development",
)
ns = client.namespace("articles")

ns.write(
    distance_metric="cosine_distance",
    schema={"text": {"type": "string", "full_text_search": True}},
    upsert_rows=[
        {"id": 1, "vector": [0.9, 0.1], "text": "The Danube flows through Central and Southeastern Europe."},
        {"id": 2, "vector": [0.1, 0.9], "text": "Object storage keeps immutable index segments durable."},
    ],
)

# Full-text BM25 search
bm25 = ns.query(
    rank_by=("text", "BM25", "danube europe"),
    top_k=5,
    include_attributes=["text"],
)

# Hybrid search: BM25 + vector ANN fused with Reciprocal Rank Fusion (RRF)
hybrid = ns.multi_query(
    queries=[
        {"rank_by": ("text", "BM25", "danube europe"), "top_k": 10, "include_attributes": ["text"]},
        {"rank_by": ("vector", "ANN", [0.9, 0.1]), "top_k": 10, "include_attributes": ["text"]},
    ],
    rerank_by=["RRF"],
    limit=5,
)
print(hybrid.results[0].rows)
```

### TypeScript

```bash
npm install @turbopuffer/turbopuffer
export TURBOPUFFER_BASE_URL=http://127.0.0.1:8000
unset TURBOPUFFER_REGION
export TURBOPUFFER_API_KEY=local-development
```

```typescript
import Turbopuffer from '@turbopuffer/turbopuffer';

const client = new Turbopuffer();
const ns = client.namespace('hello-fish');

await ns.write({
  distance_metric: 'cosine_distance',
  schema: { text: { type: 'string', full_text_search: true } },
  upsert_rows: [
    { id: 1, vector: [1, 0], text: 'quick brown fox' },
    { id: 2, vector: [0, 1], text: 'slow dog' },
  ],
});

const res = await ns.query({
  rank_by: ['vector', 'ANN', [1, 0]],
  top_k: 2,
  include_attributes: true,
});
console.log(res.rows);
```

## Features and compatibility

The compatibility suite in [`compat/`](compat/README.md) runs **121 recorded scenarios** through the official turbopuffer Python SDK against blowfish and diffs the responses against fixtures recorded from real turbopuffer, plus Python (sync and async) and TypeScript SDK retry scenarios against a low-limit backpressure server. All 121 recorded scenarios and the retry checks pass in CI in both normal mode and forced-indexing mode. A TypeScript SDK smoke test (`compat/smoke-ts`) exercises writes, ANN, BM25, metadata, backpressure retries, and async copy polling.

| Area | Status | Details |
| --- | --- | --- |
| Writes | Supported | Upsert by rows and by columns, patch, delete, delete or patch by filter, conditional writes, and `$ref_new` |
| Vectors | Supported | Dense `f32`/`f16`, `int8`, sparse vectors, and multi-vector (`[][N]f32`, `late_interaction: true`, scanned over document columns) |
| Search | Supported | ANN, exact kNN, BM25 (single, multi-field, and prefix), hybrid via `multi_query` and RRF, and score expressions (`Sum`, `Max`, `Product`, `Decay`, `Saturate`, and distance) |
| Filters | Supported | Comparisons, array operators, `Glob`, `IGlob`, `Regex`, fuzzy matching, token filters, and `null` |
| Results | Supported | Highlighting, aggregations and group-by, `include_attributes` and `exclude_attributes`, attribute ordering, `offset`, `limit`, and paginated export |
| Namespaces | Supported | Copy (sync and async), branch, metadata, schema, list, delete, cache warming, recall, explain, and legacy v1 endpoints |
| Consistency | Supported | Strong and eventual consistency |
| Retry and backpressure | Supported | `Retry-After` and `retry-after-ms` on `429` and `503`; `x-should-retry` by status ([`docs/research/sdk-compat-audit.md`](docs/research/sdk-compat-audit.md)) |
| Native embedding and reranking | Supported (configurable) | `embed` schema, `["Embed", ...]`, and model `rerank_by` via the `[models]` registry ([`docs/models.md`](docs/models.md)); returns `400` when unconfigured |
| Cross-deployment copy | Unsupported | Returns a clear `400` error |
| Customer-managed encryption keys | Unsupported | Returns a clear `400` error |
| Keys, billing, regions, and pinning | Different | API keys and projects are blowfish's own ([`docs/auth.md`](docs/auth.md)); `billing` fields are placeholders and `performance` fields differ; there are no managed regions; metadata pinning is advisory |

Full inventory and edge cases: [`docs/compatibility.md`](docs/compatibility.md).

## Embedding and reranking

Operators map model names (such as `google/embeddinggemma-2` or turbopuffer's model identifiers) to backends in `[models]` inside `blowfish.toml` or via `BLOWFISH_MODELS_JSON`:

- `local`: runs a GGUF model in-process on CPU via `llama.cpp` (requires a build with `--features local-models`, included in the Docker image, with AVX2 `x86-64-v3` flags on x86-64);
- `openai_embeddings`: calls any OpenAI-compatible `POST /v1/embeddings` endpoint (Ollama, vLLM, TEI, `llama-server`, Fireworks, or OpenAI);
- `rerank`: calls any Cohere/Jina-style `POST /v1/rerank` endpoint.

```toml
[models."google/embeddinggemma-2"]
provider = "local"
task = "embedding"
hf_repo = "ggml-org/embeddinggemma-2-GGUF"
hf_file = "embeddinggemma-2-Q8_0.gguf"
dims = 768
allowed_dims = [128, 256, 512]
max_input_tokens = 2048
query_prefix = "task: search result | query: "
document_prefix = "title: none | text: "

[models."qwen/qwen3-reranker-8b"]
provider = "rerank"
base_url = "https://api.fireworks.ai/inference"
model = "accounts/fireworks/models/qwen3-reranker-8b"
api_key_env = "FIREWORKS_API_KEY"
```

Measured on an 8-vCPU VM:

- **Local query embedding** (EmbeddingGemma 2, Q8_0): about **35 ms** median.
- **Local CPU reranking** (Qwen3-Reranker-0.6B, Q8_0): about **0.8 s per candidate** (~23.5 s for 30 candidates), which is too slow for interactive traffic.
- **Hosted GPU reranker** (Qwen3-Reranker-8B on Fireworks): about **0.25 s for 30 candidates**.

In practice, local CPU query embedding works well, while reranking (and large bulk backfills) should point at a local GPU server or a hosted API. Full configuration and limits: [`docs/models.md`](docs/models.md).

## Auth, projects, and keys

One blowfish deployment is one **org**:

- **Org superadmin keys** manage projects and every key in the deployment.
- **Projects** isolate namespaces under `<storage-prefix>/projects/<id>/` (another project's namespaces return `404`, never `403`). Each project issues three kinds of scoped keys:
  - `admin`: full namespace access in its project, plus creating, listing, and revoking `read_write` and `read` keys for that project;
  - `read_write`: writes, schema updates, queries, metadata, warming, recall, and explain in its project;
  - `read`: read-only queries, metadata, schema reads, warming, recall, and explain in its project.
- **`BLOWFISH_ADMIN_KEY`** is a break-glass secret supplied via the environment and never stored in the bucket. Set `--require-auth` (`require_auth = true`) outside local development so startup refuses to run without it. Use `BLOWFISH_ADMIN_KEY` once to mint an org superadmin key, then use minted keys for day-to-day administration.

Everything is managed through the JSON admin API under `/_blowfish/v1/` (`projects`, `projects/{id}/keys`, `org/keys`, `whoami`, and `status`), specified in [`docs/api/admin.openapi.yaml`](docs/api/admin.openapi.yaml), or through the optional Go web console in [`webapp/`](webapp/README.md) (magic-link email sign-in with an admin allowlist).

Mint a superadmin key, create a project, and mint a project `read_write` key:

```bash
export BF=http://127.0.0.1:8000

# 1. Mint an org superadmin key using the break-glass key (token is shown once)
curl -sS -X POST "$BF/_blowfish/v1/org/keys" \
  -H "Authorization: Bearer $BLOWFISH_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"ops"}'
export SUPER=bf_...

# 2. Create an isolated project
curl -sS -X POST "$BF/_blowfish/v1/projects" \
  -H "Authorization: Bearer $SUPER" \
  -H "Content-Type: application/json" \
  -d '{"id":"acme","name":"Acme Search"}'

# 3. Mint a read_write key for SDK clients in that project
curl -sS -X POST "$BF/_blowfish/v1/projects/acme/keys" \
  -H "Authorization: Bearer $SUPER" \
  -H "Content-Type: application/json" \
  -d '{"name":"acme-app","kind":"read_write"}'
```

Pass the returned `bf_...` project key as `TURBOPUFFER_API_KEY` in any official SDK; it automatically scopes requests to `acme`. Details: [`docs/auth.md`](docs/auth.md).

## Deploying

- **Object storage:** point `storage.url` at `s3://bucket/prefix` on AWS S3, Cloudflare R2, MinIO, or SeaweedFS, and configure `storage.endpoint`, `storage.region`, and standard AWS credentials or workload identity ([`docs/configuration.md`](docs/configuration.md), [`blowfish.example.toml`](blowfish.example.toml)). At startup, writable nodes probe `If-None-Match: *` conditional creates; backends that enforce them run in `multi_writer` mode, and backends that do not fall back to `single_writer` mode.
- **Read replicas and caching:** start additional stateless query nodes with `--read-only` (`storage.read_only = true`) pointed at the same bucket prefix, backed by memory and local NVMe caches (`storage.cache.disk_dir`). Route by namespace with consistent hashing at your load balancer for cache locality.
- **Host operations, TLS, metrics, and backups:** [`docs/operations.md`](docs/operations.md) covers running under `systemd` behind Caddy, memory sizing, large namespaces (the automatic recluster and `readercopy`), graceful shutdown, and consistent prefix backups. Prometheus metrics (`GET /metrics` or `--metrics-listen`) are documented in [`docs/metrics.md`](docs/metrics.md), and threat model and request limits in [`docs/security.md`](docs/security.md).

## How it works

Writes are group-committed to a write-ahead log in object storage using create-only conditional puts (`If-None-Match: *`) and epoch fencing, and acknowledged only after the WAL slot is durable in the bucket. In the background, an indexer compacts WAL entries into immutable segments containing columnar documents, an IVF vector index with RaBitQ 1-bit quantized codes plus full-precision reranking vectors, chunked BM25 posting lists, per-segment ID filters, and filter indexes. Memory-sized defaults and jemalloc with adaptive page decay keep ingest, builds, and `recluster` inside a 4 GB instance at 10M rows. A namespace-wide cluster directory—and, for large reclustered namespaces, a paged slab directory plus a compact manifest reader copy—lets cold ANN queries run in about four sequential object-store round trips regardless of segment count. Query nodes keep only local memory and disk caches, so read replicas (`--read-only`) hold no authoritative state; each namespace has a single active writer, and a startup probe verifies conditional-write support on the bucket. Full architecture: [`DESIGN.md`](DESIGN.md).

## Documentation

- **Getting started and configuration**
  - [`docs/quickstart.md`](docs/quickstart.md) — Docker, Cargo, Python SDK, and TypeScript SDK setup
  - [`docs/configuration.md`](docs/configuration.md) — CLI flags, environment variables, memory sizing, and TOML settings (template: [`blowfish.example.toml`](blowfish.example.toml))
  - [`docs/compatibility.md`](docs/compatibility.md) — Supported features, retry headers, differences, and unsupported endpoints (test suite: [`compat/README.md`](compat/README.md))
  - [`docs/models.md`](docs/models.md) — Native embedding and model reranking (`local`, `openai_embeddings`, and `rerank`)
- **Auth, security, and operations**
  - [`docs/auth.md`](docs/auth.md) — Org, projects, API keys, legacy roles, and `/_blowfish/v1/` usage
  - [`docs/api/admin.openapi.yaml`](docs/api/admin.openapi.yaml) — OpenAPI 3.1 specification for the admin API
  - [`webapp/README.md`](webapp/README.md) — Optional Go admin web console with magic-link sign-in
  - [`docs/operations.md`](docs/operations.md) — `systemd` and Caddy deployment, storage modes, read replicas, large-namespace tools, and backups
  - [`docs/metrics.md`](docs/metrics.md) — Prometheus metrics and scrape configuration
  - [`docs/security.md`](docs/security.md) — Threat model, route authorization matrix, input bounds, and durability guarantees
- **Benchmarks, architecture, and research**
  - [`docs/benchmarks/README.md`](docs/benchmarks/README.md) — Benchmark summary, 10M and 100M results, methodology, parity tables, known gaps, and reproduction guide
  - [`docs/benchmarks.md`](docs/benchmarks.md) — Detailed engineering log across scale iterations 0–18 and 100M runs
  - [`docs/cost-estimates.md`](docs/cost-estimates.md) — Estimated infrastructure and benchmark costs for blowfish (S3 and R2), turbopuffer, and Amazon S3 Vectors
  - [`DESIGN.md`](DESIGN.md) — Overall architecture, object layout, WAL group commit, indexing, and query execution
  - [`docs/design/ann-directory.md`](docs/design/ann-directory.md) — Namespace-wide ANN cluster directory, slabs, and paged directory design
  - [`docs/design/projects-and-keys.md`](docs/design/projects-and-keys.md) — Multi-project isolation and key model design
  - [`docs/research/sdk-compat-audit.md`](docs/research/sdk-compat-audit.md) — Official SDK compatibility and retry-header audit
  - [`docs/research/turbopuffer-api.md`](docs/research/turbopuffer-api.md) — Upstream turbopuffer HTTP API contract notes
  - [`docs/research/turbopuffer-sdks.md`](docs/research/turbopuffer-sdks.md) — Official SDK wire protocol notes
  - [`docs/research/architecture.md`](docs/research/architecture.md) — Object-storage search architecture notes and prior art
  - [`docs/research/oracle-probes.md`](docs/research/oracle-probes.md) — Sanitized HTTP wire traces from oracle probes

## Contributing

Bug reports, compatibility fixes, and focused pull requests are welcome. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for build prerequisites, the test and compatibility suite commands, workspace layout, and commit conventions. To report a security vulnerability privately, see [`SECURITY.md`](SECURITY.md).

## License

MIT. See [`LICENSE`](LICENSE).
