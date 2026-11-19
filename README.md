# blowfish

[![CI](https://github.com/dorkitude/blowfish/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dorkitude/blowfish/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/dorkitude/blowfish)](https://github.com/dorkitude/blowfish/releases/latest)
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

## Why blowfish (and when to use turbopuffer instead)

Use blowfish to:

- develop, test, and run CI against the turbopuffer API without a turbopuffer account or network access;
- self-host modest workloads, or keep search indexes inside your own cloud or on-premises bucket.

For production at scale, we recommend [turbopuffer](https://turbopuffer.com) itself. Because the APIs match, moving from blowfish to turbopuffer is a base-URL and API-key change.

blowfish is an independent open-source project and is not affiliated with or endorsed by turbopuffer.

## Install

### Docker

Run on a local volume for development (port `8000`, auth off):

```bash
git clone https://github.com/dorkitude/blowfish.git
cd blowfish
docker compose up --build --wait blowfish
```

To test against a local S3-compatible store, run the `s3` profile, which starts SeaweedFS and a `blowfish-s3` container on port `8001`:

```bash
docker compose --profile s3 up --build --wait blowfish-s3
curl --fail http://127.0.0.1:8001/_blowfish/v1/status
```

### Cargo (development)

With a stable Rust toolchain installed:

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

1. Set `TURBOPUFFER_BASE_URL` to your blowfish address (for example, `http://127.0.0.1:8000`).
2. Set `TURBOPUFFER_API_KEY` to your blowfish `bf_...` key, or any nonempty string when server auth is disabled (the SDKs reject an empty key client-side).
3. **Unset `TURBOPUFFER_REGION`.** Official SDKs validate that a region is only set when the base URL contains a `{region}` template placeholder, and throw an error before sending any request if both a plain `TURBOPUFFER_BASE_URL` and `TURBOPUFFER_REGION` are present.

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

The compatibility suite in [`compat/`](compat/README.md) runs **121 recorded scenarios** through the official turbopuffer Python SDK against blowfish and diffs the responses against fixtures recorded from real turbopuffer. All 121 pass in CI in both normal mode and forced-indexing mode. A TypeScript SDK smoke test (`compat/smoke-ts`) exercises writes, ANN, BM25, metadata, and async copy polling.

| Area | Status | Details |
| --- | --- | --- |
| Writes | Supported | Upsert by rows and by columns, patch, delete, delete or patch by filter, conditional writes, and `$ref_new` |
| Vectors | Supported | Dense `f32`/`f16`, `int8`, and sparse vectors |
| Search | Supported | ANN, exact kNN, BM25 (single, multi-field, and prefix), hybrid via `multi_query` and RRF, and score expressions (`Sum`, `Max`, `Product`, `Decay`, `Saturate`, and distance) |
| Filters | Supported | Comparisons, array operators, `Glob`, `IGlob`, `Regex`, fuzzy matching, token filters, and `null` |
| Results | Supported | Highlighting, aggregations and group-by, `include_attributes` and `exclude_attributes`, attribute ordering, `offset`, `limit`, and paginated export |
| Namespaces | Supported | Copy (sync and async), branch, metadata, schema, list, delete, cache warming, recall, explain, and legacy v1 endpoints |
| Consistency | Supported | Strong and eventual consistency |
| Native embedding and reranking | Supported (configurable) | `embed` schema, `["Embed", ...]`, and model `rerank_by` via the `[models]` registry ([`docs/models.md`](docs/models.md)); returns `400` when unconfigured |
| Cross-deployment copy | Unsupported | Returns a clear `400` error |
| Customer-managed encryption keys | Unsupported | Returns a clear `400` error |
| Multi-vector ANN | Unsupported | turbopuffer gates this per organization; blowfish returns the same `400` error |
| Keys, billing, regions, and pinning | Different | API keys and projects are blowfish's own ([`docs/auth.md`](docs/auth.md)); `billing` fields are placeholders and `performance` fields differ; there are no managed regions; metadata pinning is advisory |

Full inventory and edge cases: [`docs/compatibility.md`](docs/compatibility.md).

## Embedding and reranking

Operators map model names (such as `google/embeddinggemma-2` or turbopuffer's model identifiers) to backends in `[models]` inside `blowfish.toml` or via `BLOWFISH_MODELS_JSON`:

- `local`: runs a GGUF model in-process on CPU via `llama.cpp` (requires a build with `--features local-models`, included in the Docker image on x86-64);
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
- **Host operations, TLS, metrics, and backups:** [`docs/operations.md`](docs/operations.md) covers running under `systemd` behind Caddy, graceful shutdown, and consistent prefix backups. Prometheus metrics (`GET /metrics` or `--metrics-listen`) are documented in [`docs/metrics.md`](docs/metrics.md), and threat model and request limits in [`docs/security.md`](docs/security.md).

## How it works

Writes are group-committed to a write-ahead log in object storage using create-only conditional puts (`If-None-Match: *`) and epoch fencing, and acknowledged only after the WAL slot is durable in the bucket. In the background, an indexer compacts WAL entries into immutable segments containing columnar documents, an IVF vector index with RaBitQ 1-bit quantized codes plus full-precision reranking vectors, chunked BM25 posting lists, and filter indexes. A namespace-wide cluster directory lets cold ANN queries run in about four sequential object-store round trips regardless of segment count. Query nodes keep only local memory and disk caches, so read replicas (`--read-only`) hold no authoritative state; each namespace has a single active writer, and a startup probe verifies conditional-write support on the bucket. Full architecture: [`DESIGN.md`](DESIGN.md).

## Benchmarks

Client-side `p50 / p90` latency in milliseconds (strong consistency on queries), compared against turbopuffer's [published numbers](https://turbopuffer.com/docs/performance).

### In-region (AWS S3)

Measured on `c7gd.2xlarge` in `us-west-2` reading a same-region S3 bucket (`v0.1.12`, fresh server process and empty caches per cold query):

| Benchmark | turbopuffer published | blowfish in-region |
| --- | --- | --- |
| Vector, 10M, cold (48 probes, recall@10 0.953 vs turbopuffer's 0.943) | 874 / 1,214 ms | 662 / 991 ms |
| Vector, 10M, warm (48 probes) | 14 / 17 ms | 70.7 / 84.1 ms |
| BM25, 1M, cold | 316 / 381 ms | 453 / 569 ms |
| BM25, 1M, warm | 13 / 18 ms | 17.1 / 34.5 ms |

In-region, blowfish's cold vector search at 10M is faster than turbopuffer's published p50 and p90 at higher recall. Cold BM25 is about 43% / 49% slower and is being worked on. Warm vector is still about 5x slower.

- **Network floor:** a 4 KB `GET` to S3 is 24.5 ms p50.
- **Cache-cold variant:** with the process warm and caches dropped before each query (turbopuffer's definition of cold), vector is **574 / 652 ms** and BM25 is **420 / 512 ms**.
- **Instance size:** `c7gd.xlarge` (4 vCPU, 8 GB) holds these numbers.
- **100M:** on the same `c7gd.2xlarge`, 100M rows (1,024-dim `f16`, 160 slabs) at 64 probes and recall@10 0.954 take **1,201 / 1,391 ms** cache-cold, **1,243 / 1,370 ms** with a fresh process, and **99.7 / 162.8 ms** warm. A cold query reads about 160 MB. turbopuffer publishes no 100M number.
- **Vectors and layout:** vectors are 1,024-dim `f16` (while turbopuffer's benchmark uses 768-dim), served from the namespace-wide ANN layout built by `recluster` (32 slabs, about 200 rows per cluster; unfiltered ANN only, writes after `recluster` fall back to the per-segment path until the next manual `recluster`; a 10M load and `recluster` peak at about 2.3 GB RSS, so they fit a 4 GB `c7gd.large`).
- **BM25:** 1M MS MARCO passages (335 MB of text); the warm BM25 row was measured on round 11 code.
- **Upserts:** not measured in-region; see the over-the-internet table below.

### Over the internet (Cloudflare R2)

Measured from an 8-vCPU VM in Los Angeles reaching Cloudflare R2 over the public internet (fresh server process and empty caches per cold query):

| Benchmark | turbopuffer published | blowfish over the internet (R2) |
| --- | --- | --- |
| Vector, 10M, cold (48 probes, recall@10 0.953 vs turbopuffer's 0.943) | 874 / 1,214 ms | 1,111 / 1,311 ms |
| Vector, 10M, warm (160 probes, per-segment layout) | 14 / 17 ms | 136 / 168 ms |
| BM25, 1M, cold | 316 / 381 ms | 1,570 / 1,885 ms |
| BM25, 1M, warm | 13 / 18 ms | 77 / 108 ms |
| Upsert, 512 KB batch | 165 / 248 ms | 792 / 1,531 ms |

- **Network floor:** a warm 4 KB `GET` to R2 was about 50 ms p50 in the cold-vector session, and about 84 ms p50 (`PUT` about 260 to 300 ms p50) in the earlier session covering BM25, warm queries, and upserts. Warm latency over the internet is mostly one strong-consistency round trip to the bucket.
- **Cold vector cost:** a cold 48-probe query makes about 400 requests and reads about 51 MB (down from about 880 requests, 117 MB, and 2,916 / 4,358 ms on the per-segment layout at 160 probes).
- **Upsert variance:** each batch is 68 rows (about 506 KB) over 300 sequential writes; a second identical R2 session measured **2,114 / 2,563 ms**, so treat R2 upsert p50 as roughly **792 to 2,114 ms**.

Methodology, probe sweeps, network RTT tables, 1M breakdown, parity numbers, known gaps, and reproduction scripts: [`docs/benchmarks/README.md`](docs/benchmarks/README.md).

## Documentation

- **Getting started and configuration**
  - [`docs/quickstart.md`](docs/quickstart.md) — Docker, Cargo, Python SDK, and TypeScript SDK setup
  - [`docs/configuration.md`](docs/configuration.md) — CLI flags, environment variables, and TOML settings (template: [`blowfish.example.toml`](blowfish.example.toml))
  - [`docs/compatibility.md`](docs/compatibility.md) — Supported features, differences, and unsupported endpoints (test suite: [`compat/README.md`](compat/README.md))
  - [`docs/models.md`](docs/models.md) — Native embedding and model reranking (`local`, `openai_embeddings`, and `rerank`)
- **Auth, security, and operations**
  - [`docs/auth.md`](docs/auth.md) — Org, projects, API keys, legacy roles, and `/_blowfish/v1/` usage
  - [`docs/api/admin.openapi.yaml`](docs/api/admin.openapi.yaml) — OpenAPI 3.1 specification for the admin API
  - [`webapp/README.md`](webapp/README.md) — Optional Go admin web console with magic-link sign-in
  - [`docs/operations.md`](docs/operations.md) — `systemd` and Caddy deployment, storage modes, read replicas, and backups
  - [`docs/metrics.md`](docs/metrics.md) — Prometheus metrics and scrape configuration
  - [`docs/security.md`](docs/security.md) — Threat model, route authorization matrix, input bounds, and durability guarantees
- **Benchmarks, architecture, and research**
  - [`docs/benchmarks/README.md`](docs/benchmarks/README.md) — Benchmark summary, methodology, parity tables, known gaps, and reproduction guide
  - [`docs/benchmarks.md`](docs/benchmarks.md) — Detailed engineering log across scale iterations 0–12
  - [`DESIGN.md`](DESIGN.md) — Overall architecture, object layout, WAL group commit, indexing, and query execution
  - [`docs/design/ann-directory.md`](docs/design/ann-directory.md) — Namespace-wide ANN cluster directory design
  - [`docs/design/projects-and-keys.md`](docs/design/projects-and-keys.md) — Multi-project isolation and key model design
  - [`docs/research/turbopuffer-api.md`](docs/research/turbopuffer-api.md) — Upstream turbopuffer HTTP API contract notes
  - [`docs/research/turbopuffer-sdks.md`](docs/research/turbopuffer-sdks.md) — Official SDK wire protocol notes
  - [`docs/research/architecture.md`](docs/research/architecture.md) — Object-storage search architecture notes and prior art
  - [`docs/research/oracle-probes.md`](docs/research/oracle-probes.md) — Sanitized HTTP wire traces from oracle probes
  - [`docs/open-source-checklist.md`](docs/open-source-checklist.md) — Pre-release open-source readiness checklist

## Contributing

Bug reports, compatibility fixes, and focused pull requests are welcome. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for build prerequisites, the test and compatibility suite commands, workspace layout, and commit conventions. To report a security vulnerability privately, see [`SECURITY.md`](SECURITY.md).

## License

MIT. See [`LICENSE`](LICENSE).
