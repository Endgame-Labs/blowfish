# Native embedding and reranking 🐡🧠

blowfish can embed text for you and rerank results with a model, the way turbopuffer does, without
tying you to one vendor. Models are **mapped by the operator** in a `[models]` table: a model name
(for example `google/embeddinggemma-2`, or one of turbopuffer's own names) points at a provider. Clients
use the standard turbopuffer syntax, and unmodified turbopuffer SDKs work.

Three provider types exist:

| `provider` | Runs | Use it for |
| --- | --- | --- |
| `local` | A GGUF model **inside the blowfish process** (llama.cpp), on CPU | The default: nothing else to run or pay for |
| `openai_embeddings` | Any OpenAI-compatible `POST {base_url}/v1/embeddings` | GPU servers and hosted APIs: llama.cpp `llama-server`, vLLM, TEI, Ollama, Fireworks, OpenRouter, OpenAI |
| `rerank` | A Cohere/Jina-style `POST {base_url}/v1/rerank` | GPU servers and hosted APIs: vLLM, TEI, Jina, Cohere, Voyage, llama.cpp `--reranking` |

With no `[models]` entries, `embed` and model `rerank_by` return a clear `400`, and everything else is
unchanged.

## Quick start: local models

Build with the `local-models` feature (it compiles llama.cpp, so it needs `cmake`, a C++ compiler, and
libclang; see [Building](#building)), then add to `blowfish.toml`:

```toml
[models."google/embeddinggemma-2"]
provider = "local"
task = "embedding"
hf_repo = "ggml-org/embeddinggemma-2-GGUF"
hf_file = "embeddinggemma-2-Q8_0.gguf"
dims = 768
allowed_dims = [128, 256, 512]          # Matryoshka sizes clients may ask for
max_input_tokens = 2048
query_prefix = "task: search result | query: "
document_prefix = "title: none | text: "

[models."qwen/qwen3-reranker-0.6b"]
provider = "local"
task = "rerank"
hf_repo = "ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF"
hf_file = "qwen3-reranker-0.6b-q8_0.gguf"
max_input_tokens = 2048
```

On first use blowfish downloads each GGUF from Hugging Face into `BLOWFISH_MODELS_CACHE_DIR` (default
`~/.cache/blowfish/models`; the container image uses `/data/models`), checks the size and SHA-256 the hub
reports (or `sha256` from the config), and loads it once. Set `HF_ENDPOINT` to use a mirror or proxy, and
`HF_TOKEN` for gated files. Use `path = "/models/x.gguf"` instead of `hf_repo` and `hf_file` for a file you
placed yourself. The first request to each model pays the download and load; later ones don't.

Both models are Apache-2.0 (checked on their Hugging Face cards): EmbeddingGemma 2 (`google/embeddinggemma-2`,
768 dimensions, Matryoshka 128/256/512/768, 8,192-token context, mean pooling, multilingual and code) and
Qwen3-Reranker-0.6B.

### Why those prefixes

EmbeddingGemma 2 is trained with task prefixes. Retrieval uses `task: search result | query: {query}` for
queries and `title: {title} | text: {content}` for documents, with `title: none` when there is no title.
blowfish adds `query_prefix` to `Embed` expressions and `document_prefix` to rows it embeds. Omitting them
still works but is less precise. Qwen3 embedding models want an instruction instead (for example
`query_prefix = "Instruct: Given a web search query, retrieve relevant passages\nQuery: "`) and no document
prefix.

### Local provider settings

| Key | Default | Meaning |
| --- | --- | --- |
| `task` | required | `"embedding"` or `"rerank"` |
| `hf_repo`, `hf_file` / `path` | required | Where the GGUF comes from |
| `sha256` | hub's value | Expected checksum of a downloaded file |
| `dims` | required for embedding | Native output size; the load fails if the model disagrees |
| `allowed_dims` | none | Other sizes a client may request. blowfish truncates and renormalizes (Matryoshka) |
| `max_input_tokens` | 8192 | Context for one input; longer inputs are truncated. Compute memory grows with it, so 2,048 is a good local choice |
| `batch_size` | 32 | Embedding inputs packed into one forward pass (rerank candidates run one at a time) |
| `threads` | half the cores | llama.cpp threads for this model |
| `workers` | 1 | Concurrent inference contexts, sharing `threads` |
| `queue` | 64 | Requests that may wait for a worker. When it is full, requests get **429** |
| `query_prefix`, `document_prefix` | empty | See above |

Inference runs on **its own dedicated threads**, separate from the HTTP, query, and indexing threads, so a
burst of embedding or reranking cannot starve them. The cost is those threads' CPU: size `threads` with the
rest of the host in mind.

### Measured on this host

Measured on the 8 vCPU AMD EPYC (Zen 4) VM that runs blowfish, **while another benchmark job was using
about half the CPU**, with the defaults above (4 threads), Q8_0 weights, and a build tuned for the host CPU
(`local-models-native`). Corpus: 1,000 English Wikipedia passages (326 characters, about 80 tokens each, from
the benchmark dataset), indexed through the real HTTP API with the turbopuffer Python SDK.

| Operation | Latency |
| --- | --- |
| Model load, first use (download excluded) | about 2 s (EmbeddingGemma), about 10 s (Qwen3 reranker, cold page cache) |
| Embed one query at write or query time (`["Embed", ...]`, about 13 tokens) | **median 35 ms** (range 29 to 58 ms) |
| Embed 100 passages in one write | 10 to 19 s, **about 150 ms per passage** (about 500 tokens per second) |
| Index 1,000 passages with `embed` (10 writes of 100) | 150 s in total |
| Rerank 30 BM25 candidates with Qwen3-Reranker-0.6B | **median 23.5 s**, about 0.8 s per candidate |
| BM25 + ANN `multi_query` (20 candidates each) reranked | 29.5 s |

Resident memory of the whole blowfish process: 17 MiB idle, 440 MiB after EmbeddingGemma loads, 713 MiB while
indexing, and 1.6 GiB with both models resident and 1,000 documents indexed. The GGUF files are 310 MB and 639
MB. (Weights are memory-mapped, so some of that is shared page cache.)

Sanity: querying with a passage's title returned a passage with that title in the top 5 for 25 of 25 sampled
titles. The reranker gave a river passage 0.98 and an unrelated one 0.00005 in a direct check.

What this means in practice: **query embedding is fast on CPU, bulk embedding is slow-ish (about 6 passages
a second), and CPU reranking is expensive.** Keep rerank candidates to 10 to 20, or point `rerank` at a GPU
or hosted provider. For a large backfill, run a GPU `openai_embeddings` provider for the load and keep the
local model for queries (the model name and dims must match).

## HTTP providers (GPU and hosted)

```toml
[models."qwen/qwen3-embedding-0p6b"]          # the name your clients send
provider = "openai_embeddings"
base_url = "http://gpu-box:8081"              # POST {base_url}/v1/embeddings
model = "Qwen3-Embedding-0.6B"                # the provider's own model ID (defaults to the name above)
api_key_env = "GPU_BOX_KEY"                   # name of an environment variable; the key never goes in the file
dims = 1024
allowed_dims = [256, 512, 768]
send_dimensions = false                       # true: ask the provider for the size instead of truncating here
max_input_tokens = 8192
batch_size = 32
concurrency = 4                               # concurrent provider requests
retries = 3                                   # on 429, 5xx, and connection errors
timeout_ms = 60000
query_prefix = "Instruct: Given a web search query, retrieve relevant passages\nQuery: "

[models."voyage/rerank-3"]
provider = "rerank"
base_url = "https://api.voyageai.com"         # POST {base_url}/v1/rerank
model = "rerank-2.5"
api_key_env = "VOYAGE_API_KEY"
```

- `endpoint_path` overrides the path (for example `/rerank` for a bare TEI).
- Rerank providers get `{model, query, documents, top_n}` and must answer `results[{index, relevance_score}]`.
- `BLOWFISH_MODELS_JSON` holds a JSON object with the same shape as the table and is merged over the file, for
  container deployments.
- **llama.cpp servers:** `llama-server -hf ggml-org/embeddinggemma-2-GGUF --embedding` for embeddings and
  `llama-server -hf ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF --reranking` for reranking, then point `base_url`
  at each.
- **Fireworks, OpenRouter, OpenAI:** use their `/v1` base URL as `base_url` (for example
  `https://api.fireworks.ai/inference`), with `api_key_env`.

Retries back off exponentially and honor `Retry-After`. After the retries, a provider `429` is returned to the
client as `429`, a provider `5xx` as `502`, an unreachable provider as `503`, a timeout as `504`, and a
provider `4xx` as `502` (blowfish's configuration is wrong, not the client's request). Provider error bodies
are logged, never returned.

## What clients write

Everything is the turbopuffer API; see [compatibility](compatibility.md) for the exact differences.

```python
ns.write(
    schema={"text": {"type": "string", "embed": "google/embeddinggemma-2", "full_text_search": True}},
    upsert_rows=[{"id": 1, "text": "The Danube is a river in Central Europe"}],
)
ns.query(rank_by=["text", "ANN", ["Embed", "a famous river"]], top_k=10)
ns.query(rank_by=["embed_text", "ANN", ["Embed", "a famous river", {"model": "google/embeddinggemma-2"}]], top_k=10)

# Model reranking: single query, or multi_query with the same object at the root
ns.query(
    rank_by=["text", "BM25", "danube"], limit=20, include_attributes=["text"],
    rerank_by={"model": "qwen/qwen3-reranker-0.6b", "attribute": "text", "query": "danube"},
)
```

**Schema.** `embed` is a model name or `{"model", "attribute"?, "dims"?, "dtype"?}`; `null` removes it.

- The embedding goes into the vector attribute named by `attribute`. Without it, blowfish creates a vector
  column named **`embed_<attribute>`** (`embed_text` for `text`; this is our reading of turbopuffer's
  docs) of type `[dims]f32` (`dtype: "f16"` for half precision). With no `distance_metric`, a new
  namespace uses cosine.
- `embed` can be added or removed at any time, but not modified. Changing it does not re-embed existing rows.
- At most four embedded attributes per namespace, and generated columns count toward the eight-vector limit.

**Writes.** A row with the source attribute set and its target vector absent is embedded at write time, even
inside `upsert_columns`.

- A supplied vector is stored as sent (checked for dimensions and type only) and skips the provider.
- Rows are embedded **before** `upsert_condition` is evaluated, so the tokens are spent even if the write is
  skipped. Supply the vector to avoid that.
- A write to a namespace with embedded attributes is limited to **256 upsert rows**.
- Inputs longer than `max_input_tokens` are truncated. Patches do not re-embed (vectors can't be patched).
- The response `performance` carries `embedding_tokens` and `embedding_ms`.

**Queries.** `["text", "ANN", ["Embed", "..."]]` uses the attribute's configured model; the explicit
`["vector", "ANN", ["Embed", "...", {"model": "..."}]]` form is for vector attributes (the model may be
omitted when an `embed` setting targets that vector). `kNN` works too. `performance` reports `embedding_tokens` and `embedding_ms`. Query embeddings sit in a small
in-memory LRU; document embeddings are never cached.

**Reranking.** `rerank_by: {"model", "attribute", "query"}`.

- The first-stage `limit` is the candidate count. `$dist` becomes the model's relevance score, sorted
  highest first.
- Multi-query: the same object at the root. Candidates are deduplicated by ID before scoring, then the root
  `limit` and `offset` apply to the reranked list.
- Rows with a missing or null attribute are not sent to the model and score zero.
- Unlike turbopuffer, the attribute need not be in `include_attributes`; it is fetched and dropped from the
  response unless you asked for it.
- Aggregations can't be reranked. `performance.rerank_ms` (a blowfish extension) reports the model call.

Qwen3-Reranker scores come from llama.cpp's classifier pooling over the model's own `rerank` prompt template,
which answers a yes/no relevance probability between 0 and 1. Scores are comparable for the same model and
query, not across models.

Unknown or unconfigured models, wrong-task models, and bad dimensions return `400` with the model name.

## Building

The `local-models` cargo feature compiles llama.cpp through the `llama-cpp-4` crate. It is off in a default
`cargo build` and `cargo test`, so those need nothing extra. With it, you need `cmake`, a C++ toolchain, and
libclang (`apt-get install cmake clang libclang-dev`).

```sh
# Slim build: HTTP providers only
cargo build --release -p blowfish
# In-process models, tuned for THIS machine's CPU (AVX-512 and so on). Use it for a host you build on and run on.
cargo build --release -p blowfish --features local-models-native
# In-process models for any x86-64 machine. The kernels need AVX2 to be fast, so also pass the flags below.
CFLAGS="-march=x86-64-v3" CXXFLAGS="-march=x86-64-v3" cargo build --release -p blowfish --features local-models
```

Without AVX2 flags llama.cpp falls back to plain SSE2 and runs about four to five times slower (32 passages: 28 s baseline, 7 s with `x86-64-v3`, 5 s with the host-tuned build, on the host above). The container image
is built with `local-models` and `x86-64-v3` flags (x86 only).

CI builds, lints, and tests the feature in its own job. The default job never compiles it.

## Local stack check

`scripts/compat_models.sh` runs the model compat scenarios against a fake provider (no weights). To run them
against real models, start blowfish with a `[models]` config, then:

```sh
cd compat && COMPAT_MODELS_EMBED=google/embeddinggemma-2 COMPAT_MODELS_RERANK=qwen/qwen3-reranker-0.6b \
  uv run compat check --models --base-url http://127.0.0.1:8000
```

## Limits and gaps

- CPU reranking is slow (see the table); there is no KV prefix reuse yet for the shared rerank prompt.
- Patches (`patch_rows`) that change an embedded source attribute leave its vector stale.
- Adding `embed` (without `attribute`) to a namespace that already has rows creates the `embed_<attribute>`
  column, but rows written before that have no vector in it.
- Local rerank candidates are scored one at a time; embedding is packed into batches.
- The local provider reads whole GGUFs through llama.cpp's mmap, so memory use includes page cache.
- Only text models are supported (EmbeddingGemma 2's image, audio, and video encoders are not loaded).
