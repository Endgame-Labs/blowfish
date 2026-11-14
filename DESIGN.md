# blowfish design 🐡

Status: approved by Kyle on 2026-11-14 (decisions in §11). Build progress is tracked in `PROGRESS.md`.

**Where blowfish fits:** blowfish is for development, testing, CI, and modest self-hosted deployments,
so nobody needs a turbopuffer account just to build against the turbopuffer API. It aims to be
correct, performant, and scalable within reason. For production at serious scale, **we strongly
recommend turbopuffer itself** as the upgrade path. Because the APIs match, the upgrade is a change of
base URL and API key.

blowfish is an open-source, self-hostable search engine that speaks the turbopuffer HTTP API. You give
it an S3-compatible bucket, and it stores everything there: a write-ahead log, immutable index files,
and a small manifest. Any number of stateless blowfish processes can serve that bucket, with memory and
local NVMe as caches. Existing turbopuffer SDKs work against it by setting `TURBOPUFFER_BASE_URL`.

Supporting research (endpoint-by-endpoint API notes, SDK wire quirks, and architecture prior art) lives
in [`docs/research/`](docs/research/). The upstream OpenAPI spec that the official SDKs are generated
from is at https://github.com/turbopuffer/turbopuffer-openapi, and it is our primary contract.

---

## 1. Goals and non-goals

**Goals**

1. **Wire compatibility.** The official SDKs (Python, TypeScript, Go, Java, Ruby, and Rust) work
   unchanged. Response bodies match turbopuffer's in field names, types, ordering semantics, status
   codes, and error shape, so that a compatibility test can diff them.
2. **Object storage is the only source of truth.** Lose every server and every disk, keep the bucket,
   and you lose nothing that was acknowledged. Nodes are disposable caches.
3. **Any S3-compatible store, and nothing else.** AWS S3, Cloudflare R2, GCS (through its S3 API or
   natively), MinIO, SeaweedFS, and the local filesystem for development. No extra database is ever
   required. Stores with conditional PUT (`If-None-Match: *`) get multi-node operation; stores without
   it get single-writer mode (§5.6).
4. **Simple to self-host.** One static binary and one config file (bucket, credentials, API keys).
   Scale out by running more copies.
5. **turbopuffer-like economics and latency.** Warm queries in the tens of milliseconds, cold queries
   in a few S3 round trips, writes acknowledged in roughly one S3 PUT.

**Non-goals (for now)**

- Features that exist only because turbopuffer is a hosted service: billing, pinning to reserved
  capacity, customer-managed encryption keys, and cross-organization copy. We return zeros or
  defaults where the response shape requires the fields, and a clear error where it doesn't.
- Hosted model calls (native `Embed` and model `rerank_by`). They may come later as optional
  pluggable providers.
- Bit-for-bit identical ANN results. ANN is approximate in both systems. We match recall, not row
  order (see §7).

## 2. Compatibility target

The full surface is large. v1 covers what nearly every real client uses. Everything later is listed so
that we return a clear `400 {"status":"error","error":"... is not supported by blowfish yet"}` instead
of silently misbehaving.

### 2.1 Endpoints

| Endpoint | v1 | Notes |
|---|---|---|
| `POST /v2/namespaces/{ns}` (write) | ✅ | All write ops below |
| `POST /v2/namespaces/{ns}/query` (single and multi) | ✅ | Multi is told apart by a `queries` body key |
| `DELETE /v2/namespaces/{ns}` | ✅ | |
| `GET /v1/namespaces` (list, `prefix`, `cursor`, `page_size`) | ✅ | |
| `GET /v1/…/metadata` and `GET /v2/…/metadata` | ✅ | The docs say v1, the SDKs call v2; serve both |
| `GET`/`POST /v1/namespaces/{ns}/schema` | ✅ | The SDKs' `exists()` relies on a 404 here |
| `GET /v1/namespaces/{ns}/hint_cache_warm` | ✅ | Prefetches the namespace into cache |
| `POST /v1/namespaces/{ns}/_debug/recall` | ✅ | We run exact search on the same node, which is cheap for us |
| `POST /v2/namespaces/{ns}/explain_query` | ✅ | Returns our own `plan_text` |
| `PATCH /v1/namespaces/{ns}/metadata` | later | `read_only` is easy; pinning is a no-op |
| Async ops (`/async`, `/v1/…/operations/{token}`) | later | The SDKs only poll when they get a 202, so sync replies are compatible |
| Legacy v1 write/query/export (`POST /v1/namespaces/{ns}`, `GET`/`HEAD /v1/namespaces/{ns}`) | later | A thin shim, only if users ask |

### 2.2 Writes (v1)

- `upsert_rows`, `upsert_columns`, `patch_rows`, `patch_columns`, and `deletes`.
- `delete_by_filter` and `patch_by_filter`, with `rows_remaining` and the documented caps.
- Conditional writes: `upsert_condition`, `patch_condition`, and `delete_condition`, including
  `$ref_new`. A failed condition is a `200` with `rows_affected: 0`, not an error (verified).
- `distance_metric` (`cosine_distance`, `euclidean_squared`), `schema` in the write, and type
  inference from the first value.
- The documented order inside one request: delete_by_filter, patch_by_filter, upserts, patches, then
  deletes. One request is atomic: a validation error rejects the whole batch (verified with mixed ID
  types).
- `copy_from_namespace` within the same blowfish deployment and bucket (a manifest copy plus
  reference-counted segment sharing, so it is nearly free).
- Later: `branch_from_namespace` (which our immutable-segment design makes cheap, so it's an early
  "later"), cross-deployment copy, `encryption`, and `disable_backpressure`.

### 2.3 Types (v1)

`string`, `int`, `uint`, `float`, `bool`, `uuid`, `datetime`, `bytes`, the `[]` array forms of the
scalars, and dense vectors `[N]f32` and `[N]f16`. IDs may be `uint`, `uuid`, or `string`, fixed per
namespace. Vectors are accepted as JSON arrays or as base64 little-endian f32 (the Python and Rust SDKs
send base64 by default). Later: `[N]i8`, sparse `{}f16` with `SparseKNN`, and multi-vector `[][N]f32`.

### 2.4 Queries (v1)

- `rank_by`:
  - Vectors: `ANN` and exact `kNN`.
  - Text: `BM25`, including `last_as_prefix`.
  - Score expressions: `Sum`, `Max`, `Product`, filters as 0/1 scores, `Attribute`, `Saturate`,
    `Decay`, and `Dist`.
  - Order by up to 8 attributes (`asc`/`desc`, nulls first in asc).
- Filters: `Eq`, `NotEq`, `In`, `NotIn`, `Lt`, `Lte`, `Gt`, `Gte`, the `Any*` comparisons, `Contains`,
  `NotContains`, `ContainsAny`, `NotContainsAny`, `Glob`, `NotGlob`, `IGlob`, `NotIGlob`,
  `ContainsAllTokens`, `ContainsAnyToken`, `ContainsTokenSequence`, `And`, `Or`, and `Not`, with
  turbopuffer's null semantics. `Glob` starts as a scan over the candidate set; a trigram index comes
  later.
- Results: `top_k` and `limit` (including `limit.per` diversification), `offset`,
  `include_attributes`/`exclude_attributes`, and `vector_encoding: "base64"`.
- Aggregations: `aggregate_by` with `Count`, `Sum`, `Min`, and `Max`, plus `group_by` (including
  `ForEachUnique`).
- Multi-query: up to 16 queries, with `rerank_by: ["RRF", …]`.
- `consistency: {"level": "strong" | "eventual"}` and `max_staleness_seconds`.
- The response always carries `performance` and `billing`. Billing values are zeros. Performance
  values are real where we have them: `server_total_ms`, `exhaustive_search_count` (the unindexed tail
  we scanned), `approx_namespace_size`, `cache_hit_ratio`, `cache_temperature`, and
  `last_included_write_at`.
- Later: `Regex` and `Fuzzy` (both need trigram indexes), `compute_attributes` with `Highlight`, and
  model `rerank_by`.

### 2.5 Protocol details we must get right

These come from reading the SDKs and probing the real service (details in `docs/research/`):

- **Errors:** always `Content-Type: application/json` and `{"status":"error","error":"…"}`. We keep
  turbopuffer's status codes: 400 for bad input, 404 for a missing namespace, 422 for a malformed body,
  and 429 for backpressure. The SDKs retry 408, 409, 429, and 5xx, so we never use those for permanent
  errors, and we send `x-should-retry: false` when a 5xx is permanent.
- **Routing:** ignore unknown query parameters (the SDKs add `?stainless_overload=…`), and tell
  single from multi-query (and copy from branch) by the body.
- **Compression:** accept `Content-Encoding: gzip` requests, and honor `Accept-Encoding`.
- **`prefer: respond-async`:** every SDK sends it. We ignore it and answer synchronously.
- **Retries without idempotency keys:** a retried write may arrive twice. Upserts, deletes, and plain
  patches are idempotent. Conditional writes are not, which matches turbopuffer.
- **Auth:** `Authorization: Bearer <key>`, with role-based API keys built in (§8.1). An admin
  super key can do everything, including managing other keys. An auth-disabled mode exists for local
  development.
- **Response details:** BM25 and score expressions return `$dist` sorted descending; ANN returns it
  ascending; ordering by attribute omits it. Documents with a score of 0 are dropped. Integer JSON
  infers `int`. The emoji in error strings is optional, but cute.

## 3. Architecture overview

```
           SDK / HTTP clients
                  │
     ┌────────────┴────────────┐     any node serves any namespace;
     │   blowfish node (×N)    │     a router may hash namespaces to nodes
     │  ┌──────┐  ┌─────────┐  │     for cache locality
     │  │ API  │  │ indexer │  │
     │  └──┬───┘  └────┬────┘  │
     │  memory + NVMe cache    │
     └────────────┬────────────┘
                  │  GET (ranges) / PUT If-None-Match / LIST
           S3-compatible bucket
     <prefix>/ns/<namespace>/{wal,manifest,segments}/…
```

A single binary runs every role. In a small deployment, one process does it all. In a larger one, you
run several API nodes and let any of them index (the work is coordinated through the bucket, see §5.4).

**Write path.** A write is validated, then joins the namespace's group-commit batch on that node. The
batch is serialized into one WAL object and written with a conditional create. When the PUT returns,
the write is durable, and we respond.

**Read path.** A strong query makes sure the node knows the latest WAL entry (one cheap request), then
searches the indexed segments (through the cache) plus a brute-force scan of the unindexed WAL tail.
It merges by document ID, with the newest version winning.

**Indexing.** In the background, an indexer folds the WAL tail into immutable segments and publishes
them by writing a new manifest version. Compaction merges small segments into larger ones.

## 4. Bucket layout

Everything lives under one configurable prefix in one bucket:

```
<prefix>/
  blowfish.json                         deployment marker: format version, created_at
  ns/<namespace>/
    manifest/<seq:020>.json             sequenced, create-only manifest versions
    wal/<seq:020>.wal                   sequenced, create-only WAL entries
    segments/<ulid>/                    immutable, never overwritten
      docs.bfc                          columnar documents (attributes and vectors)
      ids.bfi                           sorted ID → row index, plus a bloom filter
      filter.bfx                        per-attribute value dictionaries and roaring bitmaps
      ann.<column>.bfv                  IVF centroids, RaBitQ codes, and full-precision vectors
      fts.<column>.bft                  term dictionary, block-max postings, and doc lengths
      live.<gen>.bfl                    deletion bitmap generations (rows superseded later)
  gc/…                                  pending-deletion records for the garbage collector
```

Design rules:

- **No object is ever overwritten.** Every write creates a new key. That avoids R2's and GCS's
  roughly one-write-per-second limit on a single key, and it makes caching trivially correct: a cached
  byte range of a key is valid forever.
- **Sequence numbers are zero-padded** so that `LIST` returns them in order.
- **Every file has a footer** (offsets, checksums, format version) sized so that one ranged GET of the
  last 64 KiB usually fetches the footer and the top-level index structure together. That keeps cold
  queries to about three round trips.
- **The file formats are our own**, built from simple parts (Arrow-style columns, roaring bitmaps,
  bit-packed postings, and RaBitQ codes), versioned from day one.

## 5. Consistency and concurrency

The bucket's conditional create (`If-None-Match: *`) is the only coordination primitive, and the only
one we rely on. Every S3-compatible store we target supports it on single PUTs, and we verified it on
R2 (both `If-None-Match: *` and `If-Match` return 412 on conflict). Conditional multipart uploads and
conditional deletes are not portable, so we don't use them.

### 5.1 The WAL

- WAL entry `N` is `wal/<N>.wal`. Committing means creating key `N` with `If-None-Match: *`. Exactly
  one writer wins each slot, which gives a total order of writes per namespace without any lease or
  leader.
- A writer that loses a slot (412) learns that its view is stale. It reads the entries it missed,
  re-validates its batch against them (this matters for conditional writes, `patch_by_filter`, and
  schema changes), and tries slot `N+1`. This is optimistic concurrency, and it makes conditional
  writes serializable across nodes.
- **Group commit:** each node batches concurrent writes per namespace and flushes one WAL entry at a
  time. The flush interval adapts: zero delay when idle, with batching kicking in under load
  (turbopuffer commits at most one entry per second per namespace; we can do better on stores with
  faster PUTs).
- **Ambiguous retries:** if a PUT times out and its retry gets a 412, we might have won the slot on
  the first attempt. Each WAL entry carries a random write ID in its object metadata, and after a 412
  we `HEAD` the slot and compare (the technique SlateDB uses).
- **Backpressure:** when the unindexed tail exceeds a limit (turbopuffer's is 2 GB), writes get 429.

### 5.2 The manifest

The manifest is the namespace's index of record: schema, distance metric, ID type, segment list with
their deletion-bitmap generations, `indexed_through` (the last WAL entry folded into segments), stats,
and GC state. It also uses sequenced, create-only keys, and it carries an **indexer epoch** so that a
stalled indexer that wakes up can't publish over a newer one (SlateDB-style fencing). We'll either use
or vendor `slatedb-txn-obj`, which implements exactly this protocol.

The schema is part of the namespace's state, so a schema change is a WAL entry like any other. The
manifest records the schema as of `indexed_through`, and readers replay schema changes from the tail.

### 5.3 Strong and eventual reads

- **Finding the head.** A node remembers the last WAL sequence it has seen for each namespace.
  - A strong query issues `HEAD wal/<last+1>`. A 404 means the node is current. The cost is one small
    request, which matches turbopuffer's roughly 10 ms floor for consistent reads.
  - If the HEAD finds an entry, the node reads forward until it hits a 404.
  - A cold node with nothing cached starts from the latest manifest (found with one `LIST`), then
    reads forward from `indexed_through`.
- **Eventual reads** skip the HEAD and use whatever the node already has, bounded by
  `max_staleness_seconds` and by a cap on the unindexed bytes they scan (turbopuffer: 128 MiB).
- **Read-your-writes on the same node** is free: the node that commits a write knows its sequence
  number.

### 5.4 Indexing coordination

Any node may index a namespace. To avoid duplicate work, indexers claim a namespace by writing a
create-only lease object, `ns/<ns>/indexer/<epoch>.json`, that carries an expiry. Correctness never
depends on the lease: the manifest's epoch fencing rejects a stale indexer's publish. In a single-node
deployment, the one node simply indexes everything.

### 5.5 Garbage collection

Superseded manifests, compacted segments, and WAL entries below `indexed_through` are deleted after
a retention window (default 1 hour). The window must be longer than any query's or writer's staleness:

- A node whose view is older than half the window refreshes the manifest before writing.
- This rule protects against a nasty bug: a stale writer could otherwise re-create a deleted WAL slot.

The retention window also gives us cheap point-in-time recovery for the last hour.

### 5.6 Stores without conditional writes

At startup, blowfish probes the bucket: it creates a scratch object with `If-None-Match: *`, then
tries again and expects a 412. If the store ignores the header, blowfish switches to
**single-writer mode**:

- **One writer process.** Exactly one blowfish process may write the prefix. Every WAL, manifest, and
  lease write is serialized in that process, which is trivially correct.
- **Read-only replicas.** Any number of other processes can serve queries with
  `--read-only`.
- **Best-effort detection.** The writer holds a heartbeat object (`_system/writer.json`, carrying a
  random process ID and refreshed every few seconds). A second writer that sees a fresh heartbeat
  from someone else refuses to start. Without conditional writes this can't be airtight, so the docs
  say so plainly.

The bucket format is the same in both modes, so moving a dataset to a store with conditional writes
needs no migration.

## 6. Indexes

Each segment covers a range of WAL entries and holds the latest version (as of that range) of every
document written in it. A document's newest version can live in only one place: the WAL tail or one
segment. Older copies are hidden by per-segment **deletion bitmaps** (like Lucene's live docs). The
indexer computes these bitmaps when it publishes a new segment, and stores them as new generations.
Queries therefore never need cross-segment ID lookups at runtime, apart from the small unindexed tail.

### 6.1 Documents and filters

- **Documents.** Each segment stores them by column, with attributes and vectors kept separately, so a
  query fetches only the columns it uses.
- **Filters.** Per filterable attribute, a segment keeps a value dictionary and a roaring bitmap of
  rows per value (or per value bucket for high-cardinality numbers), plus sorted runs for range
  queries.
- **Query evaluation.** A filter compiles to a bitmap expression evaluated per segment. Null
  semantics follow turbopuffer exactly (`Eq null` means missing, `Lt` matches nulls, and so on).

### 6.2 Vector search (ANN)

**Choice: an IVF clustered index with RaBitQ-quantized codes and full-precision reranking**, the
SPANN/SPFresh family that turbopuffer itself uses. We are not using HNSW. A graph index walks a chain
of dependent reads, and against object storage each hop is a 60–100 ms round trip. A clustered index
reads in a fixed number of rounds:

1. Centroids (cached, small).
2. Probe the `nprobe` nearest clusters' RaBitQ codes in one batch of parallel ranged GETs.
3. Rerank the best candidates with full-precision vectors.

Per segment, we build k-means centroids (about √n clusters, and a two-level centroid tree once a
segment passes a few million vectors). Each cluster stores its row IDs, its 1-bit RaBitQ codes, and its
full-precision vectors, all contiguous so that one ranged GET serves one cluster.

- **Filtered ANN:** each cluster has a small summary of which filter bitmaps intersect it. The planner
  skips clusters with no matches, widens `nprobe` when the filter is selective, and switches to exact
  kNN over the filter bitmap when the filtered set is small. This is turbopuffer's "native filtering"
  idea.
- **Segments below a size threshold** (a few thousand rows) skip the index and are searched exactly.
- **Recall is a feature, not an afterthought.** `_debug/recall` compares ANN against exact search.
  Nodes also sample about 1% of live queries to compare in the background, and export recall as a
  metric.
- **Later:** SPFresh-style incremental cluster split and merge, so that large namespaces don't need
  full segment rebuilds during compaction.

### 6.3 Full-text search (BM25)

- **Tokenizing:** we use `tantivy`'s tokenizer pipeline (word splitting, lowercasing, ASCII folding,
  stemming, and stopwords) configured to mimic turbopuffer's `word_v4` tokenizer and parameters (`k1`,
  `b`, `max_token_length`, and so on).
- **Scoring:** we use tantivy's standalone `Bm25Weight`, not its index format, which assumes a local
  directory with mmap. Our own format is a term dictionary (an FST) and posting lists in fixed blocks,
  bit-packed with the `bitpacking` crate, with a maximum score per block. That lets block-max MAXSCORE
  skip most of the index on top-k queries. This is the same approach turbopuffer described for its
  FTS v2.
- **Corpus statistics** (document count, average length, and document frequency per term) are
  namespace-wide, so BM25 scores don't depend on how documents are split into segments. Each segment
  stores its own counts. The manifest keeps the totals, and queries adjust them for the WAL tail.
- **Score parity:** our goal is to match turbopuffer's BM25 scores to within a small float tolerance.
  The oracle returned 0.49917623 and 0.45665967 for a three-document test. If its tokenizer has
  undocumented details, we'll settle for rank parity and document the difference.
- **Phrase queries:** `ContainsTokenSequence` needs token positions. We store them in a separate
  stream that's only read when a phrase query asks for it.

### 6.4 Hybrid search

Hybrid search is a multi-query (for example, one ANN query and one BM25 query) fused with
`rerank_by: ["RRF", …]`, or a single `rank_by` score expression. Both run against the same snapshot,
so their results are consistent with each other.

## 7. Caching and performance

- **Cache:** a `foyer` hybrid cache with a memory tier and an NVMe tier, keyed by `(object key,
  aligned range)`. Requests are aligned to 1 MiB blocks (tunable), and concurrent misses for the same
  block share one GET. Because objects are immutable, cache entries never need invalidating; they are
  only evicted.
- **Always warm:** the latest manifest and WAL tail of recently used namespaces, the centroids, term
  dictionaries, footers, and ID indexes.
- **`hint_cache_warm`** prefetches a namespace's small structures and its most-probed clusters.
- **Node affinity:** in a multi-node deployment, a stateless router (or the client's own load
  balancer, using consistent hashing on the namespace name) sends a namespace to the same node most of
  the time. Correctness never depends on it.
- **Compute:** queries run on Tokio for I/O and on a dedicated thread pool for CPU-heavy scoring. We
  use SIMD distance kernels from `numkong` (SimSIMD's successor) and a hand-written popcount kernel for
  RaBitQ.
- **Targets** for v1, on one node with a warm cache and 1M documents of 768-dimensional vectors:
  - ANN: p50 under 20 ms at recall@10 of at least 0.9.
  - BM25: p50 under 20 ms.
  - Cold queries: three or four object-storage round trips.
  - Writes: p50 of one S3 PUT plus a few ms.

## 8. Language and libraries

**Rust**, for predictable latency, SIMD, low memory overhead, and a single static binary, and because
most of the relevant ecosystem (tantivy, the object_store crate, SlateDB, and Lance) is Rust.

| Need | Crate | Why |
|---|---|---|
| HTTP | `axum`, `tokio`, `tower-http` (gzip) | Standard and fast |
| JSON | `serde`, `serde_json` (`simd-json` if profiling says so) | |
| Object storage | `object_store` | S3, R2, GCS, Azure, and local; `PutMode::Create` maps to `If-None-Match` |
| Manifest fencing | `slatedb-txn-obj` (or vendored) | A proven create-only sequenced-object protocol |
| Cache | `foyer` | Hybrid memory and disk; used by SlateDB and RisingWave |
| Bitmaps | `roaring` | |
| Text analysis | `tantivy` (tokenizers and `Bm25Weight` only), `rust-stemmers` | |
| Postings | `bitpacking`, `fst` | |
| Vectors | our own IVF and RaBitQ; `numkong`, `half` | No off-the-shelf crate does object-storage IVF the way we need. `lance-index` and `rabitq-rs` are references |
| Columns | Arrow-style layouts written by us (maybe `arrow-buffer`) | Full Parquet costs more round trips than we want |
| Observability | `tracing`, Prometheus metrics | |

The pre-1.0 crates (`object_store`, `foyer`, and `slatedb-txn-obj`) are pinned exactly and wrapped
behind our own traits.

**Considered and rejected for the core: building on Lance.** Lance already has IVF with RaBitQ, an
inverted index, and compare-and-swap commits, so it's the fastest route to a demo. But it's a
batch-oriented table format. Frequent small upserts create many versions and fragments that need
compaction, and it has no notion of a brute-forced WAL tail behind a group-commit writer. We'll
benchmark against it instead.

### 8.1 API keys, roles, and metrics

**Roles.** They mirror turbopuffer's API-key roles, so client code behaves the same way:

| Role | Can do |
|---|---|
| `admin` | everything on its namespaces, including delete |
| `write` | writes and queries |
| `read` | queries, metadata, schema, and recall |
| `list` | list namespaces |
| `metrics` | read `/metrics` and `/_blowfish/v1/status` (blowfish-only) |
| `keys` | manage API keys through the admin API, but never mint a key with more power than its own (blowfish-only) |

**Key scope.** Each key has a set of roles, plus an optional list of namespace prefixes it is
limited to (the default is all namespaces). It also has a name, a creation time, an optional expiry,
and a last-used time (sampled).

**The super key.** It comes from config or the `BLOWFISH_ADMIN_KEY` environment variable. It can do
everything, including managing keys and reading metrics, like Cloudflare's global key. It is never
stored in the bucket.

**Key storage.** Keys live in the bucket under `_system/keys/<key_id>.json`. They are stored as
SHA-256 hashes (keys are long random tokens, so a slow hash adds nothing), and the plaintext is shown
once, at creation.

- **Revocation** writes `_system/keys/<key_id>.revoked`.
- **Propagation:** nodes cache the key set and refresh it every few seconds, so a new or revoked key
  takes effect everywhere within about 5 seconds, and immediately on the node that handled the change.
- **Token format:** `bf_<key_id>_<secret>`, so a node can look up the key without scanning.

**Admin API.** It lives under `/_blowfish/v1/` and needs the super key or a key with the `keys` role:

- `POST`, `GET`, and `DELETE /_blowfish/v1/keys[/{id}]`
- `GET /_blowfish/v1/status`, which reports the storage mode, nodes, and indexing lag

The `blowfish keys` CLI subcommand wraps the same API.

**Metrics.** Prometheus text format at `GET /metrics`. It is served on the main port with the super
key or a `metrics` role, and optionally on a separate unauthenticated port bound to localhost. Metrics
cover:

- Requests by endpoint, status, and key name.
- Latency histograms.
- Rows written and queried.
- WAL commit latency and batch sizes.
- Unindexed bytes per namespace and indexing lag.
- Cache hits and bytes by tier.
- Object-store requests by operation and status.
- Sampled ANN recall.

## 9. Testing compatibility

The test that matters is: **the official SDKs, the same calls, both servers, and diffed results.**

1. **Scenario suite** (`compat/`, Python with `uv`, using the official `turbopuffer` package). Each
   scenario is a sequence of SDK calls (writes, queries, metadata, and errors) against a fresh
   namespace.
2. **Two targets.** The suite runs against real turbopuffer (oracle namespaces named
   `blowfish-oracle-*`, deleted afterward) and against blowfish.
3. **Normalized diff.** Drop `performance`, `billing`, and timestamps. Compare `$dist` within a float
   tolerance. Sort ties where turbopuffer leaves ordering unspecified. Compare error status codes, and
   error messages by category.
   - **ANN results** are compared by recall against exact search, not by row order.
4. **Recorded fixtures.** Oracle responses are recorded as golden files in the repo, so CI runs
   against blowfish alone and doesn't need a turbopuffer account. Re-recording is a manual,
   occasional job.
5. **Every SDK.** A smaller smoke suite runs the TypeScript, Go, and Rust SDKs against blowfish, to
   catch wire-level differences: base64 vectors, gzip, retries, and the headers each SDK expects.
6. **Property and fuzz tests.** Random operation sequences run against blowfish and against a simple
   in-memory reference model, asserting identical results. Crash-injection tests kill nodes mid-commit
   and mid-index, then check that every acknowledged write survives exactly once.
7. **Storage matrix.** Integration tests run against local files, MinIO or SeaweedFS in Docker, and a
   real R2 bucket (a `dev/` prefix), with a conditional-write self-test at startup.

## 10. Milestones

Each milestone ends with the compat suite green for its scope and a short demo.

| # | Milestone | What it delivers |
|---|---|---|
| **M1** | **Walking skeleton** | Rust workspace; config; the `object_store` layer and a startup conditional-write self-test; the WAL with group commit and CAS; the HTTP API for write, query, metadata, schema, list, and delete, with **exhaustive search only** (no indexes yet, everything is a scan); the full filter language, order-by, `kNN`/`ANN` (exact), BM25 (exact, computed at query time), aggregations, and multi-query with RRF; the compat harness with the first recorded oracle fixtures. Correct but slow: the reference implementation every later index is checked against |
| M2 | Segments and the indexer | Manifest with fencing; the indexer folds the WAL into columnar segments with filter bitmaps and deletion bitmaps; queries merge segments with the WAL tail; GC; crash-injection tests |
| M3 | Vector index | IVF with RaBitQ and reranking; filtered ANN; `_debug/recall` and sampled recall metrics; benchmarks against turbopuffer's published numbers |
| M4 | Full-text index | Tokenizer parity, block-max postings, phrase positions, namespace-wide statistics, and BM25 score parity tests |
| M5 | Cache and performance | foyer memory and NVMe cache, aligned ranged reads, `hint_cache_warm`, a cold-query round-trip budget, and load tests |
| M6 | Multi-node and operations | Indexer leases, node-affinity routing, Prometheus metrics, a Docker image, a Helm chart or compose file, and docs |
| M7 | The "later" list | Branching and copy, `Regex`/`Fuzzy`/`Glob` with trigram indexes, `i8` and sparse vectors, `Highlight`, async ops, and the legacy v1 shim |

M1 is deliberately index-free. It locks down the API semantics (the hard, fiddly part of
compatibility) against an implementation that's simple enough to be obviously right, and then every
later milestone is a performance change that the same test suite guards.

## 11. Decisions (Kyle, 2026-11-14)

1. **License:** MIT.
2. **Positioning:** "compatible with the turbopuffer API". blowfish exists so people can build and
   test against that API without a turbopuffer account. We strongly recommend turbopuffer as the
   upgrade path for production scale.
3. **BM25:** scores reasonably close to turbopuffer's are fine. Ranking should match on ordinary
   corpora.
4. **Storage:** 100% turbopuffer-compatible, with no extra moving parts. No Postgres or SQLite
   fallback: stores without conditional writes run in single-writer mode (§5.6).
5. **Auth and metrics:** robust role-based API key management and Prometheus metrics out of the box,
   plus an admin super key (§8.1).
6. **Scope:** build all milestones, not just M1.
