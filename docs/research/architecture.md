# blowfish: architecture research (turbopuffer internals, prior art, Rust libraries)

Researched 2026-11-14. Numbers are quoted from the linked pages as of that date. Items marked
**(inference)** are my reading, not something the source states.

---

## 0. Recommendation summary

### Architecture I'd pick

A **turbopuffer-shaped design: one namespace = one object-storage prefix, a WAL of immutable,
sequence-numbered objects, asynchronous indexing into immutable segments, and a stateless query tier
with a memory+NVMe cache**. Concretely:

1. **WAL = create-only, contiguous sequence numbers.** Each commit writes
   `ns/<ns>/wal/<seq:020>.wal` with `If-None-Match: *` (`object_store::PutMode::Create`). Whoever
   creates slot `N` wins and everyone else gets 412. This is the SlateDB WAL pattern. It needs only
   the most portable conditional primitive (create-if-absent), it never overwrites a key (so it avoids
   R2's and GCS's ~1 write/s per key limit), and it gives a total order for free.
2. **Group commit per namespace** in the writer process: buffer concurrent upserts and flush one WAL
   object per interval. turbopuffer runs at 1 WAL entry/s/namespace. I'd make it configurable and
   default to roughly 100 to 250ms, then measure. The API acks only after the PUT returns.
3. **Manifest = sequenced, create-only versions with epoch fencing**
   (`ns/<ns>/manifest/<id:020>.manifest`), which is SlateDB's design. It holds the writer epoch,
   the indexer epoch, the `indexed_through_seq` cursor, the list of live segments, the schema, and
   tombstone and GC state. **The published `slatedb-txn-obj` crate (0.18.1) already implements this**
   (`FenceableTransactionalObject`, `ObjectStoreSequencedStorageProtocol`), so use it or vendor it.
4. **Strongly consistent reads** cost one cheap round trip: `HEAD wal/<last_known_seq+1>`. A 404 means
   the node is up to date. Otherwise read forward. This matches turbopuffer's "~10ms consistent read"
   floor ([tradeoffs](https://turbopuffer.com/docs/tradeoffs)). Eventual consistency skips the check.
5. **Async indexer** (separate role, the same binary): reads the WAL past the cursor and builds
   **immutable segments** for vectors, full text, and filters. It uploads them as new keys, then CASes
   the manifest to advance the cursor. Background compaction merges segments. Queries search the
   indexed segments **plus a brute-force scan of the unindexed WAL tail**, and merge by id with
   latest-sequence-wins. Bound the tail (turbopuffer: 2 GB unindexed returns 429, and more than
   128 MiB of outstanding writes makes new writes invisible until indexed).
6. **Vector index (v1): IVF with a centroid tree, RaBitQ 1-bit codes, and full-precision rerank.**
   This is the SPANN/SPFresh family that turbopuffer uses, not HNSW. Cluster-at-a-time reads suit
   byte-range GETs and NVMe pages, the number of round trips is bounded (cold query in 2 to 3 round
   trips), and it vectorizes well. Ship immutable per-segment IVF first. Add SPFresh-style incremental
   split/merge/reassign (LIRE) later, when segment merges get expensive.
7. **Full-text:** tantivy's **tokenizer pipeline** and its standalone **`Bm25Weight`** scorer, with
   **blowfish's own posting format**: fixed-size blocks of 128 to 512 postings, `bitpacking` 128-int frames,
   block-max scores, and vectorized block-max MAXSCORE. This is exactly what turbopuffer's FTS v2 did.
   Don't use tantivy's index/segment format. It assumes a directory with random access and mmap.
8. **Filters:** per-segment columnar attribute storage plus `roaring` bitmaps per (attribute value),
   and a cluster-granularity summary bitmap so the ANN planner can skip clusters with no matches
   (turbopuffer's "native filtering").
9. **Cache:** `foyer` hybrid cache (memory plus NVMe) keyed by `(object key, aligned byte range)`.
   Fetch in aligned chunks (WarpStream uses 4 MiB) with single-flight deduplication of concurrent GETs.
10. **Serving:** `axum` on `tokio`, with CPU-heavy scoring on a dedicated rayon or blocking pool so it
    doesn't stall the async reactor. Distance kernels from `numkong` (the successor to SimSIMD) or a
    small hand-written `std::arch` kernel for the RaBitQ popcount path.

### Libraries at a glance

| Need | Pick | Notes |
|---|---|---|
| Object storage | `object_store` 0.14 | S3/R2/MinIO/GCS/Azure/local. `PutMode::Create`/`Update` map to `If-None-Match`/`If-Match`. `S3ConditionalPut::ETagMatch` is the default. No conditional multipart. |
| Manifest CAS + fencing | `slatedb-txn-obj` 0.18 | Generic, published, and production-used inside SlateDB. Pre-1.0. |
| Text analysis | `tantivy` 0.27 tokenizers (or `tantivy-tokenizer-api` plus `rust-stemmers`) | Use `tantivy::query::Bm25Weight::for_one_term` standalone. |
| Posting compression | `bitpacking` 0.9 | turbopuffer uses it: ~741M postings/s/core decode. |
| ANN | Own IVF + RaBitQ. Reference code: `rabitq-rs`, `lance-index` (IVF_RQ), `diskann` (MSFT, Rust) | No off-the-shelf Rust crate does object-storage IVF the way blowfish needs. |
| Bitmaps | `roaring` 0.12 (pure Rust) | Use `croaring` only if profiling demands it. |
| Columnar | `arrow` / `parquet` 60.x (or Arrow IPC for segments) | Parquet for the attribute columns, a custom format for vector clusters and postings. |
| Cache | `foyer` 0.22 | Hybrid memory+disk; used by SlateDB and RisingWave. |
| SIMD distances | `numkong` 7.x (formerly `simsimd`), `half` for f16 | Hand-write the RaBitQ kernel (popcount, AVX-512 VPOPCNTDQ). |
| HTTP | `axum` 0.8, `tokio` 1.x | |

### Main risks

1. **Conditional-write portability.** AWS S3, R2, GCS (via generation preconditions), and
   SeaweedFS support it. **MinIO community edition went into maintenance mode in Dec 2025 and was
   later archived** (it still supports `If-Match`/`If-None-Match`, but its future is uncertain).
   **Garage reportedly lacks it.** So blowfish needs a startup self-test plus a documented fallback (see §2).
2. **Ambiguous conditional-put retries.** A PUT that succeeded but timed out, then got retried, returns
   412, so a naive client thinks it lost the race. SlateDB solves this by tagging each conditional put
   with a ULID in object metadata and checking it after a 412/409. blowfish must do the same (see §2.6).
3. **ANN quality under updates and filters.** Recall can drift with deletes, splits, and selective
   filters. Copy turbopuffer's **continuous recall measurement** (sample ~1% of queries and compare
   against exhaustive search) from day one.
4. **Cold-query latency is set by round trips.** Every format decision (file footers, centroid
   placement, coalesced range reads) has to keep a cold query to about 3 sequential GET round trips.
5. **API-compatibility surface is large.** turbopuffer's query language includes rank_by expression
   trees, aggregations, group_by, multi-query with RRF, conditional writes, patch/delete_by_filter,
   and regex/glob with trigram indexes. Prioritize ruthlessly. Pin a compatibility test suite against
   the real API (there's a `turbopuffer` integration on this VM).
6. **Pre-1.0 dependencies** (`slatedb-txn-obj`, `foyer`, `object_store` all break APIs between
   minors). Pin exact versions and wrap them behind blowfish's own traits.

### An alternative to keep in mind: build on Lance

`lance` 14.x already provides an object-storage table format with conditional-put commits, IVF_RQ
(RaBitQ), an inverted FTS index, and scalar/bitmap indexes. "turbopuffer API on top of Lance" is the
fastest path to a demo. I'd still not make it the core, because Lance is a batch-oriented table
format (fragments and versions). Small, frequent upserts create many versions and fragments that need
compaction, and it has no notion of a brute-forced WAL tail with a per-namespace group-commit writer.
It's still worth reading `lance-index` for IVF_RQ and the inverted index, and benchmarking against it.
**(inference)**

---

## 1. How turbopuffer works (from its docs and blog)

### 1.1 Storage layout and tiers ([architecture](https://turbopuffer.com/docs/architecture))

- "Each namespace has its own prefix on object storage." The WAL is at `s3://tpuf/{namespace_id}/wal`.
  The index lives under `/{org_id}/{namespace}/index` with `centroids.bin`, `clusters-1.bin`,
  `clusters-2.bin`, and the namespace config. An **indexing queue** also lives in object storage.
- Tier table: DRAM 300 GB/s, ~100ns, $2/GB-mo. NVMe 10 GB/s, 100µs, $0.08/GB-mo. S3 <5 GB/s, ~100ms,
  $0.02/GB-mo.
- Measured S3 GET: p50 63ms, p99 78ms, p999 118ms. S3 PUT: p50 100ms, p99 195ms, p999 274ms.
- Memory and NVMe caches sit in front of S3 on each query node. "After the first query, a namespace's
  documents are cached on NVMe." Queries are routed to the same node for cache locality, but "any query
  node can serve queries from any namespace."
- Launch post ([blog/turbopuffer](https://turbopuffer.com/blog/turbopuffer)): $/TB-month of RAM+3x SSD
  $3,600, 3x SSD $600, **S3+SSD cache $70**, S3 alone $20. "Object storage is the source of truth",
  "object-storage-first, not tiering." Uptime 99.99% since launch.

### 1.2 Write path, WAL, and batching

- "Every write adds a new file to the WAL directory inside the namespace's prefix." "If a write
  returns successfully, data is guaranteed to be durably written to object storage."
  ([architecture](https://turbopuffer.com/docs/architecture))
- Writes go to a memory buffer, then **group-commit** to the WAL. "**Each namespace can currently write
  1 WAL entry per second.**" Concurrent writes to the same namespace are batched into one entry. If a
  new batch starts within 1s of the previous one, it can take up to 1s to commit.
- The architecture diagram marks WAL entries as indexed+committed, committed-but-unindexed, or
  written-but-not-committed, with a **"CAS commit point"** and an **index cursor**. turbopuffer doesn't
  publish the exact WAL naming or CAS protocol. **(inference)** A create-only sequenced key (or an
  If-Match on a head object) is the obvious realization, and SlateDB's protocol (§3.1) is a published
  equivalent.
- Upsert latency (512KB): p50 165ms, p90 248ms, p99 850ms
  ([performance](https://turbopuffer.com/docs/performance)). Tradeoffs page: "Writes can take up to
  200ms to commit, but a namespace handles thousands of writes per second."
- Limits ([limits](https://turbopuffer.com/docs/limits)): per-namespace write throughput **10k
  writes/s @ 32 MB/s**. Max upsert request 512 MB. **Max ingested-but-unindexed data 2 GB; above this,
  upserts return HTTP 429.** Max docs per shard 500M @ 1TB, up to 256 shards per namespace. Max dims
  10,752. Max doc 64 MiB, id 64 bytes, filterable value 4 KiB.
- Sharded namespaces: "Writes commit once to a shared WAL and apply to all shards with all-or-nothing
  semantics." ([guarantees](https://turbopuffer.com/docs/guarantees))

### 1.3 Async indexing and the brute-forced tail

- "After data is committed to the log, it is asynchronously indexed." Indexers are separate
  `./tpuf indexer` processes. "Any data that has not yet been indexed is still available to search",
  using "a slower exhaustive search of recent data in the log."
  ([architecture](https://turbopuffer.com/docs/architecture))
- From [guarantees](https://turbopuffer.com/docs/guarantees): "Once a namespace has more than
  **128MiB of outstanding writes**, new writes aren't visible until they're indexed and loaded into
  cache. This takes tens of seconds for small namespaces and tens of minutes for large ones." This is
  the backstop that bounds tail brute-force cost.
- Index jobs are scheduled through a **queue that is one JSON file on object storage**
  ([object-storage-queue](https://turbopuffer.com/blog/object-storage-queue)). The whole queue
  ("well under 1 GiB") is one `queue.json`, updated with CAS. Requests that arrive during an in-flight
  write are **group-committed** into the next CAS. A **stateless broker** is the only process that
  touches the object, and it acks only after a durable commit. On broker failover, the new broker's
  address is written into `queue.json`, and CAS makes two brokers safe. Workers write heartbeat
  timestamps per job, and an expired heartbeat lets another worker take over. The result is FIFO,
  at-least-once delivery. Throughput is about 5 writes/s under contention, and GCS has a 1 write/s
  per-object limit. Tail latency is about 10x lower than the previous sharded queue. This queue is
  **not** in the write path.

### 1.4 Consistency model

- "turbopuffer provides strong consistency by default." A strong read needs an object-storage round
  trip, "~10ms for consistent read." The tradeoffs page says the floor "matches object storage's
  `GET IF-NOT-MATCH` latency" (S3 metadata p50 10ms, p90 17ms. GCS p50 12 to 18ms).
  **(inference)** The node does a conditional GET or HEAD on a head or manifest object and gets
  304/404 when nothing changed.
- Eventual consistency: "staleness of up to about one hour can be observed in the worst case." The
  query parameter is `consistency: eventual` with `max_staleness_seconds` (max 3600)
  ([query](https://turbopuffer.com/docs/query)).
- "Over 99.8% of queries return consistent data." Staleness during scaling or failover is ~100ms.
  CAP: favors consistency when object storage is unreachable.
- Read-after-write is not a formal guarantee for eventual reads.
- Batches are atomic. **Conditional writes** (`upsert_condition`, `patch_condition`,
  `delete_condition`, with `$ref_new`) are "evaluated atomically with the write ... as if run under
  Serializable isolation". `patch_by_filter` and `delete_by_filter` run as Read Committed (limits 50k
  and 5M rows) ([guarantees](https://turbopuffer.com/docs/guarantees),
  [write](https://turbopuffer.com/docs/write)). **(inference)** Since one writer per namespace owns the
  WAL order, it can evaluate conditions against its in-memory view at commit time. CAS on the WAL slot
  is what makes that view authoritative.

### 1.5 Query path and latency

- Cold query: "3-4 roundtrips, as little as ~400ms". 1) metadata, 2) first index level (filter index,
  centroid index, unindexed WAL) **in parallel**, 3) clusters.
  ([architecture](https://turbopuffer.com/docs/architecture))
- Published numbers ([performance](https://turbopuffer.com/docs/performance)):
  - Vector, 768-dim f16, 10M docs (~15GB), strong: **warm p50 14ms / p90 17 / p99 27**;
    **cold p50 874ms / p90 1214 / p99 1686**.
  - BM25, 1M docs (~300MB), strong: **warm p50 13 / p90 18 / p99 29**; **cold p50 316 / p90 381 /
    p99 559**.
  - Older launch numbers (p90): 1M 768-dim vectors cold 444ms / warm 10ms; BM25 1M cold 285ms /
    warm 18ms.
- Concurrency: 16 concurrent queries per unpinned namespace (a 17th waits up to 800ms, then 429).
  More than 5k queries/s per namespace. Top-k max 10k. Recall@10 90 to 100%.
- Warm-cache "pre-flight" queries and namespace "pinning" (reserved, always-warm nodes) are the
  latency escape hatches.

### 1.6 Vector index: ANN v3 ([ann-v3](https://turbopuffer.com/blog/ann-v3))

- SPFresh-based centroid index, extended into a **multi-level tree of clusters, branching factor ~100**
  ("wide, shallow"), sized so that **if data vectors fit on SSD, centroids fit in DRAM** (the DRAM:SSD
  ratio is 10 to 50x). Tree height bounds cold round trips.
- Probe about **500 clusters × ~100 vectors** per machine to hit the recall target.
- **RaBitQ**: each vector is stored at full precision and as 1 bit/dim codes (16 to 32x compression).
  Only candidates whose distance-estimate interval overlaps the top-k boundary get reranked, which is
  **<1% of vectors**. Upper quantized levels live in L3 (a 504 MiB shared cache in their example), the
  bottom quantized level in DRAM, and full-precision vectors on NVMe.
- The system became compute-bound at about 1k QPS. Moving the popcount kernel to AVX-512 `VPOPCNTDQ`
  gave +30% in microbenchmarks and +5% end to end.
- Target: 100B vectors × 1024 dims f16 (~200 TiB), >1k QPS, **p99 ≤ 200ms**. Random sharding at
  ingest, broadcast queries, merge top-k.
- [native-filtering](https://turbopuffer.com/blog/native-filtering): SPFresh clusters split and merge
  "so the index never needs periodic full rebuilds". At most 2 object-storage round trips cold.

Background papers: SPANN (centroids in memory, posting lists on disk, boundary vectors replicated
into several clusters) https://arxiv.org/abs/2111.08566. SPFresh / LIRE (in-place split, merge, and
reassign) https://arxiv.org/abs/2410.14452. RaBitQ https://arxiv.org/abs/2405.12497. Extended
(multi-bit) RaBitQ https://arxiv.org/abs/2409.09913.

### 1.7 Native filtering ([native-filtering](https://turbopuffer.com/blog/native-filtering))

- Post-filtering drops recall to 0% in their example. Pre-filtering gives 100% recall but takes ~10s.
  The target is ~90% recall at ~25ms.
- Each doc has an address `{cluster_id}:{local_id}`. Attribute indexes live in an **LSM KV** keyed by
  `(attribute_value, cluster_id)`, with the value a **compressed bitmap of local_ids**. They update
  when SPFresh rebalances.
- The planner picks **the nearest clusters that contain at least one match**, then evaluates only the
  matches. A **downsampled cluster-granularity copy** of each index lets cold queries do bitmap
  algebra at cluster level before fetching exact bitmaps.

### 1.8 Full-text search

- [fts-v2-postings](https://turbopuffer.com/blog/fts-v2-postings): the storage engine is "an LSM-tree
  that persists data to object storage". Posting lists are split into **blocks targeting 256 postings
  (split above 512, merge below 128)**. v1 partitioned postings by vector cluster, and the median
  partition held ~1.5 postings, so overhead dominated. Compression uses **Quickwit's `bitpacking`
  crate** on 128-posting frames at ~741M postings/s/core (~4.5 GB/s, versus ~1 GB/s for zstd). On
  MSMARCO 40M the index shrank from 51.6 GiB to 5.22 GiB.
- [fts-v2-maxscore](https://turbopuffer.com/blog/fts-v2-maxscore): **block-max MAXSCORE, vectorized**,
  rather than WAND. It processes runs of doc IDs per iterator in batches for prefetching, branch
  predictability, and SIMD. This follows Lucene's move away from WAND.
  [fts-v2](https://turbopuffer.com/blog/fts-v2): up to 20x faster, e.g. "lord of the rings" went from
  75ms to 6ms at k=100 on 5M Wikipedia docs. "Direct inspiration from Tantivy and Lucene."
- [fully-vectorized-search-engine](https://turbopuffer.com/blog/fully-vectorized-search-engine)
  (Oct 2026): every query runs through a vectorized, batch-at-a-time operator pipeline (`BM25Term`,
  `Filter`, `Decay`, `TopKByTextScore`, `Project`, ...). The average text query has about 17 operators.
- Performance advice: regex and glob use a **trigram index**. If indexing is slow with both ANN and
  BM25 on one namespace, split them into separate namespaces.

### 1.9 Continuous recall ([continuous-recall](https://turbopuffer.com/blog/continuous-recall))

Sample **~1% of live queries** and rerun them exhaustively. Alert if recall@10 falls below 90 to 95%,
including for filtered queries. A recall endpoint is exposed. This is cheap to build into blowfish
from the start, and it's the main defense against ANN regressions.

### 1.10 API surface to match ([query](https://turbopuffer.com/docs/query), [write](https://turbopuffer.com/docs/write))

`POST /v2/namespaces/:ns/query`: `rank_by` (ANN, kNN, BM25, Sum/Max/Product, attribute order,
SparseKNN), `filters` (Eq, Gte, Lt, ContainsAny, And/Or, Glob, Regex, ...), `consistency`,
`aggregate_by` (Count/Sum/Min/Max), `group_by`, multi-query (≤16) with `rerank_by: ["RRF"]`,
`include_attributes`, and `limit {total, per}`. Writes: upsert/patch rows or columns, deletes,
`*_condition`, `patch_by_filter`/`delete_by_filter`, `copy_from_namespace`, and schema (types are
inferred; `filterable`, `full_text_search`, `regex`, and `glob` settings).

---

## 2. S3-compatible conditional writes

### 2.1 AWS S3 ([docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html))

- **`If-None-Match: *`** on **PutObject, CompleteMultipartUpload, and CopyObject** (GA Aug 2024).
  **`If-Match: <etag>`** on the same three APIs (Nov 2024). SigV4 only.
- Results: 200, **412 Precondition Failed**, or **409 Conflict** on a concurrent delete race. For
  PutObject you can retry after a 409. For CompleteMultipartUpload you must restart the whole
  multipart upload. `If-Match` returns 404 if the object was deleted.
- "If multiple conditional writes ... occur for the same object name, the first write operation to
  finish succeeds."
- In-progress multipart uploads are invisible to conditions. The condition is evaluated at Complete.
- Bucket policies can **enforce** conditional writes (`s3:if-none-match` / `s3:if-match` condition
  keys).
- S3 has been strongly read-after-write consistent (including LIST) since Dec 2020.

### 2.2 Cloudflare R2

- The compatibility table ([R2 S3 API](https://developers.cloudflare.com/r2/api/s3/api/)) lists
  **If-Match, If-None-Match, If-Modified-Since, and If-Unmodified-Since on PutObject and
  CompleteMultipartUpload** (also on Get and Head).
- [R2 extensions](https://developers.cloudflare.com/r2/api/s3/extensions/): conditions also work on
  **CreateMultipartUpload** but "are not retained for upload completion", so you have to send them
  again on Complete. CopyObject destination conditions use non-standard `cf-copy-destination-if-*`
  headers (beta).
- **R2 limit: concurrent writes to the same key are capped at "1 per second"**, returning 429
  ([limits](https://developers.cloudflare.com/r2/platform/limits/)). A design that overwrites one hot
  object (a single `head.json` or `queue.json`) is capped at about 1 commit/s per namespace on R2.
  Create-only new keys avoid this limit.

### 2.3 MinIO

- MinIO has supported `If-Match` and `If-None-Match` on uploads since PR #16551 (Feb 2023)
  ([MinIO blog](https://www.min.io/blog/leading-the-way-minios-conditional-write-feature-for-modern-data-workloads)).
  The AIStor compatibility page lists them on PutObject, CopyObject, and CreateMultipartUpload.
  `object_store` names MinIO alongside R2 as an `ETagMatch` store.
- **Status risk:** MinIO community edition stopped shipping binaries in Oct 2025, moved to maintenance
  mode in Dec 2025, and has since been reported archived
  ([InfoQ](https://www.infoq.com/news/2025/12/minio-s3-api-alternatives)). Self-hosters are moving to
  SeaweedFS (whose wiki says conditional If-Match/If-None-Match is atomic cluster-wide), Garage
  (reportedly **no** conditional writes; unverified), and RustFS (young; conditional support
  unverified). blowfish's docs should name tested backends.

### 2.4 Google Cloud Storage ([preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions))

- Native: `ifGenerationMatch` / `x-goog-if-generation-match`. **Generation `0` means "only if no live
  object exists"**, which is create-if-absent. A matching generation gives CAS. Metageneration
  preconditions exist too. Compose supports destination and source preconditions. Failures return 412.
- In the XML (S3-interop) API, `If-Match` and `If-None-Match` apply only to **reads**, so an S3 client
  pointed at GCS gets no conditional PUT. Use `object_store`'s native GCS backend, which maps
  `PutMode::Create` to `ifGenerationMatch=0`.
- GCS also has a ~1 write/s per-object mutation rate (cited in turbopuffer's queue post).

### 2.5 What's portable

| Primitive | AWS S3 | R2 | MinIO | GCS (native) | Azure |
|---|---|---|---|---|---|
| Create-if-absent (`If-None-Match: *`) on PUT | yes | yes | yes | yes (gen=0) | yes |
| CAS on ETag/version (`If-Match`) on PUT | yes | yes | yes | yes (generation) | yes |
| Conditional CompleteMultipartUpload | yes | yes (resend on Complete) | create only? (verify) | n/a (compose) | n/a |
| Conditional DELETE | yes (`If-Match`, GA Sep 2025: https://aws.amazon.com/about-aws/whats-new/2025/09/amazon-s3-conditional-deletes-s3-general-purpose-buckets) | no (not in table) | listed by AIStor | yes | yes |
| Per-key write rate | high | **1/s** | high | **~1/s** | high |

**Portable subset to design on:** single-request `PutObject` with `If-None-Match: *`, then `If-Match`
on small objects. Don't rely on conditional multipart (keep WAL entries and manifests under the
single-PUT limit, 5 GiB, and in practice much smaller), on conditional delete, or on hot-key
overwrites. `object_store` exposes exactly this: `PutMode::{Overwrite, Create, Update(UpdateVersion)}`.
`AmazonS3Builder::with_conditional_put` defaults to `S3ConditionalPut::ETagMatch`, and the old
DynamoDB variant is deprecated. `PutMultipartOptions` has **no** mode field
([docs.rs](https://docs.rs/object_store/latest/object_store/enum.PutMode.html)).

### 2.6 Gotcha: ambiguous retries

If a conditional PUT succeeds on the server but the response is lost, the client's retry gets
412/AlreadyExists, and the client wrongly concludes it lost. SlateDB's `RetryingObjectStore` attaches
a **ULID put-id as object metadata on every conditional put**. On 412/AlreadyExists it HEADs the
object, and if the put-id matches, it treats the write as a success
(`slatedb/src/retrying_object_store.rs`). blowfish must do the same, or store a writer id and epoch
inside the object body and verify on conflict.

### 2.7 Fallbacks for stores without conditional writes

1. **External transactional store for the commit point** (SlateDB RFC 0001 "two-phase write"):
   write to a temp key, insert `(dest, committed=false)` into DynamoDB, Postgres, or SQLite (the
   uniqueness constraint arbitrates), copy to the final key, then mark it committed. That costs about
   2 PUTs, 2 DB writes, and 1 DELETE per commit. Lance does the same with an external manifest store
   (DynamoDB keyed by `base_uri`, `version`). For blowfish: a `CommitStore` trait with
   `ObjectStoreCas` (default), `Postgres`, and `Sqlite` (single node) implementations.
2. **Single-writer lease without CAS:** Neon's **generation numbers**
   ([RFC 025](https://github.com/neondatabase/neon/blob/main/docs/rfcs/025-generation-numbers.md)).
   An external authority hands out a monotonically increasing generation per tenant. Every object key
   carries the generation suffix, so competing writers never write the same key. Readers load "the
   highest index not exceeding my generation". Only **deletions** need a live check with the
   authority, and if the check fails, the object leaks and a scrubber cleans it up later. This works
   on any S3, but it needs a coordinator.
3. **Single-node / embedded mode:** a local lock file. This is fine for `blowfish --single-node`
   against Garage or a filesystem.
4. Detect at startup: try `PutMode::Create` twice on a probe key and confirm the second fails. Refuse
   to start multi-writer mode if it doesn't.

---

## 3. Prior art

### 3.1 SlateDB (Rust LSM on object storage); the most directly reusable

- Version 0.18.1 (Nov 2026). A release about every 2 months. Built on `object_store`, with foyer
  block caches and a `DbReader`.
- **Manifest** ([RFC 0001](https://github.com/slatedb/slatedb/blob/main/rfcs/0001-manifest.md)):
  `manifest/<20-digit id>.manifest`. IDs are "monotonically increasing and contiguous", and the
  highest ID is current. An update is read-modify-write, creating the next ID with create-if-absent.
  If another writer took that ID, re-read and reapply.
- **Writer fencing:** `writer_epoch` (u64) is kept in the manifest and as metadata on every WAL SST.
  On startup: read the manifest, bump the epoch (and snapshot to protect WAL SSTs), CAS the manifest,
  list the WAL to find the next free ID, then **write an empty "fencing" SST with the new epoch at
  that ID using create-if-absent**. If the slot holds an older epoch, retry at the next ID. If it holds
  a newer epoch, "the current writer has been fenced". Zombies find out only when they next try to
  write. "SST IDs in `wal` will be contiguous and monotonically increasing", and readers apply WAL
  SSTs while they stay contiguous, waiting on gaps.
- The compactor has its own `compactor_epoch`. **Snapshots/checkpoints** (with an expiry) pin
  manifests so GC can't delete SSTs that a reader still uses. This is the same problem as blowfish
  query nodes holding an old segment list.
- The code is factored into **`slatedb-txn-obj`** (published crate): `TransactionalObject`,
  `FenceableTransactionalObject` ("On `init`, it bumps the epoch field ... fencing out stale writers"),
  and `ObjectStoreSequencedStorageProtocol` (create-only sequenced versions), with a GET
  `If-None-Match` fast path for checking a cached version.
- RFC 0030 (pluggable WAL) states the core tension: "object store PUTs themselves take 10s of ms",
  and lowering the flush interval raises PUT cost. Readers and CDC have to poll.
- **What to borrow:** the contiguous-ID WAL with an epoch in each object, sequenced create-only
  manifests, epoch fencing for both the writer and the indexer, snapshot-pinned GC, and the
  put-id-verified retry. Possibly the crate itself.
- **Use SlateDB itself as blowfish's KV?** turbopuffer runs its attribute and posting indexes on an LSM
  KV over object storage, so SlateDB could serve that role (one DB per namespace, keys
  `(attr, value, cluster)` mapped to bitmaps). It's tempting, but it adds a second WAL and manifest
  under blowfish's own and ties it to a pre-1.0 format. I'd start with blowfish's own immutable segments and revisit.
  **(inference)**

### 3.2 Lance / LanceDB

- Columnar format with versioned manifests. Commits use put-if-not-exists or rename-if-not-exists, or
  an external manifest store (DynamoDB keyed on `base_uri`, `version`). Concurrent writers write
  `_transactions/` files, and conflicts are **rebased** where possible ("if the transaction file is
  missing, assume it conflicts").
- Indexes: IVF_PQ (default), **IVF_RQ (RaBitQ, 1 bit default, multi-bit option; dims divisible by 8)**,
  HNSW variants, an inverted FTS index, and scalar/bitmap indexes
  ([LanceDB quantization docs](https://docs.lancedb.com/indexing/quantization)). The vendor reports
  5-bit IVF_RQ at 96.2% recall@10 versus 74.8% for unrefined IVF_PQ.
- Borrow: transaction-file rebase for concurrent compaction versus writes, the IVF_RQ implementation
  (`lance-index`), and fragment and deletion-file ideas.

### 3.3 Quickwit / tantivy on S3

- Each **split** is a tantivy index plus a **hotcache** (<0.1% of split size, ~10MB) holding the
  footers, term-dictionary skip data, and fast-field headers. The hotcache lets a searcher open a split
  in "<60ms" even on high-latency storage. Searchers are stateless. The metastore is Postgres (or a
  JSON file on S3 for small setups) ([Quickwit 101](https://quickwit.io/blog/quickwit-101),
  [architecture](https://quickwit.io/docs/overview/architecture)).
- Borrow: **put everything a cold query needs for its first round trip into a small "hot" section**
  (the segment footer, centroids, block-max metadata, and term-dictionary index). Then use coalesced
  byte-range reads. Quickwit is append-only log search with no upserts, which is why it works
  differently from blowfish.

### 3.4 Chroma (distributed)

- Gateway, query nodes (serving indexes from object storage with an SSD cache), and compactor nodes
  that build HNSW or **SPANN** segments from the log and persist them to object storage. Built in Rust
  with Arrow.
- **wal3** ([trychroma.com/engineering/wal3](https://trychroma.com/engineering/wal3)): a log "built
  purely on object storage", one prefix per collection. Fragments are created with `If-None-Match`,
  then linked into a **manifest updated with `If-Match`** (a lock-free-queue construction). "A write
  must not be acknowledged until it is readable from the log." **Setsum** (an associative, commutative
  multiset checksum) checks that live plus garbage-collected data equals all data written, and it
  caught a GC bug. Borrow setsum-style invariants for blowfish's GC, the riskiest component.

### 3.5 WarpStream ([blog](https://www.warpstream.com/blog/minimizing-s3-api-costs-with-distributed-mmap))

- Stateless agents batch all partitions into one file per **250ms or 4 MiB** (P99 write about 400ms).
  Metadata lives in a separate control plane. A "distributed mmap" cache uses a consistent-hash ring
  over agents, **aligned 4 MiB chunks**, and deduplication of concurrent GETs, run once per AZ.
- Borrow: aligned-chunk caching with single-flight, consistent-hash routing of namespaces to query
  nodes (turbopuffer does the same for cache locality), and cost math (PUT count scales with
  throughput, not with the number of namespaces, if batching is per process rather than per namespace).
  **(inference)** With many tiny namespaces, per-namespace WAL objects cost PUTs; turbopuffer accepts
  this.

### 3.6 Neon

- Safekeepers (Paxos) hold the hot WAL, and pageservers upload layer files and `index_part.json` to S3.
  **Generation numbers** (above) give split-brain safety **without S3 atomics**. This is the canonical
  "no-CAS" design.

### 3.7 Milvus 2.6

- Replaced Kafka/Pulsar with **Woodpecker**, a "zero-disk" WAL with log data on object storage and
  metadata in **etcd**. Vendor-reported throughput: 750 MB/s direct-to-S3 versus 71 MB/s on MinIO.
  A separate streaming node handles fresh data while sealed segments are indexed. This is the same
  "fresh tail plus indexed segments" split, but with etcd as the coordinator, which blowfish avoids.

### 3.8 Turso / libsql bottomless

- A virtual WAL that streams SQLite WAL frames to S3 for backup and restore. libsql retired its native
  and durable WAL experiments in Mar 2025. This is backup-to-S3 rather than S3 as the source of truth,
  so there's little to borrow beyond frame batching.

### 3.9 Vespa

- Not object-storage-native (content nodes with local disk), but it's the best reference for
  **hybrid ranking**: HNSW combined with filters, with automatic fallback to exact search when the
  filter is very selective; multi-phase ranking (first-phase cheap, second-phase rerank); and
  `nativeRank`/BM25 combined with closeness. Borrow the planner rule: if the filter matches less than
  ~X% of docs, do exact kNN over the filtered set (turbopuffer's `kNN` is effectively this).

---

## 4. Rust libraries (versions from crates.io, 2026-11-14)

| Crate | Version | Recent dl (90d) | Notes |
|---|---|---|---|
| `object_store` | 0.14.2 | 31M | Apache Arrow project. S3/R2/MinIO (`ETagMatch`), GCS, Azure, local, memory. `PutMode::Create`/`Update`. `get_ranges` coalesces range reads. **No conditional multipart.** `LocalFileSystem` lacks `Update`. Breaks APIs between minors. |
| `opendal` | 0.59.5 | 7M | Alternative with more backends. `object_store` is the de-facto choice for DB engines (SlateDB, Lance, DataFusion). |
| `slatedb-txn-obj` | 0.18.1 | 0.9M | Manifest CAS and epoch fencing primitives. |
| `slatedb` | 0.18.1 | 1.7M | Embedded LSM on object storage. |
| `tantivy` | 0.27.0 | 5.5M | Mature BM25 engine. **Reusable pieces:** `tokenizer` (TextAnalyzer, SimpleTokenizer, LowerCaser, RemoveLongFilter, Stemmer, AsciiFoldingFilter, NgramTokenizer), `tantivy-tokenizer-api` (0.7), and **`query::Bm25Weight::for_one_term(doc_freq, total_docs, avg_fieldnorm)`** with `.score(fieldnorm_id, tf)` and `.max_score()`, which works standalone. tantivy's index format assumes a `Directory` with random reads and mmap. Quickwit makes it work on S3 with hotcache plus caching, but it's an immutable-segment append design that is awkward for upserts, deletes, and cluster-aligned addressing. **Recommend: tokenizers and the scorer only, custom postings.** |
| `bitpacking` | 0.9.3 | 7.2M | SIMD bitpacking (simdcomp port). turbopuffer uses it for postings. |
| `rust-stemmers` / `lindera` / `charabia` | 1.2 / 6.2 / 0.10 | — | Snowball stemmers, CJK morphology, and Meilisearch's multilingual segmenter. |
| `roaring` | 0.12.0 | 18.6M | Pure-Rust roaring bitmaps, with serialization compatible with the portable format. `croaring` 2.9 (C bindings) is faster for some operations. |
| `arrow` / `parquet` | 60.1.0 | 28M / 25M | Mature. Parquet for attribute columns (page index and row-group stats support skipping). Arrow IPC for in-memory batches. |
| `foyer` | 0.22.6 | 4.3M | Hybrid memory+disk cache (S3-FIFO/LRU, block engine on NVMe, admission policies). Used by RisingWave and SlateDB. Pre-1.0 API churn. Alternatives: `moka` (memory only, very mature), `quick_cache`. |
| `axum` / `tokio` | 0.8.9 / 1.54 | — | Standard choice. |
| `usearch` | 2.26.6 | 0.8M | C++ HNSW with Rust bindings. Fast, with f16/i8, filtering predicates, and mmap'd view of a serialized index. **In-memory graph; not object-storage friendly** (random access per hop). Good for a centroid index or small namespaces. |
| `hnsw_rs` | 0.3.5 | 0.6M | Pure-Rust HNSW, actively updated, with mmap of data. Same caveat. |
| `instant-distance` | 0.6.1 | — | Pure-Rust HNSW, **unmaintained since 2023**. Skip. |
| `diskann` (Microsoft) | 0.62.0 | 0.5M | **DiskANN3, now Rust** on `main` (MIT). In-memory and disk providers, Fresh/IP-DiskANN updates, PQ/scalar/spherical quantization, label filtering, and a pluggable `DataProvider` trait. The README calls the providers "illustrative examples". Graph traversal means a chain of dependent reads, which is bad on S3 and OK on NVMe. Worth evaluating as a per-segment NVMe-resident index later. |
| `rabitq-rs` | 0.9.0 | low | RaBitQ plus IVF and MSTG. A small project; use it as a reference implementation. `rabitq` (kemingy) 0.2 is stale since 2024. |
| `lance-index` | 14.0.2 | 2.7M | Production IVF_PQ / IVF_RQ / HNSW / inverted index code, coupled to Lance types. A good reference; extracting parts is non-trivial. |
| `faiss` | 0.13 | — | FFI to C++ FAISS. Heavy build. Useful only offline, for benchmarking recall. |
| `numkong` | 7.8.7 | 0.3M | Successor to SimSIMD (`simsimd` 6.5 last released Mar 2026): 2000+ SIMD kernels (x86, Arm, RISC-V), f16/bf16/i8 dot, cosine, L2, and Hamming. |
| `half` | 2.7.1 | — | f16/bf16 types. |
| `pulp` | 0.22 | — | Safe runtime-dispatched SIMD, if I write custom kernels. |
| `memmap2`, `rkyv` | 0.9 / 0.8 | — | mmap of NVMe cache files, and zero-copy segment metadata. |

### ANN choice: IVF/SPFresh versus graph indexes on object storage

- **IVF / centroid trees (SPANN, SPFresh, turbopuffer):** a query is "read the centroids, then fetch K
  posting lists in one parallel batch". Round trips are bounded by tree depth (2 to 3), reads are
  large and sequential (they coalesce well in byte-range GETs and 4 KiB NVMe pages), scanning is
  SIMD-friendly, and with RaBitQ codes plus a rerank of <1% it's bandwidth-efficient. Incremental
  updates through split/merge/reassign (SPFresh LIRE). Filters map naturally to "clusters with a
  match".
- **Graphs (HNSW, DiskANN/Vamana):** better recall per distance computation in RAM. But each hop
  depends on the previous one (tens to hundreds of sequential reads), which is fatal against 60 to
  100ms S3 GETs and only acceptable from NVMe or RAM. Deletes and filters are harder. turbopuffer argues
  graphs are harder to vectorize.
- **Plan:** v1 builds a two-level IVF per segment (centroids in the segment hot section, clusters as
  contiguous byte ranges), stores RaBitQ 1-bit codes next to f16/f32 vectors, and reranks with full
  precision. Merge segments during compaction by re-clustering. v2 moves to a namespace-wide SPFresh
  tree with LIRE updates once segment fan-out hurts. The HNSW crates (`usearch`) can index the
  **centroids** of very large namespaces.

---

## 5. Sketch of the object layout (for discussion)

```
<bucket>/<prefix>/ns/<namespace>/
  manifest/00000000000000000042.manifest   # create-only; epochs, cursor, segments, schema
  wal/00000000000000001234.wal             # create-only; one group commit; writer_epoch + put-id in header
  seg/<ulid>/hot.bin                       # footer: centroids, cluster offsets, term dict index, block-max meta, attr summaries
  seg/<ulid>/vec.bin                       # clusters: [rabitq codes | f16 vectors | ids], contiguous per cluster
  seg/<ulid>/fts-<attr>.bin                # posting blocks (128-512 postings, bitpacked, block-max)
  seg/<ulid>/attrs.parquet                 # columnar attributes
  seg/<ulid>/filters.bin                   # roaring bitmaps per (attr,value[,cluster]) plus cluster-level summaries
  seg/<ulid>/deletes.roaring               # tombstones applied after segment creation (or kept in manifest)
```

- Writer: fence through the manifest epoch, then append WAL slots with create-if-absent and verify
  put-ids on conflict.
- Indexer: fence with an indexer epoch. Build a segment from WAL range `[a, b]`, upload it, then CAS a
  manifest that adds the segment and sets `indexed_through = b`.
- GC: delete WAL objects and segments only when no non-expired manifest snapshot references them.
  Track them with setsum-style accounting (wal3).
- Query: get the manifest (cached; conditional GET), then `HEAD wal/<next>` for a strong read. In
  parallel, fetch the segment hot sections and the WAL tail. Then fetch clusters, postings, and
  bitmaps with coalesced range GETs, and merge with latest-seq-wins plus tombstones.
