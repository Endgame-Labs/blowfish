# Configuration 🐡

Run `blowfish serve --config blowfish.toml`. The complete annotated template is
[`blowfish.example.toml`](../blowfish.example.toml). No config file is loaded implicitly. Set a storage
URL through a file, environment variable, or flag; it has no implicit host directory default.

Precedence is CLI flags, then environment variables, then TOML. The `--read-only`, `--single-writer`, and `--require-auth` flags and their environment
variables enable their settings; they do not disable a true value in the file. Engine `--indexing`
accepts an explicit `true` or `false`. Unknown top-level TOML keys
are errors. Nested storage settings currently follow `bf-storage`'s parser.

| TOML key | Flag | Environment | Default |
| --- | --- | --- | --- |
| `listen` | `--listen` | `BLOWFISH_LISTEN` | `0.0.0.0:8000` |
| `admin_key` | `--admin-key` | `BLOWFISH_ADMIN_KEY` | Auth disabled, with a startup warning |
| `require_auth` | `--require-auth` | `BLOWFISH_REQUIRE_AUTH` | `false` |
| `metrics_listen` | `--metrics-listen` | `BLOWFISH_METRICS_LISTEN` | Disabled |
| `storage.url` | `--storage-url` | `BLOWFISH_STORAGE_URL` | Required |
| `storage.endpoint` | `--storage-endpoint` | `BLOWFISH_STORAGE_ENDPOINT` | Backend default |
| `storage.region` | `--storage-region` | `BLOWFISH_STORAGE_REGION` | AWS provider default |
| `storage.profile` | `--storage-profile` | `BLOWFISH_STORAGE_PROFILE` | AWS provider chain |
| `storage.read_only` | `--read-only` | `BLOWFISH_READ_ONLY` | `false` |
| `storage.single_writer` | `--single-writer` | `BLOWFISH_SINGLE_WRITER` | `false` |

| `engine.indexing` | `--indexing` | `BLOWFISH_INDEXING` | true |
| `engine.index_rows` | `--index-rows` | `BLOWFISH_INDEX_ROWS` | 1000 |
| `engine.index_bytes` | `--index-bytes` | `BLOWFISH_INDEX_BYTES` | 8388608 |
| `engine.index_age_ms` | `--index-age-ms` | `BLOWFISH_INDEX_AGE_MS` | 5000 |
| `engine.index_interval_ms` | `--index-interval-ms` | `BLOWFISH_INDEX_INTERVAL_MS` | 250 |
| `engine.compaction_segments` | `--compaction-segments` | `BLOWFISH_COMPACTION_SEGMENTS` | 4 |
| `engine.retention_ms` | `--retention-ms` | `BLOWFISH_RETENTION_MS` | 3600000 |
| `engine.lease_duration_ms` | `--lease-duration-ms` | `BLOWFISH_LEASE_DURATION_MS` | 30000 |
| `engine.max_unindexed_bytes` | `--max-unindexed-bytes` | `BLOWFISH_MAX_UNINDEXED_BYTES` | memory limit / 8, within 128 MiB to 2000000000 |
| `engine.eventual_tail_bytes` | `--eventual-tail-bytes` | `BLOWFISH_EVENTUAL_TAIL_BYTES` | 134217728 |
| `engine.ann_nprobe` | `--ann-nprobe` | `BLOWFISH_ANN_NPROBE` | 64 |
| `engine.ann_rerank_factor` | `--ann-rerank-factor` | `BLOWFISH_ANN_RERANK_FACTOR` | 32 |
| `engine.recall_sample_rate` | `--recall-sample-rate` | `BLOWFISH_RECALL_SAMPLE_RATE` | 0.01 |
| `storage.cache.memory_bytes` | `--cache-memory-bytes` | `BLOWFISH_CACHE_MEMORY_BYTES` | host/container limit / 4, capped at 8 GiB |
| `storage.cache.disk_bytes` | `--cache-disk-bytes` | `BLOWFISH_CACHE_DISK_BYTES` | 68719476736 |
| `storage.cache.disk_dir` | `--cache-disk-dir` | `BLOWFISH_CACHE_DISK_DIR` | Disabled |
| `storage.cache.namespace` | `--cache-namespace` | `BLOWFISH_CACHE_NAMESPACE` | automatic |
| `storage.cache.block_size` | `--cache-block-size` | `BLOWFISH_CACHE_BLOCK_SIZE` | 1048576 |
| `storage.cache.max_coalesce_blocks` | `--cache-max-coalesce-blocks` | `BLOWFISH_CACHE_MAX_COALESCE_BLOCKS` | 8 |
| `storage.cache.fetch_concurrency` | `--cache-fetch-concurrency` | `BLOWFISH_CACHE_FETCH_CONCURRENCY` | 16 |
| `storage.cache.prefetch_concurrency` | `--cache-prefetch-concurrency` | `BLOWFISH_CACHE_PREFETCH_CONCURRENCY` | 4 |

### Models (native embedding and reranking)

`[models."<name>"]` tables map the model names clients use (for example `google/embeddinggemma-2`, or
turbopuffer's own names) to a provider. `BLOWFISH_MODELS_JSON` holds a JSON object of the same shape and is
merged over the file. With no entries, `Embed` and model `rerank_by` return a 400. Every field is documented in
[models.md](models.md). `provider = "local"` runs a GGUF model in-process and needs a build with
`--features local-models`. Related environment variables: `BLOWFISH_MODELS_CACHE_DIR` (default
`~/.cache/blowfish/models`), `HF_ENDPOINT` (default `https://huggingface.co`), and `HF_TOKEN` for gated files.

`BLOWFISH_ADMIN_KEY` is the deployment's **break-glass** key, with org superadmin powers and never stored in
the bucket. Mint an org superadmin key with it (see [auth.md](auth.md)), and use that key, not this one, for
programs and the admin webapp. Projects need no configuration: they are created through the admin API, and
their namespaces live under `<storage-prefix>/projects/<id>/`. The admin webapp has its own environment
variables, listed in [`webapp/README.md`](../webapp/README.md).

`--config PATH` selects the file. The legacy top-level `storage_url` aliases `storage.url`; do not
set both. `RUST_LOG` controls tracing, defaulting to `blowfish=info,bf_engine=info`.

Storage schemes implemented today are `file:///absolute/path`, `s3://bucket/optional-prefix`, and
`memory://`. Memory storage is disposable. Local directories are created if necessary. S3 buckets
must already exist. For an S3-compatible service, set `storage.endpoint` and `storage.region`.
An `http://` endpoint enables unencrypted backend HTTP for local testing.

S3 authentication uses the object-store AWS provider chain, including `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and supported workload identities. An explicit
`storage.profile` reads that profile from `AWS_SHARED_CREDENTIALS_FILE` or `~/.aws/credentials`.
Pass credentials through your deployment's secrets mechanism; keep them out of repository files,
URLs, and command history. Status responses strip URL credentials, query strings, and fragments.

The container defaults to `file:///data` and `0.0.0.0:8000`, runs as UID/GID `10001`, and uses `/data`
as a volume. Bind mounts need ownership or write permission for that UID. Its healthcheck calls
`/` on port 8000 with `BLOWFISH_ADMIN_KEY`; use that environment variable for authenticated container
deployments, and override the healthcheck if you change the internal port or configure the key only
in a file. Compose publishes ports on loopback by default.

The `keys` CLI has separate client settings: `--url` / `BLOWFISH_URL` (default
`http://127.0.0.1:8000`), `--admin-key` / `BLOWFISH_ADMIN_KEY`, and `--json`. See
[authentication](auth.md). SIGINT or SIGTERM initiates graceful HTTP shutdown.

Durations ending in `_ms` are milliseconds. The legacy `BF_INDEX_ROWS`, `BF_INDEX_AGE_MS`,
`BF_INDEX_INTERVAL_MS`, `BF_MAX_UNINDEXED_BYTES`, and `BF_ANN_NPROBE` variables remain aliases.
Flags and `BLOWFISH_*` variables take precedence over aliases, which take precedence over TOML.
Set `--indexing false` to disable background indexing. Recall sampling must be between 0 and 1.

### Memory sizing

blowfish links jemalloc, with background purging, as its allocator. Where memory matters, these defaults follow the
memory limit (host RAM, or the cgroup limit when lower), and each has an override:

| What | Default | Override |
| --- | --- | --- |
| Unindexed WAL tail (write backpressure) | limit / 8, 128 MiB to 2 GB | `BF_MAX_UNINDEXED_BYTES` |
| Tail bytes at which a bulk load flushes | three quarters of the unindexed limit | `BF_BULK_FLUSH_BYTES` |
| Segments a bulk load accumulates before compaction runs while writes arrive | 1,024 | `BF_DEFER_COMPACTION_SEGMENTS` |
| Rows in a built or compacted segment | limit / 80,000, 25,000 to 200,000 | `BF_MAX_SEGMENT_ROWS` |
| Rows accumulated before a bulk flush | the smaller of 100,000 and the segment cap | `BF_BULK_FLUSH_ROWS` |
| Segment builds at once | 1 below 8 GiB, otherwise 2 | `BF_BUILD_CONCURRENCY` |
| Decoded metadata cache | limit / 8, 512 MiB to 4 GiB | `BF_DECODED_CACHE_BYTES` |
| Block cache in memory | limit / 4, up to 8 GiB | `storage.cache.memory_bytes` |

The row caps assume 1,024-dimension vectors. Builds and compactions keep vectors packed at their stored precision (2 KB per f16 row)
and normalize on the fly, compaction and `recluster` reads bypass the in-memory block cache, and the text statistics object is
merged by streaming, so none of these terms grows with the namespace. `recluster` sizes `max_slab_rows` at 12 KB per row
(about 350,000 on a 4 GB machine). Diagnostics:
`BF_MEM_LOG_SECS=N` prints allocator and RSS statistics every N seconds, `/metrics` exposes
`blowfish_alloc_bytes{kind}` (allocated, active, resident, retained), and a build with `--features heap-profile` writes jemalloc
heap profiles when run with `_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19` and `BF_HEAP_DUMP_DIR=DIR`.

### Allocator decay: short while writing, long while only querying

jemalloc returns freed pages to the OS after a decay time. A short decay keeps RSS flat while an ingest or a build
churns through gigabytes; a long one avoids a page fault for each fresh 4 KB of the 100 MB or so that every ANN query
reads. blowfish switches between the two by itself: it uses the short decay while a write, a segment build or
compaction, or a `recluster` is in flight and for a while after, and the long one otherwise (a query-only or idle server).
A process starts with the short decay and settles to the long one after the linger time. Setting a decay in
`_RJEM_MALLOC_CONF` turns the switching off.

| What | Default | Variable |
| --- | --- | --- |
| Dirty and muzzy page decay while writes or builds run | 1,000 ms | `BF_BUSY_DECAY_MS` |
| Decay while only queries run | 30,000 ms | `BF_IDLE_DECAY_MS` |
| How long the short decay stays after the last write or build ends | 15,000 ms | `BF_DECAY_LINGER_MS` |

The rayon pool is sized by the machine (rayon's default, the CPUs the process may use, which honors a cgroup quota).
Set `RAYON_NUM_THREADS` to override it.

### Compact manifest copy for cold queries

A manifest of a large namespace is mostly per-segment text headers (the dictionary chunk index of each full-text
segment: about 6.6 of 9.8 MB of JSON at 100M rows). Only BM25 and full-text filters read them, and a segment without
one reads its own footer instead. When a publication's manifest is larger than 256 KB and the text headers are over
half of it, blowfish writes the fixed-key reader copy (`manifest-latest`) without them. A cold ANN query reads that
copy (370 KB instead of 3.4 MB at 100M rows). A query that ranks by text, and every writer and indexer, read the full manifest
slot instead (one more round trip for a cold text query), and a server holding the compact copy replaces it before
it publishes anything. The manifest slots are unchanged, so older binaries read these namespaces as before (their cold
text queries read the footers). Namespaces published before this existed keep their full copy until their next
publication, or until you run
`cargo run --release -p bf-engine --example readercopy -- storage.json NAMESPACE`.

| What | Default | Variable |
| --- | --- | --- |
| Smallest manifest (encoded bytes) that gets a compact copy | 262,144 | `BF_READER_COPY_MIN_BYTES` |
| Read the compact copy for ANN queries (set to `0` to read the full manifest, for A/B runs) | on | `BF_READER_COPY` |

### Paged ANN directory (large reclustered namespaces)

`recluster` writes the slab directory twice: whole (for older readers) and, once it lists at least 100,000 clusters, as a
paged copy. A cold query reads the paged copy's small header (segment table, one one-bit code per cell, a page table) and
then only the pages of the cells nearest the query, instead of the whole directory (about 100 MB at 100M rows). Queries
rank only the clusters in those pages, so warm queries also skip ranking every cluster. All of these are environment
variables; the defaults suit 1,024-dimension vectors.

| What | Default | Variable |
| --- | --- | --- |
| Read paged directories (set to `0` to read the whole slab directory, for A/B runs) | on | `BF_ANN_PAGED` |
| Cells' entries a query reads, as a multiple of the clusters it will probe (higher: more recall, more bytes) | 16 | `BF_ANN_PAGE_MARGIN` |
| How strongly a cell's spread pulls it up the ranking (see `PagedDirectory::select_cells`) | 0.5 | `BF_ANN_PAGE_KAPPA` |
| Entries per cell when `recluster` (or `repage`) builds the paged copy | 24 | `BF_ANN_PAGE_ENTRIES` |
| Smallest directory (in clusters) that gets a paged copy | 100,000 | `BF_ANN_PAGED_MIN_ENTRIES` |
| Scan each slab's clusters while a cold query's other reads are still arriving (set to `0` for A/B runs) | on | `BF_ANN_SCAN_MEMO` |
| Start a cold query's directory pages and scan reads from the hint, before the manifest arrives (set to `0` for A/B runs) | on | `BF_ANN_SPECULATE` |

`cargo run --release -p bf-engine --example repage -- storage.json NAMESPACE ATTRIBUTE` adds a paged copy to a namespace
that was reclustered before paging existed, without rebuilding any slab.
