# Configuration

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
| `storage.warm_connections` | — | `BF_WARM_CONNECTIONS` | `256` (S3 only; `0` disables) |
| `engine.indexing` | `--indexing` | `BLOWFISH_INDEXING` | `true` |
| `engine.index_rows` | `--index-rows` | `BLOWFISH_INDEX_ROWS` | `1000` |
| `engine.index_bytes` | `--index-bytes` | `BLOWFISH_INDEX_BYTES` | `8388608` |
| `engine.index_age_ms` | `--index-age-ms` | `BLOWFISH_INDEX_AGE_MS` | `5000` |
| `engine.index_interval_ms` | `--index-interval-ms` | `BLOWFISH_INDEX_INTERVAL_MS` | `250` |
| `engine.compaction_segments` | `--compaction-segments` | `BLOWFISH_COMPACTION_SEGMENTS` | `4` |
| `engine.retention_ms` | `--retention-ms` | `BLOWFISH_RETENTION_MS` | `3600000` |
| `engine.lease_duration_ms` | `--lease-duration-ms` | `BLOWFISH_LEASE_DURATION_MS` | `30000` |
| `engine.max_unindexed_bytes` | `--max-unindexed-bytes` | `BLOWFISH_MAX_UNINDEXED_BYTES` | Memory limit / 8, within 128 MiB to 2,000,000,000 |
| `engine.eventual_tail_bytes` | `--eventual-tail-bytes` | `BLOWFISH_EVENTUAL_TAIL_BYTES` | `134217728` |
| `engine.ann_nprobe` | `--ann-nprobe` | `BLOWFISH_ANN_NPROBE` | `64` |
| `engine.ann_rerank_factor` | `--ann-rerank-factor` | `BLOWFISH_ANN_RERANK_FACTOR` | `32` |
| `engine.recall_sample_rate` | `--recall-sample-rate` | `BLOWFISH_RECALL_SAMPLE_RATE` | `0.01` |
| `storage.cache.memory_bytes` | `--cache-memory-bytes` | `BLOWFISH_CACHE_MEMORY_BYTES` | Host/container limit / 4, capped at 8 GiB |
| `storage.cache.disk_bytes` | `--cache-disk-bytes` | `BLOWFISH_CACHE_DISK_BYTES` | `68719476736` |
| `storage.cache.disk_dir` | `--cache-disk-dir` | `BLOWFISH_CACHE_DISK_DIR` | Disabled |
| `storage.cache.namespace` | `--cache-namespace` | `BLOWFISH_CACHE_NAMESPACE` | Automatic |
| `storage.cache.block_size` | `--cache-block-size` | `BLOWFISH_CACHE_BLOCK_SIZE` | `262144` |
| `storage.cache.max_coalesce_blocks` | `--cache-max-coalesce-blocks` | `BLOWFISH_CACHE_MAX_COALESCE_BLOCKS` | `32` |
| `storage.cache.coalesce_gap_blocks` | `--cache-coalesce-gap-blocks` | `BLOWFISH_CACHE_COALESCE_GAP_BLOCKS` | `0` |
| `storage.cache.fetch_concurrency` | `--cache-fetch-concurrency` | `BLOWFISH_CACHE_FETCH_CONCURRENCY` | `512` |
| `storage.cache.prefetch_concurrency` | `--cache-prefetch-concurrency` | `BLOWFISH_CACHE_PREFETCH_CONCURRENCY` | `4` |
| `storage.cache.hedge_budget` | — | `BF_HEDGE_BUDGET` | `0.15` (`0` disables hedged ranged reads) |

With a disk cache directory and an S3-compatible backend, `storage.cache.disk_dir` also holds staged recluster-tier routing and code files (`ann-staging-v1/`), budgeted at the smaller of 20 GiB and a quarter of free disk, separately from `disk_bytes`. See [operations](operations.md#running-under-systemd-with-caddy) (Local ANN staging).

### Models (native embedding and reranking)

`[models."<name>"]` tables map the model names clients use (for example `google/embeddinggemma-2`, or
turbopuffer's own names) to a provider. `BLOWFISH_MODELS_JSON` holds a JSON object of the same shape and is
merged over the file. With no entries, `Embed` and model `rerank_by` return a 400. Every field is documented in
[models.md](models.md). `provider = "local"` runs a GGUF model in-process and needs a build with
`--features local-models`. Related environment variables: `BLOWFISH_MODELS_CACHE_DIR` (default
`~/.cache/blowfish/models`), `HF_ENDPOINT` (default `https://huggingface.co`), `HF_TOKEN` for gated files, and
`BLOWFISH_LLAMA_LOGS` (when set, enables verbose `llama.cpp` stderr logging).

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
`BF_INDEX_INTERVAL_MS`, `BF_MAX_UNINDEXED_BYTES`, and `BF_ANN_NPROBE` variables remain aliases in `blowfish serve`.
Flags and `BLOWFISH_*` variables take precedence over aliases, which take precedence over TOML.
Set `--indexing false` to disable background indexing. Recall sampling must be between 0 and 1.

### Memory sizing

blowfish links jemalloc, with background purging, as its allocator. Where memory matters, these defaults follow the
memory limit (host RAM, or the cgroup limit when lower):

| What | Default | Override |
| --- | --- | --- |
| Unindexed WAL tail (write backpressure) | limit / 8, 128 MiB to 2 GB | `engine.max_unindexed_bytes`, `--max-unindexed-bytes`, `BLOWFISH_MAX_UNINDEXED_BYTES`, `BF_MAX_UNINDEXED_BYTES` |
| Block cache in memory | limit / 4, up to 8 GiB | `storage.cache.memory_bytes`, `--cache-memory-bytes`, `BLOWFISH_CACHE_MEMORY_BYTES` |
| Tail bytes at which a bulk load flushes | three quarters of `max_unindexed_bytes` | `BF_BULK_FLUSH_BYTES` |
| Segments a bulk load accumulates before compaction runs while writes arrive | 1,024 | `BF_DEFER_COMPACTION_SEGMENTS` |
| Rows in a built or compacted segment | limit / 80,000, 25,000 to 200,000 | `BF_MAX_SEGMENT_ROWS` |
| Rows accumulated before a bulk flush | the smaller of 100,000 and the segment cap | `BF_BULK_FLUSH_ROWS` |
| Segment builds at once | 1 below 8 GiB, otherwise 2 | `BF_BUILD_CONCURRENCY` |
| Decoded metadata cache | limit / 4, 512 MiB to 4 GiB | `BF_DECODED_CACHE_BYTES` |

The decoded cache budget covers all decoded data, in three parts: decoded segment metadata (two thirds), decoded ANN
directory clusters (a quarter), and decoded document column blocks (a twelfth). Together they stay within
`BF_DECODED_CACHE_BYTES`.

`blowfish serve` reads every `BF_*` engine variable above, plus `BF_INDEX_BYTES`, `BF_MAX_SEGMENT_BYTES`, `BF_BULK_FLUSH_AGE_MS`, `BF_WAL_PIPELINE`, `BF_RETENTION_SECS`, `BF_ANN_RERANK`, and `BF_EXACT_FETCH`. CLI flags and TOML keys take precedence over them. `BF_BULK_FLUSH_BYTES` follows `max_unindexed_bytes` (three quarters of it) unless set explicitly.

Memory detection: every memory-sized default, the block cache included, reads one limit: the smaller of host RAM and the process's own cgroup (`/proc/self/cgroup` -> `/sys/fs/cgroup<path>/memory.max`, then `/sys/fs/cgroup/memory.max`). On macOS host RAM comes from `hw.memsize`. When nothing can be read it assumes 4 GiB.

The row caps assume 1,024-dimension vectors. Builds and compactions keep vectors packed at their stored precision (2 KB per f16 row)
and normalize on the fly, compaction and `recluster` reads bypass the in-memory block cache, and the text statistics object is
merged by streaming, so none of these terms grows with the namespace. The automatic recluster sizes its slabs from the same limit: what is left after the block cache, the decoded cache, the
unindexed limit, and a running segment build, at 12 KB per row, clamped to 50,000–700,000. It defers when fewer than
50,000 rows fit. Diagnostics:
`BF_MEM_LOG_SECS=N` prints allocator and RSS statistics every N seconds, `/metrics` exposes
`blowfish_alloc_bytes{kind}` (allocated, active, resident, and retained), and a build with `--features heap-profile` writes jemalloc
heap profiles when run with `_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19` and `BF_HEAP_DUMP_DIR=DIR` (interval `BF_HEAP_DUMP_SECS`, default 30).

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
copy (375 KB instead of 3.4 MB at 100M rows). A query that ranks by text, and every writer and indexer, read the full manifest
slot instead (one more round trip for a cold text query), and a server holding the compact copy replaces it before
it publishes anything. The manifest slots are unchanged, so older binaries read these namespaces as before (their cold
text queries read the footers). Namespaces published before this existed keep their full copy until their next
publication, or until you run
`cargo run --release -p bf-engine --example readercopy -- storage.json NAMESPACE`.

| What | Default | Variable |
| --- | --- | --- |
| Smallest manifest (encoded bytes) that gets a compact copy | 262,144 | `BF_READER_COPY_MIN_BYTES` |

### Paged ANN directory (large reclustered namespaces)

The automatic recluster writes the slab directory twice: whole (for older readers) and, once it lists at least 100,000 clusters, as a
paged copy. A cold query reads the paged copy's small header (segment table, one one-bit code per cell, and a page table) and
then only the pages of the cells nearest the query, instead of the whole directory (about 100 MB at 100M rows). Queries
rank only the clusters in those pages, so warm queries also skip ranking every cluster. All of these are environment
variables; the defaults suit 1,024-dimension vectors.

| What | Default | Variable |
| --- | --- | --- |
| Cells' entries a query reads, as a multiple of the clusters it will probe (higher: more recall, more bytes) | 16 | `BF_ANN_PAGE_MARGIN` |
| How strongly a cell's spread pulls it up the ranking (see `PagedDirectory::select_cells`) | 0.5 | `BF_ANN_PAGE_KAPPA` |
| Entries per cell when the recluster builds the paged copy | 24 | `BF_ANN_PAGE_ENTRIES` |
| Smallest directory (in clusters) that gets a paged copy | 100,000 | `BF_ANN_PAGED_MIN_ENTRIES` |

### Automatic recluster

The indexer reclusters a namespace in the background when its layout would gain from it, and installs the new tier in
its next publication (see [operations](operations.md#large-namespaces-automatic-recluster-and-compact-manifest-copy)).
It is always on and has no settings: a base and at most one delta tier, 32 coarse cells (shared by the base and its
delta), leaves of about 200 rows, half the cores, and slabs sized from memory as above. A delta is built once the rows
outside the tiers reach 4% of the base (and a quarter of the delta), and merges into a new base at half of it, or once
deletes and updates mask a fifth of the tiers' rows. Its spill file goes in the disk cache's directory (`storage.cache.disk_dir`), or the system's temporary
directory when there is no disk cache.

### Recluster leaf assignment

The recluster partitions the rows into coarse cells and trains each cell's leaf clusters on a sample of its rows. A cell
with more than 1,024 leaves trains two-level (about the square root of the leaves as parents, then each parent's share of
leaves); a smaller one trains one-level and groups its leaves by a small k-means. Each row then joins its nearest leaf
among the leaves of its four nearest cells, not only its own cell's, searching the 12 nearest leaf groups. Every
leaf center is then recomputed once from all of its rows and the rows assigned again, so rows near a cell boundary sit
with their true neighbors. There are no settings: this is the only assignment.

On BEIR NQ (2.68M rows), recall@10 at 64 probes is 0.961 at the default 32 cells (0.955 with each cell clustered on its
own), and 0.961 with the rows forced into 8 cells (about 1,700 leaves a cell, the size of a 10M layout's cells; 0.950 on
its own). The 8-cell job took about the same time and memory as the per-cell assignment it replaced: see
[Iteration 21](benchmarks.md#iteration-21-one-recluster-assignment-at-every-size-and-a-lighter-query-path-beir-nq-one-machine),
which also estimates the cost at 100M.

### ANN probes and query concurrency

The probe count is `ann_nprobe` (default 64; `engine.ann_nprobe`, `--ann-nprobe`, `BLOWFISH_ANN_NPROBE`, or `BF_ANN_NPROBE`)
per square root of the rows' worth of clusters. That scaling assumes one clustering of all the rows, which a reclustered
namespace has. A namespace as loaded has one independent clustering per segment, so the neighbors of a query spread
over every segment and the same probe count finds fewer of them. On BEIR NQ (2.68M rows in 25 segments), recall@10 was 0.907 at 64
probes as loaded, against 0.954 reclustered. When `ann_nprobe` is left at its default, an unreclustered layout probes
`min(S^0.3, 4)` times as many clusters. `S = (sum of rows)^2 / sum of rows^2` is the effective number of equal segments,
so small tail segments barely count. Examples: 25 equal segments give 2.6 times the probes, a 10M namespace of 70 segments
about 3.5 times, and 100 segments or more 4 times. Reclustered (slab) layouts are never scaled, and neither is a probe
count you set yourself: an explicit `ann_nprobe` is used as given. There is no per-query probe parameter.

The cost: each extra probed cluster is one more cluster blob and code block read, and on a cold query one more object
store range read (adjacent clusters in the block layout are coalesced). Scanned rows and cold bytes therefore scale with the
multiplier, at most 4 times. On NQ as loaded, warm p50 went from 5.3 to 9.4 ms and p99 from 10.5 to 29.4 ms, for recall@10
from 0.906 to 0.958. Reclustering removes the cost: it gets the recall at the base probe count.

| What | Default | Variable |
| --- | --- | --- |
| Queries the engine runs at once | twice the available CPUs, at least 8 | `BF_QUERY_CONCURRENCY` |
| Segment searches one query runs at once | 8 | `BF_SEARCH_CONCURRENCY` |
| Queries in flight up to which one query fans its slab work out over the thread pool (more run each query on its own thread) | half the CPUs | `BF_FANOUT_QUERIES` |
| Least time between two sampled recall comparisons (the first waits this long after startup, and a comparison that took `t` delays the next by at least `50 × t`) | 60,000 ms | `BF_RECALL_SAMPLE_INTERVAL_MS` |
| Cache block size for scattered small reads (rerank vectors) | 16,384 | `BF_SPARSE_BLOCK_BYTES` |

Warm queries run on a fixed pool of query CPU workers: `N - 1` workers when the process has `N >= 8` CPUs, and `N` otherwise
(at least one), so the async runtime keeps a core free to deliver completions and responses under sustained load. A warm
plan gets one poll on this pool; a plan that has to wait for I/O continues on the I/O path. Cold snapshots use the I/O path.
On 8 CPUs this cut warm NQ p90 by 8-11% with throughput 2-3% higher (`docs/benchmarks.md`, "Round Q3"). There is no
setting for the pool size.

On x86-64 the RaBitQ scan uses AVX-512 (`VPOPCNTQ`) when the CPU has it; on arm64 it uses NEON (`CNT` and pairwise adds).
Both give bit-identical results to the portable path.

Indexes built since iteration 20 store their one-bit codes in a fast-scan layout: transposed in blocks of 32 rows, scored
with 4-bit table lookups (byte shuffles on AVX-512BW, `TBL` on NEON, a scalar loop elsewhere, all giving the same sums).
That is about 3 times faster per row on x86-64 and 5 times on Apple M4. Indexes in the older row-major layout are still
read and scanned as before, and the background indexer upgrades them to the new layout.

A running server upgrades older-layout indexes to this layout in the background; `blowfish reindex NAMESPACE` (or
`--all`, with `--dry-run` to report counts and bytes per layout) does the same upgrade now. It takes the storage flags and
variables of `serve` (`--config`, `--storage-url`, `--storage-endpoint`, `--storage-region`, `--storage-profile`). See
"Bucket format 3" in [operations](operations.md).

### Connection warming, hedging, and diagnostics

These environment-only switches are read directly by the storage and engine crates (including under `blowfish serve`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `BF_WARM_CONNECTIONS` | adaptive (starts at `storage.warm_connections`, `256`) | Fix the S3 warm connection pool size (`0` disables connection warming) |
| `BF_WARM_FLOOR` | `16` | Minimum idle S3 connections kept warm after an idle window |
| `BF_HEDGE_BUDGET` | `storage.cache.hedge_budget` (`0.15`) | Extra-request fraction for hedged ranged GETs (`0` disables). A read is duplicated when it lags the reads that started around it (any size), or, with too few siblings, when it outlasts the observed p90 (up to 1 MiB) |
| `BF_READ_GAP_KB` | layout default | Override read range-merge gap in KiB |
| `BF_TRACE_READS` | off | Print each backend read and its timing to stderr |
| `BF_TRACE_STAGES` | off | Print per-stage query and indexer timings to stderr |

### Removed settings

Following SPEC decision 25 (one way to do each thing), these environment variables are gone. Each one only chose an
older or slower path, and setting it now has no effect:

| Variable | What it did | Now |
| --- | --- | --- |
| `BF_RECLUSTER_GLOBAL` | Chose cross-cell or per-cell leaf assignment in `recluster` | Cross-cell assignment is the only one |
| `BF_RECLUSTER_GROUPS` | Leaf groups searched per row during assignment | Fixed at 12 |
| `BF_RECLUSTER_REFINE` | Refinement rounds after assignment | Exactly one |
| `BF_ANN_DIRECTORY` | `0` forced the per-segment ANN path | Directories are always used when present |
| `BF_ANN_BLOCKS` | `0` turned off the block layout for unfiltered queries | One `.bfa` object serves filtered and unfiltered queries |
| `BF_ANN_SLABS` | `0` ignored a recluster's slabs | A recluster tier is always used for the rows it covers |
| `BF_ANN_PAGED` | `0` read the whole slab directory | Paged directories are always read in pieces |
| `BF_ANN_SCAN_MEMO` | `0` turned off scanning while a cold query's reads arrive | Always on |
| `BF_ANN_SPECULATE` | `0` waited for the manifest before starting scan reads | Always on |
| `BF_READER_COPY` | `0` made ANN queries read the full manifest | The compact copy is always read |
| `BF_ANN_SEGMENT_SPREAD` | Exponent of the as-loaded probe multiplier | Fixed at 0.3 |
| `BF_IVF_CLUSTER_FACTOR` | Made index-build leaf clusters larger | Leaf clusters are sqrt(rows) |
| `BF_NO_AVX512` | Turned off the AVX-512 scan kernels | AVX-512 is used whenever the CPU has it |
| `BF_ANN_FASTSCAN` | Chose the row-major code layout (removed with the reindex work) | Fast-scan is the only layout written |

### Single ANN objects and upgrades

I write one `.bfa` ANN object per vector attribute in each segment or slab; there is no layout setting.
`blowfish reindex` runs the same automatic upgrade as the background indexer, and `--dry-run` reports
legacy row-major, legacy fast-scan, legacy other, and current single-object counts and bytes. Even a
legacy fast-scan or flat index needs the object upgrade. Existing flat and paged slab directory sizing
settings keep their meaning. The deployment marker and manifest version are 3; I stop older writers
before starting an upgrade, and restore a complete prefix backup if I need to roll back.


### Document-v2 storage

I write document-v2 alongside the single ANN layout, under bucket and manifest generation 3. Dense
vectors live in ANN objects; string and string-array document blocks use zstd level 3. Compression
is automatic, with no setting or alternate writer. Other attribute blocks retain their JSON encoding.
Vector projection reads checksummed ANN ranges, and compaction streams whole leaves at stored precision.
`BF_SPARSE_BLOCK_BYTES` controls the same small cache blocks for vector projection as for reranking.

The background format upgrade converts ANN objects first, then rewrites one document/ID pair per fenced
publication. `blowfish reindex` runs the same upgrade and reports pending legacy document counts and bytes,
alongside ANN layout counts. I retain the separate `idfilter.bff` for existence checks; `ids.bfi` no longer
stores its older, redundant Bloom filter. I stop older writers before upgrading and keep a complete prefix
backup for rollback, as described in [operations](operations.md).
