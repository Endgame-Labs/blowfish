# bf-storage 🐡

Object storage, conditional creates, immutable range caching, and backend metrics.

## Cache API

```rust,no_run
# async fn example(store: &bf_storage::Storage) -> Result<(), bf_storage::Error> {
use bf_storage::CacheStats;
let reader = store.range_reader("segments/example/docs.bfc").await?;
let stats = CacheStats::default();
let footer = reader.read_with_stats(reader.len().saturating_sub(65536)..reader.len(), &stats).await?;
let columns = reader.read_batch_with_stats(&[0..128, 1024..2048], &stats).await?;
assert!((0.0..=1.0).contains(&stats.cache_hit_ratio()));
let temperature = stats.temperature(); // "hot", "warm", or "cold"
let manifest = store.get_cached("manifests/0000000001.json").await?;
let warming = store.prefetch("segments/example/docs.bfc", vec![0..128])?;
warming.await.expect("warming task")?;
# Ok(()) }
```

`range_reader_with_length(key, logical_len, has_header)` skips both HEAD and header discovery when
an index already knows the layout. Objects created by `Storage::create` have a 24-byte portable retry
header; raw backend objects do not. All ranges and reader lengths address logical payload bytes.
`head` and `list` retain their existing physical-length semantics, including that header.

**Use cached APIs only for immutable, create-only keys that will never be reused.** Cache entries do
not expire or invalidate. `get`, `get_range`, and `head` deliberately retain uncached behavior for
mutable system objects, writer heartbeats, fresh WAL-head checks, and existing callers. Engine and
WAL integrations should opt into `range_reader` and `get_cached` for immutable data. A missing object
is never cached. `get_cached` shares blocks and single-flight with range reads; it is intended for
small manifests and WAL entries, and it assembles the full payload in memory.

Memory uses Moka 0.12.16 with byte weights. Disk uses checksummed files, atomic rename, and LRU
payload-file eviction. This is the simpler hybrid design allowed by the M5 brief: it avoids Foyer's
block-engine allocation granularity and lets small disk budgets and tier metrics remain explicit.
Each block has an independent allocation, so retaining a block does not pin a coalesced GET buffer.
Adjacent missing blocks across an entire batch share one GET (up to eight blocks by default).
Concurrent overlapping readers share futures, including with memory caching disabled. If a reader
is cancelled, another waiter can finish the fetch; if all waiters cancel, a later read can retry.
Completed errors are not retained.

Disk files have SHA-256 checksums and are validated on read. Missing, corrupt, or unreadable cache
files become misses; a failed cache write does not fail a successful backend read. Files survive
reopening without a graceful shutdown, though a machine crash may lose recently written cache data.
The directory is exclusively locked for the cache's lifetime. Startup removes abandoned temporary
files, indexes existing blocks, and evicts to the configured budget. Cache directories must be
private to the service user, and cache files contain object data. No credentials belong in config
files checked into source control.

## Configuration

`StorageConfig.cache` is a `CacheConfig`, and both support serde defaults:

| Field | Default | Meaning |
|---|---|---|
| `memory_bytes` | 25% of RAM, capped at 8 GiB | Resident payload budget; Linux cgroup v2 limits are respected; portable fallback is 1 GiB |
| `disk_dir` | absent | Disk is disabled until a directory is supplied |
| `disk_bytes` | 64 GiB | Budget for payload files, including 40-byte checksum headers |
| `block_size` | 1 MiB | Aligned logical block size in bytes |
| `max_coalesce_blocks` | 8 | Maximum adjacent missing blocks in one GET |
| `fetch_concurrency` | 64 | Global maximum simultaneous cache-fill requests |
| `prefetch_concurrency` | 4 | Maximum accepted background hints |
| `namespace` | derived by `Storage::open` | Stable identity for the backend/bucket |

`from_backend_with_cache` requires an explicit, stable `namespace` when disk is enabled. Never share
one identity between different backends. S3 identities include the effective endpoint and region,
including environment defaults. Fresh `memory://` backends receive unique identities. Storage
prefixes, block size, logical length, and header
layout also participate in cache identity. Budget accounting excludes filesystem allocation slack,
filenames, in-memory indexes, metadata, caller-held output buffers, and in-flight GET buffers. Moka
applies byte-weighted admission and eviction; maintenance runs after insertion. Zero disables a tier.
A positive layout cache holds at most 16,384 objects independently of the payload budget.

Prefetch hints return a `JoinHandle<Result<(), Error>>`. `PrefetchBusy` means the hint was not accepted;
the HTTP layer can still reply with its documented acceptance shape as a best-effort hint. Dropping
an accepted handle leaves warming running. Prefetch processes bounded chunks and shares the global
cache-fill concurrency limit with foreground reads. Disk filesystem I/O uses Tokio blocking workers.
Planning and disk index updates are serialized; backend GETs run concurrently. This deliberately
simple disk tier trades some disk-hit parallelism for predictable eviction and reopening behavior.

## Query performance and metrics

Share one `CacheStats` across parallel reads of a query. Counters measure requested logical bytes,
including repeated and overlapping ranges; overfetched block bytes are excluded. The hit ratio is
`(memory_bytes + disk_bytes) / total_bytes`, or `1.0` when no bytes were requested. Single-flight
waiters count as misses because they require a backend fetch. Temperature is `hot` for all-memory
reads or no reads, `cold` for all misses, and `warm` for disk hits or mixed hits and misses. These are
blowfish's thresholds, using turbopuffer's wire values, not claims about hosted turbopuffer's policy.
Snapshot counters after all query reads complete. Failed reads may have charged planned bytes.

The `metrics` facade emits these names, with base units and bounded label values:

| Metric | Type | Unit and labels |
|---|---|---|
| `blowfish_cache_hits_total` | counter | Block lookups; `tier=memory,disk` |
| `blowfish_cache_misses_total` | counter | Missing block lookups, including concurrent waiters |
| `blowfish_cache_bytes` | gauge | Resident payload bytes for `tier=memory`; payload files plus checksum headers for `tier=disk` |
| `blowfish_cache_evictions_total` | counter | Capacity evictions; `tier=memory,disk` |
| `blowfish_object_store_requests_total` | counter | Logical backend operations; `op=get,get_range,head,put,list,delete,copy`, `status=ok,not_found,conflict,error` |
| `blowfish_object_store_request_seconds` | histogram | Seconds through completion, including GET body consumption; `op` as above |
| `blowfish_object_store_bytes_total` | counter | Successfully transferred payload bytes, including retry headers and range overfetch; `op` as above |

Tier byte gauges aggregate live storage handles. Install the metrics recorder before opening storage.
Metadata lookups are excluded from cache hit ratios and hit counters, but their backend requests are
instrumented. HEAD, LIST, DELETE, and COPY report zero payload bytes. LIST counts one logical call
through exhaustion, including all pages; S3 retries and pagination are internal to `object_store`.
Cancelled operations are not counted as completed. Startup probes and heartbeat operations are
instrumented. The wrapper's unused multipart-upload passthrough is not instrumented; the public
`Storage` API writes via single PUTs.

## Verification and opt-in benchmark

```bash
source ~/.cargo/env
cargo fmt --check
cargo clippy -p bf-storage --all-targets -- -D warnings
cargo test -p bf-storage
```

The disposable S3/R2 benchmark reads an 8 MiB object in eight distinct 1 MiB ranges, then repeats
those reads five times. It prints cold and warm mean, p50, and p95 latency. Cold latency includes
cache insertion; metadata discovery is excluded because the reader has the known logical length.
The unique run prefix and its object are deleted even when a timed read fails.

```bash
export BF_CACHE_BENCH_URL=s3://YOUR_BUCKET/dev/cache-bench
export BF_CACHE_BENCH_ENDPOINT=https://YOUR_S3_ENDPOINT
export BF_CACHE_BENCH_PROFILE=YOUR_AWS_PROFILE
export BF_CACHE_BENCH_REGION=auto
cargo run -p bf-storage --release --example cache_bench
BF_CACHE_BENCH_DISK_ONLY=1 cargo run -p bf-storage --release --example cache_bench
```

Use only a disposable development bucket. Endpoint and profile values stay in the environment.

`BackendStats` provides task-scoped request diagnostics without a metrics recorder.
Enter `stats.scope("snapshot", future)` or `stats.scope("search", future)`, propagate
the same `Arc` explicitly to spawned tasks, and call `snapshot()` after completion.
Measurements include not-found requests, physical transferred bytes, and summed
backend durations. Concurrent request durations overlap. Engine diagnostics use
this API to distinguish consistency requests from uncached immutable reads.
