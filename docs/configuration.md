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
| `engine.max_unindexed_bytes` | `--max-unindexed-bytes` | `BLOWFISH_MAX_UNINDEXED_BYTES` | 2000000000 |
| `engine.eventual_tail_bytes` | `--eventual-tail-bytes` | `BLOWFISH_EVENTUAL_TAIL_BYTES` | 134217728 |
| `engine.ann_nprobe` | `--ann-nprobe` | `BLOWFISH_ANN_NPROBE` | 128 |
| `engine.ann_rerank_factor` | `--ann-rerank-factor` | `BLOWFISH_ANN_RERANK_FACTOR` | 64 |
| `engine.recall_sample_rate` | `--recall-sample-rate` | `BLOWFISH_RECALL_SAMPLE_RATE` | 0.01 |
| `storage.cache.memory_bytes` | `--cache-memory-bytes` | `BLOWFISH_CACHE_MEMORY_BYTES` | host/container limit / 4, capped at 8 GiB |
| `storage.cache.disk_bytes` | `--cache-disk-bytes` | `BLOWFISH_CACHE_DISK_BYTES` | 68719476736 |
| `storage.cache.disk_dir` | `--cache-disk-dir` | `BLOWFISH_CACHE_DISK_DIR` | Disabled |
| `storage.cache.namespace` | `--cache-namespace` | `BLOWFISH_CACHE_NAMESPACE` | automatic |
| `storage.cache.block_size` | `--cache-block-size` | `BLOWFISH_CACHE_BLOCK_SIZE` | 1048576 |
| `storage.cache.max_coalesce_blocks` | `--cache-max-coalesce-blocks` | `BLOWFISH_CACHE_MAX_COALESCE_BLOCKS` | 8 |
| `storage.cache.fetch_concurrency` | `--cache-fetch-concurrency` | `BLOWFISH_CACHE_FETCH_CONCURRENCY` | 16 |
| `storage.cache.prefetch_concurrency` | `--cache-prefetch-concurrency` | `BLOWFISH_CACHE_PREFETCH_CONCURRENCY` | 4 |

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
