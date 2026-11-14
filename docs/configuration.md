# Configuration 🐡

Run `blowfish serve --config blowfish.toml`. The complete annotated template is
[`blowfish.example.toml`](../blowfish.example.toml). No config file is loaded implicitly. Set a storage
URL through a file, environment variable, or flag; it has no implicit host directory default.

Precedence is CLI flags, then environment variables, then TOML. Boolean flags or true environment
values enable a setting; they do not disable a true value in the file. Unknown top-level TOML keys
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
