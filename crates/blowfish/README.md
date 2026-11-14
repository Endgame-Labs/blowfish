# Running blowfish 🐡

```sh
source ~/.cargo/env
cargo run -p blowfish -- serve --config blowfish.toml
```

A local configuration:

```toml
listen = "127.0.0.1:8000"

[storage]
url = "file:///tmp/blowfish-data"
```

An S3-compatible configuration:

```toml
listen = "0.0.0.0:8000"

[storage]
url = "s3://your-bucket/search-prefix"
region = "us-east-1"
# endpoint = "https://your-s3-endpoint.example"
# profile = "your-aws-profile"
```

AWS environment credentials and explicit AWS shared-credentials profiles are
supported. Set the custom endpoint and region `auto` for R2. Storage startup probes
conditional create. A backend that ignores conditional create uses a serialized
single writer with a best-effort heartbeat; a second live writer refuses to start.
`--single-writer` forces that mode, and `--read-only` serves replicas without writes
or a mutating startup probe.

Flags and environment variables override the config:

| Flag | Environment variable |
| --- | --- |
| `--storage-url` | `BLOWFISH_STORAGE_URL` |
| `--storage-endpoint` | `BLOWFISH_STORAGE_ENDPOINT` |
| `--storage-region` | `BLOWFISH_STORAGE_REGION` |
| `--storage-profile` | `BLOWFISH_STORAGE_PROFILE` |
| `--listen` | `BLOWFISH_LISTEN` |
| `--admin-key` | `BLOWFISH_ADMIN_KEY` |
| `--require-auth` | `BLOWFISH_REQUIRE_AUTH` |
| `--metrics-listen` | `BLOWFISH_METRICS_LISTEN` |
| `--read-only` | `BLOWFISH_READ_ONLY` |
| `--single-writer` | `BLOWFISH_SINGLE_WRITER` |

The listener defaults to `0.0.0.0:8000`. If an admin key is configured, every request
requires `Authorization: Bearer <key>`. Otherwise, authorization is disabled.
`bf-auth::KeyStore` enforces roles and namespace scopes. Use `--require-auth` to make a
missing super key a startup error. `blowfish keys` manages keys through the admin API.
See [authentication](../../docs/auth.md) and [metrics](../../docs/metrics.md).
Logs use `tracing`, controlled by `RUST_LOG`.

Use an official SDK with `TURBOPUFFER_BASE_URL=http://localhost:8000`, any nonempty
`TURBOPUFFER_API_KEY` when auth is disabled, and `TURBOPUFFER_REGION` unset.

```sh
scripts/smoke.sh
BLOWFISH_SDK_SMOKE=1 scripts/smoke.sh
```

The second command also runs the official Python SDK using uv. Both smoke scripts
use disposable namespaces and delete them afterward. The shell script owns its
local file store and server process, and removes them on exit.

The HTTP adapter also serves the legacy v1 write, query, export, HEAD, and delete routes. Legacy nested attributes, object-form filters, and null-vector deletes are translated into engine operations. Export cursors are opaque, namespace-bound, and paginate by ID; `page_size` is an optional local extension (1–1,000, default 1,000).

`POST /v2/namespaces/{ns}/async` starts a background namespace copy. It returns 202, a token, and a relative, same-origin `Location`; the explicit endpoint omits `Preference-Applied`. Copy writes with `Prefer: respond-async` use the same operation path and include `Preference-Applied: respond-async`, so current SDKs transparently poll. Ordinary writes respond synchronously. Running and finished status objects are immutable records under `_system/operations/`, readable by any node. Finished results expire after one hour and are deleted when an expired result is polled. A process crash during an in-flight copy does not resume that job automatically; completed records survive restarts. Completion persistence retries transient storage failures, but a sustained failure can leave a running record.

`PATCH /v1/namespaces/{ns}/metadata` persists `read_only` and pinning through `Engine::patch_metadata`. Read-only settings reject document and schema writes with the upstream 400 error. They are not a barrier for writes already in flight. Pinning accepts booleans, null, or a replicas object, is echoed in metadata, and has no scheduling effect. Copy inherits read-only, while pinning remains destination-local.
