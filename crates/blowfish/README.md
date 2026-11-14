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
| `--read-only` | `BLOWFISH_READ_ONLY` |
| `--single-writer` | `BLOWFISH_SINGLE_WRITER` |

The listener defaults to `0.0.0.0:8000`. If an admin key is configured, every request
requires `Authorization: Bearer <key>`. Otherwise, authorization is disabled.
`blowfish::Authorizer` is the adapter seam for the role-based bf-auth implementation.
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
