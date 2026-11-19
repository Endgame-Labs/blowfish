# Quickstart

blowfish is for development, CI, and modest self-hosted workloads. For production at scale, we
recommend turbopuffer itself. The official SDKs use the same API, so upgrading means changing the
base URL and API key.

## Docker

From a checkout:

```bash
docker compose up --build --wait blowfish
curl --fail http://127.0.0.1:8000/
```

This binds to loopback, stores data in the `blowfish-data` volume, and runs without auth for local
development. To enable auth, provision `BLOWFISH_ADMIN_KEY` in your shell or deployment environment,
then run `BLOWFISH_REQUIRE_AUTH=true docker compose up --wait blowfish`. See [auth](auth.md).

To exercise S3 instead of the local filesystem:

```bash
docker compose --profile s3 up --build --wait blowfish-s3
curl --fail http://127.0.0.1:8001/_blowfish/v1/status
```

The first image build may take longer than a minute. Once built, the local stack starts with the same command.

That profile starts SeaweedFS with a precreated `blowfish` bucket, disposable demo credentials,
and a separate blowfish at port 8001. Startup probes conditional writes; status should report
`multi_writer`. The profile is a local test setup. `docker compose --profile s3 down` stops services,
while preserving volumes. Add `--volumes` only when you intend to erase the demo data.

## Cargo

Install the current stable Rust toolchain, then:

```bash
source ~/.cargo/env
cargo run -p blowfish -- serve --storage-url file:///tmp/blowfish --listen 127.0.0.1:8000
```

For S3, use `--storage-url s3://your-bucket/your-prefix` with the backend endpoint, region, and AWS
credentials described in [configuration](configuration.md). For a config file, copy
[`blowfish.example.toml`](../blowfish.example.toml) and pass `--config blowfish.toml`.

## Official Python SDK

Point the client at blowfish, and **unset `TURBOPUFFER_REGION`**. A region on a base URL without a
`{region}` placeholder causes official clients to reject the configuration.

```bash
export TURBOPUFFER_BASE_URL=http://127.0.0.1:8000
unset TURBOPUFFER_REGION
export TURBOPUFFER_API_KEY=local-development
uv run --with turbopuffer python - <<'PY'
from turbopuffer import Turbopuffer

client = Turbopuffer()
ns = client.namespace("hello-fish")
ns.write(
    distance_metric="cosine_distance",
    upsert_rows=[{"id": 1, "vector": [1.0, 0.0], "message": "hello fish"}],
)
print(ns.query(rank_by=("vector", "ANN", [1.0, 0.0]), top_k=1, include_attributes=True))
PY
```

The SDK requires a nonempty API key even when server auth is disabled. When auth is enabled, set
`TURBOPUFFER_API_KEY` to a project API key (`bf_...`; see [auth](auth.md)).

## Official TypeScript SDK

In your application, install `@turbopuffer/turbopuffer`, and use the same environment:

```bash
npm install @turbopuffer/turbopuffer
export TURBOPUFFER_BASE_URL=http://127.0.0.1:8000
unset TURBOPUFFER_REGION
export TURBOPUFFER_API_KEY=local-development
```

```typescript
import Turbopuffer from '@turbopuffer/turbopuffer';

const client = new Turbopuffer();
const ns = client.namespace('hello-fish');
await ns.write({
  distance_metric: 'cosine_distance',
  upsert_rows: [{ id: 1, vector: [1, 0], message: 'hello fish' }],
});
console.log(await ns.query({
  rank_by: ['vector', 'ANN', [1, 0]],
  top_k: 1,
  include_attributes: true,
}));
```

See [configuration](configuration.md), [compatibility](compatibility.md), [models](models.md),
[operations](operations.md), [metrics](metrics.md), and [benchmarks](benchmarks/README.md) for
configuration, deployment, and performance details.
