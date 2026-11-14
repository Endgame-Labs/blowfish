# blowfish 🐡

An open-source, self-hostable search engine that is compatible with the [turbopuffer](https://turbopuffer.com)
API. Bring any S3-compatible bucket (AWS S3, Cloudflare R2, GCS, MinIO, ...), point blowfish at it, and it
does the rest: vector, full-text, and hybrid search over namespaces kept in object storage, with the
official turbopuffer SDKs working unchanged.

**Why it exists:** so you can develop, test, and run CI against the turbopuffer API without a turbopuffer
account, and self-host modest workloads. For production at scale, we strongly recommend
[turbopuffer](https://turbopuffer.com) itself: since the APIs match, upgrading is a change of base URL and
API key.

Status: under construction. See [`DESIGN.md`](DESIGN.md) for the design and [`PROGRESS.md`](PROGRESS.md)
for where the build is.

## Quickstart

```bash
docker compose up --build --wait blowfish
curl --fail http://127.0.0.1:8000/
```

This local setup uses a persistent volume and disables auth. Set `BLOWFISH_ADMIN_KEY` and
`BLOWFISH_REQUIRE_AUTH=true` to require API keys. For an S3 test stack, run
`docker compose --profile s3 up --build --wait blowfish-s3`, then use port 8001.

With Cargo: `cargo run -p blowfish -- serve --storage-url file:///tmp/blowfish`.
Point official SDKs at `TURBOPUFFER_BASE_URL=http://127.0.0.1:8000`, set a nonempty
`TURBOPUFFER_API_KEY`, and **unset `TURBOPUFFER_REGION`**.

- [Quickstart and official SDK examples](docs/quickstart.md)
- [Configuration](docs/configuration.md) and [example TOML](blowfish.example.toml)
- [API keys, roles, CLI, and admin API](docs/auth.md)
- [Operations, storage, replicas, and backups](docs/operations.md)
- [Prometheus metrics](docs/metrics.md)

blowfish is an independent project and is not affiliated with or endorsed by turbopuffer.

License: MIT.
