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

blowfish is an independent project and is not affiliated with or endorsed by turbopuffer.

License: MIT.
