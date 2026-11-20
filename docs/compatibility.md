# Compatibility

This table follows the feature inventory in [`DESIGN.md`](../DESIGN.md) §2 and the recorded scenarios in [`compat/`](../compat/README.md).
The fixture suite tests representative behavior, not every combination or every upstream limit.

| Feature | Status | Coverage or limitation |
| --- | --- | --- |
| Namespace writes, row/column upserts, patches, and deletes | Supported | Recorded write scenarios |
| Conditional writes, filter mutations, and `$ref_new` | Supported | Recorded condition and filter scenarios |
| Copy within a deployment | Supported | Recorded copy and async-copy scenarios |
| Namespace branching | Supported | Recorded branch scenario |
| Cross-deployment copy and customer-managed encryption | Unsupported | Clear 400; Design §2.2 |
| Scalar and array attributes, UUID, datetime, bytes, and dense float vectors | Supported | Type round-trip scenarios |
| Sparse vectors and int8 vectors | Supported | Recorded sparse and i8 scenarios |
| Multi-vector (`[][N]f32`, `late_interaction: true`) search | Supported | Scans immutable document columns rather than an ANN index; turbopuffer gates this per organization (`m7_multivector`) |
| ANN, exact vector search, BM25, and scoring expressions | Supported | ANN, BM25, and score scenarios |
| Attribute ordering, filters, and token filters | Supported | Ordering and filter scenarios |
| Regex, glob, and fuzzy filters, and highlighting | Supported | Recorded regex, glob, fuzzy, and highlight scenarios |
| Aggregation, grouping, result projection, and pagination | Supported | Aggregate, include, exclude, and export scenarios |
| Multi-query | Supported | Recorded multi-query scenario |
| Reranking and computed attributes | Supported with configured models | RRF always works. Model `rerank_by` works for models the operator maps in `[models]` (see [models.md](models.md)); unconfigured models get a clear 400 |
| Native embedding (`embed` schema setting and `Embed` expressions) | Supported with configured models | Works for models the operator maps in `[models]`, in-process or through an HTTP provider (see [models.md](models.md)). Without a `[models]` entry, a clear 400. Differences: the model list is the operator's, the generated vector column is named `embed_<attribute>` (my reading of the docs), and patches do not re-embed |
| Strong and eventual consistency | Supported | Recorded eventual scenario and engine tests |
| Metadata, schema, namespace list/delete, warming, recall, and explain | Supported | Recorded endpoint scenarios |
| Metadata pinning | Partial | Pinning is advisory |
| Async operations and legacy v1 endpoints | Supported | Recorded async and legacy scenarios |
| SDK retry and backpressure headers | Supported | `Retry-After` (whole seconds, >= 1) and `retry-after-ms` on `429` and `503` (WAL backpressure delay estimated from the indexer's smoothed drain rate, clamped to 0.5–30 s; 1 s default); `x-should-retry: true` on `408`, `409`, `429`, `502`, `503`, and `504`, and `false` on `500`. Verified by Python (sync and async) and TypeScript SDK retry scenarios; see [`research/sdk-compat-audit.md`](research/sdk-compat-audit.md) |
| Authentication and API keys | Partial | Blowfish's own org, project, and scoped key model (`admin`, `read_write`, `read`, plus legacy roles in `default`); see [`auth.md`](auth.md) |
| Billing and service performance fields | Partial | Billing values are placeholders, performance fields differ, and `Server-Timing` is not sent (legacy SDKs treat timing figures as absent) |
| Managed regions and turbopuffer-hosted deployment features | Unsupported | Self-hosted storage and deployment |

## Official SDK client notes

- **Base URL and path prefixes:** set `TURBOPUFFER_BASE_URL` to the blowfish server URL. Routes are absolute, so serving blowfish under a path prefix requires a reverse proxy that strips the prefix.
- **Region setting:** unset `TURBOPUFFER_REGION` whenever `TURBOPUFFER_BASE_URL` has no `{region}` placeholder; the official SDKs reject the combination client-side.
- **Full wire audit:** see [`research/sdk-compat-audit.md`](research/sdk-compat-audit.md) for the header-by-header check against the official Python, TypeScript, Go, Java, Ruby, and Rust SDKs.
