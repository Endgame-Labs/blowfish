# Compatibility 🐡

This table follows the feature inventory in `DESIGN.md` §2 and the recorded scenarios in `compat/`.
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
| Multi-vector (vector-array) ANN | Matches turbopuffer's gated response | turbopuffer gates it per organization; blowfish returns the same 400 |
| ANN, exact vector search, BM25, and scoring expressions | Supported | ANN, BM25, and score scenarios |
| Attribute ordering, filters, and token filters | Supported | Ordering and filter scenarios |
| Regex, glob, and fuzzy filters, and highlighting | Supported | Recorded regex, glob, fuzzy, and highlight scenarios |
| Aggregation, grouping, result projection, and pagination | Supported | Aggregate, include, exclude, and export scenarios |
| Multi-query | Supported | Recorded multi-query scenario |
| Reranking and computed attributes | Supported with configured models | RRF always works. Model `rerank_by` works for models the operator maps in `[models]` (see [models.md](models.md)); unconfigured models get a clear 400 |
| Native embedding (`embed` schema setting and `Embed` expressions) | Supported with configured models | Works for models the operator maps in `[models]`, in-process or through an HTTP provider (see [models.md](models.md)). Without a `[models]` entry, a clear 400. Differences: the model list is the operator's, the generated vector column is named `embed_<attribute>` (our reading of the docs), and patches do not re-embed |
| Strong and eventual consistency | Supported | Recorded eventual scenario and engine tests |
| Metadata, schema, namespace list/delete, warming, recall, and explain | Supported | Recorded endpoint scenarios |
| Metadata pinning | Partial | Pinning is advisory |
| Async operations and legacy v1 endpoints | Supported | Recorded async and legacy scenarios |
| Authentication and API keys | Partial | Blowfish's own key management and roles |
| Billing and service performance fields | Partial | Billing values are placeholders, and performance fields differ |
| Managed regions and turbopuffer-hosted deployment features | Unsupported | Self-hosted storage and deployment |
