# Compatibility 🐡

This table follows the feature inventory in `DESIGN.md` §2 and the recorded scenarios in `compat/`.
The fixture suite tests representative behavior, not every combination or every upstream limit.

| Feature | Status | Coverage or limitation |
| --- | --- | --- |
| Namespace writes, row/column upserts, patches, and deletes | Supported | Recorded write scenarios |
| Conditional writes, filter mutations, and `$ref_new` | Supported | Recorded condition and filter scenarios |
| Copy within a deployment | Supported | Recorded copy and async-copy scenarios |
| Branch, cross-deployment copy, and customer encryption | Unsupported | Design §2.2 |
| Scalar and array attributes, UUID, datetime, bytes, and dense float vectors | Supported | Type round-trip scenarios |
| Sparse, integer, and multi-vector search | Unsupported | Design §2.3 |
| ANN, exact vector search, BM25, and scoring expressions | Supported | ANN, BM25, and score scenarios |
| Attribute ordering, filters, and token filters | Supported | Ordering and filter scenarios |
| Regex and fuzzy filters | Unsupported | Design §2.4 |
| Aggregation, grouping, result projection, and pagination | Supported | Aggregate, include, exclude, and export scenarios |
| Multi-query | Supported | Recorded multi-query scenario |
| Reranking and computed attributes | Partial | See unsupported forms in Design §2.4 |
| Strong and eventual consistency | Supported | Recorded eventual scenario and engine tests |
| Metadata, schema, namespace list/delete, warming, recall, and explain | Supported | Recorded endpoint scenarios |
| Metadata pinning | Partial | Pinning is advisory |
| Async operations and legacy v1 endpoints | Supported | Recorded async and legacy scenarios |
| Authentication and API keys | Partial | Blowfish's own key management and roles |
| Billing and service performance fields | Partial | Billing values are placeholders, and performance fields differ |
| Managed regions and turbopuffer-hosted deployment features | Unsupported | Self-hosted storage and deployment |
