# turbopuffer public HTTP API: implementation spec for blowfish

Researched 2026-11-14. Sources:

- Docs as markdown (append `.md`): https://turbopuffer.com/docs/<page>.md. The index is https://turbopuffer.com/llms.txt, and everything in one file is https://turbopuffer.com/llms-full.txt (about 1 MB). Docs say "Last updated: November 13, 2026".
- **Official OpenAPI spec** (generates the Stainless SDKs): https://github.com/turbopuffer/turbopuffer-openapi/blob/main/openapi.yml (last commit 2026-10-28). It's linked from https://turbopuffer.com/docs/api-overview. **When the docs and the spec disagree, follow the spec and the SDK, because that's what the clients actually send and parse.**
- Python SDK v3.1.0 (2026-10-28): https://github.com/turbopuffer/turbopuffer-python. I read it for wire behavior (paths, headers, body rewriting, base64 vectors, async polling, retries).

The upstream spec is https://github.com/turbopuffer/turbopuffer-openapi (not vendored here).

---

## 0. Endpoint inventory (what the current SDKs call)

| Method | Path (as sent by the SDK) | Purpose | Doc page |
|---|---|---|---|
| GET | `/v1/namespaces?cursor=&prefix=&page_size=` | List namespaces | /docs/namespaces |
| POST | `/v2/namespaces/{ns}` | Write (upsert/patch/delete/conditional/by-filter/copy/branch/schema/create) | /docs/write |
| POST | `/v2/namespaces/{ns}?stainless_overload=copyFrom` | `copy_from()`: same endpoint; SDK nests the body (see 0.1) | /docs/write |
| POST | `/v2/namespaces/{ns}?stainless_overload=branchFrom` | `branch_from()`: same endpoint, body nested | /docs/branching |
| POST | `/v2/namespaces/{ns}/async?stainless_overload=startCopyFrom` | Start an async copy. Returns 202 + `{"token"}` + `Location` | spec only |
| GET | `/v1/namespaces/{ns}/operations/{token}` (SDK appends `?stainless_overload=pollCopyFrom`) | Poll an async operation | /docs/api-overview |
| DELETE | `/v2/namespaces/{ns}` | Delete namespace (`delete_all()`) | /docs/delete-namespace |
| POST | `/v2/namespaces/{ns}/query` | Query (single) | /docs/query |
| POST | `/v2/namespaces/{ns}/query?stainless_overload=multiQuery` | Multi-query (same endpoint, body has `queries`) | /docs/query |
| POST | `/v2/namespaces/{ns}/explain_query` | Query plan text | spec only (undocumented) |
| GET | `/v2/namespaces/{ns}/metadata` | **SDK uses v2.** Docs say `GET /v1/.../metadata`. Serve both | /docs/metadata |
| PATCH | `/v1/namespaces/{ns}/metadata` | Update pinning / read_only | /docs/metadata |
| GET | `/v1/namespaces/{ns}/schema` | Get schema (`schema()`) | spec only (old docs) |
| POST | `/v1/namespaces/{ns}/schema` | Update schema (`update_schema()`) | spec only |
| GET | `/v1/namespaces/{ns}/hint_cache_warm` | Warm-cache hint | /docs/warm-cache |
| POST | `/v1/namespaces/{ns}/_debug/recall` | Recall evaluation | /docs/recall |

The spec says these are intentionally omitted (deprecated) but may still be hit by old clients:
- `GET /v1/namespaces/{ns}` was the old export: columnar output with a `cursor`/`next_cursor`. Deprecated in favor of paging v2 queries.
- `HEAD /v1/namespaces/{ns}` was old metadata delivered in headers: `x-turbopuffer-dimensions`, `x-turbopuffer-approx-num-vectors`, and `x-turbopuffer-created-at`. Deprecated in favor of count queries and the schema endpoint.
- See section 13 for the legacy v1 write/query shapes.

### 0.1 SDK wire quirks blowfish must tolerate

1. **`?stainless_overload=...` reaches the server.** The Python SDK leaves it in the URL (`path_template("/v2/namespaces/{namespace}?stainless_overload=branchFrom")`). The server must ignore unknown query params. Its values are `copyFrom`, `branchFrom`, `startCopyFrom`, `multiQuery`, and `pollCopyFrom`.
2. **Body nesting for overloads** (`src/turbopuffer/lib/nested_body.py`). The SDK rewrites the body before sending it:
   - `branchFrom`: `{"source_namespace": X}` becomes `{"branch_from_namespace": {"source_namespace": X}}`
   - `copyFrom` and `startCopyFrom`: `{source_namespace, source_api_key?, source_region?, dest_encryption?}` becomes `{"copy_from_namespace": {...same object...}}`
   - **So `branch_from_namespace` can be a string or `{"source_namespace": "..."}`.** `copy_from_namespace` can be a string or `{"source_namespace", "source_api_key"?, "source_region"?, "dest_encryption"?}`. Note that `dest_encryption` can sit *inside* the copy object (from the SDK) in addition to top-level `encryption` (from the docs).
3. **Every SDK request carries `prefer: respond-async`** (`lib/respond_async.py`). The SDK only switches to polling when the response is `202` **and** has the header `preference-applied: respond-async` **and** a `Location` header. That header must resolve to the same scheme, host, and port as the request, or the SDK rejects it. It polls `GET <Location>` every 1s (with `prefer` removed) until `{"status":"finished"}`. A server that ignores `prefer` and answers synchronously is fully compatible.
4. **Vectors are base64-encoded on writes by the Python SDK** (`_utils/_transform.py: _encode_write_vectors`). In `upsert_rows` and `upsert_columns`, the SDK encodes the key `vector` plus any schema attribute with a truthy `ann` (excluding `[]`-prefixed types, meaning vector arrays). It encodes them as base64 of little-endian f32. A list of lists becomes a list of base64 strings. Only lists whose first element is a Python `float` get encoded, so a list of ints passes through as a JSON array. **The server must accept both JSON arrays and base64 strings for every dense vector attribute.** Patch payloads are not encoded.
5. **Compression.** The SDK sends `Accept-Encoding: identity` by default. With `compression=True` it sends `Accept-Encoding: gzip` and gzips requests over 1024 bytes (`Content-Encoding: gzip`). Support gzip request bodies and optionally gzip responses.
6. **Retries.** The SDK retries on 408, **409**, 429, and any 5xx, unless the response carries `x-should-retry: true|false`. It honors `retry-after-ms` (non-standard, in milliseconds) and `retry-after` (seconds or an HTTP date). Use `409` deliberately: the docs use it for "index not yet built" on queries, and the SDK will retry it.
7. **Other headers the SDK sends:** `Authorization: Bearer <key>`, `User-Agent`, `X-Stainless-Lang/Package-Version/OS/Arch/Runtime/Runtime-Version`, `X-Stainless-Async`, `x-stainless-retry-count`, `x-stainless-read-timeout`, `X-Request-Id` (a client-generated ID, new in v3.0.0), and `X-turbopuffer-Client-Telemetry` (a JSON blob of past request records). The server can advertise `X-turbopuffer-Client-Telemetry-Sample-Rate` to control sampling. Ignore all of these safely.
8. **Base URL.** The default is `https://{region}.turbopuffer.com`. It can be overridden with `base_url` or the env var `TURBOPUFFER_BASE_URL`. If the override has no `{region}` placeholder, the SDK errors when `region` is also set. Extra headers can come from the env var `TURBOPUFFER_CUSTOM_HEADERS` (newline-separated). So blowfish only needs `TURBOPUFFER_BASE_URL=https://blowfish.example`.

---

## 1. Auth, encoding, compression, errors, async (https://turbopuffer.com/docs/api-overview)

- **Auth:** `Authorization: Bearer <API_KEY>` on every call. The OpenAPI scheme is `http`/`bearer`, `bearerFormat: API key`.
  - Key format (from the BYOC docs at https://turbopuffer.com/docs/byoc/operations): `tpuf_` + 32 characters of `[a-zA-Z0-9]`.
  - BYOC stores keys as `base64(sha256(full_key))` per org ID. Org IDs are 24 characters of `[a-z0-9]`.
  - Roles: `admin` (the default), `write`, `read`, and `list`. Listing namespaces requires `list` or `admin` ("This endpoint is available to API keys with list or admin permissions", per /docs/namespaces).
  - One key applies to all namespaces in its org. This is a good model for blowfish multi-tenancy.
- **Encoding:** JSON request and response bodies.
- **Compression:** "The API supports standard HTTP compression headers." The official clients disable it by default.
- **Error body (all endpoints):**
  ```json
  {"status": "error", "error": "an error message"}
  ```
  The spec's `ErrorResponse` has required `status` (const `"error"`) and `error` (string). Every endpoint's `default` response is an ErrorResponse.
- **Status codes mentioned in the docs:**
  - `200`: success. `202`: the cache-warm response per the spec, and async accepted.
  - `400`: bad request, for example the same document ID twice in one write ("the request will fail with an HTTP 400 error"), or "destination namespace already exists" on a copy.
  - `404`: namespace not found (for example a write with `create_namespace:false`), or an async-operation result polled more than 1h after it finished.
  - `409`: a query that depends on an index that is not yet built, after `filterable`/`full_text_search`/`regex`/`glob`/`fuzzy` was enabled on an existing attribute.
  - `429`: rate or backpressure. Sources: the unindexed backlog exceeds 2 GB (or 1M rows); more than 16 concurrent queries per namespace, after an 800ms wait; or embedding rate limits.
  - `5xx`: internal.
- **Asynchronous requests** (supported for `copy_from_namespace` and recall):
  - The request carries `Prefer: respond-async`. The server *may* answer `202 Accepted` with `Preference-Applied: respond-async`, `Location: /v1/namespaces/{ns}/operations/{token}`, and body `{"token": "tpuf-abc123"}`. "The `Prefer: respond-async` header is just a hint. The server may return a sync response."
  - Poll with `GET /v1/namespaces/{ns}/operations/{token}`. The schema is `CopyFromNamespaceOperation`:
    ```json
    {"status": "running", "start_time": "2026-...Z", "progress": "index: copied 104857600 bytes"}
    {"status": "finished", "start_time": "...", "finish_time": "...",
     "result": {"success": {"status": "OK", "message": "namespace cloned successfully"}}}
    {"status": "finished", "start_time": "...", "finish_time": "...",
     "result": {"error": {"status_code": 400, "detail": {"status": "error", "error": "destination namespace already exists"}}}}
    ```
    `progress` is optional and freeform. `result` has exactly one of `success` (a WriteResult) or `error` (`{status_code, detail: ErrorResponse}`). In the spec, `start_time` is required for both variants and `finish_time` is required when finished, though the docs examples omit them.
  - Results are retained 1 hour after finishing, after which a poll returns `404`.
  - The spec also has the explicit async endpoint `POST /v2/namespaces/{ns}/async` (body `{"copy_from_namespace": {...}}` after SDK nesting), which always returns `202 {"token"}` + `Location`.
- **Timeouts:** in the SDK, a client timeout during polling raises a timeout, but "the operation will still continue server-side".

---

## 2. Regions / base URLs (https://turbopuffer.com/docs/regions)

Each region is its own base URL: `https://{region}.turbopuffer.com`. The region list is `aws-ap-southeast-2`, `aws-ca-central-1`, `aws-eu-central-1`, `aws-eu-west-1`, `aws-eu-west-2`, `aws-us-east-1`, `aws-us-east-2`, `aws-us-west-2`, `aws-ap-south-1`, `aws-sa-east-1`, `aws-ap-southeast-1`, `gcp-us-central1` (compute-constrained), `gcp-us-east1`, `gcp-us-west1`, `gcp-us-east4`, `gcp-northamerica-northeast2`, `gcp-europe-west3`, `gcp-europe-west1`, `gcp-asia-southeast1`, `gcp-asia-northeast3`, and `azure-eastus2` (private preview).

Region names only matter to blowfish in `copy_from_namespace.source_region`, for cross-region copies. Blowfish could map a region to a peer blowfish URL or reject it.

---

## 3. IDs, attributes, and types (https://turbopuffer.com/docs/write#schema, plus the spec's `Id` and `AttributeType`)

**Document IDs** (`Id` = anyOf):
- unsigned 64-bit integer (JSON integer >= 0)
- UUID (string; stored as 128-bit **only if** the schema declares `"id": "uuid"`)
- string up to 64 bytes

`id` is the only non-nullable attribute. Max ID size is 64 bytes. The `id` type can be declared in the schema (`"id": "uuid"`, `"string"`, or `"uint"`). The metadata example shows `"id": {"type": "uint"}`.

**Attribute names:** up to 128 bytes, must not start with `$`, and at most 1,024 attribute names per namespace. Types are inferred from the first occurrence. Every attribute is nullable. If a new attribute is added, existing docs read it as null.

**Types** (the `type` string):

| Type | Notes |
|---|---|
| `string` | inferable |
| `int` | i64, inferable (JSON integers infer as `int`; the metadata example shows inferred `uint`, so check) |
| `uint` | u64, must be declared explicitly |
| `float` | f64 |
| `uuid` | 128-bit, explicit; JSON value is a UUID string |
| `datetime` | explicit; ISO 8601 string with a mandatory date and optional time and zone (`"2015-01-20"`, `"2015-01-20T12:34:56"`, `"2015-01-20T12:34:56-04:00"`); stored as i64 ms since epoch, UTC |
| `bool` | inferable |
| `bytes` | explicit; base64 string in JSON; stored and billed decoded; never filterable or indexed (gets the non-filterable discount) |
| `[]string`, `[]int`, `[]uint`, `[]float`, `[]uuid`, `[]datetime`, `[]bool` | arrays; `[]string`/`[]int`/`[]bool` inferable |
| `[N]f32`, `[N]f16`, `[N]i8` | dense vector of dimension N. `vector` is auto-inferred as a vector; other vector columns must be declared. f16 and i8 must be declared at namespace creation |
| `[][N]f32` | variable-length array of N-dim vectors for late interaction (private beta) |
| `{}f16` | sparse vector: `{"dim0": 0.1, "dim1": 0.2}` (string keys, f16 weights); not filterable; needs `sparse_knn` for `SparseKNN` |

**Vector wire encoding (input and output):** a JSON number array, or a base64 string of **little-endian f32** bytes, regardless of the schema element type (f16 and i8 are still sent as f32 base64). Vector arrays (`[][N]f32`) don't support base64 yet, according to an SDK comment. All docs must include every vector attribute if the namespace has vector indexes, and all vectors in a column must share dimensions.

**Spec `Columns.vector`** is `oneOf [array of Vector, Vector]`. That means a column may also be a single base64 string. Presumably that's the concatenation of all vectors; this is unverified, so accept it defensively.

**Schema object per attribute** (`AttributeSchema` = a type string shorthand | `AttributeSchemaConfig` | `AttributeSchemaDrop`):

```jsonc
"attr": "[]uuid"                       // shorthand for {"type": "[]uuid"}
"attr": {
  "type": "string",                    // required
  "filterable": true,                  // default true; default false when full_text_search, regex, glob, or fuzzy is set
  "regex": false,                      // trigram index for Regex
  "glob": false,                       // trigram index for Glob/IGlob (Glob also works with filterable for back-compat)
  "fuzzy": false,                      // enables the Fuzzy filter
  "full_text_search": true | {         // string or []string only
    "tokenizer": "word_v4",            // word_v4 (default; default upgraded over time) | word_v3 | word_v2 | word_v1 | word_v0 | whitespace | smartcn | kuromoji | nori | pre_tokenized_array
    "case_sensitive": false,
    "language": "english",             // arabic danish dutch english finnish french german greek hungarian italian norwegian portuguese romanian russian spanish swedish tamil turkish
    "stemming": false,
    "remove_stopwords": false,         // docs: default false (changed Jan 2026). The spec description is stale and says true
    "ascii_folding": false,
    "max_token_length": 39,            // 1..254 bytes; longer tokens are dropped
    "k1": 1.2,                         // > 0
    "b": 0.75,                         // [0, 1]
    "k3": 8.0                          // > 0, query-term-frequency saturation (in the docs, not yet in the spec)
  },
  "ann": true | {"distance_metric": "cosine_distance", "late_interaction": false},  // required true for vector types
  "sparse_knn": {"distance_metric": "dot_product"},   // {}f16 only; dot_product = sum(x*y), higher is better
  "embed": "model-name" | {"model": "...", "attribute": "...", "dims": 1024, "dtype": "..."} | null   // native embedding (turbopuffer-hosted)
}
"attr": {"drop": true}                 // spec only: drops the attribute; cannot combine with other settings
```

The metadata response echoes the schema in full form, for example `"vector": {"type": "[2]f32", "ann": {"distance_metric": "cosine_distance"}}` and `"my-number": {"type": "uint", "filterable": true, "full_text_search": null}`.

**Tokenizer details** (https://turbopuffer.com/docs/fts#tokenizers):
- `word_v3`/`word_v4` use UAX #29 (Unicode 17) segmentation; v4 is about 3x faster and fixes edge cases.
- `word_v2` makes tokens from ideographic codepoints, alphanumeric runs, and emoji glyph sequences, and discards everything else (Unicode 16).
- `word_v1` is like v2 but treats ideographs as alphanumeric (Unicode 10). `word_v0` is like v1 but discards emoji.
- `whitespace` splits on Unicode whitespace and keeps punctuation. It doesn't support stemming or stopwords.
- `smartcn`, `kuromoji`, and `nori` are CJK dictionary tokenizers. On those you cannot set language, case_sensitive, stemming, remove_stopwords, or ascii_folding.
- `pre_tokenized_array` is for `[]string` only. Each element is one token. Queries (`BM25`, `ContainsAllTokens`) must pass a `[]string`. Matching is case-sensitive with no stemming or stopwords. It's incompatible with language, stemming:true, remove_stopwords:true, case_sensitive:false, and highlighting.
- Stopword removal is not supported for arabic, greek, romanian, tamil, or turkish. Stopword list: https://snowballstem.org/algorithms/english/stop.txt
- `ascii_folding` is applied after stemming and stopword removal.

**Schema change rules** (https://turbopuffer.com/docs/write#updating-attributes):
- Online, in-place changes are allowed for `filterable`, `full_text_search` (params included), `regex`, `glob`, `fuzzy`, and `embed` (add or remove only, not modify).
- A write may be schema-only (`{"schema": ...}`) if the namespace exists.
- Changing the type of an existing attribute is an **error**. Other index setting changes and attribute deletions "currently cannot be done in-place". The spec, however, has `{"drop": true}`.
- After enabling an index, queries that depend on it return **409** until it's built.
- Changing FTS params triggers a background rebuild, and the old settings keep serving until it's done.
- The vector column count and types are fixed at namespace creation. There's a max of 8 vector columns, and embedded attributes count toward that.
- `distance_metric` applies to all vector columns in the namespace.

**Billing notes** (only useful for emitting `billing` fields): unindexed (non-filterable) attributes bill at 50%. Indexed attributes bill at logical size times the number of indexes, so `filterable` + `full_text_search` = 200%.

---

## 4. Write: `POST /v2/namespaces/:namespace` (https://turbopuffer.com/docs/write)

Namespace names must match `[A-Za-z0-9-_.]{1,128}`. A namespace is created implicitly by the first write that writes documents, or explicitly with `create_namespace`. The max request payload is **512 MB**.

### 4.1 Request fields (`Write` schema)

| Field | Type | Default | Semantics |
|---|---|---|---|
| `upsert_rows` | `Row[]` | | Each row is `{"id": Id, ...attrs}`. Overwrites whole documents |
| `upsert_columns` | `Columns` | | `{"id": [..], "<col>": [..], ...}`. Equal-length arrays; `null` for missing |
| `patch_rows` | `Row[]` | | Writes only the given keys. IDs that don't exist are ignored (no create). **Vectors can't be patched** |
| `patch_columns` | `Columns` | | Columnar patch, same rules |
| `deletes` | `Id[]` | | Delete by ID |
| `upsert_condition` | Filter (with `$ref_new`) | | Per-doc condition for upserts. If the doc exists and the condition is true, write. If it exists and the condition is false, skip. If it doesn't exist, **write unconditionally** |
| `patch_condition` | Filter | | For patches. Missing IDs are ignored without evaluating. Doesn't apply to `patch_by_filter` |
| `delete_condition` | Filter | | For `deletes`. `$ref_new` resolves to null for every attribute. Doesn't apply to `delete_by_filter` |
| `delete_by_filter` | Filter | | Delete every doc matching the filter |
| `delete_by_filter_allow_partial` | bool | false | If false and more than the limit match, **fail** and delete nothing. If true, delete up to the limit and set `rows_remaining: true` |
| `patch_by_filter` | `{"filters": Filter, "patch": {attr: value}}` | | Both keys required. Vectors can't be patched |
| `patch_by_filter_allow_partial` | bool | false | Same semantics as above, with a limit of 50k |
| `return_affected_ids` | bool | false | Adds `upserted_ids`, `patched_ids`, and `deleted_ids` (only successful writes) |
| `distance_metric` | `"cosine_distance"` \| `"euclidean_squared"` | | Required unless a copy or branch is set, or the namespace has no vector columns. `cosine_distance = 1 - cos_sim` (range 0..2); `euclidean_squared = sum((x-y)^2)`; lower is better for both |
| `schema` | `{attr: AttributeSchema}` | inferred | See section 3. May be sent on every write |
| `copy_from_namespace` | string \| `{source_namespace, source_api_key?, source_region?}` (+ `dest_encryption?` from the SDK) | | Destination must be empty, and the request can't contain documents or schema changes. Inherits encryption (unless `encryption` or `dest_encryption` is set), sharding (unless `sharding` is set), and `read_only`. May run async |
| `branch_from_namespace` | string \| `{source_namespace}` | | Instant copy-on-write clone. Destination must be empty. Inherits `read_only`, but not pinning |
| `create_namespace` | bool | unset | `true` creates the namespace even with no docs; requires the `id` type in `schema`; no-op if it exists. `false` never creates; **404** if missing. Unset means implicit creation |
| `sharding` | `{"num_shards": int>=1}` | | Only on the first write or a copy. 1..8 by default, up to 256. Docs are assigned by `hash(id) % num_shards` |
| `encryption` | `{"mode": "customer-managed", "key_name": "...", "encryption_context"?: {k: v}}` \| `{"mode": "default"}` | | CMEK (GCP resource name or AWS ARN). `encryption_context` is AWS only; its keys can't start with `tpuf:` or `aws:` |
| `disable_backpressure` | bool | false | Disables the 429 when unindexed data exceeds 2 GB or 1M rows. Only for upserts and delete-by-id. Strong queries then error above the threshold. Eventual queries search only the first 128 MiB unindexed |

Filter grammar is in section 6.3. `$ref_new` is `{"$ref_new": "attr_name"}` and can stand in for any value literal in a condition.

Examples:
```json
{"upsert_condition": ["Or", [["updated_at", "Lt", {"$ref_new": "updated_at"}], ["updated_at", "Eq", null]]]}
{"upsert_condition": ["id", "Eq", null]}
{"upsert_condition": ["version", "Lt", {"$ref_new": "version"}]}
```
The second is "insert if not exists". The third is a version check.

**Order of operations within one request:** `delete_by_filter` first, then `patch_by_filter` (which skips rows just deleted), then everything else (upserts, patches, and deletes). Patches to rows deleted earlier in the request are ignored. Any combination of fields is allowed in one request.

**Duplicate ID** anywhere in the request returns **400**.

**Atomicity:** "All writes in an upsert are applied simultaneously". Conditional writes are atomic (serializable). `patch_by_filter` and `delete_by_filter` behave like Read Committed: phase 1 finds IDs on a snapshot, and phase 2 re-evaluates the filter atomically on those IDs (https://turbopuffer.com/docs/guarantees). Writes are durable in object storage on return. WAL group commit batches writes for up to about 1s.

**Limits:** `delete_by_filter` max is 5M rows per request and `patch_by_filter` max is 50k. The "max ingested, unindexed data" is 2 GB, giving 429 until indexing catches up.

### 4.2 Response (`WriteResult`). Required: `status`, `message`, `rows_affected`, `billing`

```jsonc
{
  "status": "OK",                                 // const "OK"
  "message": "documents committed successfully",
  "rows_affected": 2,                             // upserted + patched + deleted
  "rows_upserted": 2,                             // only if upsert_* present
  "rows_patched": 0,                              // only if patch_* or patch_by_filter present; counts applied patches only
  "rows_deleted": 0,                              // only if deletes or delete_by_filter present; counts actual deletions
  "rows_remaining": false,                        // filter-based writes; true if more matches remain
  "upserted_ids": [1, 2],                         // only with return_affected_ids and when non-empty
  "patched_ids": [],
  "deleted_ids": [],
  "billing": {
    "billable_logical_bytes_written": 1234,       // required
    "query": {                                    // optional: conditional, patch_by_filter, delete_by_filter, or cross-region copy
      "billable_logical_bytes_queried": 0,
      "billable_logical_bytes_returned": 0
    }
  },
  "performance": {
    "server_total_ms": 12,                        // required in spec
    "embedding_tokens": 0,                        // native embedding only
    "embedding_ms": 0
  }
}
```
Copy success message: `"namespace cloned successfully"`. Older doc examples show just `{"status": "OK"}`.

### 4.3 Copy / branch details
- Copy can cross org (`source_api_key`), region (`source_region`, e.g. `"aws-us-east-1"`), and cloud.
- A copy is safe while the source is indexing: it includes committed writes and indexing progress, and the destination continues from there.
- Cross-region CMEK copies must specify a destination key.
- Copy throughput is about 72 MB/s.
- A branch is constant-time, fully independent afterwards, can be chained without limit, and the source can be deleted without affecting the branch.

---

## 5. Query: `POST /v2/namespaces/:namespace/query` (https://turbopuffer.com/docs/query)

The request body is `QueryConfig` (`vector_encoding`, `consistency`) + `Query` + optional `rerank_by`.

### 5.1 Request fields

| Field | Type | Default | Notes |
|---|---|---|---|
| `rank_by` | RankBy expr | | Required unless `aggregate_by` is set. Docs with a score of 0 are excluded |
| `top_k` | int | | Alias for `limit.total`. Max 10,000 |
| `limit` | int \| `{"total": int, "per"?: {"attributes": [str], "limit": int}}` | | Required (or `top_k`). `total` max 10,000. `per` gives diversification (max per limit-key value). With model `rerank_by`, it's the candidate count |
| `offset` | int | 0 | Skip N. Not for aggregations. `limit + offset` must be < 10,000. Spec: "only with explicit `rank_by` and `top_k` or `limit`" |
| `filters` | Filter | | Section 6.3. Required for `kNN` |
| `include_attributes` | `string[]` \| bool | `id` only | `true` returns all; `false` returns none (spec). Unknown names are presumably an error, since exclude says it is the one that silently ignores them |
| `exclude_attributes` | `string[]` | | Mutually exclusive with `include_attributes`. Unknown names silently ignored |
| `compute_attributes` | `{name: Expr}` | | Max 256. See 5.4 |
| `aggregate_by` | `{label: AggFn}` | | Mutually exclusive with `rank_by` and `include_attributes`. Max 8 functions |
| `group_by` | `(string \| {outName: ["ForEachUnique", arrayAttr]})[]` | | Only with `aggregate_by`. Max 10k groups (`limit.total`), sorted by group key |
| `rerank_by` | `{"model", "attribute", "query"}` (single query) | | Model reranking (turbopuffer-hosted models). `$dist` becomes the rerank score, sorted descending |
| `distance_metric` | DistanceMetric | | Present in the spec's `Query` (legacy); not in the docs |
| `vector_encoding` | `"float"` \| `"base64"` | `float` | Output vector format. base64 is LE f32 |
| `consistency` | `{"level": "strong"}` \| `{"level": "eventual", "max_staleness_seconds"?: 1..3600}` | strong | Eventual searches at most 128 MiB of unindexed WAL and reuses the node's cached view for `max_staleness_seconds` (default 60). The strong variant has `additionalProperties: false` |

### 5.2 `rank_by` grammar (spec `RankBy` anyOf)

**Vector:**
```jsonc
["vector", "ANN", [0.1, 0.2, ...]]                    // RankByAnn. $dist = distance, ascending (lower = better)
["tokens", "ANN", [[0.4, 0.3], [1.0, 0.0]]]           // RankByAnnMulti (late interaction): sum over query vectors of min distance to doc vectors
["content", "ANN", ["Embed", "foxes that jump"]]      // RankByAnnExpr (native embedding)
["vector", "ANN", ["Embed", "text", {"model": "voyage/voyage-4"}]]
["vector", "kNN", [...]]                              // exact search; REQUIRES filters (also Multi and Expr forms)
["sparse_vector", "SparseKNN", {"dim0": 0.2, "dim3": 0.1}]   // dot product, higher = better
```
The query vector must match the column dimension. kNN consumes 2 concurrency slots and aggregations consume 4.

**Text / score expressions** (`RankByText`, recursive). Scores are non-negative and higher is better:
```jsonc
["content", "BM25", "quick fox"]
["content", "BM25", ["tok1", "tok2"]]                          // pre_tokenized_array
["content", "BM25", "lazy wal", {"last_as_prefix": true}]      // prefix matches score 1.0
["Sum", [expr, expr, ...]]
["Max", [expr, expr, ...]]
["Product", 2.0, expr]          // weight must be non-negative
["Product", expr, 2.0]          // reversed form also allowed
<Filter>                        // any filter as a score: 1 if it matches, else 0 (e.g. ["species", "Eq", "whale"], ["company_name", "Fuzzy", ...])
["Attribute", "clicks"]         // numeric attr value as score; signed types (int, float) must be wrapped in Max or Saturate or they are rejected
["Saturate", expr, {"midpoint": 100, "exponent": 1}]   // x^e / (x^e + m^e), range [0, 1); max(x, 0) is implicit
["Decay", expr, {"midpoint": 2, "exponent": 1}]        // m^e / (x^e + m^e), range (0, 1]
["Dist", ["Attribute", "published_at"], "2026-02-03T12:13:14"]   // |value - origin|; for datetime the midpoint may be ms or "6h" (s, m, h, d, w)
```
`SparseKNN` composes with `Sum`, `Max`, `Saturate`, and so on.

**Order by attribute:**
```jsonc
["timestamp", "desc"]                         // or "asc"
[["category", "asc"], ["score", "desc"]]      // up to 8 keys; array attributes are not supported
```
Nulls and missing values sort **first in asc and last in desc**. Ties are unordered. No `$dist` is returned when ordering by attribute without reranking. A lookup is `["id", "asc"]`.

### 5.3 Aggregations (`aggregate_by`)
```jsonc
{"my_count": ["Count"],
 "my_sum":   ["Sum", "cool_score"],     // int, uint, or float
 "my_min":   ["Min", "attr"],           // any scalar type except bytes (new Nov 2026)
 "my_max":   ["Max", "attr"]}
// DEPRECATED: ["Count", "attr"] counts non-null values of attr
```
`group_by` example:
```json
{"group_by": ["color", "size"]}
{"group_by": [{"tag": ["ForEachUnique", "tags"]}, "status"]}
```
`ForEachUnique` explodes an array so that each unique element is its own group.

The docs warn that aggregations are "still being optimized" and are not recommended for latency-sensitive use on namespaces over 1M docs.

### 5.4 `compute_attributes` (`Expr`)
```jsonc
{
  "my_vector_score": ["vector", "VectorDist", [1.0, 2.0, 3.0]],
  "my_bm25_score":   ["content", "BM25", "pufferfish"],      // any RankBy expr ("Score")
  "my_highlights":   ["Highlight", "content", {
      "fragment_by": "sentence",            // word | sentence (default) | paragraph | none
      "rank_fragments_by": <rank_by using "$fragment" as the attr>,  // default is the query's rank_by
      "fragment_limit": 3,                  // default 3, max 1024
      "include_offsets": "utf-8"            // utf-8 | utf-16 | codepoints
  }]
}
```
Highlighting is **beta** and free during the beta. Highlight output per row is an array of
`{"text": str, "fragment_range"?: [start, end], "match_ranges"?: [[s, e], ...], "array_index"?: int}`.
Ranges are half-open and absolute within the attribute value, in the `include_offsets` unit. The ranges appear only when `include_offsets` is set. `array_index` is set for `[]string` attributes.
- BM25 highlighting needs `full_text_search` on the attribute. Fuzzy highlighting needs `fuzzy`.
- Matches never span fragment boundaries.
- Computed attributes never affect matching or ranking.

### 5.5 Multi-query (same endpoint; the SDK appends `?stainless_overload=multiQuery`)
```jsonc
{
  "queries": [ {Query}, {Query}, ... ],   // up to 16; each is a full Query without vector_encoding or consistency
  "vector_encoding": "float",             // root only
  "consistency": {"level": "strong"},     // root only
  "rerank_by": ["RRF"] | ["RRF", {"rank_constant": 60, "weights": [2, 1]}] | {"model", "attribute", "query"},
  "limit": 10 | {"total": 10},            // root: caps the combined reranked list
  "offset": 0                             // root: requires rerank_by and limit
}
```
- `queries` is mutually exclusive with the single-query fields.
- All subqueries run on the same snapshot (snapshot isolation).
- Each subquery counts against the concurrency limit.
- RRF score = sum over subqueries of `weight / (rank_constant + rank)`. Rank is 1-based. `rank_constant` is an integer >= 1, default 60. Weights must be > 0, default 1, with exactly one per subquery. RRF needs at least 2 subqueries and no aggregations.
- With `rerank_by`, `results` holds a **single** combined result.
- With model reranking, results are deduped by ID before scoring.

### 5.6 Response (`QueryResult`; `performance` and `billing` are required)
```jsonc
{
  "rows": [ {"id": 8, "$dist": 1.7, "extra_attr": "puffer"} ],   // present only when rank_by is set
  "aggregations": {"my_count": 42},                                // aggregate_by without group_by
  "aggregation_groups": [ {"color": "blue", "size": "small", "my_grouped_count": 2} ],  // with group_by: group-key fields plus labels
  "billing": {"billable_logical_bytes_queried": 0, "billable_logical_bytes_returned": 0},
  "performance": {
    "cache_hit_ratio": 1.0, "cache_temperature": "hot",   // hot | warm | cold
    "server_total_ms": 5, "query_execution_ms": 4,
    "exhaustive_search_count": 0,     // unindexed docs scanned
    "approx_namespace_size": 720,
    "last_included_write_at": "...",  // in the docs, not the spec
    "embedding_tokens": 0, "embedding_ms": 0              // optional
  }
}
```
Multi-query response (`MultiQueryResult`):
```json
{"results": [{"rows": [...]}, {"aggregations": {...}}], "performance": {...}, "billing": {...}}
```
- Rows are flat objects: `id`, then the requested attributes, then computed attributes, then `$dist`.
- Vectors come back as arrays, or as base64 with `vector_encoding: "base64"`.
- `$dist` is the vector distance for ANN/kNN, the score for BM25 and score expressions, the rerank score with `rerank_by`, and is omitted when ordering by attribute.
- The spec's `QueryPerformance` requires `cache_hit_ratio`, `cache_temperature`, `server_total_ms`, `query_execution_ms`, `exhaustive_search_count`, and `approx_namespace_size`. **Emit these always**, because some SDKs (Go, Java) may hard-require them.

### 5.7 Pagination (https://turbopuffer.com/docs/query#pagination and https://turbopuffer.com/docs/export)
- **Export or scan:** `rank_by: ["id", "asc"]`, `limit: 10000`, and `filters: ["id", "Gt", last_id]`. Loop until fewer than the limit come back. Docs inserted during the export are included.
- **Search pagination:** either exclude seen IDs (`["id", "NotIn", [...]]`) or use `offset`.

### 5.8 `POST /v2/namespaces/:namespace/explain_query` (spec only)
The body is the same as a single query. The response is `{"plan_text": "..."}`.

---

## 6. Filters (https://turbopuffer.com/docs/query#filtering, plus the spec's `Filter`)

### 6.1 Shape
A filter is a JSON array `[attr, Op, value]` or `[attr, Op, value, params]`, or a logical form:
```json
["And", [filter, ...]]
["Or", [filter, ...]]
["Not", filter]
```
Values must match the attribute type, or be an array of that type for set operators. `id` is filterable. Datetime values are ISO strings compared in ms. Filters can also appear inside `rank_by` (score 0 or 1) and in write conditions (with `$ref_new`).

### 6.2 Complete operator list

| Op | Value | Semantics |
|---|---|---|
| `Eq` | value \| null | Exact match. `null` matches docs **missing** the attribute. Works on `id`. On arrays (Aug 2025), it matches the array equal to the value; see the permissions guide note about `["groups", "Eq", []]` post-filtering |
| `NotEq` | value \| null | Inverse. `null` matches docs **having** the attribute |
| `In` | `[values]` | Value in list |
| `NotIn` | `[values]` | Value not in list |
| `Contains` | value | Array attribute contains the value |
| `NotContains` | value | Inverse |
| `ContainsAny` | `[values]` | Array intersects the list |
| `NotContainsAny` | `[values]` | Inverse |
| `Lt`, `Lte`, `Gt`, `Gte` | value | Numeric, lexicographic for strings, ms for datetimes. **Null quirk:** `Lt` and `Lte` match null attribute values (`Lt` doesn't if the operand itself is null). `Gt` and `Gte` don't mention nulls, so nulls sort lowest |
| `AnyLt`, `AnyLte`, `AnyGt`, `AnyGte` | value | Any array element satisfies the comparison |
| `Glob`, `NotGlob` | globset pattern | Unix glob on `string` and `[]string` (Rust `globset` syntax: `*`, `?`, `**`, `[..]`, `{a,b}`). Needs `glob: true` (or `filterable` for back-compat) |
| `IGlob`, `NotIGlob` | pattern | Case-insensitive glob |
| `Regex` | string | On `string`. Needs `regex: true`. Rust `regex` crate semantics (no look-around or backrefs). A trigram index pre-narrows candidates |
| `Fuzzy` | string, params `{"max_edit_distance": [{"min_query_chars": int, "distance": 0..2}, ...], "case_sensitive"?: bool (default true)}` | Fuzzy **substring** match by Levenshtein distance on `string` and `[]string`. Needs `fuzzy: true`. `min_query_chars >= 3*(distance+1)`. Queries shorter than the first threshold match nothing. Diacritic and case differences count 1 each, and case doesn't count when `case_sensitive` is false |
| `ContainsAllTokens` | string \| `[]string`, params `{"last_as_prefix"?: bool}` | All tokens present in any order. Needs FTS. On `[]string`, tokens may be spread across elements. Prefix matching compares bytes |
| `ContainsAnyToken` | string \| `[]string`, params `{"last_as_prefix"?}` | Any token present. Needs FTS |
| `ContainsTokenSequence` | string \| `[]string` | Phrase: tokens adjacent and in order. Needs FTS. Per array element, never across elements. Partial postfilter |
| `And`, `Or`, `Not` | | Logical |

Example:
```json
["company_name", "Fuzzy", "turbopufer", {"max_edit_distance": [{"min_query_chars": 3, "distance": 0}, {"min_query_chars": 6, "distance": 1}], "case_sensitive": false}]
```

Legacy (pre-v2 query API): filters could be an object `{"attr": [["Eq", v], ...]}`. See section 13.

---

## 7. Namespace metadata (https://turbopuffer.com/docs/metadata)

### 7.1 `GET /v1/namespaces/:ns/metadata` (docs) and `GET /v2/namespaces/:ns/metadata` (spec and SDK). Serve both.

Response (`NamespaceMetadata`). Required: `schema`, `approx_logical_bytes`, `approx_row_count`, `created_at`, `updated_at`, `encryption`, `index`.
```jsonc
{
  "schema": { "id": {"type": "uint"}, "vector": {"type": "[2]f32", "ann": {"distance_metric": "cosine_distance"}}, ... },
  "approx_logical_bytes": 4398046511104,
  "approx_row_count": 720,
  "created_at": "2024-03-15T10:30:45Z",
  "updated_at": "2024-04-16T09:27:32Z",       // data or schema last modified
  "last_write_at": "2024-03-19T09:12:14Z",    // docs only: data last modified
  "encryption": {"mode": "default"} | {"mode": "customer-managed", "key_name": "..."},
  "index": {"status": "up-to-date"} | {"status": "updating", "unindexed_bytes": 128, "unindexed_rows": 2},
  "pinning": {"replicas": 2, "status": {"updated_at": "...", "ready_replicas": 1, "replicas": 2, "utilization": 0.73}},  // only when pinned
  "branching": {"parent": "source-ns"},       // docs only; only for branches
  "sharding": {"num_shards": 8},              // only when sharded
  "read_only": true                           // omitted when false
}
```
Caveat: the curl example in the docs shows stale names (`approx_size_bytes`, `approx_num_rows`, and `"encryption": {"sse": true}`). The spec, the SDK, and the field reference all use `approx_logical_bytes`, `approx_row_count`, and `{"mode": ...}`. Use the spec names. Billed as a zero-row query. Approximate counts don't update while backpressure is disabled and the backlog is indexing.

### 7.2 `PATCH /v1/namespaces/:ns/metadata`
Request (`NamespaceMetadataPatch`):
```jsonc
{"pinning": {"replicas": 2} | true | false | null,   // a missing field means no change; null or false removes pinning; true uses defaults (replicas = 1)
 "read_only": true}                                   // rejects doc and schema writes; metadata PATCH still works; not a transactional barrier
```
Response: the full `NamespaceMetadata`. Pinning is a hosted feature, so blowfish can accept and echo it or reject it.

### 7.3 Schema endpoints (spec and SDK; older docs)
- `GET /v1/namespaces/:ns/schema` returns `{attr: AttributeSchemaConfig}`.
- `POST /v1/namespaces/:ns/schema` takes a body of `{attr: AttributeSchema}` (the desired changes) and returns the full updated schema `{attr: AttributeSchemaConfig}`.

---

## 8. List namespaces: `GET /v1/namespaces` (https://turbopuffer.com/docs/namespaces)

Query params:
- `cursor` (string; the previous `next_cursor`)
- `prefix` (string; `foo` matches `foo` and `foo-bar`)
- `page_size` (int, 1..1000, default 100; the docs mistype it as a string)

Response:
```json
{"namespaces": [{"id": "my-namespace"}, {"id": "test-namespace"}], "next_cursor": "MDk0ZGY4..."}
```
- `next_cursor` is present only when more pages exist. It is opaque; the example looks like base64 of the last namespace name.
- Results come back in lexicographic order (implied by the cursor).
- Listing is consistent (since July 2025).
- Requires the `list` or `admin` key role.

---

## 9. Delete namespace: `DELETE /v2/namespaces/:ns` (https://turbopuffer.com/docs/delete-namespace)
- Deletes all documents irrecoverably. After the 200, the name can be reused.
- Response: the docs example shows `{"status": "ok"}`, while the spec says const `"OK"`. **Return `"OK"`**, which matches the spec and what the SDK models parse. Some SDKs may be lenient.
- Deleting a source doesn't affect its branches, and vice versa.

---

## 10. Warm cache: `GET /v1/namespaces/:ns/hint_cache_warm` (https://turbopuffer.com/docs/warm-cache)
Response:
```json
{"status": "ACCEPTED", "message": "cache warm hint accepted"}
```
The spec gives HTTP **202** and requires `status` (const `"ACCEPTED"`). It's free if the namespace is already warm or warming; otherwise it's billed as a zero-row query. For blowfish this can prefetch index files from S3 to local disk.

---

## 11. Recall: `POST /v1/namespaces/:ns/_debug/recall` (https://turbopuffer.com/docs/recall)

Request:
```jsonc
{"num": 25,                     // default 25; number of sampled searches
 "top_k": 10,                   // default 10
 "filters": <Filter>,           // optional
 "rank_by": <RankBy>,           // optional; if set, num must be null or 1
 "include_ground_truth": false} // spec only
```
Response (the first three are required):
```jsonc
{"avg_recall": 1.0, "avg_exhaustive_count": 10.0, "avg_ann_count": 10.0,
 "ground_truth": [ {"query_vector": [...], "nearest_neighbors": [Row with $dist and vector]} ]}  // only with include_ground_truth
```
- It samples `num` random existing vectors as queries, runs ANN plus an exhaustive search, and reports recall = |ANN ∩ exact| / |exact|.
- Can run async (via `Prefer`).
- Billing: when avg_recall >= 0.9 and the namespace is non-empty, it bills one query per sample per 100K docs, with a minimum of `num`.

---

## 12. Limits table (https://turbopuffer.com/docs/limits)

| Metric | Limit |
|---|---|
| Max docs per namespace | 128B @ 256TB (sharded); a single index/shard is 500M docs @ 1TB (soft) |
| Max shards per namespace | 256 (8 by default) |
| Max namespaces | unlimited |
| Max pinned namespaces | 256 |
| Max vector columns per namespace | 8 (fixed at creation; embedded attributes count) |
| Max embedded attributes per namespace | 4 |
| Max dense vector dims | 10,752 |
| Max dims per sparse vector | 4,096 (total sparse dims unlimited) |
| Max write throughput per namespace | 10k writes/s @ 32 MB/s |
| Max namespace copy throughput | 72 MB/s |
| Max upsert batch request size | 512 MB |
| Max upsert batch with embedded attributes | 256 rows |
| Max rows affected by patch_by_filter | 50k (`rows_remaining`) |
| Max rows affected by delete_by_filter | 5M (`rows_remaining`) |
| Max ingested, unindexed data | 2 GB (then 429 on upserts); `disable_backpressure` mentions 2 GB or 1M rows |
| Max queries per namespace | 5k+/s |
| Max queries in a multi-query | 16 |
| Max concurrent queries per unpinned namespace | 16; the 17th waits up to 800ms, then gets 429. Aggregate/group-by = 4 slots, kNN = 2 slots |
| Vector recall@10 | 90-100% |
| Max attribute value size | 8 MiB (arrays: total bytes; element count unlimited) |
| Max filterable value size | 4 KiB (arrays: per element) |
| Max document size | 64 MiB |
| Max ID size | 64 bytes |
| Max attribute name length | 128 bytes; cannot start with `$` |
| Max attribute names per namespace | 1,024 |
| Max namespace name length | 128 bytes; `[A-Za-z0-9-_.]{1,128}` |
| Max full-text query length | 8,192 |
| Max `limit.total` (top_k) | 10,000 (`limit + offset` < 10,000) |
| Max aggregation groups per query | 10k |
| Max aggregate functions per query | 8 |
| Max order-by keys | 8 |
| Max computed attributes per query | 256 |
| Max fragments per highlight | 1024 (default 3) |
| Eventual-consistency unindexed window | 128 MiB; `max_staleness_seconds` 1..3600 (default 60) |
| Async operation result retention | 1 hour |

---

## 13. Deprecations, legacy API, and beta features

**Deprecated / legacy** (still worth tolerating if old clients matter):
- **v2 write API (April 2025)** and **v2 query API (May 2025)** replaced the v1 versions. Current docs only describe v2 for write and query. Legacy shapes, from Python SDK v0.1.x:
  - `POST /v1/namespaces/:ns` upsert, columnar: `{"ids": [...], "vectors": [[...] | base64 | null], "attributes": {"col": [...]}, "distance_metric", "schema", "encryption"}`. It also accepted a row form via `upserts`. **Delete** was done by upserting `vectors: null` for the IDs. `{"delete_by_filter": ...}` and `{"copy_from_namespace": "..."}` were also sent to this endpoint.
  - `POST /v1/namespaces/:ns/query`: `{"vector", "distance_metric", "top_k" (default 10), "include_vectors", "include_attributes", "filters" (array **or legacy object form** `{"attr": [["Eq", v]]}`), "rank_by", "consistency", "vector_encoding_format"}`. The response was rows of `{"id", "vector", "attributes": {...}, "dist"}`, nested and **not flat**.
  - `GET /v1/namespaces/:ns?cursor=` was the export, with a columnar response `{"ids", "vectors", "attributes", "next_cursor"}`.
  - `HEAD /v1/namespaces/:ns` was metadata in headers: `x-turbopuffer-dimensions`, `x-turbopuffer-approx-num-vectors`, and `x-turbopuffer-created-at`.
  - `DELETE /v1/namespaces/:ns` was namespace deletion (now v2), returning `{"status": "ok"}`. `DELETE /v1/namespaces/:ns/index` deleted indexes.
  - `/v1/vectors/*` was deprecated in favor of `/v1/namespaces` (Dec 2024).
  - Recommendation: implement v2 only, and optionally add a thin v1 shim later.
- `["Count", "attr"]` aggregate: deprecated, use `["Count"]`.
- `Glob` relying on `filterable` (instead of `glob: true`) is kept "for backwards compatibility".
- `remove_stopwords` default changed from true to false in Jan 2026 (the spec description still says true).
- The default tokenizer is upgraded over time (now `word_v4`). Persist the tokenizer explicitly per attribute so old namespaces keep their behavior.
- Old doc anchors `/docs/upsert` redirect to `/docs/write`.

**Beta / preview:**
- Highlighting (beta; free during the beta).
- Late interaction (`[][N]f32` with `ann.late_interaction`): private beta.
- Rust client: beta.
- `azure-eastus2`: private preview.
- Native embedding and sharding are GA (Aug and Sep 2026). Native reranking is GA (Oct 2026).
- Audit logs: opt-in beta.
- Aggregations are "still being optimized".

**Hosted-only features blowfish may stub or reject:**
- Native embedding (`embed` schema, `["Embed", ...]`) and model reranking (`rerank_by` with a model; models like `voyage/rerank-3`, `cohere/rerank-v3.5`, `zeroentropy/zerank-2`, `typesafe/jev-1.13.0`). These could be implemented by calling an external provider.
- Pinning, CMEK, cross-org and cross-region copy, and billing numbers (these can be computed approximately or zeroed).

---

## 14. Semantics worth matching exactly (gathered from all pages)

- **Consistency:** strong is the default, and a query sees every write acknowledged before it started. On strong queries, the WAL must be scanned exhaustively for unindexed data. A write is durable when it returns.
- **Patches never create docs.** Conditional patches and deletes skip missing IDs. Conditional upserts on missing IDs always insert.
- **Score-0 exclusion:** in `rank_by` score expressions (BM25, filters-as-score, and so on), documents with score 0 are dropped from results.
- **BM25 prefix:** `last_as_prefix` matches score a flat 1.0.
- **Ordering:** vector `$dist` ascending; BM25 and score expressions descending; RRF and rerank descending.
- **`include_attributes` default** is only `id` (plus `$dist`).
- **Nulls:** `Eq null` means missing; `NotEq null` means present; `Lt`/`Lte` include nulls; nulls sort first in asc order-by.
- **Columnar writes:** `null` marks a missing value. Arrays may be `[]`.
- **Datetime** input accepts a date-only form. Filters compare ms values. The output format is not specified in the docs; likely ISO 8601. Verify against a real instance if possible.
- **Write ordering in one request:** delete_by_filter, then patch_by_filter, then upserts, patches, and deletes.
- **`copy_from_namespace` and `branch_from_namespace`** require an empty destination. The error for a non-empty destination is 400 `"destination namespace already exists"`.
- **Sharding** is invisible to clients (`hash(id) % num_shards`). It only appears in metadata.
