# turbopuffer SDK wire protocol: research for blowfish

Researched 2026-11-14 from shallow clones of the official SDK repos (read only; nothing executed).

## 0. The gold: the OpenAPI spec is public

- Repo: https://github.com/turbopuffer/turbopuffer-openapi (Stainless-generated, "primarily for internal use, may break")
- Raw: https://raw.githubusercontent.com/turbopuffer/turbopuffer-openapi/main/openapi.yml (HTTP 200, OpenAPI 3.1, 2764 lines)
- Spec version read: (commit 33c99dc, 2026-10-28, "add encryption_context to customer-managed encryption")
- `.stats.yml` in every SDK only says `configured_endpoints: 16` (no spec URL). The spec repo is the source.
- The spec's comment says two endpoints are **deliberately left out**: `GET /v1/namespaces/{ns}` (export, deprecated in favor of paging with v2 queries) and `HEAD /v1/namespaces/{ns}` (deprecated). No current SDK calls them. Section 6 lists the legacy shapes.
- There is also `turbopuffer/turbopuffer-apigen` (Stainless extensions for the filter, rank_by, and expression tuple types), and `turbopuffer/tpuf-benchmark`.

## 1. Latest SDK versions (as cloned 2026-11-14)

| SDK | Version | Release date | HEAD commit date |
|---|---|---|---|
| turbopuffer-python | 3.1.0 (3.0.0 was 2026-10-23) | 2026-10-28 | 2026-10-29 |
| turbopuffer-typescript | 2.14.0 | 2026-10-29 | 2026-11-11 |
| turbopuffer-go | 2.10.0 (module v2) | 2026-10-20 | 2026-10-28 |
| turbopuffer-java | 2.12.0 | 2026-10-28 | 2026-10-28 |
| turbopuffer-ruby | 2.11.0 | 2026-10-29 | 2026-10-29 |
| turbopuffer-csharp | 0.12.0 | 2026-10-20 | 2026-10-28 |
| turbopuffer-rust | 0.1.0-beta.2 (hand-tuned Stainless + custom) | 2026-10-27 | 2026-11-13 |

All are generated from the same spec, so they share paths, bodies, retry rules, and error handling. The differences are noted below.

## 2. Base URL, region, env vars, and localhost

All SDKs work the same way:

- Default base URL template: `https://{region}.turbopuffer.com` (Go uses the literal placeholder `REGION`: `https://REGION.turbopuffer.com/`).
- Env vars: `TURBOPUFFER_API_KEY` (required), `TURBOPUFFER_REGION`, `TURBOPUFFER_BASE_URL`, `TURBOPUFFER_CUSTOM_HEADERS` (newline-separated `Name: value` lines merged into default headers in Python, TS, Go, and Ruby), `TURBOPUFFER_TELEMETRY` (off by default), and `TURBOPUFFER_LOG` (TS only). Java also reads the system properties `turbopuffer.apiKey`, `turbopuffer.region`, and `turbopuffer.baseUrl`.
- Constructor args: `base_url` / `baseURL` / `option.WithBaseURL` / `.baseUrl()`, `region`, and `api_key`.
- **Region rules (enforced on the client side, which matters for blowfish users):**
  - If the base URL contains `{region}` and no region is set, the client throws "region is required".
  - If the base URL does **not** contain `{region}` and a region **is** set (including from the `TURBOPUFFER_REGION` env var), the client throws "region is set, but would be ignored". So users who point at blowfish must **unset `TURBOPUFFER_REGION`**, or use a base URL with a `{region}` placeholder (for example `http://{region}.blowfish.internal:8080`).
- **`http://localhost:PORT` works.** Nothing forces https. Python adds a trailing `/` to the base path and joins relative paths. Go's `WithBaseURL` appends `/` to a non-empty path and resolves `v1/...` relative to it. TS concatenates `baseURL + path`. A path prefix such as `http://host:8080/tpuf` works in all of them.
- An API key is always required by the client (any non-empty string). blowfish can accept any token or check its own.
- The namespace goes in a path segment and is percent-encoded (Go `url.PathEscape`, Python `path_template` with the RFC 3986 segment set). Python rejects `.`/`..` segments.

## 3. Endpoints (all 16 configured endpoints, plus the legacy ones)

`{ns}` is the namespace name. Request bodies are JSON (`Content-Type: application/json`). Unless noted, a success is `200` with a JSON body. Every endpoint's `default` response is `ErrorResponse`.

**`?stainless_overload=...` is sent on the wire** by Python, TS, Go, Java, and Ruby. Rust does **not** send it. blowfish must **ignore unknown query params** and tell the overloads apart by body shape: `queries` present means multi-query, and `branch_from_namespace` / `copy_from_namespace` are ordinary write-body keys.

| SDK method | Method + path | Request body | Response |
|---|---|---|---|
| `client.namespaces()` (list) | `GET /v1/namespaces?cursor=&prefix=&page_size=` (page_size 1..1000) | none | `{"namespaces":[{"id":str}], "next_cursor"?: str}` |
| `write` | `POST /v2/namespaces/{ns}` | `Write` (§3.1) | `WriteResult` |
| `branch_from` | `POST /v2/namespaces/{ns}?stainless_overload=branchFrom` | `{"branch_from_namespace":{"source_namespace":str}}` (the SDK nests it) | `WriteResult` |
| `copy_from` | `POST /v2/namespaces/{ns}?stainless_overload=copyFrom` | `{"copy_from_namespace":{"source_namespace":str,"source_api_key"?,"source_region"?,"dest_encryption"?}}` (Python, TS, Ruby, and Java nest the whole param object, so `dest_encryption` lands **inside** `copy_from_namespace`; Go's CopyFrom drops it) | `WriteResult` |
| `start_copy_from` | `POST /v2/namespaces/{ns}/async?stainless_overload=startCopyFrom` | same nested `{"copy_from_namespace":{...}}` | **202** + `Location` header, `{"token":str}` |
| `poll_copy_from` | `GET /v1/namespaces/{ns}/operations/{token}?stainless_overload=pollCopyFrom` | none | `CopyFromNamespaceOperation` (§3.6) |
| `delete_all` | `DELETE /v2/namespaces/{ns}` | none | `{"status":"OK"}` |
| `query` | `POST /v2/namespaces/{ns}/query` | `QueryConfig` + `Query` + `rerank_by`? (§3.2) | `QueryResult` |
| `multi_query` | `POST /v2/namespaces/{ns}/query?stainless_overload=multiQuery` | `QueryConfig` + `{"queries":[Query...], "rerank_by"?, "limit"?, "offset"?}` | `MultiQueryResult` |
| `explain_query` | `POST /v2/namespaces/{ns}/explain_query` | `QueryConfig` + `Query` | `{"plan_text"?: str}` |
| `metadata` | `GET /v2/namespaces/{ns}/metadata` | none | `NamespaceMetadata` (§3.4) |
| `update_metadata` | `PATCH /v1/namespaces/{ns}/metadata` | `{"pinning"?: null\|bool\|{"replicas"?:int>=1}, "read_only"?: bool}` | `NamespaceMetadata` (see the v1 encryption caveat in §3.4) |
| `schema` | `GET /v1/namespaces/{ns}/schema` | none | `{attr_name: AttributeSchemaConfig}` |
| `update_schema` | `POST /v1/namespaces/{ns}/schema` | `{attr_name: AttributeSchema}` (the bare map; the SDK sends `schema` param as the body) | `{attr_name: AttributeSchemaConfig}` |
| `hint_cache_warm` | `GET /v1/namespaces/{ns}/hint_cache_warm` | none | **202** `{"status":"ACCEPTED","message"?:str}` |
| `recall` | `POST /v1/namespaces/{ns}/_debug/recall` | `{"num"?:int,"top_k"?:int,"filters"?:Filter,"include_ground_truth"?:bool=false,"rank_by"?:RankBy}` | `{"avg_recall":num,"avg_exhaustive_count":num,"avg_ann_count":num,"ground_truth"?:[{"query_vector":[num],"nearest_neighbors":[Row]}]}` |
| `exists()` (helper, Python and TS) | `GET /v1/namespaces/{ns}/schema` | none | true on 2xx, false on **404**. blowfish must return 404 for a missing namespace. |

### 3.1 `Write` body (POST /v2/namespaces/{ns}): every field is optional

```
upsert_columns: Columns            upsert_rows: [Row]
patch_columns:  Columns            patch_rows:  [Row]
deletes:        [Id]
upsert_condition / patch_condition / delete_condition: Filter-ish expression (any JSON)
distance_metric: "cosine_distance" | "euclidean_squared"
schema: {attr: AttributeSchema}
branch_from_namespace: str | {"source_namespace": str}
copy_from_namespace:   str | {"source_namespace": str, "source_api_key"?: str, "source_region"?: str}
delete_by_filter: Filter            delete_by_filter_allow_partial: bool
patch_by_filter: {"patch": {attr: value}, "filters": Filter}   patch_by_filter_allow_partial: bool
return_affected_ids: bool (default false)
encryption: Encryption              sharding: {"num_shards": int>=1}   (inaugural write only)
disable_backpressure: bool          create_namespace: bool
```
- `create_namespace`: true creates the namespace even with no docs (`schema` must declare the `id` type). false never creates it and returns 404 if it is missing. Omitted means the first write that has documents creates it.

`WriteResult` (required: `status`, `message`, `rows_affected`, `billing`):
```
{"status":"OK", "message":str, "rows_affected":int,
 "rows_upserted"?:int, "rows_patched"?:int, "rows_deleted"?:int, "rows_remaining"?:bool,
 "upserted_ids"?:[Id], "patched_ids"?:[Id], "deleted_ids"?:[Id],   // only with return_affected_ids and non-empty
 "billing":{"billable_logical_bytes_written":int, "query"?:QueryBilling},
 "performance"?:{"server_total_ms":int, "embedding_tokens"?:int, "embedding_ms"?:int}}
```

### 3.2 Query body

`QueryConfig`:
- `vector_encoding`: `"float"` | `"base64"` (for the response)
- `consistency`: `{"level":"strong"}` | `{"level":"eventual","max_staleness_seconds"?: 1..3600 (default 60)}`

`Query`:
```
rank_by: RankBy (any JSON)   top_k: int   offset: int (needs rank_by and top_k/limit)
filters: Filter              include_attributes: bool | [str]   exclude_attributes: [str]
aggregate_by: {label: AggregateBy}   group_by: [str | {out_name: ["ForEachUnique", attr]}]
compute_attributes: {name: Expr}     distance_metric: DistanceMetric
limit: int | {"total": int, "per"?: {"attributes":[str], "limit": int}}
```
- Single query only: `rerank_by: {"model":str,"attribute":str,"query":str}`.
- Multi-query: `rerank_by: ["RRF"] | ["RRF", {"rank_constant"?:int>=1 (default 60),"weights"?:[num>0]}] | {"model","attribute","query"}`, plus `limit: int | {"total":int}`, plus `offset`.

`QueryResult` = `SingleQueryResult` + required `performance` and `billing`:
```
{"rows"?: [Row],                      // present when rank_by is set; ordered by rank_by
 "aggregations"?: {label: value},     // with aggregate_by
 "aggregation_groups"?: [{...group key attrs..., ...aggregation labels...}],  // with group_by
 "billing": {"billable_logical_bytes_queried":int, "billable_logical_bytes_returned":int},
 "performance": {"cache_hit_ratio":num, "cache_temperature":"hot"|"warm"|"cold", "server_total_ms":int,
                 "query_execution_ms":int, "exhaustive_search_count":int, "approx_namespace_size":int,
                 "embedding_tokens"?:int, "embedding_ms"?:int}}
```
- All six `performance` fields and both `billing` fields are **required** by the spec. Send zeros if unknown.
- `MultiQueryResult`: `{"results":[SingleQueryResult], "performance", "billing"}`. With `rerank_by` it holds a single combined result.
- Rows carry a `$dist` key for the distance or score (Python types `$dist: float`, and README examples show `Row(id=1, vector=None, $dist=0.009, name='foo')`). Attributes are flattened into the row object next to `id`, `vector`, and `$dist`. Highlight compute attributes return `[{"text":str,"fragment_range"?:[s,e],"match_ranges"?:[[s,e]],"array_index"?:int}]`.

### 3.3 Grammar (JSON tuples): Filter / RankBy / Expr / AggregateBy

**Filters** (`[attr, op, value]`):
- `Eq` and `NotEq` (null means the attribute is missing / present)
- `In` and `NotIn` (array)
- `Contains`, `NotContains`, `ContainsAny`, and `NotContainsAny`
- `Lt`, `Lte`, `Gt`, and `Gte` (ints numeric, strings lexicographic, datetimes in ms)
- `AnyLt`, `AnyLte`, `AnyGt`, and `AnyGte` (array attributes)
- `Glob`, `NotGlob`, `IGlob`, and `NotIGlob` (Rust globset syntax)
- `Regex` (needs `regex:true` in the schema)
- `Fuzzy` as `[attr,"Fuzzy",str,{"max_edit_distance":[{"min_query_chars":int,"distance":0..2}],"case_sensitive"?:bool=true}]`
- `ContainsAllTokens`, `ContainsAnyToken`, and `ContainsTokenSequence` (value is a str or [str], optionally followed by `{"last_as_prefix":bool}`; ContainsTokenSequence takes no params)
- Combinators: `["Not", F]`, `["And", [F...]]`, and `["Or", [F...]]`

**RankBy:**
- Vector: `[attr,"ANN",[f32...]]`, `[attr,"ANN",[[f32...]...]]` (late interaction), `[attr,"ANN",Expr]`, and the same three forms with `"kNN"` (kNN requires `filters`). Sparse: `[attr,"SparseKNN",{"dim":weight}]`.
- Text: `[attr,"BM25",str|[str]]` (+ `{"last_as_prefix":bool}`), `["Sum",[R...]]`, `["Max",[R...]]`, `["Product",w,R]` / `["Product",R,w]`, a bare Filter (scores 1/0), `["Attribute",attr]`, `["Saturate",R,{"midpoint"?,"exponent"?}]`, `["Decay",R,{"midpoint"?,"exponent"?}]`, and `["Dist",R,origin]`.
- Attribute order: `[attr,"asc"|"desc"]` or a list of those.

**Expr:**
- `{"$ref_new":attr}`
- `["Embed",str]` and `["Embed",str,{"model":str}]`
- `[attr,"VectorDist",[f32]]`
- `["Highlight",attr]` and `["Highlight",attr,{"rank_fragments_by"?,"fragment_by"?:"none"|"sentence"|"paragraph"|"word","fragment_limit"?:int=3,"include_offsets"?:"utf-8"|"utf-16"|"codepoints"}]`
- any RankBy (its score)

**AggregateBy:** `["Count"]`, `["Sum",attr]`, and the deprecated `["Count",attr]`.

### 3.4 Schema and metadata types

- **`AttributeSchema`** = a type string (shorthand) | `AttributeSchemaConfig` | `{"drop":true}`.
- **`AttributeSchemaConfig`** (required: `type`):
  - `type`: `string|int|uint|float|uuid|datetime|bool|[]string|[]int|[]uint|[]float|[]uuid|[]datetime|[]bool|[DIMS]f16|[DIMS]f32|{}f16`, plus `[][N]f32` vector arrays. Rust's docs also mention `i8` vectors.
  - `filterable`, `regex`, `glob`, and `fuzzy`: bool.
  - `full_text_search`: bool | `{"k1"?:1.2,"b"?:0.75,"language"?:"english"...,"stemming"?:false,"remove_stopwords"?:true,"ascii_folding"?:false,"case_sensitive"?:false,"max_token_length"?:39,"tokenizer"?:"word_v4"|"word_v0..3"|"pre_tokenized_array"}`.
  - `ann`: bool | `{"distance_metric"?, "late_interaction"?:bool}`.
  - `sparse_knn`: `{"distance_metric":"dot_product"}`.
  - `embed`: str (model) | `{"model":str,"attribute"?:str,"dims"?:int}` | null.
- **`NamespaceMetadata`** (required: `schema`, `approx_logical_bytes`, `approx_row_count`, `created_at`, `updated_at`, `encryption`, `index`):
  ```
  {"schema":{attr:AttributeSchemaConfig}, "approx_row_count":int64, "approx_logical_bytes":int64,
   "created_at":RFC3339, "updated_at":RFC3339,
   "encryption": {"mode":"default"} | {"mode":"customer-managed","key_name":str,"encryption_context"?:{str:str}},
   "index": {"status":"up-to-date"} | {"status":"updating","unindexed_bytes":int,"unindexed_rows":int},
   "pinning"?: {"replicas"?:int, "status"?:{"updated_at","ready_replicas","replicas","utilization"}},
   "sharding"?: {"num_shards":int}, "read_only"?: bool (omitted when false)}
  ```
- **Caveat from the Rust SDK** (`src/custom/encryption.rs`): the real `PATCH /v1/.../metadata` returns the **v1 encryption shape**, `{"sse":true}` or `{"cmek":{"key_name":...}}`, and Rust decodes both shapes. blowfish should return the v2 `{"mode":...}` shape everywhere; every SDK accepts it.

### 3.5 Errors

- Body: `{"status":"error","error":"<message>"}` (both fields required in the spec; confirmed by the docs at turbopuffer.com/docs/api-overview).
- SDK exception classes by status: 400 BadRequest, 401 Authentication, 403 PermissionDenied, 404 NotFound, 409 Conflict, 422 UnprocessableEntity, 429 RateLimit, >=500 InternalServer, and other statuses map to the generic APIStatusError.
- **Always send JSON error bodies with `Content-Type: application/json`.** Go does `apierror.UnmarshalJSON(body)` on any status >= 400, and a non-JSON body surfaces as a JSON decode error instead of an APIError. TS builds its message from `error.message` or the JSON-stringified body.
- Success responses **must** carry `Content-Type: application/json`. TS parses the body as JSON only when the content type matches, otherwise it returns text. Python raises "Expected Content-Type ... application/json" in strict mode. Go errors for non-JSON content types.

### 3.6 Async operations (`Prefer: respond-async`)

- **Every request** from Python, TS, Go, Java, Ruby, C#, and Rust carries the header `prefer: respond-async`, unless the caller overrides it.
- blowfish may simply **ignore it and respond synchronously** (the docs call it a hint). The SDK only switches to polling when the response is `202` **and** has `Preference-Applied: respond-async`.
- If blowfish does implement it:
  - Reply `202` with a `Location: /v1/namespaces/{ns}/operations/{token}` header (same origin, otherwise the SDK rejects it to avoid leaking the key) and the body `{"token":...}`.
  - Poll responses (`GET`, sent without `prefer`): `{"status":"running","start_time":RFC3339,"progress"?:str}` or `{"status":"finished","start_time","finish_time","result":{"success":<original 200 body>} | {"error":{"status_code":int,"detail":{"status":"error","error":str}}}}`.
  - The SDK polls every 1s, within the client timeout. Results are kept for 1h, then 404.
  - Officially used for `copy_from_namespace` and recall.
- `hint_cache_warm` (202) and `start_copy_from` (202) return 202 **without** `Preference-Applied`, so they are not treated as async polling.

## 4. Headers the SDKs send

- `Authorization: Bearer <api_key>`
- `Accept: application/json`, plus `Content-Type: application/json` on bodies.
- `User-Agent`:
  - Python: `Turbopuffer/Python 3.1.0`
  - TS: `Turbopuffer/JS 2.14.0`
  - Go: `Turbopuffer/Go 2.10.0`, and on namespace handles `Turbopuffer/Go 2.10.0 (share-count=N)`
  - Ruby, Java, and others use similar Stainless patterns.
  - The legacy Python SDK sent `tpuf-python/<v> ...`.
- Stainless platform headers:
  - `X-Stainless-Lang`, `X-Stainless-Package-Version`, `X-Stainless-OS`, `X-Stainless-Arch`, `X-Stainless-Runtime`, `X-Stainless-Runtime-Version`, and `X-Stainless-Retry-Count` (0..N).
  - Python sends `X-Stainless-Async: false|async:asyncio` and `x-stainless-read-timeout: <secs>`.
  - TS, Go, and Rust send `X-Stainless-Timeout: <secs int>`.
  - Java also sends `X-Stainless-OS-Version` and `X-Stainless-Kotlin-Version`.
  - Ignore all of them.
- `prefer: respond-async` (see §3.6).
- **Compression**: all SDKs default to `compression=false` and send **`Accept-Encoding: identity`**.
  - With `compression=true`, Python, TS, and Rust gzip request bodies **larger than 1024 bytes** (gzip level 1 in Python) and set `Content-Encoding: gzip`. They also send `Accept-Encoding: gzip`.
  - Go and Ruby only change Accept-Encoding; Go's net/http then adds gzip and decodes it transparently.
  - **blowfish must accept `Content-Encoding: gzip` request bodies** and should honor Accept-Encoding (send identity when asked).
- **No idempotency header.** `idempotencyHeader` is unset in TS and Python, so no `Idempotency-Key` is sent.
- Optional telemetry (only when `TURBOPUFFER_TELEMETRY=1`): `X-turbopuffer-Client-Telemetry: {"version":1,"records":[...]}` and `X-Request-Id: tpreq_<base32>`. The server can return `X-turbopuffer-Client-Telemetry-Sample-Rate: 0..1`. Safe to ignore.
- Custom headers come from `TURBOPUFFER_CUSTOM_HEADERS` or `default_headers`.

## 5. Retries and timeouts

- **Retried statuses**: 408, **409**, 429, and every status >= 500, plus connection errors and timeouts.
  - The response header `x-should-retry: true|false` overrides that decision. This is a useful lever for blowfish, for example to send `false` on a non-retryable 5xx.
- **Max retries**: 4 by default (Python, TS, Go, Java, and Ruby). **TS `write()` uses `maxRetries: 6`.**
- **Backoff**: `0.25s * 2^n`, capped at 8s, with up to 25% negative jitter.
  - The SDKs honor `retry-after-ms` (milliseconds, non-standard) first, then `Retry-After` (seconds as a float, or an HTTP date).
  - The value is used only when 0 < value <= 60s in TS and Go, or up to 30 min in Python (`RETRY_AFTER_LIMIT_SECS = 1800`).
  - blowfish should send `Retry-After` on 429 and 503.
- Because retried POSTs carry no idempotency key, **writes can be replayed**. The upsert, patch, and delete semantics are idempotent by ID, except conditional writes.
- **Timeouts**:
  - Python: 60s total, 5s connect.
  - TS: 60000 ms.
  - Java: 1 min.
  - Ruby: 60s.
  - Go: no per-request timeout by default, with a 10 min `ResponseHeaderTimeout` on its default transport.
  - Python connection pool: 512 keep-alive connections, 60s keepalive.

## 6. Legacy and deprecated endpoints (pre-Stainless Python v0.4.1; not in current SDKs)

- `HEAD /v1/namespaces/{ns}`: metadata in the response headers `x-turbopuffer-dimensions`, `x-turbopuffer-approx-num-vectors`, and `x-turbopuffer-created-at`.
- `GET /v1/namespaces/{ns}?cursor=<c>` (**export**): columnar `{"ids":[...], "vectors":[[...]]|null, "attributes":{name:[...]}, "next_cursor":str|null}`.
- `DELETE /v1/namespaces/{ns}` (old delete-all) and `DELETE /v1/namespaces/{ns}/index`.
- `GET /v1/namespaces/{ns}/_debug/recall?num=&top_k=` (old recall, sent as GET with query params).
- Old clients parsed the `Server-Timing` header (`cache;hit_ratio=..;temperature=..`, `exhaustive_search;count=..`, `processing_time;dur=..`, `query_execution_time;dur=..`).
- v0.4.x already wrote and queried through `/v2/namespaces/{ns}` and `/v2/namespaces/{ns}/query`, and its retries covered the same codes (>=500, 408, 409, 429).
- Supporting export or HEAD is optional. Modern SDKs export by paging v2 queries, for example `rank_by: ["id","asc"]` with an `id` `Gt` filter.

## 7. Vector encoding, IDs, and rows vs columns

- **Vector on the wire** (`Vector`): either a JSON array of numbers (f32 width) or a **base64 string of little-endian float32 bytes** (standard alphabet, padded). Python: `struct.pack("<{n}f")` + `b64encode`.
- **Writes are base64 by default in Python and Rust.**
  - Python (`_utils/_transform.py::_encode_write_vectors`) and Rust (`src/custom/vectors.rs`) rewrite upserts before sending them.
  - Which attributes get encoded: the `vector` attribute, plus any attribute that **this same request's** `schema` gives `ann` (not false), except `[]...` vector-array types.
  - Where: `upsert_rows[*]` and `upsert_columns` (each element of the column becomes one base64 string). `patch_*` is **not** encoded.
  - Python only encodes when the first element is a float (an int list is left as is). Rust encodes any all-number array.
  - TS, Go, Java, and Ruby send whatever the user passes, usually float arrays.
  - Per Rust's doc comment, **the API accepts f32-LE base64 for every vector type, including `f16` and `i8` attributes**, and converts it. blowfish must therefore decode base64 as f32 regardless of the declared element type, and infer dims as `bytes/4` for untyped `vector`.
- **Reads**: rows return float arrays unless `vector_encoding:"base64"`. Then the API encodes the elements **in the vector's own type** (f16 as 2-byte LE, f32 as 4-byte). No SDK decodes it automatically; users get the string. Vectors appear only when included (`include_attributes` true or containing `vector`).
- **Columns format**: `{"id":[Id...], "vector"?: [Vector...] | Vector, "<attr>": [values...]}`. A single flat `Vector` is all docs' elements back to back, accepted only as a number array. Missing values are `null`.
- **Rows format**: `[{"id":Id, "vector"?:Vector, "<attr>":value, ...}]` (`additionalProperties: true`).
- **Id**: a UUID string | an arbitrary string | an unsigned 64-bit integer.
  - Go's response `ID` union holds `String` or `Int int64`.
  - TS `ID = string | number`, so u64 above 2^53 loses precision in JS.
  - blowfish should return IDs in the same JSON type they were written in (a number stays a number, a UUID stays a string).
- **Datetime** attributes: ISO-8601 strings (Python serializes `datetime.isoformat()`).
- **Request JSON** is compact (orjson when available in Python).

## 8. Practical checklist for blowfish compatibility

1. Implement the 16 routes in §3. Ignore `stainless_overload` and other unknown query params.
2. Return `Content-Type: application/json` everywhere, including errors shaped `{"status":"error","error":"..."}`.
3. Return 404 for missing namespaces on `GET /v1/.../schema`, because `exists()` depends on it.
4. Accept gzip request bodies. Respect `Accept-Encoding: identity`.
5. Accept base64 f32-LE vectors and float arrays for any vector attribute.
6. Fill every required response field: `WriteResult.status/message/rows_affected/billing`, `QueryResult.performance` (all six fields) and `billing`, and the required `NamespaceMetadata` fields.
7. Ignore `prefer: respond-async`, or implement it with 202 + `Preference-Applied` + `Location`.
8. Use 429 or 503 with `Retry-After` for backpressure. Avoid 409 for non-retryable conflicts, because the SDKs retry 409.
9. Tell users to set `TURBOPUFFER_BASE_URL=http://host:port` **and unset `TURBOPUFFER_REGION`**.
