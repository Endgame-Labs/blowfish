# SDK compatibility audit

Checked 2026-11-20 against the official SDKs' current sources (Python 3.1.0 from PyPI; TypeScript, Go, Java, Ruby, and
Rust cloned at HEAD) and the OpenAPI spec. "Test" names point at a scenario in `compat/` (run by
`scripts/compat_both.sh`) or a Rust test in `crates/blowfish/tests/`. Background is in `turbopuffer-sdks.md`.

Status: **Match** (implemented and tested), **Match, untested** (implemented, no dedicated test), **Gap fixed**
(fixed in this change), or **Open** (also listed in `SPEC.md` section 2).

## Retry and backpressure

| Behavior | SDKs | blowfish | Status and evidence |
|---|---|---|---|
| Retry on 408, 409, 429, and 5xx, plus connection errors and timeouts | all | Every transient error uses one of those statuses | Match |
| `x-should-retry: true` or `false` overrides the status rule | Python, TS, Go, Java, Ruby, Rust | Sent as `true` on 408, 409, 429, 502, 503, and 504, as `false` on 500 (corrupt data, which a retry cannot fix), and absent on other statuses. `ApiError::with_should_retry` overrides per error | Gap fixed. Rust test `retry_headers_by_status` |
| `retry-after-ms` read first (0 to 60 s honored in TS and Go, up to 30 min in Python, Java, Ruby, and Rust) | all | Sent on every 429 and 503, in whole milliseconds | Gap fixed. Test `python_retries_on_429` measures the wait |
| `Retry-After` in seconds (float or HTTP date) | all | Sent on every 429 and 503, as whole seconds rounded up, at least 1 | Gap fixed. Rust test `backpressure_429_carries_retry_headers` |
| Delay value from real state | n/a | For the unindexed-WAL limit: the backlog divided by the indexer's recent throughput (smoothed per publish), clamped to 0.5 to 30 s, and 1 s before the first measurement. Other 429 and 503 responses use 1 s | Gap fixed. `crates/bf-engine/src/drain.rs` |
| Retry count: 4 by default, 6 for TS `write()` | all | A backlog that clears within about 30 s survives the default four retries only when the delay is honored, which it now is | Match. Tests set `max_retries=6` so a slow indexer cannot fail them |
| Backoff when no header is sent: `0.25 s * 2^n`, at most 8 s, with jitter | all | Not applicable; headers are always present on 429 and 503 | Match |
| 409 is retried, so a permanent conflict must not use it | all | Every 409 blowfish returns is transient (an index still building, a copy source that changed, another indexer claiming the namespace) | Match |
| 5xx from a failed model provider | all | 502, 503, and 504 retry by default | Match |

## Wire format

| Behavior | blowfish | Status and evidence |
|---|---|---|
| Error body `{"status":"error","error":"..."}` with `Content-Type: application/json` on every status | One error type renders every error, including 404 for unknown routes, 405, 413, and 415 | Match. Helper `call` in `crates/blowfish/tests/http.rs` asserts the content type; scenarios `raw_invalid_json`, `raw_content_type`, `raw_unknown_route` |
| Success responses carry `Content-Type: application/json` | Yes | Match. Same helper |
| Error classes by status (400, 401, 403, 404, 409, 422, 429, 5xx) | Statuses follow the oracle | Match. `error_*` scenarios, and `python_no_retry_raises_rate_limit` for 429 |
| `Accept-Encoding: identity` (SDK default) | The response is not compressed | Match. tower-http `CompressionLayer` honors it |
| `Accept-Encoding: gzip` (SDK `compression=true`) | The response is gzip-compressed | Match, untested |
| Gzip request bodies (`Content-Encoding: gzip`, over 1024 bytes) | `RequestDecompressionLayer`, with the size limit applied to the compressed and the decompressed body | Match. Scenario `raw_gzip`; unit test in `crates/blowfish/src/lib.rs` |
| `?stainless_overload=...` and other unknown query parameters | Ignored; overloads are told apart by body shape | Match. Scenario `raw_unknown_params` |
| `Prefer: respond-async` | Honored for copy and recall: 202, `Location` on the same origin, `Preference-Applied: respond-async`, `{"token"}`, then poll | Match. Scenarios `async_prefer_write`, `async_prefer_copy`, `async_copy`, `async_copy_failure`; TypeScript smoke |
| Poll results kept 1 hour, then 404 | Same | Match. `crates/blowfish/src/operations.rs` |
| `start_copy_from` and `hint_cache_warm` return 202 without `Preference-Applied` | Same | Match. Scenarios `hint_cache_warm`, `async_copy` |
| Vector encoding: base64 f32 on writes, float or base64 on reads | Both | Match. Scenarios `base64`, `ann`; legacy smoke |
| IDs keep their JSON type (number, string, UUID) | Yes | Match. Scenarios `type_*` and `types_roundtrip` |
| Namespace in a percent-encoded path segment | Yes; names are validated | Match, untested for non-ASCII (names are ASCII-only) |
| `Authorization: Bearer <key>` | Checked when auth is on; any key is accepted when it is off | Match. `crates/blowfish/tests/auth.rs` |
| Stainless and telemetry headers (`X-Stainless-*`, `X-Request-Id`, `X-turbopuffer-Client-Telemetry`, `x-stainless-read-timeout`) | Ignored | Match. Every scenario sends them |
| `X-turbopuffer-Client-Telemetry-Sample-Rate` response header | Not sent; telemetry is off by default in every SDK | Match |
| Custom headers (`TURBOPUFFER_CUSTOM_HEADERS`) | Ignored | Match |
| Idempotency: no key is sent, so retried writes replay | Upserts, patches, and deletes by ID are idempotent; conditional writes can differ on replay, as at turbopuffer | Match |

## Client behavior

| Behavior | blowfish | Status and evidence |
|---|---|---|
| `TURBOPUFFER_BASE_URL` to `http://host:port` works; a path prefix works | Routes are absolute, so a prefix needs a proxy that strips it | Match for the base URL; document the prefix case. The README shows the setup |
| `TURBOPUFFER_REGION` must be unset when the base URL has no `{region}` | Client-side rule | Documented in the README |
| `exists()` reads `GET /v1/namespaces/{ns}/schema` and needs 404 | 404 for a missing namespace | Match. Scenario `error_missing`, `sdk_operations_smoke.py` |
| Namespace list pagination (`cursor`, `prefix`, `page_size` 1 to 1000, `next_cursor`) | Same | Match. Scenario `list_pagination` |
| Export by paging v2 queries (`id` asc, `Gt` filter) | Same | Match. Scenarios `export_full`, `export_page` |
| Timeouts: Python 60 s total and 5 s connect, TS 60 s, Java 1 min, Ruby 60 s | A write that waits on the group commit or a cold read returns well inside 60 s at the tested sizes. The server sets no deadline of its own | Match, untested at the limit |
| Async polling every 1 s within the client timeout | Operations finish well inside it | Match. TypeScript smoke and `sdk_operations_smoke.py` |
| Legacy Python 0.1.x and 0.4.x paths (HEAD metadata, export, v1 query) | Implemented | Match. `compat/legacy_smoke.py` |
| Legacy SDKs parse `Server-Timing` for cache and timing figures | Not sent; the SDKs treat the figures as absent | Open |
| Other SDKs (Java, Ruby, Go, Rust, C#) end to end | Only Python and TypeScript are exercised by tests. The others share the generated retry, error, and header code that was read for this audit | Open: add a Go or Rust smoke when one is wanted |

## What the audit found

1. **Missing retry headers (fixed).** blowfish's 429 and 503 responses carried no `Retry-After`, so the SDKs fell back to
   their own backoff (about 0.25 s growing to 8 s, with jitter). That works but ignores the server's own estimate, and a
   client could burn all four default retries before a 2 s drain finished. All error responses now go through one
   function that adds the headers.
2. **Only 500 had `x-should-retry` (fixed).** It is now set on the transient statuses too.
3. **Open items** are collected in `SPEC.md` section 2.
