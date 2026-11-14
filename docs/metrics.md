# Prometheus metrics 🐡

The main HTTP listener serves `GET /metrics`. When auth is enabled, use the super key or a key with
`metrics`. `--metrics-listen 127.0.0.1:9100` serves a second `/metrics` without authentication. Bind
that listener only to localhost or a trusted monitoring network. Both expose the same recorder.

All crates emit through the `metrics` facade. The binary installs `metrics-exporter-prometheus`
once, so storage, cache, and WAL instrumentation share the same endpoint. Names follow
`blowfish_<area>_<name>`. Use base units: seconds, bytes, and counts. Counters end in `_total`;
histograms describe their measured quantity. Do not label by namespace, object key, token, or raw URL.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `blowfish_http_requests_total` | Counter | `route`, `method`, `status`, `key_name` | Completed HTTP requests, including authorization failures |
| `blowfish_http_request_duration_seconds` | Histogram | `route`, `method`, `status`, `key_name` | Time through authorization and handler, until response headers are ready |
| `blowfish_http_requests_in_flight` | Gauge | None | Requests currently in authorization or handlers; cancellations decrement it |
| `blowfish_write_rows_total` | Counter | `operation` | Successful row operations: `upserted`, `patched`, or `deleted`; conditional no-ops add zero |
| `blowfish_query_requests_total` | Counter | `kind` | Successful queries: `ann`, `knn`, `bm25`, `order`, `aggregate`, or `score`; multi-query counts each component |
| `blowfish_query_rows_scanned_total` | Counter | None | Sum of successful responses' `performance.exhaustive_search_count` |
| `blowfish_wal_commit_duration_seconds` | Histogram | `outcome` | Each WAL append attempt, including serialization, payload, slot, and losing-payload cleanup; `success`, `conflict`, or `error` |
| `blowfish_wal_batch_requests` | Histogram | None | Atomic requests grouped into each newly committed WAL slot; tombstones have zero |
| `blowfish_wal_cas_conflicts_total` | Counter | None | WAL slots won by another writer |
| `blowfish_wal_bytes_written_total` | Counter | None | Encoded payload and slot bytes newly created; includes losing payloads, excludes retry hits, storage framing, and transport overhead |

`route` is the Axum route template, such as `/v2/namespaces/{ns}/query`; unknown routes use
`unmatched`. `key_name` is the configured key's display name, `super`, `development`, or
`unauthenticated`. Key names are visible to metrics readers, so avoid confidential names, and limit
key churn to control cardinality. Metrics appear after first use, and reset on process restart.
Histograms export the usual `_bucket`, `_sum`, and `_count` series. The in-flight gauge includes the
scrape itself when scraping the main listener. Counters describe this node, not deployment totals.

Other crates should reserve `blowfish_cache_*` for cache activity and `blowfish_object_store_*` for
backend operations, using this same facade and base-unit convention. Those metrics are supplied by
the cache/storage implementation; they are not fabricated by this HTTP adapter.

Example scrape configuration (replace the placeholder through your secret provisioning):

```yaml
scrape_configs:
  - job_name: blowfish
    static_configs:
      - targets: ["127.0.0.1:8000"]
    authorization:
      type: Bearer
      credentials_file: /run/secrets/blowfish_metrics_key
```

A useful request-rate expression is `sum by (route, status) (rate(blowfish_http_requests_total[5m]))`.
For p95 latency, use
`histogram_quantile(0.95, sum by (le, route) (rate(blowfish_http_request_duration_seconds_bucket[5m])))`.
