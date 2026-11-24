# Prometheus metrics

The main HTTP listener serves `GET /metrics`. When auth is enabled, use an org superadmin key,
the break-glass `BLOWFISH_ADMIN_KEY`, or a legacy key with the `metrics` role. `--metrics-listen 127.0.0.1:9100`
serves a second `/metrics` without authentication. Bind that listener only to localhost or a trusted
monitoring network. Both expose the same recorder.

All crates emit through the `metrics` facade. The binary installs `metrics-exporter-prometheus`
once, so allocator, storage, cache, WAL, and engine instrumentation share the same endpoint. Names follow
`blowfish_<area>_<name>`. Base units are seconds, bytes, and counts (with `blowfish_segment_build_millis_total`
in milliseconds). Counters end in `_total`; histograms describe their measured quantity. Do not label
by namespace, object key, token, or raw URL.

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
| `blowfish_commit_stage_seconds` | Histogram | `stage` | Duration of write commit stages: `catch_up`, `for_writes`, `prepare`, `append`, or `replay` |
| `blowfish_segment_build_millis_total` | Counter | None | Cumulative wall time in milliseconds spent building segments |
| `blowfish_segment_build_rows_total` | Counter | None | Total rows built into immutable segments |
| `blowfish_ann_recall` | Histogram | None | Sampled overlap between ANN and exhaustive results (at most one comparison per `BF_RECALL_SAMPLE_INTERVAL_MS`, none in the first interval after startup) |
| `blowfish_recluster_running` | Gauge | None | 1 while an automatic recluster build runs in this process |
| `blowfish_recluster_builds_total` | Counter | `outcome` | Finished recluster builds: `installed`, `discarded` (the segments it covered changed before it could be installed), or `failed` |
| `blowfish_recluster_build_seconds` | Histogram | None | Time from a build's start until its tier was installed or discarded |
| `blowfish_recluster_rows_total` | Counter | None | Rows written into installed recluster tiers |
| `blowfish_recluster_deferred_total` | Counter | `reason` | Builds the trigger wanted but held back: `bulk_load`, `compaction`, `memory`, or `disk` |
| `blowfish_recluster_discarded_total` | Counter | `reason` | Built tiers dropped before installation: `coverage`, `timeout`, or `superseded` |
| `blowfish_recluster_tiers_stale_total` | Counter | None | Installed tiers retired because a segment they cover is gone with no lineage (a schema rebuild or a namespace deletion; compaction never causes one) |
| `blowfish_lineage_merges_total` | Counter | None | Compactions that merged tier-covered segments and recorded where their rows went |
| `blowfish_alloc_bytes` | Gauge | `kind` | jemalloc memory accounting refreshed on each `/metrics` scrape: `allocated`, `active`, `resident`, or `retained` |
| `blowfish_cache_hits_total` | Counter | `tier` | Cache block hits by `memory` or `disk` tier |
| `blowfish_cache_misses_total` | Counter | None | Cache block misses |
| `blowfish_cache_evictions_total` | Counter | `tier` | Evicted cache blocks by `memory` or `disk` tier |
| `blowfish_cache_bytes` | Gauge | `tier` | Accounted cache bytes by `memory` or `disk` tier |
| `blowfish_object_store_requests_total` | Counter | `op`, `status` | Backend operations by outcome |
| `blowfish_object_store_request_seconds` | Histogram | `op` | Backend operation duration |
| `blowfish_object_store_bytes_total` | Counter | `op` | Bytes transferred by backend operations |
| `blowfish_object_store_hedges_total` | Counter | `outcome` | Duplicate hedged ranged GETs (`outcome="issued"`) sent for tail latency |

`route` is the Axum route template, such as `/v2/namespaces/{ns}/query`; unknown routes use
`unmatched`. `key_name` is the configured key's display name, `super`, `development`, or
`unauthenticated`. Key names are visible to metrics readers, so avoid confidential names, and limit
key churn to control cardinality. Metrics appear after first use and reset on process restart.
Histograms export the usual `_bucket`, `_sum`, and `_count` series. The in-flight gauge includes the
scrape itself when scraping the main listener. All counters, gauges, and histograms describe this
node's activity, not deployment totals.

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
