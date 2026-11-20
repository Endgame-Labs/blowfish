# WORKER_REPORT: tpuf100m, branch wt/tpuf100m, unpushed

## Result
The same 100M rows are loaded into turbopuffer (`blowfish-oracle-wiki100m`, kept in place for the coordinator) and measured. Docs
section: "turbopuffer at 100M, measured by me" at the end of `docs/benchmarks.md`. Commits: scale_load billing and `--stop-after-units`,
5xx retry, docs plus `bench/scale/tpuf_warm.py`. The cold sampler never reported "cold" for its probe, so there is no cold p50 or p90;
the deep-cold sample is the coordinator's, after a day idle. Nothing needed in other crates.

## Load
| | |
|---|---:|
| Rows | 100,000,000 (English, de, fr, es, ru; ids match `oracle-100m.npz`) |
| Time | 52,998 s (14.7 h), 1,887 docs/s average; 2,100 to 2,400 early |
| Billable bytes written | 450,425,358,787 (450.4 GB; 4.5 GB per 1M rows) |
| Raw bytes sent | 451.5 GB |
| Namespace size (`approx_logical_bytes`) | 246.8 GB, `approx_row_count` 100,000,000 |
| 429 retries / transport retries | 4,846 / 64 |
| Index "up-to-date" | about 2 min after the last write |

Cost: $2/GB list is about $901 (the 1M check had projected $897). With the batch discount likely about $450 to $901; I have not seen the invoice.
A 502 from the integration crashed the loader at 60.67M rows; I made `send()` retry any 5xx like a 429 and resumed from the checkpoint (idempotent upserts).

## First query after indexing (2026-11-20, a few minutes after "up-to-date")
| Query | Client ms | server_total_ms | Temperature | Cache hit ratio | Recall@10 |
|---|---:|---:|---|---:|---:|
| 1 | 2,085 | 1,966 | cold | 0.15 | 1.0 |
| 2 | 1,433 | 1,327 | cold | 0.60 | 0.9 |
| 3 | 730 | 624 | cold | 0.66 | 1.0 |

## Idle-gap samples (tpuf_cold.py, stopped by the coordinator after the 19,200 s probe; no 6 h probe)
| Idle gap | server_total_ms | Client ms | Temperature | Recall@10 |
|---:|---:|---:|---|---:|
| 0 s (warm-up) | 14 | 132 | hot | 1.0 |
| 300 s | 16 | 334 | hot | 0.9 |
| 600 s | 44 | 373 | hot | 1.0 |
| 1,200 s | 131 | 441 | warm | 1.0 |
| 2,400 s | 145 | 435 | warm | 0.7 |
| 4,800 s | 170 | 459 | warm | 1.0 |
| 9,600 s | 202 | 482 | hot | 1.0 |
| 19,200 s | 503 | 782 | warm | 0.9 |

The probe never reported "cold", so there is no time-to-cold. The follow-up query after each probe (a different query, no idle) was cold after the 300 s and
600 s gaps (842 and 542 ms server) and warm after 19,200 s (429 ms). Those are in `cold.jsonl` format in the run's scratch (deleted); the figures above are complete.

## Recall and warm (oracle_compare.py --tpuf-only, oracle-100m.npz, 200 queries per type, one pass, then 1,000 warm)
| Query | Recall@10 | Client p50 / p90 / p99 | server_total_ms p50 / p90 / p99 | Temperature |
|---|---:|---|---|---|
| ANN | 0.947 | 310 / 525 / 636 | 232 / 426 / 535 | 14 hot, 186 warm |
| BM25 | | 215 / 430 / 805 | 159 / 373 / 750 | 11 cold, 12 hot, 177 warm |
| Hybrid | | 90 / 119 / 157 | 32 / 63 / 80 | 200 hot |
| Selective ANN | 0.995 | 445 / 682 / 1,218 | 358 / 590 / 1,110 | 4 hot, 196 warm |
| Broad ANN | 0.962 | 148 / 311 / 507 | 88 / 256 / 449 | 59 hot, 141 warm |

1,000 warm ANN queries at concurrency 1 (`bench/scale/tpuf_warm.py`, strong consistency, after one pass; all hot): client 70.2 / 75.6 / 115.6 ms,
server_total_ms 14 / 19 / 53.1 ms, recall@10 0.9465.

## Beside blowfish at 100M (c7gd.2xlarge in-region, from scale100m-report)
| | blowfish 48 probes (64) | turbopuffer (mine, over the internet) |
|---|---|---|
| ANN recall@10 | 0.941 (0.9555) | 0.947 |
| Warm ANN p50 / p90 ms | 333 / 378 (387 / 434) | 14 / 19 server, 70 / 76 client |
| First 200-query pass, ANN p50 / p90 | | 232 / 426 server, 310 / 525 client |
| Cold, cache-cold p50 / p90 | 2,698 / 3,071 (2,687 / 2,936) | not obtained |

At 10M turbopuffer's first-pass ANN server time was 88 / 203 ms; at 100M, 232 / 426 ms.

## Notes
- `.scratch/` cleaned (loader checkpoint, outputs, uv cache removed). `bench/scale/data/oracle-100m.npz` untouched.
- Quota used about 6% of the weekly limit. The namespace is the only thing left behind.
