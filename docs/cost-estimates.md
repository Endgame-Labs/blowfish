# Cost estimates

This page estimates what it costs to load, store, and query vector datasets with blowfish on AWS S3 and Cloudflare R2, alongside our 100M benchmark comparison with turbopuffer and our 10M run on Amazon S3 Vectors.

All figures are **estimates** derived from published list prices (checked 2026-11-20, with Cloudflare R2 checked 2026-11-15) and our benchmark run logs, not invoices. Measurements use 1,024-dim `f16` Cohere Wikipedia embeddings with text. Prices change; check the providers' pricing pages before budgeting.

## What it costs to run blowfish

blowfish costs are infrastructure only: one or more machines plus bucket storage and requests. As a worked baseline for 1,024-dim `f16` documents with text, storing 100M documents in AWS S3 costs about $22/month, plus a `c7gd.2xlarge` (about $265/month on demand) while you want it serving. A 10M dataset fits on a `c7gd.large` (about $66/month on demand).

| Scale | Machine (`us-west-2`, on-demand) | Monthly machine cost (24/7) | Monthly storage (list) | Per cold query (S3 requests) |
| --- | --- | --- | --- | --- |
| 10M | `c7gd.large` (2 vCPU, 4 GB, local NVMe) | ~$66 (`$0.0907/h`) | ~$1.30 on S3, ~$0.84 on R2 (`56.25 GB`, older layout) | Not separately logged |
| 100M | `c7gd.2xlarge` (8 vCPU, 16 GB) | ~$265 (`$0.3629/h`) | ~$21.70 on S3 (`~945 GB` at `$0.023/GB-month`) | ~$0.0005 (`~1,200` GETs, `~160 MB`) |

A few details behind those numbers:

- **Warm vs. cold queries:** At 100M in-region on S3, a fully cold query (empty caches) reads about 160 MB in about 1,200 GET requests (`1,200 / 1,000 * $0.0004 = ~$0.0005`), and same-region transfer to EC2 is free. Warm queries are served from memory and local NVMe cache and cost nothing extra in S3 charges.
- **Loading and reclustering:** Compute cost to build a namespace is small compared to running a node 24/7. At 10M, loading took `3,427 s` (about 1 hour, about $0.09 of compute) on `c7gd.large`, or `2,222 s` (about 37 minutes, about $0.11) on `c7gd.xlarge` (`$0.1814/h`). At 100M on `c7gd.2xlarge`, loading took 5.7 hours (`20,445 s` at `4,891 docs/s`, about $2.10 of instance time) and `recluster` took 1.7 hours (`6,248 s`, about $0.60). S3 request charges for the 100M load were not measured; at list prices, they are estimated at a few dollars.
- **Stopping the machine:** Because the bucket is the database and blowfish servers are stateless caches, you can stop the machine when you do not need to serve queries and pay only for storage—about $1.30/month on S3 (or $0.84/month on R2) at 10M, and about $22/month on S3 at 100M.

## Our 100M benchmark: blowfish vs. turbopuffer

We loaded the same 100M rows of 1,024-dim `f16` Cohere Wikipedia embeddings with text into both blowfish (in-region `c7gd.2xlarge` with same-region S3) and turbopuffer (loaded 2026-11-19 to 2026-11-20). Both columns are estimates from list prices and our run logs; we have not seen the turbopuffer invoice yet.

| Cost component (100M rows) | blowfish (`c7gd.2xlarge` + S3, in-region) | turbopuffer |
| --- | --- | --- |
| Initial load | ~$2.70 compute (`5.7 h` load ~$2.10 + `1.7 h` recluster ~$0.60) + unmeasured S3 requests (est. a few dollars) | ~$450–$901 est. (`450.4 GB` written; up to `$2.00/GB`, less up to 50% batch discount; invoice not yet seen) |
| Ongoing storage | ~$21.70/month (`~945 GB` at `$0.023/GB-month`) | Up to ~$81/month (`~245 GB` logical at up to `$0.33/GB-month`, ~$2.70/day) |
| Serving compute (24/7) | ~$265/month (`c7gd.2xlarge` on demand), or $0 when stopped | Included (monthly plan minimums apply; see [Prices used](#prices-used)) |
| Queries (benchmark) | ~$0.0005 per cold query; $0 extra warm; two ~4 h benchmark sessions ~$1.50 each at list (~$2.40 logged for one, including extras) | Cents for our benchmark queries (up to `$1.00/PB` queried, with 80%–96% marginal discounts) |

How the turbopuffer estimates work out:

- **Writes:** turbopuffer bills `f16` vectors at 4 bytes per dimension for writes (plus text) and reported `450.4 GB` of billable logical bytes written across the 100M rows (about `4.49 GB` per 1M rows). At the `$2.00/GB` ceiling, that is `450.4 * $2.00 = ~$901`. turbopuffer's per-request batch discount—`min(0.5, max(0, 0.2 * (log10(KB) - 1)))`, reaching 50% at about 3.2 MB batches—puts the floor at about `$450`. Our requests were batched, so the invoice is likely between `$450` and `$901`, though we have not seen it yet.
- **Storage:** For storage and queries, turbopuffer bills `f16` vectors at 2 bytes per dimension and reported about `2.4 GB` of logical bytes per 1M rows—about `245 GB` at 100M. At up to `$0.33/GB-month`, that is up to `245 * $0.33 = ~$81/month` (about `$2.70/day`).
- **Bulk load vs. steady state:** Bulk-loading 100M rows in one go is the worst case for per-GB write pricing. Once the data is loaded, keeping a `c7gd.2xlarge` running 24/7 for blowfish (`~$265/month` compute plus `~$22/month` storage, or `~$287/month`) costs more in monthly infrastructure than turbopuffer's `~$81/month` storage charge (and sits close to its `$256/month` Scale plan minimum). If you rebuild large datasets often or shut down compute between runs, blowfish's pay-only-for-storage model keeps costs low.

## Amazon S3 Vectors (10M)

On the same 1,024-dim `f16` Wikipedia dataset at 10M rows, our estimated cost for Amazon S3 Vectors was:

- **Upload:** about `$8` (`40 GB` logical at `$0.20/GB`, estimated from our run).
- **Loader compute:** about `$2.50` (about 12 hours of `c7gd.xlarge` at `$0.1814/h`).
- **Queries:** small query charges on top of upload and compute.

As with the other sections, these are estimates from our run logs rather than an invoice.

## S3 vs. R2

blowfish works against both AWS S3 and Cloudflare R2. The main pricing differences are storage and egress:

- **Storage:** AWS S3 Standard (`us-west-2`) is `$0.023/GB-month` (first 50 TB), while Cloudflare R2 Standard is `$0.015/GB-month` (plus a monthly free tier of `10 GB-month`, `1M` Class A operations, and `10M` Class B operations). On our measured `56.25 GB` 10M namespace (older layout), that is about `$1.30/month` on S3 vs. about `$0.84/month` on R2. At `945 GB` (100M), list storage is about `$21.70/month` on S3 vs. about `$14.20/month` on R2 (`945 * $0.015`).
- **Requests:** List rates are close—S3 PUT/LIST is `$0.005` per 1,000 (`$5.00/M`) vs. R2 Class A at `$4.50/M`; S3 GET is `$0.0004` per 1,000 (`$0.40/M`) vs. R2 Class B at `$0.36/M`.
- **Egress:** S3 data transfer in and same-region transfer to EC2 are free, while internet egress is about `$0.09/GB`. In an over-the-internet run from outside AWS at 100M, a cold query read about `300 MB`, which costs about `$0.03` per cold query (`0.3 GB * $0.09/GB`) in egress alone. R2 has free egress.

Run blowfish in the same AWS region as your S3 bucket when possible; if your compute sits outside AWS and reads over the internet, prefer R2 to avoid egress charges.

## What these numbers leave out

We love turbopuffer and use it on many projects. Comparing raw cloud infrastructure against a managed service leaves out the parts that matter most for many teams:

- **What turbopuffer's price buys:** a managed service—operations, durability, scaling, multi-tenancy, support, and no machines to run.
- **What blowfish's numbers omit:** your time running it, on-call, backups, and engineering.
- **Workload shape:** bulk-loading 100M rows in one go is the worst case for per-GB write pricing; steady workloads with ongoing writes and continuous serving look different.
- **Estimates, not invoices:** every number on this page is an estimate from list prices and our run logs. Actual bills depend on batch sizes, cache hit rates, plan minimums, and current provider pricing.

## Prices used

Published list prices used in the estimates above (check each provider's page for current rates):

- **AWS EC2 On-Demand** (`us-west-2`, Linux; checked 2026-11-20 — [AWS EC2 On-Demand Pricing](https://aws.amazon.com/ec2/pricing/on-demand/)):
  - `c7gd.large` (2 vCPU, 4 GB, local NVMe): `$0.0907/h` (~`$66/month`)
  - `c7gd.xlarge` (4 vCPU, 8 GB): `$0.1814/h` (~`$132/month`)
  - `c7gd.2xlarge` (8 vCPU, 16 GB): `$0.3629/h` (~`$265/month`)
- **AWS S3 Standard** (`us-west-2`; checked 2026-11-20 — [Amazon S3 Pricing](https://aws.amazon.com/s3/pricing/)):
  - Storage: `$0.023/GB-month` (first 50 TB)
  - Requests: `$0.005` per 1,000 PUT/LIST; `$0.0004` per 1,000 GET
  - Transfer: data transfer in free; same-region transfer to EC2 free; internet egress about `$0.09/GB`
- **Cloudflare R2 Standard** (checked 2026-11-15 — [Cloudflare R2 Pricing](https://developers.cloudflare.com/r2/pricing/)):
  - Storage: `$0.015/GB-month`
  - Requests: `$4.50` per million Class A (PUT, LIST); `$0.36` per million Class B (GET, HEAD)
  - Egress: free
  - Monthly free tier: `10 GB-month`, `1M` Class A, and `10M` Class B
- **turbopuffer** (checked 2026-11-20 — [turbopuffer Pricing](https://turbopuffer.com/pricing)):
  - Storage: up to `$0.33/GB-month` (logical bytes)
  - Writes: up to `$2.00/GB` written, with a per-request batch discount of `min(0.5, max(0, 0.2 * (log10(KB) - 1)))` (50% at about 3.2 MB batches), a 10 KB minimum per write, and a further 50% off non-filterable attributes
  - Queries: up to `$1.00/PB` queried, with an 80% marginal discount between 32 GB and 128 GB queried and 96% beyond 128 GB
  - `f16` vector billing: 2 bytes per dimension for storage and queries; 4 bytes per dimension for writes
  - Plan monthly minimums: Launch `$16`, Scale `$256`, and Enterprise from `$4,096` (with a 35% usage premium)
- **Amazon S3 Vectors** (from our run's estimate):
  - Upload: `$0.20/GB` logical
