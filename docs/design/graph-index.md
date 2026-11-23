# Closing the throughput gap with Qdrant

Status: proposed (2026-11-23). Answers SPEC decision 24 ("a graph index to compete with Qdrant, design doc first").
Follows decision 25: one layout and one query path, reached by automatic upgrade, with no setting that picks the
old one.

## The problem, in numbers

BEIR NQ (2.68M x 1,024 f16, cosine), recall@10 about 0.95, 16 clients (iterations 19 and 20):

| Machine | blowfish (reclustered) | Qdrant 1.20, HNSW m 16, `hnsw_ef` 64 | Gap |
|---|---:|---:|---:|
| VM, server pinned to 6 vCPUs | 573 q/s (54 probes) | 2,565 to 3,164 q/s | 4.5 to 5.5x |
| Apple M4, 10 cores shared with clients | 870 q/s (64 probes, iteration 19 code) | 7,572 q/s | 8.7x |
| Peak RSS on the M4 | 5.4 GB (4 GiB of it is the block cache setting) | 16.1 GB (including mapped files) | |

Turned into CPU per query (vCPUs divided by q/s, which assumes the server is saturated, as it is at 16 clients):

| | vCPU-ms per query |
|---|---:|
| blowfish, VM, iteration 20 | 6,000 / 573 = 10.5 |
| blowfish, VM, iteration 19 | 6,000 / 395 = 15.2 |
| Qdrant, VM | 6,000 / 2,565 to 3,164 = 1.9 to 2.3 |

What the index work costs inside those 10.5 ms:

- The fast-scan kernel costs about 5 ns a row on the VM (iteration 20). At about 118,000 rows scanned per query
  (iteration 19's count at 64 probes; fewer at 54), the scan is roughly 0.5 to 0.6 ms.
- Rerank is 32 x k = 320 exact distances over f16 vectors that are already in the block cache: tens of
  microseconds of arithmetic, plus the cache lookups and checksum checks.
- Ranking 13,391 leaf centers by one-bit codes was 1% of CPU after iteration 19.

So by this arithmetic **the index (scan, ranking, and rerank arithmetic) is about a tenth of a blowfish query's
CPU**, and the other ~9 ms is per-query overhead: HTTP and JSON, snapshot and state handling, per-slab task
setup, block-cache lookups and checksums, allocation, and hydration. Iteration 21 is profiling exactly
this; its breakdown replaces this estimate when it lands. The consequence holds whatever the exact split: **a
graph index on its own cannot close the gap.** Even a free index leaves blowfish near 6,000 / 9.5 = 630 q/s on
the VM. Qdrant's whole query, HTTP included, costs about 2 ms.

## What has to stay true

- Object storage is the only source of truth, and a cold query costs a fixed handful of waves (SPEC §1 and §5).
  Every graph hop that misses the cache is a dependent object-store read: 25 to 125 ms in-region (iteration 17
  measured 100 to 125 ms p50 for small ranged reads of objects that aren't hot). A cold query cannot walk a graph.
- Memory well below Qdrant's. Qdrant keeps every vector in RAM (5.5 GB of f16 at NQ, plus about 0.35 GB of
  level-0 links at m 16). blowfish's warm working set is its codes (144 bytes a row, 0.39 GB at NQ), its
  directory, and the rerank vectors that queries actually touch.
- The 10M memory target: load and recluster within a 4 GB machine (SPEC §6).

## Options considered

### A. Better probing on the current IVF-RaBitQ layout

Smaller leaves scan fewer rows at the same recall. Iteration 12 measured this offline at 1M (rows scanned to
recall 0.943): 55K at about 1,000 rows a leaf, 39K at 300, 16K at 100. Iteration 20 tried 100-row leaves on NQ
and got 0.955 at 38 probes, but ranking twice as many centers ate the gain (3 to 4% slower). Iteration 20's own
conclusion: "a finer IVF pays only with a cheaper center search, such as a small index over the centers."

- Gain: fewer rows scanned; the cold path benefits too (fewer bytes per probe).
- Cost: more leaves to rank and more, smaller ranges per cold query.
- Ceiling: the scan is already a small share of a query, so on its own this is worth maybe 5 to 10%.

### B. A graph over leaf centers, held in memory (the SPANN arrangement)

SPANN and SPFresh keep small posting lists on disk and an SPTAG graph over the posting-list centers in memory.
For blowfish, that means a proximity graph (Vamana-style, out-degree 16) over the leaf centers. Ranking is then a
beam search that evaluates a few thousand centers, rather than a sort of every center in the directory or in
the pages the query fetched.

- It makes option A pay: leaves of 32 to 64 rows become affordable, so a query scans fewer rows to reach 0.95.
- Memory: about 250 bytes a leaf (the one-bit center code, 16 neighbor IDs, and the entry fields), so 10 MB at NQ
  with 64-row leaves, 39 MB at 10M, and 390 MB at 100M.
- Cold queries keep reading the paged directory's header and the pages of their nearest cells. The graph is a
  section of those same pages, so a cold query walks only the edges inside pages it already fetched, and never
  hops to the object store.
- Build: the nearest neighbors of each leaf center among the centers of its own and adjacent cells. Recluster
  already computes those cells for cross-cell assignment. This costs seconds at NQ and minutes at 100M.

### C. A graph over every row, built per slab and stored in S3 (DiskANN layout)

DiskANN keeps PQ codes in RAM and puts each node's full vector and adjacency list together in one 4 KB disk
sector; a query walks about 8 to 10 hops with a beam of 4 to 8.

- Cold from S3: each hop is a dependent read, so 8 to 10 waves of 25 to 125 ms, which is 4 to 10 times today's
  cold budget. Ruled out, as SPEC §5 already says.
- Warm, with the graph in memory: it beats the scan on CPU, but per-row random access fits blowfish's codes
  badly. The fast-scan layout transposes 32 rows into a 4 KB block, so estimating one row touches the whole block.
  The fix, as in SymphonyQG (SIGMOD 2025), is to store each node's neighbors' codes beside the node in fast-scan
  order: 32 x 148 bytes, which is about 4.7 KB a row, or 12.7 GB at NQ. That is more than Qdrant uses.
- A row graph that shares the existing codes (128 bytes of adjacency a row: 0.34 GB at NQ, 1.3 GB at 10M, 12.8 GB
  at 100M) is affordable in memory. It saves about 0.4 ms of scan per query, which is under 5% of today's query.

### D. HNSW over hot clusters only

This is a cut-down C for the slabs that queries touch most. It has the same per-row access problem, and it adds a
residency-dependent second path for the same rows. Ruled out under decision 25.

### E. The status quo plus per-query overhead work

This is the largest lever by the arithmetic above, and it has nothing to do with the index.

## Recommendation

One query path, in this order:

1. **Cut the per-query overhead first** (iteration 21 and the round after it). The target is 5 vCPU-ms per query
   on the VM, which is about 1,200 q/s at the same recall, with no format change. Until this is done, no index
   change can show up in q/s.
2. **Then a graph over leaf centers inside the paged directory, with 64-row leaves (options A and B together).**
   This becomes the only directory format at every namespace size (see `simplify.md`: the flat slab directory and
   the `BF_ANN_PAGED*` thresholds go). A query ranks the coarse cells from the header, fetches their pages (cold) or
   finds them resident (warm), and runs a beam search over the leaf graph from the best cell's entry leaves. It
   probes the leaves the search returns. A probe now covers about a third of the rows, so the default
   `ann_nprobe` has to be re-derived from G2's curve to keep recall@10 at 0.95 or better.
3. **No row-level graph.** Option C's warm-only variant is not worth its memory or a second path. Revisit it only
   if, after steps 1 and 2, scan plus rerank exceed half of query CPU, measured.

What is realistic:

- **VM:** 1,200 to 1,500 q/s at recall@10 0.95, about half of Qdrant's 2,565 to 3,164, with a resident working
  set under 1 GB beyond the block cache, against Qdrant's 5.5 GB or more.
- **M4:** 2,000 to 3,000 q/s against Qdrant's 7,572.
- **Matching Qdrant** would need every vector in RAM and an in-memory row graph, which means giving up the memory
  targets and building a second, warm-only path. I don't recommend it. blowfish's case is "close enough warm, and
  it also works cold from S3 at a fraction of the memory."
- **SPEC §5 still holds** ("No graph indexes" for the reasons it gives): the proposed graph is over centers, lives
  in pages a query reads anyway, and never costs an object-store hop. If Kyle accepts this, §5 should say "no
  row-level graph; a center graph inside the directory".

## On-disk and object-store format changes

- **Paged slab directory, version 2** (written by recluster, which becomes automatic; see `auto-recluster.md`):
  - Each entry gains 16 neighbor entry IDs (u32, 64 bytes).
  - The header gains, per cell, the entry IDs of its two leaves nearest the cell center (the beam's starting
    points).
  - Page boundaries are unchanged: edges to entries on pages the query did not fetch are skipped on a cold query.
    The cell tour keeps most neighbors on the same or adjacent pages; G2 below measures how many are lost.
  - A small namespace (under 100,000 leaves today) also gets the paged format, with as few cells as it needs.
- **Slab leaves of 64 rows** (from 200). Each leaf carries an exact rotated f32 center (4,148 bytes), which is about
  65 bytes a row at 64 rows against 21 at 200. With the single ANN object of `disk-footprint.md` that is one copy:
  +44 bytes a row, or 0.44 GB at 10M and 4.4 GB at 100M.
- No change to segment objects, documents, the WAL, or text indexes.

## Compatibility and migration

- **Old namespaces:** a slab set whose directory is the flat format or paged version 1, or whose leaves are 200
  rows, is "older than current". The automatic recluster treats that as a rebuild trigger (its gain estimate is
  forced above the threshold), so every namespace converges on the new layout without anyone running anything.
  Until its rebuild publishes, the namespace reads its old directory through the upgrade-only reader.
- **Older binaries:** version 2 pages carry a new magic, so a 0.1.14-or-earlier binary fails loudly on them and
  never misreads them. Older writers must not run against an upgraded bucket at all (see the manifest fence in
  `simplify.md`). There is no path back to the old format, per decision 25.

## Memory and S3 cost

| | NQ (2.68M) | 10M | 100M |
|---|---:|---:|---:|
| Leaves at 64 rows | 42K | 156K | 1.56M |
| Leaf graph and entries, resident when warm | about 10 MB | 39 MB | 390 MB (cold: only the pages touched, 4 MB header plus about 20 MB of pages) |
| Codes, resident when warm (unchanged) | 0.39 GB | 1.44 GB | 14.4 GB (warm only for hot cells; 100M doesn't fit a 16 GB box whole) |
| Extra centers from 64-row leaves (S3) | 0.12 GB | 0.44 GB | 4.4 GB |
| Extra S3 cost a month ($0.023/GB) | under $0.01 | $0.01 | $0.10 |
| Extra build time | seconds | about a minute | about 10 minutes (estimate: beam-built graph over 1.56M centers) |

## How to measure success

- NQ on the VM, interleaved A/B against main, 16 clients, recall@10 at least 0.95: **q/s at least 1,200** after
  step 1, and at least 25% more after step 2 at equal recall. Also vCPU-ms per query and the profile breakdown.
- The same on the Apple M4 three-way, with peak RSS beside Qdrant's.
- **Memory needed for throughput:** the smallest block cache that keeps q/s within 5% of its 3 GiB figure. Report
  it beside Qdrant's resident size.
- **Cold, no regression:** 10M and 100M in-region cache-cold and fresh-process p50 / p90 at 48 and 64 probes
  within run-to-run spread of the last published numbers, at equal or better recall. The same 10M check on a
  c7gd.large.
- **Recall-per-row curve:** rows scanned to reach 0.95 at 200, 100, 64, and 32-row leaves, with exhaustive center
  ranking and with the graph.

## Plan (one worker per round)

| Round | Work | Gate |
|---|---|---|
| G1 | Per-query overhead on NQ: take iteration 21's profile, remove the top fixed costs (allocation, task setup, cache lookups and checksums on hot blocks, JSON in and out). | 1,000 q/s or more; else report the floor and why |
| G2 | Offline study (a Rust example over NQ, no format change): leaf sizes 200, 100, 64, and 32 with cross-cell assignment; exhaustive ranking against a beam search over a center graph of degree 16; edges lost at page boundaries. | Go only if rows scanned at 0.95 fall at least 30% and graph ranking costs under 10% of the scan |
| G3 | Build: the center graph in recluster, paged directory version 2 at every size, 64-row leaves. Delete the flat slab directory writer. The automatic upgrade rebuilds older slab sets. | Old reclustered namespaces upgrade unattended; tests for the pages format |
| G4 | Query: beam search over the graph for warm and cold queries; delete exhaustive entry ranking. NQ VM A/B, M4 three-way, 10M cold no-regression in-region. | Targets above |
| G5 (only if the profile asks) | Re-measure the scan and rerank share. If over half of CPU, write the row-graph study as its own design doc. | |
