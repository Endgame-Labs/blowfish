# Changelog

Notable changes per release, newest first. Dates are Pacific time (PT); details and benchmark methodology are in [docs/benchmarks.md](docs/benchmarks.md).

## [Unreleased]

## [0.1.59] - 2026-11-26

### Fixed

- **The warm graph now builds and serves on an 8 GiB host:** Admission overstated a build's memory and refused it. The reservation charged both build-time adjacency forms for the whole build and then added a quarter. The memory gate counted reclaimable page cache as used, and it also demanded a fifth of the limit free on top of the reservation. The reservation is now the measured construction peak plus a stated sixteenth, the gate ignores reclaimable page cache, and the resident row-to-ID table is charged to the graph. On an Apple M4 with BEIR NQ (2.68M x 1024-d) in an 8 GiB cgroup, the graph built in 399 s under live load, peaking at 4.21 GiB of anonymous memory, with no OOM or swap. 16 clients then served 7,899 q/s at recall@10 0.961, against 2,702 q/s for IVF before, and against 6,023 q/s for Qdrant 1.20 in the same setup in Round MEM2. A 6 GiB cgroup still refuses the build. See Round GM1 in `docs/benchmarks.md`.

## [0.1.58] - 2026-11-26

### Changed

- **Selective filtered vector queries are 3.7 to 5.9 times faster at 16 clients, with the same recall:** When a warm graph is ready, a filtered query whose filter leaves at most 1 row in 64 of the tier (1.56 percent) no longer runs IVF. It scores only the allowed rows' resident SQ8 codes, keeps the best `max(3k, 30)`, and reranks them exactly, with the same shadowed-row and deletion handling as the graph path. On an on-demand Graviton3 host with full BEIR NQ (2.68M x 1024-d), 16 clients, hash-distributed filters: 502 to 2,954 q/s at 0.5 percent, 388 to 1,807 at 1 percent, and 304 to 1,121 at 2 percent (p99 66.5 to 19.6 ms), recall@10 1.000 in both. Above 1.56 percent IVF answers: the scan reads every allowed row's 1 KiB code, so its cost grows with the filter while IVF's stays flat, and the two tie near 8 percent. A single client sees little change below 1 percent. See Round G14 in `docs/benchmarks.md`.

## [0.1.57] - 2026-11-26

### Changed

- **Cold filtered vector queries are 1.4 to 15 times faster:** Filters on `id` seek the sorted per-segment IDs instead of decoding every segment's full ID dictionary, filtered queries use the parallel, coalesced, nearest-first slab scan that unfiltered queries already used, a filter that matches nothing reads no slab row maps, and lineage and filter translation touch only the rows that match. On 10M x 1024-d vectors in pure S3, cold, single client, with identical ordered IDs and distances, p50 went from 6,177 to 397 ms for `id < 1000`, 1,669 to 843 ms for a broad numeric filter (`text_length >= 300`), 1,867 to 1,369 ms for a 0.4% equality filter, and 803 to 202 ms for a filter matching nothing. Throughput with 16 clients and unfiltered latency are unchanged within measurement noise. See Rounds C26, C27, and C28 in `docs/benchmarks.md`.

## [0.1.56] - 2026-11-26

### Changed

- **`top_k` 100 on the warm graph is 1.7 times faster, and `top_k` 10 15 percent faster, with identical results:** An ID-only query answered by the warm graph used to hydrate each result row's ID from the document files, 32 segments at a time; the first such query now builds a resident row-to-ID table in the background (8 bytes per row, unsigned IDs only), and later queries read the final rows' IDs from it. Graph queries up to 100 rows also stay on the HTTP reactor now (the gate was 10). On an Apple M4 with BEIR NQ (2.68M x 1024-d), 16 clients, IDs only, one promoted server: `top_k` 100 went from 2,733 to 4,627 q/s with p99 11.6 to 8.3 ms, and `top_k` 10 from 8,512 to 9,796 q/s with p99 4.3 to 3.5 ms, at the same recall to seven digits. Against Qdrant 1.20 at matched recall, blowfish now serves 29 percent more q/s at `top_k` 100 (p99 8.3 against 17.4 ms) and 14 percent more at `top_k` 10. See Round G12 in `docs/benchmarks.md`.

### Fixed

- **`include_attributes: false` no longer hydrates every attribute:** Asking for no attributes read all scalar columns of the result rows and then dropped them, while leaving the field out read none. Both now read none.

### Benchmarks

- **Against Qdrant on an Apple M4, now at both `top_k` 10 and 100:** On BEIR NQ (2.68M x 1024-d), 16 clients, IDs only, at matched recall, blowfish served `top_k` 100 at 4,511 q/s against about 3,491 for Qdrant 1.20 (p99 8.3 against 17.4 ms), and `top_k` 10 at 9,776 against about 8,566 (p99 3.8 against 5.8 ms). See Round G12 in `docs/benchmarks.md`.

## [0.1.55] - 2026-11-26

### Changed

- **The warm graph builds 3.6 times faster:** Graph construction steers its beam search with one-bit codes and shares one beam core with serving, and its build pool is a third of the cores (two to six). On an Apple M4 with BEIR NQ (2.68M x 1024-d), the build took 4.8 minutes instead of 17.2, used 2.8 times less CPU, and the graph began serving after 25 minutes instead of 46.5. Serving speed and recall are unchanged or slightly better (`top_k` 10: 8,439 q/s against 8,104; `top_k` 100: 2,765 against 2,685; recall within 0.003). See Round GB1 and Round G12 in `docs/benchmarks.md`.

### Fixed

- **Overwrites and deletes no longer read old vectors just to count them:** Since v0.1.45, removing rows from a segment whose vectors live in a reclustered tier projected each removed row's vector out of the slabs to compute its size for namespace statistics. The size is now computed from the slab row lists that are already resident, with no vector reads, and the statistics are unchanged.
- **Quiet namespaces publish missing slab row maps:** A test now proves that a writable namespace with no new writes still publishes row maps that a bucket written before v0.1.45 lacks, so cold filtered queries stop rebuilding them. Read-only servers never repair; open the namespace once with a writer. On a repaired 10M layout, cold filtered vector queries took about 1.8 to 2.0 s at p50 instead of 3.4 to 3.6 s (Rounds C26 and C27 in `docs/benchmarks.md`).

## [0.1.54] - 2026-11-26

### Fixed

- **The warm graph could stop serving when the request mix changed:** The promotion test compared the graph's CPU, measured on whatever `top_k` recent queries used, with an IVF baseline measured on other requests. After a run of `top_k` 10 traffic, a later run of `top_k` 100 made the graph look more expensive than IVF, and it stopped serving after a few minutes. The saving is now a ratio of graph to IVF cost measured on the same calibration blocks. The status field `graph_cpu_seconds` is replaced by `graph_cost_ratio`.

### Benchmarks

- **Against Qdrant on an Apple M4:** On BEIR NQ (2.68M x 1024-d), 16 clients, IDs only, with the graph promoted the ordinary way, blowfish served `top_k` 10 at 8,417 q/s (recall 0.958, p99 4.7 ms) against about 7,740 q/s for Qdrant 1.20 at the same recall (p99 6.6 ms). At `top_k` 100 Qdrant is ahead (about 3,625 against 2,200-2,750 q/s at recall 0.953). See Round G11 in `docs/benchmarks.md`.

## [0.1.53] - 2026-11-26

### Changed

- **Cold S3 queries fetch far fewer bytes:** The range cache filled misses in 1 MiB blocks, so a cold 10M-row vector query fetched about 205 MB to use 42 MB. The default block is now 256 KiB, and up to 32 blocks (still 8 MiB) coalesce into one GET, so dense reads stay large while scattered reads stop dragging in whole megabytes. A block that is the whole response, or that the cache does not keep, now shares the response buffer instead of being copied. On 10M x 1024-d pure-S3 cold queries on same-region S3 at 16 concurrent clients, throughput went from 17.3 to 25.6 q/s (1.48x), server CPU per query from 335 to 224 ms, and p50 / p90 from 887 / 1,267 to 592 / 823 ms. A single client is 1.21x faster at p50. Warm repeat queries still hit the cache (about 3 percent more CPU). The defaults `storage.cache.block_size` and `storage.cache.max_coalesce_blocks` change accordingly; disk-cache entries from older releases are keyed by block size, so they are not reused and age out under the disk budget.

## [0.1.52] - 2026-11-26

### Changed

- **Busy namespaces reach the warm graph in minutes, not days:** Every recall sample used to compare the graph and IVF answers against an exhaustive scan, paced at 50 times its own duration, so on BEIR NQ (2.68M x 1024-d) a sample came about every 10 minutes and calibration would have needed about 107 hours. A sample now compares both answers against the exact-distance top-k of the union of the graph's answer, IVF's answer, and IVF at twice the probes (all reranked exactly), which costs milliseconds. One sample in 16 is still checked against an exhaustive scan. Offline over all NQ queries, the union-driven controller settles at least as close to exhaustive calibration as exhaustive calibration does against itself on a different query stream, and on the high side (+0.0012 recall on average). On an Apple M4 under ordinary 16-client load, the graph was serving 32 minutes after load (calibration 4.5 minutes), at 8,106 q/s for `top_k` 10 and 2,799 q/s for `top_k` 100.

## [0.1.51] - 2026-11-26

### Changed

- **Cold S3 queries hedge their slowest reads:** A cold query's parallel read wave ends when its slowest GET does. Hedging used to fire only after a per-store p90 learned from at least 24 samples, and never for reads over 1 MiB, so it rarely helped cold scans (which coalesce into reads of up to 8 MiB). A read is now duplicated once 70 percent of the reads that started within 50 ms of it have finished and it has outlasted their latencies, at any size; the p90 timer remains for reads with too few siblings. The bookkeeping is a lock-free ring of 10 ms slots, so it stays cheap under concurrency. On 10M x 1024-d pure-S3 cold queries on same-region S3, p90 improved 1.32x for IDs-only and 1.34x with attributes (single client, 8 fresh process pairs), with 16-client throughput unchanged. Server CPU per query rises about 6 percent from the extra GETs. The default `storage.cache.hedge_budget` rises from 0.10 to 0.15 extra requests per request.

## [0.1.50] - 2026-11-26

### Changed

- **The warm graph serves `top_k` up to 100:** Requests above 30 rows used the IVF tier. The graph now serves `top_k + offset` up to 100, widening its beam and exact-rerank shortlist with the request (beam about 2.56x `k`, never below the calibrated beam; shortlist 1.5x `k`). Requests of 30 rows or fewer are unchanged. On BEIR NQ (2.68M x 1024-d) on an Apple M4 at 16 clients, `top_k` 100 went from 883 to 2,806 q/s (3.2x) with recall@100 0.9523 to 0.9530.

### Fixed

- **Busy namespaces could never promote the warm graph:** The promotion cost test timed the IVF path, kept warm by ordinary traffic, against the graph's first, cold touch after a truth scan, so the busier the namespace, the more it favored IVF (measured on NQ: graph 3.3 ms against IVF 1.7 ms at 520 q/s, while at steady state the graph is faster). Both paths now run their trial block once untimed and are costed at steady state.

## [0.1.49] - 2026-11-26

### Changed

- **Cold queries read a long WAL tail's payloads sooner:** When a cold reader finds a long tail, it now also lists the payload prefix and starts reading payload bodies while the commit slots are still being confirmed. A body is used only when its confirmed commit record names exactly that payload (sequence and write ID) and, when recorded, the same size; anything else is dropped. Every body, speculative or not, counts against the in-flight byte budget until the replay has consumed it, so memory stays bounded. Up to 16 payload reads run at once (was 8), within the same byte budget. On same-region S3, a cold read of 32 pending 150 KB payloads is 1.28x faster than v0.1.47; empty and short tails are unchanged.

## [0.1.48] - 2026-11-26

### Changed

- **Cold staged vector queries that return attributes are about 1.4x faster:** When exact vectors are staged on local disk, a cold query returning attributes used to read each 2-200 KB column block as a whole 1 MiB cache block, fetch the `id` column even though the staged row-to-ID map already had it, and start speculative document reads for about three times `top_k` unreranked candidates before the roughly 2 ms local rerank named the winners. It now reads column blocks at their exact size, takes IDs from the verified staged map (falling back to the stored column if the map is retired), takes one exact top-k prefix early in the scan instead of guessing, and hydrates the winning segments concurrently. On a 10M x 1024-d cold staged setup on same-region S3, attribute queries went from 92.7 / 128.3 to 66.5 / 93.9 ms p50 / p90. IDs-only and unstaged cold queries are unchanged (a 16-process paired control: 0.99x, CI includes 1.0). Every measured reply matched exactly.

## [0.1.47] - 2026-11-26

### Changed

- **Cold queries find a long WAL tail faster:** A cold reader found committed but unindexed writes by probing slots in growing waves (2, 4, 8, 16, then 32), one round trip each. When the first two waves are full, it now also lists the WAL prefix and reads the whole listed run in one wave. Conditional slot reads still decide where the tail ends, so a stale listing only falls back to probing. On same-region S3, a cold strong query with 32 pending external payloads went from 325 to 255 ms (1.27x), and with 32 inline entries 1.5x faster. Empty and short tails are unchanged.

## [0.1.46] - 2026-11-26

### Changed

- **Faster warm BM25:** The scoring loop no longer allocates a candidate list per window, orders windows only as far as the score threshold needs, and folds row bounds over touched rows only. Results are identical (ordered IDs and score bits). On 1M MS MARCO on a 16-vCPU Graviton host, warm throughput went from 2,654 to 3,264 q/s at 16 clients (1.23x) and from 918 to 1,012 q/s at one client (1.10x). Cold queries are unchanged.

## [0.1.45] - 2026-11-26

**Bucket format change: generation 6 (manifest format 5).** The upgrade is automatic and one way: the first publication by v0.1.45 upgrades the bucket, after which older releases refuse to open it. Stop every older server against a bucket before you start v0.1.45 on it.

### Changed

- **Every vector is stored once:** After a recluster, each covered row's vector used to live both in its segment's ANN object and in the namespace-wide tier. Covered segments' ANN objects are now retired in a fenced publication, and the tier becomes the only copy. Projection, exact search, filtered search, writes, recluster, compaction, branches, and copies all read covered vectors through the tier. On 2.68M BEIR NQ, the settled store went from 16.9 GB to 8.3 GB, with identical results on all 3,452 test queries, unchanged recall, and unchanged warm and cold latency.
- **Filtered vector search runs on the tier:** Each slab gets a small resident row map (about 6 bytes a row), so a filtered query probes the tier instead of falling back to the segments' own indexes. On a 10M pure-S3 cold run, selective filters went from 2.94 s to 2.32 s p50 (recall 1.000), and broad filters from 2.63 s to 1.83 s, with recall up from 0.950 to 0.992.
- **Branches and copies pin the tiers they depend on,** and a tier is never dropped before the segments it covers get their own vector objects back.

## [0.1.44] - 2026-11-26

### Changed

- **Cold queries read pending writes in parallel:** A strong-consistency query on a cold node fetched each committed but not yet indexed write payload one at a time. With 32 such payloads in S3, cold p50 was about 1,013 ms, about 800 ms of it in serial fetches. Payloads are now fetched concurrently and applied strictly in WAL order: the same case takes about 300 ms (3.2x faster), with identical results, and empty or small WAL tails are unchanged.
- **The concurrency is bounded by bytes:** At most 8 payloads and 64 MiB of stored payload bytes are in flight. A larger payload still proceeds, alone. New commit records carry their payload's stored size. Records from older releases have no size and are fetched one at a time. With 8 payloads of 64 MiB each, peak memory is 397 MiB, down from 720 MiB with a count-only bound. Older binaries ignore the new field.

## [0.1.43] - 2026-11-26

### Fixed

- **A pass of large `top_k` queries no longer halves warm vector speed:** On a tier whose exact vectors do not all fit in memory, a run of top_k 100 queries filled the exact-vector cache and evicted the rows ordinary queries need, and the cache kept the large-query rows afterwards. On 2.68M BEIR NQ, warm top_k 10 fell from about 1,400 to about 600 q/s at 16 clients and did not recover. Large queries are now recognized relative to the tier's recent workload and fill only free space in the same memory budget, so ordinary queries keep their rows. After a top_k 100 pass, warm top_k 10 stays within 4% of a fresh server, a workload of only top_k 100 queries is about 1.3x faster, and memory use is unchanged. New metrics count exact-row cache lookups, inserts, evictions, and the reasons a query leaves the compiled warm path.

## [0.1.42] - 2026-11-25

### Fixed

- **Leftover recluster spill files are cleaned up:** Releases before the locked spill directories wrote loose `spill-<uuid>.bin` files into the system temporary directory, and an interrupted build left them behind (one test machine had 25 GB of them). Startup now deletes loose spill files that have been untouched for an hour. `docs/operations.md` describes where spills live and how they are recovered.

## [0.1.41] - 2026-11-25

### Fixed

- **Recluster on Apple M-series (SME) no longer corrupts assignments:** The BF16 matrix kernel that NumKong 7.8.7 selects on CPUs with SME (Apple M4) writes whole output tiles even when the last tile is partial. It overwrote the next row's first distances and wrote past the end of the output buffer, so recluster could assign rows to the wrong center. blowfish now selects reviewed BF16 kernels itself (x86 Haswell, Skylake, and Genoa, Arm NEON BFDOT, and a portable fallback) and never uses SME or AMX, without changing NumKong's global dispatch. Graviton and x86 were not affected by the overwrite.
- **BF16 subnormals are flushed explicitly in the screening bound:** A tiny BF16 input multiplied by a large one could exceed the certified error bound on x86. Conversion now flushes subnormals to zero and counts the loss in the bound, so screening stays exact.

## [0.1.40] - 2026-11-25

### Fixed

- **Dropped queries release their workers:** When a query's caller went away (a client disconnect, a timeout, or shutdown) while its plan waited on I/O, the plan kept running on the blocking pool and held its worker permits until it finished. Every query plan is now aborted at its next await point when its caller is dropped, and releases its permits promptly. Results of queries that complete are unchanged, and warm throughput is unchanged within noise.

## [0.1.39] - 2026-11-25

### Changed

- **Cold queries on a staged node search while the manifest loads:** A cold vector query on a node with a ready local stage now runs routing, the local scan, and the rerank against the stage's last confirmed checkpoint while the manifest read and WAL tail checks are still in flight. The result is returned only if the manifest confirms the same generation and indexed-through sequence and the WAL checks find no newer writes; otherwise blowfish discards it and runs the normal path. On a fully staged 10M-vector namespace, cold IDs-only p50 / p90 went from 52.7 / 61.8 ms to 32.1 / 35.9 ms (200 cold query groups), with identical results. Pure object-storage latency is unchanged within run-to-run variance.

## [0.1.38] - 2026-11-25

### Changed

- **Faster cold queries on a staged node:** A cold query on a node with a ready local stage now starts its WAL tail checks at the same time as the manifest read, using the indexed-through sequence saved with the stage, and keeps their results only if the manifest confirms that sequence and generation. Without a ready stage, nothing changes. The final rerank's local reads now run on the bounded worker pool; before, they ran one after another even though they looked concurrent. On a fully staged 10M-vector namespace, cold IDs-only p50 / p90 went from 54.8 / 73.0 ms to 49.7 / 67.3 ms (200 cold query groups), and final-rerank p90 from 9.6 to 2.6 ms. Results are identical, and pure object-storage latency is unchanged within run-to-run variance.

## [0.1.37] - 2026-11-25

### Changed

- **Recluster spill skips deleted rows:** Recluster copied every row's vector into its local spill file, including rows already deleted, and filtered them out later. It now writes only live rows, keeping their global row IDs, so the output is byte-identical. With 30% of rows deleted, the spill takes 70% of the bytes it did.

## [0.1.36] - 2026-11-25

### Changed

- **All 10M exact vectors fit in the local stage:** The local ANN stage now stores exact vectors with a lossless 15-bit packing: when every f16 value in a 16 KiB block has a clear high exponent bit, as normalized embeddings do, that bit is dropped; any other block is stored as-is. Reads reconstruct the exact bits. On a 10M-vector namespace the whole stage now takes 19.50 GiB, under the unchanged 20 GiB cap, where before 9.50M of the 10M vectors fit. Cold IDs-only queries went from 57.8 / 103.4 ms to 57.5 / 88.5 ms p50 / p90, with 3 object reads per query instead of 4.9 and identical results. Pure object-storage latency is unchanged. Object storage bytes are unchanged; older local stages are rebuilt automatically. Recluster's writer-side staging uses the same packing.
- **Named ID projection uses staged row IDs:** A query with `include_attributes: ["id"]` now takes the same staged row-ID path as a default IDs-only query, instead of reading documents from object storage.

### Fixed

- **Local stage disk accounting on XFS:** Streamed stage files held open kept the filesystem's speculative preallocation beyond their end (a 65 MiB file occupied about 128 MiB). Each stage file is now truncated to its stored length before the final sync.

## [0.1.35] - 2026-11-25

### Changed

- **New recluster tiers are query-ready in seconds:** When the node that runs recluster also serves queries, it now writes each slab's routing directory, codes, and exact vectors to its local stage while uploading them, and activates them when the manifest commit publishes the tier. Before, the first query fetched them back from object storage. On a 10M-vector namespace, the new tier's codes became usable 2.3 s after the commit instead of 87.4 s, and it was fully staged in 3.0 s instead of 88.1 s. Recluster time and query results are unchanged. A failed commit, a cancelled build, or a crash discards the local files. Nodes that only serve queries are unchanged, and there is no bucket format change.

## [0.1.34] - 2026-11-25

### Changed

- **Much faster filtered BM25:** When a filter admits few, scattered rows of a text segment (at most a quarter of its rows, and fewer than 24 per touched window), blowfish scores those rows directly instead of walking windows. On a 1M-document MS MARCO namespace at 16 clients, a random 0.5% filter rose from 823 to 4,153 queries/s and a random 10% filter from 1,169 to 1,660. Unfiltered queries are unchanged. Scores and order are bit-identical. No format change and no setting.

### Fixed

- **Filter cache key cost:** The cache key for a segment's filter result was built by serializing the whole segment description, including its dictionary chunk indexes, on every filtered query: about a fifth of the CPU of a selective filter. The key now uses the segment's immutable object keys, sizes, WAL range, and live-row generation. This alone made contiguous 0.5% filters 1.73x faster (4,981 to 8,621 queries/s) and also applies to filtered vector queries.

## [0.1.33] - 2026-11-25

### Changed

- **Connection warmer refresh 45 s to 15 s:** S3 closes idle connections after 20 to 40 seconds, so the warmer now refreshes before that happens. No measurable end-to-end change in-region; it keeps the first cold query after a quiet spell from paying for new connections.

## [0.1.32] - 2026-11-25

### Changed

- **Faster warm BM25 for common terms:** When a term is dense in a text segment, its second use builds a byte-per-row array of term frequencies (with an overflow table for large values), up to 32 terms per segment reader, within the decoded-posting budget and charged to the cache like any other decoded data. Windows score those terms straight from the array instead of decoding postings, and exact per-window (tf, length) frontiers keep the block bounds tight. On a 1M-document MS MARCO namespace, warm full-text throughput at 16 clients rose from 1,947 to 2,465 queries/s and single-client throughput from 776 to 875 queries/s; the median cold first query fell from 231 to 200 ms. Restart time and write latency are unchanged. Scores and order are bit-identical. No format change and no setting.

## [0.1.31] - 2026-11-25

### Fixed

- **Warm BM25 cache accounting (up to 1.37x faster):** Each text segment reader was charged a fixed estimate for its decoded-posting memo, four times the memo's limit, instead of the bytes it actually held. The general cache filled with charges for memory that did not exist and evicted whole readers, so the memo had to stay small. Readers are now charged what their memos hold as they grow and shrink, and memo pools are sized from the cache budget (1/32 of it, 8 to 64 MiB). On a 1M-document MS MARCO namespace, warm full-text throughput at 16 clients rose from 1,433 to 1,966 queries/s and single-client throughput from 645 to 797 queries/s, with p90 down 27 and 37 percent. Scores and order are bit-identical, and writes are not slower. No format change and no setting.

## [0.1.30] - 2026-11-25

### Changed

- **Faster bulk ingest (about 1.4x):** Three CPU costs on the write path are gone. Index builds find each row's nearest center with a certified BF16 matrix screen and recompute only the surviving centers with the original kernel, so every assignment and every index byte is unchanged (segment-build time per row about halves on Graviton). Large WAL payloads store the vectors of f16 attributes as f16 base64 (exactly the same values in half the bytes), which halves the JSON, leaves little for zstd to do, and cuts uploaded WAL bytes by 29%. Vector values are normalized straight to base64 instead of through a JSON number per element. On a full Natural Questions load (2.68M rows of 1,024-d f16 vectors, four clients of 5,000-row writes, one c7gd.2xlarge), paired against v0.1.29: 17.4K to 24.5K rows/s, write p90 about 1,150 to 890 ms, with no backpressure retries, and 53% less server CPU. Query results are unchanged. No setting.
- **Bucket format 5 (WAL encoding 2):** Large WAL payloads can now be written in encoding 2 (see above), so the bucket format moves to 5: writable startup writes `_system/format/5.json`. v0.1.29 and older refuse the bucket with `BucketFormat { required: 5, supported: 4 }`, and there is no in-place downgrade. Stop older writers and back up the prefix before upgrading, as for format 4. New binaries read every existing WAL payload.

## [0.1.29] - 2026-11-25

### Changed

- **Faster warm graph search:** The resident graph index caches vector norms, keeps its beam in a contiguous buffer with a bitset of visited nodes, prefetches neighbor codes, stores locations densely, and uses a four-accumulator int8 dot product on ARM. On an Apple M4 with the Natural Questions benchmark and the production server, 16-client throughput at recall 0.944 is 8,720 queries/s versus Qdrant's 8,407 on the same machine, 9 to 11 percent above v0.1.28, at 2.65M instructions per query (was 3.57M). Memory is unchanged (about 4.4 GB versus Qdrant's 5.6 GB). `/status` now reports each cached namespace's resident graph state.
- **Faster, lighter local ANN staging:** A tier's staged companions (document directories and row-to-ID maps) now live in one local pack file with an offset table, fetched with whole-object GETs whose identity is checked against the recorded segment. The stage publishes in two phases, so scan codes are usable before companions finish. On a cold 100M-vector S3 namespace, scan codes are usable after 79 s instead of 155 s and the stage is complete after about 89 s, with 325 open staging files instead of about 3,900. Live queries during staging and results are unchanged. Stages written by v0.1.27 and v0.1.28 are adopted as they are.

## [0.1.28] - 2026-11-25

### Changed

- **Faster warm BM25 scoring:** Each decoded posting block now carries a small table of where every 128-row scoring window starts, so scoring a window no longer binary-searches each block. Offset-keyed caches use a cheaper hasher, and the span cache is more compact. On a 1M-document MS MARCO namespace, warm full-text throughput at 16 clients rose from 1,100 to 1,258 queries/s (p50 12.2 to 10.9 ms), and single-client throughput from 531 to 582 queries/s. Scores and order are bit-identical. No format change and no setting.

## [0.1.27] - 2026-11-25

### Changed

- **Faster cold result hydration:** Local ANN stages now also hold each covered segment's document column directory and, for numeric IDs, an 8-byte-per-row row-to-ID map, admitted before exact vectors under the same staging limit, CRC checks, and adoption records. IDs-only queries finish without any storage request after the scan, and attribute queries read the directory locally and fetch only data blocks. On a cold, staged 10M-vector S3 namespace, IDs-only p50 fell from 127 to 58 ms (2.2x) with GETs per query down from 38 to 4.5, and queries returning title and text fell from 140 to 102 ms; at 100M, IDs-only p50 fell from 396 to 309 ms. Results are identical. The extra files make first staging of the 100M namespace take about 172 s instead of 92 s; scan codes are still staged first. String-ID segments keep the previous path. There is no setting.

## [0.1.26] - 2026-11-25

### Changed

- **Faster cold queries with staged exact vectors:** Local ANN staging now also stages whole exact-vector objects when they fit in the existing staging limit, after the scan codes, so the exact rerank reads them from local disk instead of S3. Staging downloads now share one bounded pool across slabs. On a cold, staged 10M-vector S3 namespace (95% of its exact vectors fit), p50 fell from 163 to 125 ms and 151 to 122 ms in two 200-query runs (about 1.27x), and p90 fell about 30%, with identical results and 60% fewer GET requests. Staging an empty disk took 68 instead of 131 s at 10M and 73 instead of 128 s at 100M, without slowing live queries. At 100M only about 1% of the exact vectors fit, so most still come from S3. Stages from v0.1.24 are adopted unchanged; exact vectors are added when a namespace is staged again. There is no setting.

## [0.1.25] - 2026-11-25

### Changed

- **Faster BM25 under writes:** Full-text queries now gather term statistics from unindexed WAL entries once per query, in parallel on the existing query workers, and reuse them for ranking. Under 1,000 text upserts per second on 1M MS MARCO passages, BM25 p50 fell from 31.4 to 22.0 ms and p90 from 44.0 to 34.0 ms, with bit-identical scores and order. Write latency changed by about 1%, within run-to-run variation.
- **Profile-guided builds by default:** The Dockerfile and `scripts/pgo-build.sh` now build blowfish with profile-guided optimization, trained in about one to two minutes on a small built-in workload (16K synthetic vectors, BM25, filters, and writes over loopback HTTP). Against the plain release build on 8 cores: filtered vector queries +98.5%, BM25 +16.0%, warm vector queries +4.7%, and durable writes +3.5%, with identical results. The small trainer keeps 85 to 101% of the gain of a profile trained on full BEIR NQ. Plain `cargo build --release` still works and is not profile-guided; see [docs/pgo.md](docs/pgo.md).

### Fixed

- **Local model build links:** Profile-instrumented `local-models` builds now link the HTTP archive omitted by llama.cpp's install step and forward its enabled TLS dependencies. The affected bindings have been used since v0.1.12; ordinary uninstrumented release builds discard the unused HTTP paths and are unaffected by this link failure.

## [0.1.24] - 2026-11-25

### Changed

- **Staged ANN files survive restarts:** Locally staged cold ANN tiers are now kept across restarts, deploys, and crashes. On startup, blowfish removes partial stages, checks each completed stage against the current manifest, the WAL, and object ETags or versions with HEAD requests, and adopts it without downloading the tier again; every 16 KiB block is still CRC-checked on read. On cold S3 namespaces after a restart, 100M-vector p50 fell from 789 to 412 ms, and the time until queries were fast again fell from 118 s to under 1 s (10M: 9.2 s to 0.6 s). Disk use stays within the existing staging limit. There is no setting.

## [0.1.23] - 2026-11-25

### Changed

- **Lower bulk write latency:** WAL group commit now also bounds each group by about 32 MiB of estimated document bytes, so large bulk upserts stop queueing behind one oversized group. Loading full BEIR NQ (2.68M 1,024-dimension vectors, four clients), write p90 fell from 2,067–2,125 ms to 1,079–1,125 ms (about 46%) with throughput unchanged within run-to-run variation. Every acknowledged row survives SIGKILL, and loaded data matches the source exactly. There is no setting.

### Fixed

- **Docker image build:** `Cargo.lock` paired bindgen's syn 3 with prettyplease 0.2 (built on syn 2), so builds with the `local-models` feature, including the Docker image, failed to compile in v0.1.16 through v0.1.22. prettyplease is now 0.3.

## [0.1.22] - 2026-11-24

### Changed

- **Faster cold vector queries:** Staged ANN files keep their handles open and are read with positional reads that merge overlapping extents while still checking every 16 KiB block. Local read-ahead starts during the snapshot's WAL check, and fully staged scans speculate on at most `min(4k, 40)` vectors and no early documents. On cold, staged S3 namespaces, p50 fell from 443 to 365 ms (-17.7%) at 100M vectors and from 200 to 183 ms (-8.8%) at 10M, with 14 to 17% fewer GET requests; results are identical.

### Fixed

- **macOS build:** Local ANN staging failed to compile on macOS since v0.1.16 because `statvfs` field widths differ there; both fields are now widened before multiplying.
- **Durable local writes:** `file://` stores now always fsync file data and publication directories before acknowledging WAL, manifest, and segment writes. Newly created directory chains and identical-write recovery also require successful syncs; a visible object can no longer hide an fsync failure. There is no durability toggle. Two dedicated NVMe full-NQ load pairs measured 0.8–1.6% less throughput and 1.8–3.3% higher write p90; sequential one-row writes added 0.14–0.17 ms at p50/p90. S3-compatible stores are unchanged. See [local filesystem durability](docs/operations.md#local-filesystem-durability) for ordering, tests, and measured cost.

## [0.1.21] - 2026-11-24

### Added

- **Automatic warm graphs:** When a cosine-f16 tier stays hot, blowfish builds an in-memory SQ8 Vamana graph for it in the background at idle priority, checks its recall against IVF on live traffic, and serves unfiltered vector queries from it once a serving effort passes. Nothing is written to the bucket; restarts, eviction, and refused builds fall back to IVF, and filtered, exact, and other-metric queries stay on IVF. On full BEIR NQ on an Apple M4, warm HTTP throughput rose from about 2,940 to 8,133 q/s at recall 0.9575 and from 3,502 to 8,558 q/s at 0.9516, within 0.6% and 3.9% of Qdrant at matched recall while using 22% less memory. Building the NQ graph reserves about 5.0 GB of RAM and 5.5 GB of temporary disk and slowed live queries by 1.6% on 8 Linux cores. There is no setting; see [docs/operations.md](docs/operations.md) and [docs/design/hot-tier-graph.md](docs/design/hot-tier-graph.md).

## [0.1.20] - 2026-11-24

### Added

- **Profile-guided build recipe:** `scripts/pgo-build.sh` builds a profile-guided blowfish binary trained on a mixed workload (warm vector, filtered, BM25, writes, and cold queries) from public NQ data. On full NQ at 8 cores, warm HTTP vector search rises 5.4% (2,339 to 2,464 q/s), filtered search 105%, BM25 16%, and writes 4%, with identical results; instruction-cache refills drop 30%. Official release builds do not use it yet. See [docs/pgo.md](docs/pgo.md).

## [0.1.19] - 2026-11-24

### Performance

- **Cheaper filtered vector search:** Filtered ANN scans now use fixed-width table chunks and a bounded per-scan membership bitset (at most 8 bytes per scanned row; widely spaced IDs fall back to bitmap merging). On full NQ, CPU per query drops 20 to 40% across ID and hash filters at 1%, 10%, and 50% selectivity. The hardest case, a 1% hash filter, goes from 67 to 40 ms and uses 61% fewer instructions, and its 16-client throughput rises from 99 to 168 q/s. All 768 per-query comparisons return identical rows and distances. Unfiltered queries cost about 2% more CPU. See [docs/benchmarks/filtered-ann.md](docs/benchmarks/filtered-ann.md).

## [0.1.18] - 2026-11-24

### Performance

- **Faster recluster, exactly:** Leaf assignment screens candidates with full-dimensional BF16 matrix scores and certified error bounds, then recomputes every possible winner with the original FP32 kernel, so assignments stay byte-identical. On ARM CPUs with native BF16 (selected automatically; other CPUs keep the original kernel), full NQ assignment drops from 176 s to 97 s and the time from the last write's acknowledgment to the full index drops from 255 s to 177 s (1.44x), at unchanged recall. A 10M assignment check went from 570 s to 349 s.

## [0.1.17] - 2026-11-24

### Changed

- **New home and license holder:** The repository moved from `dorkitude/blowfish` to [`endgame-labs/blowfish`](https://github.com/endgame-labs/blowfish); old URLs, clones, and remotes redirect. The MIT license's copyright holder is now Endgame Labs, Inc.

### Performance

- **Per-core query serving:** Warm queries on compiled tiers now run on per-core reactors instead of a shared pool, while cold, nonresident, and background work stays on shared I/O. On full NQ at 16 clients, throughput rises 2% on 4 cores, 5 to 6% on 8 cores (p90 8.0 to 6.9 ms), and 21% on 16 cores (3,709 to 4,486 q/s), with identical ordered results. Cold S3 latency is unchanged within noise. See [docs/benchmarks.md](docs/benchmarks.md).

## [0.1.16] - 2026-11-24

### Upgrade notes

- **Bucket and manifest format 4:** Stop every older writer and back up the entire prefix before upgrading. Compaction through recluster tiers adds `lineage` to the manifest, moving the manifest and bucket formats to 4. Writable startup writes `_system/format/4.json`, and v0.1.15 refuses the upgraded bucket with `BucketFormat { required: 4, supported: 3 }`. There is no in-place downgrade.
- **Background text index upgrades (formats 6 and 7):** Existing format-4, format-5, and format-6 text indexes are repacked directly into format 7 in the background, combining lossless phrase-position compression with compact short posting lists (the format-6-to-7 test took about 4 s for 1M rows).
- **Measured v0.1.15 upgrade:** Separate local-disk and S3 stores with 200K NQ rows, 1,024-dimensional vectors, text, six segments, and 32 slabs reached text format 7 in 30.24 and 27.11 s under continuous query traffic. Recall@10 stayed 0.9725 and 0.9775, respectively; every BM25 check matched 40/40, and 42,640 candidate requests had zero errors or exhaustive fallbacks. Both old-binary restarts refused cleanly. Text shrank about 3.50x; old objects remain until GC. See [Upgrading to v0.1.16](docs/operations.md#upgrading-to-v0116) for marker timings, byte totals, and the limits of `reindex --dry-run`.

### Added

- **Local ANN staging:** With an S3-compatible backend and a disk cache directory, blowfish copies each recluster tier's routing directory and vector codes to local disk after the tier is first queried, and serves later cold queries from that copy while full vectors stay in object storage. The budget is the smaller of 20 GiB and a quarter of free disk, in `<disk_dir>/ann-staging-v1/`, with no setting. In-region on S3 with identical results, cold p50/p90 drops from 325/452 to 220/312 ms at 10M vectors (GETs per query 351 to 140, bytes 47.5 to 5.8 MB) and from 715/1,023 to 400/508 ms at 100M (GETs 910 to 157), using 1.6 GB and 16.3 GB of disk.
- **Compaction through recluster tiers:** Compaction now merges segments covered by a recluster tier instead of skipping them, recording surviving row lineage so tiers keep answering without a rebuild, and delta tiers can now build beside a base rebuild and survive its install. On BEIR NQ (2.68M rows), compaction reduced 51 segments to 33 with both tier keys unchanged, recall@10 unchanged at 0.9521, and instructions per query flat (23.17M to 23.09M).
- **Truthful index status in namespace metadata:** Namespace metadata now reports `{"status": "updating", "unindexed_bytes": ..., "unindexed_rows": ...}` while unindexed rows remain in the WAL tail and switches to `up-to-date` once published, instead of always reporting `up-to-date`. Polling metadata also refreshes the checkpoint while data is pending so a reader sees index publications from another node without a new write.

### Changed

- **Text index format 6 (compressed phrase positions):** Drops the element index for scalar string fields and delta-codes phrase positions losslessly. On 1M MS MARCO passages, text indexes shrink from 743.5 MB to 348.2 MB (2.14x smaller) and whole segments from 932.5 MB to 537.2 MB (1.74x smaller), with identical IDs and score bits across 8,480 HTTP responses and cold phrase queries reading 28.7% fewer objects and 18.4% fewer bytes (p90 560 to 541 ms).
- **Text index format 7 (compact partial blocks and directories):** Partial posting blocks now store only their real rows (LEB128 row deltas and term frequencies) instead of padding to 128, and block directories use variable-width records with offsets derived from frame sizes. On 1M MS MARCO passages, text indexes shrink a further 1.69x from 336.3 MB to 198.8 MB (−40.9%), lossless across every posting and all 56.7M phrase occurrences with identical scores and ordered hits across 1,000 queries.
- **Bulk-load scheduling across retries:** Bulk-load detection and deferred compaction now hold across each `429` response's advertised `Retry-After`, and the quiet period starts when queued writes complete rather than when batches are prepared. On full BEIR NQ with four clients, load time drops from 168 to 150 s and write p90 drops from 1,961 to 808 ms (2.4x lower); with eight clients, the first base covers all 2.68M rows instead of building a premature 730K-row base plus a merge, and the index is ready in 219 s instead of 266 s.
- **Published benchmarks:** Added in-region AWS benchmarks for v0.1.14 (10M and 100M vector search, 1M MS MARCO BM25, and upgrade timings), Apple M4 BEIR NQ comparisons at v0.1.15 against Qdrant 1.20.0 and LanceDB 0.41.0, and matched 2–12 GiB memory-budget comparisons against Qdrant on an Apple M4.

### Performance

- **BM25 under concurrent queries:** BM25 scoring no longer holds the segment's bound-cache lock while it scores, reading a shared snapshot of cached bounds and publishing new ones afterward, and it reuses per-thread scratch buffers instead of allocating about 4 MB per query. On 1M MS MARCO passages (warm, 1,000 queries), 16-client throughput rises from 744 to 2,134 q/s (2.87x), single-client p50 drops from 2.12 to 1.11 ms, and instructions per query drop 27%, with identical ordered IDs and score bits. Cold p50 is within 1% (161 to 163 ms) and cold p90 drops from 224 to 216 ms.
- **Balanced warm slab scans:** A warm vector query's resident slab scans now split into row-balanced CPU batches, sized by the workers and the queries already in flight, instead of one task per slab, so one crowded slab no longer sets the tail. On BEIR NQ (same index, identical ordered results on all 3,452 queries), warm p90 drops 12.8–17.3% at 48 and 64 probes, real-S3 cold p90 drops 4.6–11.8%, and throughput at 4 and 16 clients is unchanged.
- **Warm queries stay on the compiled path after compaction:** A compaction that fully covered a recluster tier's segments made the tier look partially masked, which silently disabled compiled warm queries. On BEIR NQ (8 CPUs, 16 clients) after compaction, throughput rises 22% at 48 probes and 19% at 64 with p90 about 23% lower, and the query CPU pool now leaves one core for completions on hosts with 8 or more CPUs (p90 8-11% lower on freshly published tiers), with identical results.
- **Cached immutable predicate results:** Filter results over immutable segments are now cached and shared between snapshot warm-up and query planning in a protected share of the column cache, with WAL shadows applied afterward. On full BEIR NQ with an ID-range filter at normal retention, warm p50 drops from 1,522 to 12 ms and 16-client throughput rises from 5 to 601 q/s with identical results.
- **Memory-mapped local objects and larger decoded cache:** Immutable local objects are now memory-mapped into the OS page cache using sequential read-ahead for reads of 64 KiB or more and random-access advice for smaller sparse reads, and the decoded-cache default rises from `limit/8` to `limit/4` (512 MiB to 4 GiB). On BEIR NQ at recall@10 0.950 on an Apple M4 under hard cgroup memory limits, warm throughput rises from 622 to 2,235 q/s at 2 GiB, 1,987 to 2,305 q/s at 4 GiB, 1,974 to 2,702 q/s at 8 GiB, and 1,959 to 2,592 q/s at 12 GiB (1.2–3.6x), while disk-cold p50 drops from 1,068 to 331 ms (3.2x lower) with identical results.
- **Compiled warm tiers and synchronous warm queries:** Resident recluster tiers now compile into a generation-scoped view with a sparse exact-vector cache and execute warm queries synchronously with reusable per-thread scratch, falling back to the normal planner on any miss. On BEIR NQ (16 clients, `c7g.2xlarge`), warm throughput increases by 31.2% at 48 probes and 27.6% at 64 probes with 10.1% fewer instructions per query, identical ordered top-10 results, and about 0.9 GB higher peak RSS within the cache budget.
- **Dedicated CPU thread pool, core-derived admission, and full small-tier exact caches:** Warm query plans now run on a fixed pool of CPU threads with query admission sized to twice the available cores (floor of 8), and tiers whose exact vectors fit in the decoded-cache budget keep all of them instead of sizing from code bytes. On 8-core BEIR NQ, HTTP warm throughput rises from 1,832 to 2,148 q/s at 48 probes and 1,584 to 1,886 q/s at 64 probes (+17–19%, identical results); a 10K-row namespace rises from 4,067 to 8,163 q/s.
- **Streaming cold slab scans and overlapped rerank:** Cold vector slab searches now reserve reads nearest leaf first, scan leaf batches as bytes arrive, and begin bounded rerank and document reads before the slowest scan read finishes. On in-region `c7gd.2xlarge` (cache-cold, paired), p50/p90 drops from 554/701 to 425/544 ms (1.30x; GET attempts +11.5%) at 10M vectors and from 1,028/1,166 to 715/907 ms (1.44x; GET attempts +1.6%) at 100M vectors, with unchanged recall@10 (0.9615 and 0.9540) and identical IDs and distances.
- **Automatic 640-row leaves for 10M+ vector tiers:** Recluster now uses 640-row leaves at 10M live vectors or above (keeping 200-row leaves below 10M), maps probe counts to the stored layout (64 becomes 70 on 640-row leaves), and skips a redundant 5.35 MB segment-directory hint per cold query on upgraded tiers. At 10M vectors (matched recall@10 0.9690 vs. 0.9700 against a 200-row control), cold p50 drops from 378 to 325 ms (−13.9%), GETs drop from 621 to 355 (1.75x fewer), bytes per query drop from 58.4 to 47.6 MB, and the build is 23.6% faster.
- **Shared partial-index metadata and candidate bounds:** Partial ANN indexes now share immutable cluster ranges, and completed slabs pass witnessed upper bounds to later scans to discard non-competitive candidates before merging. On warm BEIR NQ, this cuts instructions per query by 12.5% at 48 probes and 12.7% at 64 probes with identical top-10 ID lists across all 31,068 tested queries.
- **Fast queries under steady writes:** Added a byte-bounded decoded WAL vector cache, bounded global top-k, and parallel exact scoring over wide tails, while preventing steady writes from triggering bulk-load mode and reusing unchanged full-text statistics. On BEIR NQ at 1,000 upserts/s, ANN p50/p90 drops from 156/451 to 15/28 ms (at 300 upserts/s, 160/280 to 29/41 ms); BM25 under 1,000 writes/s drops from 4.2/9.2 s to 1.7/2.1 s.
- **BM25 under heavy writes:** BM25 queries over an unindexed WAL tail now compute statistics only for query terms, reuse cached per-row term counts from immutable tail objects in a bounded CLOCK pool (one-eighth of the decoded-cache budget), and correct for replaced or deleted indexed rows from the query terms' masked postings and stored lengths. On 1M MS MARCO at 1,000 text-changing upserts/s, p50/p90 drops from 539/721 ms to about 30/41 ms (18x faster p50) with bit-identical results across 4,000 validation queries.
- **Selective BM25 posting-cache probes and reused normalization bounds:** BM25 readers now probe the posting cache selectively and reuse normalization bounds across scoring passes.
- **Faster recluster readiness after bulk loads:** Active bulk loads now defer recluster until writes go quiet, prioritize the first base build ahead of remaining segment compaction, use a four-row SIMD dot-product kernel (NEON, AVX2, and AVX-512) with 128-row training batches, and manage per-build spill files with lock-guarded cleanup and startup sweeping. On BEIR NQ (`c7gd.2xlarge`, in-region S3), time from the last acknowledged write to a ready full tier drops from 756.7 to 257.5 s (2.94x sooner) and full build time drops from 426.4 to 255.0 s (1.67x); added the `blowfish_recluster_stage_seconds{stage}` metric.

### Fixed

- **Recall on selective filtered vector queries:** Filtered ANN queries no longer cap their probe count at three times the unfiltered count. The probe budget now grows with the filter's selectivity (counted from segment membership maps), plus a margin for one more surviving neighbor, and the `BF_FILTER_PROBE_CAP` setting is removed. On BEIR NQ at default probes, recall@10 for a filter matching 1% of rows by hash rises from 0.936 to 1.0, and for 10% from 0.961 to 0.989; 50% filters are unchanged. The extra recall costs search work: at 1% the CPU per query is 38% higher than raising the unfiltered probe count to reach the same recall, and an ID-range filter matching 10% uses 18% more CPU at unchanged recall.
- **Orphaned WAL payloads after failed write groups:** When a write group's slot commit failed, cancelling later uploads and deleting their payloads immediately could let an in-flight `PUT` land after the `DELETE` and leave an orphan object behind. Cleanup now waits for an abandoned upload to finish before deleting its payload, and GC reclaims expired unreferenced payloads after filling empty slot ranges with empty WAL entries so a paused writer loses its conditional commit.

## [0.1.15] - 2026-11-23

### Upgrade notes

- **Bucket and manifest format 3 (no in-place downgrade):** Stop all older writers and back up the full prefix before starting v0.1.15; writable startup writes `_system/format/3.json`, causing v0.1.14 and older binaries to refuse the bucket with `BucketFormat { required: 3, supported: 2 }`. ANN objects, old manual recluster slab sets (adopted as a tier without rebuilding), v2 documents, and older text indexes upgrade automatically in the background while queries stay indexed (`blowfish reindex --dry-run` reports remaining legacy counts).

### Added

- **Automatic, always-on recluster with base and delta tiers:** A background job now builds a reclustered base tier once a namespace reaches 100K rows across 2+ segments with an estimated gain of at least 1.25, maintains a delta tier on the base's coarse cells (merging into a new base at half the base size or rebuilding once updates or deletes mask one-fifth of rows), and runs one unified query plan across base, delta, and uncovered segments. On BEIR NQ (2.68M 1,024-dim `f16` rows), the automatic base build took 231 s at 791 MB RSS after a 269 s load, reaching 587 q/s at recall@10 0.962; with 5% of rows rewritten, instructions per query rose 11.3% (down from 36% with the earlier side search), and new `blowfish_recluster_*` metrics report progress.

### Changed

- **Single `.bfa` ANN object and vector-free, compressed `docs.bfc` v2:** Each segment and slab ANN index is now one range-read `.bfa` object (blobs, codes, and vectors), while `docs.bfc` v2 drops vector columns, compresses string columns with zstd, and drops the Bloom filter from `ids.bfi`. Storage footprint drops from 5,945 to 3,581 bytes/row (−40%) on BEIR NQ and from 5,406 to 2,887 bytes/row on the synthetic set, where cold float projection falls from 165.54 to 44.22 ms.
- **Graceful shutdown for background jobs:** On `SIGTERM` or `SIGINT`, all background jobs (indexer, compaction, upgrades, recluster, GC, and recall sampler) stop within seconds with a shared 5 s budget for in-flight work instead of blocking shutdown for minutes during a recluster build. Measured shutdown on BEIR NQ is 0.1 s, and interrupted work resumes safely on restart.
- **Recall sampler skips startup burst:** The background recall sampler now skips its first interval after startup and uses at most about 2% of the time thereafter, avoiding rereading about 5.5 GB on BEIR NQ right after every restart.

### Performance

- **Lower warm vector query overhead:** Added a typed `rank_by` reader, a batched center-ranking kernel, and single-pass query vector hashing. On warm BEIR NQ (16 clients, 6 vCPUs, 64 probes), instructions per query drop from 25.5M to 15.3M (−40%) at 574 q/s.
- **Exact MaxScore BM25 pruning and concurrent hybrid queries:** Added exact MaxScore-style pruning with window maxima and shared score floors, and parallelized hybrid multi-query branches with shared cutoffs. On local 1M MS MARCO (2 CPUs), warm 1-client p50/p90 drops from 18.4/42.4 to 11.3/21.8 ms, 16-client throughput rises from 50.2 to 99.9 q/s (−73% instructions/query), and all 12,240 responses match exhaustive results; on 100K BEIR NQ hybrid queries, warm multi-query p50/p90 drops from 14.8/22.6 to 12.0/15.8 ms (RRF 12.3/18.8 to 10.5/14.0 ms; cold RRF 335/392 to 303/366 ms).
- **Faster text index repacking and lower update memory:** Format-4 text indexes repack into format 5 without re-tokenizing in bounded batches (960K rows in 16 segments upgrade in 4.8 s instead of 62.7 s at 1.0 GB vs. 2.0 GB peak, byte-identical), and unconditional upserts no longer load old rows (on 681K BEIR NQ rows, update peak memory drops from 6.40 to 3.14 GiB and post-update RSS from 4.52 to 0.67 GiB).

### Removed

- **Manual recluster examples:** Removed `examples/recluster.rs` and `examples/repage.rs`, replaced by automatic background reclustering.

## [0.1.14] - 2026-11-23

### Upgrade notes

- **Bucket and manifest format 2 (no in-place downgrade):** Stop all older processes and back up the entire deployment prefix before starting v0.1.14; writable startup writes `_system/format/2.json` and moves manifests to format 2 (renaming `indexed_through` to `checkpoint_wal`), which v0.1.13 and older binaries cannot read or write. Legacy row-major vector indexes and format 1–4 text indexes remain readable while upgrading automatically in the background (or immediately via `blowfish reindex`).

### Added

- **Automatic vector index upgrade and `blowfish reindex`:** Writable servers now rewrite legacy row-major per-segment `.bfv` indexes and recluster slabs into the fast-scan layout automatically one index at a time from stored quantized codes without retraining, and `blowfish reindex [NAMESPACE|--all] [--dry-run]` runs or inspects the upgrade on demand.
- **Apple Silicon Docker images and fatal signal backtraces:** Docker images now build and smoke-test on `arm64` alongside `x86_64` (plus native macOS CI), and fatal signals (`SIGSEGV`, `SIGBUS`, `SIGILL`, and `SIGFPE`) print the signal, fault address, thread, and stack frames to stderr before exiting.

### Changed

- **Cross-cell recluster assignment:** Recluster now trains cells above 1,024 leaves two-level, assigns rows in 4-row blocks across their 4 nearest cells (12 nearest leaf groups), and runs one center-recompute and reassignment pass. On BEIR NQ at 64 probes, recall@10 reaches 0.961 at 32 cells (138 s build vs. 158 s per-cell at 0.955) and 0.961 at 8 cells (230 s vs. 216 s per-cell at 0.950), though 100M-shaped cells project to about 5 hours on 8 vCPUs compared to 1.7 hours previously.
- **Auto-scaled probes for unreclustered multi-segment namespaces:** Default `ann_nprobe` now scales by `min(S^0.3, 4)` on unreclustered multi-segment namespaces (leaving explicit `ann_nprobe` values and reclustered slabs unchanged), raising default as-loaded BEIR NQ recall@10 from 0.907 to 0.956–0.960.
- **Unified memory-limit detection:** Every memory-sized default, including the block cache, now uses the smaller of host RAM and the process's own cgroup `memory.max`, and macOS reads `hw.memsize` instead of assuming 4 GiB.

### Performance

- **Fast-scan vector index layout and 16-lane SIMD distance kernels:** New `.bfv` indexes transpose 1-bit RaBitQ codes into 32-row blocks with four precomputed `f32` factors scored via 4-bit lookup tables (`vpshufb` on AVX-512BW, `TBL` on NEON), and 16-lane AVX-512 distance kernels replace `numkong` `f32` distances (about 65 ns vs. 300–700 ns per 1,024-dim pair). Per-row scanning drops from 24 to 5 ns on x86-64 and from 26.5 to 5.4 ns on Apple M4, and older row-major indexes also gain AVX-512 and NEON popcount paths.
- **Multi-client vector query throughput:** Paced the background recall sampler, switched rerank reads to 16 KiB sparse blocks, skipped the store-wide planning lock on cached batches, moved decoded clusters to a sharded CLOCK cache, hydrated only projected columns, capped slab fan-out under load, and enforced `BF_DECODED_CACHE_BYTES` across all three decoded caches. On BEIR NQ (2.68M passages, 6 vCPUs, 16 clients), throughput rose from 27.1 to 616 q/s reclustered (recall@10 0.954 to 0.961; 1-client warm p50 28.7 to 5.3 ms) and from 27.7 to 400 q/s as loaded (recall@10 0.907 to 0.956; 1-client warm p50 23.5 to 5.2 ms).
- **Text index format 5 and BM25 latency:** Format-5 `.bft` indexes bundle FST dictionaries, term directories, and short posting lists, add compact `text_keys` row-to-ID maps fetched in the opening read wave, and precompute term weights and exact `f64` saturation tables with row-level pruning. On local 1M MS MARCO (2 CPUs, `overlap@10` 1.000), warm 1-client p50/p90 drops from 45.5/99.8 to 18.4/44.1 ms, fresh cold p50/p90 against a 25 ms/request local S3 facade drops from 457/670 to 255/353 ms, and mean cold reads per query drop from 189.0 to 117.2.

### Fixed

- **macOS file-descriptor exhaustion:** Startup now raises the soft open-file limit to the hard limit (256 to 61,440 on the test Mac), fixing `EMFILE` and `503` errors on ~100-segment `file://` namespaces.
- **jemalloc arena decay crash:** Adaptive jemalloc decay now configures arenas individually instead of via `arena.4096.*`, fixing macOS `mallctl` support and an intermittent jemalloc 5.3 cold-start segfault.

### Removed

- **Legacy layout and tuning environment variables:** Removed `BF_RECLUSTER_GLOBAL`, `BF_RECLUSTER_GROUPS`, `BF_RECLUSTER_REFINE`, `BF_ANN_FASTSCAN`, `BF_ANN_DIRECTORY`, `BF_ANN_BLOCKS`, `BF_ANN_SLABS`, `BF_ANN_PAGED`, `BF_ANN_SCAN_MEMO`, `BF_ANN_SPECULATE`, `BF_READER_COPY`, `BF_ANN_SEGMENT_SPREAD`, `BF_IVF_CLUSTER_FACTOR`, and `BF_NO_AVX512`; setting them now has no effect.

## [0.1.13] - 2026-11-19

### Upgrade notes

- **Additive on-disk changes and active `BF_*` variables:** New objects (`idfilter.bff`, paged slab directories, compact `manifest-latest` copies, and `Manifest::pruned_through`) are additive so existing namespaces open unchanged without re-ingestion; existing reclustered namespaces can add paged directories and compact manifest copies via `examples/repage.rs` and `examples/readercopy.rs`. Review deployment environments for leftover `BF_*` variables because `blowfish serve` now honors all of them.

### Added

- **100M-document scale on a single node:** blowfish now loads, reclusters, and queries 100M 1,024-dim `f16` documents on one `c7gd.2xlarge` (8 vCPU, 16 GB RAM, and local NVMe) against same-region AWS S3. Loading took 5.7 h (4,891 docs/s average, 6.8 GB peak RSS), recluster took 1.7 h (160 slabs, 499,920 clusters, 6.0 GB peak RSS), and 64-probe queries (recall@10 0.954) run at 1,243/1,370 ms fresh cold (1,201/1,391 ms cache-cold) and 99.7/162.8 ms warm (72.6/90.4 ms warm at 48 probes, recall@10 0.941).
- **Official SDK retry headers:** Every `429` and `503` response now sends `Retry-After` and `retry-after-ms` (estimated from WAL backlog and drain rate, clamped to 0.5–30 s), and `x-should-retry` is set to `true` on `408`, `409`, `429`, `502`, `503`, and `504` and `false` on `500`, verified against the official Python and TypeScript SDKs.

### Changed

- **Memory-sized defaults and jemalloc with adaptive decay:** Linked jemalloc (`bf-alloc`) with adaptive dirty/muzzy page decay and derived segment row caps, flush thresholds, build concurrency, caches, and recluster slabs from host RAM or cgroup memory. Combined with a packed `f16` index builder, streamed text-statistics merges, flat BM25 postings, and cache-bypassing scans, a 4 GB `c7gd.large` (swap off) holds a 10M load and recluster at 2.3–2.9 GB peak RSS (down from 13–16 GB for ingest and 16.9 GB for recluster).
- **`blowfish serve` honors all `BF_*` engine variables:** Server startup now initializes from `EngineConfig::from_env()` so all `BF_*` engine knobs take effect, with CLI flags and TOML settings retaining precedence.

### Performance

- **Flat ingest throughput at scale:** Per-segment ID Bloom filters (`idfilter.bff`), larger bulk flushes, deferred compaction up to 1,024 segments, parallel text-statistics merges, and concurrent ANN directory writes keep ingest flat as namespaces grow. On 10M rows, ingest throughput rises from 2,126 to 4,501 docs/s on `c7gd.xlarge` (4,704 to 2,222 s) and to 2,918 docs/s on `c7gd.large` (11,568 to 3,427 s).
- **Paged slab directory and compact manifest copy:** Recluster now writes a tour-ordered paged slab directory for namespaces with at least 100,000 clusters (4.0 MB header at 100M; about 18 MB fetched per 64-probe cold query instead of 185.7 MB) alongside a compact `manifest-latest` copy (375 KB instead of 3.4 MB at 100M) and speculative scan reads. Cold 100M reads drop from about 327 MB to 159.9 MB per query, and 10M in-region vector cold p50/p90 reaches 680/907 ms fresh and 533/656 ms cache-cold (recall@10 0.953–0.956).
- **Parallel slab scans for warm vector queries:** Probed slabs now run on separate runtime tasks with `as_chunks` bit-plane transposition. Warm p50/p90 drops from 163.0/195.3 to 99.7/162.8 ms at 100M (64 probes), from 119.6/138.0 to 72.6/90.4 ms at 100M (48 probes), and from 42.2/54.7 to 36.6/50.8 ms at 10M (48 probes).
- **Faster cold BM25 queries:** Co-located term records, manifest text headers, window-pruned posting reads, concurrent segment reads, and the fixed-key manifest copy cut in-region 1M MS MARCO fresh cold p50/p90 from 453/569 ms in v0.1.12 to 380/526 ms fresh and 301/369 ms cache-cold (17.1/34.5 ms warm; `overlap@10` 1.000).

### Fixed

- **WAL payload GC stalls:** `Manifest::pruned_through` now tracks reclaimed WAL progress so GC reads and deletes only `(pruned_through, through]` with 32-way concurrency instead of scanning serially from sequence 1 on every pass, which previously stalled indexing for 30–230 s per pass at 22M–25M rows.

[Unreleased]: https://github.com/endgame-labs/blowfish/compare/v0.1.59...HEAD
[0.1.59]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.59
[0.1.58]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.58
[0.1.57]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.57
[0.1.56]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.56
[0.1.55]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.55
[0.1.54]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.54
[0.1.53]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.53
[0.1.52]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.52
[0.1.51]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.51
[0.1.50]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.50
[0.1.49]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.49
[0.1.48]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.48
[0.1.47]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.47
[0.1.46]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.46
[0.1.45]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.45
[0.1.44]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.44
[0.1.43]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.43
[0.1.42]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.42
[0.1.41]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.41
[0.1.40]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.40
[0.1.39]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.39
[0.1.38]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.38
[0.1.37]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.37
[0.1.36]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.36
[0.1.35]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.35
[0.1.34]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.34
[0.1.33]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.33
[0.1.32]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.32
[0.1.31]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.31
[0.1.30]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.30
[0.1.29]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.29
[0.1.28]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.28
[0.1.27]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.27
[0.1.26]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.26
[0.1.25]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.25
[0.1.24]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.24
[0.1.23]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.23
[0.1.22]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.22
[0.1.21]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.21
[0.1.20]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.20
[0.1.19]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.19
[0.1.18]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.18
[0.1.17]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.17
[0.1.16]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.16
[0.1.15]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.15
[0.1.14]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.14
[0.1.13]: https://github.com/endgame-labs/blowfish/releases/tag/v0.1.13
