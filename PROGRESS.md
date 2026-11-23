# blowfish build progress 🐡

Newest first. Each entry says what works now and how it was checked.

## 2026-11-23

- **Iteration 22: the fixed costs of a warm vector query** (`docs/benchmarks.md`).
  - A warm reclustered NQ query takes 15.3 M instructions against main's 25.5 M (−40%), and 21.4 M cycles against
    26.6 M (−20%), with the same results (recall@10 0.961). At 16 clients on a shared VM that was 574 against 509 q/s,
    and 657 q/s at 48 probes (recall@10 0.951). The 1,000 q/s gate is not met: a query still costs about 7 ms of CPU at
    64 probes and 5.9 ms at 48. The default stays at 64 probes until recall at 10M and 100M layouts supports 48.
  - Changes: a typed reader for plain `rank_by` vectors, query bodies parsed without `serde_path_to_error`, vectorized
    fast-scan bookkeeping with factors checked once a cluster, one batched kernel for directory center dots, integer
    probe-pick keys, stub lists built once a section, and the per-slab SipHash of the query vector removed from the
    scan memo key.
  - Checked: fmt, workspace clippy, and `cargo test --workspace` (256 passed); compat 121/121; the branch serves
    main's reclustered store with identical recall; and the `bench` ladder against main (same recall at 10K, 50K, and
    200K rows, with warm ANN p50 lower).

- **Iteration 21: one recluster assignment at every size, and a lighter query path** (`docs/benchmarks.md`).
  - `recluster` trains a large cell's leaves two-level and assigns rows across cells in blocks of 4, against leaf groups.
    Cross-cell assignment is now the only one. At 32 cells on BEIR NQ it takes 138 s against main's 253 s, at the same
    recall@10 (0.961). At 8 cells (10M-sized cells) it gives 0.961 against the old per-cell 0.950, at about the same
    time and memory. A 100M-shaped cell run (about 16,000 leaves a cell) puts a 100M assignment at about 5 hours on 8
    vCPUs, against 1.7 hours for the last full 100M recluster.
  - Queries: k-means, center distances, and rerank use new 16-lane f32 kernels in place of numkong's. The text stats header
    is shared instead of copied into every query's state. Rerank checks a whole cluster's CRC once, and the
    decoded-cluster cache hashes with foldhash. On NQ, reclustered q/s at 16 clients went from 580 to 616 at the same
    recall, and as-loaded 1-client p99 went from 27.9 to 9.3 ms.
  - Per SPEC decision 25, these settings are removed: `BF_RECLUSTER_GLOBAL`, `BF_RECLUSTER_GROUPS`,
    `BF_RECLUSTER_REFINE`, `BF_ANN_DIRECTORY`, `BF_ANN_BLOCKS`, `BF_ANN_SLABS`, `BF_ANN_PAGED`, `BF_ANN_SCAN_MEMO`,
    `BF_ANN_SPECULATE`, `BF_READER_COPY`, `BF_ANN_SEGMENT_SPREAD`, `BF_IVF_CLUSTER_FACTOR`, and `BF_NO_AVX512`. They are
    listed in `docs/configuration.md`.
  - Checked: fmt, workspace clippy, and `cargo test --workspace` (252 passed); compat 121/121; the branch serving main's
    store gives the same recall; and the `bench` ladder against main
    (same recall at 10K, 50K, and 200K rows, with warm ANN p50 a little lower).
- **BM25 round: local measurements** (`docs/benchmarks.md`). I loaded 1M MS MARCO passages,
  matched both arms to six segments, and ran the 1,000 dev queries on CPUs 6–7 at nice 10.
  - Warm c1 p50/p90: 45.5/99.8 ms before, 18.4/44.1 ms after. Warm c16: 658.5/827.4 ms before,
    326.3/441.3 ms after. Strict overlap@10 is 1.000 for every query at both client counts.
  - Fresh-process queries through a local read-only S3 facade with 25 ms per request: 457/670 ms
    before, 255/353 ms after; 189 to 117 reads, 4.35 to 3.61 MB, and 3.25 to 3.02 mean waves.
    The mean RTT-only floor is 81.3 to 75.4 ms. The facade models dependencies, and does not model
    AWS tails, throughput, TLS setup, or the in-region CPU. All 60 cold queries match exact truth.
  - Live text index size: 730.4 to 743.8 MB (+1.8%), plus 246 bytes of compact ID maps. An earlier
    copy-based bundle prototype grew indexes 33% and was replaced with the single-copy layout.
    The warm 13/18 ms and two-wave targets remain open; AWS confirmation remains outstanding.
    Other jobs shared the VM, so timings are sensitive to contention.
  - New local runners (`bench/scale/bm25_local.py` and `local_s3.py`) keep temporary logs under
    `.scratch/`, bound client and process waits, stop their exact child PIDs, and clean up on exit.

- **Compact IDs for BM25:** text segments now store a checksummed row-to-ID map using numeric
  strides or string prefixes, admitted under a fixed 128 KiB encoded-size budget. ID-only BM25
  opens that map with text metadata and skips document footers and winner-ID column reads.
  Larger maps record a 17-byte marker and use existing ranged ID reads, with no operator switch.
  - The normal indexer backfills older segments once, and checkpoint comparison notices the new
    file role. The manifest schema and bucket fence remain unchanged. The six local 1M-row
    segments upgraded automatically; their ID maps total 246 bytes.
  - Checked: formatting, release clippy for `bf-text` and `bf-engine`, all targets, and warnings
    denied; 136 Rust tests; compat 121/121 in normal and forced modes; SDK retry checks 4/4 in
    each mode; and TypeScript smoke and retry checks in each mode. The read-count regression
    requires zero document reads, unchanged answers, and a backfill followed by an idle no-op.

- **Exact BM25 bounds and bundled text reads:** text format 5 keeps dictionaries, term directories,
  and up to 32 posting blocks per term together, with one stored copy of every payload. Bundles
  contain at most 128 terms and use an estimated 32 KiB budget; large directories occupy their own
  bundle. Long postings and phrase positions remain independent.
  - Sparse contributions are summed at the same row before taking window bounds. Dense scoring skips
    rows below the remaining-score bound. Term weights and cached exact f64 saturation factors avoid
    repeated logarithms and divisions while preserving reference score bits. Immutable decoded sparse
    postings use a bounded cache charged to the engine metadata budget.
  - The background indexer rebuilds one older text field per fenced publication. Checkpoint comparison
    notices replaced text objects even when segment IDs and live masks stay unchanged, so later passes
    adopt the upgrade instead of repeating it. All six active indexes of the local 1M-row namespace
    upgraded automatically. The manifest schema and bucket fence versions are unchanged.
  - Checked: formatting, clippy for `bf-text` and `bf-engine` with all targets and warnings denied,
    132 Rust tests, compat 121/121 in normal and forced modes, SDK retry checks 4/4 in each mode,
    and TypeScript smoke and retry checks in each mode. Both c1 and c16 match exhaustive BM25 on all
    1,000 dev queries. All 60 fresh-process cold queries also match.

- **Bucket format fence:** format-2 manifests rename the required checkpoint field to `checkpoint_wal`,
  and every manifest decoder rejects unsupported versions. Writers and maintenance tools cannot republish
  unfamiliar manifests. Legacy format-1 manifests remain readable and upgrade on publication.
  - Append-only, create-only `_system/format/<version>.json` markers raise the deployment's required
    generation monotonically. Startup checks before its mutating probe; writes, indexing, and GC check
    again. Project-scoped engines share the root fence. Observing a newer marker disables all mutations
    through that process. Read-only startup never raises the marker.
  - Verified against a real binary built from tag `v0.1.13`: it created a legacy local bucket, this branch
    read and upgraded it, and the old binary then failed decoding with `missing field indexed_through`.
    Queries and writes returned 500, indexing and GC deferred, and all object hashes stayed unchanged
    during the old-binary run. The new binary still returned both acknowledged rows afterward.
  - Writable and read-only startup refused a newer marker without changing the bucket. All verification
    used local filesystem storage. The upgrade and backup requirements are in `docs/operations.md`.
  - Checked formatting, workspace clippy with all targets and warnings denied, and workspace tests
    (243 passed, 4 ignored). Compat passed 121/121 in both normal and forced-indexing modes, alongside
    SDK retry checks and the TypeScript smoke suite. Idle indexing still performs no object-store I/O.

- **Automatic upgrade of old vector indexes** (`docs/operations.md`, "Upgrading to v0.1.14"). Indexes in the older
  row-major layout only became fast-scan when compaction or `recluster` rewrote them, so a read-only namespace stayed
  on the old layout. The background indexer now rewrites each per-segment index and each `recluster` slab in the
  fast-scan layout by itself, from the stored codes (no retraining), one index at a time. It writes new objects, then
  swaps a batch into the manifest with the fenced publish the indexer uses, retires the old objects for garbage
  collection, and rebuilds the ANN directories. It skips indexes already upgraded. `blowfish reindex [NAMESPACE|--all]
  [--dry-run]` runs the same upgrade now and reports counts and bytes per layout.
  - Per SPEC decision 25 (one way to do each thing): `BF_ANN_FASTSCAN` and the row-major writer are gone, and there is no
    way back to the old layout. Reading it stays, so it can be upgraded. Tests make old-layout files with
    `bf_vector::downgrade_layout`, which only the `legacy-fixtures` feature builds.
  - Checked: `bf-engine/tests/reindex.rs` (answers identical, a rerun is a no-op, writes racing an upgrade are kept,
    slabs upgraded, and a server with indexing on upgrades by itself) and `bf-vector/tests/relayout.rs`.

- **Iteration 20: more q/s on BEIR NQ** (`docs/benchmarks.md`). This used the same VM and limits as iteration 19, with
  interleaved runs against main.
  - Reclustered: 573 q/s at 16 clients at recall@10 0.954, against main's 395 at 0.955. At the default 64 probes,
    recall@10 is 0.960, up from 0.955.
  - As loaded at default settings: 387 q/s against 269, at the same recall (0.958).

  What changed:
  - New indexes use a fast-scan code layout. It holds 32-row transposed blocks scored with 4-bit table shuffles
    (AVX-512BW, NEON, and scalar, all with the same sums), and it is about 3 times faster per row (5 times on an Apple
    M4). The table rounding is part of the error bound, and the candidates are rescored with a fine table where the
    rounding matters.
  - The scan applies the cut inside its block loop.
  - Cached reads are zero-copy, the query is rotated once per query, and f16 converts a block at a time.
  - `recluster` assigns rows to their nearest leaf across their four nearest cells. It does this by default only when
    cells hold at most 1,024 leaves, so the 10M and 100M layouts are unchanged. Forced on with 10M-size cells, it took
    5 times the per-cell job.
  - The decoded cache now stays within `BF_DECODED_CACHE_BYTES`.

  Old indexes still read: the branch on main's reclustered store gives the same recall.

  Checked:
  - `cargo test --workspace`, clippy, and the compat suite (121/121);
  - the `bf-vector` and `bf-engine` tests on an Apple M4 (NEON);
  - the `bench` ladder against main (same recall at 10K, 50K, and 200K rows).

  New settings (`BF_RECLUSTER_GLOBAL`, `BF_RECLUSTER_GROUPS`, and `BF_RECLUSTER_REFINE`) were added here and
  removed again in iteration 21.

## 2026-11-22

- **Iteration 19: BEIR Natural Questions on one machine** (`docs/benchmarks.md`). I reproduced Kerry Kurian's outside
  benchmark ("Blowfish on BEIR Natural Questions") on this VM: 2.68M f16 vectors at 1,024 dimensions, a server on 6
  pinned CPUs with 12 GB. Then I fixed what it found. Reclustered at 64 probes, recall@10 is 0.955 (was 0.954):
  - warm p50/p95/p99 went from 28.7/457/732 to 8.3/13.3/18.1 ms;
  - 16-client throughput went from 27 to 444 q/s.

  As loaded at default settings, recall@10 went from 0.907 to 0.960: the default probe count now scales up, by at most
  4 times, for independently clustered segments. An explicit probe count is used as given, and reclustered layouts are
  unchanged. The causes:
  - the recall sampler's exhaustive scans;
  - 1 MiB rerank blocks;
  - a store-wide lock on fully cached reads;
  - per-query decode and allocation;
  - hydrating unprojected text;
  - rayon fan-out under load.

  The fixes added AVX-512 and NEON RaBitQ kernels, both bit-identical to the scalar path, and a bounded scan. On the
  same VM, Qdrant 1.20 at ef 64 serves about 6 times blowfish's q/s, and LanceDB 0.41 at 120 probes about a sixth of
  it. On an Apple M4, with all three engines, blowfish went from 38 to
  870 q/s at 16 clients (p99 656 to 4.1 ms), against LanceDB's 281 and Qdrant's 7,572 q/s at the same recall.
  blowfish now also raises its open-file limit at startup. macOS's default of 256 made as-loaded queries on about a
  hundred segments fail with a 503.

  Checked:
  - `cargo test --workspace`, clippy, the compat suite (121/121), and the Python and TypeScript SDK smokes;
  - the small `bench` ladder (10K to 200K rows, against main): same recall, lower warm ANN latency, and cold ANN within
    run-to-run spread.

  New settings are in `docs/configuration.md`.

## 2026-11-21

- **Issue #2 root cause found and fixed.** The cold-start segfault came from jemalloc 5.3: blowfish set decay with
  the `arena.4096.*` (all-arenas) shortcut, which jemalloc accepts but then reads one past the end of its arena array,
  into `arenas_lock`. After any contention for that lock, its wait-time counter was used as an arena pointer. The
  decay thread first runs 500 ms after start, so builds before 0457539 crashed about half a second after "ready" on
  some starts. Reproduced on x86_64 (11 of 20 runs at 16 contending threads, 13 of 20 at 64); the per-arena code
  already on main had 0 failures in 80 runs, and a regression test now covers it.
- **Crash reports on fatal signals (issue #2).** blowfish now writes the signal, fault address, thread, and stack frames
  to stderr before dying on SIGSEGV, SIGBUS, SIGILL, or SIGFPE, then hands the signal back so the exit status and Rust's
  stack-overflow message stay the same. Issue #2 (a segfault on 14 of 68 cold restarts on an M4 Max, arm64 Docker) did
  not reproduce: 0 crashes in 120 cold restarts on an M4 across the same Docker image (small and 100K x 768 namespaces,
  8 concurrent queries at startup) and a native macOS build. Checked: real null fault, stack overflow, and kill on Linux,
  and real fault on macOS. Also: turbopuffer's 100M namespace still reported warm (201 ms) after a day idle.

## 2026-11-20

- **turbopuffer at 100M, measured by me.** I loaded the same 100M rows into turbopuffer (14.7 h, 450.4 GB billable, 4,846
  429 retries) and measured recall and latency (`docs/benchmarks.md`, "turbopuffer at 100M, measured by me"). Warm ANN
  server time is 14 / 19 ms p50 / p90 with recall@10 of 0.9465; blowfish's current warm p50 / p90 is 72.6 / 90.4 ms at 48 probes. The
  namespace never reported cold over idle gaps up to 19,200 s (503 ms server time, warm). `scale_load.py` now retries any
  5xx through the shared backpressure limiter and sums turbopuffer's write billing.
- **Docker on Apple Silicon, and macOS (issue #1).** The image now builds on arm64: trixie base images, clang, and
  numkong's SME targets off on arm64 (GCC 12 rejected numkong's inlined NEON dot-product intrinsics). CI builds and
  smoke-tests the image on x86_64 and arm64 runners and builds and tests natively on macOS. Testing on a real Mac
  found two macOS bugs, now fixed: the adaptive jemalloc decay never switched (macOS rejects the all-arenas mallctl
  shortcut, so arenas are now set one by one), and a compaction test depended on host memory. Checked: CI green on all
  jobs, the full test suite on an M-series Mac, and the official Python SDK running vector and BM25 queries against a
  native macOS build.

## 2026-11-19

- **100M run (branch `wt/scale100m`): load, recluster, and cold and warm ANN, in-region and over the internet.**
  - **Load:** 100M Cohere Wikipedia rows (English, then de, fr, es, ru) into S3 on one c7gd.2xlarge in 20,445 s, 4,891 docs/s on average (6,000 to 6,600 docs/s to 50M, 2,000 over the last 5M). Peak server RSS 6.8 GB.
  - **Bug fixed:** the WAL payload GC re-read the whole log serially every pass, stalling indexing for minutes past 22M rows. Now `Manifest::pruned_through` plus 32-way concurrency (`Wal::prune_payloads_from`, tested).
  - **Recluster:** 6,248 s, 160 slabs, 499,920 clusters, 98.7 MB directory, peak RSS 6.0 GB.
  - **Cold ANN, 100M, in-region, 48 probes:** fresh 3,105 / 4,051 ms, cache-cold 2,698 / 3,071, warm 333 / 378 (10M: 662 / 991, 574 / 652, 70.7 / 84.1). Recall@10 0.941 (200 queries); 64 probes 0.9555 and 2,899 / 3,806 fresh. A cold query reads 294 MB (10M: 50.7): the two directory hints are 186 MB of it.
  - **Over the internet (not like-for-like):** fresh 6,399 / 8,929 ms, warm 448 / 513. The 100M data stays under `big/` in the bucket until Kyle has seen the results.
  - **Checked:** truth spot-checked against brute force on nine shards; `cargo test -p bf-wal -p bf-engine`, clippy.

## 2026-11-17

- **Perf iteration 11 (10M): block fragments, fewer requests per cold ANN query.**
  - **Diagnosis:** a cold ANN query made 1,069 requests and read 75 MB at 64 probes (2,076 and 153 MB at 160). The scan wave is almost all of it: each probed cluster cost two reads (blob from the fragment, codes from the index object), and 64 probes are about 540 clusters over 70 segments. The rerank is about 50 small reads.
  - **Changes:** block fragments (`annfrag2:`, directory `ann2:`, one read per probed cluster, blocks in nearest-neighbor tour order); the scan's reads start during the WAL tail read; no documents' footers in the scan wave; reranks off the CPU workers; every segment starts at once. Old binaries and filtered queries keep using the plain objects. The 10M namespace was upgraded in place (additive).
  - **10M cold, same-session A/B, 30 queries:** 64 probes p50 / p90 2,421 / 3,291 to 1,502 / 1,834 ms (1,112 to 462 requests, 67.6 to 52.9 MB); 160 probes p50 5,422 to 3,280 ms (2,104 to 879 requests, 137.9 to 106.5 MB), with a heavy R2 tail on the p90. Recall is unchanged (0.8855 at 64, 0.9465 at 160). The stop rule is still not met.
  - **Checked:** fmt, clippy, `cargo test`, compat 121/121 in both modes, and a test that upgrades a namespace without block fragments.

- **Perf iteration 10 merged (10M): ingest and the first full 10M run.**
  - **Pipelined WAL:** up to `BF_WAL_PIPELINE` (8) groups upload at once, and slots still commit in order. On 200K docs, ingest went from 1,152 to 2,040 docs/s. The remaining limit is the VM's upload bandwidth to R2, about 14 MB/s.
  - **Other changes:** an adaptive warm pool (floor `BF_WARM_FLOOR`), and a cold open now fetches the ANN directory in parallel with the manifest.
  - **10M cold, p50 / p90:**
    - ANN: 3,000 / 4,380 ms;
    - BM25: 4,380 / 5,820 ms;
    - hybrid: 4,160 / 4,770 ms;
    - selective: 6,560 / 8,710 ms.

    A cold query makes 1,100 to 2,600 requests and reads 61 to 268 MB. The target is ANN p50 ≤ 1,224 ms, so this is not there yet.
  - **Recall at 10M:** ANN is 0.886 at 64 probes and 0.947 at 160, against turbopuffer's 0.943. Selective is 0.968 at 64 probes and 0.992 at 160.
  - **Memory:** RSS reaches 13 to 16 GB past 7M rows and spikes during compaction. A heap profile comes next.
  - **Fix:** prefix search on an empty chunked text dictionary no longer panics. A proptest found it.

## 2026-11-16

- **Native embedding and model reranking merged.**
  - **Registry:** a `[models]` registry maps turbopuffer model names to providers: `local` (in-process llama.cpp GGUF, cargo feature `local-models` or `local-models-native`), `openai_embeddings`, and `rerank`.
  - **turbopuffer surface:** `embed` in the schema, write-time embedding (256-row cap, embedded before conditions), `["Embed", ...]` in ANN and kNN, `rerank_by {model, attribute, query}` on single and multi-queries, and `embedding_tokens` and `embedding_ms`.
  - **Real check on CPU** (EmbeddingGemma 2 and Qwen3-Reranker-0.6B):
    - query embedding: median 35 ms;
    - reranking: about 0.8 s per candidate, so use a GPU or a hosted rerank provider for real traffic;
    - memory: about 1.6 GiB RSS with both models loaded.
  - **Open:** patches don't re-embed; rerank runs candidates one at a time with no prefix reuse; and the 256-row cap applies even when vectors are supplied.

- **Perf iteration 9 merged (1M): R2 tail latency.**
  - **Floor model:** one GET is 72 ms p50 and 309 ms p99. A wave of 256 parallel GETs takes about 290 ms on warm connections and about 1.05 s on fresh ones; a new TLS connection costs about 65 ms.
  - **Changes:**
    - A warm connection pool (`BF_WARM_CONNECTIONS`, default 256).
    - Hedged ranged reads, capped at 10% extra requests.
    - The ANN half of hybrid no longer waits for BM25 term warm-up.
    - An interleaved A/B harness (`bench/scale/ab.py`).
  - **Same-session cold p50 / p90 at 1M:**

    | Query | Before | After |
    |---|---|---|
    | ANN | 1,392 / 1,950 ms | 1,278 / 1,593 ms |
    | BM25 | 1,069 / 1,816 ms | 869 / 1,594 ms |
    | hybrid | 1,635 / 2,714 ms | 1,188 / 1,752 ms |
    | selective | 1,958 / 2,642 ms | 1,973 / 2,600 ms |

  - **Recall and parity:** unchanged.
  - **Open:** selective needs a new structure (1,900 requests); the cold ANN layout should put blobs next to codes; the manifest and directory are read in sequence; and the warm pool is fixed and always on, costing about $5 a month in R2 HEADs even when idle.

- **Projects, keys, and the admin console merged and deployed.**
  - **Model:** org superadmin keys; projects with isolated namespaces under `projects/<id>/`, with `default` mapping to the root; and project `admin`, `read_write`, and `read` keys.
  - **API:** `whoami`, the admin API, and `docs/api/admin.openapi.yaml`.
  - **Webapp:** the Go app in `webapp/` with magic-link sign-in through the exe.dev email gateway.
  - **Deployment:** live serves the console at `/console/` through Caddy, run by the `blowfish-webapp` systemd unit. It uses a minted superadmin key named "console".
  - **Checks:** the turbopuffer SDK smoke test passed after the upgrade.

- **Live instance upgraded** to main (f5b08a9), from abb3cfd. The previous binary is kept for rollback, and the `live/` prefix was backed up first. The turbopuffer SDK smoke test passed for upsert, ANN, BM25, and filtered queries. Unauthenticated requests still get 401.
- **New docs:**
  - `docs/design/projects-and-keys.md`: org, projects, project keys, the admin API, and the Go admin webapp with magic-link login.
  - `docs/open-source-checklist.md` (removed 2026-11-20).

- **Perf iteration 8 merged (1M).**
  - **Changes:**
    - FTS dictionaries are chunked (2,048 terms) and corpus statistics are chunked, with the chunk index in the manifest.
    - BM25 term prefetch runs in the snapshot wave, and the ANN half of hybrid no longer waits for statistics.
    - Document lengths are bit-packed.
    - Compaction produces evener segments: 9 instead of 15.
  - **Bytes per cold query:** BM25 38 → 6.5 MB, hybrid 61 → 28 MB. Requests are flat: ANN 374, selective 1,899.
  - **Latency:** no measurable change. R2 varied about 2× between sessions; cold ANN ranged from 1,472 to 2,656 ms on the same layout.
  - **Recall:** ANN 0.952 and selective 1.000.
  - **Open:** selective request count, cold ANN probe requests, warm p50 (71-113 ms, unexplained), and peak RSS (5.4-9.6 GB).

- **Perf iteration 7 merged (1M).**
  - **Ingest:** 740 → 1,199 docs/s over the whole load. The tail is no longer cloned or re-downloaded, bulk loads build large segments, and flush and compaction run concurrently. GET bytes during the load fell from 5.1 GB to 1.5 GB.
  - **Tail writes:** an unindexed tail keeps the ANN directory path.
  - **BM25:** large segments take hit IDs from the docs `id` column, so BM25 reads 38 MB per cold query, down from 49 MB.
  - **Cold p50 at 1M** (best of two sessions): ANN 1,118 ms, BM25 1,212 ms, hybrid 1,525 ms, selective 1,699 ms. No cold-latency gain this round.
  - **Recall:** ANN 0.949 against turbopuffer's 0.939; broad 0.978 against 0.977.
  - **Open:** chunked FTS dictionaries and stats (28 MB + 5.9 MB read whole), selective filters (1,949 requests), cold ANN request count, vectors stored twice, and WAL upload bytes.

- **Perf iteration 6 merged (1M).**
  - **Changes:**
    - The disk cache writes behind reads with a bounded queue. Awaiting block writes had cost 400-900 ms per cold wave.
    - Filtered ANN goes through the directory.
    - Row-to-cluster maps in fragments make small filters exact and fetch their vectors directly.
  - **Cold p50 / p90 at 1M** (quiet session; R2 noise swung results up to 40%):

    | Query | Before | After |
    |---|---|---|
    | ANN | 2,000 / 2,355 ms | 1,065 / 1,257 ms |
    | BM25 | 2,371 / 2,712 ms | 1,240 / 1,428 ms |
    | Hybrid | 2,404 / 2,983 ms | 1,478 / 1,576 ms |
    | Selective | 3,800 / 4,310 ms | 1,623 / 2,053 ms |

  - **Warm p50:** about 48-53 ms.
  - **Recall:** ANN 0.942 against turbopuffer's 0.938 at 1M; selective 1.000; broad 0.973 against 0.977.
  - **Parity overlap:** ANN 0.929, BM25 0.990.
  - **Ingest:** 740 docs/s, falling as N grows.
  - **Open for 10M:**
    - Unindexed tail upserts disable the directory path.
    - BM25 reads 49 MB per cold query (ID indexes, lengths, and statistics), which grows linearly.
    - Selective filters need about 1,900 requests.
    - Ingest is bound by bytes uploaded per doc.

- **Perf iteration 5 merged (100K): scale-ready ANN directory.**
  - **Changes:**
    - Directory centers are 1-bit codes: 196 B per leaf, down from about 1.1 KB. The 100K directory is 139 KB; projected at 10M it's about 4.4 MB.
    - Segments write fragments, so publication never reopens existing segments. A 60-segment test shows cold ANN stays at 3 waves.
    - Flat-segment vectors are prefetched in parallel.
  - **Cold p50 / p90:** ANN 1,044 / 1,681 ms, within run-to-run noise of iteration 4 (829-1,088 ms); BM25 943 / 1,429 ms; hybrid 1,023 / 1,141 ms; selective 1,120 / 1,260 ms.
  - **Warm p50:** about 59 ms, of which the strong-consistency HEAD is about 55 ms.
  - **QPS at concurrency 32:** 107-172.
  - **Recall and parity:** ANN recall 0.984. Parity overlap: ANN 0.968, BM25 0.982.
  - **Open:** filtered ANN through the directory, sparse rerank ranges, and BM25 block-max.

- **Perf iteration 4 merged (100K): namespace ANN directory.**
  - **Changes:** a persisted leaf directory per publication, referenced from the manifest. Cold unfiltered ANN opens no segment index: 3 waves in a 20-segment test. Reranking is error-bounded, and hydration reads the docs `id` column instead of the ID index.
  - **Cold p50:** ANN 1,937 → about 900 ms (old path in the same session: 1,160 ms); BM25 1,415 → 725 ms; hybrid 2,167 → 1,080 ms; selective 1,616 → 1,172 ms.
  - **Requests:** cold ANN GETs per query 240 → 129.
  - **Ingest:** 616-655 → 798-920 docs/s, with 0 429s.
  - **Recall and parity:** recall@10 0.984, parity ANN overlap 0.962. The old path scores the same on this reload.
  - **Open:** filtered ANN still uses the per-segment path; directory centers are int8 (about 1 KB per leaf); directory rebuilds reopen segments; ID indexes are still read whole on upserts.

- **turbopuffer reference at 1M and 10M loaded.** These are kept namespaces for later blowfish comparisons at the same sizes.
  - **Recall@10 for turbopuffer:** ANN 0.938 at 1M and 0.943 at 10M; selective 0.998 and 0.993; broad 0.977 and 0.971.
  - **Server p50:** about 12 ms at 1M, all hot. At 10M, 53-172 ms across query types, mostly warm, with tails of 300-900 ms.
  - **BM25 IDF:** the constant +0.169 offset from 100K doesn't reappear at 1M or 10M. Only the rarest terms differ much, so it isn't a document-count factor.
  - **Tooling:** `oracle_compare.py` gained `--tpuf-only`, `--data`, and `--cold-probe`. New: `make_oracle_truth.py` and `bm25_idf_tpuf_only.py`. Details are in `docs/benchmarks.md`.
- **Perf iteration 3 merged (100K).**
  - **Changes:**
    - The manifest slot is inline (13 KB, down from 867 KB), and term statistics moved to an FST object.
    - Text segments are now v2, with lazy per-term records.
    - Snapshot, tail probe, and hydration are overlapped, and hybrid subqueries run concurrently.
  - **Cold results:**
    - BM25 is 1.42 s and 7 MB (was 1.64 s and 24 MB).
    - Hybrid is 2.17 s and 22 MB (was 2.71 s and 39 MB).
    - Selective is 1.62 s (was 2.46 s).
    - ANN is flat, at about 1.9 s.
    - R2 latency was about 25 ms higher during the run, and quiet single traces show ANN about 1.1 s and BM25 about 0.9 s.
  - **Waves:** still 5-6.
  - **Recall@10:** ANN 0.988.
  - **Eventual-consistency warm:** ANN 35 ms and BM25 9 ms, against about 100 ms strong.
  - **turbopuffer parity:** unchanged.
  - **BM25 scores:** for about 40% of terms, turbopuffer's IDF is a constant +0.169 higher, with identical ordering. Its namespace reports 100,000 rows, so the cause is still open.
  - **Compatibility:** older binaries can't read the new text segments and manifests.
  - **Checks:** clippy, 161 tests, and compat 121/121 in both modes.
- **turbopuffer reference (100K) added.**
  - **Namespace:** the same 100K docs, loaded once into a kept turbopuffer namespace.
  - **Comparison:** `bench/scale/oracle_compare.py` compares parity, recall, and latency at each milestone.
  - **First result against main:**
    - overlap@10 is 0.963 (ANN), 0.982 (BM25), 0.973 (hybrid), 0.998 (selective), and 0.992 (broad).
    - Recall@10 for blowfish vs turbopuffer is 0.980 vs 0.963 (ANN), 1.000 vs 0.998 (selective), and 0.995 vs 0.996 (broad).
    - Warm client p50 is about 75 ms on both sides, but turbopuffer's server time is about 15 ms, while about 70 ms of blowfish's is the R2 consistency HEAD.
    - BM25 scores run 1.5-2% below turbopuffer's, with identical top-1. Corpus statistics are suspected; to be fixed.
- **Perf iteration 2 merged (100K docs on R2, with inner loops).**
  - **IVF format:** rotated centers, compact codes stored contiguously, a popcount scan, and merged reads.
  - **Manifests:** large ones are zstd-compressed.
  - **ANN defaults:** nprobe 64 and rerank 32. Recall@10 is 0.966 at 100K; re-sweep at larger scale.
  - **Cold ANN:** 1,021 to 238 R2 requests, 31 to 16 MB, and 2.5 to 1.84 s p50.
  - **Warm p50:** about 75 ms, of which about 70 ms is the strong-consistency HEAD.
  - **Ingest:** 660-830 docs/s with zero 429s.
  - **Compatibility:** new index files need this binary; older binaries can't read them, but this one reads old files.
  - **Checks:** clippy, 160 tests, and compat 121/121 in both modes.
  - **Next:** cold read waves (about 6), lazy BM25 term dictionaries, term stats out of the manifest, and chunked ID indexes.
- **Perf iteration 1 merged (measured on 4.86M docs on R2).**
  - **Cross-segment ANN:** instead of running IVF in every segment, it ranks cluster centers across all
    segments, scans only the globally nearest clusters, and reranks one merged pool. Default nprobe is now
    512, chosen from a recall sweep at 10M.
  - **Block cache:** lock-free and batched, with CRC blocks and exact-size reads.
  - **Decoded-metadata cache:** charges resident size.
  - **Auth:** rare key refresh when auth is disabled.
  - **Ingest:** pipelined builds and multipart uploads, and a skipped catch-up LIST before commits.
  - **Harness:** a reusable `bench/scale/`.
  - **Warm results:**
    - BM25 is 82 ms p50 (was 4.45 s).
    - ANN is about 255-321 ms (was 1.24 s), with recall@10 0.98-0.99.
  - **Cold results:**
    - ANN is 27.7 s with 5,067 R2 requests and 419 MB per query (was 44.6 s and about 30,000 requests).
    - Hybrid is 6,752 requests per query (was about 20,200).
  - **Ingest:** about 600 docs/s.
  - **Checks:** clippy, 158 tests, and compat 121/121 in both modes.
  - **Next:** iteration 2 at 500K, with inner loops at 100K. Targets are cold bytes and requests per query,
    warm ANN CPU, and WAL commit serialization.

## 2026-11-15

- **10M-document R2 baseline merged (iteration 0).** 10M real Wikipedia passages with 1024-dim
  embeddings loaded in 5 h 33 min (500 docs/s end to end), with 1,279 backpressure retries and no
  OOMs. The retained namespace is 56.25 GB, or 5,625 bytes/doc, in 60 segments. ANN recall@10 is
  0.997. Warm ANN p50 is 1.24 s and warm BM25 p50 is 4.45 s at concurrency 1. Cold single queries
  take 45-50 s. Fixes merged with it: bounded segment builds and compaction, compact resident
  vectors, shared BM25 corpus stats, batched column reads, quieter idle indexers, filtered ANN
  skipping unused columns, and a tunable decoded-metadata cache. Raising that cache's budget
  alone took warm BM25 from 4.1 s to 83 ms in a probe. Full numbers are in `docs/benchmarks.md`.
  The query matrix was cut short when Codex ran out of credits. Checked with clippy, 156 tests,
  and compat 121/121 in both modes (new `scripts/compat_both.sh`). Next: at least five
  profile, fix, wipe, and reload iterations.

- **R2 10M-document scale test started, running alone.** It uses real text and embeddings on a
  separate, memory-capped instance under the bucket's `scale/` prefix, and doesn't touch the live
  service. Afterward, harden and then perf2 resume one at a time, with perf2's local benchmarks capped at
  100k docs.

## 2026-11-14

- **Always-on deployment documented and verified.** Added systemd and Caddy guidance covering
  stable release installation, root-only credentials, resource limits, readiness, and redeployment.
  A live S3-compatible deployment passed official Python SDK upsert, ANN, BM25, and filter checks
  through Caddy, including query recovery after a restart. Object placement was checked directly,
  and the test namespace was deleted. Existing upstream private access controls were preserved.
  Verified: release build, formatting, strict Clippy, and the Rust test suite.

- **perf2 partially merged.** These committed pieces are in:
  - Per-query backend diagnostics.
  - Batched, planned BM25 posting reads for cold queries.
  - Warm strong-consistency WAL checks that run alongside the cached index search.
  - A paused-copier crash test.

  Verified: 149 Rust tests, and compat **121/121 in both modes**.
- **⚠️ Workers stopped by low memory.** The VM ran low on memory (likely the 1M×768 benchmark running
  next to a second worker's build). Claude Code stopped the perf2 and harden workers. Their committed
  work is safe; their uncommitted work is still in their worktrees. They are paused until Kyle says to
  resume.
- **Security audit merged.** An independent pass fixed 11 real issues, each with a regression test.
  - **High:**
    - Scoped `keys` managers could inspect and revoke broader keys.
    - Stale key caches failed open.
    - Array grouping could expand exponentially.
    - Highlights could amplify memory.
    - A computed `id` could corrupt rows and panic RRF.
  - **Medium:** compressed-body limits, vector dimension caps, value and document size caps,
    expression depth limits (with a 512-case proptest harness), and fuzzy and highlight work budgets.
  - **Docs:** `docs/security.md` covers the threat model, a route-by-route auth matrix, limits, and
    how to report vulnerabilities.
  - **Secrets:** no credentials or private hostnames anywhere in git history (targeted scan).
  - **Verified on main:** 143 Rust tests pass, and compat was 121/121 in both modes on the audit
    branch.
  - **Open gaps** (being worked next):
    - **High:** aggregate admission control and namespace-handle eviction.
    - **Medium:** async operations don't resume after a crash; `read_only` isn't serialized with
      writes; historical values aren't revalidated when `filterable` is enabled; and branch pins are
      permanent.
- **Final wave launched:**
  - **Audit:** an independent security and correctness audit, fixing what it finds, covering RBAC on
    every route, path traversal into object keys, DoS limits, WAL and GC safety, panics, and secrets.
  - **perf2:** the R2 warm-path latency, batched cold BM25 prefetch, default tuning, and branch-pin
    reclamation.
- **Perf merged (M5).**
  - **Changes:**
    - Segment reads go through the byte cache with known lengths, so no HEAD requests.
    - A decoded-metadata LRU keeps IVF centroids, FSTs, ID indexes, and filter dictionaries in memory
      (512 MiB default).
    - Segments are searched in parallel on a blocking worker pool, and scalar order-by, aggregates,
      and group_by now stream.
    - Small WAL commits (64 KiB or less) are a **single PUT**: GC later erases the inline payload in
      place while keeping the slot fence. The WAL slot format is now v3, so upgrade all readers
      before writers.
  - **Local 1M×768:**

    | | Before | Now |
    |---|---|---|
    | BM25 p50 | 113 ms | **8 ms** |
    | ANN p50, 32 probes (recall 0.99) | | **23 ms** |
    | ANN p50, default 128 probes | 238 ms | **47 ms** |

  - **R2, 100k×768:**
    - Warm ANN p50 is 466 ms and warm BM25 is 190 ms, which still looks too slow. I'm investigating.
    - Cold ANN takes 1.7 s and cold BM25 0.86 s.
    - Small-commit p50 is 184 ms, down from 1,017 ms.
  - **Integration fixes found while merging with m7:**
    - Highlights lost their source text after indexing, because perf's IDs-only fast path didn't know
      about `compute_attributes`. Fixed.
    - A fail-closed test assumed the cache would miss, so it now checks a cold node.
  - **Verified on main:** 129 Rust tests, and compat **121/121 in both modes**. The Python and
    TypeScript SDK smoke tests pass in both modes.
- **M7 merged: compat is now 121/121 in both normal and forced-indexing modes.**
  - **Pattern filters:** fuzzy, regex, glob, and iglob, with trigram indexes. They also work as ranking
    scores.
  - **Vectors:** sparse f16 vectors with indexed `SparseKNN`, native `i8` vectors, and cosine late
    interaction over multi-vectors (exhaustive).
  - **Highlights:** `Highlight` computed attributes with fragment ranking and offsets.
  - **Branching:** `branch_from_namespace`, with same-bucket copies sharing segments copy-on-write.
  - **Plans:** real `explain_query` output.
  - **Hosted models:** clear errors for `Embed` and model reranking.

  I also fixed a security bug found during the merge: a namespace-scoped key could branch an
  out-of-scope source, because only `copy_from_namespace` checked source access. Both now do, with a
  regression test that fails without the fix. Known limit: branch pins are never reclaimed, so shared
  storage from deleted branches is kept indefinitely (documented in the engine README).
- **Multinode merged.** Every engine and cache setting can now be set in `blowfish.toml`, by CLI flag,
  or through a `BLOWFISH_*` environment variable. Metrics recording is installed before the engine
  starts. Tests and docs:
  - **Multi-node:** `scripts/multinode.sh` runs three nodes on one SeaweedFS bucket behind Caddy
    round-robin and passes **111/111**.
  - **Crash test:** a node was killed and restarted during concurrent writes. All 67 acknowledged writes
    survived, and exactly one conditional update won.
  - **CI and docs:** a GitHub Actions workflow and `docs/compatibility.md`.

  Point-in-time restore isn't implemented, and the docs now say so; the retention window is only a GC
  grace period.
- **Wave 4 launched:**
  - **perf:** cache wiring, decoded in-memory segment structures, parallel segment search, streaming
    order-by and aggregates, single-PUT WAL commits, and R2 cold/warm benchmarks.
  - **m7:** fuzzy, regex, and glob with trigram indexes; sparse and i8 vectors; `Highlight`; branching;
    real `explain_query` plans; and multi-vector search.
  - **multinode:** full config exposure, a three-node compat run on SeaweedFS behind a proxy,
    kill/restart tests, a single-writer-mode check, a compatibility matrix doc, and GitHub Actions CI.
- **M2–M4 merged: blowfish is now a real indexed engine.**
  - **Segments:** immutable segments (columnar docs, ID index plus bloom filter, roaring filter
    bitmaps, and live-row generations) published through epoch-fenced manifests.
  - **Indexer:** a background indexer with row, byte, and age thresholds, tiered compaction, GC with a
    retention window, and cold starts from the manifest plus the WAL tail. A node never replays full
    history.
  - **Indexes:** IVF with RaBitQ (`.bfv`) and BM25 postings (`.bft`, with namespace-wide corpus
    statistics) per segment. Write backpressure returns 429. Recall sampling is reported as
    `blowfish_ann_recall`.
  - **Tests:** fault injection at every storage step (acknowledged writes survive exactly once), and
    differential tests comparing indexed and exhaustive results.
  - **Compat:** **111/111 with indexing forced on** (`BF_INDEX_ROWS=1`). 103 Rust tests pass.
  - **First 1M×768 benchmark (local disk):** recall@10 0.99, ANN p50 238 ms, BM25 p50 113 ms, and cold
    open 10 ms. The latency comes from reads that bypass the cache and from sequential segment search,
    which is the next wave.
- **Extras merged. Compat is now 111/111.** That's the original 96 plus 15 new scenarios recorded from
  turbopuffer.
  - **Async operations:** `POST /v2/namespaces/{ns}/async` plus operation polling, with state kept in
    the bucket so any node can answer a poll. Both current SDKs poll transparently.
  - **`PATCH` metadata:** `read_only` is enforced; pinning is accepted and echoed back but does nothing.
  - **Legacy v1 API shim:** columnar writes, nested query rows, cursor export, and `HEAD` metadata. It
    was tested end to end with the old turbopuffer 0.1.37 Python SDK.
- **Ops merged.** Auth, admin, metrics, packaging, and docs are in:
  - **Auth:** RBAC API keys enforced on every route, plus the super key (`BLOWFISH_ADMIN_KEY`) and
    `--require-auth`. Without a super key, blowfish runs in dev mode with a loud warning.
  - **Admin API:** `/_blowfish/v1/keys` and `/_blowfish/v1/status`.
  - **CLI:** `blowfish keys create|list|revoke`.
  - **Metrics:** Prometheus at `/metrics`, gated by key, or open on a separate `--metrics-listen` port.
  - **Docker:** a 63 MB non-root Docker image, and Compose for a local volume or SeaweedFS S3 (which
    passed the CAS probe).
  - **Docs:** quickstart, configuration, auth, metrics, and operations under `docs/`.
- **Cache merged (M5 library).** It has two tiers: memory (moka) and a checksummed disk tier, read in
  1 MiB aligned blocks. Missing blocks are coalesced into ranged GETs, and concurrent fetches of one
  block share a single request. It also supports prefetch, object-store and cache metrics, and
  turbopuffer-style `cache_temperature`. The engine will use it when the index worker's segments land.
- **Verified on main:** compat **96/96 with auth enforced**, using a key minted by `blowfish keys
  create`. `/metrics` returns 401 without a key and works with the super key, and unknown keys get 401.
- **Compat: 96/96 🎉** blowfish M1 passes every scenario against the recorded turbopuffer fixtures,
  and also when diffed live against real turbopuffer. The official Python and TypeScript SDK smoke
  tests pass. The fixes:
  - Numeric constants in score expressions, `Decay` over signed attributes, and `ForEachUnique` null
    grouping.
  - `rows_remaining` omission and nanosecond datetimes.
  - 400 and 415 for malformed requests, and a JSON error for unknown routes.

  Two deliberate differences, both documented in `compat/README.md`:
  - **Exact metadata counts:** turbopuffer reports 0 until it has indexed; blowfish reports exact counts.
  - **`explain_query` on unindexed namespaces:** turbopuffer errors; blowfish returns a plan.
- **M1 merged: blowfish serves the turbopuffer API.**
  `blowfish serve --storage-url file:///path` (or `s3://bucket/prefix`) works today.
  - **Storage:** CAS on object storage, verified on real R2, with single-writer mode as a fallback.
  - **Writes:** a write-ahead log with group commit that stays serializable across nodes (tested with
    two engines racing on one namespace).
  - **Queries:** every write and query feature in DESIGN.md §2, computed by exhaustive scan.
  - **HTTP:** gzip, turbopuffer-shaped errors, and the official Python SDK smoke test passing.
  - **First compat run:** **84/96 passing**. The fix worker is closing the remaining 12.
  - **Known cost:** each commit is two PUTs (a payload, then a small CAS slot record), so that deleting
    a namespace can physically erase its data. This is a perf follow-up.
- **Wave 2 launched:** compat fixes (aiming for 96/96); RBAC wiring, the admin API, the keys CLI,
  Prometheus metrics, Docker, and docs; and the foyer memory+NVMe range cache (M5).
- **`bf-vector` merged.** It provides vector distance math and the IVF index with RaBitQ.
  - **Distances:** SIMD kernels for f32 and f16, covering cosine and squared Euclidean distance.
  - **Index build:** deterministic seeded k-means, with a two-level centroid tree from 262k vectors.
  - **Codes:** RaBitQ 1-bit codes with correction factors and error bounds.
  - **Format:** checksummed immutable `.bfv` blobs, with coalesced range reads.
  - **Search:** filter-aware, with an exact fallback when the filter is selective.
  - **Quality:** recall@10 is **0.973** on 100k GloVe vectors with held-out queries.
  - **Speed:** on 1M×768 vectors, p50 is 5 ms at nprobe 16 and 20 ms at nprobe 128 (warm, in memory).
    The engine integration comes in wave 2.
- **`bf-text` merged.** It covers tokenization, BM25 scoring, and the immutable posting-list segment
  format.
  - **Tokenizer:** `word_v4` is built on turbopuffer's own published tokenizer crate, `alyze`. It
    handles all 18 languages and every documented tokenizer.
  - **Scoring:** BM25 includes `k3` and prefix matches. It is checked against 715 cases recorded
    from real turbopuffer, 712 of which match. English scores agree to float precision, and the
    other scores are within 1%. The three exceptions are documented in `crates/bf-text/README.md`.
  - **Segment format:** an FST dictionary and 128-posting bit-packed blocks searched with block-max
    MAXSCORE, plus phrase positions and checksums.
  - **Speed:** on 1M docs, the index takes 39 bytes per doc and a query takes 3.9 ms at p50 (in
    memory). The engine doesn't use it yet; that's wave 2.
- **Compat suite expanded to 96 scenarios**, passing 96/96 when run live against real turbopuffer itself.
  New coverage:
  - Namespace listing and cursor pagination, recall, cache warming, copy, and type round-trips (f16
    vectors included).
  - Malformed HTTP requests, gzip, and unknown query parameters.
  - ID-based export.
  - A seeded 2,000-vector ANN recall@10 check.
  - A TypeScript SDK smoke test (`compat/smoke-ts/run.sh <base-url>`).

  Two oracle quirks are now captured in the fixtures: successful copies omit `performance`, and
  `explain_query` returns 400 on a fresh, unindexed namespace.
- **`compat/` merged.** This is the compatibility suite: 81 scenarios driving the official turbopuffer
  Python SDK, with golden responses recorded from real turbopuffer. It has three commands: `uv run compat
  record`, `uv run compat check --base-url ...`, and `uv run compat diff-live`. Each scenario uses a
  throwaway namespace that is deleted afterward, and I verified that no oracle namespaces were left
  behind. Known gaps, queued for a follow-up worker:
  - Scenarios for namespace listing, pagination, recall, and copy.
  - Round-tripped values for more types.
  - ANN recall-based comparison.
  - A TypeScript smoke test (Node isn't installed).
- **`bf-auth` merged.** It provides role-based API keys:
  - **Roles:** turbopuffer's `admin`, `write`, `read`, and `list`, plus blowfish's `metrics` and `keys`.
  - **Scopes:** keys can be limited to namespace prefixes, and can expire.
  - **Storage:** SHA-256 hashed secrets with constant-time checks, kept in the bucket under
    `_system/keys/`, and an external super key.
  - **Delegation:** a key can never create a key more powerful than itself.

  Revocation reaches other nodes on the next background refresh. The library is tested (`cargo test
  -p bf-auth`), but the server doesn't use it yet; it gets wired in wave 2.
- **Wave 1 launched.** Five parallel workers, each in its own git worktree and branch:
  - core (M1: types, storage, WAL, exhaustive engine, and the HTTP server)
  - `bf-text` (tokenizers, BM25, and the posting index)
  - `bf-vector` (distances, and IVF with RaBitQ)
  - `bf-auth` (role-based API keys)
  - `compat/` (the official-SDK compatibility suite, with recorded turbopuffer fixtures)
- **Kickoff.** DESIGN.md approved, with Kyle's decisions recorded in §11: MIT, turbopuffer as the
  upgrade path, built-in role-based API keys and metrics, and single-writer mode for stores without
  conditional writes. The Cargo workspace skeleton and crate boundaries are in place.

## Perf iteration 12 (10M): namespace-wide recluster

`Engine::recluster` repartitions every row into 32 spatial slabs (about 200-row leaves) beside the segments (`Manifest::ann_slabs`);
unfiltered ANN answers from them while they cover the live segments. 10M cold, fresh process: p50 2,916 to 1,082 ms and p90 4,358 to
1,592 ms at recall 0.945 (goal met), 880 to 383 requests, 116 to 47 MB. Details and the offline curves in `docs/benchmarks.md`.

## Milestones

| # | Milestone | Status |
|---|---|---|
| M1 | Walking skeleton: WAL, full API, exhaustive search, compat harness | ✅ done, compat 96/96 |
| M2 | Segments, indexer, and GC | ✅ done |
| M3 | Vector index (IVF + RaBitQ) | ✅ done (perf tuning next) |
| M4 | Full-text index | ✅ done (perf tuning next) |
| M5 | Cache and performance | ✅ done locally; R2 warm-path tuning next |
| M6 | Multi-node, auth, metrics, and packaging | in progress |
| M7 | The "later" list | ✅ done (hosted Embed and model rerank intentionally unsupported) |

## Perf iteration 13: cold BM25 at 1M (branch `wt/perfiter13`)

- Cold BM25 over R2 from this VM went from 839 / 1,080 / 1,425 ms (p50 / p90 / p99, fresh process) to 673 / 923 / 1,167 in a same-session A/B,
  with 14.7 MB per query down to 4.3 MB and overlap@10 against exhaustive BM25 at 1.0000 over 1,000 queries. Goal (p50 395 or p90 476) not met from
  this VM; the chain is five hops instead of seven, so in-region it projects to about 260 to 330 ms.
- New: text segment version 4 (records beside postings), manifest text headers, `manifest-latest` copy, window-pruned posting reads, segments
  processed concurrently. Older segments and namespaces keep working. Details and next steps: `docs/benchmarks.md`, "Iteration 13".
- New namespace under `scale/bm25/` (`msmarco-1m`, 1M MS MARCO passages, 9 segments).
- Iteration 14 (memory), partial: jemalloc (`bf-alloc`) with stats and an optional heap profiler; segment rows, build concurrency, unindexed tail, and
  recluster slab size follow the memory limit; `IndexBuilder::build_owned`. 1M recluster peak RSS 8.4 GB to 2.5 GB; 1M ingest fits 4 GiB (3.4 GB), 3M ingest still
  OOMs at 1.5M rows (compaction working set and text statistics). 10M on c7gd.large not validated. Cold BM25 at 1M in-region: 380 / 526 ms fresh, 301 / 369 cache-cold,
  overlap 1.0. Details: `docs/benchmarks.md`, "Iteration 14 (memory)".
- Iteration 15 (memory): 10M loads and reclusters on a c7gd.large with swap off, no OOM: ingest peak 2.26 GB (VmHWM), recluster 1.76 GB (target 3.5 GB). Packed f16 index
  builder (`build_packed`), vectors packed through compaction, streamed text statistics merge, scans bypass the block cache, flat BM25 postings, smaller recluster sample.
  Cold ANN at 48 probes, 10M on the large: 769 / 973 ms fresh, 623 / 740 cache-cold, recall 0.9545. Ingest took 11,568 s (864 docs/s average, decaying with size).
  Details: `docs/benchmarks.md`, "Iteration 15 (memory)".
- Iteration 16 (ingest): ingest stays flat as the namespace grows. 10M on a c7gd.xlarge loads in 2,222 s (4,501 docs/s average, 4,540 over the last 1M against 4,187 to 4,683
  at 1M) instead of 4,704 s (2,126 average, 921 over the last 1M); on the c7gd.large 3,427 s instead of 11,568 s, peak RSS 2.3 GB. Per-segment ID filter (`idfilter.bff`: range plus a
  24-bit Bloom, so existence checks no longer read ID lists), bulk loads flush at three quarters of a larger unindexed limit (memory / 8) and defer compaction to 1,024 segments (each
  row built once), the text statistics merge runs in parallel beside the builds, and the ANN directory layouts are written together. Recluster (2,967 s, 1.2 GB) and cold ANN
  (600 / 751 ms cache-cold, recall 0.956) are unchanged. Projection: about 6 hours for 100M if flat, 10 to 14 hours with the remaining linear terms (statistics object, directories,
  manifest). Details: `docs/benchmarks.md`, "Iteration 16 (ingest)".
- Iteration 17 (100M): cold and warm ANN at 100M, in-region on a c7gd.2xlarge. Cache-cold p50 / p90 at 64 probes (recall@10 0.954) went from 2,677 / 2,953 ms to 1,164 / 1,348,
  fresh process from 3,366 / 4,111 to 1,228 / 1,411, warm from 378 / 425 to 168 / 201 (48 probes: 949 / 1,131 cache-cold, 1,047 / 1,223 fresh, 123.5 / 142.9 warm at recall 0.941).
  Bytes per cold query fell from 327 MB to 163 MB. New: a paged slab directory (a 4 MB header with one one-bit code per cell, then one page per cell of about 24 clusters; `repage` adds it to
  an existing recluster in 39 s), hints that drop the layouts not in use, a manifest read that lists only what follows the fixed-key copy, directory pages and scan reads that start from the
  hint before the manifest arrives, scans that run while a cold query's reads arrive, and namespace-state clones that share the segment list. Cold targets met (1,200 and 1,500 ms); warm
  at 64 probes misses its 150 ms target. 10M does not regress (cache-cold 561 / 683 against 665 / 911, warm 43.2 / 55.8 against 47.4 / 59.0). Details: `docs/benchmarks.md`, "Iteration 17 (100M)".
- Iteration 18 (100M warm and manifest): warm p50 / p90 at 100M and 64 probes (recall@10 0.954) went from 163.0 / 195.3 ms to 99.7 / 162.8 (48 probes: 119.6 / 138.0 to 72.6 / 90.4). Warm
  queries used about two of eight cores: the scan stage's per-slab tasks were polled from one future, and cache-hit reads never yield, so their copies and decodes ran on one thread. Each task now
  runs on the runtime. Also: adaptive jemalloc decay (1 s while writing or building and for 15 s after, 30 s while only querying; no measurable warm gain alone), a cheaper `PlaneQuery`
  construction, and a compact `manifest-latest` copy without per-segment text headers (375 KB instead of 3.4 MB at 100M; written on publish, `examples/readercopy.rs` for older namespaces; older binaries
  read it unchanged). Cold 100M did not move beyond noise (1,201 / 1,391 cache-cold, 1,243 / 1,370 fresh at 64 probes); 10M does not regress (533 / 656 cache-cold, 680 / 907 fresh, 36.6 / 50.8 warm).
  Details: `docs/benchmarks.md`, "Iteration 18 (100M)".

## SDK compatibility: retry and backpressure headers (branch `wt/sdkcompat`)

- Every 429 and 503 now carries `Retry-After` (whole seconds, at least 1) and `retry-after-ms`; `x-should-retry` is `true` on 408, 409, 429, 502, 503, and 504 and `false` on 500.
  For the unindexed-WAL limit the delay is the backlog divided by the indexer's smoothed throughput, clamped to 0.5 to 30 s (1 s before the first publish).
  `ApiError` gained `retry_after_ms` and `should_retry` (builders `with_retry_after_ms`, `with_should_retry`); `HttpError` renders the headers.
- Tests: Rust (`backpressure_429_carries_retry_headers`, `retry_headers_by_status`, `drain` unit test); `compat/src/compat/sdk_retry.py` (4 scenarios, the official Python SDK sync and async
  retrying a 429 and waiting the advertised time) and a TypeScript smoke section, both against a second server with a 1-byte limit that `scripts/compat_both.sh` starts in each mode.
- Audit of every behavior the SDKs rely on: `docs/research/sdk-compat-audit.md`; open items in `SPEC.md` section 2.
- `scale_load.py`: one limiter shared by all in-flight threads, honors `retry-after-ms` / `Retry-After` (also on 503), full-jitter backoff, gradual ramp-up, and reports 429 / 503 counts and
  sample header values in its progress line. The checkpoint format is unchanged (one optional `retries_503` field).
