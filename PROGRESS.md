# blowfish build progress 🐡

Newest first. Each entry says what works now and how it was checked.

## 2026-11-16

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
  - `docs/open-source-checklist.md`.

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
