# blowfish build progress 🐡

Newest first. Each entry says what works now and how it was checked.

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
