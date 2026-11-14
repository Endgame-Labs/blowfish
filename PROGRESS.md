# blowfish build progress 🐡

Newest first. Each entry says what works now and how it was checked.

## 2026-11-14

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
| M2 | Segments, indexer, and GC | in progress |
| M3 | Vector index (IVF + RaBitQ) | library done; engine integration in progress |
| M4 | Full-text index | library done; engine integration in progress |
| M5 | Cache and performance | in progress |
| M6 | Multi-node, auth, metrics, and packaging | in progress |
| M7 | The "later" list | in progress (async ops, PATCH metadata, legacy v1 done) |
