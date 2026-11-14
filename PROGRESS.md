# blowfish build progress 🐡

Newest first. Each entry says what works now and how it was checked.

## 2026-11-14

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
| M1 | Walking skeleton: WAL, full API, exhaustive search, compat harness | in progress |
| M2 | Segments, indexer, and GC | not started |
| M3 | Vector index (IVF + RaBitQ) | not started |
| M4 | Full-text index | not started |
| M5 | Cache and performance | not started |
| M6 | Multi-node, auth, metrics, and packaging | not started |
| M7 | The "later" list | not started |
