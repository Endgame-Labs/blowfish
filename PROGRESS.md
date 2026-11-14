# blowfish build progress 🐡

Newest first. Each entry says what works now and how it was checked.

## 2026-11-14

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
