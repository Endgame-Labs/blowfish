# blowfish build progress 🐡

Newest first. Each entry says what works now and how it was checked.

## 2026-11-14

- **Kickoff.** DESIGN.md approved, with Kyle's decisions recorded in §11: MIT, turbopuffer as the
  upgrade path, built-in role-based API keys and metrics, and single-writer mode for stores without
  conditional writes. The Cargo workspace skeleton and crate boundaries are in place.

## Milestones

| # | Milestone | Status |
|---|---|---|
| M1 | Walking skeleton: WAL, full API, exhaustive search, compat harness | not started |
| M2 | Segments, indexer, and GC | not started |
| M3 | Vector index (IVF + RaBitQ) | not started |
| M4 | Full-text index | not started |
| M5 | Cache and performance | not started |
| M6 | Multi-node, auth, metrics, and packaging | not started |
| M7 | The "later" list | not started |
