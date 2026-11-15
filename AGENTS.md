# AGENTS.md

Guidance for coding agents (Claude Code, Codex, Pi, ...) working in this repo.

## What this is

blowfish is an open-source, self-hostable service that is **compatible with the turbopuffer API** and works
the same way: object storage is the source of truth. You bring an S3-compatible bucket, and blowfish does the
rest. Existing turbopuffer clients and SDKs should work against it unchanged. It will be published as open
source, so nothing private (hostnames, keys, customer data, or anything from a `*.local` file) goes in tracked files.

## Read AGENTS.md.local first, every session

**Always read `AGENTS.md.local` (next to this file) before doing anything else.** It holds this machine's
private details: the VM, its integrations, storage buckets, and credentials, and how to use each one. It is
ignored by git (`.gitignore` ignores `*.local`), so it never leaves the machine. If it is missing, say so
instead of guessing.

## Rules

- **No clutter on the machine.** Put every scratch file, log, benchmark data directory, and test output under
  `.scratch/` at the repo root (git-ignored; a worktree uses its own `.scratch/`), never in `/tmp` or the home
  directory. Give each run its own subdirectory, and **delete it when the run ends**, pass or fail (use a
  `trap` or a finally block). Copy the numbers worth keeping into `PROGRESS.md` or a doc first. Benchmarks
  that write to a bucket delete their prefix when done, too. Before you finish a task, check that `.scratch/`
  holds nothing you made.

- Never commit secrets or print their values. Private details go in a `*.local` file, never in tracked files.
- Before every commit, check the diff for secrets, tokens, internal hostnames, and anything from a `*.local` file.
- Use `uv` for anything Python.
- Commit your own changes as you go, and push them. Leave other sessions' uncommitted work alone.
- Use the Oxford comma in everything you write here: "a, b, and c".

## Layout and conventions

The design is `DESIGN.md`; the build log is `PROGRESS.md` (update it with every commit that changes
what works). Research notes and the upstream API contract are in `docs/research/`.

Rust workspace, one crate per concern (`crates/`):

| Crate | Owns |
|---|---|
| `bf-types` | The data model and the turbopuffer wire types: IDs, values, attribute types, schema, documents, write/query request and response serde, filter and rank_by ASTs, and the API error type |
| `bf-storage` | The object-store layer: `object_store` wrapper, conditional puts, the startup CAS probe, single-writer mode, and the range-read cache |
| `bf-wal` | WAL entry format, group commit, tail reading, and the manifest |
| `bf-engine` | Namespace state, query execution (exhaustive and indexed), segments, the indexer, compaction, and GC |
| `bf-text` | Tokenizers, BM25, and the posting-list index format. No dependency on other bf crates |
| `bf-vector` | Distance kernels, IVF with RaBitQ, and its on-disk format. No dependency on other bf crates |
| `bf-auth` | API keys, roles, and the key store |
| `blowfish` | The binary: HTTP server (axum), admin API, metrics, CLI, and config |

`compat/` is the Python compatibility suite (run with `uv`), which drives the official turbopuffer SDK
against blowfish and diffs against recorded turbopuffer responses.

Rules for every change:

- `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` pass before each commit.
- Small commits with clear messages, authored as the repo's git user, with no AI attribution lines.
- Stay inside the crate or directory you were asked to work on. If you need a change elsewhere, say so
  in your final report rather than making it.
- The real turbopuffer (when a session has access to one) is a paid account: use only tiny namespaces
  prefixed `blowfish-oracle-`, and delete them when done.
