# Contributing to blowfish

## Prerequisites

- **Rust:** current stable Rust toolchain with `rustfmt` and `clippy` (workspace uses Rust 2024 edition).
- **Python:** [`uv`](https://docs.astral.sh/uv/) for running the compatibility suite (`compat/`) and benchmark scripts. Always invoke Python tools via `uv`, never bare `pip`.
- **Go 1.22+:** required only when working on the optional admin web console in [`webapp/`](webapp/README.md).
- **`cmake`, `clang`, and `libclang`:** required **only** when building with `--features local-models` or `--features local-models-native` (in-process `llama.cpp` GGUF inference). Default `cargo build` and `cargo test` do not compile `llama.cpp` and need no C/C++ toolchain beyond standard Rust linking. On Debian or Ubuntu: `sudo apt-get install cmake clang libclang-dev`.
- **Node.js and `npm`:** optional, used only by the TypeScript SDK smoke test (`compat/smoke-ts/run.sh`).

## Build

Build the main binary (HTTP model providers enabled; no `llama.cpp` build):

```bash
cargo build -p blowfish
cargo build --release -p blowfish
```

Build with in-process GGUF model support ([`docs/models.md`](docs/models.md)):

```bash
# Portable x86-64 binary with AVX2 kernels:
CFLAGS="-march=x86-64-v3" CXXFLAGS="-march=x86-64-v3" \
  cargo build --release -p blowfish --features local-models

# Or tuned for the build host's own CPU:
cargo build --release -p blowfish --features local-models-native
```

Build the admin web console:

```bash
cd webapp && go build -o webapp .
```

## Test and lint

Every change must pass formatting, Clippy with warnings denied, and the workspace test suite before commit:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

If you touch `crates/bf-models` or feature-gated `local-models` code, also run the `local-models` checks:

```bash
cargo clippy -p bf-models -p blowfish --features bf-models/local-models,blowfish/local-models --all-targets -- -D warnings
cargo test -p bf-models --features local-models
```

Run the compatibility suite against a local release build in both normal mode and forced-indexing mode (121 recorded scenarios using the official `turbopuffer` Python SDK, plus Python and TypeScript SDK retry scenarios against a low-limit backpressure server; requires `uv`):

```bash
scripts/compat_both.sh
```

Run the native embedding and reranking compatibility scenarios against the deterministic fake provider (no model weights needed):

```bash
scripts/compat_models.sh
```

If you touch `webapp/`, run the Go checks:

```bash
cd webapp && go vet ./... && go test ./...
```

## Workspace layout

The Rust workspace has one crate per concern under `crates/`:

| Crate | Owns |
| --- | --- |
| `bf-types` | The data model and the turbopuffer wire types: IDs, values, attribute types, schema, documents, write/query request and response serde, filter and `rank_by` ASTs, and the API error type |
| `bf-storage` | The object-store layer: `object_store` wrapper, conditional puts, the startup CAS probe, single-writer mode, and the range-read cache |
| `bf-wal` | WAL entry format, group commit, tail reading, and the manifest |
| `bf-engine` | Namespace state, query execution (exhaustive and indexed), segments, the indexer, compaction, and GC |
| `bf-text` | Tokenizers, BM25, and the posting-list index format. No dependency on other `bf` crates |
| `bf-vector` | Distance kernels, IVF with RaBitQ, and its on-disk format. No dependency on other `bf` crates |
| `bf-models` | The `[models]` registry, HTTP embedding and rerank provider clients, and optional in-process `llama.cpp` GGUF inference (`local-models`) |
| `bf-alloc` | Global jemalloc allocator, adaptive busy/idle page decay, `blowfish_alloc_bytes` gauges, and optional `heap-profile` dumps |
| `bf-auth` | API keys, projects, roles, and the key store |
| `blowfish` | The binary: HTTP server (`axum`), admin API, metrics, CLI, and config |

Other top-level directories:

- `compat/` — Python compatibility suite (`uv`) and TypeScript SDK smoke test (`compat/smoke-ts/`), diffing blowfish responses against recorded turbopuffer fixtures.
- `webapp/` — Optional Go admin web console (standard library only, magic-link sign-in).
- `bench/scale/` — Scale and parity benchmark harness ([`docs/benchmarks/README.md`](docs/benchmarks/README.md)).
- `docs/` — User, operator, design, and API research documentation ([`DESIGN.md`](DESIGN.md) at the root holds the system architecture).

Keep crate boundaries clean: `bf-text` and `bf-vector` must not depend on other `bf-*` crates.

## Compatibility is the contract

Wire compatibility with the turbopuffer API and official SDKs is blowfish's primary contract:

- Any change to request parsing, query semantics, ranking, filtering, schema validation, status codes, retry headers (`Retry-After`, `retry-after-ms`, and `x-should-retry`; see [`docs/research/sdk-compat-audit.md`](docs/research/sdk-compat-audit.md)), or response shapes must include a compatibility scenario in `compat/` (in addition to crate-level unit or integration tests).
- Features that blowfish intentionally does not implement (such as cross-deployment copy or customer-managed encryption keys) must return a clear `400` error using the `{"status":"error","error":"..."}` envelope rather than silently ignoring fields.

## Commit, hygiene, and style guidelines

- **Small, focused commits:** make atomic commits with clear messages stating what changed and why.
- **No secrets or private details:** never commit API keys, tokens, credentials, bucket names, account IDs, internal hostnames, customer data, or anything from a git-ignored `*.local` file. Inspect `git diff --staged` before every commit.
- **No clutter outside `.scratch/`:** put any temporary data, server logs, caches, or benchmark scratch directories under `.scratch/` at the repository root (git-ignored), never in `/tmp` or `$HOME`, and remove them when your run finishes.
- **Documentation style:** use the Oxford comma ("a, b, and c") across all prose and comments. Keep writing direct, concrete, and free of marketing filler, emoji, and exclamation marks.

## Security issues

Do not report security vulnerabilities in public GitHub issues or pull requests. See [`SECURITY.md`](SECURITY.md) for private disclosure instructions.
