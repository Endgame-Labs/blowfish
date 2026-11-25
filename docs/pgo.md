# Profile-guided release builds

Docker and source release builds use `scripts/pgo-build.sh`. Each build trains its own profile on
its own architecture, compiler, source, features, and native flags. The Docker builder pins Rust
1.100.0 and its multi-architecture image digest. No profile or downloaded training corpus is shipped
in the repository. Thin LTO and four codegen units remain unchanged. Plain Cargo release builds
remain useful for development and as a non-PGO fallback.

## Build a release

Install Rust's `llvm-tools-preview` component, plus the ordinary native build requirements. The
shell driver uses Bash and GNU coreutils (`realpath -m`, `sha256sum`, and NUL-delimited sorting).
Run the script from any directory, with a new output directory beneath the checkout's `.scratch/`:

```bash
source ~/.cargo/env
rustup component add llvm-tools-preview
scripts/pgo-build.sh .scratch/release
# Copy the binary outside the build directory before cleaning it.
install -m 755 .scratch/release/blowfish ./blowfish
rm -rf .scratch/release
```

`docker build -t blowfish .` runs the same script with `local-models` enabled. It preserves the
existing x86-64-v3 C/C++ flags and ARM Clang/SME exclusions. Training runs natively for each
architecture; a cross-compiled instrumented server must be executable on the build host. Prefer
native x86_64 and aarch64 builders rather than training under emulation. The runtime image contains
only the optimized server, its ordinary libraries, and certificates: no trainer, corpus, or profile.

The non-PGO fallback stays unchanged:

```bash
cargo build --locked --release -p blowfish
```

Do not add `profile-generate` or `profile-use` to workspace Cargo settings. Release PGO has one
maintained trainer; the external-trainer interface exists for paired qualification experiments,
rather than a second supported release recipe.

## What training covers

`crates/blowfish/examples/pgo_train.rs` generates a deterministic corpus without network access:
16,384 clustered 1,024-dimensional cosine-f16 vectors, a separate 10,000-row BM25 and filter corpus,
eight category values, and disjoint write IDs. It builds real persisted segment indexes and a
reclustered IVF tier through the engine's public API. Fixture-builder and HTTP-client counters are
excluded from the merged profile.

The trainer starts the production server on ephemeral loopback ports, with eight Tokio threads and
eight Rayon threads. Sixteen clients send 20,712 vector queries, 1,024 filtered vector queries,
1,024 BM25 queries, and 256 sixteen-row writes. Only the first write sends schema, matching ordinary
namespace creation followed by ingestion. Eight fresh server processes read the generated store
through a read-only loopback S3 facade, exercising startup, range requests, XML metadata, signing,
and cache-cold decoding. The facade uses dummy credentials and never contacts a vendor service.
Training fails if indexed queries miss rows, writes are not acknowledged, or the compiled IVF scorer
is never reached. Servers must exit successfully after SIGTERM so LLVM flushes all counters. The
synthetic fixture and logs are deleted on success or failure. The caller owns the overall build
directory and removes it after copying the binary and any evidence it needs.

LLVM counters use atomic updates during training, as qualified in P1. An explicit Cargo target
prevents build scripts from being instrumented. Native C/C++ dependencies are built normally; this
workflow profiles Rust code. The compiler's own `llvm-profdata` merges the profiles. The merged
profile's content hash is part of the compiler flags, preventing a shared Cargo cache from reusing
an optimized build with a different profile at the same path. Every build generates a fresh profile,
so there is no committed profile to go stale or refresh at release time.

The build directory retains the instrumented binary, merged profile, profile summary, optimized
binary, Cargo logs, training duration, toolchain version, lockfile hash, Rust-source hashes, and
output hashes. These support auditing a build; deterministic input data does not promise
byte-identical profiles from a multithreaded workload. CPU scheduling, the compiler, and native
toolchains can still affect the result.

Optional environment variables:

| Variable | Purpose |
|---|---|
| `BF_PGO_FEATURES` | Cargo feature list, identical in both build stages |
| `RUSTFLAGS` | Common Rust flags, identical in both build stages |
| `BF_PGO_TARGET` | Explicit target triple; defaults to the compiler host |
| `LLVM_PROFDATA` | Matching merger; defaults to the Rust toolchain's copy |
| `CARGO_TARGET_DIR` | Shared compilation cache; defaults inside the run directory |

## Release checklist

- Run formatting, scoped Clippy, tests, and the SDK compatibility checks.
- Build and smoke-test native x86_64 and aarch64 Docker images. Existing CI does
  this on both architectures; a successful image build also completes training.
- Check training completed, the merged profile is nonempty, and LLVM did not
  report a profile hash mismatch or discard a hot function's profile.
- After compiler, trainer, or significant query changes, repeat the paired
  qualification below before claiming the retained performance gains.
- Copy the optimized binary or publish the image, record its source and toolchain,
  and remove the owned build directory. Never publish the instrumented binary.

## Qualification protocol

The retained P1 harness is `crates/blowfish/benches/pgo_experiment.py`. Use the same frozen full NQ
and 10K-row regression stores, the same host, flags, features, and compiler for the plain,
full-NQ-profile, and compact-profile arms. Train the full profile by passing its trainer after `--`
to `scripts/pgo-build.sh`; that command receives `BF_PGO_BINARY` and `LLVM_PROFILE_FILE`, and must
stop and wait for every server. An empty profile is an error.

```bash
scripts/pgo-build.sh .scratch/reference -- \
  uv run --project bench/nq python crates/blowfish/benches/pgo_experiment.py train \
  --training mixed --data "$DATA" --store "$STORE" --small-store "$SMALL" \
  --out .scratch/reference-training
```

To rebuild qualification inputs, use the public download, preparation, and load steps in
`bench/nq/README.md`, then `pgo_experiment.py fixture` to create the 10K-row regression store. Keep
both stores immutable after indexing settles. Release training needs neither of these stores.

Create a JSON mapping from arm names to absolute binary paths, then run each paired comparison in
ABBA order:

```bash
uv run --project bench/nq python crates/blowfish/benches/pgo_experiment.py measure \
  --data "$DATA" --store "$STORE" --small-store "$SMALL" \
  --arms .scratch/arms.json --out .scratch/measurement \
  --warm-epochs 3 --epochs 8
```

Use a fresh output directory for each comparison. Server CPUs are 0–7, and client CPUs are 8–15.
Builds and uploads must finish before timing. Every measured vector response must be cache-hot,
tail-free, from the full 2,681,468-row population, and counted by the compiled scorer. Compare
complete ordered rows and distances for warm, filtered, BM25, and cold queries; compare write
responses after removing timing fields. The compact trainer must retain at least 80% of the
contemporaneous full-NQ profile's gain in each of warm vectors, filtered vectors, BM25, and writes.
Compare both profiles on the same current source: P1 predates durable local filesystem writes, so
its historical write rate and percentage are not the current write reference. Its training cost must
stay below about ten minutes for training during the build to remain the release path. See the Round
P2 entry in `benchmarks.md` for measurements and limitations.

The cold facade inserts 25 ms per object-store request; it does not reproduce real S3 tails. The 10K
text/filter fixture cannot establish full-corpus BM25 or filter performance. Training and
measurement use P1's public query set for the full-profile reference; the compact trainer uses only
generated inputs. Qualification exercises compiled IVF, before automatic warm graph promotion. Graph
throughput, full-corpus BM25, and model inference performance are not established by it.

## P1 measurement setup

The measured source is `c50bc93`, whose main parent is v0.1.19 (`480d6c8`),
including H1's retained instrumentation. One owned c7g.4xlarge runs Ubuntu 24.04,
Rust 1.100.0 (LLVM 23.1.5), Clang 19 for native dependencies, and a Clang 22 linker
with its ARM builtins runtime. Every binary uses `target-cpu=neoverse-v1`, the
unchanged workspace release profile, and retained ELF relocations. The linker
wrapper used on this Ubuntu ARM host is:

```bash
mkdir -p .scratch/pgo-tools
cat > .scratch/pgo-tools/linker <<'SH'
#!/usr/bin/env bash
exec clang-22 -Wl,--emit-relocs "$@" \
  "$(clang-22 --print-runtime-dir)/libclang_rt.builtins-aarch64.a"
SH
chmod +x .scratch/pgo-tools/linker
export CC=clang-19 CXX=clang++-19
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="$PWD/.scratch/pgo-tools/linker"
```

NQ is rebuilt from public data, then frozen for every arm. Its 2,681,468 rows
occupy 32 slabs; all three public input hashes match H1's recorded hashes. This
is a new checkpoint, not H1's deleted store. Queries use 48 probes, rerank factor
32, top ten, ID projection, and default strong consistency. Authentication is
disabled only on the benchmark's loopback listener. Server CPUs 0–7 and client
CPUs 8–15 are exclusive during measurements; builds, uploads, and validation
jobs finish before timing starts.

Each unprofiled ABBA comparison has three complete warm epochs and eight measured
epochs per process, with two processes per arm. Counter passes use three warm
and four measured epochs per process in a separate ABBA comparison. No throughput
gate uses a profiled rate. Query comparisons check complete ordered rows and
distances, and every full-NQ measured response passes cache, tail, population,
and compiled-query checks. Warm medians pool the sixteen measured epochs per arm.

The 10K filtered and BM25 fixtures have real NQ texts and vectors, plus synthetic
categories. Each measured epoch has 1,024 queries. Write trials time 255 disjoint
sixteen-row requests after namespace creation, with a new store per trial; these
are serial batch rates, not maximum ingestion throughput. Cold trials have twenty
fresh processes per arm in each direction, through the fixed-delay facade. The
latency column reports the median of the p50 values from each direction.
These limited fixtures cannot establish performance for every filter, corpus,
write pattern, or real S3 tail distribution.

## P1 mixed-profile result

Mixed PGO with atomic training counters clears the requested gates on this host:
**2,339.21 → 2,464.43 warm full-NQ HTTP queries/sec (+5.35%)**, with identical
ordered results and no measured regression in the other four fixtures. The
margin over the throughput threshold is small; this is a measured screen on the
fixtures above, not a release-policy change.

| Workload | Ordinary release | Mixed PGO, atomic training | Change |
|---|---:|---:|---:|
| Warm full-NQ HTTP, queries/sec | 2,339.21 | 2,464.43 | +5.35% |
| Filtered 10K, queries/sec | 2,098.34 | 4,311.12 | +105.45% |
| BM25 10K, queries/sec | 21,824.93 | 25,306.07 | +15.95% |
| Serial 16-row writes, requests/sec | 88.76 | 92.30 | +3.99% |
| Simulated cold, p50 ms | 267.00 | 262.11 | −1.83% |

The original mixed profile used LLVM's default non-atomic updates. It reaches
2,346.30 → 2,444.54 queries/sec (+4.19%), and misses the throughput gate. LLVM
partially ignores one inlined fast-scan closure's profile in that build. Repeating
the identical training mix with atomic counter updates removes that warning.
The atomic driver above is the retained recipe. No profile hash-mismatch warning
occurs, and native C dependencies remain outside this instrumentation.

A BOLT 22 screen on top of the original mixed PGO build gives
2,453.30 → 2,434.89 queries/sec (−0.75%), and is rejected. BM25 also falls 5.40%
relative to that paired PGO control; filtered throughput is +0.24%, writes are
−0.48%, and simulated-cold p50 is +1.76%. Every ordered result agrees. This
screen uses 231,475 CPU-wide user-cycle samples over 69,040 warm queries,
without branch stacks. BOLT reports twelve profiled functions it cannot optimize
and recommends substantially more samples. Its separate counter pass also has
worse L1 instruction-cache and frontend-stall counts. This rejects the tested
sample profile and layout, not every possible BOLT configuration.

The qualifying mixed build's separate CPU-wide ABBA pass gives:

| User event per full-NQ query | Ordinary release | Mixed PGO, atomic training | Change |
|---|---:|---:|---:|
| Cycles | 8,479,001 | 8,063,594 | −4.90% |
| L1 instruction-cache refills | 20,778 | 14,504 | −30.20% |
| Frontend stalled cycles | 402,000 | 273,159 | −32.05% |

Every event has 100% scheduling coverage. Process user CPU is approximately
3,317 → 3,150 µs/query in this counting pass; system CPU is about 75 µs in both
arms. The reduced instruction-cache and frontend counts support H1's layout
lead. They do not identify exclusive function costs, and overlapping stall
events must not be added together.

## Training-set sensitivity

With atomic counters, a profile containing only the six vector-training epochs
gives **2,347.67 → 2,475.63 queries/sec (+5.45%)**. Filtered queries improve
56.58%, BM25 improves 4.91%, and simulated-cold p50 falls 4.24%. Serial write
throughput falls **88.91 → 87.65 requests/sec (−1.42%)** in the main matrix.
This is within the 2% guardrail, but it is a real tradeoff to report, rather than
a claim that untrained paths cannot slow down. The mixed profile improves writes
and is the retained recipe.

This vector-only control merges only the raw profile from the vector server of
the atomic mixed trainer. Separate `%m-%p` files isolate that server from the
filtered, BM25, write, and cold server processes. Both profiles therefore contain
the exact same vector-training execution; their difference is the additional
workloads, not a separately sampled vector run. Both optimized builds have no
partial-profile or hash-mismatch warning.

The initial non-atomic vector-only screen gives +5.03% warm throughput, but
−2.11% write throughput, and fails the guardrail. Its other paths improve.
Consequently, neither initial non-atomic arm qualifies: mixed misses the warm
throughput gate, and vector-only misses the write gate. Atomic training counters
are part of the tested recipe; do not substitute those initial profiles.

A dedicated write confirmation repeats eight ABBA blocks, with sixteen fresh
stores per arm and 8,160 timed batch requests in total. Medians are
89.38 → 88.12 requests/sec (−1.41%). The mean paired log-ratio corresponds to
−1.37%, with a block-bootstrap 95% interval of −1.61% to −1.10% (20,000 resamples,
seed 27). Thus vector-only training has a small, repeatable write penalty, and
that penalty stays inside the 2% gate on this fixture. Mixed training avoids it.

The mixed profile's simulated-cold p90 also improves, 295.90 → 291.03 ms
(−1.65%), using the same per-direction aggregation as p50. Across the five
unprofiled comparison pairs and their separate counter passes, all 828,480
measured full-NQ query responses pass ordered-row/distance equality and warm-tier
admission. Results on other CPUs, real S3, full-corpus BM25, selective full-corpus
filters, held-out queries, and different compiler versions remain unmeasured.
