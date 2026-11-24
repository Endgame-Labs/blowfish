# Profile-guided release experiment

This is an opt-in measurement recipe, not the workspace's release policy.
The experiment keeps the workspace's thin LTO and four codegen units. It compares
an ordinary release, a mixed-workload profile, and a vector-only profile on the
same frozen store and CPU allocation. Release adoption requires a separate decision.

## Build and train

Install Rust's `llvm-tools-preview` component, and use the same compiler, lockfile,
target, and flags for every arm. `scripts/pgo-build.sh` implements Rust's
instrumentation workflow: build with `-Cprofile-generate`, run the supplied
trainer, merge with the toolchain's `llvm-profdata`, and rebuild with
`-Cprofile-use`. It uses an explicit Cargo target, so build scripts are not
instrumented. See the [rustc PGO documentation](https://doc.rust-lang.org/rustc/profile-guided-optimization.html).

The driver uses `-Cllvm-args=-instrprof-atomic-counter-update-all` so threads do not
lose shared counter updates during eight-core training. This changes training
instrumentation, not the production build, which has no profiling counters. LLVM documents the
accuracy/overhead tradeoff under `-fprofile-update` in the
[Clang user manual](https://clang.llvm.org/docs/UsersManual.html#cmdoption-fprofile-update).

Prepare full NQ with `bench/nq/download.sh`, `prepare.py`, and
`load.py --vectors-only`. Wait for indexing, compaction, and automatic reclustering
to settle, and stop the writer. Keep this store immutable for training and
measurement. `DATA` must contain the public raw corpus parquet files too, for the
10K-row text and filtered regression fixture.

```bash
source ~/.cargo/env
rustup component add llvm-tools-preview
export RUSTFLAGS='-Ctarget-cpu=neoverse-v1'
# Set CC, CXX, and the Cargo target linker to working, identical tools for all arms.
export DATA="$PWD/.scratch/nq-data"
export STORE="$PWD/.scratch/nq-run/store"
export SMALL="$PWD/.scratch/pgo-small"
export TARGET=$(rustc -vV | sed -n 's/^host: //p')
CARGO_TARGET_DIR="$PWD/.scratch/pgo-baseline" \
  cargo build --locked --release --target "$TARGET" -p blowfish --bin blowfish
BASE="$PWD/.scratch/pgo-baseline/$TARGET/release/blowfish"
uv run --project bench/nq python crates/blowfish/benches/pgo_experiment.py fixture \
  --binary "$BASE" --data "$DATA" --store "$STORE" --small-store "$SMALL" \
  --out .scratch/pgo-fixture
scripts/pgo-build.sh .scratch/pgo-mixed -- \
  uv run --project bench/nq python crates/blowfish/benches/pgo_experiment.py train \
  --training mixed --data "$DATA" --store "$STORE" --small-store "$SMALL" \
  --out .scratch/pgo-train-mixed
scripts/pgo-build.sh .scratch/pgo-vector -- \
  uv run --project bench/nq python crates/blowfish/benches/pgo_experiment.py train \
  --training vector --data "$DATA" --store "$STORE" --small-store "$SMALL" \
  --out .scratch/pgo-train-vector
```

Each build directory must be new. The trainer receives `BF_PGO_BINARY` and
`LLVM_PROFILE_FILE`; it must stop every instrumented process gracefully and wait
for exit. The driver refuses an empty profile. It retains the instrumented
binary, merged profile, profile summary, optimized binary, and Cargo output under
the requested scratch directory. The caller must remove these after recording
results. `BF_PGO_TARGET` overrides the default host target; `LLVM_PROFDATA`
overrides the toolchain's merger. `RUSTFLAGS` supplies the common build flags.
Native C dependencies are not instrumented by this Rust-only recipe.

The supplied trainer uses six full vector epochs with sixteen HTTP clients and
eight server cores. The mixed trainer also exercises 1,024 filtered queries,
1,024 BM25 queries, 31 sixteen-row writes after namespace creation, and eight
fresh-process queries through the cold facade. The vector-only arm deliberately
omits those paths to test training-set sensitivity.

## Compare

Create an external JSON file mapping arm names to absolute binary paths. Use
`baseline` and `mixed` for the primary comparison, and `baseline` and `vector`
for sensitivity. Then run:

```bash
uv run --project bench/nq python crates/blowfish/benches/pgo_experiment.py measure \
  --data "$DATA" --store "$STORE" --small-store "$SMALL" \
  --arms .scratch/pgo-arms.json --out .scratch/pgo-throughput \
  --warm-epochs 3 --epochs 8
```

Two arms run ABBA. Three arms run forward, then reverse. The warm vector loop
uses H1's HTTP adapter, JSON bodies, sixteen-client distribution, and request
clock boundaries. Server threads use CPUs 0–7, and clients use CPUs 8–15. Every
measured vector response must be fully cached, tail-free, from the full
2,681,468-row population, and counted by the compiled scorer. Every warm,
filtered, BM25, and cold query must return identical complete ordered rows and
distances across arms. Real write responses must agree after removing timing
fields. Each write arm starts an empty, owned store and writes disjoint IDs.

Cold runs use fresh processes and H1's read-only local S3 facade with 25 ms per
request. They model dependent object-store round trips, not real S3 tails,
throttling, or connection behavior. The regression fixture has 10K real NQ texts,
vectors, and synthetic categories; it does not establish full-corpus BM25 or
filtered performance. Training and measurement use the same public query set.

Run counters separately from throughput:

```bash
uv run --project bench/nq python crates/blowfish/benches/pgo_experiment.py measure \
  --data "$DATA" --store "$STORE" --small-store "$SMALL" \
  --arms .scratch/pgo-arms.json --out .scratch/pgo-pmu \
  --workloads vector --warm-epochs 3 --epochs 4 \
  --perf-events '{cycles:u,r01:u,r23:u}'
```

On Neoverse V1, `r01` counts L1 instruction-cache refills, and `r23` counts frontend
stalled cycles. The harness requires at least 99.9% scheduling coverage for every
event, rejects missing counters, and reports counts per measured query.
CPU-wide counts cover all user work on server CPUs; keep those cores exclusive.
Do not present a profiled throughput number as the unprofiled pass gate.

The requested gate is at least 5% higher warm vector HTTP throughput, identical
results, and no measured BM25, filtered, write, or cold regression over 2%.
Vector-only sensitivity must be reported separately, even if the mixed arm passes.

## Optional BOLT screen

Install a BOLT version that understands the binary's generated instructions.
Build all compared arms with `-Clink-arg=-Wl,--emit-relocs`, or add that option to
the common linker wrapper. Retain symbols, collect CPU-wide `cycles:u` samples
from a warmed PGO server, and use the matching optimized binary:

```bash
perf2bolt -ba -p .scratch/pgo-bolt/perf.data \
  -o .scratch/pgo-bolt/perf.fdata .scratch/pgo-mixed/blowfish
llvm-bolt .scratch/pgo-mixed/blowfish -o .scratch/pgo-bolt/blowfish \
  -data=.scratch/pgo-bolt/perf.fdata -reorder-blocks=ext-tsp \
  -reorder-functions=cdsort -split-functions -split-all-cold -split-eh
```

`-ba` handles cycle samples without branch stacks. This supplies less information
than hardware branch sampling. Follow the [LLVM BOLT guide](https://github.com/llvm/llvm-project/blob/main/bolt/README.md),
and validate the rewritten executable before timing it. An unsupported binary or
an incorrect result rejects that screen; it is not a successful optimization.

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
