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
