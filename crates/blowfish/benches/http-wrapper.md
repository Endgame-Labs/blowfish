# H1: warm HTTP wrapper measurements

Status: negative result on the v0.1.17 baseline (`13bce27`). No runtime or build
profile change qualifies for the requested 50% gap reduction and 8% full-NQ
throughput gates. Only measurement instrumentation and these findings are retained.

## Method

One owned c7g.4xlarge, Ubuntu 24.04, Rust 1.100.0, and Clang 19. Server and native
controls use CPUs 0–7; 16 HTTP clients use CPUs 8–15. Release builds use
`-C target-cpu=neoverse-v1`, thin LTO, and four codegen units unless explicitly
identified as the separate fat-LTO experiment. No other workloads run during
measurement. Instruction counts and cycle samples are separate from throughput
runs; profiling itself slows the HTTP path.

The full fixture has 2,681,468 NQ passages, 1,024-dimensional f16 vectors,
32 slabs, and 13,392 leaves. A second compiled fixture contains 10,000 rows,
plus category and label attributes. Both use all 3,452 query vectors, 48 probes,
a rerank factor of 32, top ten, ID-only projection, strong consistency, and a
frozen file store. The HTTP server opens the store read-only. This does **not**
make it the exclusive writer.

Three full warm epochs precede measurement. The unprofiled matrix uses eight
measured epochs per process, then repeats the arms in reverse order. Every
measured response must be compiled, fully cached, tail-free, and identical in
ordered IDs and distances. Per-counter groups fit three PMU counters and have
100% scheduling coverage. Native measurements use the engine's exact compiled
scorer, including its existing validated-vector cache, rather than a different
packed-array representation.

Public data hashes:

| File | SHA-256 |
|---|---|
| `corpus.f16` | `b1f1fb5d2d49dad476e3f0419ae3ea2975f907ddff94e447133e868ba31be185` |
| `queries.npy` | `841a621c983f0299d60873ee976990806f5d01e20116516f7bdc9842f4be2aaf` |
| `truth.npy` | `0989770850ca34cefbabde1d9c5fa38fd539d84c3829199145cd832f39165a52` |

## Initial matched costs

These are warmed counting passes, rather than unprofiled throughput claims.
CPU from the external task-clock counter includes every process thread.

| Per query | 10K native | 10K HTTP | Full NQ native | Full NQ HTTP |
|---|---:|---:|---:|---:|
| User instructions | 3.017M | 3.782M | 21.495M | 22.442M |
| User cycles | 1.075M | 1.674M | 7.516M | 8.374M |
| Process CPU, µs | 419 | 785 | 2,929 | 3,427 |

The full-NQ HTTP path adds 4.4% user instructions, but about 17% process CPU in
these counting passes. Subtracting instruction totals does not explain the
CPU difference. Separate process user/system measurements, without PMU
attachment, are required before attributing that difference to application code.

Selected named-function shares from warmed user-cycle sampling, with zero lost
samples, are below. These are shares of each path's own cycles, not absolute
phase times. Inlined work and shared helpers prevent a complete phase partition.

| Function | 10K native | 10K HTTP | Full NQ native | Full NQ HTTP |
|---|---:|---:|---:|---:|
| `Metric::distance_unchecked` | 26.49% | 17.35% | 2.85% | 2.49% |
| `Tables::block_sums` | 15.42% | 10.37% | 36.54% | 33.04% |
| `PlaneQuery::dot` | 1.05% | 0.75% | 20.79% | 18.94% |
| `FastQuery::reset` | 10.38% | 6.55% | 6.69% | 6.20% |
| `PlaneQuery::reset` | 4.92% | 3.03% | 5.91% | 5.31% |
| `l2_squared_f32` | 2.54% | 1.91% | 6.17% | 5.71% |
| JSON `parse_number` | — | 4.95% | — | 0.97% |
| `indexed::rank_key` | — | 0.66% | — | 0.15% |

## Four proposed costs

1. **Flatten buffering is absent on the numeric-vector request path.**
   `serde_derive` deserializes recognized fields with `MapAccess::next_value`;
   only flattened unknown fields go through `ContentVisitor`. `RankBy` already
   reads numeric vectors with its own visitor. Response flattening serializes
   directly to the output map. A four-repeat microprobe measured request decoding
   at approximately 51.2 µs with flatten and 51.1 µs without it; response encoding
   was approximately 1.126 µs with flatten and 1.136 µs without it. This is not a
   useful optimization, and no parser or serializer changes remain.
2. **The strong-read WAL probe is real, and remains necessary.** There is one
   failed `openat` per query on this backend, rather than a `stat` syscall.
   This fixture's read-only opens use Conditional mode. Read-only opening never
   acquires the single-writer heartbeat, even if a single-writer flag is supplied. A read-only replica can observe an external writer, so skipping its
   probe would violate strong consistency. No such skip is retained.
3. **A single-pass projection prototype helps the small fixture, but fails the
   full gate.** It shares semantic validation, avoids the reference evaluator's
   score-map reconstruction, and projects final ANN winners directly. Its 10K
   paired screen improves throughput about 3.4%, and reduces user instructions
   from 3.775M to 3.716M. The full path actually executes this shortcut, but the
   full matrix is neutral. The prototype is discarded.
4. **Identity codec wrappers have a measurable, small cost.** Removing both
   codec layers entirely saves about 39,000 instructions and 20 µs per 10K query,
   but breaks compressed requests and negotiated compression. A separate
   identity-only bypass keeps the original layers on encoded traffic and copies
   their `Vary` predicate; its extra middleware leaves only about 6,000 instructions
   saved on 10K. Its full-NQ
   gain is below the qualification threshold, so it is discarded. Gzip handling
   remains unchanged.

The additional slab-metadata borrowing prototype saves approximately 36,000
instructions on full NQ, but does not improve throughput, and is discarded.

| First full-NQ matrix | Median q/s | Change from its baseline |
|---|---:|---:|
| Baseline | 2,385 | — |
| Borrowed slab metadata | 2,378 | −0.29% |
| Borrowing plus single-pass projection | 2,388 | +0.11% |

A separate baseline/combined-identity-bypass matrix gives 2,373 → 2,390 q/s,
about +0.71%. These are distinct paired experiments; the percentages are not
added. All ordered results agree.

## Code-layout screens

A further forward/reverse matrix tests two source/build-only candidates. Every
ordered row and distance agrees across its 165,696 responses.

| Layout | Median q/s | Change |
|---|---:|---:|
| Thin LTO, four codegen units | 2,379 | — |
| Cold error outlining | 2,372 | −0.28% |
| Fat LTO, one codegen unit | 2,408 | +1.25% |

Cold outlining marks API-error constructors and error responses cold, and moves
JSON rejection/path-tracking fallbacks out of successful extraction. It does not
improve the full matrix. Fat LTO reduces L1I refills from about 21,105 to 18,361
and frontend stalled cycles from 399,501 to 350,866 in separate per-task counter
passes, but misses the throughput gate. The fat build takes 5m03s on this host.
Neither source annotations nor workspace build settings are changed.

A query-only seven-core control reaches 2,372 q/s with zero misses. This is an
idealized screen of reserving one of eight cores for HTTP: the query side alone
already misses the target, before any dispatch or HTTP cost is added. It rules
out that split for this fixture, not every partition or a future graph workload.

## Stall and syscall attribution

The following user-only PMU events are collected in separate, fully scheduled
passes. Stall events overlap and are not additive.

| Event/query | 10K native | 10K HTTP | Full NQ native | Full NQ HTTP |
|---|---:|---:|---:|---:|
| L1I refills | 577 | 18,850 | 1,649 | 21,040 |
| L1D refills | 13,419 | 20,151 | 74,076 | 85,172 |
| L2D refills | 4,716 | 21,893 | 14,528 | 33,738 |
| Frontend stalled cycles | 18,553 | 269,436 | 52,130 | 410,436 |
| Backend stalled cycles | 410,728 | 565,579 | 3,128,705 | 3,381,495 |
| Branch mispredictions | 1,517 | 6,599 | 3,928 | 9,835 |

This is consistent with instruction-working-set disruption and lower IPC around
HTTP work, rather than a large redundant application-instruction budget. It is
an attribution lead, not a proof that one named function causes the difference.

Dispatch metrics show every measured full-NQ query running inline on its HTTP
reactor: exactly 41,424 additional `path="inline"` dispatches, with no increase
in `ready`, `cold`, or `inline_yielded`. There is no measured query-worker handoff
to remove. The asynchronous WAL HEAD still wakes blocking workers.

The 10K strace window contains 13,808 queries. Per query it records one `writev`,
1.019 `recvfrom`, one failed `openat`, 2.021 `futex`, and 1.086 `epoll_pwait`.
The full-NQ window similarly has one `writev`, 1.019 `recvfrom`, one failed
`openat`, 2.053 `futex`, and 1.254 `epoll_pwait` per query. Each response already
uses one vectored write. Strace timings are deliberately excluded from CPU and
throughput comparisons because tracing perturbs scheduling heavily.

A separate kernel-cycle sample has no lost samples. It includes VFS path lookup,
TCP receive/write processing, futex wakeups, IRQ/spinlock work, and SVE access
handling. Kernel-event samples can skid into userspace; their percentages are not
an exclusive partition of kernel CPU.

## Separate profiler overhead from serving cost

Per-task PMU switching adds substantial system CPU on this virtualized host.
A six-process scope comparison runs `none`, `pid`, `wide`, `wide`, `pid`, and
`none`, with three warm and four measured epochs each. CPU-wide groups count
only CPUs 0–7, with clients and the counter driver on CPUs 8–15. These cores have
no other application workload. CPU values come from process accounting, not
CPU-wide task-clock, which has a different scope.

| Full-NQ counter scope | Median q/s | User CPU µs/query | System CPU µs/query |
|---|---:|---:|---:|
| No PMU attachment | 2,381 | 3,267 | 71 |
| Per-task PMU | 2,257 | 3,292 | 170 |
| CPU-wide PMU | 2,371 | 3,274 | 75 |

On 10K the corresponding system costs are 59 / 168 / 59 µs/query. A CPU-only
run through the maintained HTTP harness independently gives approximately
637 + 58 µs on 10K and 3,259 + 71 µs on full NQ. Its longer warm-up and query
sequence differ from the scope comparison; do not treat it as another paired arm.

The new native process-accounting control measures about 415.4 user + 0.07 system
µs on 10K, and 2,954.4 + 0.12 µs on full NQ, with zero compiled misses. Compared
with the unprofiled HTTP scope control, the full-NQ process CPU gap is about
**384 µs/query**, consistent with the original roughly 0.39-ms target. These are
fresh CPU measurements; the earlier table retains the original counting passes.

The cache lead survives removing per-task PMU switching: CPU-wide full-NQ counts
are about 20,615 L1I refills and 395,640 frontend stalled cycles/query, versus
per-task values 21,144 and 409,046, and native-worker values 1,649 and 52,130.
All event groups have 100% scheduling coverage. CPU-wide counters also include
background user work on the selected cores; native-worker counters cover the
scoring threads. Neither difference is an exclusive function-time attribution.
The earlier per-task system-CPU excess must not be presented as production cost.

## Validation and limits

Formatting, scoped all-target Clippy with warnings denied, and the three scoped
Rust suites pass: 264 tests, zero failures, and eight explicit ignores. The
compiled native control is additionally run explicitly on both fixtures.
Recorded compatibility passes 121/121, compatibility unit tests pass 12/12, and
Python metadata, operations, legacy 0.1.37, and TypeScript smoke checks pass on
the retained baseline runtime. Neutral prototypes are rejected before additional
compatibility qualification; no claim is made that they are shippable changes.

These results concern warm, compiled IVF on one Graviton host, rather than cold
S3, macOS, a graph index, or every HTTP connection distribution. The required
headline performance improvement remains unachieved.

## Reproduction

Prepare NQ with `bench/nq/download.sh`, `prepare.py`, and `load.py --vectors-only`;
wait for automatic reclustering and compaction, and stop the writer. Reuse that
immutable store for every arm. Build the 10K store with
`bf-engine`'s `server_tax_fixture` example and calculate its own exact truth.
Use `warm_ab.py --identical-results --warm-epochs 3 --epochs 8` for unprofiled
forward/reverse matrices.

The native ignored control accepts a `.scratch` JSON file through
`BF_BENCH_COMPILED`, with `store`, `namespace`, `queries` (headerless f32),
`dims`, `probes`, `threads`, and `repeats`. `BF_BENCH_PROFILE_STOP` stops after
warm-up, so a profiler can attach before `SIGCONT`. The control now reports
whole-process `cpu_user_us_per_query` and `cpu_system_us_per_query` on Linux,
including background cache work. These fields are null on other platforms.

`crates/blowfish/benches/server_tax.py --pid PID` reports the same CPU field names
from Linux process accounting. Add `--no-perf` to measure those fields without
PMU attachment. Add `--perf-cpus 0-7` instead to collect CPU-wide events on
exclusive server CPUs while retaining process CPU accounting through `--pid`.
The two counting modes are mutually exclusive. CPU-wide events include every
workload on those CPUs, so keep the client and other work elsewhere. Long query
sequences reduce `/proc`'s clock-tick quantization;
`--query-repeats 4 --repeats 2` is a useful minimum for this fixture. CPU accounting
includes the measurement sequence and result collection; keep that interval
consistent across arms.

For separate user/kernel counters use `cycles:u,instructions:u` and
`cycles:k,instructions:k` in different passes. Raw Neoverse V1 groups are
`cycles:u,r01:u,r03:u` (L1I/L1D), `cycles:u,r17:u,r23:u` (L2D/frontend), and
`cycles:u,r24:u,r10:u` (backend/branch). For native raw groups use
`BF_BENCH_EVENTS=l1i:01,l1d:03` and the analogous pairs. Do not combine all events
into a multiplexed group.
