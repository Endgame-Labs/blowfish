# blowfish spec

This file records the decisions behind blowfish and why they were made. The README is for users, `DESIGN.md` is the
architecture, and `PROGRESS.md` is the build log. This file is for whoever builds blowfish next: read it before
changing direction on anything listed here.

Every section says how settled it is:

- **Decided**: Kyle chose it. Change it only when he says so.
- **Proposed**: built or written down, and not yet confirmed by Kyle.
- **Open**: needs a discussion, a measurement, or a go-ahead before anyone builds it.

The decision log at the end lists changes in date order. When Kyle decides something, update the section and add a
log entry in the same commit.

## 1. What blowfish is

**Decided** (2026-11-14).

- An open-source, self-hostable search engine that is compatible with the turbopuffer API. Official turbopuffer SDKs
  work against it unchanged by setting the base URL.
- It exists so people can build and test against the turbopuffer API without a turbopuffer account. For production
  at serious scale, I recommend turbopuffer itself; because the APIs match, the upgrade is a change of base URL and
  API key.
- Object storage is the only source of truth. Lose every server and disk, keep the bucket, and nothing acknowledged
  is lost. Nodes are caches.
- Any S3-compatible store and nothing else: no Postgres or SQLite fallback. Stores without conditional writes run in
  single-writer mode.
- One static binary and one config file. Scale out by running more copies.
- Role-based API keys, an admin super key, and Prometheus metrics come built in.
- License: MIT. The repo is public at `dorkitude/blowfish`.

## 2. Compatibility

**Decided** (2026-11-14).

- The upstream OpenAPI spec is the contract. Response bodies match turbopuffer's in field names, types, ordering
  semantics, status codes, and error shape.
- BM25 scores only need to be close to turbopuffer's; rankings should match on ordinary corpora.
- ANN matches turbopuffer on recall, not on row order.
- **Full compatibility with the official SDKs** (Kyle, 2026-11-20), including their retry and backpressure behavior:
  429 and 503 responses carry `Retry-After` (and `retry-after-ms`), and `x-should-retry` is set where the SDKs read it.
  Every wire behavior the SDKs rely on (`docs/research/turbopuffer-sdks.md`) is either implemented or listed here as
  Open.
- Backpressure (2026-11-20): every 429 and 503 carries `Retry-After` (whole seconds, at least 1) and `retry-after-ms`.
  For the unindexed-WAL limit the delay is the backlog divided by the indexer's recent throughput, clamped to 0.5 to
  30 s. `x-should-retry` is `true` on 408, 409, 429, 502, 503, and 504, and `false` on 500. The full audit is in
  `docs/research/sdk-compat-audit.md`.
- The compatibility suite (`compat/`) drives the official SDK against blowfish and diffs against recorded turbopuffer
  responses. It must pass in both modes (`scripts/compat_both.sh`) before every merge.
- Checks against the real turbopuffer use a paid account: only tiny namespaces prefixed `blowfish-oracle-`, deleted
  when the comparison is done.

**Open** (SDK compatibility, from `docs/research/sdk-compat-audit.md`).

- `Server-Timing` response header, which the legacy Python SDK (0.1.x and 0.4.x) can parse for cache and timing figures.
- Only the Python and TypeScript SDKs are exercised by tests. Go, Java, Ruby, Rust, and C# share the generated retry,
  error, and header code, which was read but not run.
- A base URL with a path prefix needs a proxy that strips the prefix.

## 3. Code and workflow

**Decided.**

- Rust workspace, edition 2024, one crate per concern (table in `AGENTS.md`). Python tools run with `uv`.
- `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, and compat 121/121 in both modes pass before
  every merge to `main`.
- Commits are authored as the repo owner, with no AI attribution lines.
- Nothing private in tracked files: no hostnames, keys, bucket names, account IDs, IPs, or anything from a `*.local`
  file. Check every diff before committing.
- Versions follow Cargo semver. Kyle asked for "0.1.012"; Cargo rejects leading zeros, so it shipped as `0.1.12`
  (2026-11-17).

## 4. Docs and copy

**Decided.**

- User-facing copy (README, CONTRIBUTING, SECURITY, benchmark write-ups, and release notes) is pragmatic, clear, and
  concise, and doesn't sound machine-written. It is written or final-passed by the Carbon copy model (2026-11-17).
- Always the Oxford comma: "a, b, and c".
- The README shows current numbers. When a benchmark changes, update the README in the same change; don't hold it for
  a later run.

## 5. Indexing

**Decided.**

- **Vectors:** IVF with RaBitQ, with full-precision rerank. Defaults: `ann_nprobe = 64` and `ann_rerank_factor = 32`.
- **Spatial slabs** (round 12): `recluster` partitions a namespace into slabs of small leaves, so a cold query reads a
  few contiguous ranges instead of every segment's clusters. Unfiltered ANN uses slabs only while they cover exactly
  the live segments; `BF_ANN_SLABS=0` turns them off.
- **No row-level graph indexes** (HNSW, DiskANN, and similar over rows). The peak-jumping research ruled them out for a
  cold, object-storage-first engine: their random hops each cost an object-store round trip. A graph over leaf centers,
  held with the paged directory, is allowed (decision 26; `docs/design/graph-index.md`).
- **BM25** (round 13): window-pruned posting reads (exact, not approximate), the term record stored before its
  blocks (text segment format v4), and text headers in the manifest. Result quality is checked as overlap@10 against
  exact BM25 (k1 1.2, b 0.75).
- **Memory** (rounds 14 to 16): jemalloc with background purge; defaults sized from the machine's or cgroup's memory
  limit; index builds from packed f16; compaction and recluster reads bypass the block cache; a small per-segment ID
  filter so existence checks skip full ID lists; bulk loads defer compaction so each row is built once.

**Proposed.**

- Recluster runs by hand (`examples/recluster.rs`). New segments written after a recluster are not covered by slabs
  until the next one. Decided 2026-11-23: recluster becomes automatic and on by default (see Open).
- **Paged slab directory** (iteration 17): once a recluster's directory lists 100,000 or more clusters it also writes a
  paged copy (a small resident header with one one-bit code per cell of about 24 neighboring clusters, then one page per
  cell). A cold query reads the header (in the manifest's wave, from a hint object) and only the pages of the cells it
  probes, and starts the scan reads from the header before the manifest arrives. The flat directory stays for older
  readers. Queries read about 16 times the clusters they probe, which keeps recall equal to the flat directory's.
  Existing reclustered namespaces get a paged copy with `examples/repage.rs`, without rebuilding slabs.
- **Warm path and allocator** (iteration 18): each slab's scan task runs on its own runtime task, so a warm 100M query uses
  every core (warm p50 160 to 100 ms at 64 probes). jemalloc's page decay is adaptive: 1 s while writes, builds, compaction,
  or `recluster` run and for 15 s after, 30 s while only queries run (`bf-alloc`, `BF_BUSY_DECAY_MS`, `BF_IDLE_DECAY_MS`,
  `BF_DECAY_LINGER_MS`).
- **Compact manifest copy** (iteration 18): the fixed-key `manifest-latest` copy omits the per-segment text headers (the
  bulk of a large manifest) and is flagged `root`. Cold ANN queries read it (375 KB against 3.4 MB at 100M); text queries,
  writers, and the indexer read the full manifest slot, which is unchanged, so older binaries and older namespaces work as
  before. `examples/readercopy.rs` rewrites the copy of an existing namespace.

**Open.**

- **Automatic recluster (decided 2026-11-23, not built):** the indexer reclusters a namespace in the background when
  its layout would gain from it (enough independently built segments, or enough rows outside its slabs), within the
  engine's memory budget. It is always on: there is no setting to turn it off and no manual-only mode. A namespace
  never waits on it: queries use the current layout until the new one is published.
- Slabs that cover new segments, and filtered ANN on slabs.
- A split manifest (a small root plus lazily read segment lists) would shrink the cold ANN read further than the compact copy's 375 KB; text-header reads on a namespace with a compact copy cost one more round trip.
- Storage cache sizing reads only the root cgroup's `memory.max`, while the engine reads the process's own cgroup;
  make the storage cache follow the engine. Planned for v0.1.14 (Kyle, 2026-11-20).
- Memory sizing on macOS: there is no `/proc/meminfo`, so memory-sized defaults assume 4 GB. Read `hw.memsize` there,
  in the same shared function as the cgroup fix (v0.1.14).
- BM25: fold term records into dictionary reads, store document lengths in blocks, and a namespace-wide text
  dictionary.
- The text statistics object, ANN directories, and manifest are still rewritten whole on each publish. Fine at 10M;
  may matter at 100M.

## 6. Performance targets

**Decided.**

- Benchmark against turbopuffer's **published** numbers (2026-11-16), and also against turbopuffer measured by me on the
  same data and queries, labelled as mine (2026-11-19). turbopuffer can't be forced cold: record its reported
  `cache_temperature` and the idle time before each sample.
- **Vector, 10M:** within 30% of turbopuffer's published cold p50 or p90, at recall at least theirs, on the smallest
  instance that holds the numbers (2026-11-17; the earlier rule was 40%).
- **BM25, 1M:** within 25% of turbopuffer's published cold p50 or p90, with overlap@10 against exact BM25 at least as
  good (2026-11-17).
- **Memory:** a 10M load and recluster fit on a 4 GB instance (c7gd.large) with swap off, peak RSS at most 3.5 GB,
  without slowing ingest or queries (2026-11-17). Met in round 15.
- **Ingest:** docs/s stays flat as a namespace grows, so 100M is practical (round 16).
- Engineering moves in iterations: profile, diagnose, fix, wipe the data, and measure again. When hill-climbing stops
  paying off, test bigger hypotheses on a branch, even ones that change the index layout or the storage scheme
  (2026-11-17).

## 7. How benchmarks are measured and published

**Decided.**

- Tables show p50 and p90 in milliseconds. No p99, no columns that can't be filled, and no internal "round" labels
  (2026-11-16).
- **Cold** is measured two ways and labelled: a fresh process with empty caches, and cache-cold with the process
  running (caches dropped, connections kept warm; turbopuffer's definition).
- **Recall@10** is measured against exact top-10 truth on the same corpus and queries, computed by brute force. Only
  measured settings are published; never interpolate recall between probe counts (2026-11-17).
- A number that doesn't reproduce on a rerun is dropped, not published (2026-11-17).
- Publish in-region numbers (benchmark host in the same cloud region as the bucket) first, then over-the-internet
  numbers, each labelled.
- Benchmarks that write to a bucket use their own prefix and delete it when done.
- Never run a writable server whose bucket prefix holds benchmark namespaces it doesn't own: a writable server indexes,
  compacts, and garbage-collects every namespace under its prefix.

**Proposed.**

- Amazon S3 Vectors comparison (2026-11-18): at 10M, S3 Vectors had recall@10 0.894 against blowfish's 0.953; warm
  p50 / p90 60 / 278 ms against 70.7 / 84.1 ms; and 178 / 276 ms after 30 minutes idle. That idle number is not
  comparable to blowfish's cold numbers, since AWS's caches can't be dropped; don't publish it as like-for-like.
  Harness in `bench/s3vectors/`.

## 8. Test infrastructure and cost

**Decided.**

- In-region benchmarks run on AWS Graviton instances with local NVMe (the c7gd family), tagged for the project and
  terminated when the run ends. Use the smallest instance that holds the numbers (2026-11-17).
- An instance may stay up while tests are running, and not longer (2026-11-17).
- Ask Kyle before anything above about $5/hour or running longer than a day.
- Coding agents working on blowfish stay under 80% of Kyle's weekly Claude Code limit, and stop with margin
  (2026-11-17; raised from 50%).

## 9. 100M

**Decided** (2026-11-17): test at 100M, in-region and over the internet, after the memory and ingest work.
Kyle approved the cost and duration on 2026-11-19: about a day of instance time on a c7gd.2xlarge, and about 1 TB in
S3 until the run is cleaned up.

**Proposed.**

- Data: the first 10M English rows as today, then rows in other languages from the same embedding model, with exact
  top-10 truth computed on the instance.
- turbopuffer publishes nothing at 100M, so I measure it myself: the same 100M rows loaded into turbopuffer
  (Kyle approved on 2026-11-19; the 1M-row check showed turbopuffer bills f16 vectors at f32 size, about $900 in
  writes for 100M, and Kyle approved that too). turbopuffer can't be guaranteed cold, so its samples are labelled by
  its reported `cache_temperature` and idle time.
- The 100M blowfish namespace stays in the bucket after round 17 until Kyle says to delete it (2026-11-19).

## Decision log

1. 2026-11-14: Design approved. MIT license; "compatible with the turbopuffer API" positioning with turbopuffer as the
   production upgrade path; BM25 scores close enough; storage only, no database fallback; built-in auth and metrics;
   build every milestone.
2. 2026-11-14: Real turbopuffer checks only in `blowfish-oracle-` namespaces, deleted when done.
3. 2026-11-15: Watch agent quota on long jobs and never drive any plan to its limit.
4. 2026-11-16: Benchmark tables compare only to turbopuffer's published numbers, as p50 and p90 in milliseconds, with
   no unfillable columns or round labels.
5. 2026-11-16: Speed target: within 40% of turbopuffer's published 10M cold numbers.
6. 2026-11-17: Target tightened to within 30% for vector and 25% for BM25, on the smallest instance that holds the
   numbers. Peak-jumping branches allowed. In-region testing on AWS.
7. 2026-11-17: Recall is measured against exact truth on the same queries; only measured probe counts are published.
8. 2026-11-17: Version 0.1.12 published with the in-region benchmarks.
9. 2026-11-17: User-facing copy goes through the Carbon copy model.
10. 2026-11-17: Memory target: 10M load and recluster within about 4 GB. Then 100M, in-region and over the internet.
11. 2026-11-17: Agent quota ceiling raised from 50% to 80% of the weekly limit.
12. 2026-11-17: Add an Amazon S3 Vectors head-to-head; run it at 10M first and report back before 100M.
13. 2026-11-19: `SPEC.md` started; kept current with every decision.
14. 2026-11-19: 100M run approved.
15. 2026-11-19: Measure turbopuffer directly, cold and warm, including the same 100M rows. Keep the 100M blowfish
    namespace in the bucket after round 17.
16. 2026-11-19: turbopuffer 100M load approved at about $900 (it bills f16 vectors as f32).
17. 2026-11-19: Iteration 18 built (status Proposed): slab scan tasks on the runtime, adaptive allocator decay, compact manifest reader copy.
18. 2026-11-20: Full compatibility with the official SDKs, including retry and backpressure headers.
19. 2026-11-20: Release v0.1.13. Delete the historical open-source checklist. `GET /` keeps returning
    `{"status":"🐡"}`. Unify cgroup memory detection in v0.1.14, not v0.1.13.
20. 2026-11-20: Support Docker on Apple Silicon and native macOS builds; CI builds the image on x86_64 and arm64 and
    tests natively on macOS (issue #1).
21. 2026-11-23: Recluster runs automatically in the background, always on ("if we have a better algo it should be
    the default").
22. 2026-11-23: Release v0.1.14 once the index upgrade path (`blowfish reindex`) and iteration 21 are merged and CI is
    green. After that, the coordinator cuts releases when it judges a release worthwhile ("the code is yours, release as
    appropriate").
23. 2026-11-23: Rerun the 10M and 100M in-region AWS benchmarks with the fast-scan layout, at most about $40 in total,
    terminating instances when done.
24. 2026-11-23: Keep improving every area in rotation: disk footprint (vectors stored twice), BM25 and hybrid speed,
    a graph index to compete with Qdrant (design doc first), and ingest throughput.
25. 2026-11-23: One way to do each thing. When an approach is better, it becomes the only one: no settings that select
    an inferior algorithm or layout, and no code kept to maintain one ("we don't need to keep around or maintain inferior
    approaches. it makes things too complex"). Older on-disk formats are read only so they can be upgraded, and the
    upgrade happens automatically; there is no supported path back to an older format.
26. 2026-11-23: Work toward Qdrant's throughput with a graph over leaf centers, not over rows, after cutting fixed
    per-query cost (`docs/design/graph-index.md`). Before any change to the manifest or bucket format, ship a format
    fence so older binaries can't rewrite or garbage-collect an upgraded bucket (`docs/design/simplify.md`).
