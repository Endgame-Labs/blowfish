# Warm-graph measurement tools

The server uses one automatic, disposable warm graph implementation. There is no graph feature,
import path, explicit promotion API, or query-budget setting. The bucket's IVF indexes remain the
source of truth. See `docs/design/hot-tier-graph.md` for admission, calibration, and eviction.

`warm_graph_bench EXPORTED_IVF NQ_DATA SCRATCH_DIRECTORY [build_workers=2]` exercises the production
bounded builder. It streams source clusters, subdivides oversized spatial partitions, constructs
two-pass R32/L64 Vamana graphs, stitches boundary rows, streams rotated SQ8, and loads compact links.
After construction it measures all 3,452 NQ queries against exact cosine top-10 truth. Exact rerank
reads local corpus rows; this offline q/s is distinct from HTTP throughput and sparse-cache behavior.
The builder removes its own spill directory on success, cancellation, or error. The caller owns and
removes the parent directory, inputs, exported fixture, and logs.

`hot_graph` retains monolithic HNSW and resident IVF **measurement baselines**. These are never loaded
by the engine. HNSW uses `hnsw_rs = 0.3.5` (MIT OR Apache-2.0), M=16, and construction ef=100.
The rejected packet experiment and duplicate experimental partition builder were removed; their
measurements and reproducible source remain in the G3 commit history.

https://docs.rs/hnsw_rs/0.3.5/

The baseline commands are `build`, `encode8`, `search`, `search8`, and `ivf`. They accept a data
directory, graph or exported-IVF path, corpus row limit, query ef or probes, repeat count, and exact
rerank count. The private baseline encoding is not a stored blowfish index. Query RSS includes the
full f16 corpus. `measure.py` counts only the resident query region, excluding loading, construction,
and encoding; hardware counters and timing are architecture specific.

Prepare public BEIR NQ data with `bench/nq/download.sh` and `prepare.py`, running Python through uv.
All inputs, outputs, temporary directories, and logs belong under the run's `.scratch/`. Build and
measure on an owned EC2 instance, or during a granted target-host slot. Compare the baseline and
candidate on the same host. Never measure while compilation or another workload overlaps.

```sh
source ~/.cargo/env
cargo build --release -p bf-vector --example warm_graph_bench --example hot_graph
uv run --no-project python crates/bf-vector/examples/g3/export_ivf.py LOCAL_STORE .scratch/g4/ivf
nice -n 19 target/release/examples/warm_graph_bench .scratch/g4/ivf .scratch/g4/nq .scratch/g4/build 2
```

`export_ivf.py` opens a local-filesystem fixture read-only. It requires one complete base tier,
integer NQ IDs, and the local filesystem object envelope; tier rows are explicitly mapped through
source segment IDs to corpus IDs.

`prepare_hot_graph_nq storage.json DATA_DIR namespace SCRATCH_DIR` creates a fresh fixture through
ordinary writes, indexing, reclustering, and garbage collection. Its short retention is private to
that disposable fixture. `hot_graph_server storage.json namespace ready_marker port` serves the real
engine on loopback. It starts on IVF, observes ordinary heat, builds locally, and calibrates through
the existing recall machinery. `/measure/{namespace}/status` provides read-only diagnostics. The
server exposes ordinary query and recall routes; it has no graph controls or import endpoints.

`http_measure.py` measures all-query recall at top 10, warms all queries, and times pre-encoded HTTP
requests over persistent connections. On macOS use 16 processes and one connection per process via
`G3_CLIENT_PROCESSES=16 G3_CLIENT_THREADS=1`; clients and servers share the host. On a 32-core Linux
host, the historical protocol pins servers to CPUs 0–23, and eight client processes to CPUs 24–31.
Adapt CPU placement for smaller owned hosts. Linux perf counts only the server. macOS uses the
stable `RUSAGE_INFO_V4` task counters. Run with `uv run --python 3.12 --no-project --with numpy python`.
The ef argument applies only to the Qdrant baseline; blowfish chooses effort through recall monitoring.

`qdrant_load.py` uses `qdrant-client==1.20.0` and the official Qdrant 1.20.0 binary: cosine, resident f16,
M=16, construction ef=100, one target segment, and no payloads. Inspect telemetry to confirm every
row is indexed before measuring. Each caller owns its processes, fixture directories, and cleanup.
Copy retained numbers into `docs/benchmarks.md`, then remove the entire owned run directory.
