# Disposable hot-tier graph kill tests

`hot_graph` is an in-process experiment, with no HTTP, bucket writes, production API, or production
format changes. It reads the NumPy corpus, queries, and exact cosine top-100 truth produced by
`bench/nq/prepare.py`. Recall is overlap with the exact top 10, rather than BEIR relevance judgments.

Construction uses `hnsw_rs = 0.3.5` (MIT OR Apache-2.0), cosine distances over the original f16 corpus,
M=16 (up to 32 base-layer links), and construction ef=100. The insertion permutation is fixed;
layer sampling and parallel insertion scheduling are not deterministic. Query tests reuse identical
edges. Links are copied out of the builder into immutable vectors, and query workers reuse byte-generation
visited arrays and heaps. The scratch graph encoding is private to this example and is never read by
blowfish. The dependency's implementation and licensing are documented here:

https://docs.rs/hnsw_rs/0.3.5/

Variants:

- `search`: direct native NumKong f16 dots with precomputed corpus norms and a normalized f16 query.
- `search8`: the same links, scored with eight-bit scalar codes after the production seeded orthogonal
  rotation. Each row and query has its own symmetric quantization scale. Exactly rerank the best 30–80
  reached rows using the original f16 corpus and normalized f32 query.
- `packet-search`: group rows spatially into packets of up to 32 rows inside existing IVF leaves,
  build a graph over normalized packet centers, and navigate by each packet's minimum production
  residual one-bit fast-scan score. Keep the best 80 rows from all visited packets for exact rerank.
  Cache each packet score within a query, so repeated upper/base-layer visits do not scan it twice.
- `ivf`: rank the production tier directory, use its scaled 48/64 probe count, run the production
  resident-code scan with per-slab top-10 bounds, apply the global top-10 upper bound, cap rerank at
  320, and directly score corpus rows. The fixture maps tier ordinals to corpus/document IDs.

The IVF baseline excludes cache calls, hydration, read planning, and HTTP, and gives every query one
CPU thread. All fixtures and input arrays must be copied under the worktree's `.scratch/`. The source
store is opened read-only by `export_ivf.py`; that exporter requires one complete NQ base tier, integer
IDs, and the local filesystem object envelope. It does not modify the source store.

```sh
source ~/.cargo/env
mkdir -p .scratch/g3/tmp
export TMPDIR="$PWD/.scratch/g3/tmp"
# Check the shared QUIET file before every heavy step. Build with low priority and two jobs.
nice -n 19 cargo build --release -j 2 -p bf-vector --example hot_graph
uv run --no-project python crates/bf-vector/examples/g3/export_ivf.py LOCAL_STORE .scratch/g3/ivf
RAYON_NUM_THREADS=2 nice -n 19 taskset -c 6,7 target/release/examples/hot_graph build .scratch/g3/data .scratch/g3/rows.graph
RAYON_NUM_THREADS=2 nice -n 19 taskset -c 6,7 target/release/examples/hot_graph encode8 .scratch/g3/data .scratch/g3/rows.graph.codes8
RAYON_NUM_THREADS=2 nice -n 19 taskset -c 6,7 target/release/examples/hot_graph packet-build .scratch/g3/data .scratch/g3/ivf
uv run --no-project python crates/bf-vector/examples/g3/measure.py .scratch/g3/f16-64 6,7 2 target/release/examples/hot_graph search .scratch/g3/data .scratch/g3/rows.graph 2681468 64 1
```

The common trailing arguments are corpus row limit (default all), ef or probes (default 64 or 48),
repetitions (default 1), and exact rerank count (default 50 for eight-bit and 80 for packets). A row
limit is only for construction smoke tests; NQ recall requires the complete corpus and matching graph.
`packet-build` uses its ef-position argument for packet size (16 or 32); `packet-search` currently
loads the 32-row fixture. Sixteen-row packets pad the SIMD block to 32 rows, just as production does.

`measure.py` starts perf counters disabled and enables them only around the resident query loop.
Instruction counts include normalization, rotation, visited and heap work, scoring, rerank, and recall
accounting. Corpus loading, graph building, code encoding, and fixture parsing are excluded. Run all
3,452 queries for every recall gate. Obtain a quiet window before reporting all-core q/s. Set
`G3_QUIET_GRANTED=1` only for that granted window. Capture build peak RSS with `/usr/bin/time -v`.
Query RSS includes the full f16 corpus, even for compressed graph search; no sparse-rerank-cache memory
claim follows from this harness. Hardware counters and wall time are architecture specific.

Copy retained results into `docs/benchmarks.md`, and remove the entire `.scratch/g3` run directory when
finished, including input copies, the exported IVF fixture, all graph files, all codes, and all logs.
