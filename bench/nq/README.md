# BEIR Natural Questions benchmark

This harness measures blowfish on BEIR Natural Questions (NQ) on one machine: 2,681,468 passages with
precomputed Cohere embed-english-v3 vectors (1,024 dimensions, stored as f16, cosine distance), 3,452 test
queries, and 4,201 qrels. It reproduces the setup of an outside benchmark (Kerry Kurian, "Blowfish on BEIR
Natural Questions", 2026-11-20). Results are in [docs/benchmarks.md](../../docs/benchmarks.md), section
"Iteration 19".

Everything runs with `uv` from this directory. Data and scratch go under the repo's `.scratch/` (git-ignored).

```sh
D=../../.scratch/nq-data; R=../../.scratch/nq-run
mkdir -p $D $R
./download.sh $D                          # about 6 GB from the Hugging Face Hub (dataset CohereLabs/beir-embed-english-v3, subset nq)
uv run python -I prepare.py $D            # corpus.f16, queries, qrels, and the exact top 100 (truth.npy, about 5 minutes)
cargo build --release -p blowfish --bin blowfish -p bf-engine --example recluster   # from the repo root
NQ_CPUS=0-5 ./serve.sh $R/store 18100 &   # the server: 12 GB cgroup, pinned to CPUs 0-5
uv run python -I load.py $D --base-url http://127.0.0.1:18100 --namespace nq
# wait for indexing to go idle, stop the server, then measure the as-loaded layout:
./run.sh $R/store $D loaded-64 BLOWFISH_ANN_NPROBE=64
# recluster (server stopped), then measure again:
echo "{\"url\":\"file://$(realpath $R/store)\"}" > $R/storage.json
../../target/release/examples/recluster $R/storage.json nq vector $R/rc-tmp
./run.sh $R/store $D reclustered-64 BLOWFISH_ANN_NPROBE=64
```

- `serve.sh STORE [PORT]` runs `target/release/blowfish serve` on `file://STORE` in a systemd scope with
  `MemoryMax=12G` (`NQ_MEM`), pinned with `taskset` to `NQ_CPUS` (default `0-5`; empty for no pinning). Server
  settings go in the environment.
- `load.py` writes `id` (the corpus row number), `vector` (`[1024]f16`, cosine), `title`, and `text`
  (`full_text_search`) in batches of 5,000 with four requests in flight. Vectors are sent as base64 f32, as the
  official SDKs send them.
- `run.sh STORE DATA TAG [VAR=value ...] [-- measure options]` starts the server with those settings, runs
  `measure.py` with its clients pinned to CPUs 6 and 7 (`NQ_CLIENT_CPUS`), appends one JSON line to
  `.scratch/nq-run/results.jsonl`, and stops the server.
- `measure.py` has three parts (`--parts`):
  - **quality:** all 3,452 queries at `top_k` 100 (four at a time): recall@10 against the exact top 10, and F1@10,
    nDCG@10, and recall@100 against the qrels (smaller k are prefixes of the top 100);
  - **latency:** one warm-up pass over the first 1,000 queries, then the same 1,000 at concurrency 1 (`top_k` 10),
    client-timed over one keep-alive connection: p50, p95, and p99;
  - **throughput:** the first 2,000 queries (`top_k` 10) with bodies encoded before the clock starts, at 1, 4, and
    16 clients. Each client is a separate process with one keep-alive connection and takes every Nth query. The rate
    is 2,000 over the wall time from a common start to the last response.

Queries send the vector as a JSON number array and ask only for `id` (`include_attributes: ["id"]`).
