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
cargo build --release -p blowfish --bin blowfish   # from the repo root
NQ_CPUS=0-5 ./serve.sh $R/store 18100 &   # the server: 12 GB cgroup, pinned to CPUs 0-5
uv run python -I load.py $D --base-url http://127.0.0.1:18100 --namespace nq
# leave the server running: it indexes, then reclusters on its own. Wait until
# blowfish_recluster_builds_total{outcome="installed"} counts the last build and indexing is idle, stop it, then:
./run.sh $R/store $D reclustered-64 BLOWFISH_ANN_NPROBE=64
# writes after the recluster: rewrite the last 5% of rows (load.py --skip), let indexing settle, and measure again
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
    client-timed over one keep-alive connection: p50, p90, p95, and p99;
  - **throughput:** the first 2,000 queries (`top_k` 10) with bodies encoded before the clock starts, at 1, 4, and
    16 clients. Each client is a separate process with one keep-alive connection and takes every Nth query. The rate
    is 2,000 over the wall time from a common start to the last response.

Queries send the vector as a JSON number array and ask only for `id` (`include_attributes: ["id"]`).

## Same-machine baselines: Qdrant and LanceDB

The same data and measurements, against Qdrant 1.20.0 (HTTP) and LanceDB 0.41 (in-process). Their clients are in
the `others` dependency group.

```sh
# Qdrant: m 16, ef_construct 100, float16 vectors in RAM, optimized to one indexed segment
docker run -d --name bf-nq-qdrant --cpuset-cpus 0-5 --memory 12g --memory-swap 12g -p 127.0.0.1:18110:6333 \
  -v $(realpath $R)/qdrant:/qdrant/storage qdrant/qdrant:v1.20.0
uv run --group others python -I qdrant.py $D --url http://127.0.0.1:18110     # upload, then wait until indexed
NQ_ENGINE=qdrant QDRANT_EF=64 taskset -c 6,7 uv run python -I measure.py $D --base-url http://127.0.0.1:18110 \
  --namespace nq --tag qdrant-ef64 --cpus 6,7
NQ_QUALITY_K=10 NQ_ENGINE=qdrant QDRANT_EF=64 ... --parts quality              # recall@10 at limit 10 (see below)
docker rm -f bf-nq-qdrant; docker rmi qdrant/qdrant:v1.20.0; sudo rm -rf $R/qdrant

# LanceDB: IVF-PQ, 1,600 partitions, 128 sub-vectors, in this process (no HTTP), pinned and capped like a server
systemd-run --user --scope -p MemoryMax=12G -p MemorySwapMax=0 taskset -c 0-5 \
  uv run --group others python -I lance.py build $D $R/lance
systemd-run --user --scope -p MemoryMax=12G -p MemorySwapMax=0 taskset -c 0-5 \
  uv run --group others python -I lance.py measure $D $R/lance --probes 80,120,160 --refine 10,20 --out $R/results.jsonl
... lance.py measure ... --quality-k 10 --quality-only                       # recall@10 at limit 10
rm -rf $R/lance
```

Qdrant searches with `max(hnsw_ef, limit)` candidates, and LanceDB reranks `refine_factor × limit`, so a quality
pass at `top_k` 100 overstates their recall@10 at a top-10 query's settings. Their recall@10 is measured at limit 10
(`NQ_QUALITY_K=10`, `--quality-k 10`); blowfish's probe count does not depend on `top_k`. LanceDB runs in the
measuring process, so its client threads share CPUs 0-5 with the engine, and its numbers carry no HTTP or JSON cost.

## On a Mac (vectors only)

For a run on another machine, copy only `corpus.f16`, `queries.npy`, `truth.npy`, and `qrels.json`. Then load
blowfish with `load.py --vectors-only`, which writes id and vector only, as Qdrant and LanceDB get.

- **blowfish:** a native release build without the `local-models` feature. Run `blowfish serve` directly, since
  `serve.sh` uses systemd. Cap the cache with `BLOWFISH_CACHE_MEMORY_BYTES`.
- **Qdrant:** the official macOS arm64 release binary (`qdrant-aarch64-apple-darwin.tar.gz`), with
  `QDRANT__STORAGE__STORAGE_PATH`, `QDRANT__SERVICE__HTTP_PORT`, and the same `qdrant.py`.
- **LanceDB:** `lance.py` via `uv`.

macOS has no `taskset`, so run with `--cpus ""`: the server and the clients share every core. Measure peak RSS with
`/usr/bin/time -l`. Run one engine at a time, and delete its data before starting the next.
