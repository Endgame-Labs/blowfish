"""Same-machine LanceDB baseline on BEIR NQ, in-process (as in the outside benchmark).

Usage:
  uv run --group others python -I lance.py build DATA_DIR DB_DIR
  uv run --group others python -I lance.py measure DATA_DIR DB_DIR --probes 20,40 --refine 10 [--out FILE]

build writes id and float16 vector (no text) and an IVF-PQ index: 1,600 partitions, 128 sub-vectors, cosine.
measure runs, per (probes, refine): quality (3,452 queries at top 100, or --quality-k, four threads), warm latency (one warm-up pass
over the first 1,000 queries, then the same 1,000 one at a time, top 10), and throughput (the first 2,000 queries,
top 10, at 1, 4, and 16 threads in this process; LanceDB's search releases the GIL). Timings include the Python
call and the Arrow result, which is what an in-process user pays; there is no HTTP hop. Run it pinned (taskset) and
memory-capped like the servers.
"""

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import lancedb
import numpy as np
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))
from measure import score  # noqa: E402

DIM = 1024


def build(data: Path, db: Path):
    vecs = np.load(data / "corpus.f16", mmap_mode="r")
    conn = lancedb.connect(db)
    schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float16(), DIM))])

    def batches():
        for s in range(0, len(vecs), 100_000):
            v = np.ascontiguousarray(vecs[s : s + 100_000])
            arr = pa.FixedSizeListArray.from_arrays(pa.array(v.reshape(-1), pa.float16()), DIM)
            yield pa.record_batch([pa.array(np.arange(s, s + len(v), dtype=np.int64)), arr], schema=schema)

    t0 = time.monotonic()
    tbl = conn.create_table("nq", data=pa.RecordBatchReader.from_batches(schema, batches()), mode="overwrite")
    t1 = time.monotonic()
    print(f"wrote {tbl.count_rows()} rows in {t1 - t0:.0f}s", flush=True)
    tbl.create_index(
        metric="cosine", num_partitions=1600, num_sub_vectors=128, vector_column_name="vector", index_type="IVF_PQ"
    )
    print(f"indexed in {time.monotonic() - t1:.0f}s", flush=True)


def measure(a):
    tbl = lancedb.connect(a.db).open_table("nq")
    q = np.load(a.data / "queries.npy")
    truth = np.load(a.data / "truth.npy")
    qrels = json.loads((a.data / "qrels.json").read_text())
    for probes in [int(x) for x in a.probes.split(",")]:
        for refine in [int(x) for x in a.refine.split(",")]:

            def search(v, k):
                t = (
                    tbl.search(v, vector_column_name="vector")
                    .distance_type("cosine")
                    .nprobes(probes)
                    .refine_factor(refine)
                    .limit(k)
                    .select(["id"])
                    .to_arrow()
                )
                return t.column("id").to_pylist()

            res = {"tag": f"lancedb-p{probes}-r{refine}", "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
            with ThreadPoolExecutor(4) as pool:
                got = list(pool.map(lambda v: search(v, a.quality_k), q))
            res["quality"] = score(got, truth, qrels)
            res["quality"]["k"] = a.quality_k
            if a.quality_only:
                print(json.dumps(res), flush=True)
                if a.out:
                    with open(a.out, "a") as f:
                        f.write(json.dumps(res) + "\n")
                continue
            for v in q[:1000]:
                search(v, 10)
            t = []
            for v in q[:1000]:
                s = time.perf_counter()
                search(v, 10)
                t.append((time.perf_counter() - s) * 1000)
            p = lambda x: round(float(np.percentile(t, x)), 1)
            res["latency_ms"] = {"p50": p(50), "p95": p(95), "p99": p(99), "mean": round(float(np.mean(t)), 1)}
            res["qps"] = {}
            for n in (1, 4, 16):
                work = [q[:2000][k::n] for k in range(n)]
                barrier = threading.Barrier(n + 1)

                def run(part):
                    barrier.wait()
                    for v in part:
                        search(v, 10)

                threads = [threading.Thread(target=run, args=(w,)) for w in work]
                for th in threads:
                    th.start()
                barrier.wait()
                s = time.perf_counter()
                for th in threads:
                    th.join()
                res["qps"][str(n)] = round(2000 / (time.perf_counter() - s), 1)
            print(json.dumps(res), flush=True)
            if a.out:
                with open(a.out, "a") as f:
                    f.write(json.dumps(res) + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["build", "measure"])
    p.add_argument("data", type=Path)
    p.add_argument("db", type=Path)
    p.add_argument("--probes", default="20,40,80,160")
    p.add_argument("--refine", default="10,20")
    p.add_argument("--out", type=Path)
    # The refine factor multiplies the limit, so recall@10 at a given refine is measured at limit 10.
    p.add_argument("--quality-k", type=int, default=100)
    p.add_argument("--quality-only", action="store_true")
    a = p.parse_args()
    if a.cmd == "build":
        build(a.data, a.db)
    else:
        measure(a)


if __name__ == "__main__":
    main()
