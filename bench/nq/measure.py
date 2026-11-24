"""Measure blowfish on BEIR NQ: quality, warm latency, and throughput.

Usage:
  uv run python measure.py DATA_DIR --base-url URL --namespace nq --tag NAME [--cpus 6,7] [--parts quality,latency,throughput]

- quality: all 3,452 test queries at top_k 100 (concurrency 4). Recall@10 against the exact top 10 (`truth.npy`),
  and F1@10, nDCG@10, and recall@100 against the qrels. Smaller k are prefixes of the top 100.
- latency: one warm-up pass over the first 1,000 queries, then the same 1,000 at concurrency 1 (top_k 10),
  timed by the client over a keep-alive HTTP connection: p50, p90, p95, and p99.
- throughput: the first 2,000 queries (top_k 10), request bodies encoded before the clock starts, at 1, 4, and 16
  clients. Each client is a separate process with one keep-alive connection and takes every Nth query; the rate is
  2,000 over the wall time from a common start to the last response.

`--cpus` pins this process and its client processes (the server is pinned separately, with taskset).
Results are printed and appended as one JSON line to `--out` (default DATA_DIR/../nq-results.jsonl).

`--first-pass --parts latency` skips warm-up and adds `latency_ms.first_query`. Use a fresh server with
empty application caches; later queries progressively warm it. This mode rejects quality and throughput
parts so they cannot warm the server first. The runner controls the OS page cache separately.
Throughput aborts and reaps its client processes if a request fails or a worker exits without a result.
"""

import argparse
import http.client
import json
import math
import multiprocessing as mp
import os
import queue
import time
from pathlib import Path
from urllib.parse import urlparse

import numpy as np


# Engine adapters: request path, request body, and result ids. `blowfish` speaks the turbopuffer API;
# `qdrant` the Qdrant REST search API (set QDRANT_EF for `hnsw_ef`).
ENGINE = os.environ.get("NQ_ENGINE", "blowfish")
# Quality queries ask for this many results. Qdrant searches with max(hnsw_ef, limit), so its recall@10 at
# a given hnsw_ef is measured with NQ_QUALITY_K=10 (recall@100 is then not meaningful).
QUALITY_K = int(os.environ.get("NQ_QUALITY_K", "100"))


def body(vec, top_k):
    v = [float(x) for x in vec]
    if ENGINE == "qdrant":
        return json.dumps(
            {"vector": v, "limit": top_k, "params": {"hnsw_ef": int(os.environ.get("QDRANT_EF", "64"))}}
        ).encode()
    return json.dumps({"rank_by": ["vector", "ANN", v], "top_k": top_k, "include_attributes": ["id"]}).encode()


class Client:
    def __init__(self, base, ns):
        u = urlparse(base)
        self.host, self.port = u.hostname, u.port
        self.path = f"/collections/{ns}/points/search" if ENGINE == "qdrant" else f"/v2/namespaces/{ns}/query"
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=600)

    def query(self, b: bytes):
        self.conn.request("POST", self.path, b, {"Content-Type": "application/json"})
        r = self.conn.getresponse()
        data = r.read()
        if r.status != 200:
            raise RuntimeError(f"{r.status}: {data[:300]!r}")
        return data


def ids_of(data: bytes):
    d = json.loads(data)
    return [int(x["id"]) for x in (d["result"] if ENGINE == "qdrant" else d["rows"])]


def pin(cpus):
    if cpus:
        os.sched_setaffinity(0, cpus)


def quality(a, q, truth, qrels):
    from concurrent.futures import ThreadPoolExecutor
    import threading

    local = threading.local()

    def run(i):
        c = getattr(local, "c", None) or Client(a.base_url, a.namespace)
        local.c = c
        return ids_of(c.query(body(q[i], QUALITY_K)))

    with ThreadPoolExecutor(4) as pool:
        got = list(pool.map(run, range(len(q))))
    return score(got, truth, qrels)


def score(got, truth, qrels):
    """Quality of `got` (per query, result ids, best first, top 100)."""
    r10, f1, ndcg, r100 = [], [], [], []
    for i, ids in enumerate(got):
        r10.append(len(set(ids[:10]) & set(truth[i, :10].tolist())) / 10)
        rel = set(qrels.get(str(i), []))
        if not rel:
            continue
        hit10 = len(set(ids[:10]) & rel)
        p, r = hit10 / 10, hit10 / len(rel)
        f1.append(0 if hit10 == 0 else 2 * p * r / (p + r))
        dcg = sum(1 / math.log2(k + 2) for k, d in enumerate(ids[:10]) if d in rel)
        idcg = sum(1 / math.log2(k + 2) for k in range(min(10, len(rel))))
        ndcg.append(dcg / idcg)
        r100.append(len(set(ids[:100]) & rel) / len(rel))
    m = lambda x: round(float(np.mean(x)), 4)
    return {"recall10_exact": m(r10), "f1_10": m(f1), "ndcg10": m(ndcg), "recall100_qrels": m(r100), "queries": len(got)}


def latency(a, q, *, warmup=True):
    c = Client(a.base_url, a.namespace)
    bodies = [body(v, 10) for v in q[:1000]]
    if warmup:
        for b in bodies:
            c.query(b)
    t = []
    for b in bodies:
        s = time.perf_counter()
        c.query(b)
        t.append((time.perf_counter() - s) * 1000)
    p = lambda x: round(float(np.percentile(t, x)), 1)
    result = {"p50": p(50), "p90": p(90), "p95": p(95), "p99": p(99), "mean": round(float(np.mean(t)), 1), "max": round(max(t), 1)}
    if not warmup:
        result["first_query"] = round(t[0], 1)
    return result


def worker(base, ns, bodies, cpus, start, out):
    try:
        pin(cpus)
        c = Client(base, ns)
        c.query(bodies[0])  # open the connection before the clock starts
        while time.time() < start:
            time.sleep(0.0005)
        for b in bodies:
            c.query(b)
        out.put((True, time.time()))
    except Exception as error:
        out.put((False, repr(error)))


def throughput(a, q, clients):
    bodies = [body(v, 10) for v in q[:2000]]
    ctx = mp.get_context("fork")
    out = ctx.Queue()
    start = time.time() + 1.0 + 0.05 * clients
    ps = [
        ctx.Process(target=worker, args=(a.base_url, a.namespace, bodies[k::clients], a.cpus, start, out))
        for k in range(clients)
    ]
    for p in ps:
        p.start()
    ends = []
    try:
        while len(ends) < len(ps):
            try:
                ok, value = out.get(timeout=1)
            except queue.Empty:
                failed = [p.exitcode for p in ps if p.exitcode not in (None, 0)]
                if failed:
                    raise RuntimeError(f"throughput workers exited without results: {failed}")
                continue
            if not ok:
                raise RuntimeError(f"throughput query failed: {value}")
            ends.append(value)
    finally:
        for p in ps:
            if p.is_alive():
                p.terminate()
        for p in ps:
            p.join()
        out.close()
        out.join_thread()
    return round(len(bodies) / (max(ends) - start), 1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", type=Path)
    p.add_argument("--base-url", default="http://127.0.0.1:18100")
    p.add_argument("--namespace", default="nq")
    p.add_argument("--tag", required=True)
    p.add_argument("--cpus", default="")
    p.add_argument("--parts", default="quality,latency,throughput")
    p.add_argument("--clients", default="1,4,16")
    p.add_argument("--out", type=Path)
    p.add_argument(
        "--first-pass", action="store_true",
        help="skip latency warm-up; requires --parts latency and a fresh server with empty application caches",
    )
    a = p.parse_args()
    if a.first_pass and a.parts != "latency":
        p.error("--first-pass requires --parts latency, so quality or throughput cannot warm the server first")
    a.cpus = {int(x) for x in a.cpus.split(",") if x}
    pin(a.cpus)
    q = np.load(a.data / "queries.npy")
    res = {"tag": a.tag, "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    parts = a.parts.split(",")
    if "quality" in parts:
        truth = np.load(a.data / "truth.npy")
        qrels = json.loads((a.data / "qrels.json").read_text())
        res["quality"] = quality(a, q, truth, qrels)
        print(json.dumps(res["quality"]), flush=True)
    if "latency" in parts:
        res["latency_ms"] = latency(a, q, warmup=not a.first_pass)
        if a.first_pass:
            res["cache_state"] = "first-pass-no-warmup"
        print(json.dumps(res["latency_ms"]), flush=True)
    if "throughput" in parts:
        res["qps"] = {}
        for n in [int(x) for x in a.clients.split(",")]:
            res["qps"][str(n)] = throughput(a, q, n)
            print(f"{n} clients: {res['qps'][str(n)]} q/s", flush=True)
    out = a.out or a.data.parent / "nq-results.jsonl"
    with open(out, "a") as f:
        f.write(json.dumps(res) + "\n")
    print(json.dumps(res))


if __name__ == "__main__":
    main()
