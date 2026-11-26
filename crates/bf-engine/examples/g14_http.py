# /// script
# dependencies = ["numpy==2.5.4"]
# ///
"""Round G14: filtered-query quality and throughput over HTTP, switching the server's scan/IVF path.

Usage: g14_http.py WORKLOAD_DIR PORT SWITCH_FILE OUTPUT_JSON CASE [CASE ...]
WORKLOAD_DIR holds CASE.json (64 request bodies) and truth.json (filtered_nq.py). For each case and
each arm (ivf, scan) the script writes the arm to SWITCH_FILE (read by the server), then measures:
recall@10 against truth, 1 client (serial) and 16 clients (8 processes of 2 threads) q/s and
p50/p99 latency. Arms alternate in order ivf, scan, scan, ivf so drift shows. The caller owns cleanup.
"""
import os, concurrent.futures, http.client, json, multiprocessing as mp, sys, time
from pathlib import Path

import numpy as np

ROUTE = "/v2/namespaces/nq/query"


def post(conn, body):
    conn.request("POST", ROUTE, body, {"content-type": "application/json"})
    r = conn.getresponse()
    data = r.read()
    if r.status != 200:
        raise RuntimeError((r.status, data[:300]))
    return data


def worker(args):
    port, bodies, first, step, count, threads = args

    def one(t):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
        lat = []
        for i in range(first * threads + t, count, step * threads):
            s = time.perf_counter()
            post(conn, bodies[i % len(bodies)])
            lat.append(time.perf_counter() - s)
        return lat

    with concurrent.futures.ThreadPoolExecutor(threads) as ex:
        return [x for part in ex.map(one, range(threads)) for x in part]


def load(port, bodies, procs, threads, count):
    start = time.perf_counter()
    with mp.get_context("fork").Pool(procs) as pool:
        parts = pool.map(worker, [(port, bodies, p, procs, count, threads) for p in range(procs)])
    elapsed = time.perf_counter() - start
    lat = np.sort(np.array([x for p in parts for x in p])) * 1000
    pct = lambda q: float(lat[min(len(lat) - 1, int(q * len(lat)))])
    return {"qps": len(lat) / elapsed, "p50_ms": pct(.5), "p99_ms": pct(.99), "queries": len(lat)}


def quality(port, bodies, truth):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
    hit = total = 0
    for body, want in zip(bodies, truth):
        rows = json.loads(post(conn, body))["rows"]
        got = {int(r["id"]) for r in rows}
        hit += len(got & set(want[:10]))
        total += min(10, len(want))
    return hit / total


def main():
    wdir, port, switch, out, *cases = sys.argv[1:]
    port = int(port)
    wdir, switch = Path(wdir), Path(switch)
    truth = json.loads((wdir / "truth.json").read_text())
    result = {}
    for case in cases:
        reqs = json.loads((wdir / f"{case}.json").read_text())
        bodies = [json.dumps(r, separators=(",", ":")).encode() for r in reqs]
        result[case] = []
        for arm in ["ivf", "scan", "scan", "ivf"]:
            switch.write_text("ivf" if arm == "ivf" else f"scan {os.environ.get('G14_SHARE', '20')}")
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
            for b in bodies * 3:  # warm both paths' caches
                post(conn, b)
            row = {"arm": arm, "recall10": quality(port, bodies, truth[case])}
            row["c1"] = load(port, bodies, 1, 1, 600 if arm == "scan" else 200)
            row["c16"] = load(port, bodies, 8, 2, 4000 if arm == "scan" else 800)
            result[case].append(row)
            print(case, json.dumps(row), flush=True)
    Path(out).write_text(json.dumps(result, indent=1))


if __name__ == "__main__":
    main()
