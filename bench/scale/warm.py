# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Warm latency at concurrency 1: one unmeasured warm-up pass over the workload, then N measured queries
(cycling through the workload), one at a time on a keep-alive connection, with strong consistency in the request.

Usage: uv run warm.py WORKLOAD.json BASE_URL NAMESPACE [--queries 1000] [--warmup-passes 1]
Prints one JSON line: n, p50/p90/p99 (ms), mean, and recall@10 when the workload items carry "truth".
"""
import argparse, json, time
import httpx
import numpy as np


def main():
    p = argparse.ArgumentParser()
    p.add_argument("workload"); p.add_argument("base_url"); p.add_argument("namespace")
    p.add_argument("--queries", type=int, default=1000); p.add_argument("--warmup-passes", type=int, default=1)
    a = p.parse_args()
    items = json.load(open(a.workload))
    url = f"{a.base_url}/v2/namespaces/{a.namespace}/query"
    ms, rec = [], []
    with httpx.Client(timeout=600) as c:
        for _ in range(a.warmup_passes):
            for it in items:
                c.post(url, json=it["request"]).raise_for_status()
        for i in range(a.queries):
            it = items[i % len(items)]
            t = time.monotonic()
            r = c.post(url, json=it["request"]); r.raise_for_status()
            ms.append((time.monotonic() - t) * 1000)
            if it.get("truth"):
                got = {int(x["id"]) for x in r.json().get("rows", [])}
                rec.append(len(got & set(it["truth"])) / 10)
    q = np.percentile(ms, [50, 90, 99])
    print(json.dumps({"n": len(ms), "p50": round(q[0], 1), "p90": round(q[1], 1), "p99": round(q[2], 1),
                      "mean": round(float(np.mean(ms)), 1), "recall": round(float(np.mean(rec)), 4) if rec else None}))


if __name__ == "__main__":
    main()
