# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Warm ANN latency against a turbopuffer namespace at concurrency 1: one unmeasured pass over the oracle
queries, then N measured queries cycling through them, one at a time on a keep-alive connection.
Reports p50 / p90 / p99 of client time and of the reported server_total_ms, recall@10 against the oracle truth,
and the cache temperatures seen.

Usage: TPUF_URL=... uv run bench/scale/tpuf_warm.py --ns blowfish-oracle-wiki100m --data bench/scale/data/oracle-100m.npz
"""
import argparse
import collections
import json
import os
import time
from pathlib import Path

import httpx
import numpy as np


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.environ.get("TPUF_URL", "https://api.turbopuffer.com"))
    ap.add_argument("--ns", required=True)
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--queries", type=int, default=1000)
    ap.add_argument("--warmup-passes", type=int, default=1)
    a = ap.parse_args()
    d = np.load(a.data)
    n = len(d["vector"])
    key = os.environ.get("TURBOPUFFER_API_KEY")
    url = f"{a.url}/v2/namespaces/{a.ns}/query"
    bodies = [{"rank_by": ["vector", "ANN", d["vector"][i].astype("f4").tolist()], "top_k": 10,
               "consistency": {"level": "strong"}} for i in range(n)]
    client = httpx.Client(timeout=300, headers={"authorization": f"Bearer {key}"} if key else {})
    for _ in range(a.warmup_passes):
        for b in bodies:
            client.post(url, json=b).raise_for_status()
    cl, sv, rec, temps = [], [], [], collections.Counter()
    for i in range(a.queries):
        t = time.perf_counter()
        r = client.post(url, json=bodies[i % n])
        cl.append((time.perf_counter() - t) * 1000)
        r.raise_for_status()
        j = r.json()
        p = j.get("performance", {})
        sv.append(p.get("server_total_ms"))
        temps[p.get("cache_temperature")] += 1
        rec.append(len({int(x["id"]) for x in j["rows"][:10]} & {int(x) for x in d["ann_ids"][i % n]}) / 10)
    pc = lambda xs, q: round(float(np.percentile(xs, q)), 1)
    print(json.dumps({"n": a.queries, "client_ms": {q: pc(cl, q) for q in (50, 90, 99)},
                      "server_total_ms": {q: pc(sv, q) for q in (50, 90, 99)},
                      "recall_at_10": round(float(np.mean(rec)), 4), "cache_temperature": dict(temps)}))


if __name__ == "__main__":
    main()
