"""Collect turbopuffer cold-query samples by waiting for its cache to go cold between queries.

turbopuffer can't be forced cold, so this script finds how long a namespace must sit idle before
`performance.cache_temperature` reports "cold", then keeps sampling at that idle gap. Each cycle
sends one ANN query, then one hot follow-up. It writes one JSON line per query to --out.

Usage: uv run --with numpy --with httpx python bench/scale/tpuf_cold.py \
         --ns blowfish-oracle-wiki1m --data bench/scale/data/oracle-1m.npz --samples 20 --out cold-1m.jsonl
Auth: TURBOPUFFER_API_KEY is sent as a bearer token when set (a keyless proxy needs neither).
"""
import argparse, json, os, time
from pathlib import Path

import httpx
import numpy as np


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.environ.get("TPUF_URL", "https://api.turbopuffer.com"))
    ap.add_argument("--ns", required=True)
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--samples", type=int, default=20)
    ap.add_argument("--first-gap", type=float, default=300, help="first idle gap in seconds; doubles until a cold read")
    ap.add_argument("--max-gap", type=float, default=6 * 3600)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    d = np.load(a.data)
    key = os.environ.get("TURBOPUFFER_API_KEY")
    client = httpx.Client(timeout=300, headers={"authorization": f"Bearer {key}"} if key else {})
    url = f"{a.url}/v2/namespaces/{a.ns}/query"
    truth = d["ann_ids"]

    def query(i, gap, role):
        body = {"rank_by": ["vector", "ANN", d["vector"][i].astype("f4").tolist()], "top_k": 10}
        t = time.perf_counter()
        r = client.post(url, json=body)
        ms = (time.perf_counter() - t) * 1000
        r.raise_for_status()
        out = r.json()
        perf = out.get("performance", {})
        got = {int(row["id"]) for row in out.get("rows", [])}
        row = {"ts": time.time(), "ns": a.ns, "q": i, "role": role, "idle_s": gap, "client_ms": round(ms, 1),
               "server_total_ms": perf.get("server_total_ms"), "cache_temperature": perf.get("cache_temperature"),
               "recall": len(got & set(int(x) for x in truth[i])) / 10}
        with a.out.open("a") as f:
            f.write(json.dumps(row) + "\n")
        print(json.dumps(row), flush=True)
        return row

    gap, cold, qi = a.first_gap, 0, 0
    query(qi % len(truth), 0, "warmup")
    while cold < a.samples:
        time.sleep(gap)
        qi += 1
        row = query(qi % len(truth), gap, "probe")
        query((qi + 100) % len(truth), 0, "hot")  # a different query right after, to record hot latency
        if row["cache_temperature"] == "cold":
            cold += 1  # keep this gap: it is long enough
        else:
            gap = min(gap * 2, a.max_gap)


if __name__ == "__main__":
    main()
