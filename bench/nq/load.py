"""Load BEIR NQ into a running blowfish: id, f16 vector (cosine), title, and text (BM25).

Usage: uv run python load.py DATA_DIR --base-url http://127.0.0.1:18100 --namespace nq [--rows N]

Document id i is corpus row i (`corpus_ids.txt` line i). Vectors go over the wire as base64 little-endian f32,
as the official SDKs send them, and are stored as f16 (the schema declares `[1024]f16`). Writes retry on 429 and
transport errors, so the server's write backpressure paces the load.
"""

import argparse
import base64
import http.client
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pyarrow.parquet as pq

DIM = 1024
SCHEMA = {
    "vector": {"type": f"[{DIM}]f16", "ann": {"distance_metric": "cosine_distance"}},
    "title": {"type": "string"},
    "text": {"type": "string", "full_text_search": True},
}

local = threading.local()


def post(base: str, path: str, body: bytes):
    u = urlparse(base)
    delay = 0.25
    while True:
        conn = getattr(local, "conn", None)
        if conn is None:
            conn = local.conn = http.client.HTTPConnection(u.hostname, u.port, timeout=600)
        try:
            conn.request("POST", path, body, {"Content-Type": "application/json"})
            r = conn.getresponse()
            data = r.read()
        except (OSError, http.client.HTTPException):
            conn.close()
            local.conn = None
            time.sleep(delay)
            delay = min(10, delay * 1.5)
            continue
        if r.status == 429 or r.status >= 500:
            time.sleep(delay)
            delay = min(10, delay * 1.5)
            continue
        if r.status != 200:
            raise RuntimeError(f"{r.status}: {data[:500]!r}")
        return json.loads(data)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", type=Path)
    p.add_argument("--base-url", default="http://127.0.0.1:18100")
    p.add_argument("--namespace", default="nq")
    p.add_argument("--rows", type=int, default=0, help="stop after this many rows (0: all)")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--inflight", type=int, default=4)
    a = p.parse_args()
    path = f"/v2/namespaces/{a.namespace}"
    vecs = np.load(a.data / "corpus.f16", mmap_mode="r")
    files = sorted((a.data / "raw" / "corpus").glob("*.parquet"))
    pool = ThreadPoolExecutor(a.inflight)
    pending = []
    row, t0, first = 0, time.monotonic(), True
    for f in files:
        for batch in pq.ParquetFile(f).iter_batches(batch_size=a.batch, columns=["title", "text"]):
            n = len(batch)
            if a.rows:
                n = min(n, a.rows - row)
            if n <= 0:
                break
            v = np.asarray(vecs[row : row + n], dtype="<f4")
            body = {
                "upsert_columns": {
                    "id": list(range(row, row + n)),
                    "vector": [base64.b64encode(x.tobytes()).decode() for x in v],
                    "title": batch.column("title").to_pylist()[:n],
                    "text": batch.column("text").to_pylist()[:n],
                }
            }
            if first:
                body["schema"] = SCHEMA
                body["distance_metric"] = "cosine_distance"
                post(a.base_url, path, json.dumps(body).encode())
                first = False
            else:
                pending.append(pool.submit(post, a.base_url, path, json.dumps(body).encode()))
            row += n
            while len(pending) >= a.inflight * 2:
                pending.pop(0).result()
            if row % 100_000 < a.batch:
                el = time.monotonic() - t0
                print(f"{row} rows {el:.0f}s {row / el:.0f} rows/s", flush=True)
        if a.rows and row >= a.rows:
            break
    for x in pending:
        x.result()
    el = time.monotonic() - t0
    print(json.dumps({"rows": row, "seconds": round(el, 1), "rows_per_s": round(row / el)}), flush=True)


if __name__ == "__main__":
    main()
