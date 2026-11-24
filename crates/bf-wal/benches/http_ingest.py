"""Paired HTTP ingest driver. Run with uv and numpy, against a disposable server.

Prepare request bodies once, then load them against each fresh baseline/candidate
store. Timing includes body reads, HTTP writes, and every durable acknowledgment.
Attributes, when supplied, are JSONL objects in the same order as corpus.npy.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from pathlib import Path
import threading
import time


def prepare(args):
    import numpy as np

    vectors = np.load(args.corpus, mmap_mode="r")
    args.bodies.mkdir(parents=True, exist_ok=True)
    attrs = iter(args.attrs.open()) if args.attrs else None
    schema = {"vector": {"type": f"[{vectors.shape[1]}]f16", "ann": True}}
    if attrs:
        schema.update({"title": {"type": "string"}, "text": {"type": "string", "full_text_search": True}})
    for first in range(0, args.rows, args.batch):
        last = min(first + args.batch, args.rows)
        v = np.asarray(vectors[first:last], dtype="<f4")
        cols = {"id": list(range(first, last)), "vector": [base64.b64encode(x.tobytes()).decode() for x in v]}
        if attrs:
            rows = [json.loads(next(attrs)) for _ in range(last - first)]
            cols.update({key: [row[key] for row in rows] for key in ("title", "text")})
        body = {"upsert_columns": cols}
        if first == 0:
            body.update(schema=schema, distance_metric="cosine_distance")
        (args.bodies / f"{first:09}.json").write_text(json.dumps(body, separators=(",", ":")))
    (args.bodies / "shape.json").write_text(json.dumps({"rows": args.rows, "batch": args.batch}))


def load(args):
    local = threading.local()
    shape = json.loads((args.bodies / "shape.json").read_text())
    files = sorted(args.bodies.glob("[0-9]*.json"))
    retries = []

    def write(path):
        body = path.read_bytes()
        if not hasattr(local, "conn"):
            local.conn = http.client.HTTPConnection(args.host, args.port, timeout=600)
        started = time.monotonic()
        while True:
            local.conn.request("POST", f"/v2/namespaces/{args.namespace}", body, {"Content-Type": "application/json"})
            response = local.conn.getresponse()
            data = response.read()
            if response.status == 429:
                retries.append(1)
                time.sleep(float(response.getheader("retry-after-ms", "500")) / 1000)
                continue
            if response.status != 200:
                raise RuntimeError(f"{response.status}: {data[:500]!r}")
            affected = json.loads(data)["rows_upserted"]
            expected = min(shape["batch"], shape["rows"] - int(path.stem))
            assert affected == expected, (affected, expected)
            return (time.monotonic() - started) * 1000

    started = time.monotonic()
    # Establish schema before issuing concurrent requests without one.
    times = [write(files[0])]
    with ThreadPoolExecutor(args.clients) as pool:
        times.extend(pool.map(write, files[1:]))
    seconds = time.monotonic() - started
    times.sort()
    print(json.dumps({"rows": shape["rows"], "seconds": seconds,
                      "acknowledged_rows_per_s": shape["rows"] / seconds,
                      "write_p50_ms": times[(len(times)-1)//2],
                      "write_p90_ms": times[int((len(times)-1)*0.9)],
                      "writes": len(times), "retries": len(retries)}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "load"])
    parser.add_argument("bodies", type=Path)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--attrs", type=Path)
    parser.add_argument("--rows", type=int, default=500_000)
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18100)
    parser.add_argument("--namespace", default="i2")
    parser.add_argument("--clients", type=int, default=4)
    args = parser.parse_args()
    (prepare if args.action == "prepare" else load)(args)
