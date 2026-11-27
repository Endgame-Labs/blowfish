# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1", "numpy==2.5.4"]
# ///
"""Dump every probed leaf and candidate of cold unfiltered ANN queries (Round C35 kill test).

Usage: c35_dump.py SERVER_BINARY STORAGE_JSON RUN_DIR --oracle oracle-10m.npz [--queries N] [--top-k K]
Starts the server (read-only storage JSON) with BF_C35_DUMP, which makes every scan unbounded and
appends one JSON line per query (see `c35_dump` in ann_dir.rs): the probed leaves in plan order, and every
candidate with its RaBitQ estimate, error bound, and exact distance. The caches are dropped before every
query. The engine's own answer (IDs and distances) goes to answers.jsonl for the identity check.
"""

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

import httpx
import numpy as np

p = argparse.ArgumentParser()
p.add_argument("binary")
p.add_argument("storage", type=Path)
p.add_argument("run", type=Path)
p.add_argument("--queries", type=int, default=200)
p.add_argument("--top-k", type=int, default=10)
p.add_argument("--port", type=int, default=8510)
p.add_argument("--oracle", type=Path, required=True)
p.add_argument("--namespace", default="wikipedia-10m")
a = p.parse_args()
a.run.mkdir(parents=True, exist_ok=True)
dump = a.run / "dump.jsonl"
dump.unlink(missing_ok=True)
log = (a.run / "server.log").open("w")
env = dict(os.environ, RAYON_NUM_THREADS="8", BF_DECODED_CACHE_BYTES=str(512 * 2**20), BF_ANN_NPROBE="64",
           BF_C35_DUMP=str(dump))
proc = subprocess.Popen([a.binary, str(a.storage), f"127.0.0.1:{a.port}"], env=env, stdout=log, stderr=log)
vec = np.load(a.oracle)["vector"]
try:
    c = httpx.Client(base_url=f"http://127.0.0.1:{a.port}", timeout=600)
    for _ in range(600):
        try:
            c.get("/").raise_for_status()
            break
        except httpx.HTTPError:
            time.sleep(0.1)
    time.sleep(1.5)
    with (a.run / "answers.jsonl").open("w") as out:
        for i in [199] + list(range(a.queries)):
            c.post("/_bench/drop_caches").raise_for_status()
            body = {"rank_by": ["vector", "ANN", vec[i].astype("f4").tolist()], "top_k": a.top_k,
                    "consistency": {"level": "strong"}, "include_attributes": ["id"]}
            t = time.perf_counter()
            r = c.post(f"/v2/namespaces/{a.namespace}/query", json=body)
            r.raise_for_status()
            if i == 199 and not out.tell():
                pass
            out.write(json.dumps({"q": i, "ms": (time.perf_counter() - t) * 1000, "rows": r.json()["rows"]}) + "\n")
            out.flush()
            print(i, round((time.perf_counter() - t) * 1000), flush=True)
finally:
    proc.terminate()
    proc.wait(timeout=10)
    log.close()
