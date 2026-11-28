# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1", "numpy==2.5.4"]
# ///
"""Cold filtered ANN queries with the Round C39 dump hook (offline kill test for 256-bit stripes).

Usage: c39_dump.py SERVER_BINARY STORAGE_JSON RUN_DIR --filter JSON [--truth-key KEY] [--queries N] [--exact]
Starts a read-only server with BF_C39_DUMP (the temporary hook in ann_dir.rs appends one binary record per
query: every matching candidate's 1,024-bit estimate and bound and its 256-bit estimate, the per-leaf row and
byte counts, and, with --exact, exact distances of the rows the sim may shortlist). The caches are dropped before
every query. rows.jsonl gets the latency, the answer, recall against the oracle's truth (when --truth-key names
one), and the GET bytes of the query by object kind (BF_TRACE_READS), so a run without --exact gives clean byte counts.
"""

import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path

import httpx
import numpy as np

p = argparse.ArgumentParser()
p.add_argument("binary")
p.add_argument("storage", type=Path)
p.add_argument("run", type=Path)
p.add_argument("--filter", type=json.loads, required=True)
p.add_argument("--truth-key")
p.add_argument("--queries", type=int, default=200)
p.add_argument("--offset", type=int, default=0)
p.add_argument("--top-k", type=int, default=10)
p.add_argument("--port", type=int, default=8530)
p.add_argument("--exact", action="store_true")
p.add_argument("--top256", type=int, default=1500)
p.add_argument("--oracle", type=Path, required=True)
p.add_argument("--namespace", default="wikipedia-10m")
a = p.parse_args()
a.run.mkdir(parents=True, exist_ok=True)
dump = a.run / "dump.bin"
dump.unlink(missing_ok=True)
logpath = a.run / "server.log"
log = logpath.open("w")
env = dict(os.environ, RAYON_NUM_THREADS="8", BF_DECODED_CACHE_BYTES=str(512 * 2**20), BF_ANN_NPROBE="64",
           BF_C39_DUMP=str(dump), BF_TRACE_READS="1", BF_TRACE_STAGES="1", BF_C39_TOP256=str(a.top256))
if a.exact:
    env["BF_C39_EXACT"] = "1"
proc = subprocess.Popen([a.binary, str(a.storage), f"127.0.0.1:{a.port}"], env=env, stdout=log, stderr=log)
z = np.load(a.oracle)
vec = z["vector"]
truth = z[a.truth_key] if a.truth_key else None
try:
    c = httpx.Client(base_url=f"http://127.0.0.1:{a.port}", timeout=900)
    for _ in range(600):
        try:
            c.get("/").raise_for_status()
            break
        except httpx.HTTPError:
            time.sleep(0.1)
    time.sleep(1.5)
    out = (a.run / "rows.jsonl").open("w")
    # One untimed warm-up (query 199) so process start costs stay out; its dump record is query 0.
    for i in [199] + list(range(a.offset, a.offset + a.queries)):
        c.post("/_bench/drop_caches").raise_for_status()
        size = logpath.stat().st_size
        body = {"rank_by": ["vector", "ANN", vec[i].astype("f4").tolist()], "top_k": a.top_k,
                "filters": a.filter, "consistency": {"level": "strong"}, "include_attributes": ["id"]}
        t = time.perf_counter()
        r = c.post(f"/v2/namespaces/{a.namespace}/query", json=body)
        r.raise_for_status()
        ms = (time.perf_counter() - t) * 1000
        time.sleep(0.2)
        text = logpath.read_text()[size:]
        by_kind: dict[str, int] = {}
        slab_leaf_bytes = 0
        gets = 0
        for line in text.splitlines():
            if line.startswith("TRACE_READ "):
                _, path, offset, limit, nbytes, end, elapsed = line.split()
                kind = ("rowmap" if path.endswith(".rows-v1.bfr") else "slabs" if "/slabs/" in path else "docs" if "/docs" in path
                        else "routing" if "ann-dir" in path or "manifest" in path else "other")
                by_kind[kind] = by_kind.get(kind, 0) + int(nbytes)
                gets += 1
        ids = [row["id"] for row in r.json()["rows"]]
        rec = {"q": i, "ms": ms, "ids": ids, "gets": gets, "bytes_by_kind": by_kind}
        if truth is not None:
            rec["recall"] = len(set(map(int, ids)) & set(truth[i].tolist())) / a.top_k
        out.write(json.dumps(rec) + "\n")
        out.flush()
        print(i, round(ms), rec.get("recall"), by_kind, flush=True)
finally:
    proc.terminate()
    proc.wait(timeout=30)
    log.close()
