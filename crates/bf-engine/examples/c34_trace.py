# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1", "numpy==2.5.4"]
# ///
"""Per-read timeline of the rerank wave of cold ANN queries (Round C34).

Usage: c34_trace.py SERVER_BINARY STORAGE_JSON RUN_DIR [--queries N] [--top-k K] [--port P]
Starts the server (read-only storage JSON) with BF_TRACE_READS and BF_TRACE_STAGES, drops the
caches before every query (one untimed warm-up first), and prints for each rerank read: start,
end, size, whether it was hedged, and when the hedge fired, all in ms from the request start,
plus the semaphore wait (TRACE_SLOT) and the sibling statistics the hedge saw. Summaries are
medians over the queries.
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
p.add_argument("--queries", type=int, default=12)
p.add_argument("--top-k", type=int, default=10)
p.add_argument("--port", type=int, default=8500)
p.add_argument("--oracle", type=Path, required=True)
p.add_argument("--namespace", default="wikipedia-10m")
a = p.parse_args()
a.run.mkdir(parents=True, exist_ok=True)
log_path = a.run / "server.log"
env = dict(os.environ, RAYON_NUM_THREADS="8", BF_DECODED_CACHE_BYTES=str(512 * 2**20), BF_ANN_NPROBE="64",
           BF_TRACE_READS="1", BF_TRACE_STAGES="1")
log = log_path.open("w")
proc = subprocess.Popen([a.binary, str(a.storage), f"127.0.0.1:{a.port}"], env=env, stdout=log, stderr=log)
vec = np.load(a.oracle)["vector"]
out = []
try:
    c = httpx.Client(base_url=f"http://127.0.0.1:{a.port}", timeout=300)
    for _ in range(300):
        try:
            c.get("/").raise_for_status()
            break
        except httpx.HTTPError:
            time.sleep(0.1)
    time.sleep(1.5)

    def one(i):
        c.post("/_bench/drop_caches").raise_for_status()
        time.sleep(0.2)
        off = log_path.stat().st_size
        body = {"rank_by": ["vector", "ANN", vec[i].astype("f4").tolist()], "top_k": a.top_k,
                "consistency": {"level": "strong"}, "include_attributes": ["id"]}
        t0 = time.time_ns() // 1000
        t = time.perf_counter()
        c.post(f"/v2/namespaces/{a.namespace}/query", json=body).raise_for_status()
        ms = (time.perf_counter() - t) * 1000
        time.sleep(0.3)
        with log_path.open() as f:
            f.seek(off)
            return t0, ms, f.read()

    one(199)
    for i in range(a.queries):
        t0, ms, text = one(i)
        base = t0 / 1000.0  # request start, epoch ms
        stages = {}
        for m in re.finditer(r"^STAGE_TIME (.+) (\d+)$", text, re.M):
            stages.setdefault(m[1], []).append(int(m[2]) / 1000.0 - base)
        reads = []
        for line in text.splitlines():
            if line.startswith("TRACE_READ "):
                _, path, o, lim, size, end, el = line.split()
                reads.append(dict(path=path, off=int(o), len=int(size), start=int(end) - int(el) - base, end=int(end) - base))
        hedges = {}
        for line in text.splitlines():
            if line.startswith("TRACE_HEDGE "):
                _, path, o, n, end, el, after, tot, done, q, timer = line.split()
                hedges[(path, int(o), int(n))] = dict(
                    start=int(end) - int(el) - base, end=int(end) - base, after=int(after), sib=int(tot), done=int(done),
                    p70_ms=int(q) / 1000, timer=int(timer))
        waits = [int(x) for x in re.findall(r"^TRACE_SLOT wait_us=(\d+)", text, re.M)]
        scan_end = max(stages.get("scan", [0]))
        # Rerank wave: every read that starts after the scan stage ended (the ID hydrate is separate).
        rr = [r for r in reads if r["start"] >= scan_end - 1]
        for r in rr:
            h = hedges.get((r["path"], r["off"], r["len"]))
            r["hedge"] = h
        out.append(dict(q=i, ms=ms, stages={k: [round(x, 1) for x in v] for k, v in stages.items()}, scan_end=scan_end,
                        n_reads=len(reads), rerank=rr, all_hedges=len(hedges), max_wait_ms=max(waits, default=0) / 1000))
        print(json.dumps(dict(q=i, ms=round(ms, 1), scan_end=round(scan_end, 1), rerank_n=len(rr),
                              rerank_first_start=round(min((r["start"] for r in rr), default=0), 1),
                              rerank_last_end=round(max((r["end"] for r in rr), default=0), 1),
                              rerank_lat_ms=sorted(round(r["end"] - r["start"]) for r in rr),
                              hedged=sum(1 for r in rr if r.get("hedge") and r["hedge"]["after"] >= 0),
                              hedges_all=sum(1 for h in hedges.values() if h["after"] >= 0),
                              max_slot_wait_ms=round(max(waits, default=0) / 1000, 1))), flush=True)
    (a.run / "trace.json").write_text(json.dumps(out))
finally:
    proc.terminate()
    proc.wait(timeout=10)
    log.close()
