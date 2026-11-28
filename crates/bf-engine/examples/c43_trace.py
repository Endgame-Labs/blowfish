#!/usr/bin/env python3
"""Round C43 step 1: per-read arrival times of the first cold BM25 posting batch.
usage: c43_trace.py TRACE_BINARY STORAGE_JSON OUT.jsonl FIRST N PORT
Needs a binary built with c43_trace.patch. One fresh process, one throwaway query, then N cold queries
(POST /_bench/drop_caches before each). Writes, per query, the latency, the wall-clock window in microseconds,
and the C43 trace lines the server printed inside it."""
import http.client, json, os, subprocess, sys, time
binary, cfg, out, first, n, port = sys.argv[1:7]
first, n, port = int(first), int(n), int(port)
env = dict(os.environ, RAYON_NUM_THREADS="6", MALLOC_ARENA_MAX="2", BF_TRACE_READS="1", BF_TRACE_C43="1")
work = json.load(open(os.environ.get("C43_WORKLOAD", "bench/scale/data/bm25-msmarco.json")))
log = out + ".log"
p = subprocess.Popen([binary, cfg, f"127.0.0.1:{port}"], stdout=open(log, "w"), stderr=subprocess.STDOUT, env=env)
conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
for _ in range(300):
    try:
        conn.request("GET", "/"); conn.getresponse().read(); break
    except Exception:
        time.sleep(0.2); conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
time.sleep(1.5)
def post(path, body=None):
    conn.request("POST", path, body=json.dumps(body) if body is not None else None, headers={"content-type": "application/json"})
    r = conn.getresponse(); return r.status, r.read()
def query(i):
    t = time.time(); s, d = post("/v2/namespaces/msmarco/query", work[i]["request"]); ms = (time.time() - t) * 1000
    assert s == 200, (s, d[:200]); return ms
query(first)
try:
    for i in range(first, first + n):
        post("/_bench/drop_caches"); time.sleep(0.3)
        pos = os.path.getsize(log)
        t0 = time.time() * 1e6
        ms = query(i)
        t1 = time.time() * 1e6
        time.sleep(0.15)
        with open(log) as f:
            f.seek(pos); chunk = f.read()
        lines = [l for l in chunk.splitlines() if l.startswith("C43 ")]
        with open(out, "a") as f:
            f.write(json.dumps(dict(i=i, ms=ms, t0=t0, t1=t1, lines=lines)) + "\n")
finally:
    p.terminate(); p.wait()
