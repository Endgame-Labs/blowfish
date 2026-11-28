#!/usr/bin/env python3
"""Cold BM25 driver (Round C41): one fresh server process, drop caches before every query, trace reads.
usage: drive.py BINARY STORAGE_JSON OUT.jsonl FIRST N PORT [ENV=VAL ...]"""
import http.client, json, os, subprocess, sys, time, signal
binary, cfg, out, first, n, port = sys.argv[1:7]; first, n, port = int(first), int(n), int(port)
env = dict(os.environ, RAYON_NUM_THREADS="6", MALLOC_ARENA_MAX="2", BF_TRACE_READS="1")
for kv in sys.argv[7:]:
    k, v = kv.split("=", 1); env[k] = v
work = json.load(open(os.environ.get("C41_WORKLOAD", "bench/scale/data/bm25-msmarco.json")))
log = out + ".log"
lf = open(log, "w")
p = subprocess.Popen([binary, cfg, f"127.0.0.1:{port}"], stdout=lf, stderr=subprocess.STDOUT, env=env)
open(out + ".pid", "w").write(str(p.pid))
conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
for _ in range(300):
    try:
        conn.request("GET", "/"); conn.getresponse().read(); break
    except Exception:
        time.sleep(0.2); conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
time.sleep(1.5)
def post(path, body=None):
    conn.request("POST", path, body=json.dumps(body) if body is not None else None, headers={"content-type": "application/json"})
    r = conn.getresponse(); d = r.read(); return r.status, d
def query(i):
    t = time.time(); s, d = post("/v2/namespaces/msmarco/query", work[i]["request"]); ms = (time.time() - t) * 1000
    assert s == 200, (s, d[:200]); return ms, d
query(first)  # throwaway: connections and code
rows = []
try:
    for i in range(first, first + n):
        post("/_bench/drop_caches"); time.sleep(0.3)
        pos = os.path.getsize(log)
        t0 = time.time() * 1000
        ms, d = query(i)
        time.sleep(0.15)
        with open(log) as f:
            f.seek(pos); chunk = f.read()
        reads = []
        for line in chunk.splitlines():
            if line.startswith("TRACE_READ "):
                parts = line.split()
                path, a, b, nb, end, el = parts[1:7]
                fb = int(parts[7]) if len(parts) > 7 else 0
                reads.append(dict(path=path, a=int(a), b=int(b), bytes=int(nb), end=int(end), el=int(el), fb_us=fb))
        rows.append(dict(i=i, ms=ms, t0=t0, reads=reads, resp=json.loads(d)))
        with open(out, "a") as f:
            f.write(json.dumps(rows[-1]) + "\n")
finally:
    p.terminate(); p.wait()
