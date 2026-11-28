#!/usr/bin/env python3
"""Round C45: the CPU and HTTP floor of a BM25 query on a local copy of the store (no object-store wait).
usage: c45_cpu.py BINARY STORAGE_JSON OUT.json FIRST N PORT
Per query: drop every cache, run it (cold: decode everything from the page cache), then run it twice more (warm). Reports p50 / p90 of each."""
import http.client, json, os, subprocess, sys, time
binary, cfg, out, first, n, port = sys.argv[1:7]; first, n, port = int(first), int(n), int(port)
work = json.load(open(os.environ.get("C45_WORKLOAD", "bench/scale/data/bm25-msmarco.json")))
env = dict(os.environ, MALLOC_ARENA_MAX="2")
env.setdefault("RAYON_NUM_THREADS", "6")
p = subprocess.Popen([binary, cfg, f"127.0.0.1:{port}"], stdout=open(out + ".log", "w"), stderr=subprocess.STDOUT, env=env)
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
def cpu_ms():
    f = open(f"/proc/{p.pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) * 1000.0 / os.sysconf("SC_CLK_TCK")
def query(i):
    t = time.perf_counter(); s, d = post("/v2/namespaces/msmarco/query", work[i]["request"]); ms = (time.perf_counter() - t) * 1000
    assert s == 200, (s, d[:200]); return ms
query(first)
rows = []
try:
    for i in range(first, first + n):
        post("/_bench/drop_caches"); time.sleep(0.1)
        c0 = cpu_ms(); cold = query(i); c1 = cpu_ms()
        rows.append(dict(i=i, cold=cold, cold_cpu=c1 - c0, warm=[query(i), query(i)]))
finally:
    p.terminate(); p.wait()
json.dump(rows, open(out, "w"))
def pct(v, q): v = sorted(v); return v[min(len(v) - 1, int(q * len(v)))]
print("cold process CPU per query (all threads, 10 ms clock ticks): mean %.1f ms" % (sum(r["cold_cpu"] for r in rows) / len(rows)))
for name, v in (("cold", [r["cold"] for r in rows]), ("warm", [r["warm"][1] for r in rows])):
    print(f"{name}: p50 {pct(v, .5):.2f} p90 {pct(v, .9):.2f} ms")
