#!/usr/bin/env python3
"""Round C42: alternating cold (and warm) BM25 pairs of two server builds, fresh process per arm per round.
usage: c42_pairs.py BASE_BIN CAND_BIN STORAGE_JSON OUT.json ROUNDS FIRST N [WORKLOAD.json]
Per round: start one fresh process per arm, one throwaway query, then for each query alternate which arm goes first;
every arm query is cold (POST /_bench/drop_caches first), followed by two warm repeats (no drop). Records latency,
object-store GETs and bytes (from /metrics deltas), and the response rows, so the results can be compared."""
import http.client, json, os, re, subprocess, sys, time, statistics as st

base, cand, cfg, out, rounds, first, n = sys.argv[1:8]
rounds, first, n = int(rounds), int(first), int(n)
work = json.load(open(sys.argv[8] if len(sys.argv) > 8 else "bench/scale/data/bm25-msmarco.json"))
ENV = dict(os.environ, RAYON_NUM_THREADS="6", MALLOC_ARENA_MAX="2")
ENV.pop("BF_TRACE_READS", None)


class Arm:
    def __init__(self, name, binary, port, log):
        self.name = name
        self.p = subprocess.Popen([binary, cfg, f"127.0.0.1:{port}"], stdout=open(log, "w"), stderr=subprocess.STDOUT, env=ENV)
        self.port = port
        self.conn = None
        for _ in range(300):
            try:
                self.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
                self.conn.request("GET", "/")
                self.conn.getresponse().read()
                break
            except Exception:
                time.sleep(0.2)
        time.sleep(1.0)

    def post(self, path, body=None):
        self.conn.request("POST", path, body=json.dumps(body) if body is not None else None, headers={"content-type": "application/json"})
        r = self.conn.getresponse()
        return r.status, r.read()

    def metrics(self):
        self.conn.request("GET", "/metrics")
        text = self.conn.getresponse().read().decode()
        gets = sum(float(l.rsplit(" ", 1)[1]) for l in text.splitlines() if l.startswith("blowfish_object_store_attempts_total{"))
        nbytes = sum(float(l.rsplit(" ", 1)[1]) for l in text.splitlines() if l.startswith("blowfish_object_store_bytes_total{"))
        return gets, nbytes

    def query(self, i):
        t = time.perf_counter()
        s, d = self.post("/v2/namespaces/msmarco/query", work[i]["request"])
        ms = (time.perf_counter() - t) * 1000
        assert s == 200, (s, d[:200])
        return ms, d

    def stop(self):
        self.p.terminate()
        self.p.wait()


rows = []
for rnd in range(rounds):
    arms = {"base": Arm("base", base, 8501, f"{out}.base.log"), "cand": Arm("cand", cand, 8502, f"{out}.cand.log")}
    for a in arms.values():
        a.query(first)
    try:
        for i in range(first, first + n):
            order = ["base", "cand"] if (i + rnd) % 2 == 0 else ["cand", "base"]
            for name in order:
                a = arms[name]
                a.post("/_bench/drop_caches")
                time.sleep(0.05)
                g0, b0 = a.metrics()
                ms, d = a.query(i)
                g1, b1 = a.metrics()
                warm = [a.query(i)[0] for _ in range(2)]
                rows.append(dict(round=rnd, i=i, arm=name, ms=ms, gets=g1 - g0, bytes=b1 - b0, warm=warm[1],
                                 rows=json.dumps(json.loads(d).get("rows", json.loads(d)), sort_keys=True)))
    finally:
        for a in arms.values():
            a.stop()
    json.dump(rows, open(out, "w"))


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


print(f"rounds={rounds} queries/round={n} pairs={len(rows) // 2}")
by = {k: [r for r in rows if r["arm"] == k] for k in ("base", "cand")}
for k, rs in by.items():
    print(k, "cold p50 %.1f p90 %.1f mean %.1f ms | GETs %.1f | KB %.0f | warm p50 %.2f p90 %.2f ms" % (
        pct([r["ms"] for r in rs], .5), pct([r["ms"] for r in rs], .9), st.mean(r["ms"] for r in rs),
        st.mean(r["gets"] for r in rs), st.mean(r["bytes"] for r in rs) / 1e3,
        pct([r["warm"] for r in rs], .5), pct([r["warm"] for r in rs], .9)))
b, c = by["base"], by["cand"]
print("cold p50 ratio base/cand %.3f, p90 ratio %.3f, warm p50 ratio cand/base %.3f" % (
    pct([r["ms"] for r in b], .5) / pct([r["ms"] for r in c], .5), pct([r["ms"] for r in b], .9) / pct([r["ms"] for r in c], .9),
    pct([r["warm"] for r in c], .5) / pct([r["warm"] for r in b], .5)))
same = sum(1 for x, y in zip(b, c) if x["rows"] == y["rows"] and x["i"] == y["i"])
print("identical responses:", same, "of", len(b))
