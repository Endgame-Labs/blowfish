#!/usr/bin/env python3
"""usage: c45_fetch_summary.py FETCH.jsonl: p50 / p90 / max wall time per (source, plan, part), first read (rep 0) apart."""
import json, sys, collections
g = collections.defaultdict(list)
first = {}
for l in open(sys.argv[1]):
    r = json.loads(l)
    k = (r["source"], r["plan"], r["part"] // 1024, r.get("inflight", 0))
    (first.__setitem__(k, r["wall_ms"]) if r["rep"] == 0 and k not in first else g[k].append(r))
def pct(v, q):
    v = sorted(v); return v[min(len(v) - 1, int(q * len(v)))]
print("source plan MB part_KiB cap parts | wall p50 p90 max | GB/s p50 | part first-byte p50 | rep0")
for k in sorted(g):
    rs = g[k]; w = [r["wall_ms"] for r in rs]
    print(f"{k[0]:8s} {k[1]:9s} {rs[0]['bytes'] / 1e6:6.1f} {k[2]:5d} {k[3]:3d} {rs[0]['parts']:3d} | {pct(w, .5):6.1f} {pct(w, .9):6.1f} {max(w):6.1f} | {pct([r['gbps'] for r in rs], .5):4.2f} | {pct([r['part_first_p50'] for r in rs], .5):5.1f} | {first.get(k, 0):6.1f}")
