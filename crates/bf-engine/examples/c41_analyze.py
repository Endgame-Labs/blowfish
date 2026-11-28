#!/usr/bin/env python3
"""analyze.py ROWS.jsonl [sizes.json]: per-query dependency chains of traced reads."""
import json, sys, statistics as st, collections, re
rows = [json.loads(l) for l in open(sys.argv[1])]
SIZES = json.load(open(sys.argv[2])) if len(sys.argv) > 2 else {}
def fts_kind(x):
    m = re.search(r"segments/([0-9A-Z]{26})/fts", x["path"]); s = SIZES.get(m.group(1)) if m else None
    if not s: return "fts"
    if x["a"] >= s["lengths_offset"]: return "lengths"
    if x["a"] >= s["metadata_offset"]: return "bundle"
    return "post"
def kind(p):
    if "manifest" in p: return "manifest"
    if p.endswith(".wal"): return "wal"
    if "text-stats" in p: return "stats"
    if "text-keys" in p: return "keys"
    if "/live." in p: return "live"
    if "/ids" in p: return "ids"
    if p.endswith("fts.1.bft"): return "fts"
    return "other:" + re.sub(r"[0-9A-Za-z\-]{20,}", "*", p.split("/")[-1])
def pct(v, q): v = sorted(v); return v[min(len(v)-1, int(q*len(v)))] if v else 0
lv_stats = collections.defaultdict(lambda: collections.defaultdict(list))
tot, depth, span_first, kinds = [], [], collections.defaultdict(list), collections.Counter()
chain_len = []
for r in rows:
    rs = sorted(({**x, "start": x["end"] - x["el"], "k": (fts_kind(x) if x["path"].endswith("fts.1.bft") else kind(x["path"]))} for x in r["reads"] if x["bytes"] or True), key=lambda x: x["start"])
    t0 = min(x["start"] for x in rs)
    # level = 1 + deepest read that finished before this one started (slack 1 ms)
    for i, x in enumerate(rs):
        x["lv"] = 1 + max([y["lv"] for y in rs[:i] if y["end"] <= x["start"] + 1] or [0])
    # critical path: from the last finisher back through the latest-ending predecessor
    cur = max(rs, key=lambda x: x["end"]); path = [cur]
    while True:
        pre = [y for y in rs if y["end"] <= cur["start"] + 1 and y is not cur]
        if not pre: break
        cur = max(pre, key=lambda y: y["end"]); path.append(cur)
    path.reverse(); chain_len.append(len(path))
    r["crit"] = [(x["k"], x["start"] - t0, x["end"] - x["start"], x["fb_us"] / 1000, x["bytes"]) for x in path]
    tot.append(r["ms"]); depth.append(max(x["lv"] for x in rs))
    for lv in sorted({x["lv"] for x in rs}):
        w = [x for x in rs if x["lv"] == lv]
        s = lv_stats[lv]
        s["n"].append(len(w)); s["bytes"].append(sum(x["bytes"] for x in w)); s["start"].append(min(x["start"] for x in w) - t0)
        s["end"].append(max(x["end"] for x in w) - t0); s["fb"].append(st.median(x["fb_us"] / 1000 for x in w)); s["el"].append(st.median(x["el"] for x in w))
        s["elmax"].append(max(x["el"] for x in w))
        for k, c in collections.Counter(x["k"] for x in w).items(): s["k:" + k].append(c)
print(f"queries {len(rows)}  client ms p50 {pct(tot,.5):.1f} p90 {pct(tot,.9):.1f}   dependency depth p50 {pct(depth,.5)} p90 {pct(depth,.9)}   critical-path reads p50 {pct(chain_len,.5)}")
print("level: queries  reads(p50)  MB(p50)  start(p50)  end(p50)  first-byte p50  total p50  slowest-read p50 | kinds (mean per query)")
for lv in sorted(lv_stats):
    s = lv_stats[lv]
    ks = {k[2:]: round(sum(v) / len(s["n"]), 1) for k, v in s.items() if k.startswith("k:")}
    print(f"L{lv}: {len(s['n']):4d}  {pct(s['n'],.5):5.0f}  {pct(s['bytes'],.5)/1e6:6.2f}  {pct(s['start'],.5):6.0f}  {pct(s['end'],.5):6.0f}  fb {pct(s['fb'],.5):5.1f}  el {pct(s['el'],.5):5.1f}  max {pct(s['elmax'],.5):5.1f} | {ks}")
# typical critical path of the median query
med = sorted(rows, key=lambda r: r["ms"])[len(rows)//2]
print("median query critical path (kind, start ms, total ms, first-byte ms, bytes):")
for c in med["crit"]: print("  ", c[0], round(c[1]), c[2], round(c[3], 1), c[4])
# critical-path time attribution by kind (sum of read totals on the path)
attr = collections.defaultdict(list)
for r in rows:
    a = collections.defaultdict(float)
    for k, s, e, fb, b in r["crit"]: a[k] += e
    for k in a: attr[k].append(a[k])
print("critical-path ms by kind (p50 over queries having it, share of queries):", {k: (round(pct(v, .5), 1), round(len(v)/len(rows), 2)) for k, v in attr.items()})
