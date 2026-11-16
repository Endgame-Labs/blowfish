"""Summarize BF_TRACE_READS lines of one query into waves.

Usage: python3 waves.py SERVER_LOG [--gap-ms 25]
A new wave starts when a request begins after every earlier request has already finished.
Prints per wave: requests, MB, start offset, wall ms, request p50 and max ms, and the biggest object kinds.
"""
import sys, re, collections
gap = 0
rows = []
for line in open(sys.argv[1]):
    if line.startswith("TRACE_READ "):
        _, path, a, b, n, end, el = line.split()
        rows.append((int(end) - int(el), int(end), int(n), int(el), path))
rows.sort()
if not rows:
    sys.exit("no reads")
t0 = rows[0][0]
waves, cur, cur_end = [], [], None
for r in rows:
    if cur and r[0] >= cur_end:
        waves.append(cur); cur = []
    cur.append(r); cur_end = max(cur_end or 0, r[1]) if cur[:-1] else r[1]
waves.append(cur)
print(f"{len(rows)} reads, {sum(r[2] for r in rows)/1e6:.1f} MB, {len(waves)} waves")
for i, w in enumerate(waves):
    el = sorted(r[3] for r in w)
    kinds = collections.Counter(re.sub(r"[0-9A-Z]{26}|[0-9a-f-]{36}|\d{10,}", "*", r[4]).split("/")[-1] for r in w)
    print(f"wave {i}: {len(w):4d} req {sum(r[2] for r in w)/1e6:6.2f} MB  start +{min(r[0] for r in w)-t0:5d} ms  wall {max(r[1] for r in w)-min(r[0] for r in w):5d} ms  "
          f"req p50 {el[len(el)//2]} max {el[-1]}  {dict(kinds.most_common(4))}")
