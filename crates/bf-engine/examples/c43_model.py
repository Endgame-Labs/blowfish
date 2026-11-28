#!/usr/bin/env python3
"""Round C43 step 1: model cancel-on-floor from c43_trace.py output.
usage: c43_model.py TRACE.jsonl
Per segment, the first posting batch: a window is scored when the last read it needs has arrived, and a read is
canceled once every window that needs it has a bound strictly below the floor that earlier arrivals set (the
floor is the largest k-th best score over the segments' completed windows). A batch ends when each window is
scored or proven useless. A query's modeled latency drops by the gap between its slowest segment now and then."""
import json, re, sys, statistics as st
INF = float("inf")

def parse(rec):
    segs, gets = {}, []
    for l in rec["lines"]:
        f = l.split()
        kv = dict(x.split("=", 1) for x in f[2:] if "=" in x)
        if f[1] == "GET":
            gets.append((int(kv["len"]), int(kv["from"]), int(kv["to"]), int(kv["start_us"]), int(kv["end_us"])))
        elif f[1] == "BATCH":
            s = segs.setdefault(kv["seg"], dict(batches=[], scores={}, end=None, len=int(kv["len"])))
            s["batches"].append(dict(t=int(kv["t_us"]), k=int(kv["k"]), windows=[], done=None))
        elif f[1] == "W":
            segs[kv["seg"]]["batches"][-1]["windows"].append((int(kv["w"]), float(kv["bound"]), kv["need"]))
        elif f[1] == "DONE":
            segs[kv["seg"]]["batches"][-1]["done"] = int(kv["t_us"])
        elif f[1] == "S":
            segs[kv["seg"]]["scores"][int(kv["w"])] = [float(x) for x in l.split("scores=")[1].strip("[]").split(",") if x.strip()]
        elif f[1] == "END":
            segs[kv["seg"]]["end"] = int(kv["t_us"])
    return segs, gets

def model(rec):
    segs, gets = parse(rec)
    info = {}
    for sid, s in segs.items():
        if not s["batches"] or s["batches"][0]["done"] is None:
            continue
        b = s["batches"][0]
        mine = [g for g in gets if g[0] == s["len"] and g[4] >= b["t"] and g[3] >= b["t"] - 3000 and g[4] <= b["done"] + 1000]
        wins = []
        for w, bound, need in b["windows"]:
            arr, ids = b["t"], []
            for item in filter(None, need.split(";")):
                off, size = map(int, item.split(":"))
                cov = [i for i, g in enumerate(mine) if g[1] < off + size and g[2] > off]
                if cov:
                    arr = max(arr, max(mine[i][4] for i in cov)); ids += cov
            wins.append(dict(w=w, bound=bound, T=arr, gets=set(ids)))
        info[sid] = dict(b=b, wins=wins, mine=mine, k=b["k"], scores=s["scores"], end=s["end"])
    if not info:
        return None
    # floor(t): largest over segments of the k-th best score among windows completed by t
    events = []
    for sid, d in info.items():
        for x in d["wins"]:
            sc = d["scores"].get(x["w"])
            if sc:
                events.append((x["T"], sid, sc))
    events.sort(key=lambda e: e[0])
    floors = []  # (time, floor) stepwise nondecreasing
    pool = {sid: [] for sid in info}
    cur = 0.0
    for t, sid, sc in events:
        pool[sid] = sorted(pool[sid] + sc, reverse=True)[: info[sid]["k"]]
        if len(pool[sid]) == info[sid]["k"]:
            cur = max(cur, pool[sid][-1])
        floors.append((t, cur))
    def useless_at(bound):  # earliest time the floor is strictly above bound
        for t, f in floors:
            if f > bound:
                return t
        return INF
    out = dict(segs=0, shift=0, last_cancelable=0)
    orig_end, new_end = 0, 0
    for sid, d in info.items():
        b = d["b"]
        res = []
        for x in d["wins"]:
            u = useless_at(x["bound"])
            res.append(min(x["T"], max(u, b["t"])))
            x["u"] = u
        model_done = max(res) if res else b["t"]
        shift = max(0, b["done"] - model_done)
        d["shift"] = shift
        orig_end = max(orig_end, d["end"] or 0)
        new_end = max(new_end, (d["end"] or 0) - shift)
        out["segs"] += 1
    out["shift"] = (orig_end - new_end) / 1000
    return out

rows = [json.loads(l) for l in open(sys.argv[1])]
base, mod, shifts, nobatch = [], [], [], 0
for r in rows:
    m = model(r)
    if m is None:
        nobatch += 1
        continue
    base.append(r["ms"]); mod.append(r["ms"] - m["shift"]); shifts.append(m["shift"])
def pct(xs, p):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(len(xs) * p / 100))]
print(f"queries {len(base)} (no batch: {nobatch})")
for name, xs in (("traced", base), ("modeled", mod)):
    print(f"{name}: p50 {pct(xs,50):.1f} p90 {pct(xs,90):.1f} mean {st.mean(xs):.1f}")
print(f"ratio p50 {pct(base,50)/pct(mod,50):.3f} p90 {pct(base,90)/pct(mod,90):.3f} mean {st.mean(base)/st.mean(mod):.3f}")
print(f"shift ms: p50 {pct(shifts,50):.1f} p90 {pct(shifts,90):.1f} max {max(shifts):.1f}; queries with shift>1ms: {sum(s>1 for s in shifts)}")
