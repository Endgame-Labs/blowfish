#!/usr/bin/env python3
"""Round C45: replay cold BM25 traces with the bundle and posting waves replaced by one scoring pack per text segment.
usage: c45_model.py TRACE.jsonl LOCAL.jsonl FETCH.jsonl SECTIONS.jsonl [SOURCE]

TRACE.jsonl  c41_drive.py rows from the S3 run (cold, BF_TRACE_READS): per query the client time, the start time, and every backend read.
LOCAL.jsonl  c41_drive.py rows from a run on a local copy of the same store (caches dropped before every query): its client time is the
             query's CPU and HTTP cost with no object-store wait, the floor for scoring from packs that are already in memory.
FETCH.jsonl  c45_fetch_runs.py rows: the wall time to fetch a plan's bytes as parallel ranged GETs (SOURCE picks "fixture" or "fresh").
SECTIONS     c45_sections.py rows (per-segment sizes).

Per query i, the modeled latency of a design that reads the pack of every segment in S right after the manifest is
    new_end = max(latest end of the reads of segments not in S, manifest end + pack wall)
    latency = traced latency - latest end of any read + new_end            (the tail after the last read is kept)
and for S = all segments it is manifest end + pack wall + CPU, with CPU taken from LOCAL (a bound: it includes every cost of today's path).
The pack wall is drawn at random from the measured reps of the plan (rep 0, the first read of a never-read object, is dropped)."""
import json, random, re, statistics as st, sys, collections

trace = {r["i"]: r for r in map(json.loads, open(sys.argv[1]))}
local = {r["i"]: r for r in map(json.loads, open(sys.argv[2]))}
fetch = [r for r in map(json.loads, open(sys.argv[3])) if not r.get("inflight")]  # rows with a cap on parts in flight are reported apart
for r in fetch:
    r.setdefault("hedge_ms", 0)
source = sys.argv[5] if len(sys.argv) > 5 else "fixture"
LIVE = {"E6WK9J": 60.3, "327NE5": 115.5, "ZVFANR": 28.4, "XY6E0J": 115.5, "74XMSK": 59.1, "2SSDY6": 115.5, "CTPXH0": 56.6, "EMC6H9": 22.3}  # KiB
segs = []
for line in open(sys.argv[4]):
    r = json.loads(line)
    if "path" in r:  # pack bytes: postings + FST + lengths + live bitmap + records (trimmed: minus what a scorer never reads)
        base = r["long_postings"] + r["short_payload"] + r["fst"] + r["lengths"] + int(LIVE[r["path"][:6]] * 1024)
        segs.append(dict(id=r["path"][:6], docs=r["docs"], file=r["size"], untrim=base + r["records"],
                         trim=base + r["records"] - r["record_dropped"]))
rng = random.Random(45)


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


def rows_for(plan, part, hedge):
    return [r for r in fetch if r["source"] == source and r["plan"] == plan and r["part"] == part and r["hedge_ms"] == hedge and r["rep"] > 0]


def walls(plan, part, hedge=0):
    return [r["wall_ms"] for r in rows_for(plan, part, hedge)]


def seg_of(path):
    m = re.search(r"segments/([0-9A-Z]{26})/", path)
    return m.group(1)[-6:] if m else None


def manifest_end(r):
    ends = [x["end"] for x in r["reads"] if "manifest" in x["path"]]
    return min(ends) - r["t0"] if ends else 0.0


def summarize(name, vals, base):
    p50, p90 = pct(vals, .5), pct(vals, .9)
    print(f"{name:46s} p50 {p50:6.1f}  p90 {p90:6.1f}  mean {st.mean(vals):6.1f}   ratio p50 {base[0] / p50:5.3f}  p90 {base[1] / p90:5.3f}")


ids = sorted(set(trace) & set(local))
base = ([trace[i]["ms"] for i in ids], )
traced = [trace[i]["ms"] for i in ids]
b = (pct(traced, .5), pct(traced, .9))
print(f"queries {len(ids)}; traced S3 latency p50 {b[0]:.1f} p90 {b[1]:.1f} ms; local (CPU) p50 {pct([local[i]['ms'] for i in ids], .5):.1f} p90 {pct([local[i]['ms'] for i in ids], .9):.1f} ms")
print(f"manifest end p50 {pct([manifest_end(trace[i]) for i in ids], .5):.1f} p90 {pct([manifest_end(trace[i]) for i in ids], .9):.1f} ms after the client sent the query")
bytes_q = [sum(x["bytes"] for x in trace[i]["reads"]) for i in ids]
gets_q = [len(trace[i]["reads"]) for i in ids]
print(f"today per query: {st.mean(bytes_q) / 1e6:.2f} MB (p50 {pct(bytes_q, .5) / 1e6:.2f}), {st.mean(gets_q):.1f} GETs")
print()
SIZE = {"trim": "trim", "untrim": "untrim", "full": "file", "min": "minimal"}
CPU_TOTAL, TAIL = 29.0, 3.0  # pipelined scoring: a segment's CPU is its share (by documents) of CPU_TOTAL ms; TAIL is the merge and response
docs = [s["docs"] for s in segs]
combos = sorted({(r["plan"], r["part"], r["hedge_ms"]) for r in fetch if r["plan"] in ("trim", "untrim", "full", "min", "min80")})
for plan, part, hedge in combos:
    rs = rows_for(plan, part, hedge)
    if not rs:
        continue
    size = rs[0]["bytes"] / 1e6
    gets = rs[0]["parts"] + 14
    tag = f"{plan} {size:.0f} MB, {part / 1048576:g} MiB parts{', hedge %d ms' % hedge if hedge else ''} ({gets} GETs)"
    w = [r["wall_ms"] for r in rs]
    flat, pipe = [], []
    for i in ids:
        mw = manifest_end(trace[i])
        for _ in range(5):
            r = rng.choice(rs)
            flat.append(mw + r["wall_ms"] + local[i]["ms"])
            pipe.append(mw + max(d + CPU_TOTAL * n / sum(docs) for d, n in zip(r["per_object_ms"], docs)) + TAIL)
    print(f"{tag}: wall p50 {pct(w, .5):.0f} p90 {pct(w, .9):.0f}")
    summarize("   CPU after the fetch (local cold floor)", flat, b)
    summarize("   CPU pipelined behind the arrivals", pipe, b)
print()
small = sorted(segs, key=lambda s: s["docs"])
for k in (3, 4, 5):
    S = {s["id"] for s in small[:k]}
    w = walls(f"small:{k}", 8 << 20)
    if not w:
        continue
    mod = []
    for i in ids:
        r = trace[i]
        per = collections.defaultdict(float)
        for x in r["reads"]:
            s = seg_of(x["path"])
            if s:
                per[s] = max(per[s], x["end"] - r["t0"])
        last = max(x["end"] for x in r["reads"]) - r["t0"]
        rest = max([v for s, v in per.items() if s not in S] or [0.0])
        for _ in range(5):
            new_end = max(rest, manifest_end(r) + rng.choice(w))
            mod.append(r["ms"] - last + new_end)
    size = sum(s["trim"] for s in small[:k]) / 1e6
    summarize(f"packs for the {k} smallest segments ({size:.0f} MB, 8 MiB parts)", mod, b)
