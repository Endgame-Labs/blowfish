#!/usr/bin/env python3
"""Round C45: time whole-pack fetches from S3 with the c45-fetch tool, over a grid of plans, part sizes, and sources.
usage: c45_fetch_runs.py FETCH_BIN BUCKET SECTIONS_JSONL OUT.jsonl REPS [PASSES]
Sources: "fixture" (the retained 1M MS MARCO store, days old, read only) and "fresh" (a server-side copy under tmp/c45/fresh/, made
shortly before). Plans give the bytes read from the start of each text segment's object (the pack sizes come from
c45_sections.py: whole pack trimmed, untrimmed, the whole object, and the small segments only). Bucket and key prefixes
come from the arguments and the environment (C45_FIXTURE_PREFIX, C45_FRESH_PREFIX); nothing is hardcoded."""
import json, os, subprocess, sys

fetch, bucket, sections, out, reps = sys.argv[1:6]
passes = int(sys.argv[6]) if len(sys.argv) > 6 else 1
fixture = os.environ["C45_FIXTURE_PREFIX"]  # e.g. tmp/<fixture>/store/ns/msmarco/segments
fresh = os.environ["C45_FRESH_PREFIX"]  # e.g. tmp/c45/fresh
live = {"E6WK9J": 60.3, "327NE5": 115.5, "ZVFANR": 28.4, "XY6E0J": 115.5, "74XMSK": 59.1, "2SSDY6": 115.5, "CTPXH0": 56.6, "EMC6H9": 22.3}
segs = []
for line in open(sections):
    r = json.loads(line)
    if "path" not in r:
        continue
    post = r["long_postings"] + r["short_payload"]
    base = post + r["fst"] + r["lengths"] + int(live[r["path"][:6]] * 1024)
    # "min": a pack nobody has built, the smallest this format's contents allow: postings, the FST, 6 bytes per block
    # (first row, last row delta, max tf, min length, size) with the term's fields folded into the FST value, lengths, live bitmap.
    # "min80": the same scaled to 80 MB in total (Sol's budget).
    segs.append(dict(id=r["path"][:6], docs=r["docs"], file=r["size"], untrim=base + r["records"],
                     trim=base + r["records"] - r["record_dropped"], post=post, minimal=base + 6 * r["blocks"]))
ids = {"E6WK9J": "01M87ZCCPR8WQT9ZPA25E6WK9J", "327NE5": "01M87ZCGQQ87NSWNJ66C327NE5", "ZVFANR": "01M87ZCH5F8ZYT0JG5J2ZVFANR",
       "XY6E0J": "01M87ZCQKH8HQA5888SPXY6E0J", "74XMSK": "01M87ZCR198AXAQ3WVW074XMSK", "2SSDY6": "01M87ZCYV09AVS95G9BP2SSDY6",
       "CTPXH0": "01M87ZCZ9E8XBRBFZCCBCTPXH0", "EMC6H9": "01M87ZD5CV9KHB7SMZNKEMC6H9"}


def key(source, s):
    return f"{fixture}/{ids[s['id']]}/fts.1.bft" if source == "fixture" else f"{fresh}/{ids[s['id']]}.bft"


def plan(name):
    if name == "manifest":
        return None
    kind, _, n = name.partition(":")
    if kind in ("min", "min80"):
        scale = 1.0 if kind == "min" else 80e6 / sum(s["minimal"] for s in segs)
        return [(s, int(s["minimal"] * scale)) for s in segs]
    chosen = segs if kind in ("trim", "untrim", "full") else sorted(segs, key=lambda s: s["docs"])[: int(n)]
    field = {"trim": "trim", "untrim": "untrim", "full": "file"}.get(kind, "trim")
    return [(s, s[field]) for s in chosen]


# C45_PLANS (comma list) and C45_PARTS_KIB (comma list) narrow the grid.
PLANS = os.environ.get("C45_PLANS", "trim,untrim,full,small:3,small:4,small:5").split(",")
PARTS = [int(k) << 10 for k in os.environ.get("C45_PARTS_KIB", "2048,4096,8192,16384,32768").split(",")]
runs = []
for p in range(passes):
    order = ["fixture", "fresh"] if p % 2 == 0 else ["fresh", "fixture"]
    for source in order:
        for name in PLANS:
            for part in PARTS:
                runs.append((source, name, part))
for source, name, part in runs:
    specs = [f"{key(source, s)}:{n}" for s, n in plan(name)]
    cmd = [fetch, bucket, "us-west-2", reps, str(part), os.environ.get("C45_INFLIGHT", "0"), "256", *specs]
    res = subprocess.run(cmd, capture_output=True, text=True)
    for line in res.stdout.splitlines():
        row = json.loads(line)
        row.update(source=source, plan=name, part=part, inflight=int(os.environ.get('C45_INFLIGHT', '0')), hedge_ms=int(os.environ.get('C45_HEDGE_MS', '0')))
        open(out, "a").write(json.dumps(row) + "\n")
    print(source, name, part >> 20, "MiB", res.stdout.count("\n"), "reps", res.stderr[-200:], flush=True)
