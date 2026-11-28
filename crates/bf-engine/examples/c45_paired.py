#!/usr/bin/env python3
"""Round C45: paired whole-pack fetch timings. One c45-fetch process per source and hedge setting; every rep runs every plan back to back,
in an order that rotates, so the plans see the same S3 conditions.
usage: c45_paired.py FETCH_BIN BUCKET SECTIONS_JSONL OUT.jsonl REPS
Plans are NAME@PART_KIB: trim (114 MB), untrim, min (92 MB), min80, and full (the whole objects, 209 MB). C45_PLANS narrows the list,
C45_HEDGE_MS_LIST (default "0,45") sets the hedge settings, C45_FIXTURE_PREFIX and C45_FRESH_PREFIX name the key prefixes (no bucket or path is hardcoded)."""
import json, os, subprocess, sys

fetch, bucket, sections, out, reps = sys.argv[1:6]
fixture, fresh = os.environ["C45_FIXTURE_PREFIX"], os.environ["C45_FRESH_PREFIX"]
LIVE = {"E6WK9J": 60.3, "327NE5": 115.5, "ZVFANR": 28.4, "XY6E0J": 115.5, "74XMSK": 59.1, "2SSDY6": 115.5, "CTPXH0": 56.6, "EMC6H9": 22.3}
IDS = {"E6WK9J": "01M87ZCCPR8WQT9ZPA25E6WK9J", "327NE5": "01M87ZCGQQ87NSWNJ66C327NE5", "ZVFANR": "01M87ZCH5F8ZYT0JG5J2ZVFANR",
       "XY6E0J": "01M87ZCQKH8HQA5888SPXY6E0J", "74XMSK": "01M87ZCR198AXAQ3WVW074XMSK", "2SSDY6": "01M87ZCYV09AVS95G9BP2SSDY6",
       "CTPXH0": "01M87ZCZ9E8XBRBFZCCBCTPXH0", "EMC6H9": "01M87ZD5CV9KHB7SMZNKEMC6H9"}
segs = []
for line in open(sections):
    r = json.loads(line)
    if "path" not in r:
        continue
    base = r["long_postings"] + r["short_payload"] + r["fst"] + r["lengths"] + int(LIVE[r["path"][:6]] * 1024)
    segs.append(dict(id=r["path"][:6], file=r["size"], trim=base + r["records"] - r["record_dropped"], untrim=base + r["records"],
                     minimal=base + 6 * r["blocks"]))
total_min = sum(s["minimal"] for s in segs)
SIZE = {"trim": lambda s: s["trim"], "untrim": lambda s: s["untrim"], "full": lambda s: s["file"], "min": lambda s: s["minimal"],
        "min80": lambda s: int(s["minimal"] * 80e6 / total_min)}
plans = os.environ.get("C45_PLANS", "trim@1536,trim@2048,trim@3072,trim@4096,full@2048,full@4096,min@2048,min80@2048").split(",")
for hedge in os.environ.get("C45_HEDGE_MS_LIST", "0,45").split(","):
    for source in ("fixture", "fresh"):
        key = lambda s: f"{fixture}/{IDS[s['id']]}/fts.1.bft" if source == "fixture" else f"{fresh}/{IDS[s['id']]}.bft"
        argv = [fetch, bucket, "us-west-2", reps, "0", "256"]
        for i, p in enumerate(plans):
            name, kib = p.split("@")
            argv += (["--"] if i else []) + [p, str(int(kib) << 10)] + [f"{key(s)}:{SIZE[name](s)}" for s in segs]
        res = subprocess.run(argv, capture_output=True, text=True, env=dict(os.environ, C45_HEDGE_MS=hedge))
        n = 0
        for line in res.stdout.splitlines():
            row = json.loads(line)
            row.update(source=source, hedge_ms=int(hedge), plan=row["name"].split("@")[0], part=int(row["name"].split("@")[1]) << 10)
            open(out, "a").write(json.dumps(row) + "\n"); n += 1
        print(source, "hedge", hedge, n, "rows", res.stderr[-200:], flush=True)
