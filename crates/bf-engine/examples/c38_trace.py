# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Critical chain of cold filtered queries (Round C38).

Usage: c38_trace.py ARM run-dir [run-dir ...]
Reads the rows.jsonl a cold_ab.py run wrote (BF_TRACE_READS=1, BF_TRACE_STAGES=1). Prints, as
medians over the arm's queries in ms from the request start: the engine's stage timestamps (live
masks, directory, memberships, predicates, plan, scan), the slab membership reads (`.rows-v1.bfr`:
first start, median end, last end, count), the first leaf read (a read of a slab object that is not
a row map), the last read end, and the response.
"""

import json
import sys
from pathlib import Path

import numpy as np

arm, *runs = sys.argv[1:]
med = lambda v: round(float(np.median(v)), 1) if v else None  # noqa: E731
for run in runs:
    rows = [json.loads(line) for line in (Path(run) / "rows.jsonl").read_text().splitlines()]
    rows = [r for r in rows if r["arm"] == arm]
    acc: dict[str, list[float]] = {}

    def put(k, v):
        acc.setdefault(k, []).append(v)

    for r in rows:
        t0 = r["request_start_us"]
        put("response_ms", r["ms"])
        put("gets", r["gets"] or 0)
        seen: dict[str, list[float]] = {}
        for s in r.get("stage_timeline", []):
            # A stage named `phase:key` happens once per segment or slab: keep its first and last.
            seen.setdefault(s["stage"].split(":")[0], []).append((s["end_us"] - t0) / 1000)
        for name, ms in seen.items():
            put("stage:" + name + " first", min(ms))
            if len(ms) > 1:
                put("stage:" + name + " last", max(ms))
        tl = r["read_timeline"]
        maps = [e for e in tl if e["object"].endswith(".rows-v1.bfr")]
        leaf = [e for e in tl if "/slabs/" in e["object"] and not e["object"].endswith(".rows-v1.bfr")
                and "ann-dir" not in e["object"] and "ann3" not in e["object"]]
        filt = [e for e in tl if e["object"].endswith("filter.bfx")]
        if filt:
            put("filter_reads_first_start", (min(e["start_us"] for e in filt) - t0) / 1000)
            put("filter_reads_last_end", (max(e["end_us"] for e in filt) - t0) / 1000)
        if maps:
            put("map_reads", len(maps))
            put("map_first_start", (min(e["start_us"] for e in maps) - t0) / 1000)
            put("map_median_end", (float(np.median([e["end_us"] for e in maps])) - t0) / 1000)
            put("map_last_end", (max(e["end_us"] for e in maps) - t0) / 1000)
            put("map_bytes_mb", sum(e["bytes"] for e in maps) / 1e6)
        if leaf:
            put("first_leaf_read_start", (min(e["start_us"] for e in leaf) - t0) / 1000)
            put("leaf_last_end", (max(e["end_us"] for e in leaf) - t0) / 1000)
            put("leaf_mb", sum(e["bytes"] for e in leaf) / 1e6)
        if tl:
            put("last_read_end", (max(e["end_us"] for e in tl) - t0) / 1000)
    print(json.dumps({"run": Path(run).name, "arm": arm, "queries": len(rows),
                      **{k: med(v) for k, v in sorted(acc.items())}}, indent=1))
