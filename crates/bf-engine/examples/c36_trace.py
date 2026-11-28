# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Live-set reads of cold queries (Round C36).

Usage: c36_trace.py ARM run-dir [run-dir ...]
Reads the rows.jsonl a cold_ab.py run wrote (BF_TRACE_READS=1). For every query it counts the
GETs of `live.*.bfl` objects, the filter objects (`filter.bfx`), and every other read, and
places them in ms from the request start. Prints medians over queries: how many live GETs,
when the last one ended, when the first filter read started, and how much of the filter's
first read waited behind the live wave (the live set is on the critical path when the filter's
first read starts after the last live GET ends).
"""

import json
import sys
from pathlib import Path

import numpy as np

arm, *runs = sys.argv[1:]
for run in runs:
    rows = [json.loads(line) for line in (Path(run) / "rows.jsonl").read_text().splitlines()]
    rows = [r for r in rows if r["arm"] == arm]
    stats = {k: [] for k in ("ms", "gets", "live_gets", "live_last_end", "filter_gets", "filter_first_start", "filter_after_live", "all_reads")}
    for r in rows:
        t0 = r["request_start_us"]
        tl = r["read_timeline"]
        live = [e for e in tl if "/live." in e["object"]]
        filt = [e for e in tl if e["object"].endswith("filter.bfx")]
        stats["ms"].append(r["ms"])
        stats["gets"].append(r["gets"] or 0)
        stats["live_gets"].append(len(live))
        stats["all_reads"].append(len(tl))
        stats["filter_gets"].append(len(filt))
        if live:
            stats["live_last_end"].append((max(e["end_us"] for e in live) - t0) / 1000)
        if filt:
            stats["filter_first_start"].append((min(e["start_us"] for e in filt) - t0) / 1000)
        if live and filt:
            stats["filter_after_live"].append(
                float(min(e["start_us"] for e in filt) >= max(e["end_us"] for e in live) - 1000)
            )
    print(json.dumps({"run": Path(run).name, "arm": arm, "queries": len(rows),
                      **{k: (round(float(np.median(v)), 1) if v else None) for k, v in stats.items()}}))
