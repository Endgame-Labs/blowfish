# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Summarize paired cold runs of cold_ab.py (Round C32).

Usage: c32_pairs.py BASE_ARM CAND_ARM run-dir [run-dir ...]
For every run directory: candidate/base p50 ratio with a 95% interval from paired resamples,
p50/p90 of each arm, mean GETs and MB, and whether every pair returned identical ordered IDs
and identical distance values (the JSON numbers, which are the f64 bits).
"""

import json
import sys
from pathlib import Path

import numpy as np

base, cand, *runs = sys.argv[1:]
rng = np.random.default_rng(32)
for run in runs:
    rows = [json.loads(line) for line in (Path(run) / "rows.jsonl").read_text().splitlines()]
    by = {}
    for r in rows:
        by.setdefault(r["q"], {})[r["arm"]] = r
    pairs = [(v[base], v[cand]) for v in by.values() if base in v and cand in v]
    b = np.array([p[0]["ms"] for p in pairs])
    c = np.array([p[1]["ms"] for p in pairs])
    exact = all(p[0]["ids"] == p[1]["ids"] and p[0]["distances"] == p[1]["distances"] for p in pairs)
    idx = rng.integers(0, len(pairs), size=(20000, len(pairs)))
    ratios = np.median(c[idx], axis=1) / np.median(b[idx], axis=1)
    lo, hi = np.percentile(ratios, [2.5, 97.5])
    mean = lambda ps, k: float(np.mean([p[k] for p in ps]))  # noqa: E731
    print(
        json.dumps(
            {
                "run": Path(run).name,
                "pairs": len(pairs),
                "base_p50_p90": [round(float(np.percentile(b, 50)), 1), round(float(np.percentile(b, 90)), 1)],
                "cand_p50_p90": [round(float(np.percentile(c, 50)), 1), round(float(np.percentile(c, 90)), 1)],
                "ratio": round(float(np.median(c) / np.median(b)), 3),
                "ci95": [round(float(lo), 3), round(float(hi), 3)],
                "gets_base_cand": [round(mean([p[0] for p in pairs], "gets")), round(mean([p[1] for p in pairs], "gets"))],
                "mb_base_cand": [
                    round(mean([p[0] for p in pairs], "bytes") / 1e6, 1),
                    round(mean([p[1] for p in pairs], "bytes") / 1e6, 1),
                ],
                "rows_returned": round(float(np.mean([len(p[0]["ids"]) for p in pairs])), 1),
                "exact": exact,
            }
        )
    )
