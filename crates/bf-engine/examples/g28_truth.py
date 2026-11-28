# /// script
# dependencies = ["numpy==2.5.4"]
# ///
"""Round G28 (from g27_truth.py): more filtered NQ workloads (10 percent) (id-range filters) and their exact top-10 truth.

Usage: uv run g27_truth.py DATA_DIR OUT_DIR [QUERIES]
DATA_DIR holds corpus.f16 and queries.npy. The NQ fixture has only `id` and `vector`, so the
filters are on `id`: `id-P` is the prefix `id < P percent`, and `sc-P` is an Or of id ranges
placed at random on a grid (the rows allowed are scattered over the id space). Writes
OUT_DIR/truth-CASE.npy (int32, QUERIES x 10) and OUT_DIR/filters.json (CASE -> filter).
"""
import json
import math
import sys
from pathlib import Path

import numpy as np

data, out = Path(sys.argv[1]), Path(sys.argv[2])
nq = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
corpus = np.load(data / "corpus.f16", mmap_mode="r")
n = len(corpus)
queries = np.load(data / "queries.npy")[:nq].astype("f4")
queries /= np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-30)
rng = np.random.default_rng(28)
ranges = {}
for pct in [10]:
    ranges[f"id-{pct}"] = [(0, math.ceil(n * pct / 100))]
# (cases, ranges, share of rows per range): total share is cases * share.
for name, count, width in [("sc-10", 5, 2.0)]:
    cells = int(round(100 / width))
    picks = sorted(rng.choice(cells, size=count, replace=False))
    ranges[name] = [(math.ceil(n * c * width / 100), math.ceil(n * (c + 1) * width / 100)) for c in picks]
filters = {}
for name, rs in ranges.items():
    one = [["id", "Lt", rs[0][1]]] if name.startswith("id-") else None
    filters[name] = one[0] if one else ["Or", [["And", [["id", "Gte", a], ["id", "Lt", b]]] for a, b in rs]]
(out / "filters10.json").write_text(json.dumps(filters))
best = {c: (np.full((nq, 10), -np.inf, "f4"), np.zeros((nq, 10), "i8")) for c in ranges}
step = 20000
for start in range(0, n, step):
    v = np.asarray(corpus[start:start + step], dtype="f4")
    v /= np.maximum(np.linalg.norm(v, axis=1, keepdims=True), 1e-30)
    sims = queries @ v.T
    ids = np.arange(start, start + len(v))
    for name, rs in ranges.items():
        mask = np.zeros(len(v), bool)
        for a, b in rs:
            mask |= (ids >= a) & (ids < b)
        if not mask.any():
            continue
        s = sims[:, mask]
        c = min(10, s.shape[1])
        top = np.argpartition(-s, c - 1, axis=1)[:, :c]
        os_, oi = best[name]
        cs = np.concatenate((os_, np.take_along_axis(s, top, 1)), 1)
        ci = np.concatenate((oi, ids[mask][top]), 1)
        keep = np.argsort(-cs, 1)[:, :10]
        best[name] = (np.take_along_axis(cs, keep, 1), np.take_along_axis(ci, keep, 1))
    if start % 400000 == 0:
        print("truth", start, n, flush=True)
for name, (_, i) in best.items():
    np.save(out / f"truth-{name}.npy", i.astype("<i4"))
for name, rs in ranges.items():
    print(name, sum(b - a for a, b in rs) / n)
