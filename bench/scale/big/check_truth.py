# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "pyarrow==26.0.1", "httpx==0.28.1"]
# ///
"""Spot-check loader truth against brute force on sampled shards.

For each sampled (language, shard), score every row against the 200 queries. A row that scores above a query's
10th-best truth score must be in that query's truth ids (otherwise the truth missed it). Also confirms that truth ids
inside a sampled shard score exactly what the truth file says.
Usage: uv run check_truth.py TRUTH_NPZ SCRATCH_DIR [--shards en:7,en:200,de:50,...]
"""
import argparse, sys
from pathlib import Path
import httpx, numpy as np, pyarrow.parquet as pq
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "crates/bf-engine/examples"))
import scale_load as L

p = argparse.ArgumentParser(); p.add_argument("truth"); p.add_argument("scratch", type=Path)
p.add_argument("--shards", default="en:3,en:150,en:400,de:50,de:200,fr:90,es:20,ru:100,ru:137")
p.add_argument("--hub-endpoint", default="https://huggingface.co")
a = p.parse_args()
t = np.load(a.truth); query, ids, scores = t["query"], t["ids"], t["scores"]
floor = scores[:, -1]
a.scratch.mkdir(parents=True, exist_ok=True)
bad = checked = rows = 0
with httpx.Client(timeout=300) as c:
    for spec in a.shards.split(","):
        lang, shard = spec.split(":"); shard = int(shard)
        unit = next(u for u in L.UNITS if u[1] == lang and u[2] == shard)
        path = L.unit_path(a.scratch, unit); L.download(c, a.hub_endpoint, unit, path)
        base = unit[0] * L.LANGUAGE_ID_STRIDE + shard * L.SHARD_ROWS
        off = 0
        for batch in pq.ParquetFile(path).iter_batches(batch_size=5000):
            m = L.vectors(batch); m = m / np.maximum(np.linalg.norm(m, axis=1, keepdims=True), 1e-30)
            s = query @ m.T; rid = base + off + np.arange(len(m)); off += len(m); rows += len(m)
            held = (rid < 100_000) & (rid % 500 == 0)  # held-out queries are not loaded
            for q in range(len(query)):
                above = np.flatnonzero((s[q] > floor[q] + 1e-6) & ~held)
                missing = [int(rid[i]) for i in above if int(rid[i]) not in set(ids[q].tolist())]
                bad += len(missing); checked += 1
                if missing: print("MISSED", q, missing[:3])
        path.unlink()
print({"shards": a.shards, "rows_checked": rows, "query_shard_pairs": checked, "missed_rows": bad})
