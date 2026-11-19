# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Write an ANN workload (200 held-out queries with exact top-10 truth) from an oracle-<size>.npz.

Usage: uv run make_workload.py ../data/oracle-100m.npz OUT.json
Same request shape as apples_workloads.py (strong consistency, top_k 10); items carry "truth" for recall@10.
"""
import json, sys
import numpy as np

o = np.load(sys.argv[1])
items = [{"kind": "ann", "query_id": i, "truth": o["ann_ids"][i].tolist(),
          "request": {"rank_by": ["vector", "ANN", o["vector"][i].astype("f4").tolist()], "top_k": 10,
                      "consistency": {"level": "strong"}}} for i in range(len(o["vector"]))]
json.dump(items, open(sys.argv[2], "w"))
print(len(items), "queries")
