# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Build data/oracle-<size>.npz from a loader checkpoint (truth.npz) for a larger prefix of the dataset.

Usage: uv run bench/scale/make_oracle_truth.py LOADER_SCRATCH/truth.npz data/oracle-1m.npz

The 200 held-out queries (vectors, titles) are the same at every size, so they are copied from oracle-100k.npz;
only the exact cosine top-10 truth (ann, selective, broad) changes with the number of loaded documents.
"""
import sys
from pathlib import Path

import numpy as np

base = np.load(Path(__file__).parent / "data" / "oracle-100k.npz")
t = np.load(sys.argv[1])
assert np.allclose(t["query"], base["vector"].astype("f4") / np.linalg.norm(base["vector"].astype("f4"), axis=1, keepdims=True), atol=1e-3)
np.savez_compressed(sys.argv[2], source_id=base["source_id"], vector=base["vector"], title=base["title"],
                    ann_ids=t["ids"], selective_ids=t["selective_ids"], broad_ids=t["broad_ids"])
print("rows", int(t["rows"]))
