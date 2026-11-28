# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Split the scan bytes of cold filtered queries (Round C39 step 1).

Usage: c39_split.py RUN_DIR [--skip 1]
Reads dump.bin (leaf records and candidates from the temporary hook) and rows.jsonl (GET bytes by kind from BF_TRACE_READS)
of a c39_dump.py run, and prints medians over the queries: leaves probed, rows per leaf, matching rows per leaf, code bytes
of matching and of non-matching rows, the share of 32-row blocks holding a match, block padding, and the GET bytes of slab
objects beyond the code bytes (cache-block alignment and the other slab reads: blobs and vectors).
"""

import json
import struct
import sys
from pathlib import Path

import numpy as np

LEAF_DT = np.dtype([("seg", "<u4"), ("cluster", "<u4"), ("rows", "<u4"), ("matching", "<u4"), ("blocks", "<u4"), ("hit", "<u4"),
                    ("bytes", "<u4"), ("cif", "<u4"), ("range", "<u8", (4,)), ("cdist", "<f4")])
run = Path(sys.argv[1])
skip = 1
buf = (run / "dump.bin").read_bytes()
rows = [json.loads(line) for line in (run / "rows.jsonl").read_text().splitlines()]
pos, recs = 0, []
while pos < len(buf):
    magic, q, k, n, nl, ne = struct.unpack_from("<6I", buf, pos)
    assert magic == 0xC39
    pos += 24 + 32
    leaves = np.frombuffer(buf, LEAF_DT, nl, pos)
    pos += nl * LEAF_DT.itemsize + n * 4 * 9 + ne * 8
    recs.append((n, leaves))
acc: dict[str, list[float]] = {}
for (n, leaves), row in zip(recs[skip:], rows[skip:]):
    nrows, matching, blocks, hit, codes = (leaves[f].astype(np.int64) for f in ("rows", "matching", "blocks", "hit", "bytes"))
    blob = (leaves["range"][:, 1] - leaves["range"][:, 0]).astype(np.int64)
    per = {
        "leaves_probed": len(leaves),
        "rows_scanned": nrows.sum(),
        "matching_rows": matching.sum(),
        "rows_per_leaf_med": float(np.median(nrows)),
        "matching_per_leaf_med": float(np.median(matching)),
        "leaves_with_no_match": int((matching == 0).sum()),
        "blocks": blocks.sum(),
        "blocks_with_a_match_pct": 100 * hit.sum() / max(blocks.sum(), 1),
        "code_bytes_mb": codes.sum() / 1e6,
        "factor_bytes_mb": 16 * nrows.sum() / 1e6,
        "matching_code_bytes_mb": (matching * 144).sum() / 1e6,
        "nonmatching_code_bytes_mb": ((nrows - matching) * 144).sum() / 1e6,
        "blob_bytes_mb": blob.sum() / 1e6,
        "block_padding_mb": (blocks * 32 * 128 - nrows * 128).sum() / 1e6,
        "slab_get_mb": row["bytes_by_kind"].get("slabs", 0) / 1e6,
        "rowmap_get_mb": row["bytes_by_kind"].get("rowmap", 0) / 1e6,
        "get_total_mb": sum(row["bytes_by_kind"].values()) / 1e6,
        "gets": row["gets"],
        "ms": row["ms"],
    }
    per["slab_get_over_code_bytes"] = per["slab_get_mb"] / max(per["code_bytes_mb"], 1e-9)
    per["alignment_and_other_slab_mb"] = per["slab_get_mb"] - per["code_bytes_mb"] - per["blob_bytes_mb"]
    for k, v in per.items():
        acc.setdefault(k, []).append(float(v))
print(json.dumps({"run": run.name, "queries": len(acc["ms"]), **{k: round(float(np.median(v)), 2) for k, v in acc.items()}}, indent=1))
