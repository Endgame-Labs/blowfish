# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "pyarrow==26.0.1"]
# ///
"""Build aligned BM25, ANN, multi-query, and RRF workloads from bench/nq's prepared data.

Usage: uv run bench/scale/nq_hybrid_workload.py NQ_DATA .scratch/RUN --queries 200
Measure each workload with bm25_local.py, pointing at an immutable NQ file store.
The hybrid workload returns two result lists; the RRF workload also fuses them on the server.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--queries", type=int, default=200)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if not args.output.resolve().is_relative_to(root / ".scratch"):
        parser.error("output must be under .scratch/")
    vectors = np.load(args.data / "queries.npy", mmap_mode="r")
    ids = (args.data / "query_ids.txt").read_text().splitlines()
    if not 1 <= args.queries <= min(len(vectors), len(ids)):
        parser.error("queries must fit the prepared query set")
    texts = {r["_id"]: r["text"] for r in pq.read_table(
        args.data / "raw/queries/test.parquet", columns=["_id", "text"]
    ).to_pylist()}
    workloads = {kind: [] for kind in ["bm25", "ann", "hybrid", "rrf"]}
    for i in range(args.queries):
        text = {"rank_by": ["text", "BM25", texts[ids[i]]], "top_k": 10,
                "include_attributes": ["id"]}
        vector = {"rank_by": ["vector", "ANN", vectors[i].tolist()], "top_k": 10,
                  "include_attributes": ["id"]}
        for kind, request in [("bm25", text), ("ann", vector),
                              ("hybrid", {"queries": [text, vector]}),
                              ("rrf", {"queries": [text, vector], "rerank_by": ["RRF"], "limit": 10})]:
            workloads[kind].append({"query_id": i, "request": request})
    args.output.mkdir(parents=True, exist_ok=True)
    for kind, rows in workloads.items():
        (args.output / f"nq-{kind}.json").write_text(json.dumps(rows))


if __name__ == "__main__":
    main()
