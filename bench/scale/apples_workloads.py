# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "pyarrow==26.0.1", "httpx==0.28.1"]
# ///
"""Write the workloads for the turbopuffer apples-to-apples runs into --out (default data/).

  ann-apples.json   200 held-out 1024-dim vectors from data/oracle-10m.npz, with exact top-10 truth per query
  bm25-msmarco.json MS MARCO dev queries (BeIR/msmarco queries joined to the dev qrels), seeded sample, strong BM25 top 10
Usage: uv run apples_workloads.py [--hub-endpoint URL] [--msmarco-queries 1000] [--out DIR]
"""
import argparse, io, json, random
from pathlib import Path

import httpx
import numpy as np
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hub-endpoint", default="https://huggingface.co")
    p.add_argument("--msmarco-queries", type=int, default=1000)
    p.add_argument("--out", type=Path, default=HERE / "data")
    a = p.parse_args()
    o = np.load(HERE / "data/oracle-10m.npz")
    ann = [{"kind": "ann", "query_id": i, "truth": o["ann_ids"][i].tolist(),
            "request": {"rank_by": ["vector", "ANN", o["vector"][i].astype("f4").tolist()], "top_k": 10,
                        "consistency": {"level": "strong"}}} for i in range(len(o["vector"]))]
    (a.out / "ann-apples.json").write_text(json.dumps(ann))
    with httpx.Client(timeout=120, follow_redirects=True) as c:
        qs = c.get(f"{a.hub_endpoint}/datasets/BeIR/msmarco/resolve/main/queries/queries-00000-of-00001.parquet").content
        dev = c.get(f"{a.hub_endpoint}/datasets/BeIR/msmarco-qrels/resolve/main/dev.tsv").text
    ids = {l.split("\t")[0] for l in dev.splitlines()[1:] if l.strip()}
    t = pq.read_table(io.BytesIO(qs)).to_pydict()
    rows = sorted((i, x) for i, x in zip(t["_id"], t["text"]) if i in ids)
    random.Random(7).shuffle(rows)
    rows = rows[: a.msmarco_queries]
    bm = [{"kind": "bm25", "query_id": i, "request": {"rank_by": ["text", "BM25", x], "top_k": 10,
                                                      "consistency": {"level": "strong"}}} for i, (_, x) in enumerate(rows)]
    (a.out / "bm25-msmarco.json").write_text(json.dumps(bm))
    print(len(ann), "ann;", len(bm), "bm25 (dev queries available:", len(ids), ")")


if __name__ == "__main__":
    main()
