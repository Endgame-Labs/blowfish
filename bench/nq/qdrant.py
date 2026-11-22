"""Load BEIR NQ vectors into a running Qdrant for a same-machine baseline, and wait until it is fully indexed.

Usage: uv run --group others python -I qdrant.py DATA_DIR --url http://127.0.0.1:18110 [--collection nq]

Settings follow the outside benchmark: HNSW m=16, ef_construct=100, float16 vectors in RAM, cosine distance, and
the collection optimized down to one segment ("fully merged") before measuring. Point id i is corpus row i, as in
blowfish. Measure it with `NQ_ENGINE=qdrant QDRANT_EF=64 measure.py ...` (`hnsw_ef` per query).
"""

import argparse
import time
from pathlib import Path

import numpy as np
from qdrant_client import QdrantClient, models


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", type=Path)
    p.add_argument("--url", default="http://127.0.0.1:18110")
    p.add_argument("--collection", default="nq")
    p.add_argument("--batch", type=int, default=1000)
    a = p.parse_args()
    c = QdrantClient(url=a.url, timeout=600, prefer_grpc=False)
    if c.collection_exists(a.collection):
        c.delete_collection(a.collection)
    vecs = np.load(a.data / "corpus.f16", mmap_mode="r")
    c.create_collection(
        a.collection,
        vectors_config=models.VectorParams(
            size=vecs.shape[1], distance=models.Distance.COSINE, datatype=models.Datatype.FLOAT16, on_disk=False
        ),
        hnsw_config=models.HnswConfigDiff(m=16, ef_construct=100),
        # No indexing while uploading; one segment at the end.
        optimizers_config=models.OptimizersConfigDiff(
            indexing_threshold=0, default_segment_number=1, max_segment_size=100_000_000
        ),
    )
    t0 = time.monotonic()
    for s in range(0, len(vecs), a.batch):
        v = np.asarray(vecs[s : s + a.batch], dtype=np.float32)
        c.upsert(
            a.collection,
            points=models.Batch(ids=list(range(s, s + len(v))), vectors=v.tolist()),
            wait=False,
        )
        if s % 100_000 == 0:
            print(f"{s} rows {time.monotonic() - t0:.0f}s", flush=True)
    t1 = time.monotonic()
    print(f"uploaded {len(vecs)} in {t1 - t0:.0f}s", flush=True)
    c.update_collection(a.collection, optimizers_config=models.OptimizersConfigDiff(indexing_threshold=10_000))
    while True:
        info = c.get_collection(a.collection)
        if info.status == models.CollectionStatus.GREEN and (info.indexed_vectors_count or 0) >= len(vecs):
            break
        print(f"indexing: {info.status} {info.indexed_vectors_count} indexed, {info.segments_count} segments", flush=True)
        time.sleep(30)
    print(f"indexed in {time.monotonic() - t1:.0f}s; segments {info.segments_count}", flush=True)


if __name__ == "__main__":
    main()
