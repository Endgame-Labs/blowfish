"""Disposable paired Qdrant 1.20.0 fixture; run with uv and qdrant-client==1.20.0.

Usage: qdrant_load.py DATA REST_PORT GRPC_PORT
The caller owns the server, directory, and cleanup. Float16 RAM vectors, M=16,
ef_construct=100, and one merged segment match the study configuration.
"""
from pathlib import Path
import sys
import time
import numpy as np
from qdrant_client import QdrantClient, models

if __name__ == '__main__':
    data, rest, grpc = sys.argv[1:]
    c = QdrantClient(host='127.0.0.1', port=int(rest), grpc_port=int(grpc),
                     prefer_grpc=True, timeout=600)
    vectors = np.load(Path(data) / 'corpus.f16', mmap_mode='r')
    c.create_collection('nq', vectors_config=models.VectorParams(
        size=vectors.shape[1], distance=models.Distance.COSINE,
        datatype=models.Datatype.FLOAT16, on_disk=False),
        hnsw_config=models.HnswConfigDiff(m=16, ef_construct=100),
        optimizers_config=models.OptimizersConfigDiff(
            indexing_threshold=0, default_segment_number=1, max_segment_size=100_000_000))
    start = time.monotonic()
    c.upload_collection('nq', vectors=vectors, ids=range(len(vectors)),
                        batch_size=1024, parallel=8, wait=True)
    print(f'uploaded rows={len(vectors)} seconds={time.monotonic()-start:.3f}', flush=True)
    c.update_collection('nq', optimizers_config=models.OptimizersConfigDiff(indexing_threshold=10_000))
    while True:
        info = c.get_collection('nq')
        print(f'indexing status={info.status} indexed={info.indexed_vectors_count} segments={info.segments_count} seconds={time.monotonic()-start:.3f}', flush=True)
        if (info.status == models.CollectionStatus.GREEN and
            info.indexed_vectors_count >= len(vectors) and info.segments_count <= 2):
            break
        time.sleep(20)
    c.close()
