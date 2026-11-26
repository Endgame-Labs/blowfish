"""Prepare six filtered NQ workloads and exact truth from bench/nq's corpus.f16 and queries.npy.

Run with uv --project bench/nq. All generated files must be under the repository .scratch/.
The first 64 distinct queries match the D4 scalar-filter recipe. Truth uses stored f16 values.
"""
import argparse
import json
import math
from pathlib import Path

import numpy as np


def bucket(ids):
    """D4's SplitMix64 buckets, with wrapping uint64 arithmetic."""
    x = np.asarray(ids, dtype=np.uint64) + np.uint64(0x9E3779B97F4A7C15)
    x = (x ^ (x >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    x = (x ^ (x >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return (x ^ (x >> np.uint64(31))) % np.uint64(10000)


def prepare(data, output):
    corpus = np.load(data / "corpus.f16", mmap_mode="r")
    queries = np.load(data / "queries.npy")[:64].astype("f4")
    normalized = queries / np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-30)
    cases = [(attribute, percent) for attribute in ["id", "bucket"] for percent in [1, 10, 50]]
    # Round G14 adds the selective hash filters and the crossover points.
    cases += [("bucket", p) for p in [0.5, 2, 5, 20]] + [("id", 5)]
    best = {}
    for attribute, percent in cases:
        name = f"{attribute}-{percent}"
        limit = math.ceil(len(corpus) * percent / 100) if attribute == "id" else round(100 * percent)
        workloads = [{"rank_by": ["vector", "ANN", vector.tolist()],
                      "filters": [attribute, "Lt", limit], "top_k": 10,
                      "include_attributes": False} for vector in queries]
        (output / f"{name}.json").write_text(json.dumps(workloads))
        best[name] = (np.full((len(queries), 10), -np.inf, dtype="f4"),
                      np.zeros((len(queries), 10), dtype="i8"))
    for start in range(0, len(corpus), 20000):
        vectors = np.asarray(corpus[start:start + 20000], dtype="f4")
        vectors /= np.maximum(np.linalg.norm(vectors, axis=1, keepdims=True), 1e-30)
        similarities = normalized @ vectors.T
        ids = np.arange(start, start + len(vectors), dtype="u8")
        buckets = bucket(ids)
        for attribute, percent in cases:
            name = f"{attribute}-{percent}"
            mask = (ids < math.ceil(len(corpus) * percent / 100) if attribute == "id"
                    else buckets < round(100 * percent))
            if not mask.any():
                continue
            scores = similarities[:, mask]
            count = min(10, scores.shape[1])
            nearest = np.argpartition(-scores, count - 1, axis=1)[:, :count]
            old_scores, old_ids = best[name]
            candidates = np.concatenate((old_scores, np.take_along_axis(scores, nearest, axis=1)), axis=1)
            candidate_ids = np.concatenate((old_ids, ids[mask][nearest]), axis=1)
            keep = np.argsort(-candidates, axis=1)[:, :10]
            best[name] = (np.take_along_axis(candidates, keep, axis=1),
                          np.take_along_axis(candidate_ids, keep, axis=1))
        if start % 200000 == 0:
            print(f"truth {start}/{len(corpus)}", flush=True)
    (output / "truth.json").write_text(json.dumps({name: value[1].tolist() for name, value in best.items()}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    if not args.output.resolve().is_relative_to(root / ".scratch"):
        parser.error("output must be under the repository .scratch/")
    args.output.mkdir(parents=True, exist_ok=True)
    prepare(args.data, args.output)
