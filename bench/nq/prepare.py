"""Turn the downloaded BEIR NQ parquet files into arrays, and compute the exact ground truth.

Usage: uv run python prepare.py DATA_DIR

DATA_DIR holds `raw/` (as `download.sh` leaves it). Writes into DATA_DIR:
- `corpus.f16` (an .npy file of float16, rows x 1,024, in parquet order; row i is passage `_id` line i of
  `corpus_ids.txt`, and blowfish document id i)
- `corpus_ids.txt` (one `_id` per row), `queries.npy` (float32), `query_ids.txt`
- `qrels.json` ({query index: [corpus row, ...]} for the 3,452 test queries)
- `truth.npy` (int32, queries x 100: the exact cosine top 100 over the float16 vectors)
"""

import json
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

DIM = 1024


def corpus_files(data: Path):
    return sorted((data / "raw" / "corpus").glob("*.parquet"))


def write_corpus(data: Path):
    out = data / "corpus.f16"
    ids_out = data / "corpus_ids.txt"
    if out.exists() and ids_out.exists():
        return
    rows = sum(pq.ParquetFile(f).metadata.num_rows for f in corpus_files(data))
    mm = np.lib.format.open_memmap(data / "corpus.f16.npy", "w+", np.float16, (rows, DIM))
    i = 0
    with open(ids_out, "w") as ids:
        for f in corpus_files(data):
            for batch in pq.ParquetFile(f).iter_batches(batch_size=20_000, columns=["_id", "emb"]):
                emb = batch.column("emb")
                flat = emb.values.to_numpy(zero_copy_only=False).astype(np.float32)
                v = flat.reshape(-1, DIM)
                mm[i : i + len(v)] = v.astype(np.float16)
                ids.write("\n".join(batch.column("_id").to_pylist()) + "\n")
                i += len(v)
                print(f"corpus {i}/{rows}", flush=True)
    mm.flush()
    del mm
    (data / "corpus.f16.npy").rename(out)


def corpus(data: Path):
    """The corpus vectors, memory-mapped (`corpus.f16` is an .npy file under another name)."""
    return np.load(data / "corpus.f16", mmap_mode="r", allow_pickle=False)


def write_queries(data: Path):
    t = pq.read_table(data / "raw" / "queries" / "test.parquet")
    q = np.asarray(t.column("emb").to_pylist(), dtype=np.float32)
    np.save(data / "queries.npy", q)
    qids = t.column("_id").to_pylist()
    (data / "query_ids.txt").write_text("\n".join(qids) + "\n")
    row = {d: i for i, d in enumerate(open(data / "corpus_ids.txt").read().split("\n")) if d}
    qi = {d: i for i, d in enumerate(qids)}
    qrels: dict[int, list[int]] = {}
    r = pq.read_table(data / "raw" / "qrels" / "test.parquet").to_pylist()
    for x in r:
        if x["score"] > 0 and x["query_id"] in qi:
            qrels.setdefault(qi[x["query_id"]], []).append(row[x["corpus_id"]])
    (data / "qrels.json").write_text(json.dumps(qrels))
    print(f"queries {len(qids)}, qrels {sum(len(v) for v in qrels.values())}")


def write_truth(data: Path, k: int = 100):
    out = data / "truth.npy"
    if out.exists():
        return
    c = corpus(data)
    q = np.load(data / "queries.npy")
    q /= np.linalg.norm(q, axis=1, keepdims=True)
    best_s = np.full((len(q), k), -np.inf, np.float32)
    best_i = np.zeros((len(q), k), np.int64)
    step = 100_000
    t0 = time.time()
    for s in range(0, len(c), step):
        block = np.asarray(c[s : s + step], dtype=np.float32)
        block /= np.maximum(np.linalg.norm(block, axis=1, keepdims=True), 1e-30)
        sims = q @ block.T
        top = np.argpartition(-sims, k, axis=1)[:, :k]
        cand_s = np.concatenate([best_s, np.take_along_axis(sims, top, 1)], axis=1)
        cand_i = np.concatenate([best_i, top + s], axis=1)
        keep = np.argpartition(-cand_s, k, axis=1)[:, :k]
        best_s = np.take_along_axis(cand_s, keep, 1)
        best_i = np.take_along_axis(cand_i, keep, 1)
        print(f"truth {s + len(block)}/{len(c)} {time.time() - t0:.0f}s", flush=True)
    order = np.argsort(-best_s, axis=1)
    np.save(out, np.take_along_axis(best_i, order, 1).astype(np.int32))


if __name__ == "__main__":
    d = Path(sys.argv[1])
    write_corpus(d)
    write_queries(d)
    write_truth(d)
