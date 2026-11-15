# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "pyarrow==26.0.1", "httpx==0.28.1"]
# ///
"""Stream real Wikipedia vectors, retry backpressure, and maintain exact cosine top-10.

Run with uv. All downloaded files, progress, and truth belong in --scratch.
The Hub endpoint may be a keyless proxy. No credentials are read by this loader.
"""
import argparse
import base64
import json
import signal
import time
from pathlib import Path

import httpx
import numpy as np
import pyarrow.parquet as pq

DATASET = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
REVISION = "ade45fb52bd549f5e8c065636fe4160a43c2af36"


def available():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable is required for the scale run")


def download(client, endpoint, shard, target):
    if target.exists():
        return
    url = f"{endpoint}/datasets/{DATASET}/resolve/{REVISION}/en/{shard:04}.parquet"
    try:
        with client.stream("GET", url, follow_redirects=True) as response:
            response.raise_for_status()
            with target.open("wb") as out:
                for block in response.iter_bytes(1 << 20):
                    out.write(block)
    except BaseException:
        target.unlink(missing_ok=True)
        raise


def vectors(batch):
    col = batch.column(batch.schema.get_field_index("emb"))
    # ListArray offsets may start at a nonzero position in a sliced record batch.
    offsets = col.offsets.to_numpy()
    flat = col.values.slice(int(offsets[0]), int(offsets[-1] - offsets[0]))
    return flat.to_numpy().reshape(len(batch), 1024).astype("<f4", copy=False)


def update_truth(query, matrix, ids, best_scores, best_ids):
    if len(ids) == 0:
        return best_scores, best_ids
    normalized = matrix / np.maximum(np.linalg.norm(matrix, axis=1, keepdims=True), 1e-30)
    scores = query @ normalized.T
    k = min(10, len(ids))
    chosen = np.argpartition(-scores, k - 1, axis=1)[:, :k]
    local = np.take_along_axis(scores, chosen, axis=1)
    joined_scores = np.concatenate((best_scores, local), axis=1)
    joined_ids = np.concatenate((best_ids, ids[chosen]), axis=1)
    keep = np.argsort(-joined_scores, axis=1, kind="stable")[:, :10]
    return np.take_along_axis(joined_scores, keep, axis=1), np.take_along_axis(joined_ids, keep, axis=1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8300")
    parser.add_argument("--hub-endpoint", default="https://huggingface.co")
    parser.add_argument("--namespace", default="wikipedia-10m")
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--batch", type=int, default=2000)
    parser.add_argument("--vector-type", choices=("f32", "f16"), default="f32",
                        help="f16 requires every source value to be exactly representable; no silent quantization")
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--resume", action="store_true", help="Replay from the last completed shard; upserts are idempotent")
    args = parser.parse_args()
    args.scratch.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    retries = pauses = rows = raw_bytes = 0
    transport_retries = 0
    best_scores = np.full((200, 10), -np.inf, dtype=np.float32)
    best_ids = np.full((200, 10), -1, dtype=np.int64)
    filtered = {name: (best_scores.copy(), best_ids.copy()) for name in ("selective", "broad")}
    held_ids = set(range(0, 100_000, 500))
    query = None
    with httpx.Client(timeout=300) as client:
        checkpoint = args.scratch / "truth.npz"
        next_shard = 0
        if args.resume and checkpoint.exists():
            saved = np.load(checkpoint)
            state = {key: saved[key].item() for key in ("rows", "raw_bytes", "next_shard", "seconds", "retries_429", "memory_pauses", "retries_transport")}
            rows, raw_bytes, next_shard = state["rows"], state["raw_bytes"], state["next_shard"]
            start -= state["seconds"]
            retries, pauses = state["retries_429"], state["memory_pauses"]
            transport_retries = state["retries_transport"]
            best_scores, best_ids, query = saved["scores"], saved["ids"], saved["query"]
            filtered = {name: (saved[f"{name}_scores"], saved[f"{name}_ids"]) for name in filtered}
        else:
            first = args.scratch / "0000.parquet"
            download(client, args.hub_endpoint, 0, first)
            held = pq.read_table(first).take(np.array(sorted(held_ids)))
            held_vectors = vectors(held.to_batches()[0])
            query = held_vectors / np.maximum(np.linalg.norm(held_vectors, axis=1, keepdims=True), 1e-30)
            queries = [{"vector": v.tolist(), "text": row["title"], "source_id": i}
                       for i, v, row in zip(sorted(held_ids), held_vectors, held.select(["title"]).to_pylist())]
            (args.scratch / "queries.json").write_text(json.dumps(queries))
            del held, held_vectors
        for shard in range(next_shard, (args.rows + 200 + 99_999) // 100_000):
            target = args.scratch / f"{shard:04}.parquet"
            download(client, args.hub_endpoint, shard, target)
            try:
                offset = 0
                for batch in pq.ParquetFile(target).iter_batches(batch_size=args.batch):
                    matrix = vectors(batch)
                    ids = np.arange(shard * 100_000 + offset, shard * 100_000 + offset + len(batch), dtype=np.int64)
                    offset += len(batch)
                    mask = np.array([int(i) not in held_ids for i in ids])
                    ids, matrix = ids[mask], matrix[mask]
                    keep = min(len(ids), args.rows - rows)
                    ids, matrix = ids[:keep], matrix[:keep]
                    if not keep:
                        break
                    if args.vector_type == "f16" and not np.array_equal(matrix, matrix.astype("f2").astype("f4")):
                        raise ValueError("source vectors are not losslessly representable as f16")
                    text = batch.select(["title", "text"]).to_pydict()
                    positions = np.flatnonzero(mask)[:keep]
                    documents = [{"id": int(i), "vector": base64.b64encode(v.tobytes()).decode(),
                                  "title": text["title"][p], "text": text["text"][p],
                                  "title_initial": (text["title"][p] or "?")[0].lower(),
                                  "text_length": len(text["text"][p] or "")}
                                 for i, v, p in zip(ids, matrix, positions)]
                    body = {"upsert_rows": documents}
                    if rows == 0:
                        body.update(distance_metric="cosine_distance", schema={
                            "text": {"type": "string", "full_text_search": True, "filterable": False},
                            "title": {"type": "string", "filterable": False},
                            "title_initial": "string", "text_length": "int", "vector": f"[1024]{args.vector_type}"})
                    delay = 0.25
                    while True:
                        while available() < 4 * (1 << 30):
                            pauses += 1
                            print(json.dumps({"event": "memory_pause", "available": available()}), flush=True)
                            time.sleep(5)
                        try:
                            response = client.post(f"{args.base_url}/v2/namespaces/{args.namespace}", json=body)
                        except httpx.TransportError:
                            transport_retries += 1
                            time.sleep(delay)
                            delay = min(10, delay * 1.5)
                            continue
                        if response.status_code == 429:
                            retries += 1
                            time.sleep(delay)
                            delay = min(10, delay * 1.5)
                            continue
                        response.raise_for_status()
                        break
                    best_scores, best_ids = update_truth(query, matrix, ids, best_scores, best_ids)
                    for name, selected in {
                        "selective": np.array([d["title_initial"] == "z" for d in documents]),
                        "broad": np.array([d["text_length"] >= 300 for d in documents]),
                    }.items():
                        filtered[name] = update_truth(query, matrix[selected], ids[selected], *filtered[name])
                    raw_bytes += matrix.nbytes + sum(len(d["title"].encode()) + len(d["text"].encode()) + 16 for d in documents)
                    rows += len(ids)
                    if rows % 10_000 < args.batch or rows == args.rows:
                        elapsed = time.monotonic() - start
                        result = dict(rows=rows, seconds=elapsed, docs_per_second=rows / elapsed,
                                      retries_429=retries, retries_transport=transport_retries,
                                      memory_pauses=pauses, raw_bytes=raw_bytes)
                        (args.scratch / "ingest.json").write_text(json.dumps(result, indent=2))
                        print(json.dumps(result), flush=True)
                    if rows == args.rows:
                        break
            finally:
                target.unlink(missing_ok=True)
            state = dict(rows=rows, raw_bytes=raw_bytes, next_shard=shard+1,
                         seconds=time.monotonic()-start, retries_429=retries,
                         memory_pauses=pauses, retries_transport=transport_retries)
            pending = args.scratch / "truth-next.npz"
            np.savez(pending, ids=best_ids, scores=best_scores, query=query, **state,
                     **{f"{name}_{part}": value for name, pair in filtered.items()
                        for part, value in zip(("scores", "ids"), pair)})
            pending.replace(checkpoint)
            (args.scratch / "checkpoint.json").write_text(json.dumps(state))
            if rows == args.rows:
                break
    print(json.dumps({"event": "complete", "rows": rows}), flush=True)


def stop_signal(*_):
    raise SystemExit(143)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_signal)
    main()
