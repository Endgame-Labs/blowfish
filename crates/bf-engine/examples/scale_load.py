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
import contextlib
import gc
import json
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import numpy as np
import pyarrow.parquet as pq

DATASET = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
REVISION = "ade45fb52bd549f5e8c065636fe4160a43c2af36"
# Shard order. English first (415 shards, about 41M rows), then these languages in this fixed order
# (shard counts at REVISION). A "unit" is one (language, shard); its index in this list is the resume
# checkpoint, and English unit i is shard i, so checkpoints from English-only runs stay valid.
LANGUAGES = (("en", 415), ("de", 208), ("fr", 179), ("es", 130), ("ru", 138), ("ja", 67))
# Document ids: language index * 10^9 + shard * 100,000 + row in shard. English (index 0) ids are the
# original shard * 100,000 + row, so the first 10M rows and oracle-10m.npz are unchanged. A shard holds
# at most 100,000 rows (checked), so ids are unique and stable.
LANGUAGE_ID_STRIDE = 1_000_000_000
SHARD_ROWS = 100_000
UNITS = [(index, language, shard) for index, (language, count) in enumerate(LANGUAGES) for shard in range(count)]


def unit_path(scratch, unit):
    index, language, shard = unit
    return scratch / (f"{shard:04}.parquet" if index == 0 else f"{language}-{shard:04}.parquet")


def available():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable is required for the scale run")


def download(client, endpoint, unit, target):
    if target.exists():
        return
    _, language, shard = unit
    url = f"{endpoint}/datasets/{DATASET}/resolve/{REVISION}/{language}/{shard:04}.parquet"
    pending = target.with_suffix(".partial")
    try:
        with client.stream("GET", url, follow_redirects=True) as response:
            response.raise_for_status()
            with pending.open("wb") as out:
                for block in response.iter_bytes(1 << 20):
                    out.write(block)
        pending.replace(target)
    except BaseException:
        pending.unlink(missing_ok=True)
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


def send(client, args, body, counters):
    """POST one batch, retrying backpressure and transport errors."""
    delay = 0.25
    while True:
        # Transport exceptions and closed responses can form cycles retaining a complete
        # serialized request body.
        gc.collect()
        while available() < args.min_available_gib * (1 << 30):
            counters["pauses"] += 1
            print(json.dumps({"event": "memory_pause", "available": available()}), flush=True)
            time.sleep(5)
        try:
            response = client.post(f"{args.base_url}/v2/namespaces/{args.namespace}", json=body)
        except httpx.TransportError:
            counters["transport"] += 1
            time.sleep(delay)
            delay = min(10, delay * 1.5)
            continue
        if response.status_code == 429:
            counters["retries"] += 1
            response.close()
            del response
            time.sleep(delay)
            delay = min(10, delay * 1.5)
            continue
        response.raise_for_status()
        # turbopuffer reports billing on writes; blowfish does not. Sum what is there.
        with contextlib.suppress(ValueError):
            billing = response.json().get("billing") or {}
            for name, value in billing.items():
                if isinstance(value, (int, float)):
                    counters["billing"][name] = counters["billing"].get(name, 0) + value
        return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8300")
    parser.add_argument("--hub-endpoint", default="https://huggingface.co")
    parser.add_argument("--namespace", default="wikipedia-10m")
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--batch", type=int, default=2000)
    parser.add_argument("--vector-type", choices=("f32", "f16"), default="f32",
                        help="f16 requires every source value to be exactly representable; no silent quantization")
    parser.add_argument("--inflight", type=int, default=2, help="concurrent writes; the server group-commits them")
    parser.add_argument("--min-available-gib", type=float, default=4.0,
                        help="pause while MemAvailable is below this (lower it on small machines)")
    parser.add_argument("--stop-after-units", type=int, default=None,
                        help="stop cleanly after this many (language, shard) units are loaded in total (resumable)")
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--resume", action="store_true", help="Replay from the last completed shard; upserts are idempotent")
    args = parser.parse_args()
    if args.rows <= 0 or args.batch <= 0:
        parser.error("rows and batch must be positive")
    args.scratch.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    rows = raw_bytes = 0
    counters = {"retries": 0, "transport": 0, "pauses": 0, "billing": {}}
    inflight = []
    pool = ThreadPoolExecutor(max_workers=args.inflight)
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
            counters.update(retries=state["retries_429"], pauses=state["memory_pauses"], transport=state["retries_transport"])
            best_scores, best_ids, query = saved["scores"], saved["ids"], saved["query"]
            if "billing" in saved.files:
                counters["billing"] = json.loads(str(saved["billing"]))
            filtered = {name: (saved[f"{name}_scores"], saved[f"{name}_ids"]) for name in filtered}
        else:
            first = args.scratch / "0000.parquet"
            download(client, args.hub_endpoint, UNITS[0], first)
            held = pq.read_table(first).take(np.array(sorted(held_ids)))
            held_vectors = vectors(held.to_batches()[0])
            query = held_vectors / np.maximum(np.linalg.norm(held_vectors, axis=1, keepdims=True), 1e-30)
            queries = [{"vector": v.tolist(), "text": row["title"], "source_id": i}
                       for i, v, row in zip(sorted(held_ids), held_vectors, held.select(["title"]).to_pylist())]
            (args.scratch / "queries.json").write_text(json.dumps(queries))
            del held, held_vectors
        prefetch = None  # (target, future) for the next unit, downloaded while this one loads
        fetcher = ThreadPoolExecutor(max_workers=1)
        for unit_index in range(next_shard, len(UNITS)):
            if rows >= args.rows:
                break
            unit = UNITS[unit_index]
            lang_index, _, shard = unit
            id_base = lang_index * LANGUAGE_ID_STRIDE + shard * SHARD_ROWS
            target = unit_path(args.scratch, unit)
            if prefetch and prefetch[0] == target:
                prefetch[1].result()
            else:
                download(client, args.hub_endpoint, unit, target)
            prefetch = None
            try:
                offset = 0
                for batch in pq.ParquetFile(target).iter_batches(batch_size=args.batch):
                    matrix = vectors(batch)
                    ids = np.arange(id_base + offset, id_base + offset + len(batch), dtype=np.int64)
                    offset += len(batch)
                    if offset > SHARD_ROWS:
                        raise ValueError(f"shard {unit} has more than {SHARD_ROWS} rows; ids would collide")
                    mask = np.array([int(i) not in held_ids for i in ids])
                    ids, matrix = ids[mask], matrix[mask]
                    keep = min(len(ids), args.rows - rows)
                    ids, matrix = ids[:keep], matrix[:keep]
                    if not keep:
                        if rows == args.rows:
                            break
                        continue
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
                            "title_initial": "string", "text_length": "int", "vector": {"type": f"[1024]{args.vector_type}", "ann": True}})  # real turbopuffer requires ann:true
                    if prefetch is None and rows + keep < args.rows and unit_index + 1 < len(UNITS):
                        following = UNITS[unit_index + 1]
                        following_path = unit_path(args.scratch, following)
                        prefetch = (following_path, fetcher.submit(download, client, args.hub_endpoint, following, following_path))
                    # Overlap the (network-bound) write with the truth computation and the next
                    # batch's preparation. The schema-carrying first write completes before any other.
                    while len(inflight) >= args.inflight:
                        inflight.pop(0).result()
                    inflight.append(pool.submit(send, client, args, body, counters))
                    if rows == 0:
                        inflight.pop(0).result()
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
                                      retries_429=counters["retries"], retries_transport=counters["transport"],
                                      memory_pauses=counters["pauses"], raw_bytes=raw_bytes, billing=counters["billing"])
                        (args.scratch / "ingest.json").write_text(json.dumps(result, indent=2))
                        print(json.dumps(result), flush=True)
                    # Release the previous request body and Arrow view before the
                    # next batch is decoded. HTTPX responses retain their requests.
                    del body, documents, matrix, batch, text, positions, ids
                    gc.collect()
                    if rows == args.rows:
                        break
            finally:
                while inflight:
                    inflight.pop(0).result()
                target.unlink(missing_ok=True)
            state = dict(rows=rows, raw_bytes=raw_bytes, next_shard=unit_index + 1,
                         seconds=time.monotonic()-start, retries_429=counters["retries"],
                         memory_pauses=counters["pauses"], retries_transport=counters["transport"])
            pending = args.scratch / "truth-next.npz"
            np.savez(pending, ids=best_ids, scores=best_scores, query=query, billing=json.dumps(counters["billing"]), **state,
                     **{f"{name}_{part}": value for name, pair in filtered.items()
                        for part, value in zip(("scores", "ids"), pair)})
            pending.replace(checkpoint)
            (args.scratch / "checkpoint.json").write_text(json.dumps(state))
            if rows == args.rows:
                break
            if args.stop_after_units is not None and unit_index + 1 >= args.stop_after_units:
                print(json.dumps({"event": "stopped", "rows": rows, "billing": counters["billing"]}), flush=True)
                break
    if prefetch:
        with contextlib.suppress(Exception):
            prefetch[1].result()
        prefetch[0].unlink(missing_ok=True)
    fetcher.shutdown()
    print(json.dumps({"event": "complete", "rows": rows}), flush=True)


def stop_signal(*_):
    raise SystemExit(143)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_signal)
    main()
