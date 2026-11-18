# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["numpy==2.5.4", "pyarrow==26.0.1", "httpx==0.28.1", "orjson==3.10.18", "requests", "boto3"]
# ///
"""Load the first 10M English Wikipedia rows (same ids as bench scale_load.py) into an S3 Vectors index."""
import argparse, json, os, time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from pathlib import Path

import httpx, numpy as np, pyarrow.parquet as pq

import s3v

DATASET = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
REVISION = "ade45fb52bd549f5e8c065636fe4160a43c2af36"
HELD = set(range(0, 100_000, 500))  # the 200 query rows, excluded exactly as scale_load.py does
A = None


def vectors(batch):
    col = batch.column(batch.schema.get_field_index("emb"))
    off = col.offsets.to_numpy()
    flat = col.values.slice(int(off[0]), int(off[-1] - off[0]))
    return flat.to_numpy().reshape(len(batch), 1024).astype("<f4", copy=False)


def download(shard, target):
    url = f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/en/{shard:04}.parquet"
    tmp = target.with_suffix(".partial")
    delay = 1
    for attempt in range(8):
        try:
            with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
                r.raise_for_status()
                with tmp.open("wb") as out:
                    for b in r.iter_bytes(1 << 20):
                        out.write(b)
            tmp.replace(target)
            return
        except Exception:
            time.sleep(delay); delay *= 2
    raise RuntimeError(f"download failed {shard}")


def put_retry(client, keys, matrix, stats):
    delay = 0.2
    while True:
        try:
            status, out, dt = client.put(keys, matrix)
        except Exception as e:  # transport
            stats["transport"] += 1; time.sleep(delay); delay = min(10, delay * 1.5); continue
        if status == 200:
            return dt
        if status in (429, 500, 502, 503, 504):
            stats["throttled" if status == 429 else "server_err"] += 1
            time.sleep(delay); delay = min(10, delay * 1.5); continue
        raise RuntimeError(f"PutVectors {status}: {str(out)[:300]}")


def do_shard(job):
    shard, limit, scratch, threads = job
    done = Path(scratch) / f"done-{shard:04}"
    if done.exists():
        return shard, 0, {}, 0.0
    t0 = time.monotonic()
    target = Path(scratch) / f"{shard:04}.parquet"
    download(shard, target)
    stats = {"throttled": 0, "server_err": 0, "transport": 0}
    client = s3v.Client()
    rows = 0
    futures = []
    with ThreadPoolExecutor(threads) as pool:
        offset = 0
        for batch in pq.ParquetFile(target).iter_batches(batch_size=500):
            m = vectors(batch)
            ids = np.arange(shard * 100_000 + offset, shard * 100_000 + offset + len(batch))
            offset += len(batch)
            keep = np.array([int(i) not in HELD for i in ids])
            ids, m = ids[keep], m[keep]
            ids, m = ids[: limit - rows], m[: limit - rows]
            if not len(ids):
                continue
            rows += len(ids)
            futures.append(pool.submit(put_retry, client, [str(int(i)) for i in ids], m, stats))
            while len(futures) > threads * 2:
                futures.pop(0).result()
            if rows >= limit:
                break
        for f in futures:
            f.result()
    target.unlink(missing_ok=True)
    done.write_text(str(rows))
    return shard, rows, stats, time.monotonic() - t0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=10_000_000)
    p.add_argument("--procs", type=int, default=4)
    p.add_argument("--threads", type=int, default=3)
    p.add_argument("--scratch", type=Path, required=True)
    a = p.parse_args()
    a.scratch.mkdir(parents=True, exist_ok=True)
    jobs, remaining, s = [], a.rows, 0
    while remaining > 0:
        n = min(remaining, 100_000 - (len(HELD) if s == 0 else 0))
        jobs.append((s, n, str(a.scratch), a.threads)); remaining -= n; s += 1
    t0 = time.monotonic(); total = 0; agg = {"throttled": 0, "server_err": 0, "transport": 0}
    with Pool(a.procs) as pool:
        for shard, rows, st, secs in pool.imap_unordered(do_shard, jobs):
            total += rows
            for k, v in st.items(): agg[k] += v
            el = time.monotonic() - t0
            print(json.dumps(dict(shard=shard, rows=rows, shard_s=round(secs, 1), loaded_this_run=total,
                                  elapsed_s=round(el), vec_per_s=round(total / el), **agg)), flush=True)
    print(json.dumps({"event": "complete", "elapsed_s": time.monotonic() - t0}), flush=True)


if __name__ == "__main__":
    main()
