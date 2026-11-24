"""NQ queries during paced upserts; run with uv and timeout, with clients on CPUs 6 and 7.

The server owns a disposable local store. Use identical store copies and ABAB arm order.
This script writes only beneath --out's caller-selected scratch directory.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import base64
import http.client
import json
import multiprocessing as mp
import os
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pyarrow.parquet as pq


def connection(base, timeout=30):
    url = urlparse(base)
    return http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)


def post(conn, path, body):
    conn.request("POST", path, body, {"Content-Type": "application/json"})
    response = conn.getresponse()
    data = response.read()
    if response.status != 200:
        raise RuntimeError(f"HTTP {response.status}: {data[:200]!r}")
    return json.loads(data)


def writer(args, stop, ready, results):
    try:
        os.sched_setaffinity(0, {6, 7})
        vectors = np.load(args.data / "corpus.f16", mmap_mode="r")
        attrs_path = args.data / "attrs.parquet"
        attrs = pq.read_table(attrs_path if attrs_path.exists() else sorted((args.data / "raw/corpus").glob("*.parquet")), columns=["title", "text"])
        title_values, text_values = attrs["title"], attrs["text"]
        rng = np.random.default_rng(17)
        ack, rejected, requests, service = 0, 0, 0, []
        local = threading.local()

        def send(body, count):
            if not hasattr(local, "conn"):
                local.conn = connection(args.base_url)
            before = time.monotonic()
            try:
                post(local.conn, f"/v2/namespaces/{args.namespace}", body)
                return count, 0, time.monotonic() - before
            except RuntimeError as error:
                if "HTTP 429" in str(error):
                    return 0, count, time.monotonic() - before
                raise

        def collect(future):
            nonlocal ack, rejected, requests
            accepted, refused, duration = future.result(timeout=40)
            ack += accepted
            rejected += refused
            requests += 1
            service.append(duration)

        batch_size = args.batch or max(1, args.rate // 10)
        ready.set()
        start = time.monotonic()
        batch, sent = 0, 0
        pending = []
        with ThreadPoolExecutor(4) as pool:
            while not stop.is_set() and time.monotonic() - start < args.seconds and sent < args.rate * args.seconds:
                if len(pending) >= 4:
                    collect(pending.pop(0))
                count = min(batch_size, args.rate * args.seconds - sent)
                ids = rng.choice(len(vectors), count, replace=False)
                values = np.asarray(vectors[ids], dtype="<f4")
                chosen_title = [title_values[int(i)].as_py() for i in ids]
                chosen_text = [text_values[int(i)].as_py() for i in ids]
                body = json.dumps({"upsert_columns": {
                    "id": ids.tolist(),
                    "vector": [base64.b64encode(v.tobytes()).decode() for v in values],
                    "title": [t + f" update {batch}" for t in chosen_title],
                    "text": chosen_text,
                }}).encode()
                pending.append(pool.submit(send, body, count))
                sent += count
                batch += 1
                delay = start + sent / args.rate - time.monotonic()
                if delay > 0:
                    stop.wait(delay)
            for future in pending:
                collect(future)
        results.put({"acknowledged": ack, "rejected": rejected, "offered": sent, "batch_size": batch_size,
                     "write_requests": requests, "elapsed_s": time.monotonic() - start,
                     "write_p50_ms": float(np.percentile(service, 50) * 1000),
                     "write_p90_ms": float(np.percentile(service, 90) * 1000)})
    except BaseException as error:
        ready.set()
        results.put({"error": repr(error)})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("data", type=Path)
    p.add_argument("--base-url", required=True)
    p.add_argument("--namespace", default="nq")
    p.add_argument("--rate", type=int, default=1000)
    p.add_argument("--batch", type=int, default=0, help="rows/request; default targets 10 requests/s")
    p.add_argument("--seconds", type=int, default=180)
    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--tag", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--server-pid", type=int)
    p.add_argument("--query-count", type=int, default=300)
    p.add_argument("--ann-hz", type=float, help="pace queries; omit for closed-loop, or use zero to disable")
    p.add_argument("--bm25-hz", type=float, help="pace queries; omit for closed-loop, or use zero to disable")
    args = p.parse_args()
    os.sched_setaffinity(0, {6, 7})
    vectors = np.load(args.data / "queries.npy")[:args.query_count]
    truth = np.load(args.data / "truth.npy")[:args.query_count, :10]
    texts = pq.read_table(args.data / "raw/queries/test.parquet", columns=["text"])["text"].to_pylist()[:args.query_count]
    bodies = {
        "ann": [json.dumps({"rank_by": ["vector", "ANN", v.tolist()], "top_k": 10, "include_attributes": ["id"]}).encode() for v in vectors],
        "bm25": [json.dumps({"rank_by": ["text", "BM25", t], "top_k": 10, "include_attributes": ["id"]}).encode() for t in texts],
    }
    rates = {"ann": args.ann_hz, "bm25": args.bm25_hz}
    bodies = {name: values for name, values in bodies.items() if rates[name] != 0}
    if not bodies:
        raise ValueError("at least one query rate must be positive")
    conn = connection(args.base_url)
    # Open both plans before writes; the timed window begins after the writer is ready.
    for name in bodies:
        for _ in range(4 if name == "ann" else 1):
            for body in bodies[name] if name == "ann" else bodies[name][:30]:
                post(conn, f"/v2/namespaces/{args.namespace}/query", body)
    context = mp.get_context("spawn")
    stop, ready, results = context.Event(), context.Event(), context.Queue()
    child = context.Process(target=writer, args=(args, stop, ready, results)) if args.rate else None
    if child:
        child.start()
        if not ready.wait(90):
            child.terminate()
            child.join(10)
            raise RuntimeError("writer readiness timed out")
    samples = {name: [] for name in bodies}
    rss = []
    start = time.monotonic()
    failures = []

    def sample(name, hz):
        client = connection(args.base_url, timeout=120)
        index = 0
        try:
            while not stop.is_set() and time.monotonic() - start < args.seconds:
                i = index % len(vectors)
                before = time.monotonic()
                response = post(client, f"/v2/namespaces/{args.namespace}/query", bodies[name][i])
                elapsed = (time.monotonic() - before) * 1000
                overlap = len(set(int(row["id"]) for row in response["rows"]) & set(truth[i].tolist())) / 10 if name == "ann" else None
                samples[name].append({"seconds": time.monotonic() - start, "ms": elapsed, "query": i,
                                      "recall10": overlap, "tail_rows": response["performance"]["exhaustive_search_count"]})
                index += 1
                delay = start + index / hz - time.monotonic() if hz else 0
                if delay > 0:
                    stop.wait(delay)
        except BaseException as error:
            failures.append(repr(error))
            stop.set()
        finally:
            client.close()

    threads = [threading.Thread(target=sample, args=(name, hz))
               for name, hz in rates.items() if name in bodies]
    for thread in threads:
        thread.start()
    try:
        while not stop.is_set() and time.monotonic() - start < args.seconds:
            if args.server_pid:
                try:
                    status = Path(f"/proc/{args.server_pid}/status").read_text()
                    rss.append(int(next(line.split()[1] for line in status.splitlines() if line.startswith("VmRSS:"))))
                except (OSError, StopIteration):
                    pass
            stop.wait(.2)
    finally:
        stop.set()
        for thread in threads:
            thread.join(125)
            if thread.is_alive():
                raise RuntimeError("query sampler did not finish before its deadline")
        if child:
            child.join(40)
            if child.is_alive():
                child.terminate()
                child.join(10)
    if failures:
        raise RuntimeError("; ".join(failures))
    writer_result = results.get(timeout=5) if child else None
    if writer_result and "error" in writer_result:
        raise RuntimeError(writer_result["error"])
    record = {"tag": args.tag, "rate": args.rate, "seconds": args.seconds, "warmup": args.warmup,
              "ann_hz": args.ann_hz, "bm25_hz": args.bm25_hz, "writer": writer_result, "max_rss_kib": max(rss, default=0), "samples": samples}
    for name, rows in samples.items():
        measured = [row for row in rows if row["seconds"] >= args.warmup]
        record[name] = {"queries": len(measured), "p50_ms": float(np.percentile([r["ms"] for r in measured], 50)),
                        "p90_ms": float(np.percentile([r["ms"] for r in measured], 90)),
                        "tail_p50": float(np.percentile([r["tail_rows"] for r in measured], 50)),
                        "tail_max": max(r["tail_rows"] for r in measured)}
        if name == "ann":
            record[name]["recall10"] = float(np.mean([r["recall10"] for r in measured]))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("a") as output:
        output.write(json.dumps(record) + "\n")
    print(json.dumps({key: value for key, value in record.items() if key != "samples"}), flush=True)


if __name__ == "__main__":
    main()
