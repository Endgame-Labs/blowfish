"""Full-NQ paired Q2 harness, using the standard bench/nq HTTP adapter and bodies."""

import argparse
import concurrent.futures
import importlib.util
import multiprocessing as mp
import queue
import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("nq", ROOT / "bench/nq/measure.py")
nq = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nq)


def worker(k, bodies, barrier, out, namespace):
    client = nq.Client("http://127.0.0.1:18100", namespace)
    try:
        client.conn.connect()
        barrier.wait(timeout=120)
        rows = []
        for i in range(k, len(bodies), 16):
            start = time.perf_counter_ns()
            raw = client.query(bodies[i])
            end = time.perf_counter_ns()
            rows.append((i, start, end, raw))
        out.put(
            (
                True,
                [
                    (i, start, end, json.loads(raw)["rows"])
                    for i, start, end, raw in rows
                ],
            )
        )
    except Exception as error:
        out.put((False, repr(error)))
    finally:
        client.conn.close()


def run(a):
    queries = np.load(a.data / "queries.npy")
    truth = np.load(a.data / "truth.npy")
    if a.count:
        queries, truth = queries[: a.count], truth[: a.count]
    if len(queries) < 16:
        raise ValueError("at least sixteen queries are required")
    if a.same_query:
        queries, truth = (
            np.repeat(queries[:1], len(queries), axis=0),
            np.repeat(truth[:1], len(truth), axis=0),
        )
    bodies = [nq.body(v, a.top_k) for v in queries]
    if a.order:
        bodies = [
            json.dumps(
                {
                    "rank_by": ["id", "asc"],
                    "top_k": a.top_k,
                    "include_attributes": ["id"],
                }
            ).encode()
        ] * len(queries)
    if a.attrs != "id":

        def project(raw):
            body = json.loads(raw)
            body["include_attributes"] = a.attrs.split(",")
            return json.dumps(body).encode()

        bodies = [project(raw) for raw in bodies]
    if a.query_repeats != 1:
        bodies *= a.query_repeats
        truth = np.tile(truth, (a.query_repeats, 1))
    local = threading.local()

    def query(item):
        i, b = item
        if not hasattr(local, "client"):
            local.client = nq.Client("http://127.0.0.1:18100", a.namespace)
        start = time.perf_counter_ns()
        raw = local.client.query(b)
        end = time.perf_counter_ns()
        rows = json.loads(raw)["rows"]
        return i, start, end, rows

    os.sched_setaffinity(0, {int(cpu) for cpu in a.client_cpus.split(",")})
    # Warm with the standard adapter, and measure on separate client processes.
    with concurrent.futures.ThreadPoolExecutor(16) as pool:
        if not a.cold:
            for _ in range(a.warm_epochs):
                list(pool.map(query, enumerate(bodies)))
    ctx = mp.get_context("fork")
    result = []
    for rep in range(a.repeats):
        barrier = ctx.Barrier(17)
        out = ctx.Queue()
        processes = [
            ctx.Process(target=worker, args=(k, bodies, barrier, out, a.namespace))
            for k in range(16)
        ]
        perf = None
        try:
            for process in processes:
                process.start()
            if a.pid:
                perf = subprocess.Popen(
                    [
                        "sudo",
                        "perf",
                        "stat",
                        "-x,",
                        "-e",
                        a.perf_events,
                        "-p",
                        str(a.pid),
                        "-o",
                        str(a.out.with_suffix(f".{rep}.perf")),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                time.sleep(0.15)
            start = time.perf_counter_ns()
            barrier.wait(timeout=120)
            rows, received = [], 0
            while received < len(processes):
                try:
                    ok, result_rows = out.get(timeout=1)
                except queue.Empty:
                    if any(p.exitcode not in (None, 0) for p in processes):
                        raise RuntimeError("benchmark client exited without a result")
                    continue
                if not ok:
                    raise RuntimeError(result_rows)
                rows.extend(result_rows)
                received += 1
            rows.sort()
            elapsed = (max(r[2] for r in rows) - min(r[1] for r in rows)) / 1e9
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join(timeout=10)
                if process.is_alive():
                    process.kill()
                    process.join()
            out.close()
            out.join_thread()
            if perf:
                perf.send_signal(signal.SIGINT)
                try:
                    perf.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    perf.kill()
                    perf.wait()
        ids = [r[3] for r in rows]
        if a.baseline:
            expected = json.loads(a.baseline.read_text())
            if a.query_repeats != 1 and len(expected) == len(queries):
                expected *= a.query_repeats
            assert ids == expected, "ordered ID/distance mismatch"
        a.out.with_suffix(".ids").write_text(json.dumps(ids))
        ms = [(r[2] - r[1]) / 1e6 for r in rows]
        recall = np.mean(
            [
                len(set(x["id"] for x in got[:10]) & set(t[:10])) / 10
                for got, t in zip(ids, truth)
            ]
        )
        record = dict(
            queries=len(rows),
            qps=len(rows) / elapsed,
            mean=float(np.mean(ms)),
            p50=float(np.percentile(ms, 50)),
            p90=float(np.percentile(ms, 90)),
            recall=float(recall),
            rep=rep,
        )
        result.append(record)
        print(json.dumps(record), flush=True)
        a.out.write_text(json.dumps(result, indent=2))
        a.out.with_suffix(".arrivals").write_text(
            json.dumps(sorted((r[1] - start, r[0]) for r in rows))
        )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--baseline", type=Path)
    p.add_argument("--count", type=int, default=0)
    p.add_argument(
        "--query-repeats",
        type=int,
        default=1,
        help="repeat the selected query sequence for longer counter and cycle samples",
    )
    p.add_argument("--warm-epochs", type=int, default=3)
    p.add_argument("--order", action="store_true")
    p.add_argument(
        "--same-query",
        action="store_true",
        help="repeat the first vector to isolate a compiled resident working set",
    )
    p.add_argument("--namespace", default="nq")
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--attrs", default="id")
    p.add_argument("--client-cpus", default="8,9,10,11,12,13,14,15")
    p.add_argument(
        "--perf-events", default="instructions,cycles,context-switches,cpu-migrations"
    )
    p.add_argument("--pid", type=int)
    p.add_argument("--repeats", type=int, default=4)
    p.add_argument("--cold", action="store_true")
    args = p.parse_args()
    if not args.out.resolve().is_relative_to(ROOT / ".scratch"):
        p.error("--out must be under the repository .scratch directory")
    if min(args.repeats, args.warm_epochs + 1, args.query_repeats) <= 0 or args.count < 0:
        p.error("invalid repeat, warm-up, or query count")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    run(args)
