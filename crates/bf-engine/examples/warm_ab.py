# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Matched-recall NQ warm throughput: frozen layouts, full query set, and 16 clients.

Usage: uv run crates/bf-engine/examples/warm_ab.py --data DATA --scratch .scratch/RUN \
    --arms ARMS.json --server-cpus 0-7 --client-cpus 8-15
ARMS.json maps names to binary, storage (StorageConfig JSON path), and probes.
Run two independent processes per arm in forward/reverse order. Every process warms
all queries twice before two measured epochs by default. Compare measured recall, never an
interpolated estimate. Storage is always read-only. Delete scratch after recording results.
"""

import argparse
import contextlib
import http.client
import json
import multiprocessing as mp
import os
import signal
import subprocess
import time
from pathlib import Path

import httpx
import numpy as np


def cpus(value):
    result = set()
    for part in value.split(","):
        lo, _, hi = part.partition("-")
        result.update(range(int(lo), int(hi or lo) + 1))
    return result


def client_worker(index, bodies, namespace, start, output, affinity):
    # A canceled parent must be able to reap clients even if a queue feeder is blocked.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        os.sched_setaffinity(0, affinity)
        connection = http.client.HTTPConnection("127.0.0.1", 18443, timeout=300)
        connection.connect()
        start.wait(timeout=120)
        records = []
        for i in range(index, len(bodies), 16):
            before = time.perf_counter_ns()
            connection.request(
                "POST",
                f"/v2/namespaces/{namespace}/query",
                bodies[i],
                {"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            raw = response.read()
            after = time.perf_counter_ns()
            if response.status != 200:
                raise RuntimeError((response.status, raw[:300]))
            records.append((i, before, after, raw))
        connection.close()
        output.put((True, records))
    except BaseException as error:  # noqa: BLE001 -- report child failures to the parent before exiting
        output.put((False, repr(error)))


def epoch(bodies, namespace, affinity):
    context = mp.get_context("fork")
    start, output = context.Barrier(17), context.Queue()
    children = [
        context.Process(
            target=client_worker, args=(i, bodies, namespace, start, output, affinity)
        )
        for i in range(16)
    ]
    try:
        for child in children:
            child.start()
        start.wait(timeout=120)
        rows = []
        for _ in children:
            ok, result = output.get(timeout=600)
            if not ok:
                raise RuntimeError(result)
            rows.extend(result)
        for child in children:
            child.join(timeout=10)
            if child.exitcode != 0:
                raise RuntimeError(f"client exit status {child.exitcode}")
        return sorted(rows)
    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
            child.join()
        output.close()
        output.join_thread()


def compiled_queries(client):
    for line in client.get("/metrics").text.splitlines():
        if line.startswith("blowfish_ann_compiled_queries_total "):
            return int(float(line.split()[1]))
    return 0


@contextlib.contextmanager
def server(arm, directory, affinity):
    config = json.loads(Path(arm["storage"]).read_text())
    config["read_only"] = True
    config["cache"] = {
        "memory_bytes": 4 * 2**30,
        "disk_bytes": 0,
        "block_size": 4000,
        "max_coalesce_blocks": 512,
        "fetch_concurrency": 512,
    }
    storage = directory / "storage.local"
    storage.write_text(json.dumps(config))
    env = dict(
        os.environ,
        RAYON_NUM_THREADS="8",
        TOKIO_WORKER_THREADS="8",
        BF_DECODED_CACHE_BYTES=str(4 * 2**30),
        BF_ANN_NPROBE=str(arm["probes"]),
    )
    with (directory / "server.log").open("wb") as log:
        process = subprocess.Popen(
            [
                "taskset",
                "-c",
                ",".join(map(str, sorted(affinity))),
                str(Path(arm["binary"]).resolve()),
                str(storage),
                "127.0.0.1:18443",
            ],
            env=env,
            stdout=log,
            stderr=log,
        )
        try:
            with httpx.Client(base_url="http://127.0.0.1:18443", timeout=300) as client:
                for _ in range(300):
                    if process.poll() is not None:
                        raise RuntimeError("server exited; inspect server.log")
                    try:
                        client.get("/").raise_for_status()
                        break
                    except httpx.HTTPError:
                        time.sleep(0.1)
                else:
                    raise RuntimeError("readiness timeout")
                yield client, process
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    def stop(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--arms", type=Path, required=True)
    parser.add_argument("--namespace", default="nq")
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--warm-epochs", type=int, default=2)
    parser.add_argument("--population", type=int, default=2_681_468)
    parser.add_argument(
        "--identical-results", action="store_true",
        help="require identical ordered rows and distances across all arms",
    )
    parser.add_argument("--server-cpus", default="0-7")
    parser.add_argument("--client-cpus", default="8-15")
    args = parser.parse_args()
    if args.epochs < 1 or args.warm_epochs < 1 or args.population < 1:
        parser.error("epochs, warm epochs, and population must be positive")
    if ".scratch" not in args.scratch.resolve().parts or args.scratch.exists():
        parser.error("scratch must be a new directory under .scratch/")
    args.scratch.mkdir(parents=True)
    os.sched_setaffinity(0, cpus(args.client_cpus))
    queries = np.load(args.data / "queries.npy")
    truth = np.load(args.data / "truth.npy")
    if len(queries) != len(truth):
        parser.error("queries and truth must have matching lengths")
    bodies = [
        json.dumps(
            {
                "rank_by": ["vector", "ANN", vector.tolist()],
                "top_k": 10,
                "include_attributes": ["id"],
                "consistency": {"level": "strong"},
            }
        ).encode()
        for vector in queries
    ]
    arms = json.loads(args.arms.read_text())
    results = []
    expected_rows = {}
    reference = None
    order = list(arms) + list(reversed(arms))
    for run, name in enumerate(order):
        directory = args.scratch / str(run)
        directory.mkdir()
        with server(arms[name], directory, cpus(args.server_cpus)) as (client, process):
            # Warm complete varied-query epochs; later epochs can use the compiled tier.
            for _ in range(args.warm_epochs):
                epoch(bodies, args.namespace, cpus(args.client_cpus))
            for repeat in range(args.epochs):
                compiled_before = compiled_queries(client)
                rows = epoch(bodies, args.namespace, cpus(args.client_cpus))
                compiled = compiled_queries(client) - compiled_before
                decoded = [json.loads(row[3]) for row in rows]
                result_rows = [result["rows"] for result in decoded]
                ids = [[int(row["id"]) for row in result["rows"]] for result in decoded]
                if name in expected_rows and result_rows != expected_rows[name]:
                    raise RuntimeError(
                        f"ordered rows or distances changed between warm runs of {name}"
                    )
                expected_rows[name] = result_rows
                if args.identical_results:
                    if reference is None:
                        reference = result_rows
                        (args.scratch / "reference.json").write_text(json.dumps(reference))
                    elif result_rows != reference:
                        raise RuntimeError(f"ordered rows or distances differ in arm {name}")
                cache = float(
                    np.mean([r["performance"]["cache_hit_ratio"] for r in decoded])
                )
                tail = {r["performance"]["exhaustive_search_count"] for r in decoded}
                population = {
                    r["performance"]["approx_namespace_size"] for r in decoded
                }
                if cache != 1.0 or tail != {0} or population != {args.population}:
                    raise RuntimeError(
                        f"not a fully warm tier: cache={cache}, tail={tail}, population={population}"
                    )
                seconds = (max(r[2] for r in rows) - min(r[1] for r in rows)) / 1e9
                latency = [(row[2] - row[1]) / 1e6 for row in rows]
                status = Path(f"/proc/{process.pid}/status").read_text()
                peak = (
                    int(
                        next(
                            line.split()[1]
                            for line in status.splitlines()
                            if line.startswith("VmHWM:")
                        )
                    )
                    * 1024
                )
                result = {
                    "arm": name,
                    "run": run,
                    "repeat": repeat,
                    "probes": arms[name]["probes"],
                    "queries": len(rows),
                    "qps": len(rows) / seconds,
                    "p50_ms": float(np.percentile(latency, 50)),
                    "p90_ms": float(np.percentile(latency, 90)),
                    "peak_rss_bytes": peak,
                    "recall": float(
                        np.mean(
                            [
                                len(set(got) & set(t[:10])) / 10
                                for got, t in zip(ids, truth)
                            ]
                        )
                    ),
                    "cache_hit_ratio": cache,
                    "tail_rows": 0,
                    "namespace_rows": args.population,
                    "compiled_queries": compiled,
                }
                (directory / f"{repeat}.ids.json").write_text(json.dumps(ids))
                results.append(result)
                (args.scratch / "results.json").write_text(
                    json.dumps(results, indent=2)
                )
                print(json.dumps(result), flush=True)
            (directory / "metrics.txt").write_text(client.get("/metrics").text)


if __name__ == "__main__":
    main()
