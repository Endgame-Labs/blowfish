# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.5", "pyarrow==26.0.1"]
# ///
"""Train and compare PGO release servers on a frozen NQ fixture.

Use with uv. All generated files live under the caller's .scratch directory.
The warm vector loop reuses H1's sixteen-client adapter, body, and timing window.
Every measured query compares complete ordered rows and distances across arms.
CPU-wide PMU passes are separate from unprofiled throughput passes.
"""

import argparse
import base64
import contextlib
import csv
import http.client
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("h1", HERE / "server_tax.py")
h1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h1)


def request(namespace, body, method="POST", suffix="/query"):
    """Make one loopback request and return its decoded, checked response."""
    connection = http.client.HTTPConnection("127.0.0.1", 18100, timeout=600)
    try:
        connection.request(
            method,
            f"/v2/namespaces/{namespace}{suffix}",
            json.dumps(body).encode(),
            {"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        raw = response.read()
        if response.status != 200:
            raise RuntimeError((response.status, raw[:500]))
        return json.loads(raw)
    finally:
        connection.close()


@contextlib.contextmanager
def server(binary, store, directory, writable=False, indexing=False, cold=False):
    """Start an eight-core production server and wait for graceful profile flush."""
    directory.mkdir(parents=True, exist_ok=True)
    env = dict(
        os.environ,
        TOKIO_WORKER_THREADS="8",
        RAYON_NUM_THREADS="8",
        BF_DECODED_CACHE_BYTES=str(4 << 30),
        TMPDIR=str(directory),
    )
    command = [
        "taskset",
        "-c",
        "0-7",
        str(Path(binary).resolve()),
        "serve",
        "--listen",
        "127.0.0.1:18100",
        "--storage-url",
        store.as_uri(),
        "--indexing",
        str(indexing).lower(),
        "--recall-sample-rate",
        "0",
        "--ann-nprobe",
        "48",
        "--ann-rerank-factor",
        "32",
        "--cache-memory-bytes",
        str(4 << 30),
        "--cache-disk-bytes",
        "0",
    ]
    if cold:
        position = command.index("--storage-url")
        command[position + 1] = "s3://local"
        command.extend(
            [
                "--storage-endpoint",
                "http://127.0.0.1:18324",
                "--storage-region",
                "us-east-1",
            ]
        )
        # Dummy values for the loopback facade; no vendor account is used.
        env.update(
            AWS_ACCESS_KEY_ID="local",
            AWS_SECRET_ACCESS_KEY="local",
            AWS_EC2_METADATA_DISABLED="true",
        )
    if not writable:
        command.append("--read-only")
    with (directory / "server.log").open("wb") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        try:
            for _ in range(600):
                if process.poll() is not None:
                    raise RuntimeError(f"server exited: {directory / 'server.log'}")
                connection = http.client.HTTPConnection("127.0.0.1", 18100, timeout=1)
                try:
                    connection.request("GET", "/")
                    if connection.getresponse().status == 200:
                        break
                except OSError:
                    pass
                finally:
                    connection.close()
                time.sleep(0.1)
            else:
                raise TimeoutError("server readiness")
            yield process
        finally:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError("server required SIGKILL; profile may be incomplete")
            if process.returncode != 0:
                raise RuntimeError(f"server exited with {process.returncode}")


@contextlib.contextmanager
def facade(args, label):
    """Model cold object-store round trips, with H1's read-only 25-ms facade."""
    with (args.out / f"{label}-facade.log").open("wb") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                str(h1.ROOT / "bench/scale/local_s3.py"),
                str(args.store),
                "--port",
                "18324",
                "--delay-ms",
                "25",
            ],
            stdout=log,
            stderr=log,
        )
        try:
            time.sleep(1)
            if process.poll() is not None:
                raise RuntimeError("cold facade failed")
            yield
        finally:
            process.terminate()
            process.wait(timeout=10)


def metrics():
    connection = http.client.HTTPConnection("127.0.0.1", 18100, timeout=60)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError("metrics failed")
        return response.read().decode()
    finally:
        connection.close()


def compiled():
    return sum(
        int(float(line.split()[1]))
        for line in metrics().splitlines()
        if line.startswith("blowfish_ann_compiled_queries_total ")
    )


def worker(index, bodies, barrier, output, namespace):
    """H1 timing and client distribution, retaining performance evidence too."""
    client = h1.nq.Client("http://127.0.0.1:18100", namespace)
    try:
        client.conn.connect()
        barrier.wait(timeout=120)
        rows = []
        for i in range(index, len(bodies), 16):
            start = time.perf_counter_ns()
            raw = client.query(bodies[i])
            end = time.perf_counter_ns()
            rows.append((i, start, end, raw))
        output.put(
            (True, [(i, start, end, json.loads(raw)) for i, start, end, raw in rows])
        )
    except Exception as error:  # noqa: BLE001 -- report child failures to the parent
        output.put((False, repr(error)))
    finally:
        client.conn.close()


def epoch(bodies, namespace):
    """Use the H1 process clients, keeping decoding outside each HTTP timer."""
    ctx = h1.mp.get_context("fork")
    barrier, output = ctx.Barrier(17), ctx.Queue()
    children = [
        ctx.Process(target=worker, args=(i, bodies, barrier, output, namespace))
        for i in range(16)
    ]
    try:
        for child in children:
            child.start()
        barrier.wait(timeout=120)
        rows = []
        for _ in children:
            ok, result = output.get(timeout=600)
            if not ok:
                raise RuntimeError(result)
            rows.extend(result)
        return sorted(rows)
    finally:
        for child in children:
            child.join(timeout=5)
            if child.is_alive():
                child.terminate()
                child.join(timeout=5)
            if child.is_alive():
                child.kill()
                child.join()
        output.close()
        output.join_thread()


def validate_warm(rows, compiled_count):
    """Reject any incomplete, cache-cold, tail-bearing, or wrong-population epoch."""
    if compiled_count != len(rows):
        raise RuntimeError(f"compiled hits {compiled_count}/{len(rows)}")
    for row in rows:
        performance = row[3]["performance"]
        if (
            performance["cache_hit_ratio"] != 1
            or performance["exhaustive_search_count"] != 0
            or performance["approx_namespace_size"] != 2681468
        ):
            raise RuntimeError(f"invalid warm fixture: {performance}")


def read_perf(path, queries, expected):
    """Normalize CPU-wide counters, rejecting unsupported or multiplexed events."""
    counters = {}
    for fields in csv.reader(path.read_text().splitlines()):
        if len(fields) < 5 or fields[2] not in expected:
            continue
        event = fields[2]
        try:
            count, scheduled = float(fields[0]), float(fields[4])
        except ValueError as error:
            raise RuntimeError(f"unavailable counter: {event}") from error
        if scheduled < 99.9 or count <= 0 or event in counters:
            raise RuntimeError(f"invalid counter coverage: {fields}")
        counters[event] = {"per_query": count / queries, "scheduled_percent": scheduled}
    if set(counters) != set(expected):
        raise RuntimeError(f"missing counters: {set(expected) - set(counters)}")
    return counters


def bodies_for(args):
    queries = np.load(args.data / "queries.npy")
    vector = [h1.nq.body(v, 10) for v in queries]
    filtered = []
    for i, body in enumerate(vector[:1024]):
        value = json.loads(body)
        value["filters"] = ["category", "Eq", i % 8]
        filtered.append(json.dumps(value).encode())
    text = [
        json.dumps(
            {
                "rank_by": ["text", "BM25", term],
                "top_k": 10,
                "include_attributes": ["id"],
            }
        ).encode()
        for term in [
            "history of the world",
            "united states president",
            "science and technology",
            "river city",
            "film music",
            "university school",
            "first world war",
            "national park",
        ]
    ]
    return {
        "vector": ("nq", vector),
        "filtered": ("small", filtered),
        "bm25": ("small", text * 128),
    }


def make_small(args, binary):
    """Build an indexed 10K-row NQ text, category, and vector regression fixture."""
    import pyarrow.parquet as pq

    store = args.small_store
    store.mkdir()
    vectors = np.load(args.data / "corpus.f16", mmap_mode="r")
    texts = (
        next(
            pq.ParquetFile(
                min((args.data / "raw/corpus").glob("*.parquet"))
            ).iter_batches(batch_size=10000, columns=["text"])
        )
        .column("text")
        .to_pylist()
    )
    with server(binary, store, args.out / "small-build", writable=True, indexing=True):
        for start in range(0, 10000, 1000):
            rows = [
                {
                    "id": i,
                    "category": i % 8,
                    "text": texts[i],
                    "vector": base64.b64encode(
                        np.asarray(vectors[i], dtype="<f4").tobytes()
                    ).decode(),
                }
                for i in range(start, start + 1000)
            ]
            body = {
                "upsert_rows": rows,
                "distance_metric": "cosine_distance",
                "schema": {
                    "vector": {"type": "[1024]f16"},
                    "category": {"type": "uint"},
                    "text": {"type": "string", "full_text_search": True},
                },
            }
            request("small", body, suffix="")
        for _ in range(600):
            body = {"rank_by": ["id", "asc"], "top_k": 1, "include_attributes": ["id"]}
            result = request("small", body)
            if result["performance"]["exhaustive_search_count"] == 0:
                break
            time.sleep(1)
        else:
            raise TimeoutError("small fixture indexing")
        (args.out / "small-metrics.txt").write_text(metrics())


def write_batches(binary, args, label, count=4096):
    store = args.out / (label + "-write-store")
    store.mkdir()
    try:
        with server(binary, store, args.out / (label + "-writes"), writable=True):
            # Disjoint IDs produce real WAL writes, rather than no-op updates.
            vectors = np.load(args.data / "queries.npy")
            rows = [
                {
                    "id": i,
                    "category": i % 8,
                    "vector": vectors[i % len(vectors)].tolist(),
                    "text": "history science city music",
                }
                for i in range(count)
            ]
            schema = {
                "vector": {"type": "[1024]f16"},
                "text": {"type": "string", "full_text_search": True},
            }
            request("writes", {"schema": schema, "upsert_rows": rows[:16]}, suffix="")
            start = time.perf_counter()
            outputs = []
            for i in range(16, count, 16):
                result = request("writes", {"upsert_rows": rows[i : i + 16]}, suffix="")
                result.pop("performance", None)
                outputs.append(result)
            elapsed = time.perf_counter() - start
            return {"qps": (count // 16 - 1) / elapsed, "rows": outputs}
    finally:
        shutil.rmtree(store)


def train(args):
    binary = os.environ["BF_PGO_BINARY"]
    workloads = bodies_for(args)
    with server(binary, args.store, args.out / "train-vector"):
        # Dominant production path: warm full-NQ HTTP, with sixteen clients on eight cores.
        for _ in range(6):
            epoch(workloads["vector"][1], "nq")
    if args.training == "mixed":
        with server(binary, args.small_store, args.out / "train-small"):
            for key in ("filtered", "bm25"):
                namespace, bodies = workloads[key]
                epoch(bodies, namespace)
        write_batches(binary, args, "train", count=512)
        with facade(args, "train"):
            for i in range(8):
                with server(
                    binary, args.store, args.out / f"train-cold-{i}", cold=True
                ):
                    request("nq", json.loads(workloads["vector"][1][i]))


def measure(args):
    arms = json.loads(args.arms.read_text())
    workloads = bodies_for(args)
    reference = {}
    records = []
    # ABBA for two arms, or forward/reverse for sensitivity with three arms.
    for run, arm in enumerate(list(arms) + list(reversed(arms))):
        for key in args.workloads.split(","):
            binary = arms[arm]
            label = f"{run}-{arm}-{key}"
            if key == "write":
                result = write_batches(binary, args, label)
                if key in reference and result["rows"] != reference[key]:
                    raise RuntimeError(f"write response mismatch: {arm}")
                reference[key] = result.pop("rows")
                record = dict(arm=arm, run=run, workload=key, **result)
                records.append(record)
                print(json.dumps(record), flush=True)
                continue
            if key == "cold":
                latencies, rows = [], []
                with facade(args, label):
                    for i in range(args.cold_queries):
                        with server(
                            binary, args.store, args.out / f"{label}-{i}", cold=True
                        ):
                            start = time.perf_counter()
                            result = request(
                                "nq", json.loads(workloads["vector"][1][i])
                            )
                            latencies.append((time.perf_counter() - start) * 1000)
                            rows.append(result["rows"])
                if key in reference and rows != reference[key]:
                    raise RuntimeError(f"cold result mismatch: {arm}")
                reference[key] = rows
                record = {
                    "arm": arm,
                    "run": run,
                    "workload": key,
                    "p50_ms": float(np.median(latencies)),
                    "latencies_ms": latencies,
                }
                records.append(record)
                print(json.dumps(record), flush=True)
                continue
            namespace, bodies = workloads[key]
            store = args.store if key == "vector" else args.small_store
            with server(binary, store, args.out / label) as process:
                for _ in range(args.warm_epochs):
                    epoch(bodies, namespace)
                for rep in range(args.epochs):
                    before_compiled = compiled()
                    perf = None
                    try:
                        if args.perf_events:
                            perf = subprocess.Popen(
                                [
                                    "sudo",
                                    "perf",
                                    "stat",
                                    "-x,",
                                    "-C",
                                    "0-7",
                                    "-e",
                                    args.perf_events,
                                    "-o",
                                    str(args.out / f"{label}-{rep}.perf"),
                                ],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                env=dict(os.environ, LC_ALL="C"),
                            )
                            time.sleep(0.15)
                        before_cpu = h1.process_cpu_seconds(process.pid)
                        rows = epoch(bodies, namespace)
                        after_cpu = h1.process_cpu_seconds(process.pid)
                    finally:
                        if perf:
                            perf.send_signal(signal.SIGINT)
                            perf.wait(timeout=10)
                            if perf.returncode not in (0, -signal.SIGINT, 130):
                                raise RuntimeError("perf failed")
                    compiled_count = compiled() - before_compiled
                    result_rows = [r[3]["rows"] for r in rows]
                    if key in reference and reference[key] != result_rows:
                        raise RuntimeError(
                            f"ordered row/distance mismatch: {arm} {key}"
                        )
                    reference[key] = result_rows
                    if key == "vector":
                        validate_warm(rows, compiled_count)
                    seconds = (max(r[2] for r in rows) - min(r[1] for r in rows)) / 1e9
                    latency = [(r[2] - r[1]) / 1e6 for r in rows]
                    record = {
                        "arm": arm,
                        "run": run,
                        "rep": rep,
                        "workload": key,
                        "queries": len(rows),
                        "qps": len(rows) / seconds,
                        "p50_ms": float(np.median(latency)),
                        "compiled": compiled_count,
                        "cpu_user_us": (after_cpu[0] - before_cpu[0]) * 1e6 / len(rows),
                        "cpu_system_us": (after_cpu[1] - before_cpu[1])
                        * 1e6
                        / len(rows),
                    }
                    if args.perf_events:
                        record["pmu"] = read_perf(
                            args.out / f"{label}-{rep}.perf",
                            len(rows),
                            args.perf_events.strip("{}").split(","),
                        )
                    records.append(record)
                    (args.out / "results.json").write_text(
                        json.dumps(records, indent=2)
                    )
                    print(json.dumps(record), flush=True)
        (args.out / "results.json").write_text(json.dumps(records, indent=2))
    for key, rows in reference.items():
        (args.out / f"{key}.rows.json").write_text(json.dumps(rows))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["fixture", "train", "measure"])
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--small-store", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--binary")
    parser.add_argument("--arms", type=Path)
    parser.add_argument("--training", choices=["vector", "mixed"], default="mixed")
    parser.add_argument("--workloads", default="vector,filtered,bm25,write,cold")
    parser.add_argument("--epochs", type=int, default=8)
    parser.add_argument("--warm-epochs", type=int, default=3)
    parser.add_argument("--cold-queries", type=int, default=20)
    parser.add_argument("--perf-events", default="")
    args = parser.parse_args()
    for field in ("data", "store", "small_store", "out"):
        path = getattr(args, field).resolve()
        if ".scratch" not in path.parts:
            parser.error(f"{field} must be under .scratch")
        setattr(args, field, path)
    if args.out.exists():
        parser.error("output directory must be new")
    if args.epochs < 1 or args.warm_epochs < 1 or args.cold_queries < 1:
        parser.error("epoch and query counts must be positive")
    args.out.mkdir(parents=True)
    os.sched_setaffinity(0, set(range(8, 16)))
    if args.mode == "fixture":
        if not args.binary:
            parser.error("fixture needs --binary")
        make_small(args, args.binary)
    elif args.mode == "train":
        train(args)
    else:
        if not args.arms:
            parser.error("measure needs --arms")
        measure(args)


if __name__ == "__main__":
    main()
