# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Measure BM25 locally, with warm clients or fresh processes over delayed local S3.

Run under `timeout 1800 taskset -c 6-7 nice -n 10 uv run ...`. Supply an already
indexed file store, and keep it unchanged during a run. JSONL results go to stdout;
JSON summaries go to stderr. Temporary server logs are removed on exit. Cold
waves use the same idle-gap rule as waves.py; the RTT floor is waves * delay_ms.
The fixed-delay model does not predict S3 tail latency or transfer throughput.
"""
import argparse
import asyncio
import contextlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import tempfile
import time

import httpx

ROOT = Path(__file__).resolve().parents[2]


@contextlib.contextmanager
def process(command, log, env=None):
    with log.open("w") as output:
        child = subprocess.Popen(command, stdout=output, stderr=output, env=env)
        try:
            yield child
        finally:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)


async def ready(client, url):
    for _ in range(120):
        try:
            await client.get(url, timeout=1)
            return
        except httpx.TransportError:
            await asyncio.sleep(0.25)
    raise TimeoutError(f"server readiness timed out: {url}")


def trace(path):
    reads = []
    for line in path.read_text().splitlines():
        if line.startswith("TRACE_READ "):
            _, _, _, _, size, end, elapsed = line.split()
            reads.append((int(end) - int(elapsed), int(end), int(size)))
    waves, previous_end = 0, -1
    for start, end, _ in sorted(reads):
        if start >= previous_end:
            waves += 1
        previous_end = max(previous_end, end)
    return {"reads": len(reads), "bytes": sum(r[2] for r in reads), "waves": waves}


def summary(rows, clients, delay_ms, seconds=None, server_pid=None):
    ms = sorted(row["ms"] for row in rows)
    out = {"queries": len(rows), "clients": clients, "p50_ms": ms[len(ms) // 2],
           "p90_ms": ms[min(len(ms) - 1, len(ms) * 9 // 10)]}
    if seconds is not None:
        out.update(seconds=seconds, queries_per_second=len(rows) / seconds)
    if server_pid is not None:
        try:
            status = Path(f"/proc/{server_pid}/status").read_text()
        except OSError:
            pass
        else:
            for line in status.splitlines():
                if line.startswith("VmHWM:"):
                    out["peak_rss_bytes"] = int(line.split()[1]) * 1024
    if rows and "waves" in rows[0]:
        for key in ["reads", "bytes", "waves"]:
            out[f"mean_{key}"] = statistics.mean(row[key] for row in rows)
        out["round_trip_floor_ms"] = out["mean_waves"] * delay_ms
    print(json.dumps(out), file=sys.stderr, flush=True)


@contextlib.contextmanager
def profile(args, pid, clients):
    """Profile only the measured pass, attaching to this harness's exact child PID."""
    if args.perf is None:
        yield
        return
    args.profile_dir.mkdir(parents=True, exist_ok=True)
    path = args.profile_dir / f"c{clients}"
    command = [str(args.perf)]
    if args.perf_record:
        command += ["record", "-g", "--call-graph", "dwarf,8192", "-F", "199",
                    "-o", str(path.with_suffix(".data"))]
    else:
        command += ["stat", "-e", "instructions,cycles", "-x", ",",
                    "-o", str(path.with_suffix(".stat"))]
    env = dict(os.environ, PERF_BUILDID_DIR=str(args.profile_dir / "buildid"))
    child = subprocess.Popen(["timeout", "600", *command, "-p", str(pid)], env=env)
    try:
        # Allow perf to attach before the first request.
        time.sleep(0.2)
        yield
    finally:
        child.send_signal(signal.SIGINT)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)


async def measure(args, work):
    items = json.loads(args.workload.read_text())[:args.queries]
    if not items:
        raise ValueError("workload is empty")
    env = dict(os.environ, RAYON_NUM_THREADS="2", TOKIO_WORKER_THREADS="2")
    command = [str(args.binary.resolve()), "serve", "--read-only", "--listen", f"127.0.0.1:{args.port}",
               "--cache-memory-bytes", "1073741824", "--cache-block-size", "4000",
               "--cache-max-coalesce-blocks", "512", "--cache-fetch-concurrency", "512"]
    url = f"http://127.0.0.1:{args.port}"
    query_url = f"{url}/v2/namespaces/{args.namespace}/query"
    async with httpx.AsyncClient(timeout=120, limits=httpx.Limits(max_connections=16, max_keepalive_connections=16)) as client:
        async def one(item):
            start = time.monotonic()
            response = await client.post(query_url, json=item["request"])
            response.raise_for_status()
            return {"query_id": item["query_id"], "ms": 1000 * (time.monotonic() - start),
                    "response": response.json()}

        if args.mode == "warm":
            command += ["--storage-url", args.store.resolve().as_uri()]
            with process(command, work / "server.log", env) as server:
                await ready(client, url)
                for item in items:
                    await one(item)
                for clients in args.clients:
                    semaphore = asyncio.Semaphore(clients)

                    async def bounded(item):
                        async with semaphore:
                            return dict(await one(item), clients=clients)

                    with profile(args, server.pid, clients):
                        start = time.monotonic()
                        rows = await asyncio.gather(*(bounded(item) for item in items))
                        seconds = time.monotonic() - start
                    for row in rows:
                        print(json.dumps(row), flush=True)
                    summary(rows, clients, args.delay_ms, seconds, server.pid)
        else:
            env.update(BF_TRACE_READS="1", BF_TRACE_STAGES="1", BF_WARM_CONNECTIONS="0",
                       AWS_ACCESS_KEY_ID="local", AWS_SECRET_ACCESS_KEY="local", AWS_DEFAULT_REGION="us-east-1")
            endpoint = f"http://127.0.0.1:{args.port + 1}"
            command += ["--storage-url", "s3://local", "--storage-endpoint", endpoint,
                        "--storage-region", "us-east-1"]
            proxy = ["uv", "run", "python", str(ROOT / "bench/scale/local_s3.py"), str(args.store.resolve()),
                     "--port", str(args.port + 1), "--delay-ms", str(args.delay_ms)]
            rows = []
            with process(proxy, work / "proxy.log"):
                await ready(client, endpoint)
                for item in items:
                    log = work / "server.log"
                    # Each process also creates fresh HTTP connections to the facade.
                    with process(command, log, env):
                        await ready(client, url)
                        row = await one(item)
                    row.update(trace(log))
                    rows.append(row)
                    print(json.dumps(row), flush=True)
                summary(rows, 1, args.delay_ms)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["warm", "cold"])
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--namespace", default="bm25")
    parser.add_argument("--workload", type=Path, default=ROOT / "bench/scale/data/bm25-msmarco.json")
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--port", type=int, default=18323)
    parser.add_argument("--delay-ms", type=float, default=25)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 16])
    parser.add_argument("--perf", type=Path, help="perf executable; collect measured-pass instruction counts")
    parser.add_argument("--perf-record", action="store_true", help="collect call stacks instead of counters")
    parser.add_argument("--profile-dir", type=Path, help="profile output directory, under .scratch/")
    args = parser.parse_args()
    if args.queries <= 0 or args.delay_ms < 0:
        parser.error("queries must be positive, and delay-ms must be nonnegative")
    if any(c < 1 or c > 16 for c in args.clients):
        parser.error("clients must be between one and sixteen")
    if args.perf is not None and (args.profile_dir is None or
                                 not args.profile_dir.resolve().is_relative_to(ROOT / ".scratch")):
        parser.error("--perf requires --profile-dir under .scratch/")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    scratch = ROOT / ".scratch"
    scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch, prefix="bm25-local-") as work:
        asyncio.run(asyncio.wait_for(measure(args, Path(work)), timeout=1800))


if __name__ == "__main__":
    main()
