# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1", "numpy==2.5.4"]
# ///
"""Bounded in-region runs using ab.py's cold path and warm.py's query method.

Set SCALE_CONFIG and SCALE_SCRATCH as for ab.py. Run under a systemd scope with
MemoryMax=10G for the 100M test. The executable must come from the release tag;
this harness never enables writes. Raw results stay under .scratch/.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import contextlib
import json
import os
from pathlib import Path
import shutil
import signal
import time

import httpx
import numpy as np

import ab


def overlap(item, response):
    """Recall or overlap against independently supplied exact top-k IDs."""
    truth = item.get("truth")
    if not truth:
        return None
    return len({int(row["id"]) for row in response.get("rows", [])} & set(truth)) / len(truth)


def summarize(rows, seconds):
    """Use the same NumPy percentiles as the existing in-region harnesses."""
    latency = [row["ms"] for row in rows]
    result = {"n": len(rows), "seconds": seconds, "qps": len(rows) / seconds,
              **{f"p{p}_ms": float(np.percentile(latency, p)) for p in (50, 90, 99)}}
    for key in ("recall", "reads", "mb", "waves"):
        values = [row[key] for row in rows if row.get(key) is not None]
        if values:
            result[key] = float(np.mean(values))
    return result


def peak_rss(proc):
    """Read the benchmark child's resident-memory high-water mark on Linux."""
    status = Path(f"/proc/{proc.pid}/status").read_text()
    return next(int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith("VmHWM:"))


@contextlib.contextmanager
def server(arm):
    proc = ab.start_server(arm, append_log=True)
    try:
        yield proc
    finally:
        ab.stop_server(proc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["fresh", "cachecold", "warm"])
    parser.add_argument("--binary", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probes", type=int, default=64)
    parser.add_argument("--queries", type=int, default=50)
    parser.add_argument("--clients", type=int, choices=[1, 16], default=1)
    args = parser.parse_args()
    if ".scratch" not in args.output.resolve().parts:
        parser.error("output must stay under .scratch/")
    if args.queries <= 0:
        parser.error("queries must be positive")
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(143)))
    ab.NS = args.namespace
    arm = ("v014", (args.binary, {"BF_ANN_NPROBE": str(args.probes)}))
    items = json.loads(args.workload.read_text())
    if args.mode != "warm" and args.queries > len(items):
        parser.error("cold queries must have distinct workload inputs")
    rows = []
    memory_peak = 0
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def cold(item):
        row, metrics, (reads, mb, waves) = ab.query_once(item)
        row.update(reads=reads, mb=mb, waves=waves, recall=overlap(item, row["response"]))
        row["object_metrics"] = metrics
        return row

    try:
        start = time.monotonic()
        if args.mode == "fresh":
            for item in items[:args.queries]:
                with server(arm) as proc:
                    rows.append(cold(item))
                    memory_peak = max(memory_peak, peak_rss(proc))
                print(json.dumps({"completed": len(rows), "ms": rows[-1]["ms"]}), flush=True)
        elif args.mode == "cachecold":
            with server(arm) as proc, httpx.Client(timeout=600) as client:
                cold(items[-1])
                start = time.monotonic()
                for item in items[:args.queries]:
                    client.post(ab.URL + "/_bench/drop_caches").raise_for_status()
                    (ab.WORK / "server.log").write_text("")
                    rows.append(cold(item))
                    memory_peak = max(memory_peak, peak_rss(proc))
                    print(json.dumps({"completed": len(rows), "ms": rows[-1]["ms"]}), flush=True)
        else:
            with server(arm) as proc, httpx.Client(timeout=120) as client:
                url = f"{ab.URL}/v2/namespaces/{args.namespace}/query"

                def one(item):
                    before = time.monotonic()
                    response = client.post(url, json=item["request"])
                    response.raise_for_status()
                    elapsed_ms = 1000 * (time.monotonic() - before)
                    response = response.json()
                    return {"query_id": item["query_id"], "ms": elapsed_ms,
                            "response": response, "recall": overlap(item, response)}

                for item in items:
                    one(item)
                start = time.monotonic()
                workload = [items[i % len(items)] for i in range(args.queries)]
                if args.clients == 1:
                    rows = [one(item) for item in workload]
                else:
                    with ThreadPoolExecutor(max_workers=args.clients) as pool:
                        rows = list(pool.map(one, workload))
                elapsed = time.monotonic() - start
                memory_peak = peak_rss(proc)
        elapsed = elapsed if args.mode == "warm" else time.monotonic() - start
        result = summarize(rows, elapsed)
        result.update(mode=args.mode, clients=args.clients, namespace=args.namespace, probes=args.probes,
                      peak_rss_bytes=memory_peak)
        args.output.write_text(json.dumps({"summary": result, "rows": rows}))
        print(json.dumps(result), flush=True)
    except BaseException:
        log = ab.WORK / "server.log"
        if log.exists():
            shutil.copyfile(log, args.output.with_suffix(".failure.log"))
        raise
    finally:
        shutil.rmtree(ab.WORK, ignore_errors=True)


if __name__ == "__main__":
    main()
