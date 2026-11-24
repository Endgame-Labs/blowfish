# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Paired, fresh-process B5 cold queries over two owned local checkpoints.

Run with uv. All outputs stay under --scratch; caches and children close on exit.
The source writers must be stopped, and both stores must represent the same checkpoint.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import time

import httpx

ROOT = Path(__file__).resolve().parents[4]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--scratch", required=True, type=Path)
p.add_argument("--baseline", required=True, type=Path)
p.add_argument("--candidate", required=True, type=Path)
p.add_argument("--baseline-store", required=True, type=Path)
p.add_argument("--candidate-store", required=True, type=Path)
p.add_argument("--candidate-env", action="append", default=[])
p.add_argument("--namespace", default="b5-msmarco")
p.add_argument("--queries", type=int, default=60)
a = p.parse_args()
run = a.scratch.resolve()
if ".scratch" not in run.parts:
    p.error("outputs must be under .scratch")
run.mkdir(parents=True, exist_ok=True)
if (run / "results.jsonl").exists():
    p.error("choose an empty result directory")
items = json.loads((ROOT / "bench/scale/data/bm25-msmarco.json").read_text())
if not 1 <= a.queries <= len(items):
    p.error("queries must select a nonempty prefix of the workload")
items = items[:a.queries]
results, facades = [], []


def metrics(client):
    out = {}
    for line in client.get("/metrics").text.splitlines():
        match = re.match(r'blowfish_object_store_(requests|bytes)_total\{(.*)\} (\S+)', line)
        if match:
            op = re.search(r'op="([^"]+)"', match[2])[1]
            key = match[1] + "_" + op
            out[key] = out.get(key, 0) + float(match[3])
    return out


def stop(server):
    server.terminate()
    try:
        server.wait(timeout=20)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait(timeout=5)


try:
    for name, port, store in [("base", 18324, a.baseline_store), ("new", 18325, a.candidate_store)]:
        with (run / f"facade-{name}.log").open("w") as log:
            facades.append(subprocess.Popen(
                ["uv", "run", "python", str(ROOT / "bench/scale/local_s3.py"),
                 str(store.resolve()), "--port", str(port), "--delay-ms", "25"],
                stdout=log, stderr=log))
    time.sleep(.5)
    for qi, item in enumerate(items):
        responses = {}
        for name in (["base", "new"] if qi % 2 == 0 else ["new", "base"]):
            work = run / "work"
            shutil.rmtree(work, ignore_errors=True)
            work.mkdir()
            port = 18324 if name == "base" else 18325
            config = {"url": "s3://local", "endpoint": f"http://127.0.0.1:{port}",
                      "region": "us-west-2", "read_only": True, "warm_connections": 128,
                      "cache": {"memory_bytes": 1 << 30, "disk_bytes": 4 << 30,
                                "disk_dir": str(work / "cache"), "block_size": 4000,
                                "max_coalesce_blocks": 512, "fetch_concurrency": 512}}
            path = work / "storage.local"
            path.write_text(json.dumps(config))
            env = dict(os.environ, AWS_ACCESS_KEY_ID="local", AWS_SECRET_ACCESS_KEY="local",
                       RAYON_NUM_THREADS="2", TOKIO_WORKER_THREADS="2", BF_TRACE_READS="1")
            if name == "new":
                env.update(pair.split("=", 1) for pair in a.candidate_env)
            binary = a.baseline if name == "base" else a.candidate
            with (run / f"{qi}-{name}.log").open("w") as log:
                server = subprocess.Popen([str(binary.resolve()), str(path), "127.0.0.1:8391"],
                                          stdout=log, stderr=log, env=env)
                try:
                    with httpx.Client(base_url="http://127.0.0.1:8391", timeout=300) as client:
                        for _ in range(400):
                            if server.poll() is not None:
                                raise RuntimeError("server exited before readiness")
                            try:
                                if client.get("/").status_code == 200:
                                    break
                            except httpx.TransportError:
                                pass
                            time.sleep(.025)
                        else:
                            raise RuntimeError("readiness timeout")
                        time.sleep(.25)
                        before = metrics(client)
                        start = time.perf_counter()
                        response = client.post(f"/v2/namespaces/{a.namespace}/query", json=item["request"])
                        ms = (time.perf_counter() - start) * 1000
                        response.raise_for_status()
                        responses[name] = response.json()["rows"]
                        after = metrics(client)
                        row = {"query": qi, "arm": name, "ms": ms,
                               "delta": {k: after.get(k, 0) - before.get(k, 0) for k in after}}
                finally:
                    stop(server)
            results.append(row)
            with (run / "results.jsonl").open("a") as output:
                output.write(json.dumps(row) + "\n")
        assert responses["base"] == responses["new"], f"query {qi} differs"
    for name in ["base", "new"]:
        rows = [r for r in results if r["arm"] == name]
        times = sorted(r["ms"] for r in rows)
        print(json.dumps({"arm": name, "queries": len(rows), "p50_ms": statistics.median(times),
                          "p90_ms": times[int(.9 * len(times))],
                          "gets": statistics.mean(sum(r["delta"].get(k, 0) for k in
                                                     ["requests_get", "requests_get_range"]) for r in rows),
                          "bytes": statistics.mean(sum(r["delta"].get(k, 0) for k in
                                                      ["bytes_get", "bytes_get_range"]) for r in rows)}))
finally:
    for server in facades:
        stop(server)
    shutil.rmtree(run / "work", ignore_errors=True)
