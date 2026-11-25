# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1", "numpy==2.5.4"]
# ///
"""Compare cold ANN queries before and after a real process restart.

Run with uv, a private JSON config (storage_url, region, and namespace), the
scale-server control and candidate binaries, and an empty directory under
.scratch/. Storage is always read-only. The candidate first provisions a stage,
then runs steady-state, restart-adoption, and control-restaging samples. Query
caches and local staging pages are dropped before each sample. No queries warm
the restarted process before its measured first 100 requests. Use --repeats 2
for a confirmation run; retain the JSON receipts, summarize them, and remove
scratch after the run. All processes and cache files are cleaned up on exit.
An optional --seed-stage reuses a completed ann-staging-v2 directory from a
previous process via hard links; its first start adopts rather than downloading.
The seed remains owned by the caller, who must remove it after measurement.
"""

import argparse
import contextlib
import json
import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path

import httpx
import numpy as np


def metrics(client):
    result = {}
    for line in client.get("/metrics").text.splitlines():
        match = re.fullmatch(r"([a-zA-Z_][a-zA-Z_0-9]*)(\{.*\})? (\S+)", line)
        if match:
            result[match[1] + (match[2] or "")] = float(match[3])
    return result


def outcome(values, name):
    return values.get(f'blowfish_ann_staging_total{{outcome="{name}"}}', 0)


def backend(values, family, operations):
    return sum(
        value
        for key, value in values.items()
        if key.startswith(f"blowfish_object_store_{family}_total{{")
        and any(f'op="{op}"' in key for op in operations)
    )


def evict(cache):
    files = [p for p in cache.glob("ann-staging-v*/*/**/*") if p.is_file() and p.name != ".lock"]
    for path in files:
        with path.open("rb") as stream:
            os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    return sum(p.stat().st_size for p in files)


@contextlib.contextmanager
def server(binary, work, config, port):
    work.mkdir(parents=True, exist_ok=True)
    storage = {
        "url": config["storage_url"],
        "region": config.get("region"),
        "endpoint": config.get("endpoint"),
        "profile": config.get("profile"),
        "read_only": True,
        "cache": {
            "memory_bytes": 8 << 30,
            "disk_bytes": 48 << 30,
            "disk_dir": str(work / "cache"),
            "block_size": 4000,
            "max_coalesce_blocks": 512,
            "fetch_concurrency": 512,
        },
    }
    (work / "storage.local").write_text(json.dumps(storage))
    (work / "server.log").write_text("")
    log = (work / "server.log").open("a+")
    env = dict(os.environ, RAYON_NUM_THREADS="8", BF_ANN_NPROBE="64", BF_DECODED_CACHE_BYTES=str(512 << 20), BF_TRACE_READS="1", BF_TRACE_STAGES="1")
    started = time.perf_counter()
    proc = subprocess.Popen(["taskset", "-c", "0-7", str(binary), str(work / "storage.local"), f"127.0.0.1:{port}"], env=env, stdout=log, stderr=log)
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=300) as client:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(f"server exited: {work}")
                try:
                    client.get("/").raise_for_status()
                    break
                except httpx.HTTPError:
                    time.sleep(0.02)
            else:
                raise RuntimeError("server startup timeout")
            yield client, started, log, proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        log.close()


def wait_ready(client, proc):
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        values = metrics(client)
        if outcome(values, "ready") or outcome(values, "adopted"):
            return values
        if outcome(values, "error") or proc.poll() is not None:
            raise RuntimeError("staging failed")
        time.sleep(0.05)
    raise RuntimeError("staging timeout")


def query(client, ns, vector):
    response = client.post(f"/v2/namespaces/{ns}/query", json={"rank_by": ["vector", "ANN", vector.astype("f4").tolist()], "top_k": 10, "consistency": {"level": "strong"}})
    response.raise_for_status()
    return response.json()


def sample(client, started, log, ns, oracle, count, cache, arm, repeat, output, prefixes):
    rows = []
    fast = None
    for qi in range(count):
        client.post("/_bench/drop_caches").raise_for_status()
        disk = evict(cache)
        before = metrics(client)
        if fast is None and (outcome(before, "ready") or outcome(before, "adopted")):
            fast = time.perf_counter() - started
        pos = os.fstat(log.fileno()).st_size
        at = time.perf_counter()
        response = query(client, ns, oracle["vector"][qi])
        ms = (time.perf_counter() - at) * 1000
        after = metrics(client)
        # The child inherits the writer's open-file description. Read with a separate
        # descriptor, so concurrent append writes cannot move our read position.
        with Path(log.name).open("rb") as reader:
            reader.seek(pos)
            trace = reader.read().decode("utf-8", errors="replace")
        plans = re.findall(r"^PLAN rows=(\d+)$", trace, re.MULTILINE)
        if not plans:
            raise RuntimeError(f"missing query plan trace for {arm} q{qi}")
        tier_gets = 0
        for match in re.finditer(r"^TRACE_READ (\S+) (\d+) (\d+) (\d+) ", trace, re.MULTILINE):
            path, start, end, size = match[1], int(match[2]), int(match[3]), int(match[4])
            for key, length in prefixes.items():
                if path.endswith("/" + key) and size and ((start == end == 0) or (end <= length + 24)):
                    tier_gets += 1
                    break
        ids = [row["id"] for row in response["rows"]]
        row = {"arm": arm, "repeat": repeat, "q": qi, "ms": ms, "ids": ids, "distances": [r.get("$dist") for r in response["rows"]], "recall": len(set(map(int, ids)) & set(map(int, oracle["ann_ids"][qi][:10]))) / 10, "planned_rows": sum(map(int, plans)), "gets": backend(after, "attempts", ["get", "get_range"]) - backend(before, "attempts", ["get", "get_range"]), "tier_gets": tier_gets, "disk_bytes": disk}
        rows.append(row)
        with output.open("a") as file:
            file.write(json.dumps(row) + "\n")
        print(json.dumps({k: v for k, v in row.items() if k not in {"ids", "distances"}}), flush=True)
    return rows, fast


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--port", type=int, default=8500)
    parser.add_argument("--seed-stage", type=Path, help="completed ann-staging-v2 directory from a previous process; reuse payload inodes")
    args = parser.parse_args()
    root = args.scratch.resolve()
    if ".scratch" not in root.parts or (root.exists() and any(root.iterdir())):
        parser.error("scratch must be empty and under .scratch/")
    config = json.loads(args.config.read_text())
    oracle = np.load(args.oracle)
    if not 0 < args.queries <= len(oracle["vector"]) or args.repeats <= 0:
        parser.error("invalid query count or repeats")
    root.mkdir(parents=True, exist_ok=True)
    ns = config["namespace"]
    summaries = []
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(143)))
    try:
        for repeat in range(args.repeats):
            work = root / f"candidate-{repeat}"
            if args.seed_stage:
                shutil.copytree(args.seed_stage.resolve(), work / "cache" / "ann-staging-v2", copy_function=os.link)
            with server(args.candidate.resolve(), work, config, args.port) as (client, started, log, proc):
                first_at = time.perf_counter()
                query(client, ns, oracle["vector"][-1])
                wait_ready(client, proc)
                provision = time.perf_counter() - first_at
                records = [json.loads(p.read_bytes()[32:]) for p in (work / "cache").glob("ann-staging-v2/*/*/complete")]
                if not records:
                    raise RuntimeError("no durable candidate stage")
                prefixes = {key: f["len"] for record in records for key, f in record["files"].items()}
                steady, _ = sample(client, started, log, ns, oracle, args.queries, work / "cache", "steady", repeat, root / "rows.jsonl", prefixes)
            evict(work / "cache")
            with server(args.candidate.resolve(), work, config, args.port) as (client, started, log, proc):
                restart, fast = sample(client, started, log, ns, oracle, args.queries, work / "cache", "adopt", repeat, root / "rows.jsonl", prefixes)
                values = wait_ready(client, proc)
                adoption = values.get("blowfish_ann_adoption_seconds_sum")
                if outcome(values, "ready") or not outcome(values, "adopted"):
                    raise RuntimeError("restart rebuilt rather than adopted")
                if sum(row["tier_gets"] for row in restart):
                    raise RuntimeError("adopted tiers incurred payload GETs")
            control_work = root / f"control-{repeat}"
            with server(args.baseline.resolve(), control_work, config, args.port) as (client, _, _, proc):
                query(client, ns, oracle["vector"][-1])
                wait_ready(client, proc)
            # A killed control leaves an orphan directory; its next startup removes it.
            with server(args.baseline.resolve(), control_work, config, args.port) as (client, started, log, proc):
                control, control_fast = sample(client, started, log, ns, oracle, args.queries, control_work / "cache", "restage", repeat, root / "rows.jsonl", prefixes)
                wait_ready(client, proc)
                control_fast = control_fast or (time.perf_counter() - started)
            for arm in [restart, control]:
                for left, right in zip(steady, arm):
                    if (left["ids"], left["distances"], left["planned_rows"]) != (right["ids"], right["distances"], right["planned_rows"]):
                        raise RuntimeError(f"result mismatch q{left['q']}")
            summary = {"repeat": repeat, "seeded_stage": bool(args.seed_stage), "provision_s": provision, "adoption_s": adoption, "adopt_time_to_fast_s": fast, "restage_time_to_fast_s": control_fast}
            for name, rows in [("steady", steady), ("adopt", restart), ("restage", control)]:
                summary[name] = {"p50_ms": float(np.percentile([r["ms"] for r in rows], 50)), "p90_ms": float(np.percentile([r["ms"] for r in rows], 90)), "first_ms": rows[0]["ms"], "mean_gets": float(np.mean([r["gets"] for r in rows])), "tier_gets": sum(r["tier_gets"] for r in rows), "max_disk_bytes": max(r["disk_bytes"] for r in rows), "recall": float(np.mean([r["recall"] for r in rows]))}
            summary["passes"] = {
                "cold_p50_within_10_percent": summary["adopt"]["p50_ms"] <= summary["steady"]["p50_ms"] * 1.10,
                "adoption_under_2s": adoption is not None and adoption < 2,
                "no_tier_payload_gets": summary["adopt"]["tier_gets"] == 0,
                "stable_disk_within_cap": len({r["disk_bytes"] for r in steady + restart}) == 1 and summary["adopt"]["max_disk_bytes"] <= 20 << 30,
            }
            summaries.append(summary)
            (root / "summary.json").write_text(json.dumps(summaries, indent=2))
            print(json.dumps(summary), flush=True)
            shutil.rmtree(work / "cache", ignore_errors=True)
            shutil.rmtree(control_work / "cache", ignore_errors=True)
    finally:
        for cache in root.glob("*/cache"):
            shutil.rmtree(cache, ignore_errors=True)


if __name__ == "__main__":
    main()
