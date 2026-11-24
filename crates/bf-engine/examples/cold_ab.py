# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1", "numpy==2.5.4"]
# ///
"""Alternate paired cold ANN queries against two read-only server builds.

Run from a worktree with uv. Supply a private config.local with storage_url,
region, namespace, and optional endpoint/profile. The oracle selects the existing
exact-truth workload; it does not change the storage prefix or namespace.

Example: uv run crates/bf-engine/examples/cold_ab.py --config CONFIG.local \
    --binary CANDIDATE --arms '{"base":{"BINARY":"BASELINE"},"stream":{}}' \
    --scratch .scratch/cold-ab --label run-1 --queries 200

Each arm can set CONFIG to a private config for a copied layout with the same
namespace name, and BF_ANN_NPROBE to match recall at a different probe budget.
Both builds need blowfish_object_store_attempts_total for physical GET accounting.
ANN row counts require BF_TRACE_STAGES output containing PLAN rows= records.
For a single-tier automatic-staging candidate, pass --stage-arm NAME. The harness
records its first query and provisioning cost, waits for publication, and evicts
the staged files from the filesystem page cache before every measured query.
Outputs, traces, and temporary storage configuration stay under --scratch. Cache
files are removed on exit. Summarize the results, and remove the run directory
when the experiment ends.
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

P = argparse.ArgumentParser(description=__doc__)
P.add_argument("--arms", required=True, help="JSON arm name -> environment")
P.add_argument("--mode", choices=["fresh", "cachecold"], default="cachecold")
P.add_argument("--queries", type=int, default=50)
P.add_argument("--offset", type=int, default=0)
P.add_argument("--probes", type=int, default=64)
P.add_argument("--label", required=True)
P.add_argument("--binary", required=True)
P.add_argument("--config", type=Path, required=True)
P.add_argument("--scratch", type=Path, required=True)
P.add_argument("--port", type=int, default=8400)
P.add_argument(
    "--stage-arm",
    action="append",
    default=[],
    help="single-tier staging arm to provision, then read with cold filesystem pages",
)
P.add_argument("--oracle", choices=["100k", "1m", "10m", "100m"], default="10m")
P.add_argument(
    "--nq-data",
    type=Path,
    help="prepared NQ queries.npy and truth.npy instead of the scale oracle",
)
a = P.parse_args()
signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(143)))
ROOT = Path(__file__).resolve().parents[3]
RUN = (a.scratch / a.label).resolve()
if ".scratch" not in RUN.parts or not RUN.is_relative_to(a.scratch.resolve()):
    P.error("scratch and label must resolve inside this run under .scratch/")
if RUN.exists() and any(RUN.iterdir()):
    P.error("run directory must be empty; choose a new label")
config = json.loads(a.config.read_text())
if a.nq_data:
    truth = {
        "vector": np.load(a.nq_data / "queries.npy"),
        "ann_ids": np.load(a.nq_data / "truth.npy")[:, :10],
    }
else:
    truth = np.load(ROOT / f"bench/scale/data/oracle-{a.oracle}.npz")
items = [
    {
        "query_id": i,
        "truth": truth["ann_ids"][i].tolist(),
        "request": {
            "rank_by": ["vector", "ANN", truth["vector"][i].astype("f4").tolist()],
            "top_k": 10,
            "consistency": {"level": "strong"},
            **({"include_attributes": ["id"]} if a.nq_data else {}),
        },
    }
    for i in range(len(truth["vector"]))
]
arms = json.loads(a.arms)
if not arms or any(not re.fullmatch(r"[A-Za-z0-9_-]+", name) for name in arms):
    P.error("arm names must contain only letters, numbers, hyphens, or underscores")
if any(name not in arms for name in a.stage_arm) or (a.stage_arm and a.mode != "cachecold"):
    P.error("stage arms must be named cachecold arms")
if a.queries <= 0 or a.offset < 0 or a.offset + a.queries > len(items) or a.probes <= 0:
    P.error("select positive probes and a nonempty slice within the oracle workload")
RUN.mkdir(parents=True, exist_ok=True, mode=0o700)
rows = []


def stats(client):
    text = client.get("/metrics").text
    out = {}
    for line in text.splitlines():
        m = re.match(
            r"blowfish_object_store_(requests|bytes|hedges|attempts)_total\{(.*)\} (\S+)",
            line,
        )
        if m:
            op = re.search(r'(?:op|outcome)="([^"]+)"', m[2])[1]
            key = m[1] + "_" + op
            out[key] = out.get(key, 0) + float(m[3])
    return out


def trace_summary(log):
    """Completed read waves and bytes by object kind, separate from attempted GETs."""
    reads, kinds, stages = [], {}, {}
    for line in log.splitlines():
        if line.startswith("TRACE_READ "):
            _, path, _, _, size, end, elapsed = line.split()
            reads.append((int(end) - int(elapsed), int(end)))
            kind = (
                "routing"
                if "ann-dir" in path or "manifest" in path
                else "slabs"
                if "/slabs/" in path
                else "docs"
                if "/docs" in path
                else "ids"
                if "/ids" in path
                else "segment_ann"
                if "/ann." in path
                else "other"
            )
            kinds[kind] = kinds.get(kind, 0) + int(size)
        elif line.startswith("STAGE "):
            name, micros, _ = line[6:].rsplit(" ", 2)
            stages[name] = stages.get(name, 0) + int(micros)
    waves, active_until = 0, None
    for start, end in sorted(reads):
        if active_until is None or start >= active_until:
            waves += 1
        active_until = max(active_until or end, end)
    return {"read_waves": waves, "read_bytes_by_kind": kinds, "stage_us": stages}


@contextlib.contextmanager
def server(name, index):
    arm_env = dict(arms[name])
    arm_config = json.loads(Path(arm_env.pop("CONFIG", a.config)).read_text())
    if arm_config["namespace"] != config["namespace"]:
        raise ValueError("paired arms must query the same namespace name")
    work = RUN / name
    work.mkdir(exist_ok=True)
    storage = {
        "url": arm_config["storage_url"],
        "region": arm_config.get("region"),
        "endpoint": arm_config.get("endpoint"),
        "profile": arm_config.get("profile"),
        "read_only": True,
        "cache": {
            "memory_bytes": 8 * 2**30,
            "disk_bytes": 48 * 2**30,
            "disk_dir": str(work / "cache"),
            "block_size": 4000,
            "max_coalesce_blocks": 512,
            "fetch_concurrency": 512,
        },
    }
    (work / "storage.local").write_text(json.dumps(storage))
    binary = arm_env.pop("BINARY", a.binary)
    env = dict(
        os.environ,
        RAYON_NUM_THREADS="8",
        BF_DECODED_CACHE_BYTES=str(512 * 2**20),
        BF_ANN_NPROBE=str(a.probes),
        BF_TRACE_READS="1",
        BF_TRACE_STAGES="1",
    )
    env.update(arm_env)
    port = a.port + index
    # Fresh processes reuse an arm directory; traces must describe only this process.
    (work / "server.log").write_text("")
    log = (work / "server.log").open("a")
    proc = subprocess.Popen(
        [str(ROOT / binary), str(work / "storage.local"), f"127.0.0.1:{port}"],
        env=env,
        stdout=log,
        stderr=log,
    )
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=300) as client:
            for _ in range(300):
                if proc.poll() is not None:
                    raise RuntimeError(f"{name} server exited, see log")
                try:
                    client.get("/").raise_for_status()
                    break
                except httpx.HTTPError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("server readiness timeout")
            time.sleep(1.5)
            if proc.poll() is not None:
                raise RuntimeError(f"{name} server exited before sampling; see log")
            yield client, work, proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        log.close()
        shutil.rmtree(work / "cache", ignore_errors=True)


def evict_staging(work):
    """Evict only this run's durable staging files, without privileged global drops."""
    files = [
        p
        for p in (work / "cache" / "ann-staging-v1").rglob("*")
        if p.is_file() and p.name != ".lock"
    ]
    for path in files:
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
            os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    return {"files": len(files), "bytes": sum(p.stat().st_size for p in files)}


def wait_staging(client, work, proc, first, start):
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("staging server exited")
        text = client.get("/metrics").text
        if re.search(r'blowfish_ann_staging_total\{outcome="error"\} [1-9]', text):
            raise RuntimeError("automatic staging failed; inspect server.log")
        if re.search(r'blowfish_ann_staging_total\{outcome="ready"\} [1-9]', text):
            provision = {
                "first_query_ms": first["ms"],
                "ready_after_ms": (time.perf_counter() - start) * 1000,
                **evict_staging(work),
            }
            (work / "provision.json").write_text(json.dumps(provision, indent=2))
            return
        time.sleep(0.1)
    raise RuntimeError("automatic staging timeout")


def query(name, item, client, work, proc):
    before = stats(client)
    req = client.build_request(
        "POST", f"/v2/namespaces/{config['namespace']}/query", json=item["request"]
    )
    start = time.perf_counter()
    res = client.send(req)
    ms = (time.perf_counter() - start) * 1000
    res.raise_for_status()
    res = res.json()
    after = stats(client)
    delta = {k: after.get(k, 0) - before.get(k, 0) for k in after}
    got = [r["id"] for r in res.get("rows", [])]
    log = (work / "server.log").read_text()
    planned = re.findall(r"^PLAN rows=(\d+)$", log, re.MULTILINE)
    row = {
        "ann_rows_scanned": sum(map(int, planned)) if planned else None,
        "arm": name,
        "q": item["query_id"],
        "ms": ms,
        "recall": len(set(map(int, got)) & set(item["truth"])) / 10,
        "ids": got,
        "distances": [r.get("$dist") for r in res.get("rows", [])],
        "gets": sum(delta.get("attempts_" + op, 0) for op in ["get", "get_range"])
        if any(k.startswith("attempts_") for k in after)
        else None,
        "completed_gets": sum(
            delta.get("requests_" + op, 0) for op in ["get", "get_range"]
        ),
        "hedges": delta.get("hedges_issued", 0),
        "bytes": sum(delta.get("bytes_" + op, 0) for op in ["get", "get_range"]),
        **trace_summary(log),
    }
    rows.append(row)
    with (RUN / "rows.jsonl").open("a") as f:
        f.write(json.dumps(row) + "\n")
    print(
        json.dumps({k: v for k, v in row.items() if k not in ["ids", "distances"]}),
        flush=True,
    )
    return row


try:
    with contextlib.ExitStack() as stack:
        servers = {}
        if a.mode == "cachecold":
            for n, name in enumerate(arms):
                servers[name] = stack.enter_context(server(name, n))
                c, w, p = servers[name]
                start = time.perf_counter()
                first = query(name, items[-1], c, w, p)
                if name in a.stage_arm:
                    wait_staging(c, w, p, first, start)
            rows.clear()
            (RUN / "rows.jsonl").write_text("")
        for qi in range(a.offset, a.offset + a.queries):
            names = list(arms)
            names = names[qi % len(names) :] + names[: qi % len(names)]
            # Reverse alternate complete rotations for multi-arm runs, so every pair
            # appears in each order equally often. Two arms retain strict AB/BA order.
            if len(names) > 2 and (qi // len(names)) % 2:
                names.reverse()
            for name in names:
                if a.mode == "cachecold":
                    c, w, p = servers[name]
                    c.post("/_bench/drop_caches").raise_for_status()
                    if name in a.stage_arm:
                        evict_staging(w)
                    (w / "server.log").write_text("")
                    query(name, items[qi], c, w, p)
                else:
                    with server(name, list(arms).index(name)) as (c, w, p):
                        query(name, items[qi], c, w, p)
    summary = {}
    for name in arms:
        part = [r for r in rows if r["arm"] == name]
        summary[name] = {
            "n": len(part),
            "p50": float(np.percentile([r["ms"] for r in part], 50)),
            "p90": float(np.percentile([r["ms"] for r in part], 90)),
            **{
                k: float(np.mean([r[k] for r in part]))
                for k in [
                    "recall",
                    "gets",
                    "completed_gets",
                    "hedges",
                    "bytes",
                    "ann_rows_scanned",
                    "read_waves",
                ]
                if all(r.get(k) is not None for r in part)
            },
        }
    (RUN / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2), flush=True)
finally:
    for name in arms:
        shutil.rmtree(RUN / name / "cache", ignore_errors=True)
