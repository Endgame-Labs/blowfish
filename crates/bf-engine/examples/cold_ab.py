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

Each arm can set BLOCK and COALESCE (cache block bytes and coalesced blocks per GET) and CONFIG to a private config for a copied layout with the same
namespace name, and BF_ANN_NPROBE to match recall at a different probe budget.
Both builds need blowfish_object_store_attempts_total for physical GET accounting.
ANN row counts require BF_TRACE_STAGES output containing PLAN rows= records.
For a single-tier automatic-staging candidate, pass --stage-arm NAME. The harness
records its first query and provisioning cost, waits for publication, and evicts
the staged files from the filesystem page cache before every measured query.
Use --stage-live-queries to measure cache-cold queries during first preparation;
compare the same query IDs across arms, since faster staging can finish sooner.
Use --paired-projections to measure IDs-only and title/text requests in every
group, with independent cache drops, and separate projection summaries.
Use --filters for a filtered workload and --truth-key for its oracle array
(for example, selective_ids). Filtered recall is omitted without matching truth.
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
P.add_argument("--filters", type=json.loads, help="JSON filter applied to every measured query")
P.add_argument(
    "--id-in",
    type=int,
    help="per-query filter [id, In, N IDs] drawn from [0, --id-max) (Round C32); exclusive with --filters",
)
P.add_argument("--id-mode", choices=["random", "clustered"], default="random")
P.add_argument("--id-max", type=int, default=10_000_000)
P.add_argument("--id-span", type=int, default=0, help="clustered: width of the ID range (default 8 per ID, at least 4096)")
P.add_argument("--id-seed", type=int, default=32)
P.add_argument("--truth-key", help="oracle array containing exact IDs for this filter")
P.add_argument("--scratch", type=Path, required=True)
P.add_argument("--port", type=int, default=8400)
P.add_argument(
    "--stage-live-queries",
    type=int,
    default=0,
    help="run this many cache-cold queries during first staging, before steady samples",
)
P.add_argument(
    "--stage-arm",
    action="append",
    default=[],
    help="single-tier staging arm to provision, then read with cold filesystem pages",
)
P.add_argument(
    "--s3-only-arm",
    action="append",
    default=[],
    help="arm with no disk cache or automatic ANN staging, for a pure-S3 control",
)
P.add_argument(
    "--two-phase-arm",
    action="append",
    default=[],
    help="staging arm that publishes codes first and companions after (Round C9 and later)",
)
P.add_argument("--stage-timeout", type=int, default=600, help="seconds to wait for staging to publish")
P.add_argument("--block-size", type=int, default=4000, help="cache block bytes (production: 262144)")
P.add_argument("--coalesce-blocks", type=int, default=512, help="max coalesced blocks per GET (production: 32)")
P.add_argument(
    "--keep-cache",
    type=Path,
    help="move the first staging arm's finished cache here at exit, as a seed for --seed-cache",
)
P.add_argument(
    "--seed-cache",
    type=Path,
    help="hard-link this kept cache into every staging arm before it starts (untimed preparation)",
)
P.add_argument("--oracle", choices=["100k", "1m", "10m", "100m"], default="10m")
P.add_argument(
    "--include-attributes",
    type=json.loads,
    help='JSON include_attributes value, for example false or ["title", "text"]',
)
P.add_argument(
    "--paired-projections",
    action="store_true",
    help="measure both default IDs-only and title/text projections in every cold query group",
)
P.add_argument(
    "--nq-data",
    type=Path,
    help="prepared NQ queries.npy and truth.npy instead of the scale oracle",
)
a = P.parse_args()
if a.paired_projections and a.include_attributes is not None:
    P.error("paired projections supply their own include_attributes values")
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
if a.id_in and a.filters is not None:
    P.error("--id-in and --filters are exclusive")


def id_in_filter(i):
    """The same N IDs for both arms of query i; random or inside one ID range."""
    rng = np.random.default_rng(a.id_seed * 1_000_003 + i)
    if a.id_mode == "random":
        ids = rng.choice(a.id_max, size=a.id_in, replace=False)
    else:
        span = a.id_span or max(8 * a.id_in, 4096)
        base = int(rng.integers(0, a.id_max - span))
        ids = base + rng.choice(span, size=a.id_in, replace=False)
    return ["id", "In", [int(x) for x in ids]]


truth_key = a.truth_key or ("ann_ids" if a.filters is None and not a.id_in else None)
if truth_key is not None and truth_key not in truth:
    P.error(f"oracle has no {truth_key!r} truth array")
items = [
    {
        "query_id": i,
        "truth": truth[truth_key][i].tolist() if truth_key else None,
        "request": {
            "rank_by": ["vector", "ANN", truth["vector"][i].astype("f4").tolist()],
            "top_k": 10,
            "consistency": {"level": "strong"},
            **({"filters": a.filters} if a.filters is not None else {}),
            **({"filters": id_in_filter(i)} if a.id_in else {}),
            **({"include_attributes": ["id"]} if a.nq_data else {}),
            **({"include_attributes": a.include_attributes} if a.include_attributes is not None else {}),
        },
    }
    for i in range(len(truth["vector"]))
]
arms = json.loads(a.arms)
if not arms or any(not re.fullmatch(r"[A-Za-z0-9_-]+", name) for name in arms):
    P.error("arm names must contain only letters, numbers, hyphens, or underscores")
if any(name not in arms for name in a.stage_arm) or (a.stage_arm and a.mode != "cachecold"):
    P.error("stage arms must be named cachecold arms")
if any(name not in arms or name in a.stage_arm for name in a.s3_only_arm):
    P.error("pure-S3 arms must be named arms without staging")
if a.queries <= 0 or a.offset < 0 or a.offset + a.queries > len(items) or a.probes <= 0:
    P.error("select positive probes and a nonempty slice within the oracle workload")
if not 0 <= a.stage_live_queries <= len(items):
    P.error("live provisioning queries must be within the oracle workload")
if a.stage_live_queries and not a.stage_arm:
    P.error("live provisioning queries require at least one staging arm")
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
    # Round C27: slab row-map reconstructions (published maps keep this at zero).
    rebuilds = re.findall(r"^blowfish_ann_row_map_rebuilds_total\{.*\} (\S+)$", text, re.MULTILINE)
    out["rowmap_rebuilds"] = sum(float(v) for v in rebuilds)
    return out


def trace_summary(log):
    """Completed read waves and bytes by object kind, separate from attempted GETs."""
    reads, kinds, stages, timeline, local, slots, stage_times, lookups = [], {}, {}, [], [], [], [], []
    hydration, c11 = [], []
    # Provisioning can still be appending a trace when an HTTP query completes.
    # Read only complete records; a partial trailing write is not a query failure.
    for line in log.rpartition("\n")[0].splitlines():
        if line.startswith("TRACE_READ "):
            _, path, offset, limit, size, end, elapsed = line.split()
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
            timeline.append(
                {
                    "kind": kind,
                    "object": path,
                    "offset": int(offset),
                    "limit": int(limit),
                    "bytes": int(size),
                    "start_us": (int(end) - int(elapsed)) * 1000,
                    "end_us": int(end) * 1000,
                }
            )
        elif line.startswith("C11_"):
            c11.append(line)
        elif line.startswith("HYDRATION "):
            _, key, phase, count, elapsed, end = line.split()
            hydration.append({"object": key, "phase": phase, "rows": int(count), "start_us": int(end) - int(elapsed), "end_us": int(end)})
        elif line.startswith("LOCAL_READ "):
            local.append(
                {key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", line)}
            )
        elif line.startswith("TRACE_SLOT "):
            slots.append(
                {key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", line)}
            )
        elif line.startswith("TRACE_CACHE "):
            lookups.append(
                {key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", line)}
            )
        elif line.startswith("STAGE_TIME "):
            name, end_us = line[11:].rsplit(" ", 1)
            stage_times.append({"stage": name, "end_us": int(end_us)})
        elif line.startswith("STAGE "):
            name, micros, _ = line[6:].rsplit(" ", 2)
            stages[name] = stages.get(name, 0) + int(micros)
    waves, active_until = 0, None
    for start, end in sorted(reads):
        if active_until is None or start >= active_until:
            waves += 1
        active_until = max(active_until or end, end)
    return {
        "filter_matching": [int(n) for n in re.findall(r"^FILTER matching=(\d+)$", log, re.MULTILINE)],
        "read_waves": waves,
        "read_bytes_by_kind": kinds,
        "stage_us": stages,
        "read_timeline": timeline,
        "local_reads": local,
        "fetch_slots": slots,
        "stage_timeline": stage_times,
        "cache_lookups": lookups,
        "hydration_timeline": hydration,
        "c11": c11,
    }


@contextlib.contextmanager
def server(name, index):
    arm_env = dict(arms[name])
    arm_config = json.loads(Path(arm_env.pop("CONFIG", a.config)).read_text())
    if arm_config["namespace"] != config["namespace"]:
        raise ValueError("paired arms must query the same namespace name")
    work = RUN / name
    work.mkdir(exist_ok=True)
    # An arm can set its own cache geometry (BLOCK, COALESCE); Round C25 compares block sizes.
    block_size = int(arm_env.pop("BLOCK", a.block_size))
    coalesce_blocks = int(arm_env.pop("COALESCE", a.coalesce_blocks))
    storage = {
        "url": arm_config["storage_url"],
        "region": arm_config.get("region"),
        "endpoint": arm_config.get("endpoint"),
        "profile": arm_config.get("profile"),
        "read_only": True,
        "cache": {
            "memory_bytes": 8 * 2**30,
            "disk_bytes": 0 if name in a.s3_only_arm else 48 * 2**30,
            "disk_dir": str(work / "cache"),
            "block_size": block_size,
            "max_coalesce_blocks": coalesce_blocks,
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
    if a.seed_cache and name in a.stage_arm:
        shutil.rmtree(work / "cache", ignore_errors=True)
        # Stage files are immutable, so links are safe; each process takes its own locks.
        # Only the staging area is seeded: the block cache starts empty, with its own lock.
        shutil.copytree(
            a.seed_cache / "ann-staging-v3", work / "cache" / "ann-staging-v3",
            copy_function=os.link, ignore=shutil.ignore_patterns(".lock"),
        )
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
        if a.keep_cache and name in a.stage_arm[:1] and not a.keep_cache.exists():
            shutil.move(work / "cache", a.keep_cache)
        shutil.rmtree(work / "cache", ignore_errors=True)


def evict_staging(work):
    """Evict only this run's durable staging files, without privileged global drops."""
    files = [
        p
        for root in (work / "cache").glob("ann-staging-v*")
        for p in root.rglob("*")
        if p.is_file() and p.name != ".lock"
    ]
    for path in files:
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
            os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    return {
        "files": len(files),
        "bytes": sum(p.stat().st_size for p in files),
        "allocated_bytes": sum(p.stat().st_blocks * 512 for p in files),
    }


def staged(text, two_phase):
    """(codes usable, everything usable). Two-phase builds publish codes, then companions."""
    done = re.search(r'blowfish_ann_staging_total\{outcome="(?:ready|adopted)"\} [1-9]', text)
    if not two_phase or not done:
        return bool(done), bool(done)
    adopted = re.search(r'blowfish_ann_staging_total\{outcome="adopted"\} [1-9]', text)
    companions = re.search(r'blowfish_ann_staging_total\{outcome="companions"\} [1-9]', text)
    return True, bool(adopted or companions)


def wait_staging(client, work, proc, first, start, two_phase=False):
    deadline = time.monotonic() + a.stage_timeout
    codes_ms = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("staging server exited")
        text = client.get("/metrics").text
        if re.search(r'blowfish_ann_staging_total\{outcome="error"\} [1-9]', text):
            raise RuntimeError("automatic staging failed; inspect server.log")
        codes, everything = staged(text, two_phase)
        if codes and codes_ms is None:
            codes_ms = (time.perf_counter() - start) * 1000
        if everything:
            coverage = re.search(
                r"^STAGED_VECTORS rows=(\d+) total_rows=(\d+)$",
                (work / "server.log").read_text(),
                re.MULTILINE,
            )
            provision = {
                "first_query_ms": first["ms"],
                "ready_after_ms": (time.perf_counter() - start) * 1000,
                "codes_ready_after_ms": codes_ms,
                **evict_staging(work),
                "backend_totals": stats(client),
                "staged_vector_rows": int(coverage[1]) if coverage else None,
                "tier_rows": int(coverage[2]) if coverage else None,
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
    request_start_us = time.time_ns() // 1000
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
        **(
            {"projection": "attrs" if item["request"].get("include_attributes") == ["title", "text"] else "ids"}
            if a.paired_projections else {}
        ),
        "q": item["query_id"],
        "ms": ms,
        "request_start_us": request_start_us,
        "recall": len(set(map(int, got)) & set(item["truth"])) / 10
        if item["truth"] is not None else None,
        "ids": got,
        "distances": [r.get("$dist") for r in res.get("rows", [])],
        "documents": res.get("rows", []),
        "gets": sum(delta.get("attempts_" + op, 0) for op in ["get", "get_range"])
        if any(k.startswith("attempts_") for k in after)
        else None,
        "completed_gets": sum(
            delta.get("requests_" + op, 0) for op in ["get", "get_range"]
        ),
        "hedges": delta.get("hedges_issued", 0),
        "rowmap_rebuilds": delta.get("rowmap_rebuilds", 0),
        "bytes": sum(delta.get("bytes_" + op, 0) for op in ["get", "get_range"]),
        **trace_summary(log),
    }
    rows.append(row)
    with (RUN / "rows.jsonl").open("a") as f:
        f.write(json.dumps(row) + "\n")
    print(
        json.dumps(
            {
                k: v
                for k, v in row.items()
                if k
                not in [
                    "ids",
                    "distances",
                    "read_timeline",
                    "local_reads",
                    "fetch_slots",
                    "stage_timeline",
                    "cache_lookups",
                    "hydration_timeline",
                    "documents",
                    "c11",
                ]
            }
        ),
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
                    live = []
                    for item in items[: a.stage_live_queries]:
                        if staged(c.get("/metrics").text, name in a.two_phase_arm)[1]:
                            break
                        c.post("/_bench/drop_caches").raise_for_status()
                        (w / "server.log").write_text("")
                        live.append(query(name, item, c, w, p))
                    (w / "live.json").write_text(json.dumps(live))
                    wait_staging(c, w, p, first, start, name in a.two_phase_arm)
            rows.clear()
            (RUN / "rows.jsonl").write_text("")
        for qi in range(a.offset, a.offset + a.queries):
            names = list(arms)
            names = names[qi % len(names) :] + names[: qi % len(names)]
            # Reverse alternate complete rotations for multi-arm runs, so every pair
            # appears in each order equally often. Two arms retain strict AB/BA order.
            if len(names) > 2 and (qi // len(names)) % 2:
                names.reverse()
            for name, attrs in [
                (name, attrs)
                for name in names
                for attrs in ((False, True) if a.paired_projections else (False,))
            ]:
                item = dict(items[qi])
                item["request"] = dict(item["request"])
                if attrs:
                    item["request"]["include_attributes"] = ["title", "text"]
                if a.mode == "cachecold":
                    c, w, p = servers[name]
                    c.post("/_bench/drop_caches").raise_for_status()
                    if name in a.stage_arm:
                        evict_staging(w)
                    (w / "server.log").write_text("")
                    query(name, item, c, w, p)
                else:
                    with server(name, list(arms).index(name)) as (c, w, p):
                        query(name, item, c, w, p)
    summary = {}
    for name, projection in [
        (name, projection)
        for name in arms
        for projection in (("ids", "attrs") if a.paired_projections else (None,))
    ]:
        part = [
            r for r in rows
            if r["arm"] == name and (projection is None or r["projection"] == projection)
        ]
        summary[name if projection is None else f"{name}/{projection}"] = {
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
