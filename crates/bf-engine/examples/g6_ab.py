# /// script
# dependencies = ["numpy==2.5.4"]
# ///
"""G6 kill test: synchronous resident path off vs on over one promoted graph, on the M4.

Same setup and flags as graph_compare.py, but after promotion it alternates the
BF_QUERY_SYNC flag file (off, on, off, on, off, on) over the same server and graph,
so recall, effort, and graph are identical across arms. No Qdrant here.

Original graph_compare description: M4 production graph/Qdrant comparison using the G4 HTTP measurement protocol.

Run from an owned measurement directory containing src/ (a repository copy),
.scratch/fixture/{store,data}, and a Qdrant fixture prepared by prepare_graph_qdrant.py.
For production qualification, pass --production, --binary, and --source-json;
the source record must contain the immutable binary's commit and SHA-256. Production
runs use the ordinary blowfish binary, per-core reactors, and authenticated admin
status diagnostics (authentication is disabled only on loopback, as for Qdrant).
Without --production, candidate-server and source.json select the profiling example.
source-ready and qdrant-ready mark completed preparation. Run one profile at a time:
uv run src/crates/bf-engine/examples/graph_compare.py high 64 --production \
    --binary .scratch/production-g5 --source-json .scratch/production-g5-source.json
Then run a fresh lower profile with a new output name and 48 probes. Each process
must earn ordinary heat admission, build its own graph, and calibrate through the
ordinary recall endpoint. No effort or promotion override is used.

Results remain under .scratch/<name>/ for the caller to record and delete. Every
paired region uses all 3,452 NQ queries, 16 persistent client processes, and 110,464
timed requests. Graph coverage and effort stability are mandatory. Standard and
PGO binaries must be reported separately if both are measured.
"""

import argparse
import http.client
import hashlib
import re
import json
import os
import pathlib
import shutil
import signal
import statistics
import subprocess
import sys
import time
import numpy as np

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("name", help="new output-directory name under .scratch/")
parser.add_argument("probes", type=int, help="ordinary IVF profile, usually 64 or 48")
parser.add_argument(
    "--production",
    action="store_true",
    help="use the real blowfish server and admin diagnostics",
)
parser.add_argument("--binary", type=pathlib.Path, help="immutable server binary")
parser.add_argument(
    "--source-json", type=pathlib.Path, help="source commit and binary SHA-256 record"
)
args = parser.parse_args()
if args.production and (args.binary is None or args.source_json is None):
    parser.error("production comparisons require --binary and --source-json")
if (
    args.probes <= 0
    or args.name in (".", "..")
    or pathlib.Path(args.name).name != args.name
):
    parser.error("use a positive probe count and a single output-directory name")
assert sys.platform == "darwin", "this protocol uses M4 task counters"
ROOT = pathlib.Path.cwd()
BASE = ROOT / ".scratch"
NAME = args.name
PROBES = args.probes
OUT = BASE / NAME
OUT.mkdir()
(OUT / "fixture").symlink_to(BASE / "fixture", target_is_directory=True)
(OUT / "tmp").mkdir()
assert (BASE / "source-ready").exists()
assert shutil.disk_usage(ROOT).free >= 8 * 2**30
SOURCE = json.loads((args.source_json or BASE / "source.json").read_text())
BINARY = (args.binary or BASE / "candidate-server").resolve()
assert hashlib.sha256(BINARY.read_bytes()).hexdigest() == SOURCE["binary_sha256"]
QUERIES = np.load(BASE / "fixture/data/queries.npy")
TRUTH = np.load(BASE / "fixture/data/truth.npy")[:, :10]


def api(port, method, path, body=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=600)
    try:
        c.request(
            method,
            path,
            None if body is None else json.dumps(body, separators=(",", ":")).encode(),
            {"content-type": "application/json"},
        )
        r = c.getresponse()
        raw = r.read()
        if r.status != 200:
            raise RuntimeError((r.status, raw[:500]))
        return json.loads(raw) if raw else None
    finally:
        c.close()


def status():
    if args.production:
        result = api(18443, "GET", "/_blowfish/v1/status")
        return result["hot_graphs"].get(
            "nq", {"graphs": [], "answered": 0, "process_reserved_bytes": 0}
        )
    return api(18443, "GET", "/measure/nq/status")


def dispatch_counts():
    if not args.production:
        return {}
    connection = http.client.HTTPConnection("127.0.0.1", 18443, timeout=30)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        raw = response.read().decode()
        assert response.status == 200
        counts = {}
        for line in raw.splitlines():
            if line.startswith("blowfish_query_cpu_dispatch_total{"):
                match = re.search(r'path="([^"]+)"', line)
                if match:
                    counts[match.group(1)] = float(line.rsplit(" ", 1)[1])
        return counts
    finally:
        connection.close()


def calibration(s):
    return next((g["calibration"] for g in s["graphs"] if g["calibration"]), None)


def log(event, **values):
    value = {"time": time.time(), "event": event, **values}
    with (OUT / "events.jsonl").open("a") as f:
        f.write(json.dumps(value) + "\n")
    print(json.dumps(value), flush=True)


def counters(pid):
    import ctypes

    data = (ctypes.c_uint64 * 64)()
    lib = ctypes.CDLL("/usr/lib/libproc.dylib")
    lib.proc_pid_rusage.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_void_p)
    if lib.proc_pid_rusage(pid, 4, ctypes.byref(data)) != 0:
        raise RuntimeError("task counters unavailable")
    return {"rss_bytes": data[8], "peak_footprint_bytes": data[30]}


def region(label, engine, pid, ef=0, steady=False, repeats=32):
    before = status() if engine == "bf" else None
    dispatch_before = dispatch_counts() if engine == "bf" else {}
    if engine == "bf" and steady:
        ef = calibration(before)["ef"]
    command = [
        "uv",
        "run",
        "--no-project",
        "--with",
        "numpy==2.5.4",
        "python",
        str(ROOT / "src/crates/bf-vector/examples/g3/http_measure.py"),
        str(BASE / "fixture/data"),
        engine,
        "18443" if engine == "bf" else "18446",
        str(ef),
        str(repeats),
        str(OUT / label),
        str(pid),
    ]
    env = dict(os.environ, G3_CLIENT_PROCESSES="16", G3_CLIENT_THREADS="1")
    with (OUT / (label + ".log")).open("wb") as f:
        subprocess.run(command, env=env, stdout=f, stderr=subprocess.STDOUT, check=True)
    result = json.loads((OUT / (label + ".json")).read_text())
    if engine == "bf":
        after = status()
        delta = after["answered"] - before["answered"]
        result["status_before"] = before
        result["status_after"] = after
        dispatch_after = dispatch_counts()
        result["dispatch_delta"] = {
            key: dispatch_after.get(key, 0) - dispatch_before.get(key, 0)
            for key in dispatch_after.keys() | dispatch_before.keys()
        }
        if steady:
            a, b = calibration(before), calibration(after)
            assert a and b and a["usable"] and b["usable"] and a["ef"] == b["ef"], (
                a,
                b,
            )
            assert delta == len(QUERIES) * (repeats + 2), (label, delta)
        else:
            assert delta == 0, (label, delta)
    (OUT / (label + ".json")).write_text(json.dumps(result, indent=2) + "\n")
    log(
        "region",
        label=label,
        qps=result["qps"],
        recall=result["recall_at_10"],
        instructions=result.get("instructions_per_query"),
        rss=result.get("rss_bytes"),
    )
    return result


def qquality(ef):
    c = http.client.HTTPConnection("127.0.0.1", 18446, timeout=120)
    found = 0
    try:
        for q, truth in zip(QUERIES, TRUTH):
            body = json.dumps(
                {
                    "vector": q.tolist(),
                    "limit": 10,
                    "with_payload": False,
                    "with_vector": False,
                    "params": {"hnsw_ef": ef},
                },
                separators=(",", ":"),
            ).encode()
            c.request(
                "POST",
                "/collections/nq/points/search",
                body,
                {"content-type": "application/json"},
            )
            r = c.getresponse()
            raw = r.read()
            assert r.status == 200, (r.status, raw[:300])
            found += len(
                {int(row["id"]) for row in json.loads(raw)["result"]}
                & set(map(int, truth))
            )
    finally:
        c.close()
    result = found / (len(QUERIES) * 10)
    log("qdrant-quality", ef=ef, recall=result)
    return result


server = heater = qdrant = None
summary = None
config = {
    "url": (BASE / "fixture/store").as_uri(),
    "read_only": True,
    "cache": {
        "memory_bytes": 4 * 2**30,
        "disk_bytes": 0,
        "block_size": 4000,
        "max_coalesce_blocks": 512,
        "fetch_concurrency": 512,
    },
}
(OUT / "storage.json").write_text(json.dumps(config))
if args.production:
    settings = [
        'listen = "127.0.0.1:18443"',
        "require_auth = false",
        "[engine]",
        "indexing = false",
        f"ann_nprobe = {PROBES}",
        "recall_sample_rate = 0.0",
        "[storage]",
        "url = " + json.dumps(config["url"]),
        "read_only = true",
        "[storage.cache]",
    ]
    settings.extend(f"{key} = {value}" for key, value in config["cache"].items())
    (OUT / "server.toml").write_text("\n".join(settings) + "\n")
try:
    env = dict(
        os.environ,
        RAYON_NUM_THREADS="8",
        TOKIO_WORKER_THREADS="8",
        BF_ANN_NPROBE=str(PROBES),
        BF_QUERY_SYNC=str(OUT / "sync.flag"),
        BF_DECODED_CACHE_BYTES=str(4 * 2**30),
        BF_RECALL_SAMPLE_INTERVAL_MS=str(24 * 3600 * 1000),
        TMPDIR=str(OUT / "tmp"),
    )
    sf = (OUT / "server.log").open("wb")
    command = (
        [str(BINARY), "serve", "--config", str(OUT / "server.toml")]
        if args.production
        else [str(BINARY), str(OUT / "storage.json"), "nq", str(OUT / "ready"), "18443"]
    )
    server = subprocess.Popen(
        command, stdout=sf, stderr=sf, env=env, start_new_session=True
    )
    (OUT / "server.pid").write_text(str(server.pid))
    for _ in range(120):
        if server.poll() is not None:
            raise RuntimeError("server exited; inspect server.log")
        try:
            if args.production:
                status()
                break
            if (OUT / "ready").exists():
                break
        except OSError:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("server did not become ready")
    hf = (OUT / "heater.log").open("wb")
    heater = subprocess.Popen(
        [
            "uv",
            "run",
            str(ROOT / "src/crates/bf-engine/examples/graph_heat.py"),
            str(OUT),
            "18443",
            "800",
            *(["--production"] if args.production else []),
        ],
        stdout=hf,
        stderr=hf,
        start_new_session=True,
    )
    begun = time.monotonic()
    build_started = None
    pre = during = None
    samples = 0
    lastlog = 0
    peak = 0
    while True:
        if server.poll() is not None or heater.poll() is not None:
            raise RuntimeError("server or heater exited")
        s = status()
        c = calibration(s)
        now = time.monotonic()
        peak = max(peak, counters(server.pid)["peak_footprint_bytes"])
        if now - lastlog > 30:
            log("status", elapsed=now - begun, status=s, counters=counters(server.pid))
            lastlog = now
        if pre is None and now - begun > 400:
            assert not any(g["building"] for g in s["graphs"]), s
            (OUT / "pause").touch()
            time.sleep(1)
            pre = region("before-build", "bf", server.pid)
            (OUT / "pause").unlink()
        building = any(g["building"] for g in s["graphs"])
        if building and build_started is None:
            build_started = now
            log("building-started")
        if building and during is None and now - build_started > 90:
            (OUT / "pause").touch()
            time.sleep(1)
            during = region("during-build", "bf", server.pid)
            (OUT / "pause").unlink()
        if c:
            if c["usable"]:
                break
            i = (samples * 47 + 19) % len(QUERIES)
            response = api(
                18443,
                "POST",
                "/v1/namespaces/nq/_debug/recall",
                {
                    "num": 1,
                    "top_k": 10,
                    "rank_by": ["vector", "ANN", QUERIES[i].tolist()],
                },
            )
            samples += 1
            if samples % 16 == 0:
                log("calibration", samples=samples, status=status())
        else:
            time.sleep(2)
        if now - begun > 4 * 3600:
            raise RuntimeError("promotion exceeded four hours")
    assert pre is not None and during is not None
    (OUT / "pause").touch()
    os.killpg(heater.pid, signal.SIGTERM)
    heater.wait(timeout=20)
    heater = None
    time.sleep(2)
    promoted = status()
    log("promoted", status=promoted, counters=counters(server.pid))
    first = region("quality-bf", "bf", server.pid, steady=True, repeats=1)
    flag = OUT / "sync.flag"
    arms = {"off": [], "on": []}
    for rep in range(3):
        for arm in ["off", "on"]:
            if arm == "on":
                flag.touch()
            else:
                flag.unlink(missing_ok=True)
            time.sleep(1)
            r = region(f"ab{rep+1}-{arm}", "bf", server.pid, steady=True)
            assert abs(r["recall_at_10"] - first["recall_at_10"]) <= 1e-9, (
                arm,
                first["recall_at_10"],
                r["recall_at_10"],
            )
            arms[arm].append(r)
    summary = {
        "source": SOURCE,
        "probes": PROBES,
        "recall": first["recall_at_10"],
        "off_qps": [r["qps"] for r in arms["off"]],
        "on_qps": [r["qps"] for r in arms["on"]],
        "off_median_qps": statistics.median(r["qps"] for r in arms["off"]),
        "on_median_qps": statistics.median(r["qps"] for r in arms["on"]),
        "off_instructions": [r.get("instructions_per_query") for r in arms["off"]],
        "on_instructions": [r.get("instructions_per_query") for r in arms["on"]],
        "final_status": status(),
        "arms": arms,
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    log("complete", off=summary["off_median_qps"], on=summary["on_median_qps"])
finally:
    exit_usage = {}
    for label, process in [("heater", heater), ("server", server), ("qdrant", qdrant)]:
        if process is None or process.poll() is not None:
            continue
        os.killpg(process.pid, signal.SIGTERM)
        deadline = time.monotonic() + 30
        while True:
            pid, result, usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                process.returncode = os.waitstatus_to_exitcode(result)
                exit_usage[label] = {
                    "peak_rss_bytes": usage.ru_maxrss,
                    "swaps": usage.ru_nswap,
                    "user_cpu_seconds": usage.ru_utime,
                    "system_cpu_seconds": usage.ru_stime,
                }
                break
            if time.monotonic() > deadline:
                os.killpg(process.pid, signal.SIGKILL)
                pid, result, usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(result)
                raise RuntimeError(f"{label} did not stop within thirty seconds")
            time.sleep(0.1)
    if summary is not None:
        summary["exit_usage"] = exit_usage
        (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
