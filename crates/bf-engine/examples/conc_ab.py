# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1", "numpy==2.5.4"]
# ///
"""Closed-loop concurrent cold pure-S3 ANN queries against one server build at a time.

Round C24b: N client threads each send queries back to back to a read-only server with no disk
cache and a small memory cache (production 1 MiB blocks, 8-block coalescing). Reports q/s,
p50/p90 latency, server CPU seconds per query (utime + stime from /proc), and GETs and hedges per
query. Builds run in rotating rounds so every build sees the same host conditions.
With --perf, also records a perf profile of the server for the first round's measured window.

uv run crates/bf-engine/examples/conc_ab.py --config CONFIG --scratch .scratch/RUN --label L \
    --builds '{"v050":"/path/bin","c24":"/path/bin","c24b":"/path/bin"}' --clients 16 --queries 400 --rounds 3
"""

import argparse, contextlib, json, os, re, shutil, subprocess, threading, time
from pathlib import Path

import httpx
import numpy as np

P = argparse.ArgumentParser()
P.add_argument("--config", type=Path, required=True)
P.add_argument("--builds", required=True)
P.add_argument("--clients", type=int, default=16)
P.add_argument("--queries", type=int, default=400, help="measured queries per build per round")
P.add_argument("--rounds", type=int, default=3)
P.add_argument("--oracle", default="10m")
P.add_argument("--scratch", type=Path, required=True)
P.add_argument("--label", required=True)
P.add_argument("--port", type=int, default=8500)
P.add_argument("--perf", action="store_true")
a = P.parse_args()
ROOT = Path(__file__).resolve().parents[3]
RUN = (a.scratch / a.label).resolve()
assert ".scratch" in RUN.parts
RUN.mkdir(parents=True, exist_ok=True)
config = json.loads(a.config.read_text())
truth = np.load(ROOT / f"bench/scale/data/oracle-{a.oracle}.npz")
n_items = len(truth["vector"])
builds = json.loads(a.builds)
CLK = os.sysconf("SC_CLK_TCK")


def cpu_s(pid):
    f = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / CLK


def counters(client):
    out = {"gets": 0.0, "hedges": 0.0}
    for line in client.get("/metrics").text.splitlines():
        m = re.match(r"blowfish_object_store_(attempts|hedges)_total\{(.*)\} (\S+)", line)
        if m:
            op = re.search(r'(?:op|outcome)="([^"]+)"', m[2])[1]
            if m[1] == "attempts" and op in ("get", "get_range"):
                out["gets"] += float(m[3])
            if m[1] == "hedges":
                out["hedges"] += float(m[3])
    return out


@contextlib.contextmanager
def server(name, binary, port):
    work = RUN / name
    work.mkdir(exist_ok=True)
    storage = {
        "url": config["storage_url"], "region": config.get("region"),
        "endpoint": config.get("endpoint"), "profile": config.get("profile"), "read_only": True,
        "cache": {"memory_bytes": 64 * 2**20, "disk_bytes": 0, "disk_dir": str(work / "cache"),
                  "block_size": 1048576, "max_coalesce_blocks": 8, "fetch_concurrency": 512},
    }
    (work / "storage.local").write_text(json.dumps(storage))
    env = dict(os.environ, RAYON_NUM_THREADS="8", BF_DECODED_CACHE_BYTES=str(512 * 2**20), BF_ANN_NPROBE="64")
    log = (work / "server.log").open("w")
    proc = subprocess.Popen([binary, str(work / "storage.local"), f"127.0.0.1:{port}"], env=env, stdout=log, stderr=log)
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=300) as c:
            for _ in range(300):
                try:
                    c.get("/").raise_for_status(); break
                except httpx.HTTPError:
                    time.sleep(0.1)
            time.sleep(1.5)
            yield c, proc
    finally:
        proc.terminate()
        try: proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.wait(5)
        log.close()
        shutil.rmtree(work / "cache", ignore_errors=True)


def request(i):
    return {"rank_by": ["vector", "ANN", truth["vector"][i % n_items].astype("f4").tolist()],
            "top_k": 10, "consistency": {"level": "strong"}, "include_attributes": ["id"]}


def run(name, binary, rnd, port, perf):
    with server(name, binary, port) as (c0, proc):
        url = f"/v2/namespaces/{config['namespace']}/query"
        for w in range(4):  # warm the process: connections, directory, centroids
            c0.post(url, json=request(900 + w)).raise_for_status()
        before, cpu0 = counters(c0), cpu_s(proc.pid)
        lat, nxt, lock = [], [rnd * 1000], threading.Lock()
        end_i = rnd * 1000 + a.queries
        def worker():
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=300) as c:
                while True:
                    with lock:
                        i = nxt[0]; nxt[0] += 1
                    if i >= end_i: return
                    t = time.perf_counter()
                    c.post(url, json=request(i)).raise_for_status()
                    with lock:
                        lat.append((time.perf_counter() - t) * 1000)
        pp = None
        if perf:
            pp = subprocess.Popen(["perf", "record", "-F", "499", "-g", "-p", str(proc.pid), "-o", str(RUN / f"{name}.perf.data")],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1)
        t0 = time.perf_counter()
        ts = [threading.Thread(target=worker) for _ in range(a.clients)]
        [t.start() for t in ts]; [t.join() for t in ts]
        wall = time.perf_counter() - t0
        cpu1, after = cpu_s(proc.pid), counters(c0)
        if pp:
            pp.send_signal(2); pp.wait()
        n = len(lat)
        return {"build": name, "round": rnd, "n": n, "qps": n / wall, "p50": float(np.percentile(lat, 50)),
                "p90": float(np.percentile(lat, 90)), "cpu_ms_per_query": 1000 * (cpu1 - cpu0) / n,
                "gets_per_query": (after["gets"] - before["gets"]) / n, "hedges_per_query": (after["hedges"] - before["hedges"]) / n}

names = list(builds)
rows = []
for rnd in range(a.rounds):
    order = names[rnd % len(names):] + names[:rnd % len(names)]
    for k, name in enumerate(order):
        r = run(name, builds[name], rnd, a.port + k, a.perf and rnd == 0)
        rows.append(r); print(json.dumps(r), flush=True)
        (RUN / "rows.json").write_text(json.dumps(rows, indent=1))
summ = {}
for name in names:
    part = [r for r in rows if r["build"] == name]
    summ[name] = {k: float(np.mean([r[k] for r in part])) for k in
                  ["qps", "p50", "p90", "cpu_ms_per_query", "gets_per_query", "hedges_per_query"]}
    summ[name]["n"] = sum(r["n"] for r in part)
(RUN / "summary.json").write_text(json.dumps(summ, indent=2))
print(json.dumps(summ, indent=2))
