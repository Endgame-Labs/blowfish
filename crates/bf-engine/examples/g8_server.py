# /// script
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Rounds G8 and G9: production blowfish on the M4, graph promotion by ordinary heat, then top_k runs.

Usage: uv run g8_server.py RUN_DIR BINARY LABEL K [K ...]
RUN_DIR holds fixture/{store,data}. Starts the server, optionally measures the IVF path first
(G8_PRE=1), heats with top_k 10 until the graph is built, feeds calibration through the ordinary
recall endpoint until the graph is usable, then runs g8_http.py (16 client processes) for each K.
Results: RUN_DIR/LABEL-<tag>-k<K>.json. The caller owns RUN_DIR and its cleanup; the server is
always stopped.

G8_SAMPLERS: concurrent recall samples (default 4). G8_QUIET=1: pause the heater once the graph
exists and send one sample at a time. Builds before the Round G9 cost fix never promote on a busy
server (warm ordinary IVF is compared with a cold graph), so they need it; the fixed build does not.
"""
import concurrent.futures
import http.client
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

import numpy as np

run, binary, label, *ks = sys.argv[1:]
run, binary = pathlib.Path(run).resolve(), pathlib.Path(binary).resolve()
here = pathlib.Path(__file__).resolve().parent
probes = os.environ.get("G8_PROBES", "64")
quiet = bool(os.environ.get("G8_QUIET"))
(run / "tmp").mkdir(exist_ok=True)
(run / "server.toml").write_text(
    "\n".join([
        'listen = "127.0.0.1:18443"', "require_auth = false", "[engine]", "indexing = false",
        f"ann_nprobe = {probes}", "recall_sample_rate = 0.0", "[storage]",
        f'url = "{(run / "fixture/store").as_uri()}"', "read_only = true", "[storage.cache]",
        f"memory_bytes = {4 * 2**30}", "disk_bytes = 0", "block_size = 4000",
        "max_coalesce_blocks = 512", "fetch_concurrency = 512",
    ]) + "\n")
env = dict(os.environ, RAYON_NUM_THREADS="8", TOKIO_WORKER_THREADS="8", BF_ANN_NPROBE=probes,
           BF_DECODED_CACHE_BYTES=str(4 * 2**30), BF_RECALL_SAMPLE_INTERVAL_MS=str(24 * 3600 * 1000),
           TMPDIR=str(run / "tmp"), **{k: v for k, v in os.environ.items() if k.startswith("G8_SRV_")})


def api(method, path, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", 18443, timeout=600)
    try:
        conn.request(method, path, None if body is None else json.dumps(body).encode(),
                     {"content-type": "application/json"})
        r = conn.getresponse()
        raw = r.read()
        if r.status != 200:
            raise RuntimeError((r.status, raw[:300]))
        return json.loads(raw)
    finally:
        conn.close()


def status():
    return api("GET", "/_blowfish/v1/status")["hot_graphs"].get("nq", {"graphs": []})


server = heater = None
try:
    sf = (run / "server.log").open("wb")
    server = subprocess.Popen([str(binary), "serve", "--config", str(run / "server.toml")],
                              stdout=sf, stderr=sf, env=env, start_new_session=True)
    for _ in range(120):
        try:
            status()
            break
        except OSError:
            time.sleep(1)
    else:
        raise SystemExit("server did not start")

    def measure(tag):
        for k in ks:
            e = dict(os.environ, G3_CLIENT_PROCESSES="16", G3_CLIENT_THREADS="1", G8_K=k)
            subprocess.run(["uv", "run", "--python", "3.12", "--no-project", "--with", "numpy", "python",
                            str(here / "g8_http.py"), str(run / "fixture/data"), "bf", "18443", "0",
                            os.environ.get("G8_REPEATS", "2"), str(run / f"{label}-{tag}-k{k}"),
                            str(server.pid)], env=e, check=True)

    if os.environ.get("G8_PRE"):
        measure("ivf")
    heater = subprocess.Popen(["uv", "run", str(here / "graph_heat.py"), str(run), "18443", "800",
                               "--production"], stdout=(run / "heater.log").open("wb"),
                              stderr=subprocess.STDOUT, start_new_session=True)
    queries = np.load(run / "fixture/data/queries.npy")
    workers = int(os.environ.get("G8_SAMPLERS", "1" if quiet else "4"))
    pool = concurrent.futures.ThreadPoolExecutor(workers)
    begun, samples, pending, last_log = time.monotonic(), 0, set(), 0.0

    def sample(i):
        api("POST", "/v1/namespaces/nq/_debug/recall",
            {"num": 1, "top_k": 10,
             "rank_by": ["vector", "ANN", queries[(i * 47 + 19) % len(queries)].tolist()]})

    while True:
        if server.poll() is not None or heater.poll() is not None:
            raise SystemExit("server or heater exited")
        cal = next((g["calibration"] for g in status()["graphs"] if g["calibration"]), None)
        if cal and quiet:
            (run / "pause").touch()
        if time.monotonic() - last_log > 60:
            last_log = time.monotonic()
            print(f"t={last_log - begun:.0f}s sent={samples}", json.dumps(cal and {
                k: cal[k] for k in ("ef", "trial_ef", "usable", "samples", "paired_quality")}), flush=True)
        if cal and cal["usable"]:
            break
        if cal:
            # Concurrent samples only raise the sampling rate; each is the same ordinary request.
            pending = {f for f in pending if not f.done()}
            while len(pending) < workers:
                pending.add(pool.submit(sample, samples))
                samples += 1
            time.sleep(1)
        else:
            time.sleep(2)
        if time.monotonic() - begun > 3 * 3600:
            raise SystemExit("promotion exceeded three hours")
    (run / "pause").touch()
    os.killpg(heater.pid, signal.SIGTERM)
    heater.wait(timeout=20)
    heater = None
    time.sleep(2)
    print("promoted", json.dumps(status()), flush=True)
    measure("graph")
    print("after", json.dumps(status()), flush=True)
finally:
    for p in (heater, server):
        if p and p.poll() is None:
            os.killpg(p.pid, signal.SIGTERM)
