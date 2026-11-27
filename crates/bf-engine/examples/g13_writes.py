# /// script
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Round G13: the warm graph with and without a light concurrent write stream.

Usage: uv run g13_writes.py RUN_DIR BINARY LABEL CAP_MINUTES [SECONDS_PER_PHASE]
RUN_DIR holds store/ (a writable copy of the NQ fixture store) and fixture/data
({corpus.f16,queries.npy,truth.npy}). Starts a server with indexing off (so writes stay in the
unindexed tail and shadow base rows), heats it with graph_heat.py until the graph is `usable`,
then for top_k 10 and 100 runs 16 single-connection clients with no writes, then with a writer
upserting existing IDs (same vector, so the exhaustive truth is unchanged) at 50 requests per
second. Reports q/s, p50/p99, recall@k, the share of queries the graph answered, and write
latency. Results: RUN_DIR/LABEL-result.json. The server and heater are always stopped.
"""
import http.client
import json
import multiprocessing as mp
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time

import numpy as np

run, binary, label, cap, *rest = sys.argv[1:]
run, binary, cap = pathlib.Path(run).resolve(), pathlib.Path(binary).resolve(), float(cap) * 60
seconds = float(rest[0]) if rest else 90.0
here = pathlib.Path(__file__).resolve().parent
PORT = 18443
CLIENTS = 16
WRITE_RATE = 50
data = run / "fixture/data"
(run / "tmp").mkdir(exist_ok=True)
(run / "server.toml").write_text("\n".join([
    f'listen = "127.0.0.1:{PORT}"', "require_auth = false", "[engine]", f"indexing = {'true' if os.environ.get('T1_INDEXING') else 'false'}",
    "ann_nprobe = 64", "[storage]", f'url = "{(run / "store").as_uri()}"', "[storage.cache]",
    f"memory_bytes = {4 * 2**30}", "disk_bytes = 0", "block_size = 4000",
    "max_coalesce_blocks = 512", "fetch_concurrency = 512"]) + "\n")
env = dict(os.environ, RAYON_NUM_THREADS="8", TOKIO_WORKER_THREADS="8", BF_ANN_NPROBE="64",
           BF_DECODED_CACHE_BYTES=str(4 * 2**30), TMPDIR=str(run / "tmp"))


def get(path):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=120)
    try:
        c.request("GET", path)
        r = c.getresponse()
        raw = r.read()
        if r.status != 200:
            raise RuntimeError((r.status, raw[:200]))
        return raw
    finally:
        c.close()


def status():
    return json.loads(get("/_blowfish/v1/status"))["hot_graphs"].get("nq", {"graphs": []})


def tail_rows():
    try:
        return json.loads(get("/v2/namespaces/nq/metadata")).get("index", {}).get("unindexed_rows")
    except Exception:  # noqa: BLE001
        return None


def client(index, k, bodies, truth, stop_at, out):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
    lat, hit, n = [], 0, 0
    i = index
    while time.monotonic() < stop_at:
        t = time.perf_counter()
        c.request("POST", "/v2/namespaces/nq/query", bodies[i % len(bodies)],
                  {"content-type": "application/json"})
        r = c.getresponse()
        raw = r.read()
        lat.append(time.perf_counter() - t)
        if r.status != 200:
            raise RuntimeError((r.status, raw[:200]))
        ids = {int(row["id"]) for row in json.loads(raw)["rows"]}
        hit += len(ids & set(map(int, truth[i % len(bodies)][:k])))
        n += 1
        i += CLIENTS
    out.put((n, hit, lat))


def writer(stop, lat, corpus, nrows, errors):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
    rng = np.random.default_rng(13)
    due = time.monotonic()
    while not stop.is_set():
        row = int(rng.integers(0, nrows))
        body = json.dumps({"upsert_rows": [{"id": row, "vector": corpus[row].astype(np.float32).tolist()}]},
                          separators=(",", ":"))
        t = time.perf_counter()
        try:
            c.request("POST", "/v2/namespaces/nq", body, {"content-type": "application/json"})
            r = c.getresponse()
            raw = r.read()
            if r.status != 200:
                errors.append((r.status, raw[:200]))
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))
            c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
        lat.append(time.perf_counter() - t)
        due += 1.0 / WRITE_RATE
        stop.wait(max(0.0, due - time.monotonic()))


def phase(k, writes, queries, truth, corpus, perf_seconds=0):
    bodies = [json.dumps({"rank_by": ["vector", "ANN", q.tolist()], "top_k": k, "include_attributes": False},
                         separators=(",", ":")).encode() for q in queries]
    # Warm up for a few seconds, untimed, so caches hold the working set.
    q = mp.Queue()
    stop_at = time.monotonic() + 8
    ps = [mp.Process(target=client, args=(i, k, bodies, truth, stop_at, q)) for i in range(CLIENTS)]
    [p.start() for p in ps]
    [q.get() for _ in ps]
    [p.join() for p in ps]
    before = status()["answered"]
    stop, wlat, werr = threading.Event(), [], []
    wt = None
    if writes:
        wt = threading.Thread(target=writer, args=(stop, wlat, corpus, len(corpus), werr))
        wt.start()
        time.sleep(1)
    t0 = time.monotonic()
    stop_at = t0 + seconds
    tails = []

    def sample():
        while time.monotonic() < stop_at:
            tails.append((round(time.monotonic() - t0), tail_rows()))
            time.sleep(5)
    threading.Thread(target=sample, daemon=True).start()
    if perf_seconds and writes:
        # G13_PERF: profile the server in the middle of the phase (diagnostics only).
        threading.Timer(seconds / 3, lambda: subprocess.run(
            ["sudo", "perf", "record", "-F", "199", "-g", "-p", str(server.pid), "-o",
             str(run / f"{label}-k{k}.perf"), "--", "sleep", str(perf_seconds)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)).start()
    q = mp.Queue()
    ps = [mp.Process(target=client, args=(i, k, bodies, truth, stop_at, q)) for i in range(CLIENTS)]
    [p.start() for p in ps]
    res = [q.get() for _ in ps]
    elapsed = time.monotonic() - t0
    [p.join() for p in ps]
    stop.set()
    if wt:
        wt.join()
    s = status()
    n = sum(r[0] for r in res)
    lat = np.array(sorted(x for r in res for x in r[2]))
    out = {"k": k, "writes": writes, "queries": n, "qps": n / elapsed,
           "recall": sum(r[1] for r in res) / (n * k), "p50_ms": float(lat[len(lat) // 2] * 1e3),
           "p99_ms": float(lat[int(len(lat) * .99)] * 1e3),
           "tail_rows": tails, "graph_answers": s["answered"] - before,
           "graph_share": (s["answered"] - before) / n,
           "usable": bool(s["graphs"] and s["graphs"][0]["calibration"] and s["graphs"][0]["calibration"]["usable"])}
    if writes:
        w = np.array(sorted(wlat))
        out.update(writes_done=len(wlat), write_errors=len(werr), write_p50_ms=float(w[len(w) // 2] * 1e3),
                   write_p99_ms=float(w[int(len(w) * .99)] * 1e3), write_rate=len(wlat) / elapsed)
    return out


server = heater = None
try:
    sf = (run / "server.log").open("wb")
    server = subprocess.Popen([str(binary), "serve", "--config", str(run / "server.toml")],
                              stdout=sf, stderr=sf, env=env, start_new_session=True)
    for _ in range(180):
        try:
            status()
            break
        except OSError:
            time.sleep(1)
    else:
        raise SystemExit("server did not start")
    begun = time.monotonic()
    heater = subprocess.Popen(["uv", "run", str(here / "graph_heat.py"), str(run), str(PORT), "800",
                               "--production"], stdout=(run / "heater.log").open("wb"),
                              stderr=subprocess.STDOUT, start_new_session=True)
    promoted = None
    smoke = bool(os.environ.get("G13_SMOKE"))  # skip promotion: exercise the phases only
    while not smoke and time.monotonic() - begun < cap:
        if server.poll() is not None or heater.poll() is not None:
            raise SystemExit("server or heater exited")
        s = status()
        g = s["graphs"][0] if s["graphs"] else None
        cal = g and g["calibration"]
        print(json.dumps({"t": round(time.monotonic() - begun), "heat": g and round(g["heat_seconds"]),
                          "building": g and g["building"], "usable": cal and cal["usable"],
                          "samples": cal and cal["samples"]}), flush=True)
        if cal and cal["usable"]:
            promoted = time.monotonic() - begun
            break
        time.sleep(30)
    result = {"label": label, "promoted_s": promoted, "phases": []}
    if promoted is not None or smoke:
        (run / "pause").touch()
        os.killpg(heater.pid, signal.SIGTERM)
        heater.wait(timeout=20)
        heater = None
        time.sleep(2)
        queries = np.load(data / "queries.npy")
        truth = np.load(data / "truth.npy")
        dims = queries.shape[1]
        rows = (data / "corpus.f16").stat().st_size // (2 * dims)
        corpus = np.memmap(data / "corpus.f16", dtype=np.float16, mode="r", shape=(rows, dims))
        phases = [(10, True), (100, True)] if os.environ.get("T1_WRITES_ONLY") else [(10, False), (100, False), (10, True), (100, True)]
        for k, writes in phases:
            r = phase(k, writes, queries, truth, corpus, int(os.environ.get("G13_PERF", "0")))
            print(json.dumps(r), flush=True)
            result["phases"].append(r)
        result["final_status"] = status()
    (run / f"{label}-result.json").write_text(json.dumps(result, indent=1))
finally:
    for p in (heater, server):
        if p and p.poll() is None:
            os.killpg(p.pid, signal.SIGTERM)
