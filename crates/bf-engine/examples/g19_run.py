# /// script
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Round G19: the general path's waste, and the typed ID path under writes, on one promoted server.

Usage: uv run g19_run.py RUN_DIR BINARY LABEL CAP_MINUTES [SECONDS]
RUN_DIR holds store/ (a writable clone of the NQ fixture store) and fixture/data/{queries.npy,truth.npy,
corpus.f16}. The binary is a candidate with a temporary A/B switch (BF_G19_SWITCH names a file holding a
number, read every 500 ms: bit 1 runs the v0.1.68 general path, bit 2 makes the typed ID path decline when
a tail or shadowed rows exist). Starts the server with indexing off, promotes the graph under ordinary heat
(graph_heat.py), then on the same server runs:

  Item 1 (empty tail, 16 clients): shapes `attr` (top_k 10, include_attributes ["id"]) and `filter`
    (top_k 10, id < 10% of the rows), arms legacy/new in the order L N N L L N N L. Each arm runs one serial
    quality pass (SHA-256 of every response's rows) and a timed pass.
  Item 2 (steady writes, 16 clients, IDs only): top_k 10 and 100, arms v0.1.68 (bits 3) and new (bits 0) in
    the order O N N O, with a writer upserting existing IDs with their stored vectors at 50 a second and a
    prober that writes a fresh row and immediately queries for it (read your writes). Then the writer stops
    and, on the frozen tail, both arms answer every query again: rows must hash the same.

Results: RUN_DIR/LABEL-result.json. The server, heater, writer, and prober are always stopped.
"""
import hashlib
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

mp.set_start_method("fork")
run, binary, label, cap, *rest = sys.argv[1:]
run, binary, cap = pathlib.Path(run).resolve(), pathlib.Path(binary).resolve(), float(cap) * 60
seconds = float(rest[0]) if rest else 20.0
here = pathlib.Path(__file__).resolve().parent
PORT = 18443
CLIENTS = 16
WRITE_RATE = 50
ROUTE = "/v2/namespaces/nq/query"
data = run / "fixture/data"
switch = run / "switch"
(run / "tmp").mkdir(exist_ok=True)
switch.write_text("0")
(run / "server.toml").write_text("\n".join([
    f'listen = "127.0.0.1:{PORT}"', "require_auth = false", "[engine]", "indexing = false",
    "ann_nprobe = 64", "[storage]", f'url = "{(run / "store").as_uri()}"', "[storage.cache]",
    f"memory_bytes = {4 * 2**30}", "disk_bytes = 0", "block_size = 4000",
    "max_coalesce_blocks = 512", "fetch_concurrency = 512"]) + "\n")
env = dict(os.environ, RAYON_NUM_THREADS="8", TOKIO_WORKER_THREADS="8", BF_ANN_NPROBE="64",
           BF_DECODED_CACHE_BYTES=str(4 * 2**30), TMPDIR=str(run / "tmp"), BF_G19_SWITCH=str(switch))


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


def post(c, route, body):
    c.request("POST", route, body, {"content-type": "application/json"})
    r = c.getresponse()
    raw = r.read()
    if r.status != 200:
        raise RuntimeError((r.status, raw[:300]))
    return raw


def body_for(shape, k, q):
    body = {"rank_by": ["vector", "ANN", q.tolist()], "top_k": k}
    if shape == "ids":
        body["include_attributes"] = False
    elif shape == "attr":
        body["include_attributes"] = ["id"]
    elif shape == "filter":
        body["include_attributes"] = False
        body["filters"] = ["id", "Lt", FILTER_BELOW]
    return json.dumps(body, separators=(",", ":")).encode()


def client(index, bodies, seconds, barrier, out):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
    c.connect()
    lat = []
    barrier.wait()
    end = time.perf_counter() + seconds
    i = index
    while time.perf_counter() < end:
        t = time.perf_counter()
        post(c, ROUTE, bodies[i % len(bodies)])
        lat.append(time.perf_counter() - t)
        i += CLIENTS
    out.put(lat)


def cpu_seconds(pid):
    text = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    minutes, rest = text.split(":")
    return int(minutes) * 60 + float(rest)


def set_bits(bits):
    switch.write_text(str(bits))
    time.sleep(1.2)


def quality(bodies, truth, k):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
    digest, overlap = hashlib.sha256(), 0
    for i, body in enumerate(bodies):
        rows = json.loads(post(c, ROUTE, body))["rows"]
        digest.update(json.dumps(rows, separators=(",", ":")).encode())
        if truth is not None:
            overlap += len({int(r["id"]) for r in rows} & set(map(int, truth[i][:k])))
    c.close()
    return digest.hexdigest(), (overlap / (len(bodies) * k) if truth is not None else None)


def timed(bodies, secs, pid):
    barrier, out = mp.Barrier(CLIENTS + 1), mp.Queue()
    procs = [mp.Process(target=client, args=(i, bodies, secs, barrier, out)) for i in range(CLIENTS)]
    for p in procs:
        p.start()
    barrier.wait()
    cpu0, t0 = cpu_seconds(pid), time.perf_counter()
    lat = np.concatenate([np.array(out.get(timeout=secs + 300)) for _ in procs])
    elapsed = time.perf_counter() - t0
    cpu1 = cpu_seconds(pid)
    for p in procs:
        p.join()
    return {"queries": int(len(lat)), "qps": len(lat) / elapsed, "p50_ms": float(np.percentile(lat, 50) * 1e3),
            "p99_ms": float(np.percentile(lat, 99) * 1e3), "cpu_us_per_query": (cpu1 - cpu0) / len(lat) * 1e6}


def arm(name, bits, bodies, truth, k, secs, pid, hashes=True):
    set_bits(bits)
    r = {"arm": name, "bits": bits}
    if hashes:
        r["rows_sha256"], r["recall"] = quality(bodies, truth, k)
    r.update(timed(bodies, secs, pid))
    print(json.dumps(r), flush=True)
    return r


def writer(stop, lat, errors, corpus, wrote):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
    rng = np.random.default_rng(19)
    due = time.monotonic()
    while not stop.is_set():
        row = int(rng.integers(0, len(corpus)))
        body = json.dumps({"upsert_rows": [{"id": row, "vector": corpus[row].astype(np.float32).tolist()}]},
                          separators=(",", ":"))
        t = time.perf_counter()
        try:
            post(c, "/v2/namespaces/nq", body)
            wrote.append(row)
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))
            c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
        lat.append(time.perf_counter() - t)
        due += 1.0 / WRITE_RATE
        stop.wait(max(0.0, due - time.monotonic()))


def prober(stop, queries, counts):
    """Write a fresh row whose vector is a query vector, then ask for it: it must lead (distance 0)."""
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=300)
    n = 0
    while not stop.is_set():
        n += 1
        q = queries[n % len(queries)]
        fresh = 4_000_000_000 + n
        post(c, "/v2/namespaces/nq", json.dumps({"upsert_rows": [{"id": fresh, "vector": q.tolist()}]},
                                                separators=(",", ":")))
        rows = json.loads(post(c, ROUTE, json.dumps({"rank_by": ["vector", "ANN", q.tolist()], "top_k": 10,
                                                     "include_attributes": False}, separators=(",", ":"))))["rows"]
        counts["probes"] += 1
        if fresh not in [int(r["id"]) for r in rows]:
            counts["missed"] += 1
        stop.wait(0.2)


server = heater = None
try:
    sf = (run / "server.log").open("wb")
    server = subprocess.Popen([str(binary), "serve", "--config", str(run / "server.toml")],
                              stdout=sf, stderr=sf, env=env, start_new_session=True)
    (run / "server.pid").write_text(str(server.pid))
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
    (run / "heater.pid").write_text(str(heater.pid))
    promoted = None
    smoke = bool(os.environ.get("G19_SMOKE"))  # skip promotion: exercise every phase briefly
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
    result = {"label": label, "promoted_s": promoted}
    if promoted is not None or smoke:
        (run / "pause").touch()
        os.killpg(heater.pid, signal.SIGTERM)
        heater.wait(timeout=20)
        heater = None
        time.sleep(2)
        queries = np.load(data / "queries.npy")
        truth = np.load(data / "truth.npy")
        dims = queries.shape[1]
        total_rows = 2_681_468
        FILTER_BELOW = total_rows // 10
        pid = server.pid
        if os.environ.get("G19_HOLD"):
            # Interactive mode: hold the promoted server and run what RUN_DIR/cmd.json says (shape, k,
            # seconds, bits, writes), writing RUN_DIR/cmd.out, until RUN_DIR/release exists.
            stop_w, w_thread, corpus_h = threading.Event(), None, None
            while not (run / "release").exists():
                cmd_file = run / "cmd.json"
                if not cmd_file.exists():
                    time.sleep(1)
                    continue
                cmd = json.loads(cmd_file.read_text())
                cmd_file.unlink()
                if cmd.get("writes") and w_thread is None:
                    corpus_h = np.memmap(data / "corpus.f16", dtype=np.float16, mode="r",
                                         shape=((data / "corpus.f16").stat().st_size // (2 * dims), dims))
                    stop_w.clear()
                    w_thread = threading.Thread(target=writer, args=(stop_w, [], [], corpus_h, []))
                    w_thread.start()
                    time.sleep(2)
                if cmd.get("writes") is False and w_thread is not None:
                    stop_w.set()
                    w_thread.join()
                    w_thread = None
                if "shape" in cmd:
                    bodies = [body_for(cmd["shape"], cmd.get("k", 10), q) for q in queries]
                    set_bits(cmd.get("bits", 0))
                    r = timed(bodies, cmd.get("seconds", 30), pid)
                else:
                    r = {"ok": True}
                (run / "cmd.out").write_text(json.dumps(r))
            stop_w.set()
            if w_thread:
                w_thread.join()
            raise SystemExit(0)
        # ---- Item 1: the general path, empty tail.
        result["item1"] = {}
        for shape in ("attr", "filter"):
            bodies = [body_for(shape, 10, q) for q in queries]
            tr = truth if shape == "attr" else None
            # Warm both code paths and the caches.
            set_bits(0)
            timed(bodies, 6, pid)
            runs = [arm("legacy" if b else "new", b, bodies, tr, 10, seconds, pid)
                    for b in (1, 0, 0, 1, 1, 0, 0, 1)]
            result["item1"][shape] = runs
        # ---- Item 2: steady writes.
        rows = (data / "corpus.f16").stat().st_size // (2 * dims)
        corpus = np.memmap(data / "corpus.f16", dtype=np.float16, mode="r", shape=(rows, dims))
        stop, wlat, werr, wrote = threading.Event(), [], [], []
        counts = {"probes": 0, "missed": 0}
        wt = threading.Thread(target=writer, args=(stop, wlat, werr, corpus, wrote))
        wt.start()
        time.sleep(2)
        probe_stop = threading.Event()
        pt = threading.Thread(target=prober, args=(probe_stop, queries, counts))
        result["item2"] = {}
        for k in (10, 100):
            bodies = [body_for("ids", k, q) for q in queries]
            set_bits(0)
            timed(bodies, 8, pid)
            runs = []
            for name, bits in (("v0.1.68", 3), ("new", 0), ("new", 0), ("v0.1.68", 3)):
                if k == 10 and not pt.is_alive() and not runs:
                    pt.start()
                set_bits(bits)
                r = {"arm": name, "bits": bits, "tail_writes": len(wrote)}
                r.update(timed(bodies, max(seconds, 30), pid))
                s = status()
                r["graph_answers_total"] = s.get("answered")
                print(json.dumps(r), flush=True)
                runs.append(r)
            result["item2"][f"k{k}"] = runs
        probe_stop.set()
        if pt.is_alive():
            pt.join()
        stop.set()
        wt.join()
        w = np.array(sorted(wlat))
        result["writer"] = {"writes": len(wlat), "errors": len(werr), "p50_ms": float(w[len(w) // 2] * 1e3),
                            "p99_ms": float(w[int(len(w) * .99)] * 1e3)}
        result["prober"] = counts
        # ---- Frozen tail: both arms answer every query; the rows must hash the same.
        result["frozen"] = {}
        for k in (10, 100):
            bodies = [body_for("ids", k, q) for q in queries]
            hashes = {}
            for name, bits in (("v0.1.68", 3), ("new", 0)):
                set_bits(bits)
                hashes[name] = quality(bodies, truth, k)
            result["frozen"][f"k{k}"] = {"hashes": hashes, "identical": hashes["v0.1.68"][0] == hashes["new"][0]}
            print(json.dumps({"frozen": k, **result["frozen"][f"k{k}"]}), flush=True)
        result["final_status"] = status()
    (run / f"{label}-result.json").write_text(json.dumps(result, indent=1))
finally:
    for p in (heater, server):
        if p and p.poll() is None:
            os.killpg(p.pid, signal.SIGTERM)
