# /// script
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Round G15 (from g11_promote.py): promote a graph under ordinary heat, stop the heat, then hold the server for
g15_http.py until RUN_DIR/release exists. No unfiltered measurement runs (g11_http.py reads Mac counters).

Usage: uv run g15_promote.py RUN_DIR BINARY LABEL CAP_MINUTES
RUN_DIR holds fixture/{store,data}. Starts the server with the production recall sampler (default
rate and interval), runs graph_heat.py (16 paced clients) against it, and logs the graph's
lifecycle every 30 seconds: heat, build, calibration window, cumulative samples (the
blowfish_ann_recall histogram count, counting every sample that reaches the controller), effort,
and `usable`. Stops at the first `usable` graph or after CAP_MINUTES. Then, for each K, runs
g8_http.py (16 client processes) against the promoted graph. Results: RUN_DIR/LABEL-*.json
and RUN_DIR/LABEL-timeline.jsonl. The caller owns RUN_DIR and its cleanup; the server and
heater are always stopped.
"""
import http.client
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time

run, binary, label, cap = sys.argv[1:]
run, binary, cap = pathlib.Path(run).resolve(), pathlib.Path(binary).resolve(), float(cap) * 60
here = pathlib.Path(__file__).resolve().parent
probes = os.environ.get("G8_PROBES", "64")
rate = os.environ.get("G10_HEAT_QPS", "800")
(run / "tmp").mkdir(exist_ok=True)
(run / "server.toml").write_text(
    "\n".join([
        'listen = "127.0.0.1:18443"', "require_auth = false", "[engine]", "indexing = false",
        f"ann_nprobe = {probes}", "[storage]",
        f'url = "{(run / "fixture/store").as_uri()}"', "read_only = true", "[storage.cache]",
        f"memory_bytes = {4 * 2**30}", "disk_bytes = 0", "block_size = 4000",
        "max_coalesce_blocks = 512", "fetch_concurrency = 512",
    ]) + "\n")
env = dict(os.environ, RAYON_NUM_THREADS="8", TOKIO_WORKER_THREADS="8", BF_ANN_NPROBE=probes,
           BF_DECODED_CACHE_BYTES=str(4 * 2**30), TMPDIR=str(run / "tmp"))


def get(path):
    conn = http.client.HTTPConnection("127.0.0.1", 18443, timeout=120)
    try:
        conn.request("GET", path)
        r = conn.getresponse()
        raw = r.read()
        if r.status != 200:
            raise RuntimeError((r.status, raw[:200]))
        return raw
    finally:
        conn.close()


def status():
    return json.loads(get("/_blowfish/v1/status"))["hot_graphs"].get("nq", {"graphs": []})


def sample_count():
    text = get("/metrics")
    # Main counts every sample in the blowfish_ann_recall histogram; this branch also counts the
    # cheap (union) samples in blowfish_ann_calibration_samples_total.
    total = sum(int(float(v)) for v in re.findall(
        rb"^blowfish_ann_calibration_samples_total\{[^}]*\} (\S+)", text, re.M))
    m = re.search(rb"^blowfish_ann_recall_count\S* (\d+)", text, re.M)
    return total or (int(m.group(1)) if m else 0)


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
    begun = time.monotonic()
    heater = subprocess.Popen(["uv", "run", str(here / "graph_heat.py"), str(run), "18443", rate,
                               "--production"], stdout=(run / "heater.log").open("wb"),
                              stderr=subprocess.STDOUT, start_new_session=True)
    out = (run / f"{label}-timeline.jsonl").open("w")
    promoted = None
    built = None
    while time.monotonic() - begun < cap:
        if server.poll() is not None or heater.poll() is not None:
            raise SystemExit("server or heater exited")
        t = time.monotonic() - begun
        s = status()
        g = s["graphs"][0] if s["graphs"] else None
        cal = g and g["calibration"]
        row = {"t": round(t), "samples_total": sample_count(), "heat": g and round(g["heat_seconds"]),
               "building": g and g["building"], "query_rate": g and round(g["query_rate"]),
               "cal": cal and {k: cal[k] for k in ("ef", "trial_ef", "usable", "samples",
                                                  "paired_quality", "build_wall_seconds")}}
        out.write(json.dumps(row) + "\n")
        out.flush()
        print(json.dumps(row), flush=True)
        if cal and built is None:
            built = t
        if cal and cal["usable"]:
            promoted = t
            break
        time.sleep(30)
    print(json.dumps({"label": label, "promoted_s": promoted, "graph_ready_s": built,
                      "samples_total": sample_count(), "cap_s": cap}), flush=True)
    (run / f"{label}-result.json").write_text(json.dumps({
        "promoted_s": promoted, "graph_ready_s": built, "samples_total": sample_count(),
        "cap_s": cap, "final": status()}))
    if promoted is not None:
        # G11: after promotion, stop the heater and measure each K in order with latency, polling the
        # graph status every 5 s into LABEL-poll.jsonl so a fall back to IVF is visible when it happens.
        (run / "pause").touch()
        os.killpg(heater.pid, signal.SIGTERM)
        heater.wait(timeout=20)
        heater = None
        time.sleep(2)
        import threading
        stop_poll = threading.Event()

        def poll():
            with (run / f"{label}-poll.jsonl").open("w") as f:
                while not stop_poll.is_set():
                    try:
                        s = status()
                        g = s["graphs"][0] if s["graphs"] else None
                        f.write(json.dumps({"t": round(time.monotonic() - begun, 1), "phase": phase[0],
                                            "graph": bool(g), "answered": s.get("answered"),
                                            "usable": bool(g and g["calibration"] and g["calibration"]["usable"]),
                                            "cal": g and g["calibration"]}) + "\n")
                        f.flush()
                    except Exception as e:  # noqa: BLE001
                        f.write(json.dumps({"t": round(time.monotonic() - begun, 1), "error": repr(e)}) + "\n")
                    stop_poll.wait(5)

        phase = ["start"]
        threading.Thread(target=poll, daemon=True).start()

        phase[0] = "hold"
        # Hold the promoted server for profiling until RUN_DIR/release exists.
        while not (run / "release").exists():
            if server.poll() is not None:
                break
            time.sleep(2)
finally:
    for p in (heater, server):
        if p and p.poll() is None:
            os.killpg(p.pid, signal.SIGTERM)
