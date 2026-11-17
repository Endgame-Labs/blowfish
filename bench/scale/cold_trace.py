"""One cold query per fresh server process with read and stage tracing, then `timeline.py`.

Usage: uv run python bench/scale/cold_trace.py --binary SERVER --namespace NS --workload bm25-msmarco.json --queries 3,10,50 \
         [--warm-process]  (drop caches via POST /_bench/drop_caches and re-run on the same process instead of restarting)
Prints the query text, client ms, and the timeline of every backend read. Reads bench/scale/config.local.
"""
import argparse, json, os, pathlib, shutil, subprocess, sys, time, urllib.request

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
CONFIG = pathlib.Path(os.environ.get("SCALE_CONFIG", HERE / "config.local"))
PORT = int(os.environ.get("SCALE_PORT", "8392"))
URL = f"http://127.0.0.1:{PORT}"
WORK = ROOT / ".scratch/scale/cold"

ap = argparse.ArgumentParser()
ap.add_argument("--binary", required=True); ap.add_argument("--namespace", required=True)
ap.add_argument("--workload", default=str(HERE / "data/bm25-msmarco.json")); ap.add_argument("--queries", default="0")
ap.add_argument("--warm-process", action="store_true"); ap.add_argument("--env", default="")
a = ap.parse_args()
inputs = json.loads(pathlib.Path(a.workload).read_text())
extra = dict(kv.split("=", 1) for kv in a.env.split(",") if kv)


def query(item):
    (WORK / "q.json").write_text(json.dumps([item]))
    res = subprocess.run(["uv", "run", str(ROOT / "crates/bf-engine/examples/scale_api.py"), str(WORK / "q.json"), URL, a.namespace, "1"],
                         capture_output=True, text=True, check=True)
    return json.loads(res.stdout.splitlines()[0])


for qi in [int(x) for x in a.queries.split(",")]:
    shutil.rmtree(WORK, ignore_errors=True); (WORK / "cache").mkdir(parents=True)
    subprocess.run(["python3", str(HERE / "mkconfig.py"), str(CONFIG), str(WORK / "storage.json"), str(WORK / "cache"),
                    "--memory-gib", "8", "--read-only"], check=True)
    env = dict(os.environ, RAYON_NUM_THREADS="6", MALLOC_ARENA_MAX="2", BF_TRACE_READS="1", BF_TRACE_STAGES="1", **extra)
    log = open(WORK / "server.log", "w")
    proc = subprocess.Popen([a.binary, str(WORK / "storage.json"), f"127.0.0.1:{PORT}"], stdout=log, stderr=subprocess.STDOUT, env=env)
    try:
        for _ in range(200):
            try:
                urllib.request.urlopen(URL + "/", timeout=1).read(); break
            except Exception:
                time.sleep(0.2)
        time.sleep(float(os.environ.get("AB_SETTLE", "1.5")))
        if a.warm_process:
            query(inputs[qi])  # warms connections and code, not data
            urllib.request.urlopen(urllib.request.Request(URL + "/_bench/drop_caches", method="POST"), timeout=60).read()
            time.sleep(0.5)
            log.write("RESET\n"); log.flush()
        t0 = time.time()
        row = query(inputs[qi])
        ms = row["ms"]
    finally:
        proc.terminate(); proc.wait()
    text = open(WORK / "server.log").read()
    text = text.split("RESET\n")[-1]
    (WORK / "q.log").write_text(text)
    print(f"=== query {qi}: {inputs[qi]['request']['rank_by'][2]!r}  client {ms:.0f} ms  top {[r['id'] for r in row['response']['rows']][:3]}")
    print("\n".join(l for l in text.splitlines() if l.startswith(("QUERY", "WARM", "T ", "STAGE q", "FTS"))))
    print(subprocess.run([sys.executable, str(HERE / "timeline.py"), str(WORK / "q.log")] + (["--all"] if os.environ.get("ALL") else []),
                         capture_output=True, text=True).stdout)
shutil.rmtree(WORK, ignore_errors=True)
