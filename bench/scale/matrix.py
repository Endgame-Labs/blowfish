"""Small HTTP query matrix: warm at concurrency 1/8/32 and cold (fresh process and disk cache
per query) at concurrency 1, for ann, bm25, hybrid, and selective filtered ANN (strong consistency).

Usage: uv run --with numpy python bench/scale/matrix.py [--cold-queries 8] [--kinds ann,bm25,...]
Reads bench/scale/config.local. Writes $SCALE_SCRATCH/out/matrix-*.json and prints markdown tables.
Server knobs come from the environment (BF_ANN_NPROBE, BF_ANN_RERANK, BF_DECODED_CACHE_BYTES, ...).
"""
import argparse, json, os, pathlib, re, shutil, subprocess, time, urllib.request
import numpy as np

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SCRATCH = pathlib.Path(os.environ.get("SCALE_SCRATCH", ROOT / ".scratch/scale"))
OUT = SCRATCH / "out"; OUT.mkdir(parents=True, exist_ok=True)
CONFIG = pathlib.Path(os.environ.get("SCALE_CONFIG", HERE / "config.local"))
NS = json.loads(CONFIG.read_text()).get("namespace", "wikipedia-10m")
URL = f"http://127.0.0.1:{os.environ.get('SCALE_PORT', '8300')}"
UNIT = "scale-server"
# Ground truth is computed by the loader over exactly the rows it loaded; fall back to the
# checked-in 10M truth when the loader scratch is absent.
_loaded = SCRATCH / "load/truth.npz"
truth = np.load(_loaded if _loaded.exists() else HERE / "data/truth.npz")
TRUTH_KEY = {"ann": "ids", "selective": "selective_ids"}


def sh(*cmd, **kw):
    return subprocess.run(cmd, check=True, **kw)


def stop():
    subprocess.run(["systemctl", "--user", "stop", f"{UNIT}.scope"], capture_output=True)
    time.sleep(1)


def start(cache):
    storage = OUT / "storage.json"
    sh("python3", str(HERE / "mkconfig.py"), str(CONFIG), str(storage), str(cache), "--memory-gib", os.environ.get("SCALE_MEMORY_GIB", "8"), "--read-only")
    log = open(OUT / "server.log", "a")
    proc = subprocess.Popen([str(HERE / "serve.sh"), str(storage)], stdout=log, stderr=subprocess.STDOUT)
    for _ in range(120):
        if proc.poll() is not None:
            raise RuntimeError("server exited")
        try:
            urllib.request.urlopen(URL + "/", timeout=1).read(); return proc
        except Exception:
            time.sleep(1)
    raise TimeoutError("server readiness")


def api(workload, concurrency, tag):
    out, err = OUT / f"{tag}.jsonl", OUT / f"{tag}.err"
    with open(out, "w") as o, open(err, "w") as e:
        sh("uv", "run", str(ROOT / "crates/bf-engine/examples/scale_api.py"), str(workload), URL, NS, str(concurrency),
           stdout=o, stderr=e)
    rows = [json.loads(l) for l in open(out) if l.startswith("{")]
    stats = [json.loads(l) for l in open(err) if l.startswith("{")][-1]
    return rows, stats


def ops():
    raw = urllib.request.urlopen(URL + "/metrics", timeout=10).read().decode()
    res = {}
    for line in raw.splitlines():
        if line.startswith("blowfish_object_store_requests_total{"):
            op = re.search(r'op="([^"]+)"', line)[1]; res[op] = res.get(op, 0) + float(line.rsplit(" ", 1)[1])
        if line.startswith("blowfish_object_store_bytes_total{"):
            op = "bytes_" + re.search(r'op="([^"]+)"', line)[1]; res[op] = res.get(op, 0) + float(line.rsplit(" ", 1)[1])
    return res


def rss_peak_gib():
    """Peak resident set of the running server process (VmHWM), from its systemd scope."""
    pids = subprocess.run(["systemctl", "--user", "show", f"{UNIT}.scope", "-p", "MainPID", "--value"],
                          capture_output=True, text=True).stdout.split()
    # The scope's main PID is the server itself when started through systemd-run --scope.
    candidates = [p for p in pids if p.isdigit() and p != "0"]
    if not candidates:
        cg = subprocess.run(["pgrep", "-f", "blowfish-scale-server"], capture_output=True, text=True).stdout.split()
        candidates = cg[-1:]
    for pid in candidates:
        try:
            for line in open(f"/proc/{pid}/status"):
                if line.startswith("VmHWM"):
                    return int(line.split()[1]) / 2**20
        except OSError:
            continue
    return None


def recall(kind, rows):
    key = TRUTH_KEY.get(kind)
    if not key:
        return None
    r = [len({int(x["id"]) for x in d["response"].get("rows", [])} & set(truth[key][d["query_id"]].tolist())) / 10 for d in rows]
    return float(np.mean(r))


def summ(rows, seconds):
    ms = [r["ms"] for r in rows]
    return {"n": len(ms), "qps": len(ms) / seconds, **{f"p{p}": float(np.percentile(ms, p)) for p in (50, 90, 99)}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cold-queries", type=int, default=8)
    ap.add_argument("--kinds", default="ann,bm25,hybrid,selective")
    a = ap.parse_args()
    results = {}
    cache = SCRATCH / "matrix-cache"
    try:
        for kind in a.kinds.split(","):
            workload = HERE / f"data/{kind}-strong.json"
            inputs = json.loads(workload.read_text())
            cold, cold_secs, cold_ops = [], 0.0, {}
            for i in range(min(a.cold_queries, len(inputs))):
                stop(); shutil.rmtree(cache, ignore_errors=True); cache.mkdir(parents=True)
                start(cache)
                one = OUT / f"{kind}-cold-in.json"; one.write_text(json.dumps([inputs[i]]))
                before = ops(); rows, st = api(one, 1, f"{kind}-cold-{i}"); after = ops()
                for k, v in after.items():
                    cold_ops[k] = cold_ops.get(k, 0) + v - before.get(k, 0)
                cold += rows; cold_secs += st["seconds"]
                print(json.dumps({"event": "cold", "kind": kind, "i": i, "ms": rows[0]["ms"]}), flush=True)
            stop(); shutil.rmtree(cache, ignore_errors=True); cache.mkdir(parents=True)
            start(cache)
            api(workload, 1, f"{kind}-warmup")
            res = {"cold": dict(summ(cold, cold_secs), ops_per_query={k: v / len(cold) for k, v in cold_ops.items()})}
            for c in (1, 8, 32):
                before = ops(); rows, st = api(workload, c, f"{kind}-warm-c{c}"); after = ops()
                res[f"warm_c{c}"] = dict(summ(rows, st["seconds"]), recall=recall(kind, rows),
                                        ops_per_query={k: (after.get(k, 0) - before.get(k, 0)) / len(rows) for k in after})
            res["rss_peak_gib"] = rss_peak_gib()
            results[kind] = res
            print(json.dumps({"event": "kind", "kind": kind, **res}), flush=True)
            (OUT / "matrix-results.json").write_text(json.dumps(results, indent=1))
    finally:
        stop(); shutil.rmtree(cache, ignore_errors=True)
    print("| Query | Cold p50 ms (n=%d) | Cold GETs/q | Cold MB/q | c1 p50/p90/p99 ms | c1 QPS | c8 QPS | c32 QPS | c32 p50 ms | recall@10 |" % a.cold_queries)
    print("| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |")
    for kind, r in results.items():
        w1, w8, w32, c = r["warm_c1"], r["warm_c8"], r["warm_c32"], r["cold"]
        o = c["ops_per_query"]
        rec = "" if w1["recall"] is None else f"{w1['recall']:.3f}"
        print(f"| {kind} | {c['p50']:.0f} | {o.get('get_range', 0):.0f} | {o.get('bytes_get_range', 0)/1e6:.0f} | "
              f"{w1['p50']:.0f}/{w1['p90']:.0f}/{w1['p99']:.0f} | {w1['qps']:.2f} | {w8['qps']:.2f} | {w32['qps']:.2f} | {w32['p50']:.0f} | {rec} |")


if __name__ == "__main__":
    main()
