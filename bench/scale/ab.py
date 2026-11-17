"""Same-session A/B: alternate arms query by query, each query on a fresh server process with an
empty disk cache (a cold query), against the same namespace. R2 latency moves by 2x between
sessions, so only interleaved comparisons count.

Usage: uv run --with numpy python bench/scale/ab.py --kind ann --queries 32 --rounds 1 \
         --arm base=/path/to/server --arm new=/path/to/server:BF_HEDGE_BUDGET=0.1,BF_WARM_CONNECTIONS=128
An arm is NAME=BINARY[:ENV=VAL,ENV=VAL]. Arms rotate their order each query. Reads
bench/scale/config.local. Prints per-arm cold p50/p90/p99, requests, MB, and waves. Port: SCALE_PORT (8391).
"""
import argparse, json, os, pathlib, re, shutil, subprocess, sys, time, urllib.request
import numpy as np

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SCRATCH = pathlib.Path(os.environ.get("SCALE_SCRATCH", ROOT / ".scratch/scale"))
CONFIG = pathlib.Path(os.environ.get("SCALE_CONFIG", HERE / "config.local"))
NS = json.loads(CONFIG.read_text())["namespace"]  # overridden by --namespace; per-arm AB_NS and AB_CONFIG env override both
PORT = int(os.environ.get("SCALE_PORT", "8391"))
URL = f"http://127.0.0.1:{PORT}"
WORK = SCRATCH / "ab"
TRUTH = {"ann": "ids", "selective": "selective_ids"}


def metrics():
    raw = urllib.request.urlopen(URL + "/metrics", timeout=10).read().decode()
    out = {}
    for line in raw.splitlines():
        m = re.match(r'blowfish_object_store_(requests|bytes|hedges)_total\{(.*)\} (\S+)', line)
        if m:
            op = re.search(r'op="([^"]+)"', m[2]); op = op[1] if op else re.search(r'outcome="([^"]+)"', m[2])[1]
            key = f"{m[1]}_{op}"; out[key] = out.get(key, 0) + float(m[3])
    return out


CUR_NS = NS


def start_server(arm, append_log=False):
    name, (binary, env) = arm
    env = dict(env)
    config = env.pop("AB_CONFIG", str(CONFIG))  # an arm may read another bucket prefix (a different layout)
    ns = env.pop("AB_NS", NS)
    shutil.rmtree(WORK, ignore_errors=True); (WORK / "cache").mkdir(parents=True)
    subprocess.run(["python3", str(HERE / "mkconfig.py"), config, str(WORK / "storage.json"), str(WORK / "cache"),
                    "--memory-gib", os.environ.get("AB_MEM_GIB", "8"), "--read-only"], check=True)
    e = dict(os.environ, RAYON_NUM_THREADS=os.environ.get("RAYON_NUM_THREADS", "6"), MALLOC_ARENA_MAX="2", BF_TRACE_READS="1", **env)
    global CUR_NS
    CUR_NS = ns
    log = open(WORK / "server.log", "a" if append_log else "w")
    proc = subprocess.Popen([binary, str(WORK / "storage.json"), f"127.0.0.1:{PORT}"], stdout=log, stderr=subprocess.STDOUT, env=e)
    for _ in range(200):
        try:
            urllib.request.urlopen(URL + "/", timeout=1).read(); break
        except Exception:
            time.sleep(0.2)
    time.sleep(float(os.environ.get("AB_SETTLE", "1.5")))  # let the connection warmer finish
    return proc


def query_once(query):
    before = metrics()
    (WORK / "q.json").write_text(json.dumps([query]))
    res = subprocess.run(["uv", "run", str(ROOT / "crates/bf-engine/examples/scale_api.py"), str(WORK / "q.json"), URL, CUR_NS, "1"],
                         capture_output=True, text=True, check=True)
    row = json.loads(res.stdout.splitlines()[0])
    after = metrics()
    d = {k: after.get(k, 0) - before.get(k, 0) for k in after}
    waves = subprocess.run([sys.executable, str(HERE / "waves.py"), str(WORK / "server.log")], capture_output=True, text=True).stdout
    m = re.search(r"(\d+) reads, ([\d.]+) MB, (\d+) waves", waves)
    return row, d, (int(m[1]), float(m[2]), int(m[3])) if m else (0, 0, 0)


def run_one(arm, query, qid):
    """Fresh process, empty disk cache: the whole cold start."""
    proc = start_server(arm)
    try:
        return query_once(query)
    finally:
        proc.terminate(); proc.wait()


class CacheCold:
    """One long-lived server per arm. Before each query, POST /_bench/drop_caches (memory, disk, decoded metadata,
    and namespace handles) and truncate the read trace. Connections stay warm: turbopuffer's definition of cold."""
    def __init__(self, arm, warm_query):
        self.arm = arm; self.proc = start_server(arm, append_log=True)
        urllib.request.urlopen(urllib.request.Request(URL + "/_bench/drop_caches", method="POST"), timeout=600).read()
        query_once(warm_query)  # one throwaway query

    def cpu_ms(self):
        f = open(f"/proc/{self.proc.pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) * 1000 / os.sysconf("SC_CLK_TCK")

    def run(self, query, qid):
        urllib.request.urlopen(urllib.request.Request(URL + "/_bench/drop_caches", method="POST"), timeout=600).read()
        open(WORK / "server.log", "r+").truncate(0)
        c0 = self.cpu_ms()
        out = query_once(query)
        out[0]["cpu_ms"] = self.cpu_ms() - c0
        return out

    def close(self):
        self.proc.terminate(); self.proc.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", default="ann"); ap.add_argument("--queries", type=int, default=32)
    ap.add_argument("--rounds", type=int, default=1)
    ap.add_argument("--workload", help="workload JSON (default data/KIND-strong.json); items with a \"truth\" list give recall@10")
    ap.add_argument("--namespace", help="namespace to query (default: config.local)")
    ap.add_argument("--arm", action="append", required=True)
    ap.add_argument("--cachecold", action="store_true", help="one process per arm; drop caches (not connections) before each query")
    a = ap.parse_args()
    global NS
    NS = a.namespace or NS
    arms = []
    for spec in a.arm:
        name, rest = spec.split("=", 1)
        binary, _, envs = rest.partition(":")
        arms.append((name, (binary, dict(kv.split("=", 1) for kv in envs.split(",") if kv))))
    inputs = json.loads(pathlib.Path(a.workload or HERE / f"data/{a.kind}-strong.json").read_text())
    res = {n: {"ms": [], "reads": [], "mb": [], "waves": [], "hedges": [], "recall": [], "cpu": []} for n, _ in arms}
    cc = {}
    if a.cachecold:
        if len(arms) != 1:
            sys.exit("--cachecold takes exactly one arm")
        cc[arms[0][0]] = CacheCold(arms[0], inputs[-1])
    for rnd in range(a.rounds):
        for qi in range(min(a.queries, len(inputs))):
            order = arms[(qi + rnd) % len(arms):] + arms[:(qi + rnd) % len(arms)]
            for arm in order:
                row, d, (reads, mb, waves) = cc[arm[0]].run(inputs[qi], qi) if cc else run_one(arm, inputs[qi], qi)
                r = res[arm[0]]
                r["ms"].append(row["ms"]); r["reads"].append(reads); r["mb"].append(mb); r["waves"].append(waves)
                r["cpu"].append(row.get("cpu_ms", 0))
                r["hedges"].append(d.get("hedges_issued", 0))
                if inputs[qi].get("truth"):
                    got = {int(x["id"]) for x in row["response"].get("rows", [])}
                    r["recall"].append(len(got & set(inputs[qi]["truth"])) / 10)
                print(json.dumps({"q": qi, "arm": arm[0], "ms": round(row["ms"]), "recall": r["recall"][-1] if r["recall"] else None, "reads": reads, "mb": mb, "waves": waves, "cpu_ms": round(row.get("cpu_ms", 0))}), flush=True)
    print(f"\n| arm ({a.kind}, n={len(res[arms[0][0]]['ms'])}) | p50 ms | p90 | p99 | mean | reads | MB | waves | hedges | recall | cpu ms |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for n, r in res.items():
        ms = r["ms"]
        rec = f"{np.mean(r['recall']):.3f}" if r["recall"] else ""
        print(f"| {n} | {np.percentile(ms, 50):.0f} | {np.percentile(ms, 90):.0f} | {np.percentile(ms, 99):.0f} | {np.mean(ms):.0f} | "
              f"{np.mean(r['reads']):.0f} | {np.mean(r['mb']):.1f} | {np.mean(r['waves']):.1f} | {np.mean(r['hedges']):.1f} | {rec} | {np.mean(r['cpu']):.0f} |")
    for c in cc.values():
        c.close()
    pathlib.Path(SCRATCH / "out").mkdir(parents=True, exist_ok=True)
    (SCRATCH / "out" / f"ab-{a.kind}-{int(time.time())}.json").write_text(json.dumps(res))
    shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    main()
