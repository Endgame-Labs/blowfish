"""Same-session A/B of cache-cold queries on warm processes: each arm is one long-running server; before every
query the arm's caches are emptied (`POST /_bench/drop_caches`), so connections and code are warm but no data is.

Usage: uv run --with numpy --with httpx python bench/scale/ab_warm.py --workload W.json --namespace NS --queries 60 \
         --arm base=/path/server:AB_CONFIG=/path/config,AB_NS=ns --arm new=/path/server:AB_CONFIG=...
Arms alternate query by query. Prints per-arm p50/p90/p99, requests, MB, and overlap@10 when items carry `truth`.
"""
import argparse, json, os, pathlib, re, shutil, subprocess, time, urllib.request
import httpx, numpy as np

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
WORK = ROOT / ".scratch/scale/abw"


def metrics(url):
    raw = urllib.request.urlopen(url + "/metrics", timeout=10).read().decode()
    n = b = 0.0
    for line in raw.splitlines():
        m = re.match(r'blowfish_object_store_(requests|bytes)_total\{(.*)\} (\S+)', line)
        if m and 'op="get"' in m[2]:
            if m[1] == "requests": n += float(m[3])
            else: b += float(m[3])
    return n, b


ap = argparse.ArgumentParser()
ap.add_argument("--workload", required=True); ap.add_argument("--namespace", required=True)
ap.add_argument("--queries", type=int, default=60); ap.add_argument("--arm", action="append", required=True)
a = ap.parse_args()
items = json.loads(pathlib.Path(a.workload).read_text())
shutil.rmtree(WORK, ignore_errors=True); WORK.mkdir(parents=True)
arms, procs = [], []
for i, spec in enumerate(a.arm):
    name, rest = spec.split("=", 1); binary, _, envs = rest.partition(":")
    env = dict(kv.split("=", 1) for kv in envs.split(",") if kv)
    config = env.pop("AB_CONFIG"); ns = env.pop("AB_NS", a.namespace)
    cache = WORK / f"cache{i}"; cache.mkdir()
    subprocess.run(["python3", str(HERE / "mkconfig.py"), config, str(WORK / f"s{i}.json"), str(cache), "--memory-gib", "8", "--read-only"], check=True)
    port = 8480 + i
    e = dict(os.environ, RAYON_NUM_THREADS="6", MALLOC_ARENA_MAX="2", **env)
    p = subprocess.Popen([binary, str(WORK / f"s{i}.json"), f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=e)
    procs.append(p); arms.append((name, f"http://127.0.0.1:{port}", ns))
try:
    for _, url, _ in arms:
        for _ in range(200):
            try: urllib.request.urlopen(url + "/", timeout=1).read(); break
            except Exception: time.sleep(0.2)
    time.sleep(3)
    res = {n: {"ms": [], "n": [], "mb": [], "ov": []} for n, _, _ in arms}
    cl = httpx.Client(timeout=120)
    for qi in range(min(a.queries, len(items))):
        k = qi % len(arms)
        for name, url, ns in arms[k:] + arms[:k]:
            cl.post(url + "/_bench/drop_caches").raise_for_status()
            time.sleep(0.2)
            n0, b0 = metrics(url)
            t = time.monotonic()
            r = cl.post(f"{url}/v2/namespaces/{ns}/query", json=items[qi]["request"]); r.raise_for_status()
            ms = (time.monotonic() - t) * 1000
            n1, b1 = metrics(url)
            x = res[name]; x["ms"].append(ms); x["n"].append(n1 - n0); x["mb"].append((b1 - b0) / 1e6)
            if items[qi].get("truth"):
                got = {int(row["id"]) for row in r.json().get("rows", [])}
                x["ov"].append(len(got & set(items[qi]["truth"])) / len(items[qi]["truth"]))
    print(f"| arm (cache-cold, warm process, n={len(res[arms[0][0]]['ms'])}) | p50 ms | p90 | p99 | requests | MB | overlap@10 |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for n, x in res.items():
        ov = f"{np.mean(x['ov']):.3f}" if x["ov"] else ""
        print(f"| {n} | {np.percentile(x['ms'], 50):.0f} | {np.percentile(x['ms'], 90):.0f} | {np.percentile(x['ms'], 99):.0f} | {np.mean(x['n']):.0f} | {np.mean(x['mb']):.1f} | {ov} |")
finally:
    for p in procs: p.terminate()
    for p in procs: p.wait()
    shutil.rmtree(WORK, ignore_errors=True)
