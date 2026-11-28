# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Round BW29: paired, fresh-process comparison of two scale-server builds with bw29_shapes.py.

Usage: bw29_ab.py STORAGE_JSON NAMESPACE BIN_A BIN_B OUT_DIR [--rounds 3] [--only a,b] [--clients 1,16] [--cold]
Every round starts each build in a fresh process (order alternates AB, BA, ...), runs the shapes once,
and stops it by its recorded PID. Warm runs prewarm on one slice and time unseen slices, so a build never
sees a query twice. With --cold the shapes' first slice runs once per fresh process, in order.
Writes OUT_DIR/{a,b}-ROUND.json and -answers.json, then prints medians over the rounds.
"""
import argparse, json, os, signal, statistics, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).parent


def start(binary, storage, port, log):
    env = dict(os.environ, RAYON_NUM_THREADS="4", MALLOC_ARENA_MAX="2")
    p = subprocess.Popen([binary, storage, f"127.0.0.1:{port}"], stdout=log, stderr=log, env=env)
    for _ in range(100):
        time.sleep(0.2)
        try:
            import urllib.request
            urllib.request.urlopen(f"http://127.0.0.1:{port}/v2/namespaces/x/metadata", timeout=1)
        except Exception as e:  # a 404 still means the server is up
            if getattr(e, "code", 0):
                return p
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("storage"); ap.add_argument("namespace"); ap.add_argument("a"); ap.add_argument("b"); ap.add_argument("out")
    ap.add_argument("--rounds", type=int, default=3); ap.add_argument("--only", default="")
    ap.add_argument("--clients", default="1,16"); ap.add_argument("--cold", action="store_true")
    ap.add_argument("--queries", type=int, default=100); ap.add_argument("--port", type=int, default=8330)
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    log = open(out / "servers.log", "ab")
    for rnd in range(a.rounds):
        for name in (("a", "b") if rnd % 2 == 0 else ("b", "a")):
            binary = a.a if name == "a" else a.b
            server = start(binary, a.storage, a.port, log)
            try:
                cmd = ["uv", "run", str(HERE / "bw29_shapes.py"), f"http://127.0.0.1:{a.port}", a.namespace,
                       str(out / f"{name}-{rnd}.json"), str(out / f"{name}-{rnd}-answers.json"),
                       "--pid", str(server.pid), "--clients", a.clients, "--queries", str(a.queries)]
                if a.only:
                    cmd += ["--only", a.only]
                if a.cold:
                    cmd += ["--cold"]
                subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
            finally:
                server.send_signal(signal.SIGTERM)
                server.wait(timeout=60)
    res = {n: [json.loads((out / f"{n}-{r}.json").read_text()) for r in range(a.rounds)] for n in "ab"}
    same = all(json.loads((out / f"a-{r}-answers.json").read_text()) == json.loads((out / f"b-{r}-answers.json").read_text())
               for r in range(a.rounds))
    keys = [c for c in (["cold"] if a.cold else [f"c{c}" for c in a.clients.split(",")])]
    for key in keys:
        print(key)
        for shape in res["a"][0]:
            def med(n, field):
                vals = [(r[shape] if a.cold else r[shape][key])[field] for r in res[n]]
                return statistics.median(vals)
            fa = [med("a", f) for f in ("p50", "p90", "cpu_ms")]
            fb = [med("b", f) for f in ("p50", "p90", "cpu_ms")]
            print(f"  {shape:12s} A p50 {fa[0]:7.1f} p90 {fa[1]:7.1f} cpu {fa[2]:7.2f} | B p50 {fb[0]:7.1f} p90 {fb[1]:7.1f} cpu {fb[2]:7.2f} | p50 x{fa[0] / fb[0]:.2f} cpu x{fa[2] / fb[2]:.2f}")
    print("identical answers (ordered ids and scores):", same)


main()
