# /// script
# dependencies = []
# ///
"""Round G28: run cases through switch arms on one promoted server.

Usage: g28_arms.py RUN_DIR PID OUT.jsonl SECONDS PORT 'ARM|ARM|...' CASE [CASE ...]
RUN_DIR holds fixture/data (queries.npy, truth*.npy, filters.json, filters10.json) and the switch file the
server reads (BF_G28_SWITCH=RUN_DIR/switch). A CASE is a filter name from filters.json (a filtered query,
`top_k` 10) or `kN` (an unfiltered query at `top_k` N, checked against truth.npy). For each case the arms run
in order (repeat the list to alternate); each runs the native driver (16 connections) for SECONDS after a
serial quality pass over ROWS queries (default 1,000), and the line records q/s, p50, p99, recall@10, server CPU
per query, the SHA-256 of every response's rows, and how many queries the graph answered.
"""
import json, os, subprocess, sys, time, urllib.request
from pathlib import Path

run, pid, out, seconds, port, arms, *cases = sys.argv[1:]
run = Path(run)
data = run / "fixture/data"
filters = {}
for name in ("filters.json", "filters10.json"):
    if (data / name).exists():
        filters.update(json.loads((data / name).read_text()))
drive = os.environ.get("DRIVE", str(run / "native_drive"))
rows = os.environ.get("ROWS", "1000")


def answered():
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/_blowfish/v1/status") as r:
        return json.load(r)["hot_graphs"]["nq"]["answered"]


with open(out, "a") as f:
    for case in cases:
        for arm in arms.split("|"):
            (run / "switch").write_text(arm)
            time.sleep(0.6)
            before = answered()
            env = dict(os.environ, NATIVE_DRIVE_ROWS=rows)
            k = "10"
            if case.startswith("k"):
                k = case[1:]
            else:
                env["NATIVE_DRIVE_FILTER"] = json.dumps(filters[case])
                env["NATIVE_DRIVE_TRUTH"] = f"truth-{case}.npy"
            raw = subprocess.run([drive, str(data), port, "bf", k, seconds, "0", "", pid],
                                 env=env, capture_output=True, text=True, check=True).stdout
            r = json.loads(raw.strip().splitlines()[-1])
            served = answered() - before
            line = {"case": case, "arm": arm, "qps": round(r["qps"]), "p50_ms": round(r["p50_ms"], 2),
                    "p99_ms": round(r["p99_ms"], 1), "recall": round(r["recall"], 4),
                    "cpu_us": round(r["server_cpu_us_per_query"]), "queries": r["queries"],
                    "graph_answers": served, "requests": r["queries"] + int(rows),
                    "sha": r["rows_sha256"][:16]}
            f.write(json.dumps(line) + "\n"); f.flush()
            print(json.dumps(line), flush=True)
