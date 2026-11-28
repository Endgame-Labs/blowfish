# /// script
# dependencies = []
# ///
"""Round G27: run filtered cases through arms on one promoted server.

Usage: g27_arms.py RUN_DIR PID OUT.jsonl SECONDS 'ARM|ARM|...' CASE [CASE ...]
RUN_DIR holds fixture/data (queries.npy, truth-CASE.npy, filters.json) and the switch file the
server reads (BF_G27_SWITCH=RUN_DIR/switch). For each case the arms run in order; each runs the
native driver (16 connections) for SECONDS after a 1,000-query serial quality pass, and the line
records q/s, p50, p99, recall@10, server CPU per query, and how many queries the graph answered.
"""
import json, os, subprocess, sys, time, urllib.request
from pathlib import Path

run, pid, out, seconds, arms, *cases = sys.argv[1:]
run = Path(run)
data = run / "fixture/data"
filters = json.loads((data / "filters.json").read_text())
drive = run / "native_drive"
rows = os.environ.get("ROWS", "1000")


def answered():
    with urllib.request.urlopen("http://127.0.0.1:18443/_blowfish/v1/status") as r:
        return json.load(r)["hot_graphs"]["nq"]["answered"]


with open(out, "a") as f:
    for case in cases:
        for arm in arms.split("|"):
            (run / "switch").write_text(arm)
            time.sleep(0.6)
            before = answered()
            env = dict(os.environ, NATIVE_DRIVE_FILTER=json.dumps(filters[case]),
                       NATIVE_DRIVE_TRUTH=f"truth-{case}.npy", NATIVE_DRIVE_ROWS=rows)
            raw = subprocess.run([str(drive), str(data), "18443", "bf", "10", seconds, "0", "", pid],
                                 env=env, capture_output=True, text=True, check=True).stdout
            r = json.loads(raw.strip().splitlines()[-1])
            served = answered() - before
            line = {"case": case, "arm": arm, "qps": round(r["qps"]), "p50_ms": round(r["p50_ms"], 2),
                    "p99_ms": round(r["p99_ms"], 1), "recall": round(r["recall"], 4),
                    "cpu_us": round(r["server_cpu_us_per_query"]), "queries": r["queries"],
                    "graph_answers": served, "requests": r["queries"] + int(rows)}
            f.write(json.dumps(line) + "\n"); f.flush()
            print(json.dumps(line), flush=True)
