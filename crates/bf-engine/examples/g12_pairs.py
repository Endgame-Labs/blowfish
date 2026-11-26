"""Round G12: paired A/B of experiment switches on one promoted server.

Usage: python3 g12_pairs.py RUN_DIR SERVER_PID FLAGS_FILE ROUNDS REPEATS "K,K" name=bits [name=bits ...]
The server polls FLAGS_FILE (BF_G12_FLAGS) twice a second. For each round and each K, every variant is
measured with g11_http.py (16 client processes), in an order that alternates by round (A B, B A, ...) so
that drift on the shared machine hits every variant alike. Results: RUN_DIR/pairs.jsonl, and a table of
medians over rounds on stdout.
"""
import json
import os
import pathlib
import statistics
import subprocess
import sys
import time

run, pid, flags, rounds, repeats, ks, *variants = sys.argv[1:]
run, rounds, repeats = pathlib.Path(run), int(rounds), int(repeats)
variants = [(v.split("=")[0], int(v.split("=")[1])) for v in variants]
here = pathlib.Path(__file__).resolve().parent
rows = []
with (run / "pairs.jsonl").open("a") as log:
    for r in range(rounds):
        order = variants if r % 2 == 0 else variants[::-1]
        for k in map(int, ks.split(",")):
            for name, bits in order:
                pathlib.Path(flags).write_text(str(bits))
                time.sleep(2)
                env = dict(os.environ, G3_CLIENT_PROCESSES="16", G3_CLIENT_THREADS="1", G8_K=str(k))
                stem = run / f"pair-{name}-r{r}-k{k}"
                out = subprocess.run(
                    ["uv", "run", "--python", "3.12", "--no-project", "--with", "numpy", "python",
                     str(here / "g11_http.py"), str(run / "fixture/data"), "bf", "18443", "0",
                     str(repeats), str(stem), pid],
                    env=env, check=True, capture_output=True, text=True)
                m = json.loads((stem.with_suffix(".json")).read_text())
                row = {"variant": name, "bits": bits, "round": r, "k": k, "qps": m["qps"],
                       "recall": m["recall_at_k"], "p50": m["p50_ms"], "p90": m["p90_ms"], "p99": m["p99_ms"],
                       "cpu_ms": m["cpu_seconds_per_query"] * 1e3,
                       "ins_m": m.get("instructions_per_query", 0) / 1e6}
                rows.append(row)
                log.write(json.dumps(row) + "\n")
                log.flush()
                print(json.dumps(row), flush=True)
                stem.with_suffix(".json").unlink(missing_ok=True)
pathlib.Path(flags).write_text("0")
print("\nvariant        k   q/s(med)  min..max        recall   p50   p90   p99   cpu_ms  Minstr")
for k in sorted({r["k"] for r in rows}):
    for name, _ in variants:
        v = [r for r in rows if r["variant"] == name and r["k"] == k]
        med = lambda key: statistics.median(x[key] for x in v)
        print(f"{name:12s} {k:4d}  {med('qps'):8.0f}  {min(x['qps'] for x in v):6.0f}..{max(x['qps'] for x in v):6.0f}"
              f"  {med('recall'):.4f}  {med('p50'):5.2f} {med('p90'):5.2f} {med('p99'):5.2f}  {med('cpu_ms'):5.2f}  {med('ins_m'):6.2f}")
