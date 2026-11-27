# /// script
# dependencies = ["numpy==2.5.4"]
# ///
"""Round G20: run measurement arms against one held server with the Python or the native driver.

Usage: uv run g20_arms.py RUN_DIR SERVER_PID OUT_JSONL SECONDS ARM [ARM...]
ARM is `driver:engine:k:ef` with driver `py` (g18_drive.py) or `native` (the `native_drive` example, built
release), engine `bf` (port 18443) or `qdrant` (port 18446), k the top_k, and ef Qdrant's `hnsw_ef` (0 for
blowfish). RUN_DIR holds fixture/data. Arms run in the order given. Each result line, with the arm's label, goes
to OUT_JSONL and stdout. Both drivers read the server's CPU from SERVER_PID over the timed pass and report their own.
Round G20 also ran blowfish arms against a candidate binary that read a temporary A/B switch file; that switch is
not in the tree.
"""
import json
import pathlib
import subprocess
import sys

run, pid, out, seconds, *arms = sys.argv[1:]
run = pathlib.Path(run).resolve()
here = pathlib.Path(__file__).resolve().parent
native = here.parents[2] / "target/release/examples/native_drive"
data = run / "fixture/data"
for n, arm in enumerate(arms):
    driver, engine, k, ef = arm.split(":")
    port = "18443" if engine == "bf" else "18446"
    args = [str(data), port, engine, k, seconds, ef, "", pid]
    cmd = (["uv", "run", "--python", "3.12", str(here / "g18_drive.py")] if driver == "py" else [str(native)]) + args
    row = json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1])
    row.update(arm=arm, n=n)
    print(json.dumps(row), flush=True)
    with open(out, "a") as f:
        f.write(json.dumps(row) + "\n")
