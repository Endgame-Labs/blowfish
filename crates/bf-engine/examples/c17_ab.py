# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Round C17: paired recluster-then-stage runs, control (BF_PRESTAGE=0) against local prestage.

Run from a worktree with uv, on the host that serves the queries. CONFIG.local supplies
storage_url (an owned s3://bucket/tmp/... copy of the namespace), namespace, region, and
optional endpoint/profile. Each run reclusters the copy, then queries from the commit until
the tier's stage and companion pack are on disk (see examples/c17_stage.rs).

    uv run crates/bf-engine/examples/c17_ab.py --config CONFIG.local \
        --binary target/release/examples/c17_stage --scratch .scratch/c17 --label run1 --reps 2
"""

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

import numpy as np

P = argparse.ArgumentParser(description=__doc__)
P.add_argument("--config", type=Path, required=True)
P.add_argument("--binary", required=True)
P.add_argument("--scratch", type=Path, required=True)
P.add_argument("--label", required=True)
P.add_argument("--reps", type=int, default=2)
P.add_argument("--queries", type=int, default=200)
P.add_argument("--oracle", default="10m")
a = P.parse_args()
ROOT = Path(__file__).resolve().parents[3]
RUN = (a.scratch / a.label).resolve()
if ".scratch" not in RUN.parts or RUN.exists() and any(RUN.iterdir()):
    P.error("run must be a new directory under .scratch/")
RUN.mkdir(parents=True, mode=0o700)
config = json.loads(a.config.read_text())
truth = np.load(ROOT / f"bench/scale/data/oracle-{a.oracle}.npz")
workload = [
    {"request": {"rank_by": ["vector", "ANN", truth["vector"][i].astype("f4").tolist()],
                 "top_k": 10, "consistency": {"level": "strong"}, "include_attributes": ["id"]}}
    for i in range(a.queries)
]
(RUN / "workload.json").write_text(json.dumps(workload))
runs = []
for rep in range(a.reps):
    for arm in (["control", "prestage"] if rep % 2 == 0 else ["prestage", "control"]):
        work = RUN / f"{rep}-{arm}"
        work.mkdir()
        (work / "storage.json").write_text(json.dumps({
            "url": config["storage_url"], "region": config.get("region"),
            "endpoint": config.get("endpoint"), "profile": config.get("profile"),
            "read_only": False,
            "cache": {"memory_bytes": 8 * 2**30, "disk_bytes": 48 * 2**30,
                      "disk_dir": str(work / "cache"), "block_size": 4000,
                      "max_coalesce_blocks": 512, "fetch_concurrency": 512},
        }))
        env = dict(os.environ, RAYON_NUM_THREADS="8", BF_DECODED_CACHE_BYTES=str(512 * 2**20),
                   BF_ANN_NPROBE="64", BF_PRESTAGE="0" if arm == "control" else "1")
        out = work / "out.json"
        subprocess.run([str(ROOT / a.binary), str(work / "storage.json"), config["namespace"],
                        str(RUN / "workload.json"), str(work / "scratch"), str(out)],
                       env=env, check=True)
        r = json.loads(out.read_text())
        r["arm"], r["rep"] = arm, rep
        runs.append(r)
        print(arm, rep, {k: r[k] for k in ("recluster_seconds", "codes_usable_ms", "fully_staged_ms", "staged_bytes")}, flush=True)
        shutil.rmtree(work / "cache", ignore_errors=True)
        shutil.rmtree(work / "scratch", ignore_errors=True)


def ids(resp):
    return json.dumps(resp, sort_keys=True)


def p(xs, q):
    return float(np.percentile(xs, q)) if xs else None


summary = {}
for arm in ("control", "prestage"):
    rs = [r for r in runs if r["arm"] == arm]
    summary[arm] = {
        "recluster_seconds": [r["recluster_seconds"] for r in rs],
        "codes_usable_s": [r["codes_usable_ms"] / 1000 for r in rs if r["codes_usable_ms"] is not None],
        "fully_staged_s": [r["fully_staged_ms"] / 1000 for r in rs if r["fully_staged_ms"] is not None],
        "staged_bytes": [r["staged_bytes"] for r in rs],
        "steady_p50_ms": [p([q["ms"] for q in r["steady"]], 50) for r in rs],
        "steady_p90_ms": [p([q["ms"] for q in r["steady"]], 90) for r in rs],
        "early_p50_ms": [p([q["ms"] for q in r["early"]], 50) for r in rs],
    }
# Reply identity: every steady reply of every run against the first run's, and early replies
# (answered while staging ran, some from object storage) against the same query's steady reply.
base = {q["i"]: ids(q["response"]) for q in runs[0]["steady"]}
summary["identical_steady"] = all(ids(q["response"]) == base[q["i"]] for r in runs for q in r["steady"])
summary["identical_early_vs_steady"] = all(
    ids(q["response"]) == base[q["i"]] for r in runs for q in r["early"] if q["i"] in base)
summary["pairs"] = sum(len(r["steady"]) + len(r["early"]) for r in runs)
(RUN / "summary.json").write_text(json.dumps(summary, indent=1))
print(json.dumps(summary, indent=1))
