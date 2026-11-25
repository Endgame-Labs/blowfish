# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Round C11 ceiling: how early could a streamed rerank have finished?

Reads the rows.jsonl of a `cold_ab.py` run whose server ran with BF_TRACE_C11=1 and
BF_TRACE_READS=1 (the `BF_TRACE_C11` hooks live on branch `c11-experiment`, not on main). For every query it takes the rerank pool (estimate and lower bound
`distance - error`, the bound pool selection trusts), each candidate's exact distance, and the
completion time of the GET that covered its vector. It then replays the arrivals:

* wait: the time the last candidate's read completed (what today's rerank waits for);
* stream: the first arrival after which every unfinished candidate's lower bound is strictly
  greater than the live exact k-th distance (ties stay unfinished);
* oracle: the first time every row of the final top k had arrived (needs the answer, so it is
  an upper limit on any scheme).

Usage: uv run c11_ceiling.py RUN/rows.jsonl [--arm NAME]
"""

import argparse
import json
import re
import statistics
from collections import defaultdict

P = argparse.ArgumentParser()
P.add_argument("rows")
P.add_argument("--arm")
a = P.parse_args()
kv = lambda line: dict(re.findall(r"(\w+)=(\S+)", line))  # noqa: E731


def analyze(row):
    start = k = None
    wins, seg, lb, exact = [], {}, {}, {}
    for line in row["c11"]:
        f = kv(line)
        if line.startswith("C11_START"):
            start, k = int(f["us"]), int(f["k"])
        elif line.startswith("C11_END"):
            end = int(f["us"])
        elif line.startswith("C11_WIN"):
            wins.append(float(f["d"]))
        elif line.startswith("C11_SEG"):
            seg[f["tag"]] = f["key"]
        elif line.startswith("C11_LB"):
            lb[(f["tag"], int(f["row"]))] = float(f["dist"]) - float(f["err"])
        elif line.startswith("C11_EXACT"):
            exact[(f["tag"], int(f["row"]))] = (int(f["start"]), int(f["end"]), float(f["d"]))
    if start is None or not exact:
        return None
    reads = defaultdict(list)
    for r in row["read_timeline"]:
        reads[r["object"]].append((r["offset"], r["offset"] + r["limit"], r["end_us"]))
    cands = []
    for key, (s, e, d) in exact.items():
        if key not in lb:  # a speculative early rerank's row, not in the final pool
            continue
        path = seg[key[0]]
        t = 0.0
        covering = [
            end_us
            for p, rs in reads.items()
            if p.endswith(path) or path.endswith(p)
            for (lo, hi, end_us) in rs
            if lo <= s and e <= hi
        ]
        if covering:
            t = max(0.0, (min(covering) - start) / 1000.0)
        cands.append((t, lb[key], d))
    cands.sort()
    final = sorted(wins + [c[2] for c in cands])
    final_k = final[k - 1] if len(final) >= k else float("inf")
    wait = max(c[0] for c in cands)
    # Oracle: every candidate at or below the final k-th distance has arrived.
    oracle = max([c[0] for c in cands if c[2] <= final_k] or [0.0])
    # Stream: after each arrival group, finish when the best unfinished bound beats the live k-th.
    heap = sorted(wins)
    stream, wrong = wait, False
    n = len(cands)
    suffix_lb = [float("inf")] * (n + 1)
    for i in range(n - 1, -1, -1):
        suffix_lb[i] = min(suffix_lb[i + 1], cands[i][1])
    arrived = list(wins)
    import bisect

    arrived.sort()
    for i in range(n + 1):
        if i:
            bisect.insort(arrived, cands[i - 1][2])
        # Candidates sharing one read complete together: only stop between distinct times.
        if i < n and i and cands[i][0] == cands[i - 1][0]:
            continue
        dk = arrived[k - 1] if len(arrived) >= k else float("inf")
        if suffix_lb[i] > dk:
            stream = cands[i - 1][0] if i else 0.0
            # Result check: no unfinished candidate may be at or below the live k-th distance.
            wrong = any(c[2] <= dk for c in cands[i:])
            break
    return {
        "q": row["q"],
        "cands": n,
        "wall_ms": (end - start) / 1000.0,
        "wait": wait,
        "stream": stream,
        "oracle": oracle,
        "wrong": wrong,
        "gets": len(
            {(r["object"], r["offset"], r["limit"]) for r in row["read_timeline"] if "/ann." in r["object"]}
        ),
    }


out = []
for line in open(a.rows):
    row = json.loads(line)
    if a.arm and row["arm"] != a.arm:
        continue
    r = analyze(row)
    if r:
        out.append(r)
med = lambda xs: statistics.median(xs)  # noqa: E731
p90 = lambda xs: sorted(xs)[int(0.9 * (len(xs) - 1))]  # noqa: E731
print(f"queries {len(out)}")
for name in ("wall_ms", "wait", "stream", "oracle"):
    xs = [r[name] for r in out]
    print(f"{name:8s} p50 {med(xs):7.1f} ms  p90 {p90(xs):7.1f} ms")
for name in ("stream", "oracle"):
    xs = [r["wait"] - r[name] for r in out]
    print(f"saving ({name}) p50 {med(xs):6.1f} ms  p90 {p90(xs):6.1f} ms  min {min(xs):.1f}")
print("queries where stopping early would change the top k:", sum(r["wrong"] for r in out))
print("candidates p50", med([r["cands"] for r in out]))

# Coverage and a generous hedging ceiling. Every read of a final top-k row still pending at
# H ms gets one fresh duplicate (a draw from all measured read durations), for free, with the
# answer known in advance. Cancelling saves nothing beyond the oracle row above.
import random

random.seed(1)
durations = []
for line in open(a.rows):
    row = json.loads(line)
    if a.arm and row["arm"] != a.arm:
        continue
    durations += [(r["end_us"] - r["start_us"]) / 1000.0 for r in row["read_timeline"] if "/ann." in r["object"]]
print(f"ann read durations p50 {statistics.median(durations):.0f} ms  p90 {p90(durations):.0f} ms  n={len(durations)}")
for hedge in (25, 40, 60):
    gains = []
    for line in open(a.rows):
        row = json.loads(line)
        if a.arm and row["arm"] != a.arm:
            continue
        res = analyze(row)
        if not res:
            continue
        # Rebuild winner arrival times as in analyze(): reuse its oracle time and wait.
        gains.append(res)
    # Approximation: the oracle finish is the slowest winner read; one hedge caps it at H + draw.
    saved = []
    for res in gains:
        draws = [min(res["oracle"], hedge + random.choice(durations)) for _ in range(200)]
        saved.append(res["wait"] - statistics.mean(draws))
    print(f"hedge at {hedge} ms: ideal saving p50 {med(saved):.1f} ms  p90 {p90(saved):.1f} ms")
