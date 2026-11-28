# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Offline kill test for Round C35: score the nearest M probed leaves exactly in the first wave.

Usage: c35_sim.py DUMP.jsonl WAVES.json [--ms 8,12,16,20]
Per query (from the round's temporary candidate dump): baseline = today's rerank (every candidate whose lower bound, estimate minus error,
is within the k-th smallest upper bound, estimate plus error); candidate for each M = score every row of the
nearest M leaves exactly (leaf order = center distance), take their exact k-th distance D, and read the exact
vectors of the other leaves' candidates whose lower bound is at most D. Reads merge as the engine's rerank
does (a cluster with at least a third of its rows read whole, runs of rows contiguous, ranges merged within
64 KiB up to 2 MiB). The second wave's time is drawn from the measured wave-max distribution for that many
16 KB reads (c35_s3wave.py), and the saving is baseline wave minus candidate wave plus nothing for the first
wave (reported apart: extra bytes and reads).
"""
import json, sys, argparse
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("dump"); ap.add_argument("waves"); ap.add_argument("--ms", default="8,12,16,20")
ap.add_argument("--rerank-factor", type=int, default=0, help="cap on the rerank pool (0 = none)")
a = ap.parse_args()
Ms = [int(x) for x in a.ms.split(",")]
waves = json.load(open(a.waves))
rng = np.random.default_rng(35)
w16 = {int(k.split(":")[1]): np.array(v["max"]) for k, v in waves.items() if k.startswith("16384:")}
ns16 = sorted(w16)

def wave_ms(n):
    """Sample the last-byte time of n parallel cold 16 KB reads (0 reads: 0 ms; between measured counts, the next one up)."""
    if n == 0:
        return 0.0
    for m in ns16:
        if m >= n:
            return float(rng.choice(w16[m]))
    return float(rng.choice(w16[ns16[-1]])) * (n / ns16[-1]) ** 0.0

def merge_reads(rows, vec_start, width, cluster_len, gap=64 << 10, cap=2 << 20):
    """rows: list of (seg, cluster, ordinal). Returns (reads, bytes) as the engine's rerank plans them."""
    by = {}
    for s, c, o in rows:
        by.setdefault((s, c), set()).add(o)
    ranges = []
    for (s, c), ords in by.items():
        base = vec_start[(s, c)]
        if len(ords) * 3 >= cluster_len[(s, c)]:
            ranges.append((s, base, base + cluster_len[(s, c)] * width))
        else:
            run = None
            for o in sorted(ords):
                if run and run[1] == o:
                    run[1] += 1
                else:
                    if run:
                        ranges.append((s, base + run[0] * width, base + run[1] * width))
                    run = [o, o + 1]
            ranges.append((s, base + run[0] * width, base + run[1] * width))
    ranges.sort()
    reads, total = 0, 0
    cur = None
    for s, lo, hi in ranges:
        if cur and cur[0] == s and lo - cur[2] <= gap and hi - cur[1] <= cap:
            cur[2] = max(cur[2], hi)
        else:
            if cur:
                reads += 1; total += cur[2] - cur[1]
            cur = [s, lo, hi]
    if cur:
        reads += 1; total += cur[2] - cur[1]
    return reads, total

rows_out = []
for line in open(a.dump):
    q = json.loads(line)
    k, width = q["k"], q["width"]
    leaves = sorted(q["leaves"], key=lambda l: (l[2], l[0], l[1]))
    rank = {(l[0], l[1]): i for i, l in enumerate(leaves)}
    vec_start = {(l[0], l[1]): l[6] for l in leaves}
    cluster_len = {(l[0], l[1]): l[3] for l in leaves}
    vec_len = [l[7] for l in leaves]
    c = q["cands"]
    seg = np.array([x[0] for x in c]); clu = np.array([x[1] for x in c]); ordn = np.array([x[2] for x in c])
    est = np.array([x[4] for x in c], dtype=np.float64); err = np.array([x[5] for x in c], dtype=np.float64)
    ex = np.array([np.nan if x[6] is None else x[6] for x in c], dtype=np.float64)
    lr = np.array([rank[(int(s), int(cl))] for s, cl in zip(seg, clu)])
    lb = est - err
    ub = est + err
    U = np.partition(ub, k - 1)[k - 1]
    pool = lb <= U
    tk = lambda mask: set(np.flatnonzero(mask)[np.argsort(ex[mask], kind="stable")[:k]].tolist())
    truth = tk(np.ones(len(c), bool))
    base_top = tk(pool)
    rows = [(int(seg[i]), int(clu[i]), int(ordn[i])) for i in np.flatnonzero(pool)]
    br, bb = merge_reads(rows, vec_start, width, cluster_len)
    rec = {"cands": len(c), "leaves": len(leaves), "base_pool": int(pool.sum()), "base_reads": br, "base_bytes": bb,
           "base_exact": base_top == truth, "per_m": {}}
    for M in Ms:
        inM = lr < M
        e = ex[inM]
        if len(e) < k:
            D = np.inf
        else:
            D = np.partition(e, k - 1)[k - 1]
        surv = (~inM) & (lb <= D)
        top = tk(inM | surv)
        rows = [(int(seg[i]), int(clu[i]), int(ordn[i])) for i in np.flatnonzero(surv)]
        sr, sb = merge_reads(rows, vec_start, width, cluster_len)
        firstM = [(int(l[0]), int(l[1]), o) for l in leaves[:M] for o in range(l[3])]
        fr, fb = merge_reads(firstM, vec_start, width, cluster_len)
        rec["per_m"][M] = {"surv_rows": int(surv.sum()), "surv_reads": sr, "surv_bytes": sb, "first_reads": fr,
                           "first_bytes": fb, "exact": top == truth, "same_as_base": top == base_top, "D_over_U": float(D / U)}
    rows_out.append(rec)

def pct(x, p): return float(np.percentile(x, p))
n = len(rows_out)
print(f"queries {n}; candidates/query median {int(np.median([r['cands'] for r in rows_out]))}; "
      f"baseline pool median {int(np.median([r['base_pool'] for r in rows_out]))}, reads median {np.median([r['base_reads'] for r in rows_out])}, "
      f"bytes median {np.median([r['base_bytes'] for r in rows_out])/1e6:.2f} MB; baseline == truth {sum(r['base_exact'] for r in rows_out)}/{n}")
base_w = np.array([wave_ms(r["base_reads"]) for r in rows_out for _ in range(20)])
print(f"modeled baseline wave: p50 {pct(base_w,50):.0f} p90 {pct(base_w,90):.0f} ms")
for M in Ms:
    s = [r["per_m"][M] for r in rows_out]
    reads = np.array([x["surv_reads"] for x in s])
    w = np.array([wave_ms(int(x)) for x in reads for _ in range(20)])
    # per-query paired saving: independent draws for base and cand wave
    sav = np.array([wave_ms(r["base_reads"]) - wave_ms(r["per_m"][M]["surv_reads"]) for r in rows_out for _ in range(20)])
    dbytes = np.array([x["first_bytes"] + x["surv_bytes"] - r["base_bytes"] for x, r in zip(s, rows_out)])
    print(f"M={M:2d}: surviving reads median {np.median(reads):.0f} p90 {pct(reads,90):.0f} (zero: {np.mean(reads==0)*100:.0f}%), rows median {np.median([x['surv_rows'] for x in s]):.0f}; "
          f"wave p50 {pct(w,50):.0f} p90 {pct(w,90):.0f} ms, saving p50 {pct(sav,50):.0f} mean {sav.mean():.0f} ms; "
          f"first-wave vectors {np.median([x['first_bytes'] for x in s])/1e6:.1f} MB in {np.median([x['first_reads'] for x in s]):.0f} reads, net bytes {np.median(dbytes)/1e6:+.1f} MB; "
          f"top-{rows_out and 10} exact vs truth {sum(x['exact'] for x in s)}/{n}, same as baseline {sum(x['same_as_base'] for x in s)}/{n}; D/U median {np.median([x['D_over_U'] for x in s]):.3f}")
