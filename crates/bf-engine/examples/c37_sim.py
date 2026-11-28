# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Offline kill test for Round C37: do extra RaBitQ code bits certify the cold top 10 without exact reads?

Usage: c37_sim.py DUMP.jsonl [--bits 2,3,4] [--queries N]
Inputs, from the round's temporary candidate dump (see Round C37): DUMP.jsonl (per query: every probed leaf and every candidate's 1-bit estimate, error
bound, and exact distance, as in C35) and DUMP.jsonl.res (per query: the rotated residuals of the rows in the 1-bit
pool, whose lower bound is at most the k-th smallest upper bound, and the rotated query residual of each leaf).

For each extension width e (extra bits per dimension, so a (1+e)-bit code), each pool row is quantized offline
the way extended RaBitQ does (a uniform grid of 2^(1+e) levels over the rotated unit residual, with the scale
that maximizes the code's cosine with the vector), its inner product with the query is estimated as <code, q> /
<code, o>, and its distance bound is the engine's own formula with the new alpha: 1.9 * 2 * |q - c| * |r| *
sqrt(1 - alpha^2) / (alpha * sqrt(D - 1)) in the metric's units. Rows still needing an exact vector are found
by the rank test: a row is settled when the number of rows that are surely below it equals the number that
may be below it (order certified), or when it is surely in or surely out of the top k (set certified). Rows that
are not settled are read exactly (their interval collapses to the exact distance) and the test repeats.
Reads merge as the engine's rerank merges them (see c35_sim.py).
"""
import argparse, json
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("dump")
ap.add_argument("--bits", default="2,3,4")
ap.add_argument("--queries", type=int, default=0)
a = ap.parse_args()
EXT = [int(x) for x in a.bits.split(",")]
D = 1024
CONF = 1.9


def quantize(o, total_bits):
    """o: (n, D) unit rows. Returns alpha = <code, o> / |code| for the best-scale uniform grid, and the unit code."""
    levels = 2 ** total_bits
    h = (levels - 1) / 2
    amax = np.abs(o).max(1, keepdims=True)
    best_cos = np.full(len(o), -1.0)
    best = np.zeros_like(o)
    for s in np.linspace(0.35, 1.0, 27):
        t = amax * s / h
        u = np.clip(np.round(o / t + h), 0, levels - 1) - h
        # grid points are at u = integer - h; |u| rounding keeps symmetric levels
        nrm = np.linalg.norm(u, axis=1)
        cos = (u * o).sum(1) / np.maximum(nrm, 1e-12)
        better = cos > best_cos
        best_cos = np.where(better, cos, best_cos)
        best[better] = u[better]
    code = best / np.maximum(np.linalg.norm(best, axis=1, keepdims=True), 1e-12)
    return best_cos, code


def settle(lb, ub, ex, k, order):
    """Return the boolean mask of rows that must be read exactly for the answer to be certified."""
    lb, ub = lb.copy(), ub.copy()
    read = np.zeros(len(lb), bool)
    for _ in range(len(lb) + 1):
        r_lo = (ub[None, :] < lb[:, None]).sum(1)
        alive = r_lo < k
        over = (lb[None, :] < ub[:, None]) & alive[None, :]
        r_hi = over.sum(1) - (lb < ub)
        amb = alive & ~read & ((r_lo != r_hi) if order else (r_hi >= k))
        if not amb.any():
            break
        read |= amb
        lb[amb] = ub[amb] = ex[amb]
    return read, lb, ub


def merge_reads(rows, vec_start, width, cluster_len, gap=64 << 10, cap=2 << 20):
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
    reads, cur = 0, None
    for s, lo, hi in ranges:
        if cur and cur[0] == s and lo - cur[2] <= gap and hi - cur[1] <= cap:
            cur[2] = max(cur[2], hi)
        else:
            reads += cur is not None
            cur = [s, lo, hi]
    return reads + (cur is not None)


def read_res(f):
    n, nl = np.frombuffer(f.read(8), "<u4")
    lv = np.frombuffer(f.read(int(nl) * (8 + 4 * D)), np.uint8).reshape(nl, 8 + 4 * D)
    rv = np.frombuffer(f.read(int(n) * (16 + 4 * D)), np.uint8).reshape(n, 16 + 4 * D)
    leaves = {(int(h[0]), int(h[1])): x for h, x in zip(lv[:, :8].copy().view("<u4"), lv[:, 8:].copy().view("<f4"))}
    hdr = rv[:, :16].copy().view("<u4").reshape(n, 4)
    res = rv[:, 16:].copy().view("<f4").reshape(n, D).astype(np.float64)
    return leaves, hdr, res


stats = {e: [] for e in EXT}
extra = []
cov = {e: [] for e in EXT}
calib, mism = [], []
f = open(a.dump + ".res", "rb")
for qi, line in enumerate(open(a.dump)):
    if a.queries and qi >= a.queries:
        break
    q = json.loads(line)
    k, width = q["k"], q["width"]
    leaves_j = q["leaves"]
    vec_start = {(l[0], l[1]): l[6] for l in leaves_j}
    cluster_len = {(l[0], l[1]): l[3] for l in leaves_j}
    c = q["cands"]
    key = {(x[0], x[1], x[2]): i for i, x in enumerate(c)}
    est1 = np.array([x[4] for x in c]); err1 = np.array([x[5] for x in c])
    exa = np.array([np.nan if x[6] is None else x[6] for x in c])
    truth = np.argsort(np.where(np.isnan(exa), np.inf, exa), kind="stable")[:k]
    leaves, hdr, res = read_res(f)
    idx = np.array([key[(int(h[0]), int(h[1]), int(h[2]))] for h in hdr])
    qc = np.stack([leaves[(int(h[0]), int(h[1]))] for h in hdr]).astype(np.float64)
    n = np.linalg.norm(res, axis=1); m = np.linalg.norm(qc, axis=1)
    exact_l2 = ((res - qc) ** 2).sum(1)
    fac = float(np.median(exa[idx] / np.maximum(exact_l2, 1e-12)))
    calib.append(fac)
    o = res / np.maximum(n[:, None], 1e-30)
    qu = qc / np.maximum(m[:, None], 1e-30)
    # engine's 1-bit estimate, recomputed from the residuals: sanity check against the dump
    a1 = np.abs(o).sum(1) / np.sqrt(D)
    d1 = n**2 + m**2 - 2 * n * m * (np.sign(o) * qu).sum(1) / np.sqrt(D) / a1
    mism.append(float(np.median(np.abs(fac * d1 - est1[idx]) / np.maximum(np.abs(est1[idx]), 1e-9))))
    lb1, ub1 = est1[idx] - err1[idx], est1[idx] + err1[idx]
    ex = exa[idx]
    leaf_ids = {(int(h[0]), int(h[1])): None for h in hdr}
    extra.append({e: len(c) * (e * D // 8 + 8) for e in EXT})
    rows_loc = [(int(h[0]), int(h[1]), int(h[2])) for h in hdr]
    for e in EXT:
        al, code = quantize(o, 1 + e)
        dot = (code * qu).sum(1) / al
        est = fac * (n**2 + m**2 - 2 * n * m * dot)
        ang = np.sqrt(np.maximum(1 - al**2, 0)) / al
        err = fac * CONF * 2 * m * n * ang / np.sqrt(D - 1)
        lb, ub = est - err, est + err
        cov[e].append(float(((ex >= lb) & (ex <= ub)).mean()))
        rec = {"pool": len(idx), "err_ratio": float(np.median(err / np.maximum(err1[idx], 1e-30)))}
        for order in (True, False):
            read, flb, fub = settle(lb, ub, ex, k, order)
            mid = (flb + fub) / 2
            r_lo = (fub[None, :] < flb[:, None]).sum(1)
            alive = np.flatnonzero(r_lo < k)
            top = alive[np.argsort(mid[alive], kind="stable")[:k]]
            got = idx[top]
            rec["order" if order else "set"] = {
                "rows": int(read.sum()),
                "reads": merge_reads([rows_loc[i] for i in np.flatnonzero(read)], vec_start, width, cluster_len),
                "exact_set": set(got.tolist()) == set(truth.tolist()),
                "exact_order": got.tolist() == truth.tolist(),
            }
        stats[e].append(rec)
    # the 1-bit baseline for the same pool: rows the engine reads today
    stats.setdefault("base", []).append({"pool": len(idx), "reads": merge_reads(rows_loc, vec_start, width, cluster_len)})

n = len(stats["base"])
print(f"queries {n}; pool (rows decoded) median {np.median([r['pool'] for r in stats['base']]):.0f} p90 {np.percentile([r['pool'] for r in stats['base']],90):.0f}; "
      f"today's reads median {np.median([r['reads'] for r in stats['base']]):.0f}")
print(f"metric factor (dump distance / L2^2) median {np.median(calib):.4f}; 1-bit estimate recomputed vs dump, median relative gap {np.median(mism):.2e}")
for e in EXT:
    s = stats[e]
    print(f"\n== {e} extra bits ({1+e}-bit code): {e*D//8+8} B/row, first-wave extra {np.median([x[e] for x in extra])/1e6:.1f} MB median "
          f"(p90 {np.percentile([x[e] for x in extra],90)/1e6:.1f}); bound coverage of the exact distance {np.mean(cov[e])*100:.1f}%; "
          f"error band vs 1-bit {np.median([r['err_ratio'] for r in s]):.3f}x")
    for name in ("order", "set"):
        rows = np.array([r[name]["rows"] for r in s]); reads = np.array([r[name]["reads"] for r in s])
        print(f"  {name:5s} certified: rows to read median {np.median(rows):.0f} p90 {np.percentile(rows,90):.0f} max {rows.max()}; "
              f"queries with zero {np.mean(rows==0)*100:.0f}%; reads median {np.median(reads):.0f} p90 {np.percentile(reads,90):.0f}; "
              f"top-{k} set exact {sum(r[name]['exact_set'] for r in s)}/{n}, order exact {sum(r[name]['exact_order'] for r in s)}/{n}")
