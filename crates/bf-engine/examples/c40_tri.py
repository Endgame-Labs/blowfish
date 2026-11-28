# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Round C40, optional estimate: how many 32-row code blocks could a triangle bound skip?

Sol 05:25 #4: each code block stores [min, max] distances to a few shared pivots; once the first
probed leaves give a k-th distance, a block whose bound exceeds it is not read. This replays the
idea on real 1,024-d embeddings (the first ROWS rows of the NQ fixture, cosine, normalized, so
Euclidean distance is monotone in it): k-means leaves of about 600 rows, the nearest FRACTION of
leaves probed, a random P_MATCH of rows matching a filter, and the true k-th filtered distance
as the cutoff (the best case: no extra read wave to find it). Rows sit in each leaf in their
original order, as in the index. A block is skipped when its bound exceeds the cutoff.

usage: c40_tri.py corpus.f16 queries.npy [rows] [queries]
"""

import sys

import numpy as np

corpus, queries_path = sys.argv[1], sys.argv[2]
ROWS = int(sys.argv[3]) if len(sys.argv) > 3 else 600_000
NQ = int(sys.argv[4]) if len(sys.argv) > 4 else 60
LEAF = 612
BLOCK = 32
FRACTION = 0.40  # probed leaves, as at the 7.9 percent filter (6,179 of 15,611)
P_MATCH = 0.079
K = 10
rng = np.random.default_rng(40)


def unit(x):
    return x / np.linalg.norm(x, axis=1, keepdims=True)


X = unit(np.fromfile(corpus, dtype=np.float16, count=ROWS * 1024).reshape(-1, 1024).astype(np.float32))
Q = unit(np.load(queries_path)[:NQ].astype(np.float32))
n = len(X)
leaves = n // LEAF
sample = X[rng.choice(n, 120_000, replace=False)]
C = sample[rng.choice(len(sample), leaves, replace=False)].copy()
for _ in range(8):
    assign = np.argmax(sample @ C.T - 0.5 * (C * C).sum(1), axis=1)
    for c in range(leaves):
        m = assign == c
        if m.any():
            C[c] = sample[m].mean(0)
assign = np.empty(n, dtype=np.int32)
for s in range(0, n, 50_000):
    assign[s : s + 50_000] = np.argmax(X[s : s + 50_000] @ C.T - 0.5 * (C * C).sum(1), axis=1)
order = np.argsort(assign, kind="stable")  # rows by leaf, original order inside a leaf
leaf_start = np.searchsorted(assign[order], np.arange(leaves + 1))
Xo = X[order]
matching = rng.random(n) < P_MATCH
Mo = matching[order]
sizes = np.diff(leaf_start)
print(f"rows {n}, leaves {leaves}, rows/leaf {sizes.mean():.0f}, probed {FRACTION:.0%}, matching {P_MATCH:.1%}")

# Pivot sets, each as (pivot vectors per leaf [leaves, P, 1024]) or global [P, 1024].
nearest_leaves = np.argsort(-(C @ C.T), axis=1)  # own center first
glob16 = unit(sample[rng.choice(len(sample), 16, replace=False)])
variants = {
    "own center (1)": ("leaf", 1),
    "own + 3 nearest centers (4)": ("leaf", 4),
    "own + 15 nearest centers (16)": ("leaf", 16),
    "16 global random rows": ("global", glob16),
    "4 global random rows": ("global", glob16[:4]),
}


def dist(a, b):  # Euclidean on unit vectors
    return np.sqrt(np.maximum(2 - 2 * (a @ b.T), 0))


results = {k: [0, 0, 0, 0] for k in variants}
best = {k: [0.0, 0.0] for k in variants}  # largest bound / cutoff, mean bound / cutoff
cutoffs = []  # blocks skipped, blocks, leaves wholly skipped, leaves
for qi, q in enumerate(Q):
    probe = np.argsort(-(C @ q))[: int(leaves * FRACTION)]
    probed_rows = np.concatenate([np.arange(leaf_start[l], leaf_start[l + 1]) for l in probe])
    dq = np.sqrt(np.maximum(2 - 2 * (Xo[probed_rows] @ q), 0))
    cutoff = np.partition(dq[Mo[probed_rows]], K - 1)[K - 1]
    cutoffs.append(float(cutoff))
    for name, (kind, arg) in variants.items():
        skipped = blocks = leaves_skipped = 0
        for l in probe:
            rows = np.arange(leaf_start[l], leaf_start[l + 1])
            if kind == "leaf":
                piv = C[nearest_leaves[l, :arg]]
            else:
                piv = arg
            dqp = dist(q[None], piv)[0]  # [P]
            dxp = dist(Xo[rows], piv)  # [rows, P]
            nb = -(-len(rows) // BLOCK)
            lb = np.zeros(nb)
            for b in range(nb):
                blk = dxp[b * BLOCK : (b + 1) * BLOCK]
                lo, hi = blk.min(0), blk.max(0)
                lb[b] = max(0.0, float(np.max(np.maximum(dqp - hi, lo - dqp))))
            ok = lb > cutoff
            best[name][0] = max(best[name][0], float(lb.max() / cutoff))
            best[name][1] += float(lb.sum() / cutoff)
            skipped += int(ok.sum())
            blocks += nb
            leaves_skipped += int(ok.all())
        r = results[name]
        r[0] += skipped
        r[1] += blocks
        r[2] += leaves_skipped
        r[3] += len(probe)
    if qi % 10 == 9:
        print(f"query {qi + 1}", flush=True)
print(f"mean k-th filtered distance {np.mean(cutoffs):.3f} (unit vectors: random pair is 1.414)")
print(f"{'pivots':32} {'blocks skipped':>15} {'leaves wholly skipped':>22} {'max bound/cutoff':>17} {'mean bound/cutoff':>18}")
for name, (s, b, ls, nl) in results.items():
    print(f"{name:32} {s / b:15.4f} {ls / nl:22.4f} {best[name][0]:17.3f} {best[name][1] / b:18.3f}")
