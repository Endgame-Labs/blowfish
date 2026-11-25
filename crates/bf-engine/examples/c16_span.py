# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Round C16: how wide is the byte span of a query's rerank reads, today and under a leaf reordering?

Inputs: the c16_layout example's tier0.entries.tsv and tier0.centers.f32, and the rows.jsonl a
cold_ab.py run wrote with BF_TRACE_READS=1 (every rerank vector read is a TRACE_READ into a slab's
.bfv object above the leaf-vector region). Each read is mapped to a leaf, and the leaf to its new offset
under a candidate order, which is simulated by moving every leaf's vectors to the new position.

Usage: uv run c16_span.py LAYOUT_DIR ROWS_JSONL [ORDERS...]   (orders: current bisect pca)
"""
import json
import sys
from collections import defaultdict

import numpy as np

layout, rows_path = sys.argv[1], sys.argv[2]
orders = sys.argv[3:] or ["current", "bisect", "pca"]
MiB, GiB = 1 << 20, 1 << 30

# --- layout ---------------------------------------------------------------------------------
ent = [l.rstrip("\n").split("\t") for l in open(f"{layout}/tier0.entries.tsv")]
slab = np.array([int(e[0]) for e in ent])
rows = np.array([int(e[2]) for e in ent])
vs = np.array([int(e[3]) for e in ent], dtype=np.int64)
ve = np.array([int(e[4]) for e in ent], dtype=np.int64)
key = {int(e[0]): e[7] for e in ent}
obj2slab = {v: k for k, v in key.items()}
dims = 1024
centers = np.memmap(f"{layout}/tier0.centers.f32", dtype="<f4", mode="r").reshape(len(ent), -1)
by_slab = defaultdict(list)
for i, s in enumerate(slab):
    by_slab[int(s)].append(i)
for s in by_slab:
    by_slab[s].sort(key=lambda i: vs[i])  # current order = ascending vector offset
vbase = {s: int(vs[ids[0]]) for s, ids in by_slab.items()}  # start of the vector region


def bisect_order(ids, c):
    """Recursive 2-means on leaf centers, depth first, sizes weighted by bytes; returns ids in order."""
    out = []

    def rec(idx):
        if len(idx) <= 2:
            out.extend(idx)
            return
        x = c[idx]
        # two seeds: the farthest pair approximated by the point farthest from the mean, then its farthest
        m = x.mean(0)
        a = x[np.argmax(((x - m) ** 2).sum(1))]
        b = x[np.argmax(((x - a) ** 2).sum(1))]
        cen = np.stack([a, b])
        for _ in range(6):
            d = ((x[:, None, :] - cen[None]) ** 2).sum(2)
            lab = d.argmin(1)
            if lab.min() == lab.max():
                break
            cen = np.stack([x[lab == k].mean(0) for k in (0, 1)])
        left = [idx[j] for j in range(len(idx)) if lab[j] == 0]
        right = [idx[j] for j in range(len(idx)) if lab[j] == 1]
        if not left or not right:
            h = len(idx) // 2
            left, right = idx[:h], idx[h:]
        # visit the half nearer to the previous leaf first, to keep the walk continuous
        if out:
            last = c[out[-1]]
            if ((c[left] - last) ** 2).sum(1).min() > ((c[right] - last) ** 2).sum(1).min():
                left, right = right, left
        rec(left)
        rec(right)

    rec(list(range(len(ids))))
    return [ids[j] for j in out]


def make_order(name, s):
    ids = by_slab[s]
    if name == "current":
        return ids
    c = np.asarray(centers[ids], dtype=np.float32)
    if name == "pca":
        x = c - c.mean(0)
        u, sv, vt = np.linalg.svd(x, full_matrices=False)
        return [ids[j] for j in np.argsort(x @ vt[0])]
    if name == "bisect":
        # bisect_order works on local indexes
        loc = bisect_order(list(range(len(ids))), c)
        return [ids[j] for j in loc]
    raise SystemExit(f"unknown order {name}")


def new_offsets(name):
    """leaf id -> new vector offset."""
    new = np.zeros(len(ent), dtype=np.int64)
    for s, ids in by_slab.items():
        pos = vbase[s]
        for i in make_order(name, s):
            new[i] = pos
            pos += ve[i] - vs[i]
    return new


# --- reads ----------------------------------------------------------------------------------
queries = [json.loads(l) for l in open(rows_path)]
sorted_vs = {s: np.array([vs[i] for i in ids]) for s, ids in by_slab.items()}
reads = []  # per query: list of (slab, leaf, inner offset, length)
for r in queries:
    out = []
    for t in r["read_timeline"]:
        if t["kind"] != "slabs" or not t["object"].endswith(".bfv"):
            continue
        s = obj2slab.get(t["object"][t["object"].find("ns/"):])
        if s is None:
            continue
        if t["offset"] < vbase[s]:
            continue
        # a read may span several leaves; split it at leaf boundaries
        off, end = t["offset"], t["limit"] if t["limit"] > t["offset"] else t["offset"] + t["bytes"]
        while off < end:
            k = int(np.searchsorted(sorted_vs[s], off, side="right")) - 1
            leaf = by_slab[s][k]
            piece = min(end, int(ve[leaf]))
            out.append((s, leaf, off - int(vs[leaf]), piece - off))
            off = piece if piece > off else end
    reads.append(out)
print(f"queries {len(reads)}, vector reads/query median {np.median([len(r) for r in reads]):.0f}")

for name in orders:
    new = new_offsets(name)
    m = defaultdict(list)
    for r in reads:
        per = defaultdict(list)
        for s, leaf, inner, ln in r:
            per[s].append(int(new[leaf]) + inner)
        slabs_hit = len(per)
        span = sum(max(v) - min(v) for v in per.values())
        # distinct windows of each size, over all slabs
        row = {"slabs": slabs_hit, "span_sum_GiB": span / GiB,
               "max_span_MiB": max(max(v) - min(v) for v in per.values()) / MiB}
        for w, tag in ((16 * MiB, "w16M"), (64 * MiB, "w64M"), (256 * MiB, "w256M"), (GiB, "w1G")):
            row[tag] = sum(len({o // w for o in v}) for v in per.values())
        for k, v in row.items():
            m[k].append(v)
    print(f"order {name}:")
    for k, v in m.items():
        print(f"  {k:14s} p50 {np.percentile(v, 50):9.2f}  p90 {np.percentile(v, 90):9.2f}")

# --- diagnostic: how many leaves does a query rerank from, per slab? ---------------------------
dl = [len({leaf for _, leaf, _, _ in r}) for r in reads]
per_slab = [np.mean([len({l for s2, l, _, _ in r if s2 == s}) for s in {x[0] for x in r}]) for r in reads]
print(f"distinct leaves/query p50 {np.median(dl):.0f} p90 {np.percentile(dl, 90):.0f}; leaves per slab hit p50 {np.median(per_slab):.2f}")
print(f"leaf MiB p50 {np.median((ve - vs) / MiB):.2f}; leaves/slab {np.median([len(v) for v in by_slab.values()]):.0f}")
