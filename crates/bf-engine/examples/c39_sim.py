# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Offline kill test for 256-bit code stripes (Round C39). Reads dump.bin files written by the temporary hook.

Usage: c39_sim.py DUMP.bin [--label NAME] [--json OUT]

Per query (all candidates are the filter's matching rows in the probed leaves):
  today   rows whose lower bound (d1 - e1) is within the k-th smallest upper bound (d1 + e1): the engine's rerank pool.
  reference  the exact top 10 over every row with an exact distance (the pool and more); today's pool must contain it.
  (a)     shortlist the N rows with the smallest 256-bit estimate and rerank exactly.
  (b)     rows whose 256-bit lower bound (d256 - m * e1) is within the k-th smallest 256-bit upper bound are survivors;
          read the second stripe of every leaf holding a survivor, rescore those leaves' rows with the full 1,024 bits,
          and rerank today's way inside them.
Bytes: stripe A of a leaf is its 16 factor bytes per row plus the first 32 of every 128 code bytes of each 32-row block;
stripe B is the other 96 code bytes of each block plus 4 bytes per row (the full-code `<b, c - g>` factor, since stripe A
carries its own). Today's code bytes per leaf are 16 n + 128 * 32 * blocks. Rerank bytes are 2,048 per exact vector.
"""

import argparse
import json
import struct
import sys
from pathlib import Path

import numpy as np

K = 10
VEC = 2048  # bytes of one exact 1,024-d f16 vector
WIDTHS = [40, 100, 200, 400, 800, 1500]
MULTS = [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 6.0]
# (name, stripe bits) of the five estimates the hook dumps
VARIANTS = [("x4-256", 256), ("x1-256", 256), ("x8-128", 128), ("x8/3-384", 384), ("x2-512", 512)]


LEAF_DT = np.dtype([("seg", "<u4"), ("cluster", "<u4"), ("rows", "<u4"), ("matching", "<u4"), ("blocks", "<u4"), ("hit", "<u4"),
                    ("bytes", "<u4"), ("cif", "<u4"), ("range", "<u8", (4,)), ("cdist", "<f4")])
BLK = 262144  # cache block bytes (production)
GAP = 16384  # BLOCK_GAP: ranges closer than this are read as one


def touched_blocks(starts, ends, gets=False):
    """Distinct 256 KiB cache blocks touched by byte ranges (after merging gaps under 16 KiB); with `gets`, also the GET count
    (runs of consecutive blocks, split at 32 blocks)."""
    if len(starts) == 0:
        return (0, 0) if gets else 0
    o = np.argsort(starts, kind="stable")
    st, en = starts[o].astype(np.int64), ends[o].astype(np.int64)
    run = np.maximum.accumulate(en)
    new = np.ones(len(st), dtype=bool)
    new[1:] = st[1:] > run[:-1] + GAP
    first = np.flatnonzero(new)
    ms = st[first]
    me = np.maximum.reduceat(en, first)
    lo, hi = ms // BLK, (me - 1) // BLK
    # Merged ranges are disjoint but may share a block at their ends.
    counts = hi - lo + 1
    blocks = np.repeat(lo, counts) + (np.arange(counts.sum()) - np.repeat(np.cumsum(counts) - counts, counts))
    ub = np.unique(blocks)
    if gets:
        run = np.flatnonzero(np.diff(ub) != 1) + 1
        lengths = np.diff(np.concatenate([[0], run, [len(ub)]]))
        return int(len(ub)), int(np.ceil(lengths / 32).sum())
    return int(len(ub))


def model_bytes(leaves, bits, a_all=True, b_mask=None, today=False, gets=False):
    """GET bytes of the slab reads under the 256 KiB cache blocks, for today's layout or for A-grouped stripes.

    Today: each leaf's blob and codes ranges where they are. A-grouped (a model): per slab object, every leaf's blob and
    stripe A packed in the leaf's old order, then every leaf's stripe B in the same order after the A section; offsets
    scale from the leaf's real start by the slab's own A or B share of a leaf, so only probed leaves' relative order matters.
    """
    total, total_gets = 0, 0
    rng = leaves["range"].astype(np.int64)
    rows, blocks = leaves["rows"].astype(np.int64), leaves["blocks"].astype(np.int64)
    blob = rng[:, 1] - rng[:, 0]
    codes = rng[:, 3] - rng[:, 2]
    a_len = 16 * rows + blocks * 32 * (bits // 8)
    b_len = blocks * 32 * (128 - bits // 8) + 4 * rows
    for seg in np.unique(leaves["seg"]):
        m = leaves["seg"] == seg
        cif = bool(leaves["cif"][m][0])
        if today:
            parts = ([(np.concatenate([rng[m, 0], rng[m, 2]]), np.concatenate([rng[m, 1], rng[m, 3]]))] if cif
                     else [(rng[m, 0], rng[m, 1]), (rng[m, 2], rng[m, 3])])
            for st, en in parts:
                nb, ng = touched_blocks(st, en, gets=True)
                total += nb
                total_gets += ng
            continue
        old = (blob[m] + codes[m]).sum()
        base = rng[m, 0] if cif else rng[m, 2]
        ra = (blob[m] + a_len[m]).sum() / old if cif else a_len[m].sum() / codes[m].sum()
        rb = b_len[m].sum() / old if cif else b_len[m].sum() / codes[m].sum()
        pos_a = (base * ra).astype(np.int64)
        starts, ends = [], []
        if a_all:
            ln = (blob[m] + a_len[m]) if cif else a_len[m]
            starts.append(pos_a)
            ends.append(pos_a + ln)
            if not cif:
                starts.append(rng[m, 0])
                ends.append(rng[m, 1])
        if b_mask is not None and b_mask[m].any():
            end_a = int((pos_a + (blob[m] + a_len[m] if cif else a_len[m])).max())
            origin = (end_a // BLK + 1) * BLK
            pos_b = origin + (base * rb).astype(np.int64)
            sel = b_mask[m]
            starts.append(pos_b[sel])
            ends.append(pos_b[sel] + b_len[m][sel])
        total += touched_blocks(np.concatenate(starts), np.concatenate(ends))
    return (total * BLK, total_gets) if gets else total * BLK


def read(path):
    buf = Path(path).read_bytes()
    pos = 0
    out = []
    while pos < len(buf):
        magic, q, k, n, nl, ne = struct.unpack_from("<6I", buf, pos)
        assert magic == 0xC39, "bad record"
        pos += 24
        qv = np.frombuffer(buf, "<f4", 8, pos)
        pos += 32
        leaves = np.frombuffer(buf, LEAF_DT, nl, pos)
        pos += nl * LEAF_DT.itemsize
        def arr(dt, count):
            nonlocal pos
            a = np.frombuffer(buf, dt, count, pos)
            pos += count * 4
            return a
        leaf = arr("<u4", n)
        ordinal = arr("<u4", n)
        d1, e1 = arr("<f4", n), arr("<f4", n)
        est = [arr("<f4", n) for _ in range(5)]
        chosen = arr("<u4", ne)
        exact = arr("<f4", ne)
        out.append(dict(q=q, k=k, n=n, qv=qv, leaves=leaves, leaf=leaf, ordinal=ordinal, d1=d1, e1=e1, est=est, chosen=chosen, exact=exact))
    return out


def kth(values, k):
    return np.partition(values, k - 1)[k - 1] if len(values) >= k else np.inf


def leaf_bytes(leaves, bits):
    """Per leaf: rows, blocks, today's code bytes, stripe A bytes (the first `bits` code bits), stripe B bytes."""
    rows, blocks = leaves["rows"].astype(np.int64), leaves["blocks"].astype(np.int64)
    today = 16 * rows + blocks * 32 * 128
    a = 16 * rows + blocks * 32 * (bits // 8)
    b = blocks * 32 * (128 - bits // 8) + 4 * rows
    return rows, blocks, today, a, b


def analyze(rec):
    n, d1, e1 = rec["n"], rec["d1"], rec["e1"]
    ex = np.full(n, np.nan, dtype=np.float64)
    ex[rec["chosen"]] = rec["exact"]
    have = ~np.isnan(ex)
    if n < K or not have.any():
        return None
    leaf = rec["leaf"]
    rows, blocks, today, _, _ = leaf_bytes(rec["leaves"], 256)
    res = {"q": rec["q"], "leaves": len(rows), "matching": n, "code_bytes_today": int(today.sum())}
    idx = np.flatnonzero(have)
    ref = set(idx[np.argsort(ex[idx], kind="stable")[:K]].tolist())
    cut1 = kth(d1 + e1, K)
    pool1 = np.flatnonzero(d1 - e1 <= cut1)
    pool1h = pool1[have[pool1]]
    today_top = set(pool1h[np.argsort(ex[pool1h], kind="stable")[:K]].tolist())
    res["today_pool"] = int(len(pool1))
    res["today_pool_missing_exact"] = int((~have[pool1]).sum())
    res["today_recall"] = len(today_top & ref) / K
    res["today_rerank_bytes"] = int(len(pool1)) * VEC
    res["model_today_get_bytes"] = model_bytes(rec["leaves"], 256, today=True)
    ok = have & (e1 > 0)
    res["err1"] = np.abs(d1[ok] - ex[ok]) / e1[ok]
    for v, (name, bits) in enumerate(VARIANTS):
        est = rec["est"][v]
        _, _, _, a, b = leaf_bytes(rec["leaves"], bits)
        res[f"{name}_err"] = np.abs(est[ok] - ex[ok]) / e1[ok]
        res[f"{name}_stripe_a"] = int(a.sum())
        res[f"{name}_stripe_b"] = int(b.sum())
        res[f"{name}_model_a_get_bytes"] = model_bytes(rec["leaves"], bits)
        # (a) shortlist by the stripe estimate alone.
        order = np.argsort(est, kind="stable")
        for w in WIDTHS:
            short = order[:w]
            sh = short[have[short]]
            top = set(sh[np.argsort(ex[sh], kind="stable")[:K]].tolist())
            res[f"{name}_a{w}_recall"] = len(top & ref) / K
            res[f"{name}_a{w}_bytes"] = int(a.sum()) + w * VEC
            res[f"{name}_a{w}_missing"] = int((~have[short]).sum())
        # (b) stripe B only for leaves holding a survivor of the stripe estimate's own bound (m times the 1,024-bit bound).
        for m in MULTS:
            cut = kth(est + m * e1, K)
            surv = est - m * e1 <= cut
            sl = np.unique(leaf[surv])
            read_b = np.zeros(len(rows), dtype=bool)
            read_b[sl] = True
            mask = read_b[leaf]
            c = kth((d1 + e1)[mask], K)
            pool = np.flatnonzero(mask & (d1 - e1 <= c))
            ph = pool[have[pool]]
            top = set(ph[np.argsort(ex[ph], kind="stable")[:K]].tolist())
            res[f"{name}_b{m}_survivors"] = int(surv.sum())
            res[f"{name}_b{m}_leaves_b"] = int(len(sl))
            res[f"{name}_b{m}_pool"] = int(len(pool))
            res[f"{name}_b{m}_missing"] = int((~have[pool]).sum())
            res[f"{name}_b{m}_recall"] = len(top & ref) / K
            res[f"{name}_b{m}_bytes"] = int(a.sum()) + int(b[read_b].sum()) + len(pool) * VEC
            res[f"{name}_b{m}_model_get_bytes"] = model_bytes(rec["leaves"], bits, b_mask=read_b) + len(pool) * VEC
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("--label", default="")
    ap.add_argument("--json")
    ap.add_argument("--skip", type=int, default=1, help="leading records to skip (the warm-up query)")
    args = ap.parse_args()
    recs = read(args.dump)[args.skip:]
    rs = [r for r in (analyze(x) for x in recs) if r]
    med = lambda key: float(np.median([r[key] for r in rs]))  # noqa: E731
    mean = lambda key: float(np.mean([r[key] for r in rs]))  # noqa: E731
    out = {"label": args.label, "queries": len(rs)}
    for key in ["leaves", "matching", "code_bytes_today", "today_pool", "today_pool_missing_exact", "today_recall", "today_rerank_bytes"]:
        out[key] = {"median": med(key), "mean": mean(key)}
    out["model_today_get_bytes"] = {"median": med("model_today_get_bytes")}
    err1 = np.concatenate([r["err1"] for r in rs])
    out["err_over_e1"] = {"d1": {"p50": float(np.percentile(err1, 50)), "p95": float(np.percentile(err1, 95))}}
    for name, bits in VARIANTS:
        err = np.concatenate([r[f"{name}_err"] for r in rs])
        v = {"bits": bits, "err_over_e1_p50": float(np.percentile(err, 50)), "err_over_e1_p95": float(np.percentile(err, 95)),
             "err_over_e1_p99": float(np.percentile(err, 99)), "stripe_a_bytes_med": med(f"{name}_stripe_a"), "stripe_b_bytes_med": med(f"{name}_stripe_b"), "model_a_get_bytes_med": med(f"{name}_model_a_get_bytes"), "a": {}, "b": {}}
        for w in WIDTHS:
            v["a"][w] = {"recall_mean": round(mean(f"{name}_a{w}_recall"), 4), "recall_min": min(r[f"{name}_a{w}_recall"] for r in rs),
                         "exact_queries": sum(r[f"{name}_a{w}_recall"] == 1 for r in rs), "bytes_med": med(f"{name}_a{w}_bytes"),
                         "missing_exact_max": max(r[f"{name}_a{w}_missing"] for r in rs)}
        for m in MULTS:
            v["b"][m] = {"recall_mean": round(mean(f"{name}_b{m}_recall"), 4), "recall_min": min(r[f"{name}_b{m}_recall"] for r in rs),
                         "exact_queries": sum(r[f"{name}_b{m}_recall"] == 1 for r in rs), "bytes_med": med(f"{name}_b{m}_bytes"), "model_get_bytes_med": med(f"{name}_b{m}_model_get_bytes"),
                         "survivors_med": med(f"{name}_b{m}_survivors"), "leaves_b_med": med(f"{name}_b{m}_leaves_b"), "pool_med": med(f"{name}_b{m}_pool"),
                         "missing_exact_max": max(r[f"{name}_b{m}_missing"] for r in rs)}
        out[name] = v
    text = json.dumps(out, indent=1)
    print(text)
    if args.json:
        Path(args.json).write_text(text)


if __name__ == "__main__":
    main()
