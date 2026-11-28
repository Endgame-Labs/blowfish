# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Fewer probed leaves under a filter (Round C39, estimate only, no format change).

Usage: c39_probes.py DUMP.bin [--label NAME] [--base 346]
A filter scales the probe count by about 1/p (346 to 6,179 leaves at 7.9 percent). Keep only the nearest L leaves (by the query's distance to the
leaf center, recorded by the hook) for L = base * (1/p)^a, a in 0.5, 0.7, 0.85 (capped at today's count), rescore with today's rule inside them
(rows whose lower bound is within the k-th smallest upper bound), and report recall@10 against the exact top 10 of today's probed set, how many
queries equal today's answer, and the modeled slab GET bytes (256 KiB blocks, as `c39_sim.py` validates).
"""

import argparse
import json

import numpy as np

import c39_sim as sim

K = 10


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("--label", default="")
    ap.add_argument("--base", type=int, default=346)
    ap.add_argument("--skip", type=int, default=1)
    args = ap.parse_args()
    recs = sim.read(args.dump)[args.skip :]
    exps = [0.5, 0.7, 0.85, 1.0]
    acc: dict[float, list[tuple]] = {a: [] for a in exps}
    p_all = []
    for rec in recs:
        lv, n = rec["leaves"], rec["n"]
        d1, e1, leaf = rec["d1"], rec["e1"], rec["leaf"]
        ex = np.full(n, np.nan)
        ex[rec["chosen"]] = rec["exact"]
        have = ~np.isnan(ex)
        idx = np.flatnonzero(have)
        ref = set(idx[np.argsort(ex[idx], kind="stable")[:K]].tolist())
        p = lv["matching"].sum() / lv["rows"].sum()
        p_all.append(p)
        order = np.argsort(lv["cdist"], kind="stable")
        rank = np.empty(len(order), dtype=np.int64)
        rank[order] = np.arange(len(order))
        base_bytes = sim.model_bytes(lv, 256, today=True)
        for a in exps:
            keep_n = min(len(lv), max(K, round(args.base * (1 / p) ** a)))
            keep = rank < keep_n
            mask = keep[leaf]
            if mask.sum() < K:
                acc[a].append((keep_n, 0.0, 0, base_bytes, 0, 0))
                continue
            cut = sim.kth((d1 + e1)[mask], K)
            pool = np.flatnonzero(mask & (d1 - e1 <= cut))
            ph = pool[have[pool]]
            top = set(ph[np.argsort(ex[ph], kind="stable")[:K]].tolist())
            nbytes = sim.model_bytes(lv[keep], 256, today=True)
            acc[a].append((keep_n, len(top & ref) / K, int(top == ref), base_bytes, nbytes, int((~have[pool]).sum())))
    out = {"label": args.label, "queries": len(recs), "p_median": float(np.median(p_all)), "today_leaves": float(np.median([len(r["leaves"]) for r in recs]))}
    for a, rows in acc.items():
        arr = np.array(rows, dtype=float)
        out[f"a={a}"] = {"leaves_median": float(np.median(arr[:, 0])), "recall_mean": round(float(arr[:, 1].mean()), 4), "recall_min": float(arr[:, 1].min()),
                         "same_answer_queries": int(arr[:, 2].sum()), "MB_median": round(float(np.median(arr[:, 4])) / 1e6, 1),
                         "MB_today": round(float(np.median(arr[:, 3])) / 1e6, 1), "bytes_ratio": round(float(np.median(arr[:, 3]) / max(np.median(arr[:, 4]), 1)), 2),
                         "pool_rows_without_exact_max": int(arr[:, 5].max())}
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
