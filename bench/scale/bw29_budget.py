# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.1.3"]
# ///
"""Round BW29 item 3: how often would a per-document token budget tighten BM25 window bounds?

A document's query terms share its token count, so a window's combined bound can be capped by
max over L in [Lmin, Lmax] of max { sum_t w_t S(tf_t, L) : tf_t <= tfmax_t, sum_t tf_t <= L }.
This replays held-out MS MARCO queries over the first N corpus rows (128-row windows), computes each
query's exact 10th-best score (theta), and counts the windows whose bound reaches theta with and
without the budget cap. Only the extra windows the cap removes matter.

Usage: bw29_budget.py CORPUS_JSONL [--rows 1000000] [--queries 300]
"""
import argparse, json, re, sys
from pathlib import Path
import numpy as np

K1, B, BLOCK = 1.2, 0.75, 128
QUERIES = Path(__file__).parent / "data" / "bm25-msmarco.json"
tok = re.compile(r"\w+")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("corpus"); ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--queries", type=int, default=300)
    a = ap.parse_args()
    qs = json.loads(QUERIES.read_text())[: a.queries]
    qterms = [list(dict.fromkeys(tok.findall(q["request"]["rank_by"][2].lower()))) for q in qs]
    wanted = {t for ts in qterms for t in ts}
    n = a.rows
    lengths = np.zeros(n, dtype=np.int32)
    post = {t: ([], []) for t in wanted}
    with open(a.corpus) as f:
        for row, line in enumerate(f):
            if row >= n:
                break
            words = tok.findall(json.loads(line)["text"].lower())
            lengths[row] = len(words)
            counts = {}
            for w in words:
                if w in post:
                    counts[w] = counts.get(w, 0) + 1
            for w, c in counts.items():
                post[w][0].append(row); post[w][1].append(c)
    avg = lengths.mean()
    windows = (n + BLOCK - 1) // BLOCK
    pad = windows * BLOCK - n
    lw = np.concatenate([lengths, np.full(pad, lengths.max() if pad else 0)]).reshape(windows, BLOCK)
    lmin, lmax = lw.min(axis=1), lw.max(axis=1)

    def sat(tf, length):
        return tf * (K1 + 1) / (tf + K1 * (1 - B + B * length / avg))

    visited_cur = visited_new = binding_total = considered = 0
    by_terms = {}
    for ts in qterms:
        ts = [t for t in ts if post[t][0]]
        if not ts:
            continue
        idf = {t: np.log(1 + (n - len(post[t][0]) + 0.5) / (len(post[t][0]) + 0.5)) for t in ts}
        score = np.zeros(n)
        mcur = np.zeros(windows)
        tfmax = {}
        for t in ts:
            rows = np.array(post[t][0]); tf = np.array(post[t][1])
            s = idf[t] * sat(tf, lengths[rows])
            np.add.at(score, rows, s)
            m = np.zeros(windows); np.maximum.at(m, rows // BLOCK, s)
            mcur += m
            tm = np.zeros(windows, dtype=np.int64); np.maximum.at(tm, rows // BLOCK, tf)
            tfmax[t] = tm
        theta = np.partition(score, -10)[-10]
        if theta <= 0:
            continue
        tf_sum = sum(tfmax.values())
        bind = tf_sum > lmin
        cur = mcur >= theta
        new = cur.copy()
        # Windows where the cap can matter: the bound reaches theta and the budget binds.
        for w in np.flatnonzero(cur & bind):
            best = 0.0
            for length in range(int(lmin[w]), int(lmax[w]) + 1):
                tfs = {t: 0 for t in ts}
                for _ in range(length):
                    gain = {t: idf[t] * (sat(tfs[t] + 1, length) - sat(tfs[t], length))
                            for t in ts if tfs[t] < tfmax[t][w]}
                    if not gain:
                        break
                    tfs[max(gain, key=gain.get)] += 1
                best = max(best, sum(idf[t] * sat(tfs[t], length) for t in ts))
            # The cap is a bound on the same quantity, so keep the tighter of the two.
            if best * (1 + 1e-6) < theta:
                new[w] = False
        visited_cur += int(cur.sum()); visited_new += int(new.sum())
        binding_total += int((cur & bind).sum()); considered += 1
        c = by_terms.setdefault(len(ts), [0, 0, 0])
        c[0] += int(cur.sum()); c[1] += int(new.sum()); c[2] += 1
    print(json.dumps({
        "queries": considered, "windows_visited_now": visited_cur, "windows_visited_with_budget": visited_new,
        "extra_pruned_percent_of_visited": round(100 * (visited_cur - visited_new) / max(1, visited_cur), 2),
        "visited_windows_where_budget_binds": binding_total,
        "by_term_count": {k: {"queries": v[2], "visited_now": v[0], "visited_with_budget": v[1]} for k, v in sorted(by_terms.items())},
    }, indent=1))


main()
