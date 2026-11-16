# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Probe how turbopuffer's BM25 inverse document frequency differs from blowfish's, term by term.

For single-term queries the score of a document is idf * tf-part, and the tf-part (term frequency saturation and
length normalization) is identical on both sides, so the ratio of the two scores for the same document equals the
ratio of the idfs. This script takes words from the held-out titles, counts the documents containing each (an exact
count from blowfish with ContainsAllTokens), computes blowfish's idf, and prints how much larger turbopuffer's idf is
(absolute difference, in nats) for the top document of each term.

Usage: TPUF_URL=<turbopuffer base url> uv run bench/scale/bm25_idf_probe.py [--blowfish URL] [--words 120]
Finding at 100K (iteration 3): most terms sit at a constant +0.169 offset, which is ln((N'+1)/(N+1)) for an
N' about 18.5 percent above the true N, independent of document frequency (so not average length, k1, or b);
terms with an offset below that are consistent with turbopuffer counting extra documents for them. See
docs/benchmarks.md, "BM25 score parity".
"""
import argparse
import math
import os
import re
from pathlib import Path

import httpx
import numpy as np

DATA = Path(__file__).resolve().parent / "data" / "oracle-100k.npz"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blowfish", default="http://127.0.0.1:8300")
    ap.add_argument("--bf-ns", default="wikipedia-100k")
    ap.add_argument("--tpuf-url", default=os.environ.get("TPUF_URL", "https://api.turbopuffer.com"))
    ap.add_argument("--tpuf-ns", default="blowfish-oracle-wiki100k")
    ap.add_argument("--words", type=int, default=120)
    ap.add_argument("--docs", type=int, default=100000, help="N, the number of documents in both namespaces")
    a = ap.parse_args()
    d = np.load(DATA)
    words = []
    for title in d["title"][:200]:
        words += re.findall(r"[A-Za-z]{5,}", str(title))
    words = list(dict.fromkeys(words))[: a.words]
    bf = f"{a.blowfish}/v2/namespaces/{a.bf_ns}/query"
    tp = f"{a.tpuf_url}/v2/namespaces/{a.tpuf_ns}/query"
    rows = []
    for w in words:
        counted = httpx.post(bf, json={"aggregate_by": {"c": ["Count"]}, "filters": ["text", "ContainsAllTokens", w],
                                       "consistency": {"level": "strong"}}, timeout=60).json()
        n = counted.get("aggregations", {}).get("c")
        if not n:
            continue
        body = {"top_k": 3, "rank_by": ["text", "BM25", w], "consistency": {"level": "strong"}}
        ours = {r["id"]: r["$dist"] for r in httpx.post(bf, json=body, timeout=60).json()["rows"]}
        theirs = httpx.post(tp, json=body, timeout=60).json()["rows"]
        ratios = [r["$dist"] / ours[r["id"]] for r in theirs if r["id"] in ours]
        if not ratios:
            continue
        idf = math.log(1 + (a.docs - n + 0.5) / (n + 0.5))
        rows.append((n, w, idf, ratios[0] * idf - idf))
    print("df  term  blowfish_idf  turbopuffer_minus_blowfish_idf")
    for n, w, idf, delta in sorted(rows):
        print(n, w, f"{idf:.3f}", f"{delta:+.4f}")
    deltas = np.array([r[3] for r in rows])
    print(f"\nterms {len(rows)}; median delta {np.median(deltas):+.4f}; "
          f"within 0.169 +/- 0.004: {int(np.sum(np.abs(deltas - 0.169) < 0.004))}")


if __name__ == "__main__":
    main()
