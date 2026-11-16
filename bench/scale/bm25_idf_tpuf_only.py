# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Estimate turbopuffer's BM25 idf offset from turbopuffer alone (read-only queries), at any namespace size.

For each word of the held-out titles: the exact document frequency n comes from a ContainsAllTokens Count; the top
documents' text comes back with a single-term BM25 query; the tf-part (k1 1.2, b 0.75, tokens = lowercase
alphanumeric runs) is computed locally from that text; and turbopuffer's idf is score / tf-part. Compared with the
idf blowfish's formula gives, ln(1 + (N - n + 0.5) / (n + 0.5)), the difference is the offset. The average document
length comes from Sum(text_length) over the namespace times a tokens-per-character ratio from the sampled texts,
so it is approximate (about 1 percent); the offsets are only trustworthy to that level.

Usage: TPUF_URL=<url> uv run bench/scale/bm25_idf_tpuf_only.py --ns blowfish-oracle-wiki1m --docs 1000000
"""
import argparse
import math
import os
import re
from pathlib import Path

import httpx
import numpy as np

DATA = Path(__file__).resolve().parent / "data" / "oracle-100k.npz"
TOK = re.compile(r"[a-z0-9]+")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tpuf-url", default=os.environ.get("TPUF_URL", "https://api.turbopuffer.com"))
    ap.add_argument("--ns", required=True)
    ap.add_argument("--docs", type=int, required=True, help="N, the loaded document count")
    ap.add_argument("--words", type=int, default=60)
    a = ap.parse_args()
    url = f"{a.tpuf_url}/v2/namespaces/{a.ns}/query"
    c = httpx.Client(timeout=120)
    post = lambda b: c.post(url, json={**b, "consistency": {"level": "strong"}}).json()
    d = np.load(DATA)
    words = []
    for t in d["title"]:
        words += re.findall(r"[A-Za-z]{5,}", str(t))
    words = list(dict.fromkeys(w.lower() for w in words))[: a.words]
    total_chars = post({"aggregate_by": {"s": ["Sum", "text_length"]}})["aggregations"]["s"]
    toks = chars = 0
    out = []
    for w in words:
        n = post({"aggregate_by": {"c": ["Count"]}, "filters": ["text", "ContainsAllTokens", w]})["aggregations"]["c"]
        r = post({"top_k": 5, "rank_by": ["text", "BM25", w], "include_attributes": ["text"]})["rows"]
        if n and r:
            out.append((n, w, [(x["$dist"], x["text"]) for x in r]))
            for x in r:
                toks += len(TOK.findall(x["text"].lower()))
                chars += len(x["text"])
    avgdl = total_chars / a.docs * (toks / chars)
    print(f"N {a.docs} avg chars {total_chars / a.docs:.1f} tokens/char {toks / chars:.4f} avgdl {avgdl:.1f}")
    print("df term blowfish_idf tpuf_idf offset")
    rows = []
    for n, w, docs in sorted(out):
        idf = math.log(1 + (a.docs - n + 0.5) / (n + 0.5))
        est = []
        for s, text in docs:
            ts = TOK.findall(text.lower())
            tf = ts.count(w)
            if tf:
                est.append(s / (tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * len(ts) / avgdl))))
        if est:
            rows.append((n, w, idf, float(np.median(est)) - idf))
    for n, w, idf, off in rows:
        print(n, w, f"{idf:.3f}", f"{idf + off:.3f}", f"{off:+.3f}")
    offs = np.array([r[3] for r in rows])
    print(f"terms {len(rows)} median offset {np.median(offs):+.4f}")
    vals, cnt = np.unique(np.round(offs, 2), return_counts=True)
    print("offset histogram (rounded 0.01):", dict(zip(vals.tolist(), cnt.tolist())))


if __name__ == "__main__":
    main()
