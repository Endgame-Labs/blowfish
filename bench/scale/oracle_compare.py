# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Run the held-out 100K workloads against blowfish and the real turbopuffer, and compare them.

Usage:
  TPUF_URL=<turbopuffer base url> uv run bench/scale/oracle_compare.py \
      --blowfish http://127.0.0.1:8300 [--bf-ns wikipedia-100k] [--tpuf-ns blowfish-oracle-wiki100k] \
      [--queries 200] [--out out.json]

Both sides hold the same first 100K Wikipedia passages (see scale_load.py). The queries are the 200
held-out passages in data/oracle-100k.npz (vectors, titles, and exact cosine top-10 truth computed by the
loader). Workloads, each with top_k 10 and strong consistency:
  ann        ANN on the held-out vector
  bm25       BM25 on the held-out title
  hybrid     multi-query ANN plus BM25 fused with RRF
  selective  ANN with title_initial == "z"
  broad      ANN with text_length >= 300
Queries alternate between the two sides so both see the same moments. Each query runs once on each side.
Blowfish is warmed with one unmeasured pass first (disable with --no-bf-warmup). turbopuffer gets exactly one
pass, and the cache temperature it reports is recorded: it cannot be forced cold.

Auth: TURBOPUFFER_API_KEY and BLOWFISH_API_KEY are sent as bearer tokens when set (a keyless proxy needs neither).
"""
import argparse
import json
import os
import time
from pathlib import Path

import httpx
import numpy as np

DATA = Path(__file__).resolve().parent / "data" / "oracle-100k.npz"
KINDS = ("ann", "bm25", "hybrid", "selective", "broad")
FILTERS = {"selective": ["title_initial", "Eq", "z"], "broad": ["text_length", "Gte", 300]}
TRUTH = {"ann": "ann_ids", "selective": "selective_ids", "broad": "broad_ids"}


def build(kind, vec, title):
    base = {"top_k": 10, "consistency": {"level": "strong"}}
    ann = {"rank_by": ["vector", "ANN", vec], "top_k": 10}
    if kind == "ann":
        return {**base, **ann}
    if kind == "bm25":
        return {**base, "rank_by": ["text", "BM25", title]}
    if kind == "hybrid":
        return {"queries": [ann, {"rank_by": ["text", "BM25", title], "top_k": 10}], "rerank_by": ["RRF"], "limit": 10,
                "consistency": {"level": "strong"}}
    return {**base, **ann, "filters": FILTERS[kind]}


def rows_of(resp):
    if "results" in resp:  # multi-query with rerank_by returns one combined result
        return resp["results"][0].get("rows", [])
    return resp.get("rows", [])


def score_of(row):
    return row.get("$dist")


def call(client, url, body):
    t = time.perf_counter()
    r = client.post(url, json=body)
    ms = (time.perf_counter() - t) * 1000
    r.raise_for_status()
    return r.json(), ms


def pct(xs, p):
    return float(np.percentile(xs, p)) if len(xs) else float("nan")


def lat(xs):
    return {"p50": pct(xs, 50), "p90": pct(xs, 90), "p99": pct(xs, 99)}


def compare(a_rows, b_rows):
    """Parity of blowfish rows (a) against turbopuffer rows (b)."""
    a_ids = [int(r["id"]) for r in a_rows]
    b_ids = [int(r["id"]) for r in b_rows]
    k = 10
    common = [i for i in a_ids if i in set(b_ids)]
    a_s = {int(r["id"]): score_of(r) for r in a_rows}
    b_s = {int(r["id"]): score_of(r) for r in b_rows}
    diffs = [abs(a_s[i] - b_s[i]) for i in common if a_s[i] is not None and b_s[i] is not None]
    rel = [abs(a_s[i] - b_s[i]) / max(abs(b_s[i]), 1e-9) for i in common if a_s[i] is not None and b_s[i] is not None]
    return {
        # Fewer than k rows is legitimate (a rare BM25 term matches few documents), so divide by the larger result.
        "overlap": len(set(a_ids[:k]) & set(b_ids[:k])) / max(1, len(a_ids[:k]), len(b_ids[:k])),
        "top1": float(bool(a_ids and b_ids and a_ids[0] == b_ids[0])),
        "pos_match": sum(x == y for x, y in zip(a_ids[:k], b_ids[:k])) / max(1, len(a_ids[:k]), len(b_ids[:k])),
        "abs_diff": float(np.mean(diffs)) if diffs else None,
        "rel_diff": float(np.mean(rel)) if rel else None,
        "max_rel_diff": float(np.max(rel)) if rel else None,
    }


def recall(rows, truth):
    return len({int(r["id"]) for r in rows[:10]} & set(int(x) for x in truth)) / 10


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--blowfish", default="http://127.0.0.1:8300")
    ap.add_argument("--bf-ns", default="wikipedia-100k")
    ap.add_argument("--tpuf-url", default=os.environ.get("TPUF_URL", "https://api.turbopuffer.com"))
    ap.add_argument("--tpuf-ns", default="blowfish-oracle-wiki100k")
    ap.add_argument("--queries", type=int, default=200)
    ap.add_argument("--kinds", default=",".join(KINDS))
    ap.add_argument("--no-bf-warmup", action="store_true")
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()

    d = np.load(DATA)
    n = min(a.queries, len(d["title"]))
    vecs = [d["vector"][i].astype("f4").tolist() for i in range(n)]
    titles = [str(t) for t in d["title"][:n]]

    def headers(env):
        k = os.environ.get(env)
        return {"authorization": f"Bearer {k}"} if k else {}

    bf = httpx.Client(timeout=300, headers=headers("BLOWFISH_API_KEY"))
    tp = httpx.Client(timeout=300, headers=headers("TURBOPUFFER_API_KEY"))
    bf_url = f"{a.blowfish}/v2/namespaces/{a.bf_ns}/query"
    tp_url = f"{a.tpuf_url}/v2/namespaces/{a.tpuf_ns}/query"

    result = {}
    for kind in a.kinds.split(","):
        bodies = [build(kind, vecs[i], titles[i]) for i in range(n)]
        if not a.no_bf_warmup:
            for b in bodies:
                call(bf, bf_url, b)
        bf_ms, tp_ms, srv_ms, exec_ms, temps = [], [], [], [], {}
        par, rec_bf, rec_tp = [], [], []
        for i, b in enumerate(bodies):
            rb, mb = call(bf, bf_url, b)
            rt, mt = call(tp, tp_url, b)
            bf_ms.append(mb)
            tp_ms.append(mt)
            perf = rt.get("performance", {})
            if "server_total_ms" in perf:
                srv_ms.append(perf["server_total_ms"])
            if "query_execution_ms" in perf:
                exec_ms.append(perf["query_execution_ms"])
            t = perf.get("cache_temperature", "unknown")
            temps[t] = temps.get(t, 0) + 1
            par.append(compare(rows_of(rb), rows_of(rt)))
            if kind in TRUTH:
                rec_bf.append(recall(rows_of(rb), d[TRUTH[kind]][i]))
                rec_tp.append(recall(rows_of(rt), d[TRUTH[kind]][i]))

        def mean(key):
            v = [p[key] for p in par if p[key] is not None]
            return float(np.mean(v)) if v else None

        result[kind] = {
            "n": n,
            "overlap@10": mean("overlap"), "top1_agree": mean("top1"), "pos_match@10": mean("pos_match"),
            "mean_abs_score_diff": mean("abs_diff"), "mean_rel_score_diff": mean("rel_diff"),
            "max_rel_score_diff": mean("max_rel_diff"),
            "min_overlap": min(p["overlap"] for p in par),
            "recall_blowfish": float(np.mean(rec_bf)) if rec_bf else None,
            "recall_tpuf": float(np.mean(rec_tp)) if rec_tp else None,
            "blowfish_client_ms": lat(bf_ms), "tpuf_client_ms": lat(tp_ms),
            "tpuf_server_total_ms": lat(srv_ms), "tpuf_query_execution_ms": lat(exec_ms),
            "tpuf_cache_temperature": temps,
        }
        print(json.dumps({kind: result[kind]}), flush=True)

    if a.out:
        a.out.write_text(json.dumps(result, indent=1))

    def f(x, p=3):
        return "-" if x is None else f"{x:.{p}f}"

    def l(x):
        return f"{x['p50']:.0f} / {x['p90']:.0f} / {x['p99']:.0f}"

    print("\nNote: turbopuffer requests go through a proxy and over the internet, so its client latency includes that"
          " path; server_total_ms does not. turbopuffer cannot be forced cold, so its cache temperature is recorded.\n")
    print(f"Parity, blowfish vs turbopuffer, {n} queries per type, top_k 10 (score diffs are over shared ids):\n")
    print("| Query | overlap@10 | min overlap | top-1 agree | same-rank@10 | mean rel score diff | recall@10 blowfish | recall@10 tpuf |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for k, r in result.items():
        print(f"| {k} | {f(r['overlap@10'])} | {f(r['min_overlap'], 1)} | {f(r['top1_agree'])} | {f(r['pos_match@10'])} | "
              f"{f(r['mean_rel_score_diff'], 4)} | {f(r['recall_blowfish'])} | {f(r['recall_tpuf'])} |")
    print("\nLatency, ms p50 / p90 / p99 (client-measured, one request at a time):\n")
    print("| Query | blowfish client | tpuf client | tpuf server_total_ms | tpuf query_execution_ms | tpuf cache temperature |")
    print("| --- | --- | --- | --- | --- | --- |")
    for k, r in result.items():
        t = ", ".join(f"{v} {c}" for c, v in sorted(r["tpuf_cache_temperature"].items()))
        print(f"| {k} | {l(r['blowfish_client_ms'])} | {l(r['tpuf_client_ms'])} | {l(r['tpuf_server_total_ms'])} | "
              f"{l(r['tpuf_query_execution_ms'])} | {t} |")


if __name__ == "__main__":
    main()
