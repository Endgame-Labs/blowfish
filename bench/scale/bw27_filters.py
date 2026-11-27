# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Round BW27: time token-set filters (ContainsAllTokens and ContainsAnyToken) on the MS MARCO fixture.

Usage: bw27_filters.py BASE_URL NAMESPACE OUT_JSON [--clients 1,16] [--passes 3] [--queries 200] [--cold]
Each shape is a list of requests built from the checked-in MS MARCO queries. Answers (ordered IDs) are
hashed per shape so two builds can be compared; per-shape p50 and p90 (ms) are written to OUT_JSON.
With --cold, each shape runs one pass at one client and reports every query's latency in order.
"""
import argparse, asyncio, hashlib, json, re, statistics, time
from pathlib import Path
import httpx

QUERIES = Path(__file__).parent / "data" / "bm25-msmarco.json"


def words(text):
    return [w for w in re.findall(r"[a-z]+", text.lower()) if len(w) > 2]


def shapes(queries, n, offset=0):
    """Queries made of words no earlier query uses, so no token repeats within or across slices."""
    seen, uniq = set(), []
    for q in queries:
        for w in words(q["request"]["rank_by"][2]):
            if w not in seen:
                seen.add(w)
                uniq.append(w)
    qs = [uniq[i:i + 3] for i in range(offset * 3, (offset + n) * 3, 3)]
    qs = [q for q in qs if len(q) == 3]
    out = {}
    def filt(op, k, prefix, rank):
        reqs = []
        for q in qs:
            toks = q[:k]
            text = " ".join(toks)
            if prefix:
                text = text[:-1] if len(toks[-1]) > 3 else text
            f = ["text", op, text] + ([{"last_as_prefix": True}] if prefix else [])
            body = {"filters": f, "top_k": 10, "include_attributes": ["id"],
                    "consistency": {"level": "strong"}}
            body["rank_by"] = ["text", "BM25", " ".join(q)] if rank else ["id", "asc"]
            reqs.append(body)
        return reqs
    for op, tag in (("ContainsAllTokens", "all"), ("ContainsAnyToken", "any")):
        for k in (1, 3):
            for prefix in (False, True):
                out[f"{tag}{k}{'p' if prefix else ''}"] = filt(op, k, prefix, False)
    out["all1+bm25"] = filt("ContainsAllTokens", 1, False, True)
    out["all3+bm25"] = filt("ContainsAllTokens", 3, False, True)
    out["any3+bm25"] = filt("ContainsAnyToken", 3, False, True)
    out["all3p+bm25"] = filt("ContainsAllTokens", 3, True, True)
    return out


async def run(client, url, reqs, clients, passes):
    lats, answers = [], {}
    for p in range(passes):
        idx = iter(range(len(reqs)))
        async def worker():
            for i in idx:
                t = time.perf_counter()
                r = await client.post(url, json=reqs[i])
                dt = (time.perf_counter() - t) * 1000
                assert r.status_code == 200, r.text[:300]
                if p > 0 or passes == 1:
                    lats.append((i, dt))
                answers[i] = [row["id"] for row in r.json()["rows"]]
        await asyncio.gather(*[worker() for _ in range(clients)])
    return lats, answers


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base"); ap.add_argument("namespace"); ap.add_argument("out")
    ap.add_argument("--clients", default="1,16"); ap.add_argument("--passes", type=int, default=3)
    ap.add_argument("--queries", type=int, default=200); ap.add_argument("--cold", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--distinct", action="store_true", help="prewarm with slice 0, then time unseen slices (one per client count)")
    a = ap.parse_args()
    queries = json.loads(QUERIES.read_text())
    sh = shapes(queries, a.queries)
    if a.only:
        sh = {k: v for k, v in sh.items() if k in a.only.split(",")}
    slices = {}
    if a.distinct:
        # Each shape draws its own three slices of unseen words (prewarm, one client, 16 clients).
        for i, name in enumerate(sh):
            base = i * 3 * a.queries
            sh[name] = shapes(queries, a.queries, base)[name]
            slices[name] = [shapes(queries, a.queries, base + a.queries * k)[name] for k in (1, 2)]
    url = f"{a.base}/v2/namespaces/{a.namespace}/query"
    result = {}
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=64), timeout=600) as client:
        for name, reqs in sh.items():
            if a.cold:
                lats, ans = await run(client, url, reqs, 1, 1)
                ms = [d for _, d in lats]
                result[name] = {"first_ms": ms[0], "p50": pct(ms, .5), "p90": pct(ms, .9),
                                "max": max(ms), "hash": hashlib.sha256(json.dumps(ans, sort_keys=True).encode()).hexdigest()[:16]}
            else:
                result[name] = {}
                await run(client, url, reqs, 1, 1)  # prewarm
                for n, c in enumerate(map(int, a.clients.split(","))):
                    if a.distinct:
                        lats, ans = await run(client, url, slices[name][n], c, 1)
                    else:
                        lats, ans = await run(client, url, reqs, c, a.passes + 1)
                    ms = [d for _, d in lats]
                    result[name][f"c{c}"] = {"p50": pct(ms, .5), "p90": pct(ms, .9)}
                    result[name]["hash"] = hashlib.sha256(json.dumps(ans, sort_keys=True).encode()).hexdigest()[:16]
                    result[name]["nonempty"] = sum(1 for v in ans.values() if v)
            print(name, json.dumps(result[name]), flush=True)
    Path(a.out).write_text(json.dumps(result, indent=1))

asyncio.run(main())
