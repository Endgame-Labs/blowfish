# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Round BW29: token filters with BM25, and limit.per, on the 1M MS MARCO fixture (needs a `cat` int column).

Usage: bw29_shapes.py BASE_URL NAMESPACE OUT_JSON ANSWERS_JSON [--clients 1,16] [--queries 100] [--cold] [--only a,b]
Each shape draws three disjoint slices of queries: a prewarm slice, the one-client slice, and the 16-client slice
(so no query repeats). Full answers (ids and scores) are written to ANSWERS_JSON for exact comparison across builds;
p50 and p90 (ms) go to OUT_JSON. With --cold, each shape runs its first slice once at one client, in order.
"""
import argparse, asyncio, json, re, time
from pathlib import Path
import httpx

QUERIES = Path(__file__).parent / "data" / "bm25-msmarco.json"


def words(text):
    return [w for w in re.findall(r"[a-z]+", text.lower()) if len(w) > 2]


def unique_triples(queries):
    seen, uniq = set(), []
    for q in queries:
        for w in words(q["request"]["rank_by"][2]):
            if w not in seen:
                seen.add(w)
                uniq.append(w)
    return [uniq[i:i + 3] for i in range(0, len(uniq) - 2, 3)]


def build(queries, n):
    """shape name -> list of 3 slices of n request bodies."""
    texts = [q["request"]["rank_by"][2].strip() for q in queries]
    triples = unique_triples(queries)
    base = {"top_k": 10, "include_attributes": ["id"], "consistency": {"level": "strong"}}

    def req(rank, filters=None, limit=None):
        b = dict(base, rank_by=rank)
        if filters:
            b["filters"] = filters
        if limit:
            b.pop("top_k")
            b["limit"] = limit
        return b

    def per(count):
        return {"total": 10, "per": {"attributes": ["cat"], "limit": count}}

    def slices(make, pool):
        usable = [make(x) for x in pool]
        usable = [u for u in usable if u is not None]
        return [usable[i * n:(i + 1) * n] for i in range(3)]

    def seq(t):
        w = words(t)
        return req(["text", "BM25", t], ["text", "ContainsTokenSequence", " ".join(w[:2])]) if len(w) >= 2 else None

    def common(t):
        w = [x for x in words(t) if len(x) > 3]
        return req(["text", "BM25", t], ["text", "ContainsAllTokens", w[0]]) if w else None

    def qwords(t, k, op):
        w = [x for x in words(t) if len(x) > 3][:k]
        return req(["text", "BM25", t], ["text", op, " ".join(w)]) if len(w) == k else None

    out = {
        "plain": slices(lambda t: req(["text", "BM25", t]), texts),
        "all3+bm25": slices(lambda w: req(["text", "BM25", " ".join(w)], ["text", "ContainsAllTokens", " ".join(w)]), triples),
        "any3+bm25": slices(lambda w: req(["text", "BM25", " ".join(w)], ["text", "ContainsAnyToken", " ".join(w)]), triples),
        "seq+bm25": slices(seq, texts),
        "all1q+bm25": slices(common, texts),
        "all2q+bm25": slices(lambda t: qwords(t, 2, "ContainsAllTokens"), texts),
        "any3q+bm25": slices(lambda t: qwords(t, 3, "ContainsAnyToken"), texts),
        "per1": slices(lambda t: req(["text", "BM25", t], limit=per(1)), texts),
        "per3": slices(lambda t: req(["text", "BM25", t], limit=per(3)), texts),
    }
    return out


async def run(client, url, reqs, clients):
    lats, answers = [], {}
    idx = iter(range(len(reqs)))

    async def worker():
        for i in idx:
            t = time.perf_counter()
            r = await client.post(url, json=reqs[i])
            dt = (time.perf_counter() - t) * 1000
            assert r.status_code == 200, r.text[:300]
            lats.append(dt)
            answers[i] = [[row["id"], row.get("$dist")] for row in r.json()["rows"]]

    t = time.perf_counter()
    await asyncio.gather(*[worker() for _ in range(clients)])
    return lats, answers, time.perf_counter() - t


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base"); ap.add_argument("namespace"); ap.add_argument("out"); ap.add_argument("answers")
    ap.add_argument("--clients", default="1,16"); ap.add_argument("--queries", type=int, default=100)
    ap.add_argument("--cold", action="store_true"); ap.add_argument("--only", default="")
    a = ap.parse_args()
    sh = build(json.loads(QUERIES.read_text()), a.queries)
    if a.only:
        sh = {k: v for k, v in sh.items() if k in a.only.split(",")}
    url = f"{a.base}/v2/namespaces/{a.namespace}/query"
    result, answers = {}, {}
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=64), timeout=600) as client:
        for name, sl in sh.items():
            answers[name] = {}
            if a.cold:
                lats, ans, _ = await run(client, url, sl[0], 1)
                result[name] = {"first_ms": lats[0], "p50": pct(lats, .5), "p90": pct(lats, .9)}
                answers[name]["cold"] = ans
            else:
                await run(client, url, sl[0], 1)
                result[name] = {}
                for n, c in enumerate(map(int, a.clients.split(","))):
                    lats, ans, wall = await run(client, url, sl[n + 1], c)
                    result[name][f"c{c}"] = {"p50": pct(lats, .5), "p90": pct(lats, .9), "qps": len(lats) / wall}
                    result[name]["nonempty"] = result[name].get("nonempty", 0) + sum(1 for v in ans.values() if v)
                    answers[name][f"c{c}"] = ans
            print(name, json.dumps(result[name]), flush=True)
    Path(a.out).write_text(json.dumps(result, indent=1))
    Path(a.answers).write_text(json.dumps(answers))

asyncio.run(main())
