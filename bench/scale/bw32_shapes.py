# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Round BW32: attribute-boosted BM25 (`Sum(BM25, weight * attribute)`) on the 1M MS MARCO fixture with non-filterable `pop` and `pr`.

Usage: bw32_shapes.py BASE_URL NAMESPACE OUT_JSON ANSWERS_JSON [--clients 1,16] [--queries 100] [--cold] [--only a,b] [--pid PID]
Each shape draws three disjoint slices of queries (prewarm, one-client, 16-client). Full answers (ids and scores) go to ANSWERS_JSON
for exact comparison across builds (bw32_compare.py); p50 and p90 (ms) go to OUT_JSON. With --cold, every query of the first slice is
preceded by the bench-only `/_bench/drop_caches`, so each runs on empty engine caches.
"""
import argparse, asyncio, json, os, re, time
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

    def per(count, attributes=("cat",)):
        return {"total": 10, "per": {"attributes": list(attributes), "limit": count}}

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

    commons = ["the", "and", "of", "to"]
    mids = ["water", "people", "city", "state", "use", "company", "years", "american"]
    phrases = ["of the", "in the", "to the", "is a", "on the", "for the"]

    def fixed(op, pool, take):
        def make(args):
            i, t = args
            toks = [pool[(i + j) % len(pool)] for j in range(take)]
            return req(["text", "BM25", t], ["text", op, " ".join(toks)])
        return lambda t: make(t)

    def boost(w, attr):
        # A float attribute must sit under Max (or Saturate or Decay); a uint may stand alone.
        base = ["Attribute", attr] if attr == "pr" else ["Max", [["Attribute", attr], 0]]
        expr = base if w == 1 else ["Product", w, base]
        return lambda t: req(["Sum", [["text", "BM25", t], expr]])

    def sat(t):
        return req(["Sum", [["text", "BM25", t], ["Product", 200, ["Saturate", ["Attribute", "pop"], {"midpoint": 300}]]]])

    out = {
        "plain": slices(lambda t: req(["text", "BM25", t]), texts),
        "pr0.05": slices(boost(0.05, "pr"), texts),
        "pr0.2": slices(boost(0.2, "pr"), texts),
        "pr1": slices(boost(1, "pr"), texts),
        "pr5": slices(boost(5, "pr"), texts),
        "pop0.002": slices(boost(0.002, "pop"), texts),
        "pop0.02": slices(boost(0.02, "pop"), texts),
        "pop0.1": slices(boost(0.1, "pop"), texts),
        "sat200": slices(sat, texts),
        "pr0.2+filter": slices(lambda t: dict(boost(0.2, "pr")(t), filters=["id", "Gte", 200000]), texts),
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


def cpu_seconds(pid):
    """User plus system CPU seconds of a process (all threads), or 0 without a pid."""
    if not pid:
        return 0.0
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base"); ap.add_argument("namespace"); ap.add_argument("out"); ap.add_argument("answers")
    ap.add_argument("--clients", default="1,16"); ap.add_argument("--queries", type=int, default=100)
    ap.add_argument("--cold", action="store_true"); ap.add_argument("--only", default="")
    ap.add_argument("--pid", type=int, default=0, help="server PID: also report server CPU ms per query")
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
                cpu = cpu_seconds(a.pid)
                lats, ans = [], {}
                for i, body in enumerate(sl[0]):
                    await client.post(f"{a.base}/_bench/drop_caches")
                    l, an, _ = await run(client, url, [body], 1)
                    lats += l; ans[i] = an[0]
                cpu = cpu_seconds(a.pid) - cpu
                result[name] = {"first_ms": lats[0], "p50": pct(lats, .5), "p90": pct(lats, .9),
                                "cpu_ms": 1000 * cpu / len(lats)}
                answers[name]["cold"] = ans
            else:
                await run(client, url, sl[0], 1)
                result[name] = {}
                for n, c in enumerate(map(int, a.clients.split(","))):
                    cpu = cpu_seconds(a.pid)
                    lats, ans, wall = await run(client, url, sl[n + 1], c)
                    cpu = cpu_seconds(a.pid) - cpu
                    result[name][f"c{c}"] = {"p50": pct(lats, .5), "p90": pct(lats, .9), "qps": len(lats) / wall,
                                             "cpu_ms": 1000 * cpu / len(lats)}
                    result[name]["nonempty"] = result[name].get("nonempty", 0) + sum(1 for v in ans.values() if v)
                    answers[name][f"c{c}"] = ans
            print(name, json.dumps(result[name]), flush=True)
    Path(a.out).write_text(json.dumps(result, indent=1))
    Path(a.answers).write_text(json.dumps(answers))

asyncio.run(main())
