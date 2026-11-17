"""overlap@10 of blowfish's BM25 top-10 against exhaustive BM25 truth (`exact_bm25`), per query and on average.

Usage: uv run python bench/scale/bm25_overlap.py TRUTH.json RESULTS.jsonl
RESULTS.jsonl is the output of `scale_api.py` (one JSON line per query with `query_id` and `response.rows`).
Ties at the 10th score are counted as matches: a returned id whose exact score equals the 10th exact score is
as good as the truth's pick. Prints the mean overlap, the share of queries with overlap 1.0, and the worst few.
"""
import json, sys

truth = {t["query_id"]: t for t in json.load(open(sys.argv[1]))}
got = {}
for line in open(sys.argv[2]):
    if line.startswith("{") and '"query_id"' in line:
        r = json.loads(line)
        got[r["query_id"]] = [int(x["id"]) for x in r["response"].get("rows", [])]
over = []
for qid, t in truth.items():
    want = t["ids"]
    if qid not in got or not want:
        continue
    over.append((len(set(got[qid][:10]) & set(want)) / len(want), qid))
mean = sum(o for o, _ in over) / len(over)
print(f"queries {len(over)}  mean overlap@10 {mean:.4f}  exact {sum(o == 1 for o, _ in over) / len(over):.4f}  worst {sorted(over)[:5]}")
