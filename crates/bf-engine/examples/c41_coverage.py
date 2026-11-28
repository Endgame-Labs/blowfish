#!/usr/bin/env python3
"""coverage.py CORPUS.jsonl WORKLOAD.json FIRST N: share of query terms in the top-4096 terms by document frequency."""
import json, re, sys, collections
corpus, work, first, n = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
tok = lambda s: re.findall(r"\w+", s.lower())
df = collections.Counter()
for line in open(corpus):
    df.update(set(tok(json.loads(line)["text"])))
N = 1_000_000
qs = [tok(w["request"]["rank_by"][2]) for w in json.load(open(work))[first:first + n]]
for top in (1024, 4096, 16384, 65536):
    keep = {t for t, _ in df.most_common(top)}
    terms = [t for q in qs for t in q]
    cov = sum(t in keep for t in terms) / len(terms)
    allcov = sum(all(t in keep for t in q) for q in qs) / len(qs)
    # df-weighted: share of the queries' summed document frequency (a proxy for posting bytes)
    w = sum(df[t] for q in qs for t in q if t in keep) / max(1, sum(df[t] for q in qs for t in q))
    unc = sum(sum(t not in keep for t in q) for q in qs) / len(qs)
    print(f"top {top:6d}: term coverage {cov:.3f}  queries fully covered {allcov:.3f}  uncovered terms per query {unc:.2f}  posting-weight covered {w:.3f}")
print("distinct terms", len(df), "queries", len(qs), "terms per query", sum(map(len, qs)) / len(qs))
