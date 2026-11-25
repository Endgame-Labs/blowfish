# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Round C10: how many rerank vector pages does a held-out query share with other queries?

Reads the rows.jsonl a cold_ab.py run wrote with BF_TRACE_READS=1 and staging disabled (so every
rerank vector read is remote), maps each query's .bfv reads above 1 MiB to 16 KiB pages, then
reports the share of a query's pages already present in other queries' page sets.

Usage: uv run page_overlap.py rows.jsonl
"""
import json,numpy as np,collections,sys
rows=[json.loads(l) for l in open(sys.argv[1])]
P=16384
def pages(r):
    s=set()
    for t in r['read_timeline']:
        if t['kind']=='slabs' and t['object'].endswith('.bfv') and t['offset']>=1<<20:
            for p in range(t['offset']//P,(t['limit']-1)//P+1): s.add((t['object'],p))
    return s
S=[pages(r) for r in rows]
print('queries',len(S),'pages/query median',np.median([len(s) for s in S]),'reads/query',np.median([sum(1 for t in r['read_timeline'] if t['object'].endswith('.bfv') and t['offset']>=1<<20) for r in rows]))
allp=collections.Counter(p for s in S for p in s)
print('distinct pages',len(allp),'total',sum(allp.values()), 'pages seen >1 time',sum(1 for c in allp.values() if c>1))
# 2-fold: train on even-index queries, test odd and vice versa
def cover(train,test):
    T=set().union(*train)
    fr=[len(s&T)/len(s) for s in test if s]
    full=sum(1 for s in test if s and len(s-T)==0)
    return np.mean(fr),np.median(fr),full
for name,(tr,te) in {'even->odd':(S[0::2],S[1::2]),'odd->even':(S[1::2],S[0::2]),'first100->last100':(S[:100],S[100:])}.items():
    print(name,cover(tr,te))
# 'leave-one-out' with all other 199 queries (upper bound for a pack made from all other queries, unlimited budget)
tot=set().union(*S)
loo=[]
for i,s in enumerate(S):
    other=collections.Counter(p for j,t in enumerate(S) if j!=i for p in t)
    loo.append(sum(1 for p in s if p in other)/len(s))
print('LOO covered fraction mean',np.mean(loo),'median',np.median(loo),'max',max(loo))
