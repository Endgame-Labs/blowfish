# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.0"]
# ///
"""Cold S3 read waves for the Round C35 model: n parallel ranged GETs at random offsets of the largest
slab objects, all started together; prints the wave's last-byte time (the maximum) over many trials.

Usage: c35_s3wave.py PREFIX OUT.json   (bucket from BF_S3_BUCKET; PREFIX like tmp/c35/10m/ns/wikipedia-10m/slabs/)
Sizes: 16 KB (a rerank read) and 1.3 MB (a leaf's exact vectors). Each trial picks fresh random offsets,
so reads are the first read of their bytes (the 16 KB blocks are not re-read within the run).
"""
import json, os, random, sys, time
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config

s3 = boto3.client("s3", region_name="us-west-2", config=Config(max_pool_connections=256))
B = os.environ["BF_S3_BUCKET"]
objs = []
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix=sys.argv[1]):
    objs += [(o["Key"], o["Size"]) for o in pg.get("Contents", []) if o["Size"] > (64 << 20)]
objs.sort(key=lambda x: -x[1])
print(len(objs), "objects", flush=True)
ex = ThreadPoolExecutor(128)
random.seed(35)

def get(key, off, n):
    t = time.perf_counter()
    s3.get_object(Bucket=B, Key=key, Range=f"bytes={off}-{off+n-1}")["Body"].read()
    return (time.perf_counter() - t) * 1000

def wave(n_reads, size):
    picks = []
    for _ in range(n_reads):
        k, sz = random.choice(objs)
        picks.append((k, random.randrange(0, sz - size), size))
    t = time.perf_counter()
    lat = list(ex.map(lambda p: get(*p), picks))
    return max(lat), lat

list(ex.map(lambda i: get(objs[0][0], 0, 100), range(64)))  # warm connections
out = {}
for size, counts, trials in ((16384, (1, 2, 3, 4, 6, 8, 12, 16, 24, 35, 48), 60), (1_300_000, (4, 8, 16, 20), 20)):
    for n in counts:
        maxes, allv = [], []
        for _ in range(trials):
            m, lat = wave(n, size)
            maxes.append(m); allv += lat
            time.sleep(0.05)
        out[f"{size}:{n}"] = {"max": maxes, "all": allv}
        s = sorted(maxes); a = sorted(allv)
        print(f"size={size} n={n:3d} wave max p50={s[len(s)//2]:.0f} p90={s[len(s)*9//10]:.0f}  single p50={a[len(a)//2]:.0f} p90={a[len(a)*9//10]:.0f}", flush=True)
json.dump(out, open(sys.argv[2], "w"))
