# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.0"]
# ///
import boto3, os, random, time, statistics as st, sys
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config
s3 = boto3.client("s3", region_name="us-west-2", config=Config(max_pool_connections=64))
B = os.environ["BF_S3_BUCKET"]  # the benchmark bucket
objs = []
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix="tmp/c34/10m/ns/wikipedia-10m/"):
    objs += [(o["Key"], o["Size"]) for o in pg["Contents"] if o["Key"].endswith(".bfa") or o["Key"].endswith(".bfb")]
objs.sort(key=lambda x: -x[1])
print(len(objs), "ann objects, largest", objs[:3])
def get(key, off, n):
    t = time.perf_counter()
    s3.get_object(Bucket=B, Key=key, Range=f"bytes={off}-{off+n-1}")["Body"].read()
    return (time.perf_counter() - t) * 1000
def wave(label, picks, n=16384, par=16):
    # warm the connections first
    with ThreadPoolExecutor(par) as ex:
        lat = sorted(ex.map(lambda p: get(p[0], p[1], n), picks))
    print(f"{label:34s} n={len(lat)} p10={lat[len(lat)//10]:.0f} p50={st.median(lat):.0f} p90={lat[len(lat)*9//10]:.0f} max={lat[-1]:.0f}")
random.seed(1)
with ThreadPoolExecutor(32) as ex:  # warm 32 connections
    list(ex.map(lambda i: get(objs[0][0], 0, 100), range(64)))
for rep in range(2):
    big = random.sample(objs[:20], 20)
    wave("16KB random offsets, 20 big objs", [(k, random.randrange(0, sz - 16384)) for k, sz in big for _ in range(3)])
    wave("16KB offset 0..1MB, same objs", [(k, random.randrange(0, 1 << 20)) for k, sz in big for _ in range(3)])
    k, sz = objs[0]
    wave("16KB random offsets, one object", [(k, random.randrange(0, sz - 16384)) for _ in range(60)])
    wave("256KB random offsets, big objs", [(k, random.randrange(0, sz - 262144)) for k, sz in big for _ in range(3)], n=262144)
    pk = [(k, random.randrange(0, sz - 16384)) for k, sz in big for _ in range(3)]
    wave("16KB random (first read)", pk)
    wave("16KB same offsets again", pk)

print("--- hedge experiment: duplicate after D ms, first answer wins")
import threading
def hedged(key, off, n, delay):
    t = time.perf_counter()
    done = threading.Event()
    def run():
        get(key, off, n); done.set()
    th = [threading.Thread(target=run)]; th[0].start()
    if not done.wait(delay / 1000):
        th.append(threading.Thread(target=run)); th[1].start()
        done.wait()
    return (time.perf_counter() - t) * 1000
for delay in (1e9, 60, 40, 25):
    res = []
    for rep in range(3):
        big = random.sample(objs[:40], 20)
        picks = [(k, random.randrange(0, sz - 16384)) for k, sz in big for _ in range(2)]
        with ThreadPoolExecutor(40) as ex:
            res += sorted(ex.map(lambda p: hedged(p[0], p[1], 16384, delay), picks))
    res.sort()
    print(f"delay={delay:>6} n={len(res)} p50={st.median(res):.0f} p90={res[len(res)*9//10]:.0f} max={res[-1]:.0f}")
