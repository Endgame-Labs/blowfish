# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["numpy==2.5.4", "boto3"]
# ///
"""Spot-check that random loaded keys exist (GetVectors, 100 keys per call). Keys 0..10,000,199 minus the 200 held-out."""
import sys, concurrent.futures as cf, boto3, numpy as np
c = boto3.client("s3vectors", region_name="us-west-2")
held = set(range(0, 100_000, 500))
rng = np.random.default_rng(1)
ids = [int(i) for i in rng.integers(0, 10_000_200, int(sys.argv[1])) if int(i) not in held]
def chunk(b):
    r = c.get_vectors(vectorBucketName="blowfish-s3v-bench", indexName="wiki10m", keys=[str(i) for i in b])
    return len(b) - len(r["vectors"])
with cf.ThreadPoolExecutor(8) as p:
    missing = sum(p.map(chunk, [ids[i:i + 100] for i in range(0, len(ids), 100)]))
print({"checked": len(ids), "missing": missing})
