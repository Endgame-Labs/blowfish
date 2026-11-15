"""Write the server's storage JSON from config.local plus cache knobs.

Usage: mkconfig.py CONFIG OUT CACHE_DIR [--memory-gib 8] [--block 4000] [--coalesce 128] [--read-only]
"""
import argparse, json
p = argparse.ArgumentParser()
p.add_argument("config"); p.add_argument("out"); p.add_argument("cache_dir")
p.add_argument("--memory-gib", type=float, default=8)
p.add_argument("--disk-gib", type=float, default=48)
p.add_argument("--block", type=int, default=4000)
p.add_argument("--coalesce", type=int, default=128)
p.add_argument("--fetch", type=int, default=128)
p.add_argument("--read-only", action="store_true")
a = p.parse_args()
c = json.load(open(a.config))
json.dump({
    "url": c["storage_url"], "endpoint": c.get("endpoint") or None,
    "region": c.get("region") or None, "profile": c.get("profile") or None,
    "read_only": a.read_only,
    "cache": {"memory_bytes": int(a.memory_gib * 2**30), "disk_bytes": int(a.disk_gib * 2**30),
              "disk_dir": a.cache_dir, "block_size": a.block, "max_coalesce_blocks": a.coalesce,
              "fetch_concurrency": a.fetch},
}, open(a.out, "w"))
