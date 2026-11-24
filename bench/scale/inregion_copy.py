# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.44.9", "zstandard==0.25.0"]
# ///
"""Copy a quiescent benchmark checkpoint without retired segments or checkpointed WAL.

Run on the benchmark instance with its role. Supply a bucket at runtime; no
credentials are read from files. Only v014/ is writable. Manifest references stay
relative to each deployment root: scale/ becomes v014/scale/, and big/ becomes
v014/big/. A live WAL tail is rejected rather than silently losing writes.
Use --plan first, and put --output beneath the checkout's .scratch/ directory.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time

import boto3
from botocore.config import Config
import zstandard


def logical(data):
    """Remove the storage layer's portable retry header, when present."""
    return data[24:] if data.startswith(b"BFPUT001") else data


def decode(data):
    """Decode a logical inline manifest or legacy manifest pointer."""
    if data.startswith(b"\x28\xb5\x2f\xfd"):
        data = zstandard.ZstdDecompressor().decompress(data, max_output_size=300_000_000)
    return json.loads(data)


def references(value):
    """Walk live SegmentFile references, excluding the retired-object ledger."""
    if isinstance(value, dict):
        if all(k in value for k in ("key", "size", "metadata_len")) and value["key"]:
            yield value["key"]
        for key, child in value.items():
            if key != "retired":
                yield from references(child)
    elif isinstance(value, list):
        for child in value:
            yield from references(child)


def listing(client, bucket, prefix):
    """List a prefix with bounded SDK retries and request deadlines."""
    return [obj for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix) for obj in page.get("Contents", [])]


def checkpoint(client, bucket, prefix, namespace):
    """Select the latest full checkpoint, live files, and matching ANN hints."""
    base = f"{prefix}/ns/{namespace}/"
    objects = {o["Key"]: o for o in listing(client, bucket, base)}
    slots = sorted(k for k in objects if k.startswith(base + "manifest/") and k.endswith(".json"))
    if not slots:
        raise ValueError(f"no checkpoint for {namespace}")

    def read(key):
        with client.get_object(Bucket=bucket, Key=key)["Body"] as body:
            return logical(body.read())

    slot = slots[-1]
    keys = {slot}
    manifest = decode(read(slot))
    if "payload" in manifest:
        payload = prefix + "/" + manifest["payload"]
        keys.add(payload)
        manifest = decode(read(payload))
    live = set(references(manifest))
    keys.update(prefix + "/" + key for key in live)
    if base + "manifest-latest" in objects:
        keys.add(base + "manifest-latest")
    for key in objects:
        if key.startswith(base + "segments/ann-dir-latest-"):
            hint = read(key)
            length = int.from_bytes(hint[:4], "little")
            if hint[4:4 + length].decode() in live:
                keys.add(key)
    through = manifest.get("checkpoint_wal", manifest.get("indexed_through"))
    tail = [key for key in objects if key.startswith(base + "wal/")
            and int(key.rsplit("/", 1)[1].split(".")[0]) > through]
    if tail:
        raise ValueError(f"{namespace} has uncheckpointed WAL; stop writers and finish indexing")
    for obj in listing(client, bucket, prefix + "/_system/format/"):
        objects[obj["Key"]] = obj
        keys.add(obj["Key"])
    if any(objects[key]["Size"] > 5 * 1024**3 for key in keys):
        raise ValueError("CopyObject cannot copy an object over 5 GiB")
    return {"prefix": prefix, "namespace": namespace, "manifest": slot,
            "checkpoint_wal": through, "rows": manifest["stats"]["rows"],
            "segments": len(manifest["segments"]),
            "slabs": sum(len(s["slabs"]) for s in manifest.get("ann_slabs", {}).values()),
            "objects": {key: {"bytes": objects[key]["Size"], "etag": objects[key]["ETag"]}
                        for key in sorted(keys)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if ".scratch" not in args.output.resolve().parts:
        parser.error("output must be under .scratch/")
    client = boto3.client("s3", region_name=args.region, config=Config(
        connect_timeout=10, read_timeout=120, max_pool_connections=32,
        retries={"max_attempts": 3}))
    plans = [checkpoint(client, args.bucket, prefix, ns) for prefix, ns in [
        ("scale", "wikipedia-10m"), ("scale", "msmarco-1m"), ("big", "wikipedia-100m")]]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plans))
    for plan in plans:
        objects = plan["objects"]
        report = {key: val for key, val in plan.items() if key not in ("objects", "manifest")}
        report.update(objects=len(objects), bytes=sum(o["bytes"] for o in objects.values()))
        print(json.dumps(report), flush=True)
        if args.plan:
            continue
        start = time.monotonic()

        def copy(item):
            key, obj = item
            destination = "v014/" + key
            client.copy_object(Bucket=args.bucket, Key=destination,
                               CopySource={"Bucket": args.bucket, "Key": key},
                               CopySourceIfMatch=obj["etag"])
            head = client.head_object(Bucket=args.bucket, Key=destination)
            if head["ContentLength"] != obj["bytes"]:
                raise ValueError("copied size mismatch")

        with ThreadPoolExecutor(max_workers=24) as pool:
            for done, _ in enumerate(pool.map(copy, objects.items()), 1):
                if done % 1000 == 0:
                    print(json.dumps({"namespace": plan["namespace"], "copied": done}), flush=True)
        current = checkpoint(client, args.bucket, plan["prefix"], plan["namespace"])
        if current != plan:
            raise RuntimeError("source changed during copy; destination must not be opened")
        print(json.dumps({"namespace": plan["namespace"], "copy_seconds": time.monotonic() - start}), flush=True)


if __name__ == "__main__":
    main()
