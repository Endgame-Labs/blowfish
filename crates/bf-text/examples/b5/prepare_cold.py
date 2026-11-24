# /// script
# requires-python = ">=3.12"
# dependencies = ["zstandard==0.25.0"]
# ///
"""Inspect or repack an owned, stopped checkpoint for the offline B5 cold test."""
import argparse
import copy
import io
import json
import os
from pathlib import Path
import shutil

import zstandard

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--scratch", required=True, type=Path)
p.add_argument("mode", choices=["inspect", "repack"])
a = p.parse_args()
root = a.scratch.resolve()
if ".scratch" not in root.parts:
    p.error("fixture must be under .scratch")
source = root / "checkpoint"
namespace = source / "ns/b5-msmarco"
slot = sorted((namespace / "manifest").glob("*.json"))[-1]
blob = slot.read_bytes()
assert blob[:8] == b"BFPUT001"
blob = blob[24:]
if blob[:4] == bytes.fromhex("28b52ffd"):
    blob = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(blob)).read()
manifest = json.loads(blob)
if a.mode == "inspect":
    (root / "manifest-decoded.local").write_text(json.dumps(manifest))
    print(json.dumps({"segments": len(manifest["segments"]), "checkpoint_wal": manifest["checkpoint_wal"]}))
else:
    # The source writer must stay stopped between inspection, packing, and replay.
    assert manifest == json.loads((root / "manifest-decoded.local").read_text())
    index = json.loads((root / "pack-index.local").read_text())
    destination = root / "checkpoint-pack"
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination, copy_function=os.link)
    file = manifest["text_stats"]
    file.update(key=index["key"], size=index["size"], metadata_len=0)
    file["stats_index"]["fields"]["text"]["chunks"] = index["groups"]

    def encode(value):
        data = json.dumps(value, separators=(",", ":")).encode()
        return zstandard.ZstdCompressor(level=3).compress(data) if len(data) > 64 * 1024 else data

    encoded = encode(manifest)
    reader = copy.deepcopy(manifest)
    for segment in reader["segments"]:
        for file in segment["files"].values():
            file.pop("text_header", None)
    reader["root"] = True
    compact = encode(reader)
    latest = compact if len(encoded) > 256 * 1024 and len(compact) < len(encoded) // 2 else encoded
    ns = destination / "ns/b5-msmarco"
    for path, payload in [(ns / "manifest" / slot.name, encoded),
                          (ns / "manifest-latest", int(slot.stem).to_bytes(8, "big") + latest)]:
        path.unlink()  # Never truncate an object still hard-linked by the baseline.
        path.write_bytes(b"BFPUT001" + bytes(16) + payload)
    print(json.dumps({"groups": len(index["groups"]), "manifest_bytes": len(encoded),
                      "reader_copy_bytes": len(latest), "packed_bytes": index["size"]}))
