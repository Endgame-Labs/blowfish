"""Checkpoint-copy regressions: live references, legacy headers, and WAL safety."""
import io
import json
import unittest

import inregion_copy as copy


class Paginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": key, "Size": len(value), "ETag": '"etag"'}
                            for key, value in self.objects.items() if key.startswith(Prefix)]}


class Store:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, _):
        return Paginator(self.objects)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


class CheckpointTests(unittest.TestCase):
    def fixture(self):
        file = {"key": "ns/example/segments/live", "size": 8, "metadata_len": 0}
        manifest = {"indexed_through": 9, "stats": {"rows": 3},
                    "segments": [{"files": {"docs": file},
                                  "live": {"key": "", "size": 0, "metadata_len": 0}}],
                    "retired": [{"key": "ns/example/segments/dead", "retired_at": "old"}]}
        base = "scale/ns/example/"
        objects = {base + "manifest/00000000000000000002.json": json.dumps(manifest).encode(),
                   base + "segments/live": b"live", base + "segments/dead": b"dead",
                   base + "wal/00000000000000000009.wal": b"checkpointed",
                   base + "segments/ann-dir-latest-current.bin":
                       (len(file["key"]).to_bytes(4, "little") + file["key"].encode() + b"hint"),
                   base + "segments/ann-dir-latest-stale.bin": b"\x04\0\0\0deadignored",
                   "scale/_system/format/00000000000000000002.json": b"fence"}
        return objects

    def test_only_live_objects_and_matching_hints_are_selected(self):
        plan = copy.checkpoint(Store(self.fixture()), "bucket", "scale", "example")
        self.assertEqual(len(plan["objects"]), 4)
        self.assertFalse(any(key.endswith(("/dead", ".wal", "stale.bin")) for key in plan["objects"]))
        self.assertEqual(plan["checkpoint_wal"], 9)

    def test_tail_is_rejected_before_any_copy(self):
        objects = self.fixture()
        objects["scale/ns/example/wal/00000000000000000010.wal"] = b"live tail"
        with self.assertRaisesRegex(ValueError, "uncheckpointed WAL"):
            copy.checkpoint(Store(objects), "bucket", "scale", "example")

    def test_legacy_pointer_and_retry_header(self):
        objects = self.fixture()
        slot = "scale/ns/example/manifest/00000000000000000002.json"
        payload = "ns/example/manifest-data/current"
        objects["scale/" + payload] = b"BFPUT001" + bytes(16) + objects[slot]
        objects[slot] = json.dumps({"payload": payload}).encode()
        # Storage strips portable headers before decoding manifests.
        plan = copy.checkpoint(Store(objects), "bucket", "scale", "example")
        self.assertIn("scale/" + payload, plan["objects"])
        self.assertEqual(copy.decode(copy.logical(objects["scale/" + payload]))["stats"]["rows"], 3)


if __name__ == "__main__":
    unittest.main()
