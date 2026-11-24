"""The official SDK must decode both index states from metadata HTTP responses."""
import json
import unittest

import httpx2 as httpx
import turbopuffer
from turbopuffer.types.namespace_metadata import IndexIndexUpdating, IndexIndexUpToDate

from compat.cli import FIXTURES, normalize


class MetadataTests(unittest.TestCase):
    def metadata(self, index):
        return {**json.loads((FIXTURES / "metadata.json").read_text()), "index": index}

    def test_sdk_parses_both_index_states(self):
        for index, model in (
            ({"status": "up-to-date"}, IndexIndexUpToDate),
            ({"status": "updating", "unindexed_bytes": 1 << 40, "unindexed_rows": 375_000}, IndexIndexUpdating),
            ({"status": "updating", "unindexed_bytes": 128, "unindexed_rows": 0}, IndexIndexUpdating),
        ):
            with self.subTest(index=index):
                body = self.metadata(index)
                # Recorded fixtures omit timestamps, which every live metadata reply includes.
                body.update(created_at="2026-01-01T00:00:00Z", updated_at="2026-01-01T00:00:00Z")
                transport = httpx.MockTransport(lambda _: httpx.Response(200, json=body))
                with turbopuffer.Turbopuffer(
                    api_key="test", base_url="http://localhost", http_client=httpx.Client(transport=transport),
                    _strict_response_validation=True,
                ) as client:
                    parsed = client.namespace("metadata").metadata().index
                self.assertIsInstance(parsed, model)
                self.assertEqual(parsed.model_dump(exclude_unset=True), index)

    def test_normalization_allows_either_valid_state(self):
        ready = normalize(self.metadata({"status": "up-to-date"}))
        pending = normalize(self.metadata({"status": "updating", "unindexed_bytes": 128, "unindexed_rows": 2}))
        self.assertEqual(ready, pending)

    def test_normalization_rejects_malformed_states(self):
        invalid = [None, {}, {"status": "unknown"}, {"status": []}]
        for key in ("unindexed_bytes", "unindexed_rows"):
            for item in (-1, 1.5, True, None, "2"):
                invalid.append({"status": "updating", "unindexed_bytes": 128, "unindexed_rows": 2, key: item})
            invalid.append({"status": "updating", key: 0})
        for index in invalid:
            with self.subTest(index=index), self.assertRaises(ValueError):
                normalize(self.metadata(index))


if __name__ == "__main__":
    unittest.main()
