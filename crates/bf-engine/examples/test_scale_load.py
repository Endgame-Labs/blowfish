# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "pyarrow==26.0.1", "httpx==0.28.1"]
# ///
"""Numerical and request-lifetime checks; run this script using uv."""
import contextlib
import gc
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import weakref

import httpx
import numpy as np
import pyarrow as pa
import scale_load
from scale_load import update_truth


class StreamingTruth(unittest.TestCase):
    def test_uneven_batches_match_full_cosine_scan(self):
        rng = np.random.default_rng(4)
        matrix = rng.normal(size=(373, 32)).astype("f4")
        query = rng.normal(size=(7, 32)).astype("f4")
        query /= np.linalg.norm(query, axis=1, keepdims=True)
        scores = np.full((7, 10), -np.inf, dtype="f4")
        ids = np.full((7, 10), -1, dtype="i8")
        for first in range(0, len(matrix), 23):
            scores, ids = update_truth(query, matrix[first:first+23],
                                      np.arange(first, min(first+23, len(matrix))), scores, ids)
        full = query @ (matrix / np.linalg.norm(matrix, axis=1, keepdims=True)).T
        np.testing.assert_array_equal(ids, np.argsort(-full, axis=1)[:, :10])
        np.testing.assert_allclose(scores, np.take_along_axis(full, ids, axis=1), atol=2e-7)

    def test_sparse_filtered_batches_keep_top_ten_across_empty_batches(self):
        matrix = np.eye(16, dtype="f4")
        query = np.arange(1, 17, dtype="f4").reshape(1, 16)
        query /= np.linalg.norm(query)
        scores = np.full((1, 10), -np.inf, dtype="f4")
        ids = np.full((1, 10), -1, dtype="i8")
        for first in range(0, 16, 3):
            scores, ids = update_truth(query, matrix[first:first+3],
                                      np.arange(first, min(first+3, 16)), scores, ids)
            scores, ids = update_truth(query, matrix[:0], np.array([], dtype="i8"), scores, ids)
        np.testing.assert_array_equal(ids, np.arange(15, 5, -1).reshape(1, 10))


class RequestLifetime(unittest.TestCase):
    def test_retry_cycles_release_requests_without_automatic_gc(self):
        vector = [1.0] + [0.0] * 1023
        held = pa.table({"emb": [vector] * 200, "title": ["query"] * 200})
        batch = pa.table({"emb": [vector] * 4, "title": ["alpha", "beta", "zeta", "delta"],
                          "text": ["A real request body fixture." * 20] * 4}).to_batches()[0]
        requests, live = [], []

        def handle(request):
            requests.append(weakref.ref(request))
            live.append(sum(ref() is not None for ref in requests))
            if len(requests) == 1:
                raise httpx.ReadError("fixture transport failure", request=request)
            return httpx.Response(429 if len(requests) < 5 else 200, json={})

        client = httpx.Client(transport=httpx.MockTransport(handle))
        scratch = Path(__file__).resolve().parents[3] / ".scratch" / "scale-10m"
        existed = scratch.exists()
        scratch.mkdir(parents=True, exist_ok=True)
        automatic = gc.isenabled()
        gc.disable()
        try:
            with tempfile.TemporaryDirectory(prefix="request-lifetime-", dir=scratch) as directory:
                with (patch("sys.argv", ["scale_load", "--scratch", directory, "--rows", "3", "--batch", "4"]),
                      patch.object(scale_load.httpx, "Client", return_value=client),
                      patch.object(scale_load, "available", return_value=1 << 40),
                      patch.object(scale_load, "download", side_effect=lambda c, e, s, p: p.touch()),
                      patch.object(scale_load.pq, "read_table", return_value=Mock(take=Mock(return_value=held))),
                      patch.object(scale_load.pq, "ParquetFile", return_value=Mock(iter_batches=Mock(return_value=[batch]))),
                      patch.object(scale_load.time, "sleep"), contextlib.redirect_stdout(io.StringIO())):
                    scale_load.main()
                self.assertEqual(live, [1] * 5)
                self.assertTrue(all(ref() is None for ref in requests))
                self.assertFalse(list(Path(directory).glob("*.parquet")))
        finally:
            client.close()
            if automatic:
                gc.enable()
            gc.collect()
            if not existed:
                scratch.rmdir()


if __name__ == "__main__":
    unittest.main()
