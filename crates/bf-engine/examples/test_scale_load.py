"""Numerical checks for the scale loader; run using uv and the loader's dependencies."""
import unittest

import numpy as np
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


if __name__ == "__main__":
    unittest.main()
