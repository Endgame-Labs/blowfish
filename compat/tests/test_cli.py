import unittest

from compat.cli import compare, error_category, normalize, register, SCENARIOS


class ComparisonTests(unittest.TestCase):
    def test_normalization_preserves_results_and_validates_metrics(self):
        self.assertEqual(normalize({"rows": [{"id": 1}], "performance": {}, "billing": {}}), {"rows": [{"id": 1}]})
        with self.assertRaises(ValueError):
            normalize({"billing": None})

    def test_distances_and_error_messages(self):
        self.assertEqual(compare({"$dist": 1.0}, {"$dist": 1.01}, tol=0.02), [])
        self.assertTrue(compare({"$dist": 1.0}, {"$dist": 1.1}, tol=0.02))
        self.assertEqual(compare({"error": {"status": 400, "message": "one"}}, {"error": {"status": 400, "message": "two"}}), [])
        self.assertEqual(error_category("Invalid query dimensions"), "dimensions")

    def test_equal_score_rows_can_change_order(self):
        rows = [{"id": 1, "$dist": 1.0}, {"id": 2, "$dist": 1.0}]
        self.assertEqual(compare(rows, list(reversed(rows))), [])

    def test_scenarios_registered(self):
        register()
        self.assertGreaterEqual(len(SCENARIOS), 80)


if __name__ == "__main__":
    unittest.main()
