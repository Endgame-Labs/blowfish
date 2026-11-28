"""Compare two bw32_shapes.py answer files: ordered IDs and scores must be identical for every query.

Usage: bw32_compare.py BASE_ANSWERS CANDIDATE_ANSWERS   (exit 1 on any difference)
"""
import json, sys

a, b = (json.load(open(p)) for p in sys.argv[1:3])
bad = total = 0
for shape in a.keys() & b.keys():
    for key in a[shape].keys() & b[shape].keys():
        for q, ans in a[shape][key].items():
            total += 1
            if b[shape][key].get(q) != ans:
                bad += 1
                print("DIFF", shape, key, q)
print(f"{total} queries compared, {bad} differ")
sys.exit(1 if bad else 0)
