"""Run with uv run --with 'turbopuffer==0.1.37' python compat/legacy_smoke.py URL."""
import sys
import uuid

import turbopuffer


def main():
    base = sys.argv[1].rstrip("/")
    name = "blowfish-oracle-legacy-sdk-" + uuid.uuid4().hex[:12]
    ns = turbopuffer.Namespace(name, api_key="dummy", base_url=base)
    clone = turbopuffer.Namespace(name + "-copy", api_key="dummy", base_url=base)
    try:
        ns.upsert(ids=[1, 2, 3], vectors=[[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]], attributes={"label": ["a", "b", "c"]}, distance_metric="cosine_distance")
        assert ns.exists()
        assert ns.dimensions() == 2
        assert ns.approx_count() == 3
        assert ns.created_at() is not None
        rows = list(ns.query(vector=[1.0, 0.0], top_k=2, include_vectors=True, include_attributes=True))
        assert rows[0].id == 1 and rows[0].attributes == {"label": "a"}
        # Like the current oracle, the old SDK keeps base64 query vectors as strings.
        assert rows[0].vector == "AACAPwAAAAA="
        floats = list(ns.query({"vector": [1.0, 0.0], "top_k": 1, "include_vectors": True, "vector_encoding_format": "float"}))
        assert floats[0].vector == [1.0, 0.0]
        assert rows[0].dist == 0.0
        filtered = list(ns.query(rank_by=["id", "asc"], filters={"label": [["Eq", "b"]]}, include_attributes=True))
        assert [row.id for row in filtered] == [2]
        exported = list(ns.vectors())
        assert [row.id for row in exported] == [1, 2, 3]
        clone.copy_from_namespace(name)
        assert len(list(clone.vectors())) == 3
        ns.delete(2)
        assert ns.delete_by_filter({"label": [["Eq", "c"]]}) == 1
        assert [row.id for row in ns.vectors()] == [1]
        print("PASS Python 0.1.37 write, query, base64, filters, HEAD, export, copy, and deletes")
    finally:
        for target in (ns, clone):
            try:
                target.delete_all()
            except turbopuffer.APIError as exc:
                if exc.status_code != 404:
                    raise


if __name__ == "__main__":
    main()
