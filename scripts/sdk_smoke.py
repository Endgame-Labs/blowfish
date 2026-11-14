"""Run with uv run --with turbopuffer python scripts/sdk_smoke.py.

Set TURBOPUFFER_BASE_URL and TURBOPUFFER_API_KEY, and unset TURBOPUFFER_REGION.
"""
import uuid

from turbopuffer import Turbopuffer


def main():
    client = Turbopuffer(compression=True, _strict_response_validation=True)
    namespace = client.namespace(f"sdk-smoke-{uuid.uuid4().hex}")
    try:
        result = namespace.write(
            distance_metric="cosine_distance",
            schema={"text": {"type": "string", "full_text_search": True}},
            upsert_rows=[
                {"id": 1, "vector": [1.0, 0.0], "text": "hello fish", "n": 1},
                {"id": 2, "vector": [0.0, 1.0], "text": "hello ocean", "n": 2},
            ],
        )
        assert result.rows_affected == 2
        result = namespace.query(
            rank_by=("vector", "ANN", [1.0, 0.0]),
            top_k=2,
            include_attributes=True,
        )
        assert result.rows[0].id == 1
        assert getattr(result.rows[0], "$dist") == 0.0
        assert namespace.query(rank_by=("text", "BM25", "fish"), top_k=10).rows[0].id == 1
        assert namespace.metadata().approx_row_count == 2
        assert namespace.schema()["vector"].type == "[2]f32"
        assert namespace.exists()
        namespace.write(patch_rows=[{"id": 1, "n": 3}], deletes=[2])
        assert namespace.query(aggregate_by={"count": ("Count",)}).aggregations["count"] == 1
        result = namespace.multi_query(
            queries=[
                {"rank_by": ("id", "asc"), "top_k": 1},
                {"rank_by": ("text", "BM25", "hello"), "top_k": 1},
            ]
        )
        assert len(result.results) == 2
        namespace.write(upsert_rows=[{"id": 3, "vector": [1.0, 1.0], "text": "fish " * 1000}])
        assert namespace.metadata().approx_row_count == 2
        print("Official turbopuffer SDK smoke passed 🐡")
    finally:
        namespace.delete_all()
    assert not namespace.exists()


if __name__ == "__main__":
    main()
