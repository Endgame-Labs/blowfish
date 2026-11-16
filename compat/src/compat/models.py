"""Native embedding and model reranking scenarios.

They run only when the server under test has models configured, and say which with
`COMPAT_MODELS_EMBED` and `COMPAT_MODELS_RERANK` (registry names), so the oracle fixtures are
unaffected. Assertions are about behavior (relevant passages first, ordering, shapes, limits),
not exact vectors, so any real model passes too.
"""
import os
import uuid

import turbopuffer

DOCS = [
    (1, "The Danube is a long river in central europe"),
    (2, "Mount Everest is the highest mountain in the himalayas"),
    (3, "A quick brown fox jumps over the lazy dog"),
    (4, "The Rhine is a major river flowing through germany"),
    (5, "Bananas are rich in potassium and grow in tropical climates"),
]


def _dims(schema):
    return int(schema["embed_text"]["type"].split("]")[0].lstrip("["))


def _ids(result):
    return [r.id for r in result.rows]


def _expect_status(status, fn):
    try:
        fn()
    except turbopuffer.APIStatusError as exc:
        assert exc.status_code == status, f"expected {status}, got {exc.status_code}: {exc.body}"
        return
    raise AssertionError(f"expected HTTP {status}")


def scenarios(embed, rerank):
    def write_and_query(ns):
        w = ns.write(schema={"text": {"type": "string", "embed": embed, "full_text_search": True}},
                     upsert_rows=[{"id": i, "text": t} for i, t in DOCS])
        assert w.rows_affected == len(DOCS)
        assert (w.performance.embedding_tokens or 0) > 0, "write reports embedding_tokens"
        schema = ns.metadata().model_dump()["schema"]
        assert schema["text"]["embed"] is not None
        dims = _dims(schema)
        r = ns.query(rank_by=["text", "ANN", ["Embed", "river europe"]], top_k=2)
        assert set(_ids(r)) == {1, 4}, _ids(r)
        assert (r.performance.embedding_tokens or 0) > 0 and r.performance.embedding_ms is not None
        r = ns.query(rank_by=["embed_text", "ANN", ["Embed", "highest mountain", {"model": embed}]], top_k=1)
        assert _ids(r) == [2], _ids(r)
        r = ns.query(rank_by=["text", "ANN", ["Embed", "river"]], top_k=1, include_attributes=["embed_text"])
        assert len(r.rows[0]["embed_text"]) == dims

    def supplied_vectors(ns):
        ns.write(schema={"text": {"type": "string", "embed": embed}}, upsert_rows=[{"id": 1, "text": "river"}])
        dims = _dims(ns.metadata().model_dump()["schema"])
        ns.write(upsert_rows=[{"id": 2, "text": "supplied", "embed_text": [1.0] + [0.0] * (dims - 1)}])
        r = ns.query(rank_by=["id", "asc"], top_k=10, include_attributes=["embed_text"])
        assert r.rows[1]["embed_text"][0] == 1.0 or abs(r.rows[1]["embed_text"][0] - 1.0) < 1e-6
        _expect_status(400, lambda: ns.write(upsert_rows=[{"id": 3, "text": "x", "embed_text": [1.0, 0.0]}]))

    def write_limit(ns):
        ns.write(schema={"text": {"type": "string", "embed": embed}}, upsert_rows=[{"id": 0, "text": "seed"}])
        _expect_status(400, lambda: ns.write(upsert_rows=[{"id": i + 1, "text": f"row {i}"} for i in range(257)]))

    def schema_changes(ns):
        ns.write(distance_metric="cosine_distance", schema={"vec": "[4]f32"}, upsert_rows=[{"id": 1, "vec": [1, 0, 0, 0]}])
        # Embedding into a 4-dimensional column cannot work for a model with other dims.
        _expect_status(400, lambda: ns.write(schema={"text": {"type": "string", "embed": {"model": embed, "attribute": "vec"}}},
                                             upsert_rows=[{"id": 2, "text": "river"}]))

    def unknown_model(ns):
        _expect_status(400, lambda: ns.write(schema={"text": {"type": "string", "embed": "blowfish-no-such-model"}},
                                             upsert_rows=[{"id": 1, "text": "x"}]))

    def rerank_single(ns):
        ns.write(schema={"text": {"type": "string", "full_text_search": True}}, upsert_rows=[{"id": i, "text": t} for i, t in DOCS],
                 distance_metric="cosine_distance")
        r = ns.query(rank_by=["id", "asc"], limit=10, include_attributes=["text"],
                     rerank_by={"model": rerank, "attribute": "text", "query": "a famous river in europe"})
        assert set(_ids(r)[:2]) == {1, 4}, _ids(r)
        dists = [row["$dist"] for row in r.rows]
        assert dists == sorted(dists, reverse=True)
        r = ns.query(rank_by=["id", "asc"], limit=3, rerank_by={"model": rerank, "attribute": "text", "query": "river"})
        assert len(r.rows) == 3 and "text" not in r.rows[0]
        _expect_status(400, lambda: ns.query(rank_by=["id", "asc"], limit=3,
                                             rerank_by={"model": "blowfish-no-such-model", "attribute": "text", "query": "x"}))

    def rerank_multi(ns):
        ns.write(schema={"text": {"type": "string", "embed": embed, "full_text_search": True}},
                 upsert_rows=[{"id": i, "text": t} for i, t in DOCS])
        q = "river in europe"
        body = dict(queries=[
            {"rank_by": ["text", "BM25", q], "limit": 5, "include_attributes": ["text"]},
            {"rank_by": ["text", "ANN", ["Embed", q]], "limit": 5, "include_attributes": ["text"]},
        ], rerank_by={"model": rerank, "attribute": "text", "query": q})
        r = ns.multi_query(limit=3, **body)
        assert len(r.results) == 1
        ids = [row.id for row in r.results[0].rows]
        assert len(ids) == 3 and len(set(ids)) == 3 and set(ids[:2]) == {1, 4}, ids
        page = ns.multi_query(limit=1, offset=1, **body)
        assert [row.id for row in page.results[0].rows] == [ids[1]]

    return [("embed_write_and_query", write_and_query), ("embed_supplied_vectors", supplied_vectors),
            ("embed_write_limit", write_limit), ("embed_dimension_mismatch", schema_changes),
            ("embed_unknown_model", unknown_model), ("rerank_single", rerank_single), ("rerank_multi_query", rerank_multi)]


def run(client):
    """Run every model scenario against the client; returns (passed, failed)."""
    embed, rerank = os.environ["COMPAT_MODELS_EMBED"], os.environ["COMPAT_MODELS_RERANK"]
    passed = failed = 0
    for name, fn in scenarios(embed, rerank):
        ns = client.namespace("blowfish-oracle-compat-models-" + name.replace("_", "-") + "-" + uuid.uuid4().hex[:8])
        try:
            fn(ns)
            print(f"PASS models_{name}", flush=True)
            passed += 1
        except Exception as exc:
            print(f"FAIL models_{name}: {type(exc).__name__}: {exc}", flush=True)
            failed += 1
        finally:
            try:
                ns.delete_all()
            except turbopuffer.NotFoundError:
                pass
    print(f"models: {passed}/{passed + failed} passed")
    return passed, failed
