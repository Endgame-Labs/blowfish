"""Check metadata indexing progress through the official SDK against a slow indexer."""
import sys
import time
import uuid

import turbopuffer


def main():
    with turbopuffer.Turbopuffer(base_url=sys.argv[1], api_key="dummy", max_retries=0, timeout=15) as client:
        for kind in ("scalar", "vector", "text"):
            ns = client.namespace("blowfish-oracle-metadata-" + uuid.uuid4().hex[:12])
            try:
                rows = [{"id": i, "n": i} for i in range(3)]
                args = {}
                if kind == "vector":
                    rows = [{**row, "vector": [1.0, float(row["id"])]} for row in rows]
                    args["distance_metric"] = "cosine_distance"
                elif kind == "text":
                    rows = [{**row, "text": "little fish"} for row in rows]
                    args["schema"] = {"text": {"type": "string", "full_text_search": True}}
                ns.write(upsert_rows=rows, **args)
                index = ns.metadata().index
                assert index.status == "updating", index
                assert type(index.unindexed_rows) is int and index.unindexed_rows == 3, index
                assert type(index.unindexed_bytes) is int and index.unindexed_bytes > 0, index
                deadline = time.monotonic() + 20
                while index.status != "up-to-date":
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"{kind} index did not catch up: {index}")
                    time.sleep(0.05)
                    index = ns.metadata().index
                assert index.model_dump(exclude_unset=True) == {"status": "up-to-date"}
            finally:
                ns.delete_all()
    print("PASS Python SDK metadata updating, exact rows, and up-to-date for scalar, vector, and text namespaces")


if __name__ == "__main__":
    main()
