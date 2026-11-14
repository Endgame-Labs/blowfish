"""Exercise current Python SDK operation models and both transparent polling clients."""
import asyncio
import sys
import time
import uuid

import turbopuffer


def main():
    client = turbopuffer.Turbopuffer(base_url=sys.argv[1], api_key="dummy", max_retries=0, timeout=15)
    name = "blowfish-oracle-python-operations-" + uuid.uuid4().hex[:12]
    names = [name, name + "-explicit", name + "-preferred", name + "-asyncio"]
    source, explicit, preferred, _ = [client.namespace(n) for n in names]
    try:
        source.write(upsert_rows=[{"id": 1, "vector": [1.0, 0.0]}], distance_metric="cosine_distance")
        started = explicit.start_copy_from(source_namespace=name)
        deadline = time.monotonic() + 15
        while True:
            operation = explicit.poll_copy_from(started.token)
            assert operation.start_time is not None
            if operation.status == "finished":
                assert operation.finish_time is not None
                assert operation.result.success.status == "OK"
                break
            if time.monotonic() > deadline:
                raise TimeoutError("explicit copy did not finish")
            time.sleep(0.05)
        assert preferred.write(copy_from_namespace=name).status == "OK"
        try:
            preferred.write(copy_from_namespace=name)
        except turbopuffer.BadRequestError as exc:
            assert exc.status_code == 400
        else:
            raise AssertionError("transparent polling must propagate operation errors")

        async def copy_async():
            async with turbopuffer.AsyncTurbopuffer(base_url=sys.argv[1], api_key="dummy", max_retries=0, timeout=15) as async_client:
                target = async_client.namespace(names[-1])
                assert (await target.write(copy_from_namespace=name)).status == "OK"
                assert (await target.query(rank_by=["id", "asc"], top_k=1)).rows[0].id == 1

        asyncio.run(copy_async())
        print("PASS Python explicit start/poll, sync polling, asyncio polling, and operation errors")
    finally:
        for target in [client.namespace(n) for n in names]:
            try:
                target.delete_all()
            except turbopuffer.NotFoundError:
                pass
        client.close()


if __name__ == "__main__":
    main()
