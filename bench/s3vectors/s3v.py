"""Minimal signed REST client for Amazon S3 Vectors (raw HTTP, so JSON is built with orjson, not boto3)."""
import time

import botocore.session
import orjson
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

REGION = "us-west-2"
HOST = f"https://s3vectors.{REGION}.api.aws"
BUCKET = "blowfish-s3v-bench"
INDEX = "wiki10m"

_creds = botocore.session.get_session().get_credentials()


class Client:
    def __init__(self, bucket=BUCKET, index=INDEX):
        self.http = requests.Session()
        self.bucket, self.index = bucket, index

    def call(self, op, body):
        """POST one operation. Returns (status, parsed body or text, seconds)."""
        data = orjson.dumps(body, option=orjson.OPT_SERIALIZE_NUMPY)
        req = AWSRequest(method="POST", url=f"{HOST}/{op}", data=data,
                         headers={"Content-Type": "application/json"})
        SigV4Auth(_creds.get_frozen_credentials(), "s3vectors", REGION).add_auth(req)
        t = time.perf_counter()
        r = self.http.post(req.url, data=data, headers=dict(req.headers), timeout=120)
        dt = time.perf_counter() - t
        try:
            out = r.json() if r.content else {}
        except ValueError:
            out = r.text
        return r.status_code, out, dt

    def put(self, keys, matrix):
        vectors = [{"key": k, "data": {"float32": v}} for k, v in zip(keys, matrix)]
        return self.call("PutVectors", {"vectorBucketName": self.bucket, "indexName": self.index, "vectors": vectors})

    def query(self, vector, top_k=10):
        return self.call("QueryVectors", {"vectorBucketName": self.bucket, "indexName": self.index,
                                          "queryVector": {"float32": vector}, "topK": top_k})
