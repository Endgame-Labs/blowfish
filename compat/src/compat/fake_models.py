"""A deterministic fake embedding and rerank provider (OpenAI-compatible and Cohere-style).

Used by `scripts/compat_models.sh` so the model scenarios run without any model weights.
Embeddings are bag-of-words vectors in 64 dimensions, so word overlap means similarity.
"""
import json
import sys
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIMS = 64


def embed(text):
    for prefix in ("query: ", "doc: ", "task: search result | query: ", "title: none | text: "):
        text = text.removeprefix(prefix)
    v = [0.0] * DIMS
    for word in "".join(c if c.isalnum() else " " for c in text.lower()).split():
        v[zlib.crc32(word.encode()) % DIMS] += 1.0
    return v


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["content-length"])))
        if self.path == "/v1/embeddings":
            reply = {"data": [{"index": i, "embedding": embed(t)} for i, t in enumerate(body["input"])],
                     "usage": {"prompt_tokens": sum(len(t.split()) for t in body["input"])}}
        elif self.path == "/v1/rerank":
            words = body["query"].lower().split()
            reply = {"results": [{"index": i, "relevance_score": sum(w in d.lower() for w in words) / len(words)}
                                 for i, d in enumerate(body["documents"])]}
        else:
            self.send_response(404)
            self.end_headers()
            return
        data = json.dumps(reply).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
