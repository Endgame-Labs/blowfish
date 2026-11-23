"""Read-only S3 facade over a local benchmark store, with a fixed request delay.

Usage: uv run python bench/scale/local_s3.py STORE --port 18324 --delay-ms 25
Point a read-only blowfish at s3://local with endpoint http://127.0.0.1:18324,
and dummy AWS credentials. This models dependent round trips, not S3's tails,
throughput, connection setup, or request throttling. Never use it for writes.
"""
import argparse
import datetime
import email.utils
import hashlib
import http.server
import pathlib
import socket
import time
import urllib.parse
import xml.sax.saxutils


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("store", type=pathlib.Path)
    parser.add_argument("--port", type=int, default=18324)
    parser.add_argument("--delay-ms", type=float, default=25)
    args = parser.parse_args()
    root = args.store.resolve()
    keys = sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        def log_message(self, *_):
            pass

        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            time.sleep(args.delay_ms / 1000)
            url = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(url.query)
            path = urllib.parse.unquote(url.path).removeprefix("/local").lstrip("/")
            if "list-type" in query:
                prefix = query.get("prefix", [""])[0]
                after = query.get("start-after", query.get("continuation-token", [""]))[0]
                selected = [k for k in keys if k.startswith(prefix) and k > after]
                limit = int(query.get("max-keys", ["1000"])[0])
                truncated = len(selected) > limit
                selected = selected[:limit]
                escape = xml.sax.saxutils.escape
                rows = []
                for key in selected:
                    stat = (root / key).stat()
                    stamp = datetime.datetime.fromtimestamp(stat.st_mtime, datetime.timezone.utc).isoformat()
                    rows.append(f"<Contents><Key>{escape(key)}</Key><LastModified>{stamp}</LastModified>"
                                f'<ETag>"local"</ETag><Size>{stat.st_size}</Size><StorageClass>STANDARD</StorageClass></Contents>')
                token = f"<NextContinuationToken>{escape(selected[-1])}</NextContinuationToken>" if truncated else ""
                body = ('<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                        f"<Name>local</Name><Prefix>{escape(prefix)}</Prefix><KeyCount>{len(selected)}</KeyCount>"
                        f"<MaxKeys>{limit}</MaxKeys><IsTruncated>{str(truncated).lower()}</IsTruncated>{token}"
                        + "".join(rows) + "</ListBucketResult>").encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Type", "application/xml")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
                return
            target = (root / path).resolve()
            if not target.is_relative_to(root) or not target.is_file():
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            stat = target.stat()
            start, end = 0, stat.st_size
            status = 200
            if raw := self.headers.get("Range"):
                lo, hi = raw.removeprefix("bytes=").split("-", 1)
                start = int(lo) if lo else max(0, end - int(hi))
                end = min(end, int(hi) + 1) if lo and hi else end
                status = 206
            self.send_response(status)
            self.send_header("Content-Length", str(end - start))
            self.send_header("Last-Modified", email.utils.formatdate(stat.st_mtime, usegmt=True))
            self.send_header("ETag", '"' + hashlib.md5(str(stat.st_mtime_ns).encode()).hexdigest() + '"')
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end - 1}/{stat.st_size}")
            self.end_headers()
            if self.command != "HEAD":
                with target.open("rb") as source:
                    source.seek(start)
                    remaining = end - start
                    while remaining:
                        data = source.read(min(remaining, 1024 * 1024))
                        self.wfile.write(data)
                        remaining -= len(data)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
