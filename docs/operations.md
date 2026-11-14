# Operating blowfish 🐡

Object storage is the source of truth. Local process state is disposable; protect the bucket and its
credentials. Use blowfish for development, testing, and modest deployments. For production at scale,
we recommend turbopuffer as the upgrade path, using the same SDKs with a different base URL and key.

## Running under systemd with Caddy

Install a release binary outside the checkout so subsequent builds cannot change the deployed
executable. On a shared build host, check available memory first, and limit Cargo parallelism:

```bash
free -g
CARGO_BUILD_JOBS=4 cargo build --release -p blowfish
sudo install -o root -g root -m 755 target/release/blowfish /usr/local/bin/blowfish.next
sudo mv /usr/local/bin/blowfish.next /usr/local/bin/blowfish
```

Create a dedicated system user named `blowfish`, and put configuration in
`/etc/blowfish/blowfish.toml`, readable by that user. Bind `listen` to `127.0.0.1:8080`, set
`require_auth = true`, and use a dedicated storage prefix. Put `BLOWFISH_ADMIN_KEY`,
`AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` in `/etc/blowfish/env`, owned by root with mode
600. Generate the admin key with a cryptographically secure random generator; never echo it or
put secrets in command arguments. Systemd reads the environment file before dropping privileges;
the service user does not need file access to it.

A starting unit at `/etc/systemd/system/blowfish.service` is:

```ini
[Unit]
Description=blowfish search service
Wants=network-online.target
After=network-online.target

[Service]
User=blowfish
Group=blowfish
EnvironmentFile=/etc/blowfish/env
ExecStart=/usr/local/bin/blowfish serve --config /etc/blowfish/blowfish.toml
Restart=on-failure
RestartSec=5
TimeoutStopSec=120
StateDirectory=blowfish
StateDirectoryMode=0750
WorkingDirectory=/var/lib/blowfish
UMask=0077
MemoryHigh=6G
MemoryMax=8G
NoNewPrivileges=true
PrivateTmp=true
PrivateDevices=true
ProtectSystem=strict
ProtectHome=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
CapabilityBoundingSet=
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6

[Install]
WantedBy=multi-user.target
```

Set `storage.cache.disk_dir` under `/var/lib/blowfish`, and configure explicit memory and disk
cache budgets. A 1 GiB memory cache and a 32 GiB disk cache are a modest starting point on a
shared 32 GiB host. The process limit covers more than the byte cache: decoded indexes, document
state, requests, and background work also consume memory. Tune WAL backpressure and concurrency
for the workload; hitting `MemoryMax` can kill the process.

When an upstream proxy terminates HTTPS, Caddy can listen on an internal HTTP port:

```caddyfile
:8000 {
    reverse_proxy 127.0.0.1:8080
}
```

Restrict access to this HTTP listener through the host or upstream network policy. For direct
Internet access, configure a domain and TLS in Caddy instead. Keep any existing upstream access
controls when adding the service. Avoid logging authorization headers.

Validate the unit and Caddy configuration, run `sudo systemctl daemon-reload`, and enable the
services with `sudo systemctl enable --now blowfish caddy`. Use
`sudo journalctl -u blowfish -u caddy -f` for logs. A systemd `active` state does not mean the
application has finished its storage probe: wait for an authenticated `GET /` through Caddy to
return 200. Missing credentials should return 401.

For each deployment, record the source commit, retain the previous installed binary for rollback,
replace the binary atomically, and restart the service deliberately. Verify an SDK write, vector
search, full-text search, and filter through the proxy; restart, wait for readiness, and repeat the
queries before deleting the test namespace. Check the object-store prefix directly as well.
Namespace deletion can retain permanent WAL fences; do not remove these manually.

## Storage and writers

Implemented backends are local filesystem, S3-compatible object storage, and disposable in-memory
storage. AWS S3, Cloudflare R2, MinIO, and SeaweedFS can be addressed through the S3 backend. Native
GCS and Azure URLs are not implemented by the current configuration parser. Use a separate prefix
for each deployment, and give its credentials access only to the required bucket/prefix.

At startup, a writable node creates a scratch object, then tries to create it again with
`If-None-Match: *`. A correct conflict response enables `multi_writer` mode. If conditional writes
are unsupported or ignored, the node uses `single_writer` mode. Probe errors such as access denied
fail startup. The scratch object is cleaned up after a completed probe. You can force single-writer
mode with `--single-writer`.

In single-writer mode, exactly one writable process may use that prefix. It serializes creates and
maintains `_system/writer.json`, refreshed every five seconds. A fresh heartbeat blocks a second
writer; a failed heartbeat disables writes. Heartbeats become stale after 30 seconds, so a restart
may need to wait. This is best-effort detection, not a distributed lock: without conditional writes,
concurrent startup is not guaranteed safe. Do not run multiple writers on such a backend.

Start replicas with `--read-only`. They do not run a mutating probe or acquire a heartbeat, and they
reject storage mutations, including key creation/revocation. They can serve strong queries against
new writes in the shared prefix. Read-only status reports `read_only`, irrespective of the writer's
coordination mode. Provision the same super key on every node when enforcing auth.

A WAL slot is a permanent CAS fence, while document-bearing payloads can be reclaimed on namespace
deletion. Do not manually delete fences, renumber WAL objects, or apply lifecycle expiry to live
objects. Keep all deployment objects together when moving between buckets or prefixes. A backend
with working conditional writes uses the same logical format as a single-writer backend.

## Health, status, and metrics

`GET /` returns `{"status":"🐡"}`. It is a process-level healthcheck, not a live bucket probe, and
requires bearer auth when auth is enabled. `GET /_blowfish/v1/status` reports version, redacted storage
URL, `multi_writer` / `single_writer` / `read_only`, uptime, and names of namespaces with cached engine
handles on this node. Cached handles can include deleted or previously missing namespaces; this is
not the deployment's list of live namespaces. Status does not yet report a node registry or indexing
lag. Use `GET /v1/namespaces` for the live, scope-filtered namespace list.

See [metrics](metrics.md) for metric names, labels, and scrape configuration. Main-port `/metrics`
requires the super key or `metrics`. The optional `--metrics-listen` endpoint is unauthenticated.
Use a trusted interface or network policy for it. Metrics are per-node and reset on restart.
Monitor error rates, WAL conflicts, latency, scanned rows, and backend availability. A storage outage
can delay key revocations reaching other nodes; see [auth](auth.md).

The packaged container runs as UID/GID 10001. Its healthcheck reads the super key from
`BLOWFISH_ADMIN_KEY`, and expects internal port 8000. The Compose S3 profile is a real local
SeaweedFS setup with demo credentials, not a hardened public object-storage service. Keep its ports
private, and replace demo credentials before adapting it for shared use. SIGINT and SIGTERM allow
in-flight main-port HTTP requests to finish before shutdown.

## Backups and recovery

Back up the **entire deployment prefix**, including `_system/keys`, revocation markers, namespace
manifests, segments, WAL slots, and WAL payloads. Keep the configured super key in your secret backup
separately; it is never stored in the bucket. Enable provider versioning or snapshot retention where
available. Treat backups as sensitive because they contain documents and key hashes.

For a portable, consistent backup today, stop every writer and background mutator, wait for shutdown,
then copy the complete prefix or snapshot the backing volume. An arbitrary live recursive copy is
not guaranteed to be a consistent point in time, particularly during namespace deletion or GC.
Provider snapshots with documented atomicity can offer another path. No backup command or online
snapshot protocol is provided by the binary yet.

Restore into an empty prefix, copy every object preserving its relative key, configure the backend
and super key, and start a read-only node first. Check status, enumerate namespaces, and run a few
known queries before starting a writer. Do not restore revoked key records without their revocation
markers. Test your restore process periodically; successful object copies alone are not enough.

## Multi-node routing and tuning

For a store with working conditional creates, point nodes at the same bucket and prefix, and give
all nodes the same authentication configuration. Route by namespace using consistent hashing at the
load balancer to improve cache locality, with healthy-node failover. Correctness does not depend on
affinity. Monitor object-store errors, WAL conflicts, cache hit rates, and unindexed tail size. Tune
index thresholds and polling for the workload, and keep the retention period longer than the longest
reader and any storage replication delay. Smaller thresholds increase indexing and object-store work.
See [configuration](configuration.md) for the available controls.

Retention is a grace period for obsolete objects, not a point-in-time recovery interface. Recovering
an earlier state requires a coordinated, consistent snapshot or provider versioning that can restore
all objects to the same point. Retention alone cannot reconstruct an older manifest, and there is no
built-in point-in-time restore command. Test provider-specific recovery before relying on it.

The storage fault tests exercise a backend that ignores conditional creates: the probe selects
single-writer mode, and a second writer is refused. A separate local-store test verifies that a
read-only instance opens while the writer is active and rejects mutations. These tests do not prove
that the best-effort heartbeat fences simultaneously starting writers.

On 2026-11-14, `scripts/multinode.sh` passed all 111 recorded compatibility scenarios through
Caddy and three nodes sharing SeaweedFS. Its restart run verified all 67 acknowledged writes and
one winning conditional update; 23 interrupted writes were unacknowledged. This is a small local
smoke and stress run, not a durability or availability guarantee.
