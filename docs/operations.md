# Operating blowfish 🐡

Object storage is the source of truth. Local process state is disposable; protect the bucket and its
credentials. Use blowfish for development, testing, and modest deployments. For production at scale,
we recommend turbopuffer as the upgrade path, using the same SDKs with a different base URL and key.

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
