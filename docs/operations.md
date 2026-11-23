# Operating blowfish

Object storage is the source of truth. Local process state is disposable; protect the bucket and its
credentials. Use blowfish for development, testing, and modest deployments. For production at scale,
I recommend turbopuffer as the upgrade path, using the same SDKs with a different base URL and key.

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

Set `storage.cache.disk_dir` under `/var/lib/blowfish`. blowfish links jemalloc and sizes its
memory-sensitive defaults from host RAM or the process's cgroup `memory.max` (`hw.memsize` on macOS; see [configuration](configuration.md)):
unindexed WAL backpressure (`limit / 8`, 128 MiB to 2 GB), decoded metadata cache (`limit / 8`,
512 MiB to 4 GiB), memory block cache (`limit / 4`, up to 8 GiB), segment row cap, and build
concurrency. jemalloc switches adaptively between a 1 s page decay during writes, segment builds,
compactions, and `recluster` and a 30 s decay while only queries run. On a 4 GB `c7gd.large`
with swap off, a 10M-row load plus `recluster` peaks at 2.3–2.9 GB RSS. The engine and the block cache read the same
limit, including a nested systemd scope's, so set explicit cache budgets only when the host is shared with other
services (for example, a 1 GiB memory cache and a 32 GiB disk cache). The process limit covers more than the byte cache: decoded indexes, document state,
requests, and background work also consume memory. Tune WAL backpressure and concurrency for the
workload; hitting `MemoryMax` can kill the process. On `429` backpressure and `503` transient
storage errors, blowfish emits `Retry-After`, `retry-after-ms`, and `x-should-retry` headers so
official SDKs back off automatically.

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

For each deployment, record the source commit, retain the previous installed binary for recovery,
replace the binary atomically, and restart the service deliberately. Verify an SDK write, vector
search, full-text search, and filter through the proxy; restart, wait for readiness, and repeat the
queries before deleting the test namespace. Check the object-store prefix directly as well.
Namespace deletion can retain permanent WAL fences; do not remove these manually.

## Upgrading to v0.1.14

Before upgrading, stop every writer and background maintenance process using the deployment prefix.
If rollback might be necessary, I recommend a consistent backup of the **entire bucket prefix before
starting the new binary** (see Backups and recovery). Retaining the old executable alone is insufficient.
Do not run old and new writers together during a format upgrade.

The binary automatically accepts legacy format-1 and format-2 buckets and raises the deployment marker to format 3
at writable startup. Namespace manifests move to format 3 on publication. Format 2 renamed the checkpoint
field while retaining its meaning; format 3 fences the single ANN object layout. Read-only startup checks compatibility without raising the marker.
The append-only `_system/format/<version>.json` markers live at the deployment root, including for
project-scoped namespaces. Their highest generation is the required bucket version; never delete or
lower a marker to force a downgrade.

Startup and garbage collection freshly check the bucket fence. Writes and manifest publication use
the shared cached result, refreshed every 60 seconds in the background and on manifest publication
conflicts or decode failures. A newer marker permanently disables mutations through that process,
even if the marker is later removed; steady-state writes add no marker requests. Every manifest decoder
also rejects unsupported versions, including the compact reader copy, so an older supported binary
cannot republish an unfamiliar manifest
or garbage-collect its objects. Format-2 manifests rename the required `indexed_through` wire field to
`checkpoint_wal`; released binaries through v0.1.13 fail to decode them instead of discarding new fields.
Those releases predate the bucket marker, so the marker alone cannot stop them from starting or touching
namespaces whose manifests have not yet been upgraded. Stop all older processes before upgrading.

v0.1.14 writes one `ann.<column>.bfa` object per vector attribute in each segment or slab. Its resident
metadata includes the directory entries and, for dense segment rows, the row-to-cluster map. Each leaf
stores a checksummed blob with its exact center, row set, and row checksums, followed by its quantized
codes. Leaves follow locality order; one region of full-precision vectors and a checksummed footer
end the object. Both filtered and unfiltered queries use this
layout. I keep the existing flat and paged slab directory formats unchanged.

The background indexer upgrades older `.bfv` indexes, including ones already using fast scan, to `.bfa`.
It copies fast-scan codes and vectors unchanged, converts older row-major codes when necessary, and does
not retrain clusters. Flat indexes also move into a single object. Documents, the WAL, text indexes, and
filter indexes retain their current formats. The redundant `.bff` and `.bfb` ANN fragments stop being written.

Each batch replaces the old `ann:`, `annfrag:`, and `annfrag2:` file entries with `annv2:` in one fenced
manifest publication. Old objects remain until normal garbage collection after the retention window.
The pass retries publication conflicts and skips a segment that compaction replaced. I can interrupt it
and resume later: published batches remain durable, and objects already in the new layout are skipped.
I bound memory by the largest index and its rewrite; older code encodings can also need an intermediate
conversion. Queries use segment directories when available and can scan document columns while a mixed
segment set has no new directory; this can temporarily increase query latency. Slab
batches retain the old directory until the final batch can replace it, including its paged copy.

To run the same upgrade now, or to see what is left, use `blowfish reindex`:

```bash
blowfish reindex --storage-url s3://BUCKET/PREFIX --dry-run NAMESPACE   # counts and bytes per layout, writes nothing
blowfish reindex --storage-url s3://BUCKET/PREFIX NAMESPACE
blowfish reindex --storage-url s3://BUCKET/PREFIX --all                 # every namespace in the bucket
```

It prints progress, and it is safe beside a running server on the same bucket. The report distinguishes
current single objects from legacy row-major, fast-scan, and other indexes.
All legacy indexes are rewritten, including flat ones. `recluster` also writes the single-object layout.

There is no supported in-place downgrade. To roll back, stop the upgraded processes, restore the complete
pre-upgrade backup into an empty prefix, and start the matching older binary there. Writes acknowledged
after that backup are absent from the restored copy. Test the restore and queries before resuming traffic.

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
coordination mode. Provision the same break-glass `BLOWFISH_ADMIN_KEY` on every node when enforcing auth.

A WAL slot is a permanent CAS fence, while document-bearing payloads can be reclaimed on namespace
deletion. Do not manually delete fences, renumber WAL objects, or apply lifecycle expiry to live
objects. Keep all deployment objects together when moving between buckets or prefixes. A backend
with working conditional writes uses the same logical format as a single-writer backend.

## Health, status, and metrics

`GET /` returns `200 OK` with a JSON `{"status": ...}` body. It is a process-level healthcheck, not a live bucket probe, and
requires bearer auth when auth is enabled. `GET /_blowfish/v1/status` reports version, redacted storage
URL, `multi_writer` / `single_writer` / `read_only`, uptime, and names of namespaces with cached engine
handles on this node. Cached handles can include deleted or previously missing namespaces; this is
not the deployment's list of live namespaces. Status does not yet report a node registry or indexing
lag. Use `GET /v1/namespaces` for the live, scope-filtered namespace list.

See [metrics](metrics.md) for metric names, labels, and scrape configuration. Main-port `/metrics`
requires an org superadmin key, the break-glass `BLOWFISH_ADMIN_KEY`, or a legacy key with the
`metrics` role. The optional `--metrics-listen` endpoint is unauthenticated.
Use a trusted interface or network policy for it. Metrics are per-node and reset on restart.
Monitor error rates, WAL conflicts, latency, scanned rows, allocator memory (`blowfish_alloc_bytes`),
hedged reads (`blowfish_object_store_hedges_total`), and backend availability. A storage outage
delays key revocations across nodes for up to two refresh intervals before generated keys fail
closed with `503`; see [auth](auth.md) and [security](security.md).

The packaged container runs as UID/GID 10001. Its healthcheck reads the break-glass key from
`BLOWFISH_ADMIN_KEY`, and expects internal port 8000. The Compose S3 profile is a real local
SeaweedFS setup with demo credentials, not a hardened public object-storage service. Keep its ports
private, and replace demo credentials before adapting it for shared use. SIGINT and SIGTERM allow
in-flight main-port HTTP requests to finish before shutdown.

If blowfish dies on a fatal signal (SIGSEGV, SIGBUS, SIGILL, or SIGFPE), it first writes a line to stderr naming the
signal, the fault address, and the thread, followed by the stack frames. The exit status is unchanged (139 for SIGSEGV).
On Linux, frames inside blowfish print as offsets: resolve them with `addr2line -f -C -i -e blowfish <offset>` against
the same binary (release builds carry debug info). Please include this output when reporting a crash.

## Large namespaces: recluster, paged directory, and compact manifest copy

For namespaces with millions of vectors, three maintenance helpers in `crates/bf-engine/examples/`
reduce cold ANN latency without rewriting documents:

- **Namespace-wide spatial slabs (`examples/recluster.rs`):** groups all live vectors in a namespace
  into spatial slabs (about 200 rows per cluster) so unfiltered cold ANN queries probe far fewer
  rows and clusters. At 100,000+ clusters, `recluster` automatically writes a paged copy of the slab
  directory beside the flat directory. Note: slabs serve unfiltered ANN only, and new segments
  flushed after `recluster` cause queries to fall back to the per-segment path until the next
  manual `recluster`.
  ```bash
  cargo run --release -p bf-engine --example recluster -- storage.json NAMESPACE ATTRIBUTE SCRATCH_DIR
  ```
- **Paged slab directory for older reclustered namespaces (`examples/repage.rs`):** adds a paged slab
  directory copy to a namespace that was reclustered before paging existed, without rebuilding slabs.
  ```bash
  cargo run --release -p bf-engine --example repage -- storage.json NAMESPACE ATTRIBUTE
  ```
- **Compact manifest copy for older large namespaces (`examples/readercopy.rs`):** rewrites the
  fixed-key `manifest-latest` object without per-segment text headers (375 KB instead of 3.4 MB at
  100M rows) for namespaces published before compact copies existed. Safe to run alongside live
  servers; full manifest slots remain unchanged.
  ```bash
  cargo run --release -p bf-engine --example readercopy -- storage.json NAMESPACE
  ```

See [configuration](configuration.md) for the environment variables controlling paging and compact
manifest copies.

## Backups and recovery

Back up the **entire deployment prefix**, including `_system/keys`, `_system/projects`, revocation markers, namespace
manifests, segments, WAL slots, and WAL payloads. Keep the configured break-glass `BLOWFISH_ADMIN_KEY` in your secret backup
separately; it is never stored in the bucket. Enable provider versioning or snapshot retention where
available. Treat backups as sensitive because they contain documents and key hashes.

For a portable, consistent backup today, stop every writer and background mutator, wait for shutdown,
then copy the complete prefix or snapshot the backing volume. An arbitrary live recursive copy is
not guaranteed to be a consistent point in time, particularly during namespace deletion or GC.
Provider snapshots with documented atomicity can offer another path. No backup command or online
snapshot protocol is provided by the binary yet.

Restore into an empty prefix, copy every object preserving its relative key, configure the backend
and break-glass key, and start a read-only node first. Check status, enumerate namespaces, and run a few
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

On 2026-11-14, `scripts/multinode.sh` passed all 111 recorded compatibility scenarios (the suite has
since grown to 121 recorded scenarios plus SDK retry checks) through Caddy and three nodes sharing SeaweedFS. Its restart run verified all 67 acknowledged writes and
one winning conditional update; 23 interrupted writes were unacknowledged. This is a small local
smoke and stress run, not a durability or availability guarantee.
