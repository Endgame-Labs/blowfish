# Operating blowfish

Object storage is the source of truth. Local process state is disposable; protect the bucket and its
credentials. Use blowfish for development, testing, and modest deployments. For production at scale,
I recommend turbopuffer as the upgrade path, using the same SDKs with a different base URL and key.

## Local filesystem durability

Writable `file://` stores always sync file data and the directory entries that publish it
before acknowledging a write. There is no switch to disable durability. This applies to WAL
payloads and commit slots, manifest versions and their latest-copy hints, segments, and mutable
system objects. S3-compatible stores keep their existing behavior. Read-only opens and mapped
reads do not write or sync files.

The pinned `object_store` 0.14.2 local backend defaults `with_fsync` to false. Blowfish now
always enables it. The backend's publication paths behave as follows:

| Operation | With fsync disabled | With fsync enabled |
|---|---|---|
| Overwrite PUT | Stage, close, and rename | Sync staged file, close, rename, and sync destination directory |
| Create-only PUT | Stage, close, and hard-link without replacing | Sync staged file, close, hard-link, and sync destination directory |
| Multipart completion | Close and rename completed staging file | Sync completed file, close, rename, and sync destination directory; no sync per part |
| Rename existing object | Rename, or copy-and-delete for create-only mode | Sync affected directories; the source file was synced when originally published |

Create-only staging-file cleanup may be lost in a crash, leaving an ignored temporary name;
the published object is already durable. The backend also syncs newly created directory
chains. Blowfish separately persists the configured root and its ancestors, and prepares
each newly used parent chain before writing into it. The latter closes a race where another
writer could see a new directory before its creator had synced its parent. Completed directory
barriers are remembered across scoped handles, so existing directories need no repeated
ancestor syncs.

A readable object is not sufficient evidence of a durable write after an I/O error: a link or
rename can succeed before its directory fsync fails. On an identical create retry or redaction
recovery, blowfish syncs the visible file and its parent before returning success. Persistent
file or directory fsync errors therefore fail the write even if its body is readable.

WAL staging awaits payload durability before publishing its commit slot. Group commit still
syncs the group's objects, rather than each document or client request. Segment uploads finish
before the manifest that references them is published. The fixed-key latest-manifest copy is
best effort; its failure is safe because the immutable manifest slot is already durable and
readers validate the copy. These ordered publication barriers cannot be combined without
changing the storage protocol. Standalone garbage-collection unlinks remain best effort;
logical deletes are acknowledged through durable WAL records.

Use a filesystem that supports directory fsync, as on Linux and macOS, and storage that honors
flush requests. An unsupported or failed sync returns an error. External changes to store files
or directories are unsupported. Disposable disk caches retain their existing write policy.
Backends supplied through the Rust `from_backend` constructors remain the caller's responsibility.

On Linux, install `strace`, and run `cargo test -p bf-storage --test local_durability`. The tests
check file sync before publication and directory sync before acknowledgment for creates,
overwrites, multipart writes, and redactions. They also cover newly created ancestors,
read-only reopening, and injected file and directory fsync failures after publication. These
checks establish syscall ordering, not a physical power-cut guarantee from the device.

### Measured local write cost

Two matched baseline/durable pairs ran serially on one dedicated on-demand c7gd.2xlarge
(eight cores, 16 GiB RAM), using its local NVMe with XFS. Both builds use native stable Rust,
Clang 19 compilation,
Clang 22 linking, and the unchanged release profile, without native CPU flags. The baseline
is `478de32`; the durable storage implementation is `27dc1c1`. Storage source hashes on the
measurement host matched the tested source.

The prepared-body HTTP loader (`crates/bf-wal/benches/http_ingest.py`), with its existing timing
wrapper, loaded all 2,681,468 public BEIR NQ vectors from the shared fixture: 1,024 dimensions,
f16 storage, cosine distance, 5,000 rows per full batch, 537 schema-first writes, and four
clients. Servers use four Tokio workers, four Rayon workers, a 4 GiB memory cache, no disk
cache, and default indexing, compaction, reclustering, and backpressure. Both server and
loader run at nice 19. Every run starts with a fresh store and dropped OS caches; builds,
preparation, and tests do not overlap timed loads. Throughput includes body reads and all
acknowledgments; p50/p90 use the original loader's complete request intervals and lower order
statistics. Retry handling is unchanged. Every measured run had zero retries.

| Pair | Local writes | Rows/s | Write p50, ms | Write p90, ms | WAL groups |
|---|---|---:|---:|---:|---:|
| 1 | Baseline, fsync off | 14,761 | 1,222.9 | 2,136.8 | 390 |
| 1 | Always durable | 14,640 | 1,241.3 | 2,174.5 | 392 |
| 2 | Baseline, fsync off | 17,707 | 876.0 | 2,074.9 | 451 |
| 2 | Always durable | 17,431 | 897.1 | 2,143.9 | 462 |

Durability reduced throughput by 0.82% and 1.56% in the two pairs, and increased write p90 by
1.76% and 3.32%. Absolute throughput varied substantially as concurrent WAL grouping changed;
the table includes group counts to make that visible. These two pairs describe this workload
on this device, not a general fsync cost or a portable performance guarantee.

A separate check used 2,000 sequential one-row writes per fresh store, each with a numeric ID
and one short string attribute, without vectors. Default background indexing stayed enabled.

| Pair | Local writes | Write p50, ms | Write p90, ms |
|---|---|---:|---:|
| 1 | Baseline, fsync off | 0.671 | 0.828 |
| 1 | Always durable | 0.811 | 0.973 |
| 2 | Baseline, fsync off | 0.647 | 0.829 |
| 2 | Always durable | 0.809 | 0.998 |

The small-write cost repeated: p50 increased by 0.140 and 0.162 ms, and p90 by 0.145 and
0.169 ms. After every small-write run, the server was killed with SIGKILL and reopened
read-only; an exact count recovered all 2,000 acknowledged rows in all four runs. Together
with the syscall and replay tests above, this checks software ordering and recovery; it does
not simulate a device power cut. Each run's store was deleted, and the temporary instance,
security group, and SSH key pair were removed after measurements.

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
unindexed WAL backpressure (`limit / 8`, 128 MiB to 2 GB), decoded metadata cache (`limit / 4`,
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

**Local ANN staging.** With an S3-compatible backend and a disk cache directory, blowfish copies each recluster tier's routing directory and vector codes (not the full vectors, which stay in object storage) to local disk after the tier is first queried, and serves later cold queries from that copy. Files go in `<storage.cache.disk_dir>/ann-staging-v1/`, one locked directory per running process; directories left by a stopped process are removed at startup. The budget is the smaller of 20 GiB and a quarter of the free space on that filesystem when the process starts; it is separate from `storage.cache.disk_bytes`. Files are keyed by tier generation and checksummed, so a new tier replaces the old files and a damaged file is rebuilt. If staging cannot finish (budget, disk error, or a failed copy), queries keep reading from object storage. Staging adds about 1.6 GB of disk for 10M vectors and 16.3 GB for 100M at 1,024 dimensions, publishes in 13 s and 144 s respectively, and cuts cold p50 from 325 to 220 ms at 10M and from 715 to 400 ms at 100M (in-region S3, same results). The local filesystem backend skips staging, since its objects are already memory-mapped. There is no setting: delete the directory to reclaim the space.

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

## Upgrading to v0.1.16

v0.1.16 raises the bucket format from generation 3 (v0.1.15) to generation 4.
Stop every older writer, back up the entire deployment prefix, and start v0.1.16.
Writable startup writes `_system/format/4.json`. From then on, v0.1.15 refuses
both local and S3 storage with this exact startup error:

```text
Error: BucketFormat { required: 4, supported: 3 }
```

There is no in-place downgrade; restore the full backup to run the older binary.
Read-only startup checks compatibility without advancing the format marker. Do
not run old and new writers together during the upgrade.

Everything else upgrades in the background while queries keep using indexes:

- **Manifests.** The next publication writes format 4, adding row lineage so
  compaction can merge segments covered by recluster tiers without rebuilding
  those tiers.
- **Text.** Format-4, format-5, and format-6 indexes repack directly into format 7,
  combining compressed phrase positions with unpadded short posting lists. Query
  results and phrase positions are preserved.
- **ANN, documents, and tiers.** Current v0.1.15 `.bfa` objects, document-v2 objects,
  and format-1 recluster tiers need no format upgrade. Older ANN and document
  layouts continue upgrading automatically. Existing large tiers acquire the new
  leaf population on their next normal recluster; startup does not rebuild them
  to apply it.

I tested independent v0.1.15 loads on local disk and S3, each with the first
200,000 BEIR NQ passages, 1,024-dimensional f16 vectors, text, six segments, and
one recluster tier of 32 slabs. The local tier covered all 200K rows; the S3 tier
covered 135K, with the remaining 65K in indexed segments. Both binaries ran on
the same 16-vCPU, 32-GB machine. Times below are seconds from v0.1.16 candidate process launch,
including startup, under continuous four-client validation traffic:

| Observed event | Local disk | S3 |
| --- | ---: | ---: |
| Generation-4 marker | 0.14 | 0.30 |
| First format-4 manifest | 29.65 | 25.34 |
| Every text object at format 7 | 30.24 | 27.11 |
| Zero legacy counts verified by dry-run | 30.38 | 28.63 |

`blowfish reindex --dry-run --storage-url STORAGE_URL NAMESPACE` reports legacy
ANN and document counts, all zero in both runs. It does not count or force text
upgrades. I separately checked every `fts:*` file's full-manifest text footer
(version 7), manifest format 4, and current tier format 1 before declaring the
upgrade complete. The existing ANN, document, and tier objects needed no upgrade.

Every check ran the same 40 ANN queries against exact top-10 ground truth over
that 200K-row corpus and 40 BM25 queries against that backend's v0.1.15 ordered
results. Recall@10 stayed at 0.9725 on local disk and 0.9775 on S3 on every
check; the independently built baseline indexes explain the difference. BM25
matched 40/40 on every check. Across 35,520 local and 7,120 S3 candidate
queries, there were zero errors and zero nonzero `exhaustive_search_count`
responses. Restarting the older binary on both backends exited with status 1 and
the error above.

Stored bytes below use decimal MB. Live bytes sum unique file references in the
latest full manifest; the entire prefix also includes the WAL, retained obsolete
objects, manifests, and unpublished build objects.

| Backend | Live referenced MB, before → after | Text MB, before → after | Entire prefix MB, before → after |
| --- | ---: | ---: | ---: |
| Local disk | 1,212.61 → 1,025.91 | 261.24 → 74.54 | 2,109.52 → 2,184.23 |
| S3 | 1,068.16 → 881.14 | 261.78 → 74.76 | 1,982.02 → 2,056.95 |

Text is about 3.50x smaller in these NQ upgrades. The entire prefix initially
grows because old text objects remain until the retention window expires; these
are pre-GC totals, not a claim of immediate bucket-space recovery. See the
[upgrade measurement details](benchmarks.md#v0116-upgrade-validation).
Background-job and shutdown behavior is described in the v0.1.15 section below.

## Upgrading to v0.1.15

v0.1.15 raises the bucket format from generation 2 (v0.1.14) to generation 3. Follow the steps in Bucket
format 3 below: stop every older writer, back up the whole prefix, then start v0.1.15. At writable startup it
writes the `_system/format/3.json` marker. From then on, v0.1.14 and older refuse the bucket
(`BucketFormat { required: 3, supported: 2 }`). There is no downgrade except restoring the backup.

Everything else upgrades in the background while queries keep using their indexes:

- **ANN objects.** Legacy `.bfv` indexes and their fragments are rewritten as single `.bfa` objects (see
  below). Slab sets made by the old manual recluster tool become a format-1 tier in the same publications.
- **Documents.** `docs.bfc` is rewritten as version 2, and `ids.bfi` loses its Bloom filter.
- **Text.** Objects older than format 5 are repacked. v0.1.14 already writes format 5.
- **Recluster.** A tier older than the binary's format is rebuilt as a base. Otherwise the usual triggers
  apply (see Large namespaces).

On my test, a v0.1.14 bucket with 200,000 rows of 1,024 dimensions and text, in 5 segments and 32 slabs,
reached manifest format 3 within 2 seconds of startup. All its ANN objects were converted 3.6 seconds after
startup. Recall, BM25 results, and errors were unchanged throughout, and every query stayed indexed. Check
what is left with `blowfish reindex --dry-run`. Every legacy count reads 0 when the upgrade is done.

**Background jobs.** Each writable node runs:

- the indexer, which flushes the WAL into segments and runs the upgrades above;
- compaction, which merges small segments;
- the automatic recluster, which builds tiers;
- garbage collection, which deletes unreferenced objects after the retention window;
- the recall sampler, which compares a sampled ANN query with an exhaustive search.

Their metrics are `blowfish_recluster_*`, `blowfish_ann_recall`, and the indexing and GC series in
[metrics](metrics.md). The recall sampler runs at most once per `BF_RECALL_SAMPLE_INTERVAL_MS` (60 seconds).
It skips the first interval after startup, so a restart doesn't reread every row of a large namespace. After
a comparison that took `t`, it waits at least `50 × t`, so it never uses more than about 2% of the time.

**Shutdown.** On SIGTERM or SIGINT, I stop accepting connections and tell every background job to stop.
In-flight requests and jobs get 5 seconds together. A job stops at its next await, or between cells of a
recluster's CPU steps. The process then exits, well within systemd's default 90-second stop timeout. The
log line `stopped background_jobs_stopped=true` means every job stopped in time. Stopping mid-job is
crash-safe:

- A recluster build leaves its build record and objects. GC keeps them for 20 minutes, then deletes them.
  The next start builds again.
- An interrupted flush, compaction, or upgrade batch publishes nothing. Its uploaded objects are orphans
  that GC deletes after the retention window.
- Already-published batches stay durable, and the next start resumes from them.

## Bucket format 3: what the upgrade does

Before upgrading, stop every writer and background maintenance process using the deployment prefix.
If rollback might be necessary, I recommend a consistent backup of the **entire bucket prefix before
starting the new binary** (see Backups and recovery). Retaining the old executable alone is insufficient.
Do not run old and new writers together during a format upgrade.

v0.1.15 automatically accepts legacy format-1 and format-2 buckets and raises the deployment marker to format 3
at writable startup. Namespace manifests move to format 3 on publication. Format 2 renamed the checkpoint
field while retaining its meaning; format 3 fences the single ANN object layout and document-v2 objects. Read-only startup checks compatibility without raising the marker.
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

v0.1.15 writes one `ann.<column>.bfa` object per vector attribute in each segment or slab. Its resident
metadata includes the directory entries and, for dense segment rows, the row-to-cluster map. Each leaf
stores a checksummed blob with its exact center, row set, and row checksums, followed by its quantized
codes. Leaves follow locality order; one region of full-precision vectors and a checksummed footer
end the object. Both filtered and unfiltered queries use this
layout. I keep the existing flat and paged slab directory formats unchanged.

The background indexer upgrades older `.bfv` indexes, including ones already using fast scan, to `.bfa`.
It copies fast-scan codes and vectors unchanged, converts older row-major codes when necessary, and does
not retrain clusters. Flat indexes also move into a single object. The redundant `.bff` and `.bfb` ANN
fragments stop being written; the WAL and filter indexes keep their formats.

The ordinary indexer upgrades text objects to format 5 in batches of at most 32 segments per fenced
publication. I repack format-4 postings directly, preserving their compressed payloads, phrase positions,
document lengths, and exact scores. A missing manifest header on a current object only needs a copy
and header extraction. Formats 1–3 are rebuilt from the immutable text column. Each segment's fields
run sequentially; independent segments run concurrently, limited by available CPUs and an estimated
working-set budget of one eighth of process memory, capped at 1 GiB. An oversized segment runs alone.
I bypass cache admission for source text and document reads, and backfill its compact `text_keys` map
in the same batch. Completed batches survive interruption, and another pass skips current objects.
This uses the existing text format 5, manifest schema, and generation-3 bucket fence.

After the ANN upgrade, I rewrite `docs.bfc` as version 2, one segment per fenced publication. Its directory
records dense-vector dimensions and stored precision, and its payload contains only other attributes.
String and string-array columns use zstd level 3 per 256-row block, with a checksum over the compressed
bytes. I batch up to 64 source column ranges during a rewrite, while bypassing cache admission and
checking every source block. Projection, filtered/exhaustive queries, and compaction obtain dense vectors from the segment's
ANN object at the declared f16, f32, or i8 precision. The returned vector encoding and values are unchanged.
Sparse and multi-vector attributes retain their existing JSON representation. I retain segment ANN objects
after reclustering; retiring them requires the separate tier-lineage work.

I also rewrite `ids.bfi` without its redundant Bloom filter in the document publication. The small,
24-bit-per-ID `idfilter.bff` still screens existence checks before the full ID list is opened. Each document
publication retires only the replaced document and ID objects; it preserves row ordinals, live masks,
ANN directories, tiers, and WAL checkpoints. Old and current document objects can coexist during the
rewrite. A restart resumes remaining segments, and a second completed run writes nothing. A failed or
ambiguous publication leaves uploaded objects for ordinary GC, so an uncertain response cannot delete
an object that was successfully published.

Each batch replaces the old `ann:`, `annfrag:`, and `annfrag2:` file entries with `annv2:` in one fenced
manifest publication. Old objects remain until normal garbage collection after the retention window.
The pass retries publication conflicts and skips a segment that compaction replaced. I can interrupt it
and resume later: published batches remain durable, and objects already in the new layout are skipped.
I bound memory by the largest index and its rewrite; older code encodings can also need an intermediate
conversion. Queries continue using ANN throughout the rewrite. Each publication builds a directory
covering both old and current objects, including mixed slab sets and their paged copies. Before the first
publication, upgrade-only readers use the existing directory or assemble resident leaf metadata without
writing storage. I keep the same global probe selection and rerank across the conversion; upgrading an
index never requires a document-vector scan.

To run the same upgrade now, or to see what is left, use `blowfish reindex`:

```bash
blowfish reindex --storage-url s3://BUCKET/PREFIX --dry-run NAMESPACE   # counts and bytes per layout, writes nothing
blowfish reindex --storage-url s3://BUCKET/PREFIX NAMESPACE
blowfish reindex --storage-url s3://BUCKET/PREFIX --all                 # every namespace in the bucket
```

It prints progress, and I can run it beside a server that supports the bucket’s current format (generation 4 for
v0.1.16). The report distinguishes
current single objects from legacy row-major, fast-scan, and other indexes. It also reports legacy
document counts and bytes, converted document/ID pairs, and total ANN, document, and ID rewrite bytes.
Directory and manifest writes are extra. `--dry-run` surveys document metadata without writing anything.
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
private, and replace demo credentials before adapting it for shared use. On SIGINT or SIGTERM, I stop
accepting connections and give in-flight requests and background jobs 5 seconds in total to finish (see
Upgrading to v0.1.15).

If blowfish dies on a fatal signal (SIGSEGV, SIGBUS, SIGILL, or SIGFPE), it first writes a line to stderr naming the
signal, the fault address, and the thread, followed by the stack frames. The exit status is unchanged (139 for SIGSEGV).
On Linux, frames inside blowfish print as offsets: resolve them with `addr2line -f -C -i -e blowfish <offset>` against
the same binary (release builds carry debug info). Please include this output when reporting a crash.

## Large namespaces: automatic recluster and compact manifest copy

**Automatic recluster.** A namespace as loaded has one clustering per segment, so a query's neighbors are spread over
every segment. The indexer rebuilds such a namespace's vectors into one namespace-wide layout, a tier of spatial slabs
with leaves of about 200 rows, in the background. Queries then scan far fewer rows for the same recall. It is always
on, and there is no setting to turn it off. The design is `docs/design/auto-recluster.md`.

- **Tiers.** A vector attribute has at most two tiers: a **base** over most rows, and a **delta** over rows written
  since, partitioned by the base's coarse cells. Rows in neither are in uncovered segments, which keep their own
  indexes.
- **When it runs.** After each publication, the indexer checks the namespace's layout from manifest numbers alone.
  - A namespace with at least 100,000 rows in two or more segments gets its first base once a default query would
    scan 1.25 times fewer rows with it.
  - With a base, the rows outside the tiers get a delta (rebuilt with the current delta's rows) once they reach 4% of
    the base's rows and a quarter of the delta's.
  - Once the delta and the rows outside it reach half the base, everything merges into a new base, which reuses the
    old base's coarse cells unless the rows have grown four times since they were trained.
  - Once deletes and updates mask a fifth of the tiers' rows, they are rebuilt as a new base without them, after any
    delta that is due (a delta takes seconds; a new base takes as long as the first one).
  - A tier whose format is older than the binary's is rebuilt as a base.
  - A build waits while compaction has merges it would run now (a long write tail defers compaction, and then the
    build doesn't wait for it), and for five minutes after a bulk load's flush (unless the
    load has outgrown what the tiers cover). A bulk load's flush is one of at least a quarter of the bulk flush rows,
    written at 2,000 rows a second or more; a steady stream of writes slower than that gets deltas as it goes. The
    time of that flush is kept in the manifest, so a restart doesn't start a build in the middle of a load.
- **What queries see.** Unfiltered ANN queries use one plan over the tiers and the segments written since, so new
  writes never send queries back to the slower per-segment path. The base probes as it would alone, and the delta
  probes and reranks `sqrt(its share of the rows)` of what it would alone, the same rule as for rows outside the
  tiers. The two agree on one rerank bound and hydrate only the global top k, so a delta costs about its share of
  rows, not a second search. Deletes and updates
  of covered rows are masked. Filtered ANN, BM25, and everything else use the segments as before. Compaction leaves
  covered segments alone.
- **What it costs.** A build uses half the cores, and waits between slabs whenever the indexer flushes or
  compacts. It reserves memory from the engine's budget (what is left after the block cache, the decoded cache,
  and the unindexed limit), at 12 KB per row of a slab, and defers when fewer than 50,000 rows fit. Its spill file
  (every row's vector at stored precision: about 2 KB a row at 1,024 dimensions in f16) goes in the disk cache's
  directory, or the system's temporary directory without one, and the build defers unless 1.1 times its size is
  free. Deferrals are counted in `blowfish_recluster_deferred_total`.
- **Safety.** A build reads a snapshot of the manifest and never stops indexing. It writes a create-only build
  record (`ns/<ns>/indexer/build-*.json`), refreshed every 10 minutes, and GC keeps the build's objects while a
  record is less than 20 minutes old. The finished tier is installed by the indexer's next publication, under its
  normal fence, and only if every segment it covers is still there (and, for a delta, its base is still installed).
  The build's records are deleted once the tier is installed or dropped. A crashed build leaves orphaned objects that GC
  deletes after the retention window. Only the node holding the namespace's indexer lease builds, and read-only
  deployments never do.
- **Logs and metrics.** Each decision is logged (`recluster: starting a build`, `recluster: deferred`, `recluster:
  tier installed`) with the numbers behind it. The `blowfish_recluster_*` metrics are listed in
  [metrics](metrics.md).

A namespace reclustered by hand with an older release is read as a tier and rewritten in the new manifest format at
its next publication. The manual `examples/recluster.rs` and `examples/repage.rs` tools are gone.

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
