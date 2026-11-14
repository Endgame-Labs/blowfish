# Security 🐡

blowfish is intended for development, CI, and modest self-hosted deployments. Authentication
and namespace isolation are security boundaries; query execution is not a hard tenant resource
isolation boundary. Put an Internet-facing deployment behind TLS, request-rate controls, and
connection limits. Use a private bucket dedicated to the deployment, and protect the host,
configuration, cache directory, logs, and storage credentials.

## Threat model

Untrusted clients can send malformed requests, use compromised or restricted API keys, name
other namespaces, and attempt resource exhaustion. Namespace permissions must hold across
all API versions, copying, branching, operation polling, and key delegation.

The operator, configured super key, process environment, local filesystem owner, and object
storage backend are trusted. Someone who can modify the bucket can replace key records or
stored data; checksums detect corruption, but are not signatures. Read access to the bucket
reveals document data and key hashes. API keys are random bearer credentials; use TLS and
never place them in URLs. Storage URLs in the privileged status response remove userinfo,
query strings, and fragments; the remaining endpoint and path are operational information.

## Authentication and delegation

Set a nonempty `BLOWFISH_ADMIN_KEY` and `--require-auth` (or `require_auth = true` in the
configuration). Requiring authentication without a nonempty super key fails startup. Without
a super key and without that requirement, development mode disables all authorization and
prints a prominent warning. Development mode must not be exposed to untrusted clients.

The main listener authenticates every route, including `/`, unknown routes, and unsupported
methods. Supply `Authorization: Bearer <token>` with the exact scheme spelling. Generated
keys use `bf_<32 lowercase hex ID>_<64 lowercase hex secret>`. Token IDs and lengths are
validated before lookup. Secret hashes and the super-key hash use constant-time comparisons;
this does not make whole-request timing independent of key existence or storage state.

Generated key secrets are stored only as SHA-256 hashes and returned once. Revocation adds a
permanent marker. Successful refreshes propagate creation and revocation every five seconds
by default. After two refresh intervals without a successful refresh, generated credentials
fail closed with 503. The configured super key continues to work for recovery. Expiration
is checked on every authentication and authorization.

A `keys` principal can create only keys whose explicit roles, namespace prefixes, and expiry
are contained within its own authority. The same containment protects listing, inspection,
and revocation: inaccessible IDs return 404. Roles are checked literally for delegation;
`admin` does not implicitly grant the `keys` or `metrics` roles. Prefixes are literal string
prefixes, not directories, patterns, or regexes. An absent prefix list means all namespaces;
an empty list means none.

## Route authorization matrix

Every entry below also permits the configured super key. `R` means any of `read`, `write`,
or `admin`; `W` means `write` or `admin`; `A` means `admin`. Namespace-scoped actions check
the percent-decoded namespace that the handler receives. Implicit HEAD requests on GET
routes use the same permission as GET.

| Method and route | Action / role | Namespace and additional checks |
| --- | --- | --- |
| GET `/` | Any authenticated key | Status marker only; this is the health route |
| GET `/metrics` | `ReadMetrics` / `metrics` | Deployment-wide operational data |
| GET `/_blowfish/v1/status` | `ReadMetrics` or `ManageKeys` / `metrics` or `keys` | Deployment-wide operational data |
| POST `/_blowfish/v1/keys` | `ManageKeys` / `keys` | Delegation containment, including expiry |
| GET `/_blowfish/v1/keys` | `ManageKeys` / `keys` | Only manageable keys are returned |
| GET `/_blowfish/v1/keys/{id}` | `ManageKeys` / `keys` | Only manageable keys are visible |
| DELETE `/_blowfish/v1/keys/{id}` | `ManageKeys` / `keys` | Only manageable keys may be revoked |
| GET `/v1/namespaces` | `ListNamespaces` / `list` or `admin` | Results and pagination exclude inaccessible names |
| POST `/v2/namespaces/{ns}` | `Write` / W | Destination scope; Query permission on every copy or branch source |
| DELETE `/v2/namespaces/{ns}` | `DeleteNamespace` / A | Destination scope |
| POST `/v2/namespaces/{ns}/async` | `Write` / W | Destination scope; Query permission on every named source |
| GET `/v1/namespaces/{ns}/operations/{token}` | `Query` / R | Namespace scope and stored operation namespace equality; token must parse as UUID |
| POST `/v1/namespaces/{ns}` | `Write` / W | Legacy translation; copy and branch source checks still apply |
| GET `/v1/namespaces/{ns}` | `Query` / R | Legacy export; namespace scope |
| HEAD `/v1/namespaces/{ns}` | `ReadMetadata` / R | Legacy metadata headers; namespace scope |
| DELETE `/v1/namespaces/{ns}` | `DeleteNamespace` / A | Legacy delete; namespace scope |
| POST `/v1/namespaces/{ns}/query` | `Query` / R | Legacy query; namespace scope |
| POST `/v2/namespaces/{ns}/query` | `Query` / R | Namespace scope, including every subquery |
| GET `/v1/namespaces/{ns}/metadata` | `ReadMetadata` / R | Namespace scope |
| GET `/v2/namespaces/{ns}/metadata` | `ReadMetadata` / R | Namespace scope |
| PATCH `/v1/namespaces/{ns}/metadata` | `WriteSchema` / W | Namespace scope |
| GET `/v1/namespaces/{ns}/schema` | `ReadSchema` / R | Namespace scope |
| POST `/v1/namespaces/{ns}/schema` | `WriteSchema` / W | Namespace scope |
| GET `/v1/namespaces/{ns}/hint_cache_warm` | `WarmCache` / R | Namespace scope |
| POST `/v1/namespaces/{ns}/_debug/recall` | `Recall` / R | Namespace scope |
| POST `/v2/namespaces/{ns}/explain_query` | `ExplainQuery` / R | Namespace scope |
| Unmatched routes and unsupported methods | Authentication required | JSON 404 or 405; no handler execution |

The optional separate metrics listener is deliberately unauthenticated. Its bind address is
operator-controlled: bind it to loopback or a trusted monitoring network. `--require-auth`
protects the main listener; it does not add authentication to this separate listener.

Operation tokens are not credentials. Polling requires read permission on the operation's
stored destination namespace. Finished results expire after one hour; cleanup on polling is
best effort. Copy and branch source access is checked when the operation starts. A worker
crash can leave a running operation without a completion record; there is no durable job
resumption or timeout recovery yet.

## Object and filesystem paths

Namespaces accept only 1–128 ASCII bytes from `[A-Za-z0-9-_.]`, excluding `.` and `..`.
Slashes, backslashes, NULs, percent signs after URL decoding, and Unicode lookalikes are
rejected. A namespace named `_system` remains under `ns/_system/`, separate from the root
`_system/` key and operation stores. Double encoding cannot turn a permitted name into a path.

Attribute names and document IDs remain data. Segment filenames use numeric column ordinals
and generated IDs, not raw attribute names. IDs are limited to 64 bytes; attribute names to
128 bytes, with no leading `$`. Key object IDs are fixed lowercase hex; operation paths use
validated UUIDs. Disk-cache directory names and block filenames are hashes. Filesystem roots,
cache directories, storage prefixes, and endpoint configuration are trusted operator inputs;
use directories that untrusted local users cannot modify or replace with symlinks.

## Input and work limits

These limits apply before committing a write or producing a successful query response.
Errors use `{"status":"error","error":"..."}`. Semantic limits return 400, body-size
limits return 413, malformed request shapes return 422, and WAL backpressure returns 429.
Malformed JSON syntax currently returns 400. Engine/storage failures use generic client
messages; detailed backend errors belong in protected server logs.

| Resource | Enforced bound |
| --- | --- |
| HTTP body | 512 MiB of compressed input and 512 MiB after decompression |
| Namespace / attribute name / ID | 128 / 128 / 64 bytes |
| Attributes / vector columns per namespace | 1,024 / 8 |
| Dense vector dimensions | 1–10,752, including inferred vectors |
| Nonzero sparse vector dimensions | 4,096 per vector |
| Logical attribute value | 8 MiB; arrays count all elements |
| Filterable scalar / array element | 4 KiB |
| Logical document | 64 MiB, including the result of patches |
| Multi-query | 1–16 queries; no nested multi-query |
| Ranked result limit plus offset | At most 10,000; positive result limit |
| Aggregate functions / order keys / computed attributes | 8 / 8 / 256 |
| Returned aggregation groups | At most 10,000 |
| Full-text and text-filter query | 8,192 bytes |
| Highlight result fragments | 1–1,024 |
| Eventual staleness | 1–3,600 seconds when specified |
| Unindexed WAL | Default 2,000,000,000-byte write backpressure; configurable |
| Eventual unindexed scan | Default 128 MiB; configurable |

Logical sizes count UTF-8 strings, decoded byte values, scalar widths, vector element widths,
and attribute names. A filterable array's 4 KiB cap is per element; its 8 MiB cap is total.
Historical data is not automatically rewritten to satisfy newly enforced limits. Enabling
`filterable` on an existing column does not yet revalidate the sizes of previously stored
values. The computed-attribute name `id` is reserved to preserve document identity.

Additional blowfish safety bounds are intentionally stricter than the upstream wire grammar:

- Filter and rank-expression JSON trees allow depth 64 and 65,536 value nodes; ordinary HTTP
  JSON decoding also retains serde_json's recursion guard.
- Grouping permits at most 32 keys and 10,000 Cartesian combinations per document, even if
  the requested result limit is smaller. Both exhaustive and streaming evaluators check this.
- Fuzzy comparisons allow at most 65,536 input text bytes and 1,000,000 text-by-query byte
  products per document. Highlight alignments apply the same check per fragment.
- Highlighting permits at most 10,000 candidate fragments and a 128 MiB estimate of retained
  document copies before ranking. The output fragment limit alone does not bound this work.
- Regex compilation retains the regex crate's compiled-size and nesting limits; pattern
  strings are additionally subject to the 8,192-byte text-filter limit.

These are individual-request safeguards, not an aggregate memory budget. The hosted service's
weighted per-namespace query admission, 800 ms queue deadline, throughput quotas, and tenant
memory isolation are not implemented. Authenticated clients can still consume substantial CPU,
memory, object-store requests, and disk space. Bound concurrency and request rates externally.
Namespace handles, async-operation records, and durable branch pins also need operational
capacity planning; automatic reclamation is incomplete.

## Durability boundaries

Acknowledgment follows the WAL slot's conditional create. Large payloads and branch descriptors
are durable before that slot is published. UUID identity resolves ambiguous PUT responses.
Permanent slot records remain occupied after deletion and GC; inline data is redacted without
creating a missing-slot window. A losing writer catches up and revalidates before retrying.

Manifest publication checks the exact predecessor, epoch, and nondecreasing checkpoint.
Leases are advisory. GC inventories candidates before advancing the epoch and rereads branch
pins after that claim; branch pin publication checks the source manifest generation. Retention
must exceed query and writer staleness. Queries do not hold durable reader leases, so arbitrary
process suspension beyond retention is not supported. Branch pins currently favor retaining
referenced files over reclamation, including abandoned copy attempts.

The test suite exercises ambiguous storage failures, slot redaction, stale epochs, deletion,
recreation, compaction, branch ancestry, and source GC. Passing tests do not prove correctness
for every S3-compatible backend: retain the startup conditional-write probe and backups.

## Reporting vulnerabilities

Do not publish credentials, customer data, or an exploit against someone else's deployment.
Report privately through the repository's Security tab when private vulnerability reporting
is enabled:

https://github.com/dorkitude/blowfish/security/advisories/new

If private reporting is unavailable, contact the maintainer privately through their GitHub
profile to arrange a secure channel. Include the affected commit/version, a minimal reproducer
using synthetic data, the required roles, expected behavior, observed behavior, and impact.
There is no promised response-time SLA. Public issues are appropriate only after coordinated
disclosure or for non-sensitive hardening suggestions.
