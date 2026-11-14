# Authentication and API keys 🐡

Set `BLOWFISH_ADMIN_KEY` to a long, random secret. This super key can perform every action, and it is
never stored in the bucket. Every main-port route, including `/`, requires a bearer key when it is
configured. Set `--require-auth` or `require_auth = true` to refuse startup without a nonempty super
key. Without a super key, auth is disabled for **all** routes, and startup logs a loud warning. That
mode is for local development only; keys stored in the bucket do not implicitly enable auth.

Clients send `Authorization: Bearer <token>`. Missing, invalid, expired, and revoked tokens receive
401; an authenticated key without the necessary role or namespace scope receives 403. Both use the
turbopuffer envelope `{"status":"error","error":"..."}`.

| Role | Permissions |
| --- | --- |
| `admin` | All namespace operations, including listing and namespace deletion |
| `write` | Writes, schema updates, queries, metadata, schema reads, cache warming, recall, and explain |
| `read` | Queries, metadata, schema reads, cache warming, recall, and explain |
| `list` | Namespace listing, filtered to this key's prefixes |
| `metrics` | Main-port `/metrics` and `/_blowfish/v1/status` |
| `keys` | Create, list, inspect, and revoke API keys, and read status |

Roles combine. Namespace `admin` does not imply `keys` or `metrics`; grant those explicitly.
`namespace_prefixes` omitted or null means unrestricted; `[]` means no namespace access. Prefixes
are literal, case-sensitive strings. They apply to every namespace route, including both the source
and destination of a copy. Listing returns only visible namespaces, with normal pagination. Metrics
and key management are deployment-wide permissions, not namespace resources; a `keys` holder can
inspect and revoke any key, so grant that role only to trusted administrators.

Keys with `keys` may create children only with a subset of their own explicit roles, narrower or
equal namespace prefixes, and an expiry no later than their own. A super key has no such restriction.
A parent with an expiry cannot mint a child without an expiry.

## CLI

With `BLOWFISH_ADMIN_KEY` set in your shell:

```bash
blowfish keys create --name application --role write --role read --prefix app- --expires 30d
blowfish keys list
blowfish keys list --json
blowfish keys revoke KEY_ID
# Remote server; flags also work before the subcommand:
blowfish keys --url https://search.example.com list
```

Creation displays the plaintext token **once**. Save it in your application's secret store. Tables
show key ID, name, roles, prefixes, and expiry. JSON output matches the admin API response. Prefer an
environment variable over `--admin-key` to avoid placing keys in shell history.

## Admin API

All routes below use the super key or `keys`, except status, which also accepts `metrics`.

| Method and path | Request / response |
| --- | --- |
| `POST /_blowfish/v1/keys` | Create body below; 201 `{"key":{...},"token":"bf_..."}` |
| `GET /_blowfish/v1/keys` | 200 `{"keys":[...]}`; revoked keys omitted |
| `GET /_blowfish/v1/keys/{id}` | 200 public metadata, or 404 |
| `DELETE /_blowfish/v1/keys/{id}` | Revoke; 200 `{"status":"OK"}`, or 404 if absent/already revoked |
| `GET /_blowfish/v1/status` | Version, redacted storage URL, storage mode, uptime seconds, and cached namespace names |

Create body:

```json
{
  "name": "application",
  "roles": ["write", "read"],
  "namespace_prefixes": ["app-"],
  "expires_at": "2099-01-01T00:00:00Z"
}
```

Expiry is optional, and must be in the future. Metadata contains `id`, `name`, `roles`,
`namespace_prefixes`, `created_at`, `expires_at`, `created_by`, and optional `last_used_at`.
`last_used_at` reflects usage observed by this node, and is not a deployment-wide audit log.
No list or inspection response includes a secret or hash. Status is operational information across
all namespaces cached on this node, so it is intentionally restricted to operational roles.

Keys live under `<storage-prefix>/_system/keys/<id>.json`, with SHA-256 hashes of random secrets.
Tokens are `bf_<id>_<secret>`. Revocation creates `<id>.revoked`. Nodes refresh their key sets every
five seconds; changes apply immediately on the node handling them, and normally within five seconds
elsewhere. A failed refresh keeps the last successfully loaded set, so a storage outage can delay
revocation propagation. Read-only replicas can authenticate and inspect keys, but cannot create or
revoke them. Rotate the super key through configuration, and restart nodes to apply the change.
