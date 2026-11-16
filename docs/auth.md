# Authentication, projects, and API keys 🐡

A blowfish deployment is one **org**. The org holds **projects**. Each project has its own namespaces and
its own API keys, and a request can only ever see its own project. Humans manage all of this in the admin
webapp (see [`webapp/`](../webapp/README.md)), and programs do the same through the admin API, which is
described in full by [`api/admin.openapi.yaml`](api/admin.openapi.yaml).

Set `BLOWFISH_ADMIN_KEY` to a long, random secret. Every main-port route, including `/`, then requires a
bearer key. Set `--require-auth` or `require_auth = true` to refuse startup without a nonempty admin key.
Without one, auth is disabled for **all** routes, and startup logs a loud warning; that mode is for local
development only.

Clients send `Authorization: Bearer <token>`. Missing, invalid, expired, and revoked tokens get 401. A key
without the necessary kind or role gets 403. A project that isn't yours gets 404. Errors use the
turbopuffer envelope `{"status":"error","error":"..."}`.

## The model

| Thing | What it is |
| --- | --- |
| Org | The deployment. Exactly one. |
| Org superadmin key | Controls every project and every key, including other superadmin keys |
| Project | `{id, name, created_at, created_by}`. The `id` is a slug of `a-z`, `0-9`, and `-`, 1 to 40 characters |
| Project key | Belongs to one project, and is an `admin`, `read_write`, or `read` key |

| Kind | Namespaces in its project | Keys |
| --- | --- | --- |
| Org superadmin | Everything, in every project | Create, list, and revoke any key, including project `admin` and superadmin keys. Create, rename, and delete projects |
| Project `admin` | Everything, including listing and deleting namespaces | Create, list, and revoke `read_write` and `read` keys in its own project. It **can't** mint, see, or revoke `admin` keys |
| Project `read_write` | Writes, schema updates, queries, metadata, cache warming, recall, and explain | None |
| Project `read` | Queries, metadata, schema reads, cache warming, recall, and explain | None |

Tokens look like `bf_<id>_<secret>`. Only a SHA-256 hash of the secret is stored, and the token is shown
**once**, when it is created. Expiry is optional. Revocation applies immediately on the node that handles it,
and within about 5 seconds on every other node.

### The break-glass key

`BLOWFISH_ADMIN_KEY` stays as a deployment **break-glass** key. It has superadmin powers and is never stored
in the bucket. Use it to mint the first org superadmin key, and to recover if every superadmin key is lost.
Programs and the webapp should use a minted superadmin key, not this one.

## Namespace isolation

- Projects `a` and `b` can each have a namespace called `docs`, and the two are unrelated.
- A key only sees its own project's namespaces. Listing returns only those, with the names exactly as the
  client wrote them.
- Another project's namespace gets **404**, never 403, so it can't be used to probe which names exist.
- A project's data lives under `<storage-prefix>/projects/<project-id>/`, with the same layout beneath as the
  root.
- The built-in project **`default`** maps to the root layout, so namespaces and keys that predate projects
  keep working with no data move. Legacy role-and-prefix keys act inside `default`.

A superadmin key chooses the project per request with the `X-Blowfish-Project` header. With no header, it
acts on `default`. Project keys ignore the header when it names their own project, and get 404 when it names
another.

```bash
export BF=https://blowfish.example.com
# A superadmin querying project "acme"
curl -s $BF/v2/namespaces/docs/query -H "Authorization: Bearer $SUPER" \
  -H 'X-Blowfish-Project: acme' -H 'content-type: application/json' \
  -d '{"rank_by":["id","asc"],"top_k":10}'
```

## Getting started

Mint your first org superadmin key with the break-glass key, then stop using the break-glass key.

```bash
export BF=https://blowfish.example.com
curl -s -X POST $BF/_blowfish/v1/org/keys -H "Authorization: Bearer $BLOWFISH_ADMIN_KEY" \
  -H 'content-type: application/json' -d '{"name":"ops"}'
# {"key":{"id":"...","kind":"superadmin",...},"token":"bf_..."}  <- save the token now
export SUPER=bf_...
```

Create a project, then its keys:

```bash
curl -s -X POST $BF/_blowfish/v1/projects -H "Authorization: Bearer $SUPER" \
  -H 'content-type: application/json' -d '{"id":"acme","name":"Acme Search"}'

for kind in admin read_write read; do
  curl -s -X POST $BF/_blowfish/v1/projects/acme/keys -H "Authorization: Bearer $SUPER" \
    -H 'content-type: application/json' -d "{\"name\":\"acme $kind\",\"kind\":\"$kind\"}"
done
```

Point the official turbopuffer SDK at blowfish with a project key, and it works unchanged:

```python
import turbopuffer
tpuf = turbopuffer.Turbopuffer(api_key="bf_...", base_url="https://blowfish.example.com")
ns = tpuf.namespace("docs")
ns.write(upsert_rows=[{"id": 1, "vector": [1.0, 0.0]}], distance_metric="cosine_distance")
print(ns.query(rank_by=("vector", "ANN", [1.0, 0.0]), top_k=5).rows)
```

## Admin API

All routes are under `/_blowfish/v1/`, and all bodies are JSON. The OpenAPI 3.1 description is
[`api/admin.openapi.yaml`](api/admin.openapi.yaml).

| Method and path | Who | Notes |
| --- | --- | --- |
| `GET whoami` | Any key | Kind, project, key ID, and expiry |
| `GET projects`, `POST projects` | Superadmin | Body `{id, name}` |
| `GET`, `PATCH`, `DELETE projects/{id}` | Superadmin | `DELETE` refuses (409) while namespaces exist, unless `?force=true`, which deletes them all, and revokes the project's keys |
| `GET`, `POST projects/{id}/keys` | Superadmin, or that project's `admin` | Body `{name, kind, expires_at?}` |
| `GET`, `DELETE projects/{id}/keys/{key_id}` | Superadmin, or that project's `admin` | A project admin can't see or revoke `admin` keys |
| `GET`, `POST org/keys` | Superadmin | Body `{name, expires_at?}` |
| `GET`, `DELETE org/keys/{key_id}` | Superadmin | |
| `GET status` | Superadmin, or a legacy `metrics` or `keys` key | Version, redacted storage URL, mode, and uptime |

What a caller gets, by key kind:

| Route | Superadmin | Project `admin` | `read_write` and `read` |
| --- | --- | --- | --- |
| `projects`, `org/keys` | OK | 403 | 403 |
| `projects/{own}/keys` | OK | OK | 403 |
| `projects/{other}/keys` | OK | 404 | 404 |
| Namespace routes of another project (header, or none) | OK, with `X-Blowfish-Project` | 404 | 404 |

The `default` project is built in: it can't be renamed or deleted, and `POST projects` with that ID is 409.
A superadmin's `GET projects/default/keys` also lists legacy keys, so they can be inspected and revoked.

### Examples

```bash
# Who am I?
curl -s $BF/_blowfish/v1/whoami -H "Authorization: Bearer $TOKEN"
# {"kind":"read_write","project":"acme","key_id":"...","name":"...","expires_at":null}

# Projects
curl -s $BF/_blowfish/v1/projects -H "Authorization: Bearer $SUPER"
curl -s -X PATCH $BF/_blowfish/v1/projects/acme -H "Authorization: Bearer $SUPER" \
  -H 'content-type: application/json' -d '{"name":"Acme, Inc."}'
curl -s -X DELETE "$BF/_blowfish/v1/projects/acme?force=true" -H "Authorization: Bearer $SUPER"

# Project keys, as a superadmin or the project's admin key
curl -s $BF/_blowfish/v1/projects/acme/keys -H "Authorization: Bearer $ACME_ADMIN"
curl -s -X POST $BF/_blowfish/v1/projects/acme/keys -H "Authorization: Bearer $ACME_ADMIN" \
  -H 'content-type: application/json' \
  -d '{"name":"ci","kind":"read","expires_at":"2099-01-01T00:00:00Z"}'
curl -s $BF/_blowfish/v1/projects/acme/keys/$KEY_ID -H "Authorization: Bearer $ACME_ADMIN"
curl -s -X DELETE $BF/_blowfish/v1/projects/acme/keys/$KEY_ID -H "Authorization: Bearer $ACME_ADMIN"

# Org superadmin keys
curl -s $BF/_blowfish/v1/org/keys -H "Authorization: Bearer $SUPER"
curl -s -X POST $BF/_blowfish/v1/org/keys -H "Authorization: Bearer $SUPER" \
  -H 'content-type: application/json' -d '{"name":"second superadmin"}'
curl -s -X DELETE $BF/_blowfish/v1/org/keys/$KEY_ID -H "Authorization: Bearer $SUPER"
```

Key metadata has `id`, `name`, `kind`, `project`, `created_at`, `expires_at`, `created_by`, and an optional
`last_used_at` (usage seen by the node that answered, not a deployment-wide audit log). No response ever
contains a secret or hash. The creating key's ID is recorded in `created_by`. A **superadmin** caller may
pass `created_by` in the body to record an attribution label such as `webapp:alice@example.com`; other
callers can't.

A child key's expiry can't outlive its creator's. Expiry must be in the future.

## Legacy keys

Keys created before projects have roles (`admin`, `write`, `read`, `list`, `metrics`, `keys`) and optional
namespace prefixes. They keep working exactly as before, inside the `default` project, and the deprecated
`/_blowfish/v1/keys` routes and the `blowfish keys` CLI still create, list, and revoke them. Those routes see
only legacy keys. New deployments should use projects and project keys instead.

| Role | Permissions |
| --- | --- |
| `admin` | All namespace operations, including listing and namespace deletion |
| `write` | Writes, schema updates, queries, metadata, schema reads, cache warming, recall, and explain |
| `read` | Queries, metadata, schema reads, cache warming, recall, and explain |
| `list` | Namespace listing, filtered to this key's prefixes |
| `metrics` | Main-port `/metrics` and `/_blowfish/v1/status` |
| `keys` | Create, list, inspect, and revoke legacy keys, and read status |

Roles combine. `namespace_prefixes` omitted or null means unrestricted; `[]` means no namespace access.
Prefixes are literal, case-sensitive strings that apply to every namespace route. Legacy keys with `keys`
create children only with a subset of their own roles, narrower or equal prefixes, and an expiry no later
than their own.

```bash
blowfish keys create --name application --role write --role read --prefix app- --expires 30d
blowfish keys list
blowfish keys revoke KEY_ID
```

| Method and path | Request / response |
| --- | --- |
| `POST /_blowfish/v1/keys` (deprecated) | Body `{name, roles, namespace_prefixes?, expires_at?}`; 201 `{"key":{...},"token":"bf_..."}` |
| `GET /_blowfish/v1/keys` (deprecated) | 200 `{"keys":[...]}` |
| `GET`, `DELETE /_blowfish/v1/keys/{id}` (deprecated) | Inspect or revoke |

## Storage and propagation

Keys live under `<storage-prefix>/_system/keys/<id>.json` (hashes only), with revocations as
`<id>.revoked`. Projects live under `<storage-prefix>/_system/projects/<id>.json`. Nodes refresh both every
five seconds; changes apply immediately on the node handling them, and normally within five seconds
elsewhere. A failed refresh keeps the last loaded set, so a storage outage can delay revocation. A key whose
project was deleted stops authenticating. Read-only replicas can authenticate and inspect, but can't create
or revoke. Rotate the break-glass key through configuration, and restart nodes to apply the change.
