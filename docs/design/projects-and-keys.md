# Org, projects, and API keys, plus the admin webapp

## Goal

A blowfish deployment is one **org**. The org holds **projects**, and each project has its own
namespaces and its own API keys. Humans manage all of this in a small webapp, signing in with
emailed magic links. Programs do the same through a documented admin API.

## Model

| Thing | What it is |
|---|---|
| Org | The deployment. Exactly one. |
| Org superadmin key | Controls every project and every key, including other superadmin keys. Minted and revoked through the API or the webapp. |
| Project | `{id, name, created_at, created_by}`. `id` is a slug of `[a-z0-9-]{1,40}`. |
| Project key | Belongs to one project. Its kind is `admin`, `read_write`, or `read`. |

What each kind of key can do:

| Kind | Namespaces in its project | Keys |
|---|---|---|
| Org superadmin | Everything, in every project | Create, list, and revoke any key, including project admin and superadmin keys. Create, rename, and delete projects. |
| Project `admin` | Everything, including listing and deleting namespaces | Create, list, and revoke **`read_write` and `read`** keys in its own project. It can't mint `admin` keys. |
| Project `read_write` | Writes, schema updates, queries, metadata, warming, recall, and explain | none |
| Project `read` | Queries, metadata, schema reads, warming, recall, and explain | none |

Key details:
- Tokens keep the existing format `bf_<id>_<secret>`, and only a SHA-256 hash is stored.
- A token is shown once, when it's created.
- Expiry is optional.
- Revocation applies immediately on the node that handles it, and within about 5 s on every other
  node, as today.

The `BLOWFISH_ADMIN_KEY` environment variable stays as a deployment **break-glass** key, with
superadmin powers, never stored in the bucket. It's how you mint the first org superadmin key and
recover if every superadmin key is lost. Programs and the webapp should use a minted superadmin key,
not the break-glass key.

## Namespace isolation

Each project has its own namespace space:
- Project `a` and project `b` can each have a namespace called `docs`, and the two are unrelated.
- A key only ever sees its own project's namespaces. Listing returns only those, and the names come
  back exactly as the client wrote them.
- A request naming another project's namespace gets 404, not 403, so it can't be used to probe
  which names exist.

Storage:
- Project namespaces live under `<storage-prefix>/projects/<project-id>/`, with the same layout
  beneath as today's root.
- A project called **`default`** maps to today's root layout, so existing namespaces and existing
  keys keep working with no data move.
- Legacy role-and-prefix keys behave as before, inside `default`.

On a superadmin key, the project is selected per request with the `X-Blowfish-Project` header:
- With no header, a superadmin's namespace routes act on `default`.
- Project keys ignore the header. A header naming a different project returns 404.

## Admin API

All of it goes under `/_blowfish/v1/`, and all of it is JSON. Errors use the turbopuffer envelope.

| Method and path | Who | Notes |
|---|---|---|
| `GET whoami` | any key | Kind, project, key ID, and expiry |
| `GET projects`, `POST projects` | superadmin | Body `{id, name}` |
| `GET`, `PATCH`, `DELETE projects/{id}` | superadmin | `DELETE` refuses while namespaces exist, unless `?force=true`, which deletes them all |
| `GET projects/{id}/keys`, `POST projects/{id}/keys` | superadmin, or that project's `admin` | Body `{name, kind, expires_at?}`. A project admin can only create `read_write` and `read` keys. |
| `GET`, `DELETE projects/{id}/keys/{key_id}` | superadmin, or that project's `admin` | A project admin can't see or revoke `admin` keys |
| `GET org/keys`, `POST org/keys` | superadmin | Superadmin keys |
| `GET`, `DELETE org/keys/{key_id}` | superadmin | |

The existing `/_blowfish/v1/keys` routes keep working against `default` and are marked deprecated.

Write `docs/api/admin.openapi.yaml`, an OpenAPI 3.1 spec, and rewrite `docs/auth.md` around this
model, with curl examples for each route.

## Webapp (`webapp/`, Go)

A small, separate Go service: standard library `net/http` and `html/template`, no JavaScript
framework, and as few dependencies as possible. It talks to blowfish only through the admin API
above, using a superadmin key from its environment (`BLOWFISH_WEBAPP_API_KEY`).

**Magic-link login:**
- The admin enters their email.
- If the address is on the allowlist (`BLOWFISH_WEBAPP_ADMINS`, comma-separated), the webapp emails a
  single-use link that's good for 15 minutes.
- The response is the same whether or not the address is on the list, so nobody can find out which
  addresses are admins.
- Link tokens are 32 random bytes, and only their hash is kept. They're single-use, and the record
  is kept in memory, since this is a single instance.
- Login attempts are rate-limited per email and per IP.

**Sessions:**
- A signed cookie (`HttpOnly`, `Secure`, `SameSite=Lax`) that lasts 12 hours.
- Every form that changes something carries a CSRF token.
- The session secret is `BLOWFISH_WEBAPP_SESSION_SECRET`.

**Mailers**, chosen with `BLOWFISH_WEBAPP_MAILER`:
- `exedev`: `POST http://169.254.169.254/gateway/email/send` with `{to, subject, body}`, plain text;
- `smtp`, for other self-hosters;
- `log`, for development, which prints the link.

**Pages:**
- Projects: list, create, rename, and delete, with a confirmation step.
- Project detail: its keys, creating one (the token is shown once, with a copy button), and revoking one.
- Org superadmin keys.

Every change is logged with the admin's email. Keys the webapp creates are recorded with
`created_by: "webapp:<email>"`.

**Serving:**
- Listen address: `BLOWFISH_WEBAPP_LISTEN`, default `127.0.0.1:8090`.
- Base URL for links: `BLOWFISH_WEBAPP_BASE_URL`.
- In a deployment, put it behind the same reverse proxy at `/console/`.

**Shipping:**
- Tests: `go test ./...` with an `httptest` fake of the admin API, covering login, CSRF, the
  allowlist, rate limits, and each page's action.
- A Dockerfile, and a `docker-compose.yml` service.
- A README in `webapp/`.

## Tests on the blowfish side

The isolation matrix:
- Every kind of key against every route, own project and other project, checking 401, 403, and 404.
- Same-name namespaces in two projects stay separate.
- Listing is filtered.
- Legacy keys and namespaces still work in `default`.
- Revocation propagates.
- A project admin can't escalate: it can't mint or revoke `admin` keys.

The compat suite must still pass 121/121 in both modes.
