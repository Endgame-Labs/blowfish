# blowfish console 🐡

A small web console for blowfish admins: sign in with an emailed magic link, then manage **projects**,
their **API keys**, and the org's **superadmin keys**. It is a separate Go service (standard library
only, no dependencies) that talks to blowfish through the documented
[admin API](../docs/api/admin.openapi.yaml), using one superadmin key from its environment. Blowfish
stays the only place permissions are enforced.

## Run it

```bash
# 1. Mint a superadmin key for the console (once), with blowfish's break-glass key.
curl -s -X POST "$BLOWFISH_URL/_blowfish/v1/org/keys" \
  -H "Authorization: Bearer $BLOWFISH_ADMIN_KEY" -H 'content-type: application/json' \
  -d '{"name":"console"}'      # copy "token"

# 2. Start the console.
export BLOWFISH_WEBAPP_API_URL=http://127.0.0.1:8000
export BLOWFISH_WEBAPP_API_KEY=bf_...                  # the superadmin key
export BLOWFISH_WEBAPP_ADMINS=you@example.com,them@example.com
export BLOWFISH_WEBAPP_SESSION_SECRET=$(openssl rand -hex 32)
export BLOWFISH_WEBAPP_MAILER=log                      # prints sign-in links; see "Mailers"
go run .                                               # or: go build -o webapp && ./webapp
```

Open `http://127.0.0.1:8090/`, enter an allowed address, and (with the `log` mailer) copy the link from
the console's output.

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `BLOWFISH_WEBAPP_API_KEY` | required | A blowfish **superadmin** key. Use a minted one, not the break-glass key |
| `BLOWFISH_WEBAPP_ADMINS` | required | Comma-separated emails allowed to sign in |
| `BLOWFISH_WEBAPP_SESSION_SECRET` | required | At least 32 characters. Signs sessions and forms. Changing it signs everyone out |
| `BLOWFISH_WEBAPP_API_URL` | `http://127.0.0.1:8000` | Where blowfish listens |
| `BLOWFISH_WEBAPP_LISTEN` | `127.0.0.1:8090` | Listen address |
| `BLOWFISH_WEBAPP_BASE_URL` | `http://127.0.0.1:8090` | The public URL, used in emailed links. Its path is the prefix the app is mounted under, such as `https://example.com/console` |
| `BLOWFISH_WEBAPP_MAILER` | `log` | `exedev`, `smtp`, or `log` |
| `BLOWFISH_WEBAPP_TRUST_PROXY` | `false` | Take the client IP from the last `X-Forwarded-For` entry, for rate limits. Set it behind a reverse proxy that sets that header |
| `BLOWFISH_WEBAPP_EXEDEV_EMAIL_URL` | `http://169.254.169.254/gateway/email/send` | The exe.dev email gateway |
| `BLOWFISH_WEBAPP_SMTP_ADDR` | | `host:port`, for the `smtp` mailer |
| `BLOWFISH_WEBAPP_SMTP_USER`, `_PASS` | | Optional SMTP login (PLAIN, over STARTTLS when offered) |
| `BLOWFISH_WEBAPP_SMTP_FROM` | | The `From` address, for the `smtp` mailer |

## Mailers

- **`exedev`**: on an exe.dev VM, posts `{to, subject, body}` to the VM's email gateway. No credentials.
- **`smtp`**: any SMTP server, for everyone else.
- **`log`**: prints the message, including the link, as a JSON log line. For development only.

## How sign-in works

- You enter an email. If it is on `BLOWFISH_WEBAPP_ADMINS`, a single-use link goes out that is good for 15
  minutes. **The response is identical whether or not the address is allowed.**
- The link opens a "Sign in" button, and clicking it uses the link, so email scanners that fetch links can't
  use it up. Tokens are 32 random bytes, and only their hash is kept, in memory. A restart invalidates
  pending links.
- Requests for links are rate limited per email (5 per 15 minutes) and per client IP (10 per 15 minutes).
- A session is a signed cookie (`HttpOnly`, `SameSite=Lax`, and `Secure` for `https` base URLs) that lasts
  12 hours. Every form that changes something carries a CSRF token. Removing an address from the allowlist
  and restarting ends its sessions.

## Pages

- **Projects**: list, create, rename, and delete (type the ID to confirm; a project with namespaces needs
  the "also delete namespaces" box).
- **Project**: its keys, creating one (the token is shown once, with a copy button), and revoking one.
- **Org keys**: the superadmin keys. The console won't revoke the key it runs on.

Every change is logged on stdout as a JSON `audit` line with the admin's email, and keys the console
creates are recorded in blowfish with `created_by: "webapp:<email>"`.

## Behind a reverse proxy at `/console/`

Set `BLOWFISH_WEBAPP_BASE_URL=https://blowfish.example.com/console` and `BLOWFISH_WEBAPP_TRUST_PROXY=true`.
The app serves everything under that path, so the proxy must **not** strip the prefix. With Caddy:

```caddyfile
blowfish.example.com {
	handle /console/* {
		reverse_proxy 127.0.0.1:8090
	}
	handle /console {
		redir /console/ permanent
	}
	handle {
		reverse_proxy 127.0.0.1:8080
	}
}
```

`GET /console/healthz` answers `ok` without signing in.

## Docker

```bash
docker build -t blowfish-webapp webapp/
docker run --rm -p 127.0.0.1:8090:8090 --env-file console.env blowfish-webapp
```

The repository's `docker-compose.yml` has a `webapp` service (profile `console`):

```bash
BLOWFISH_ADMIN_KEY=... docker compose up -d blowfish           # then mint a superadmin key
BLOWFISH_WEBAPP_API_KEY=bf_... BLOWFISH_WEBAPP_ADMINS=you@example.com \
BLOWFISH_WEBAPP_SESSION_SECRET=$(openssl rand -hex 32) docker compose --profile console up -d webapp
```

## Development

```bash
go vet ./... && go test ./...
```

The tests drive the whole app against an `httptest` fake of the admin API: sign-in, the allowlist, link
expiry and reuse, rate limits, CSRF, session expiry, and every page's action.
