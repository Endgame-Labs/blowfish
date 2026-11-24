# Security Policy

## Supported versions

blowfish is pre-1.0 software. Security fixes are applied to the `main` branch and the most recent tagged release only.

| Version | Supported |
| --- | --- |
| `main` / latest `0.x` tag | Yes |
| Older `0.x` tags and commits | No |

## Reporting a vulnerability

Do **not** open a public GitHub issue or pull request for an undisclosed vulnerability, and do not test against deployments you do not own.

Report vulnerabilities privately through GitHub private vulnerability reporting:

https://github.com/endgame-labs/blowfish/security/advisories/new

If private vulnerability reporting is unavailable on the repository, contact the maintainer privately through the Endgame Labs GitHub organization (https://github.com/endgame-labs) to arrange a secure channel before sharing details.

Please include:

- the affected commit SHA or release version;
- a minimal reproducer using synthetic data;
- the required configuration and API key kind or role (for example, unauthenticated, project `read`, project `admin`, or `superadmin`);
- expected behavior, observed behavior, and security impact.

blowfish is maintained on a best-effort basis and does not offer a guaranteed response-time SLA. Public issues are appropriate only after coordinated disclosure or for non-sensitive hardening improvements.

## Security model and operator responsibilities

Authentication and project/namespace isolation are enforced security boundaries when auth is enabled (`BLOWFISH_ADMIN_KEY` set with `--require-auth` or `require_auth = true`). Running without `BLOWFISH_ADMIN_KEY` enables unauthenticated development mode and must never be exposed to untrusted networks. Query execution enforces per-request size and complexity bounds, but is not yet a hard multi-tenant CPU or memory isolation boundary; place internet-facing deployments behind TLS, connection limits, and request-rate controls, and restrict access to the object-storage bucket and any unauthenticated `--metrics-listen` port.

See [`docs/security.md`](docs/security.md) for the full threat model, route authorization matrix, input limits, and durability boundaries.
