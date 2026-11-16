# Open-source readiness checklist

Status as of 2026-11-16. Checked boxes are verified, not assumed.

## Legal and naming

- [x] MIT `LICENSE` at the root, and `license = "MIT"` in the workspace `Cargo.toml`.
- [ ] **Name collision:** `blowfish` is already a crates.io crate (the Blowfish cipher) and a
  well-known cipher name.
  - Decide whether to keep the GitHub name and publish crates as `blowfish-db-*` or `bf-*`, or rename.
  - Check `bf-*` crate names on crates.io before publishing, or mark the crates `publish = false`.
- [ ] **turbopuffer naming:** describe blowfish as "compatible with the turbopuffer API" (nominative
  use). Don't use turbopuffer's logo, and don't imply endorsement. Add a short disclaimer to the
  README.
- [ ] **Dependency licenses:** run `cargo deny check licenses` (add `deny.toml`), and check the compat
  suite's Python and Node dependencies.
- [ ] Decide whether to add `NOTICE` or third-party attributions, if any dependency needs them.

## Secrets and private details

- [x] Tracked files have no keys, bucket names, account IDs, or `/home` paths. Git history only has
  the `YOUR-ACCOUNT` endpoint placeholder.
- [x] `*.local` files are git-ignored (`AGENTS.md.local` holds this machine's details).
- [ ] **Author email in history:** 208 commits are authored with a personal address, which becomes
  public.
  - Either accept it, or rewrite history with a GitHub noreply address before flipping public.
  - Rewriting history is only easy *before* anyone forks.
- [ ] **`compat/pyproject.toml`** lists a personal email under `authors`; switch it to the noreply
  address or remove it.
- [ ] **Expiring short links:** `docs/benchmarks.md` has hop.rip links, which are 10-day links that
  will die. Replace them with full URLs.
- [ ] **Recorded fixtures:** `compat/fixtures/` holds responses recorded from a real turbopuffer
  account. Scan them for account, org, or request IDs, and for namespace names that reveal anything.
- [ ] Run a secret scanner over the full history (`gitleaks detect` or `trufflehog git`) as a final gate.

## Repo hygiene

- [ ] **README:** it's 47 lines today. It needs:
  - what blowfish is;
  - a quickstart (Docker and binary);
  - a compatibility table;
  - the honest "upgrade to turbopuffer" recommendation;
  - benchmark headlines with their caveats;
  - status: pre-1.0, formats may change.
- [ ] `CONTRIBUTING.md`: build, test, the compat suite, the commit style, and the Oxford comma rule.
- [ ] `SECURITY.md`: a private disclosure address or GitHub security advisories, and supported versions.
- [ ] `CODE_OF_CONDUCT.md`, such as Contributor Covenant.
- [ ] `CHANGELOG.md`, starting at the first public tag.
- [ ] Issue and PR templates under `.github/`.
- [ ] **Agent-facing files:**
  - Decide whether `AGENTS.md`, `CLAUDE.md`, and `PROGRESS.md` stay public, or are trimmed or moved
    to `docs/`.
  - `PROGRESS.md` is a long internal build log, and `docs/research/` holds notes about turbopuffer.
- [ ] Remove or document `bench/scale/` assumptions: it expects a git-ignored config with the bucket
  and endpoint.

## Quality gates

- [x] CI exists (`.github/workflows/ci.yml`).
  - [ ] Confirm it runs fmt, clippy `-D warnings`, tests, and the compat suite in both modes.
- [x] Compat suite: 121/121 in normal and forced-indexing modes on main.
- [ ] **Freeze the storage formats**, or version them with an explicit compatibility promise. Every
  perf round so far has changed segment and manifest formats.
- [ ] Hardening work (paused `harden` branch): admission control, and revalidation during schema
  changes. Finish and merge.
- [ ] Soak test: a long write-heavy run, crash and kill during indexing and compaction, recovery,
  and two query nodes.
- [ ] Fuzz the wire-format parsers (filters, rank_by, segment readers) with `cargo fuzz`.
- [ ] 10M benchmark on the final formats, published with methodology and caveats.

## Product surface

- [ ] **Org, projects, and keys** (design: `docs/design/projects-and-keys.md`), with the admin
  OpenAPI spec.
- [ ] **Admin webapp** (`webapp/`, Go) with magic-link login.
- [ ] **Docs:** `docs/quickstart.md`, `configuration.md`, `operations.md`, `auth.md`, and
  `security.md`, reviewed for accuracy against current flags and the projects model.
- [ ] **Release artifacts:** tagged GitHub releases with Linux x86_64 and arm64 binaries, plus a
  container image on GHCR. Build them from CI with provenance.
- [ ] A minimal example: `docker compose up` with MinIO, plus a sample SDK script.

## Flip

- [ ] Final private-string grep of the tree and history, and the secret-scanner run. Known hit: one integration hostname reached history in the perfiter9 merge (`bench/scale/parity.sh`, removed in 8af1e32).
- [ ] Make `dorkitude/blowfish` public, and turn on security advisories and Dependabot.
- [ ] Tag `v0.1.0`.
- [ ] Clean up turbopuffer reference namespaces (`blowfish-oracle-wiki*`) and the R2
  `scale/` and `live-backup-*` prefixes when they're no longer needed. These aren't public, but they
  still cost money.
