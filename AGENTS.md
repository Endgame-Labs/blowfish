# AGENTS.md

Guidance for coding agents (Claude Code, Codex, Pi, ...) working in this repo.

## What this is

blowfish is an open-source, self-hostable service that is **compatible with the turbopuffer API** and works
the same way: object storage is the source of truth. You bring an S3-compatible bucket, and blowfish does the
rest. Existing turbopuffer clients and SDKs should work against it unchanged. It will be published as open
source, so nothing private (hostnames, keys, customer data, or anything from a `*.local` file) goes in tracked files.

## Read AGENTS.md.local first, every session

**Always read `AGENTS.md.local` (next to this file) before doing anything else.** It holds this machine's
private details: the VM, its integrations, storage buckets, and credentials, and how to use each one. It is
ignored by git (`.gitignore` ignores `*.local`), so it never leaves the machine. If it is missing, say so
instead of guessing.

## Rules

- Never commit secrets or print their values. Private details go in a `*.local` file, never in tracked files.
- Before every commit, check the diff for secrets, tokens, internal hostnames, and anything from a `*.local` file.
- Use `uv` for anything Python.
- Commit your own changes as you go, and push them. Leave other sessions' uncommitted work alone.
- Use the Oxford comma in everything you write here: "a, b, and c".
