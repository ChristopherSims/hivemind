# Security policy

Hivemind is a **local-first, single-owner, loopback-only reference
implementation** for the coordination of agents you control. It is not a
production multi-user service, a hardened sandbox, or an OS-level isolation
mechanism.

## Supported version

Alpha `0.1.0` on the default branch. Fixes land on `main`; pull the latest
commit rather than expecting backports.

## Trust boundary (what this software does and does not protect)

**It protects:**
- Canonical state, claims, and permissions against mistakes by agent code:
  every mutation rechecks the authenticated principal, current revision,
  claim ownership, and coordinator epoch server-side. Model output is treated
  as untrusted evidence, never as permission.
- Credentials: tokens are stored hashed; private worker configs are written
  to owner-only directories; no token is placed in URLs or MCP tool calls.

**It does not protect:**
- Against a hostile process running as the same OS user — file reservations,
  tool-boundary guards, and process supervision are cooperative policy, **not
  a filesystem/network sandbox**. Use separate OS accounts or dedicated
  sandboxing for adversaries.
- Against exposing the service beyond localhost: do **not** put this server
  on a LAN or behind a proxy. There is no TLS, no enterprise identity, and
  no multi-tenant authorization.
- Model providers, Git remotes, or message contents from being seen by the
  configured services themselves.

`docs/limitations.md` records every unvalidated boundary in detail.

## Reporting a vulnerability

Open a GitHub security advisory (Security → Report a vulnerability), or open
a regular issue prefixed `security:` if you cannot use advisories. Include:
affected endpoint/module, reproduction, and impact. Do not include live
credentials, real private data, or exploit payloads that leak third-party
terms. We aim to acknowledge reports within 7 days; alpha fixes are
best-effort on `main`.