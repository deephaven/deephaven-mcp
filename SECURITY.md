# Security Policy

This document tells you **how to report a security vulnerability** in
Deephaven MCP and **which versions receive security fixes**. For the
operator-facing security model, hardening checklist, and threat model,
see [`docs/SECURITY.md`](docs/SECURITY.md).

## Reporting a vulnerability

**Do not file public GitHub issues for security vulnerabilities.**

Report privately via GitHub Security Advisories:

> [https://github.com/deephaven/deephaven-mcp/security/advisories/new](https://github.com/deephaven/deephaven-mcp/security/advisories/new)

### What to include

- **Affected component** — the systems server
  (`dh-mcp-systems-server`), the CLI (`dhcli`), the docs server
  (`dh-mcp-docs-server`), or shared library code.
- **Affected version** — release tag, or `main` commit hash if
  reporting against an unreleased change.
- **Minimal reproduction** or proof-of-concept.
- **Your assessment of impact** — confidentiality, integrity, and/or
  availability, with a concrete attack scenario where possible.

### What to expect

- An acknowledgment within a few business days.
- A coordinated-disclosure timeline negotiated with you. We will not
  disclose the issue publicly without giving you advance notice.
- Credit in the release notes for the fix unless you ask to remain
  anonymous.

## Supported versions

Security fixes are applied to the latest tagged release. Older releases
are not supported; please upgrade before reporting an issue against
them.

| Version | Supported |
|---|---|
| Latest tagged release | Yes |
| Older releases | No |

## Out of scope

The following are **not** in scope for this project's vulnerability
process. Please direct reports to the listed parties instead.

- **Upstream package issues** in `pydeephaven`,
  `deephaven-coreplus-client`, `mcp.server.fastmcp`, `uvicorn`, or
  `starlette`. Report to the upstream maintainers.
- **Customer infrastructure misconfiguration** — open reverse proxies,
  weak network ACLs, exposed credentials in your own environment. Not a
  Deephaven MCP defect.
- **Documented credential-disclosure behavior** — e.g. the plaintext
  session token `security.credential_retrieval_mode` returns. Its
  default, `dynamic_only`, covers only sessions this server minted a
  token for at the caller's request; widening it to reach
  operator-authored static credentials is an explicit opt-in. Both are
  deliberate, documented choices, not defects.
- **Third-party MCP clients** — Claude Desktop, Cursor, VS Code Copilot,
  Windsurf, etc. Report those to the vendor.
