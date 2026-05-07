---
description: Improve pydocs
---

Review the pydocs in this file for correctness, completeness, and clarity.  Be sure to also review the pydocs for the file.  Just change the pydocs, and do not change the source code.  

To make code review easy, make sure there is a reason for any change.  If there isn't a significant improvement from a change, do not make the change to make code reviews easier.

Pydocs should include typehints.

A function/class docstring documents its **contract** (what it accepts, what it returns, what it raises) — not its surrounding context. Specifically, do not include any of the following in a docstring:

- A list of callers (it's grep-recoverable and creates maintenance friction when callers change).
- Per-caller behavioral exposition (each caller's reason for using the function belongs in that caller's docstring or in a design comment, not here).
- Future-evolution hedging like "current rules", "at present, only...", or "additional rules can be added" (the docstring describes what the function does today; future changes are documented when they happen).
- Implementation rationale beyond what a caller needs to use the function correctly (rationale belongs in commit messages, design docs, or inline comments at the implementation site).

If a docstring describes the world *outside* the function (callers, design history, future plans), it's wrong.

Functions marked as MCP tools (@mcp_server.tool()) will be used by AI agents.  Their documentation should be very detailed and specific to be maximally useful to an AI agent.