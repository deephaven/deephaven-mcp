"""Canonical redaction placeholder for sensitive values.

This module is the single source of truth for the textual marker used
throughout ``deephaven_mcp`` when a sensitive value (auth token,
password, private key, PSK, etc.) has been stripped from logs, config
dumps, or object representations.

Using a shared constant instead of scattered literal strings keeps the
output format consistent across all redaction sites (config redactors,
launcher command scrubbing, MCP tool JSON redaction, credential
``__repr__`` methods) and makes any future change to the marker a
one-line edit.

Consumers should import ``REDACTED`` rather than hard-coding the
literal string, except in test assertions where hard-coding the literal
is preferred so the test fails loudly if the canonical value ever
changes.
"""

__all__ = ["REDACTED"]


REDACTED: str = "[REDACTED]"
"""Canonical placeholder substituted for any sensitive value in logs,
config dumps, or ``__repr__`` output. The bracketed uppercase form is
the widely-recognized convention in ops tooling and log aggregators."""
