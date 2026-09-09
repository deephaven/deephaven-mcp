"""Canonical output markers for values withheld from user-facing output.

This module is the single source of truth for the textual markers used
throughout ``deephaven_mcp`` when a sensitive value (auth token,
password, private key, PSK, etc.) has been stripped from logs, config
dumps, or object representations, and when a value could not be parsed
well enough to redact and so was suppressed wholesale.

Using shared constants instead of scattered literal strings keeps the
output format consistent across all redaction sites (config redactors,
launcher command scrubbing, MCP tool JSON redaction, credential
``__repr__`` methods) and makes any future change to a marker a
one-line edit.

Consumers should import these constants rather than hard-coding the
literal strings, except in test assertions where hard-coding the literal
is preferred so the test fails loudly if a canonical value ever
changes.
"""

__all__ = ["REDACTED", "UNPARSEABLE"]


REDACTED: str = "[REDACTED]"
"""Canonical placeholder substituted for any sensitive value in logs,
config dumps, or ``__repr__`` output. The bracketed uppercase form is
the widely-recognized convention in ops tooling and log aggregators."""

UNPARSEABLE: str = "[UNPARSEABLE]"
"""Canonical placeholder substituted for a whole value that could not be
parsed, and so was suppressed rather than selectively redacted: an
unparseable value may hide a secret anywhere in it."""
