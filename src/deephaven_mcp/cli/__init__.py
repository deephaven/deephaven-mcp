"""``dh-mcp`` command-line interface for the Deephaven MCP systems server.

Entry point: :func:`deephaven_mcp.cli._main.main`, bound to the
``dh-mcp`` console script in ``pyproject.toml``. A ``click`` noun-verb
tree with the ``daemon``, ``tool``, and ``config`` noun groups plus the
``introspect`` noun group (machine-readable metadata: ``tree`` /
``command`` / ``errors``).

The CLI manages a per-user local daemon (started on demand from
``dh-mcp-systems-server --daemon``) and dispatches MCP tool calls to
it over a loopback HTTP transport.

User-facing reference: ``docs/CLI.md``. Internal module structure:
``docs/DEVELOPER_GUIDE.md``.
"""

from __future__ import annotations

# Empty by design: the CLI is invoked via the ``dh-mcp`` console
# script (see ``pyproject.toml``), not via ``from deephaven_mcp.cli
# import ...``. The empty ``__all__`` is locked by
# ``tests/cli/test_init.py:test_all_is_empty_by_design`` so adding
# a public re-export is a deliberate, reviewable diff.
__all__: list[str] = []
