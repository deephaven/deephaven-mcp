"""``dhcli`` — the Deephaven command-line tool for humans and AI agents.

Entry point: :func:`deephaven_mcp.cli._main.main`, bound to the
``dhcli`` console script in ``pyproject.toml``. A ``click`` noun-verb
tree: runtime noun groups (``session``, ``system``, ``table``,
``catalog``, ``pq``, ``tool``, ``docs``), the ``daemon`` and
``config`` operational groups, the ``agents`` noun group
(machine-readable metadata: ``tree`` / ``command`` / ``errors``), and
the ``self`` group (tool self-management; its ``completion`` verb
prints shell tab-completion scripts).

The tool's scope is operating Deephaven from the shell; today its
runtime commands are backed by a per-user local daemon (started on
demand from ``dh-mcp-systems-server --daemon``) to which MCP tool
calls are dispatched over a loopback HTTP transport — the current
mechanism, not the tool's ceiling.

User-facing reference: ``docs/CLI.md``. Internal module structure:
``docs/DEVELOPER_GUIDE.md``.
"""

from __future__ import annotations

# Empty by design: the CLI is invoked via the ``dhcli`` console
# script (see ``pyproject.toml``), not via ``from deephaven_mcp.cli
# import ...``. The empty ``__all__`` is locked by
# ``tests/cli/test_init.py:test_all_is_empty_by_design`` so adding
# a public re-export is a deliberate, reviewable diff.
__all__: list[str] = []
