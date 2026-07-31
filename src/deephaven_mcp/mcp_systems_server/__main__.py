"""Module entry point for ``python -m deephaven_mcp.mcp_systems_server``.

Delegates to :func:`deephaven_mcp.mcp_systems_server.server.main`, the
same callable wired to the ``dh-mcp-systems-server`` console script. The
``dhcli`` CLI spawns the background daemon via this module form so the
daemon runs under the caller's interpreter (see
:func:`deephaven_mcp.cli._daemon._build_spawn_command`).
"""

from deephaven_mcp.mcp_systems_server.server import main

if __name__ == "__main__":  # pragma: no cover
    main()
