"""Tests for the deephaven_mcp.mcp_systems_server.__main__ module entry point."""


def test_main_module_reexports_server_main():
    """``python -m deephaven_mcp.mcp_systems_server`` runs the server's main.

    The module-execution form is how the ``dhcli`` CLI spawns the
    background daemon, so the ``__main__`` module must bind the same
    callable as the ``dh-mcp-systems-server`` console script.
    """
    import deephaven_mcp.mcp_systems_server.__main__ as entry
    from deephaven_mcp.mcp_systems_server.server import main

    assert entry.main is main
