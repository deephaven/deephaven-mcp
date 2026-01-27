def test_module_all_exports():
    import deephaven_mcp.mcp_systems_server as mod

    # __all__ should include only mcp_server
    assert hasattr(mod, "mcp_server")
    assert "mcp_server" in mod.__all__
