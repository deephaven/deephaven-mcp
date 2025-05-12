def test_version_module_sanity():
    import deephaven_mcp._version as v

    assert hasattr(v, "__version__")
    assert isinstance(v.__version__, str)
    assert hasattr(v, "__version_tuple__")
    assert isinstance(v.__version_tuple__, tuple)
    assert hasattr(v, "version")
    assert isinstance(v.version, str)
    assert hasattr(v, "version_tuple")
    assert isinstance(v.version_tuple, tuple)


def test_package_version_matches_internal():
    import deephaven_mcp
    import deephaven_mcp._version as v

    assert hasattr(deephaven_mcp, "__version__")
    assert isinstance(deephaven_mcp.__version__, str)
    assert deephaven_mcp.__version__ == v.__version__
