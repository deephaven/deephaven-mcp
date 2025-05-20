import importlib


def test_version_module_sanity():
    import deephaven_mcp._version as v

    # Existence checks
    assert hasattr(v, "__version__")
    assert hasattr(v, "__version_tuple__")
    assert hasattr(v, "version")
    assert hasattr(v, "version_tuple")
    # Type checks
    assert isinstance(v.__version__, str)
    assert isinstance(v.__version_tuple__, tuple)
    assert isinstance(v.version, str)
    assert isinstance(v.version_tuple, tuple)
    # Default value checks (if not replaced by setuptools_scm)
    assert v.__version__ == v.version
    assert v.__version_tuple__ == v.version_tuple


def test_package_version_matches_internal():
    import deephaven_mcp
    import deephaven_mcp._version as v
    assert hasattr(deephaven_mcp, "__version__")
    assert isinstance(deephaven_mcp.__version__, str)
    assert deephaven_mcp.__version__ == v.__version__
