"""
Tests for the deephaven_mcp.enterprise package init (Enterprise system support).
"""

import importlib
from unittest import mock

import pytest

# Path to the module we are testing
MODULE_UNDER_TEST = "deephaven_mcp.enterprise"


@pytest.fixture(autouse=True)
def clear_module_cache():
    """Ensures the module under test is re-imported for each test case."""
    if MODULE_UNDER_TEST in importlib.sys.modules:
        del importlib.sys.modules[MODULE_UNDER_TEST]
    yield
    if MODULE_UNDER_TEST in importlib.sys.modules:
        del importlib.sys.modules[MODULE_UNDER_TEST]


def test_enterprise_init_coreplus_client_not_found():
    """
    Test that importing deephaven_mcp.enterprise raises the custom ImportError
    if deephaven_coreplus_client is not found.
    """
    # Ensure 'deephaven_coreplus_client' is not in sys.modules to simulate it not being installed.
    # This is important because if it was somehow imported by another test, it might exist.
    if "deephaven_coreplus_client" in importlib.sys.modules:
        del importlib.sys.modules["deephaven_coreplus_client"]

    # Patch importlib.util.find_spec for 'deephaven_coreplus_client' to return None.
    # This makes the import machinery believe the module cannot be found.
    def mock_find_spec(name, package=None):
        if name == "deephaven_coreplus_client":
            return None
        # For any other module, fall back to the original find_spec to allow normal imports.
        # This requires storing the original find_spec before patching.
        return _original_find_spec(name, package)

    _original_find_spec = importlib.util.find_spec
    with mock.patch("importlib.util.find_spec", side_effect=mock_find_spec):
        with pytest.raises(
            ImportError,
            match="The 'deephaven_mcp.enterprise' module requires 'deephaven-coreplus-client' to be installed",
        ):
            importlib.import_module(MODULE_UNDER_TEST)


def test_enterprise_init_coreplus_client_found(capfd):
    """
    Test that deephaven_mcp.enterprise imports successfully and prints messages
    if deephaven_coreplus_client is found.
    """
    # Create a mock for deephaven_coreplus_client
    mock_coreplus = mock.MagicMock()
    mock_coreplus.__version__ = "0.1.2-mock"  # Example version

    # Simulate deephaven_coreplus_client being installed by placing the mock in sys.modules
    # This ensures that when enterprise/__init__.py tries `import deephaven_coreplus_client`, it gets our mock.
    with mock.patch.dict(
        importlib.sys.modules, {"deephaven_coreplus_client": mock_coreplus}
    ):
        try:
            imported_module = importlib.import_module(MODULE_UNDER_TEST)
            assert imported_module is not None
        except ImportError as e:
            pytest.fail(
                f"Importing {MODULE_UNDER_TEST} failed when deephaven_coreplus_client was mocked: {e}"
            )

        # Check for the print statements
        captured = capfd.readouterr()
        assert (
            "INFO: 'deephaven-coreplus-client' found. Deephaven MCP enterprise submodule is available."
            in captured.out
        )
        assert mock_coreplus.__version__ in captured.out
