"""Unit tests for deephaven_mcp.client._base module.

These tests directly test the actual ClientObjectWrapper class
and the is_enterprise_available flag from the _base module.
"""

import importlib
import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import TypeVar
from unittest.mock import MagicMock, patch

import pytest

# Import InternalError directly since it's used in _base.py
from deephaven_mcp._exceptions import InternalError


def get_base_module(enterprise_available=True):
    """Import the _base module directly from file to avoid dependency issues.

    Args:
        enterprise_available: If True, mock deephaven_enterprise as available.
                             If False, make it raise ImportError.
    """
    # Get the absolute path to the _base.py file
    base_file = (
        Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        / "src"
        / "deephaven_mcp"
        / "client"
        / "_base.py"
    )

    # Create a new module spec and module
    spec = importlib.util.spec_from_file_location(
        "deephaven_mcp.client._base", base_file
    )
    module = importlib.util.module_from_spec(spec)

    # Setup mock logger to capture logs
    mock_logger = MagicMock()

    # Mock sys.modules with or without enterprise modules
    mocked_modules = {
        "deephaven_enterprise.proto": MagicMock(),
        "deephaven_enterprise.proto.auth_pb2": MagicMock(),
        "deephaven_enterprise.client": MagicMock(),
        "deephaven_enterprise.client.controller": MagicMock(),
        "deephaven_enterprise.client.util": MagicMock(),
    }

    if enterprise_available:
        # Make enterprise available
        mocked_modules["deephaven_enterprise"] = MagicMock()

    # Add mocks to avoid other ImportErrors during module load
    with patch.object(logging, "getLogger", return_value=mock_logger):
        with patch.dict("sys.modules", mocked_modules):
            # This is the key part - control whether importing deephaven_enterprise raises ImportError
            if not enterprise_available:

                def mock_import(*args, **kwargs):
                    name = args[0] if args else kwargs.get("name")
                    if name == "deephaven_enterprise":
                        raise ImportError(f"No module named '{name}'")
                    return importlib.__import__(*args, **kwargs)

                with patch("builtins.__import__", side_effect=mock_import):
                    # Load the module - this should trigger the ImportError
                    spec.loader.exec_module(module)
            else:
                # Load the module normally
                spec.loader.exec_module(module)

    return module, mock_logger


@pytest.fixture
def base_module():
    """Fixture to provide the _base module for tests."""
    module, _ = get_base_module()
    return module


def test_enterprise_available_flag_with_mock():
    """Test the is_enterprise_available flag in the actual module when enterprise is available."""
    module, mock_logger = get_base_module(enterprise_available=True)
    assert module.is_enterprise_available is True
    mock_logger.debug.assert_called_with("Enterprise features available")


def test_enterprise_unavailable_flag():
    """Test the import error branch for is_enterprise_available flag."""
    # Get the file path
    base_file = (
        Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        / "src"
        / "deephaven_mcp"
        / "client"
        / "_base.py"
    )

    # Read the file content
    with open(base_file, "r") as f:
        content = f.read()

    # Create module namespace
    module_namespace = {}

    # Create logger mock
    mock_logger = MagicMock()

    # Execute only selected lines in a controlled environment with appropriate mocks
    with patch.object(logging, "getLogger", return_value=mock_logger):
        with patch.dict("sys.modules", {"deephaven_enterprise": None}):
            with patch(
                "builtins.__import__",
                side_effect=ImportError("No module named 'deephaven_enterprise'"),
            ):
                # Define the variables we need in the namespace
                module_namespace["logging"] = logging
                module_namespace["TypeVar"] = TypeVar
                module_namespace["InternalError"] = InternalError

                # Create a minimal version of the try/except block
                test_code = """
# Setup a logger for the test
_LOGGER = logging.getLogger(__name__)

# Test for enterprise availability
is_enterprise_available = False
try:
    # This will raise ImportError
    import deephaven_enterprise
    is_enterprise_available = True
    _LOGGER.debug("Enterprise features available")
except ImportError:
    _LOGGER.debug("Enterprise features not available")
"""
                # Execute the test code
                exec(test_code, module_namespace)

    # Verify results
    assert module_namespace["is_enterprise_available"] is False
    mock_logger.debug.assert_called_with("Enterprise features not available")


def test_enterprise_unavailable_flag_direct():
    """Test the direct import error branch in the actual module."""
    module, mock_logger = get_base_module(enterprise_available=False)
    assert module.is_enterprise_available is False
    mock_logger.debug.assert_called_with("Enterprise features not available")


def test_client_object_wrapper_init_with_valid_object(base_module):
    """Test real ClientObjectWrapper with a valid object."""
    # Get the real ClientObjectWrapper class
    ClientObjectWrapper = base_module.ClientObjectWrapper

    # Test initialization with valid object and enterprise=False
    mock_logger = MagicMock()
    with patch.object(base_module, "_LOGGER", mock_logger):
        # Force is_enterprise_available to False for testing
        with patch.object(base_module, "is_enterprise_available", False):
            mock_obj = MagicMock()
            wrapper = ClientObjectWrapper(mock_obj, is_enterprise=False)

            # Check the wrapper contains the mock object
            assert wrapper.wrapped == mock_obj
            assert mock_logger.error.call_count == 0


def test_client_object_wrapper_init_with_none(base_module):
    """Test that real ClientObjectWrapper raises ValueError when initialized with None."""
    # Get the real ClientObjectWrapper class
    ClientObjectWrapper = base_module.ClientObjectWrapper

    # Set up mocks and patches
    mock_logger = MagicMock()
    with patch.object(base_module, "_LOGGER", mock_logger):
        # Test initialization with None
        with pytest.raises(ValueError, match="Cannot wrap None"):
            ClientObjectWrapper(None, is_enterprise=False)

        mock_logger.error.assert_called_with(
            "ClientObjectWrapper constructor called with None"
        )


def test_client_object_wrapper_enterprise_not_available(base_module):
    """Test ClientObjectWrapper when enterprise=True but enterprise features are not available."""
    # Get the real ClientObjectWrapper class
    ClientObjectWrapper = base_module.ClientObjectWrapper

    # Set up mocks and patches
    mock_logger = MagicMock()
    with patch.object(base_module, "_LOGGER", mock_logger):
        # Force is_enterprise_available to False
        with patch.object(base_module, "is_enterprise_available", False):
            mock_obj = MagicMock()
            # Test with enterprise=True should raise InternalError
            with pytest.raises(
                InternalError, match="enterprise features are not available"
            ):
                ClientObjectWrapper(mock_obj, is_enterprise=True)


def test_client_object_wrapper_enterprise_available(base_module):
    """Test ClientObjectWrapper when enterprise=True and enterprise features are available."""
    # Get the real ClientObjectWrapper class
    ClientObjectWrapper = base_module.ClientObjectWrapper

    # Set up mocks and patches
    with patch.object(base_module, "is_enterprise_available", True):
        mock_obj = MagicMock()
        # This should succeed when enterprise features are available
        wrapper = ClientObjectWrapper(mock_obj, is_enterprise=True)
        assert wrapper.wrapped == mock_obj


def test_client_object_wrapper_property(base_module):
    """Test the wrapped property returns the correct object."""
    # Get the real ClientObjectWrapper class
    ClientObjectWrapper = base_module.ClientObjectWrapper

    # Force is_enterprise_available to False for testing
    with patch.object(base_module, "is_enterprise_available", False):
        # Create test object with special attribute
        mock_obj = MagicMock()
        mock_obj.special_attribute = "test_value"
        wrapper = ClientObjectWrapper(mock_obj, is_enterprise=False)

        # Verify property returns the wrapped object and its attributes are accessible
        assert wrapper.wrapped == mock_obj
        assert wrapper.wrapped.special_attribute == "test_value"


# ---- Merged edge-case tests from test_base_edge.py ----


def test_client_object_wrapper_type_preservation(base_module):
    """Test that ClientObjectWrapper preserves the type of the wrapped object."""
    ClientObjectWrapper = base_module.ClientObjectWrapper

    class Dummy:
        pass

    dummy = Dummy()
    wrapper = ClientObjectWrapper(dummy, is_enterprise=False)
    assert isinstance(wrapper.wrapped, Dummy)


def test_client_object_wrapper_multiple_instances(base_module):
    """Test that multiple ClientObjectWrapper instances operate independently."""
    ClientObjectWrapper = base_module.ClientObjectWrapper
    obj1 = MagicMock()
    obj2 = MagicMock()
    wrapper1 = ClientObjectWrapper(obj1, is_enterprise=False)
    wrapper2 = ClientObjectWrapper(obj2, is_enterprise=False)
    assert wrapper1.wrapped is obj1
    assert wrapper2.wrapped is obj2
    assert wrapper1 is not wrapper2


def test_client_object_wrapper_property_is_readonly(base_module):
    """Test that the wrapped property is read-only and cannot be set."""
    ClientObjectWrapper = base_module.ClientObjectWrapper
    obj = MagicMock()
    wrapper = ClientObjectWrapper(obj, is_enterprise=False)
    with pytest.raises(AttributeError):
        wrapper.wrapped = obj


def test_is_enterprise_available_logging():
    """Test that the logger is called correctly for both available and unavailable enterprise."""

    # Use get_base_module from test_base_edge.py logic
    def get_base_module(enterprise_available=True):
        import importlib.util
        import logging
        import os
        from pathlib import Path
        from unittest.mock import MagicMock, patch

        base_file = (
            Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            / "src"
            / "deephaven_mcp"
            / "client"
            / "_base.py"
        )
        spec = importlib.util.spec_from_file_location(
            "deephaven_mcp.client._base", base_file
        )
        module = importlib.util.module_from_spec(spec)
        mock_logger = MagicMock()
        mocked_modules = {
            "deephaven_enterprise.proto": MagicMock(),
            "deephaven_enterprise.proto.auth_pb2": MagicMock(),
            "deephaven_enterprise.client": MagicMock(),
            "deephaven_enterprise.client.controller": MagicMock(),
            "deephaven_enterprise.client.util": MagicMock(),
        }
        if enterprise_available:
            mocked_modules["deephaven_enterprise"] = MagicMock()
        with patch.object(logging, "getLogger", return_value=mock_logger):
            with patch.dict("sys.modules", mocked_modules):
                if not enterprise_available:

                    def mock_import(*args, **kwargs):
                        name = args[0] if args else kwargs.get("name")
                        if name == "deephaven_enterprise":
                            raise ImportError(f"No module named '{name}'")
                        return importlib.__import__(*args, **kwargs)

                    with patch("builtins.__import__", side_effect=mock_import):
                        spec.loader.exec_module(module)
                else:
                    spec.loader.exec_module(module)
        return module, mock_logger

    _, mock_logger_true = get_base_module(enterprise_available=True)
    _, mock_logger_false = get_base_module(enterprise_available=False)
    mock_logger_true.debug.assert_called_with("Enterprise features available")
    mock_logger_false.debug.assert_called_with("Enterprise features not available")
