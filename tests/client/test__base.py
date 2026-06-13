"""Unit tests for deephaven_mcp.client._base module.

These tests directly exercise the ClientObjectWrapper class from the _base module.
"""

from unittest.mock import MagicMock, patch

import pytest

import deephaven_mcp.client._base as base_module
from deephaven_mcp.client._base import ClientObjectWrapper


def test_client_object_wrapper_init_with_valid_object():
    """Test ClientObjectWrapper with a valid object."""
    mock_logger = MagicMock()
    with patch.object(base_module, "_LOGGER", mock_logger):
        mock_obj = MagicMock()
        wrapper = ClientObjectWrapper(mock_obj)

        assert wrapper.wrapped == mock_obj
        assert mock_logger.error.call_count == 0


def test_client_object_wrapper_init_with_none():
    """Test that ClientObjectWrapper raises ValueError when initialized with None."""
    mock_logger = MagicMock()
    with patch.object(base_module, "_LOGGER", mock_logger):
        with pytest.raises(ValueError, match="Cannot wrap None"):
            ClientObjectWrapper(None)

        mock_logger.error.assert_called_with(
            "ClientObjectWrapper constructor called with None"
        )


def test_client_object_wrapper_property():
    """Test the wrapped property returns the correct object."""
    mock_obj = MagicMock()
    mock_obj.special_attribute = "test_value"
    wrapper = ClientObjectWrapper(mock_obj)

    assert wrapper.wrapped == mock_obj
    assert wrapper.wrapped.special_attribute == "test_value"


def test_client_object_wrapper_type_preservation():
    """Test that ClientObjectWrapper preserves the type of the wrapped object."""

    class Dummy:
        pass

    dummy = Dummy()
    wrapper = ClientObjectWrapper(dummy)
    assert isinstance(wrapper.wrapped, Dummy)


def test_client_object_wrapper_multiple_instances():
    """Test that multiple ClientObjectWrapper instances operate independently."""
    obj1 = MagicMock()
    obj2 = MagicMock()
    wrapper1 = ClientObjectWrapper(obj1)
    wrapper2 = ClientObjectWrapper(obj2)
    assert wrapper1.wrapped is obj1
    assert wrapper2.wrapped is obj2
    assert wrapper1 is not wrapper2


def test_client_object_wrapper_property_is_readonly():
    """Test that the wrapped property is read-only and cannot be set."""
    obj = MagicMock()
    wrapper = ClientObjectWrapper(obj)
    with pytest.raises(AttributeError):
        wrapper.wrapped = obj
