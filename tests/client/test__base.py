"""Unit tests for deephaven_mcp.client._base module.

These tests directly exercise the ClientObjectWrapper class from the _base module.
"""

from unittest.mock import MagicMock, patch

import grpc
import pytest

import deephaven_mcp.client._base as base_module
from deephaven_mcp.client._base import ClientObjectWrapper, describe_exception_chain


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


# ---------------------------------------------------------------------------
# describe_exception_chain
# ---------------------------------------------------------------------------


class _FakeGrpcCall(grpc.RpcError, grpc.Call):
    """Minimal real ``grpc.Call``/``grpc.RpcError`` double for chain tests."""

    def __init__(self, code, details):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details

    def initial_metadata(self):
        return ()

    def trailing_metadata(self):
        return ()

    def is_active(self):
        return False

    def time_remaining(self):
        return None

    def cancel(self):
        return False

    def add_callback(self, _callback):
        return False


def test_describe_exception_chain_plain():
    assert describe_exception_chain(ValueError("boom")) == "boom"


def test_describe_exception_chain_grpc_detail():
    grpc_err = _FakeGrpcCall(
        grpc.StatusCode.INVALID_ARGUMENT, "Column Foo has unsupported type"
    )
    try:
        try:
            raise grpc_err
        except grpc.RpcError as cause:
            raise Exception("failed to finish FetchTableOp operation") from cause
    except Exception as exc:
        message = describe_exception_chain(exc)

    assert message == (
        "failed to finish FetchTableOp operation -> "
        "gRPC INVALID_ARGUMENT: Column Foo has unsupported type"
    )


def test_describe_exception_chain_skips_duplicate_substring():
    inner = ValueError("root detail")
    outer = RuntimeError("wrapper: root detail")
    outer.__cause__ = inner

    assert describe_exception_chain(outer) == "wrapper: root detail"


def test_describe_exception_chain_handles_cycle():
    first = ValueError("a")
    second = ValueError("b")
    first.__cause__ = second
    second.__cause__ = first

    assert describe_exception_chain(first) == "a -> b"


def test_describe_exception_chain_grpc_code_none():
    grpc_err = _FakeGrpcCall(None, "no code available")

    assert describe_exception_chain(grpc_err) == "gRPC UNKNOWN: no code available"
