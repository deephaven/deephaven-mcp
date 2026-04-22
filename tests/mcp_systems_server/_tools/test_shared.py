"""
Tests for deephaven_mcp.mcp_systems_server._tools.shared.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow
import pytest
from conftest import MockContext

from deephaven_mcp._exceptions import RegistryItemNotFoundError
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.mcp_systems_server._tools.shared import (
    check_response_size,
    format_initialization_status,
    format_meta_table_result,
    get_enterprise_session,
    get_session_from_context,
    redact_json_sensitive_fields,
    _redact_recursive,
)
from deephaven_mcp.resource_manager import InitializationPhase

# ===========================================================================
# format_initialization_status tests
# ===========================================================================


def test_format_initialization_status_completed_no_errors():
    """COMPLETED phase with no errors returns None."""
    assert format_initialization_status(InitializationPhase.COMPLETED, {}) is None


def test_format_initialization_status_completed_with_errors():
    """COMPLETED phase with errors reports connection issues."""
    errors = {"prod": "timeout"}
    result = format_initialization_status(InitializationPhase.COMPLETED, errors)
    assert result is not None
    assert "connection issues" in result["status"]
    assert result["errors"] == errors


def test_format_initialization_status_not_started():
    """NOT_STARTED phase reports discovery has not yet started."""
    result = format_initialization_status(InitializationPhase.NOT_STARTED, {})
    assert result is not None
    assert "not yet started" in result["status"]
    assert "errors" not in result


def test_format_initialization_status_partial():
    """PARTIAL phase reports discovery has not yet started."""
    result = format_initialization_status(InitializationPhase.PARTIAL, {})
    assert result is not None
    assert "not yet started" in result["status"]
    assert "errors" not in result


def test_format_initialization_status_loading():
    """LOADING phase reports discovery is actively running."""
    result = format_initialization_status(InitializationPhase.LOADING, {})
    assert result is not None
    assert "actively running" in result["status"]
    assert "errors" not in result


def test_format_initialization_status_failed():
    """FAILED phase reports critical failure, not in-progress."""
    result = format_initialization_status(InitializationPhase.FAILED, {})
    assert result is not None
    assert "failed critically" in result["status"]
    assert "in progress" not in result["status"]
    assert "errors" not in result


def test_format_initialization_status_failed_with_errors():
    """FAILED phase with errors includes both status and errors."""
    errors = {"sys": "cancelled"}
    result = format_initialization_status(InitializationPhase.FAILED, errors)
    assert result is not None
    assert "failed critically" in result["status"]
    assert result["errors"] == errors


def test_format_initialization_status_loading_with_errors():
    """LOADING phase with errors includes both status and errors."""
    errors = {"sys": "partial failure"}
    result = format_initialization_status(InitializationPhase.LOADING, errors)
    assert result is not None
    assert "actively running" in result["status"]
    assert result["errors"] == errors


# ===========================================================================
# get_session_from_context tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_session_from_context_success():
    """Test get_session_from_context successfully retrieves a session."""
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    result = await get_session_from_context("test_function", context, "test:session:id")

    assert result is mock_session
    mock_registry.get.assert_called_once_with("test:session:id")
    mock_session_manager.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_session_from_context_session_not_found():
    """Test get_session_from_context propagates RegistryItemNotFoundError from registry."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(
        side_effect=RegistryItemNotFoundError(
            "No item with name 'nonexistent:session' found"
        )
    )

    context = MockContext({"session_registry": mock_registry})

    with pytest.raises(RegistryItemNotFoundError, match="No item with name"):
        await get_session_from_context("test_function", context, "nonexistent:session")

    mock_registry.get.assert_called_once_with("nonexistent:session")


@pytest.mark.asyncio
async def test_get_session_from_context_keyerror_still_propagates():
    """Test get_session_from_context still propagates KeyError."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    with pytest.raises(KeyError, match="Session not found"):
        await get_session_from_context("test_function", context, "nonexistent:session")


@pytest.mark.asyncio
async def test_get_session_from_context_session_connection_fails():
    """Test get_session_from_context propagates exception when session.get() fails."""
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(
        side_effect=Exception("Failed to establish connection")
    )
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with pytest.raises(Exception, match="Failed to establish connection"):
        await get_session_from_context("test_function", context, "test:session:id")

    mock_registry.get.assert_called_once_with("test:session:id")
    mock_session_manager.get.assert_called_once()


# ===========================================================================
# check_response_size tests
# ===========================================================================


def test_check_response_size_acceptable():
    """Test check_response_size with acceptable size."""
    result = check_response_size("test_table", 1000000)  # 1MB
    assert result is None


def test_check_response_size_warning_threshold():
    """Test check_response_size with size above warning threshold."""
    with patch("deephaven_mcp.mcp_systems_server._tools.shared._LOGGER") as mock_logger:
        result = check_response_size("test_table", 10000000)  # 10MB
        assert result is None
        mock_logger.warning.assert_called_once()
        assert "Large response (~10.0MB)" in mock_logger.warning.call_args[0][0]


def test_check_response_size_over_limit():
    """Test check_response_size with size over maximum limit."""
    result = check_response_size("test_table", 60000000)  # 60MB
    assert result == {
        "success": False,
        "error": "Response would be ~60.0MB (max 50MB). Please reduce max_rows.",
        "isError": True,
    }


# ===========================================================================
# get_enterprise_session tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_enterprise_session_success():
    """Test get_enterprise_session with a valid CorePlusSession."""
    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    session, error = await get_enterprise_session("test_function", context, "test-session-id")

    assert session is mock_session
    assert error is None


@pytest.mark.asyncio
async def test_get_enterprise_session_not_enterprise():
    """Test get_enterprise_session with a non-enterprise session."""
    mock_session = MagicMock(spec=BaseSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    session, error = await get_enterprise_session("test_function", context, "test-session-id")

    assert session is None
    assert error is not None
    assert error["success"] is False
    assert "test_function only works with enterprise (Core+) sessions" in error["error"]
    assert "test-session-id" in error["error"]
    assert error["isError"] is True


@pytest.mark.asyncio
async def test_get_enterprise_session_exception():
    """Test get_enterprise_session returns error dict when session retrieval raises."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("connection refused"))

    context = MockContext({"session_registry": mock_registry})

    session, error = await get_enterprise_session("test_function", context, "test-session-id")

    assert session is None
    assert error is not None
    assert error["success"] is False
    assert "connection refused" in error["error"]
    assert error["isError"] is True


# ===========================================================================
# format_meta_table_result tests
# ===========================================================================


def _make_arrow_table():
    """Build a small pyarrow table that mimics a Deephaven meta table."""
    return pyarrow.table(
        {
            "Name": ["Date", "Price"],
            "DataType": ["LocalDate", "double"],
            "IsPartitioning": [False, False],
        }
    )


def test_format_meta_table_result_without_namespace():
    """Without namespace the result has no 'namespace' key."""
    arrow_table = _make_arrow_table()
    result = format_meta_table_result(arrow_table, "daily_prices")

    assert result["success"] is True
    assert result["table"] == "daily_prices"
    assert result["format"] == "json-row"
    assert result["row_count"] == 2
    assert len(result["data"]) == 2
    assert result["data"][0]["Name"] == "Date"
    assert result["data"][1]["Name"] == "Price"
    assert len(result["meta_columns"]) == 3
    assert "namespace" not in result


def test_format_meta_table_result_with_namespace():
    """With namespace the result includes the 'namespace' key."""
    arrow_table = _make_arrow_table()
    result = format_meta_table_result(arrow_table, "daily_prices", namespace="market_data")

    assert result["success"] is True
    assert result["namespace"] == "market_data"
    assert result["table"] == "daily_prices"


def test_format_meta_table_result_meta_columns_schema():
    """meta_columns reflects the schema of the arrow table itself."""
    arrow_table = _make_arrow_table()
    result = format_meta_table_result(arrow_table, "t")

    col_names = [c["name"] for c in result["meta_columns"]]
    assert col_names == ["Name", "DataType", "IsPartitioning"]


# ===========================================================================
# _redact_recursive tests (internal helper, keeps underscore)
# ===========================================================================


def test_redact_recursive_scalar_string():
    assert _redact_recursive("plain") == "plain"


def test_redact_recursive_scalar_int():
    assert _redact_recursive(42) == 42


def test_redact_recursive_scalar_none():
    assert _redact_recursive(None) is None


def test_redact_recursive_empty_dict():
    assert _redact_recursive({}) == {}


def test_redact_recursive_empty_list():
    assert _redact_recursive([]) == []


def test_redact_recursive_dict_sensitive_key():
    result = _redact_recursive({"password": "hunter2", "host": "db.local"})
    assert result == {"password": "[REDACTED]", "host": "db.local"}


def test_redact_recursive_dict_non_sensitive_key():
    data = {"host": "db.local", "port": 5432}
    assert _redact_recursive(data) == data


def test_redact_recursive_nested_dict():
    data = {"jdbc": {"password": "secret", "driver": "com.mysql.Driver"}}
    result = _redact_recursive(data)
    assert result == {"jdbc": {"password": "[REDACTED]", "driver": "com.mysql.Driver"}}


def test_redact_recursive_list_of_scalars():
    data = ["a", 1, None]
    assert _redact_recursive(data) == data


def test_redact_recursive_list_of_dicts():
    data = [{"token": "abc", "id": 1}, {"token": "xyz", "id": 2}]
    result = _redact_recursive(data)
    assert result == [{"token": "[REDACTED]", "id": 1}, {"token": "[REDACTED]", "id": 2}]


# ===========================================================================
# redact_json_sensitive_fields tests
# ===========================================================================


def test_redact_json_sensitive_fields_none():
    assert redact_json_sensitive_fields(None) is None


def test_redact_json_sensitive_fields_empty_string():
    assert redact_json_sensitive_fields("") is None


def test_redact_json_sensitive_fields_no_sensitive_keys():
    import json
    data = {"host": "localhost", "port": 5432, "database": "testdb"}
    result = redact_json_sensitive_fields(json.dumps(data))
    assert json.loads(result) == data


@pytest.mark.parametrize("key", ["password", "passwd", "token", "secret", "api_key", "apikey", "api_secret"])
def test_redact_json_sensitive_fields_each_key(key):
    import json
    data = {key: "supersensitive", "other": "keep"}
    result = redact_json_sensitive_fields(json.dumps(data))
    parsed = json.loads(result)
    assert parsed[key] == "[REDACTED]"
    assert parsed["other"] == "keep"


def test_redact_json_sensitive_fields_nested():
    import json
    data = {"jdbcConfig": {"password": "secret123", "host": "db.example.com"}}
    result = redact_json_sensitive_fields(json.dumps(data))
    parsed = json.loads(result)
    assert parsed["jdbcConfig"]["password"] == "[REDACTED]"
    assert parsed["jdbcConfig"]["host"] == "db.example.com"


def test_redact_json_sensitive_fields_array_of_dicts():
    import json
    data = [{"token": "abc123", "id": 1}, {"token": "xyz789", "id": 2}]
    result = redact_json_sensitive_fields(json.dumps(data))
    parsed = json.loads(result)
    assert parsed[0]["token"] == "[REDACTED]"
    assert parsed[0]["id"] == 1
    assert parsed[1]["token"] == "[REDACTED]"
    assert parsed[1]["id"] == 2


def test_redact_json_sensitive_fields_invalid_json():
    raw = "not valid json {{"
    result = redact_json_sensitive_fields(raw)
    assert result == raw


def test_redact_json_sensitive_fields_mixed_keys():
    import json
    data = {"username": "admin", "password": "hunter2", "database": "prod"}
    result = redact_json_sensitive_fields(json.dumps(data))
    parsed = json.loads(result)
    assert parsed["password"] == "[REDACTED]"
    assert parsed["username"] == "admin"
    assert parsed["database"] == "prod"
