"""
Tests for deephaven_mcp.mcp_systems_server._tools.shared.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow
import pytest
from conftest import MockContext

from deephaven_mcp._exceptions import InternalError, RegistryItemNotFoundError
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.mcp_systems_server._tools.shared import (
    _redact_recursive,
    build_table_data_response,
    check_response_size,
    error_response,
    format_initialization_status,
    format_meta_table_result,
    get_community_registry,
    get_config_manager,
    get_enterprise_registry,
    get_enterprise_session,
    get_registry_from_context,
    get_session_from_context,
    redact_json_sensitive_fields,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    EnterpriseSessionRegistry,
    InitializationPhase,
)

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
# get_config_manager tests
# ===========================================================================


def test_get_config_manager_returns_config_manager():
    """get_config_manager returns the config_manager from the lifespan context."""
    mock_config_manager = MagicMock()
    context = MockContext({"config_manager": mock_config_manager})
    assert get_config_manager(context) is mock_config_manager


# ===========================================================================
# get_community_registry tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_community_registry_returns_registry():
    """get_community_registry returns a CommunitySessionRegistry."""
    mock_registry = MagicMock(spec=CommunitySessionRegistry)
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )
    result = await get_community_registry(context)
    assert result is mock_registry


@pytest.mark.asyncio
async def test_get_community_registry_raises_on_wrong_type():
    """get_community_registry raises InternalError when registry is not a CommunitySessionRegistry."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )
    with pytest.raises(InternalError, match="CommunitySessionRegistry"):
        await get_community_registry(context)


# ===========================================================================
# get_enterprise_registry tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_enterprise_registry_returns_registry_and_applies_creds():
    """get_enterprise_registry returns the registry and applies the request creds."""
    from deephaven_mcp.auth.credentials import PasswordCredentials

    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.apply_credentials = AsyncMock()
    creds = PasswordCredentials(username="alice", password="pw")
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        },
        creds=creds,
    )
    result = await get_enterprise_registry(context)
    assert result is mock_registry
    mock_registry.apply_credentials.assert_awaited_once_with(creds)


@pytest.mark.asyncio
async def test_get_enterprise_registry_propagates_apply_credentials_rejection():
    """``shared.get_enterprise_registry`` propagates errors from ``apply_credentials``.

    The helper forwards the per-request credentials to the registry's
    :meth:`apply_credentials` via :func:`get_registry_from_context` and must
    propagate whatever exception that call raises, without re-implementing
    the rejection logic itself.
    """
    from deephaven_mcp.auth.credentials import PSKCredentials

    rejection = InternalError("Unsupported credentials type 'PSKCredentials'")
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.apply_credentials = AsyncMock(side_effect=rejection)
    creds = PSKCredentials(psk="x")
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        },
        creds=creds,
    )
    with pytest.raises(InternalError, match="Unsupported credentials type"):
        await get_enterprise_registry(context)
    mock_registry.apply_credentials.assert_awaited_once_with(creds)


@pytest.mark.asyncio
async def test_get_enterprise_registry_missing_creds_raises():
    """Missing scope[SCOPE_KEY_CREDENTIALS] raises InternalError."""
    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        },
        creds=None,
    )
    with pytest.raises(InternalError, match="Authenticated credentials are missing"):
        await get_enterprise_registry(context)


def test_get_request_credentials_rejects_non_credentials_scope_value():
    """Wrong type at scope[SCOPE_KEY_CREDENTIALS] raises InternalError.

    If the auth middleware (or a misconfigured test fixture) attaches an
    object that is not a :class:`Credentials` instance to the ASGI
    scope, the helper must raise :class:`InternalError` with a clear
    "wrong type" message rather than silently returning the bogus value
    to downstream callers.
    """
    from deephaven_mcp.mcp_systems_server._tools.shared import (
        _get_request_credentials,
    )

    # Pass a plain string in place of a Credentials instance.
    context = MockContext({}, creds="not-a-credentials-instance")
    with pytest.raises(InternalError, match="not a Credentials instance"):
        _get_request_credentials(context)


def test_get_request_credentials_no_request_raises():
    """The private credentials helper raises when context has no request."""
    from deephaven_mcp.mcp_systems_server._tools.shared import (
        _get_request_credentials,
    )

    class _NoReqRequestContext:
        request = None

    class _NoReqContext:
        request_context = _NoReqRequestContext()

    with pytest.raises(InternalError, match="no associated HTTP request"):
        _get_request_credentials(_NoReqContext())


@pytest.mark.asyncio
async def test_get_enterprise_registry_idempotent_re_apply():
    """Re-applying the same creds in subsequent calls is fine (idempotent)."""
    from deephaven_mcp.auth.credentials import PasswordCredentials

    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.apply_credentials = AsyncMock()
    creds = PasswordCredentials(username="alice", password="pw")
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        },
        creds=creds,
    )
    await get_enterprise_registry(context)
    await get_enterprise_registry(context)
    assert mock_registry.apply_credentials.await_count == 2


@pytest.mark.asyncio
async def test_get_enterprise_registry_raises_on_wrong_type():
    """get_enterprise_registry raises InternalError when registry is not an EnterpriseSessionRegistry."""
    mock_registry = MagicMock(spec=CommunitySessionRegistry)
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )
    with pytest.raises(InternalError, match="EnterpriseSessionRegistry"):
        await get_enterprise_registry(context)


# ===========================================================================
# get_registry_from_context tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_registry_from_context_returns_registry():
    """get_registry_from_context returns the registry from the lifespan context."""
    mock_registry = MagicMock()
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )
    result = await get_registry_from_context(context)
    assert result is mock_registry


@pytest.mark.asyncio
async def test_get_registry_from_context_enterprise_applies_credentials():
    """For enterprise registries, get_registry_from_context applies credentials."""
    from deephaven_mcp.auth.credentials import PasswordCredentials

    mock_registry = MagicMock(spec=EnterpriseSessionRegistry)
    mock_registry.apply_credentials = AsyncMock()
    creds = PasswordCredentials(username="alice", password="pw")
    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        },
        creds=creds,
    )
    result = await get_registry_from_context(context)
    assert result is mock_registry
    mock_registry.apply_credentials.assert_awaited_once_with(creds)


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

    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )

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

    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )

    with pytest.raises(RegistryItemNotFoundError, match="No item with name"):
        await get_session_from_context("test_function", context, "nonexistent:session")

    mock_registry.get.assert_called_once_with("nonexistent:session")


@pytest.mark.asyncio
async def test_get_session_from_context_keyerror_still_propagates():
    """Test get_session_from_context still propagates KeyError."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )

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

    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )

    with pytest.raises(Exception, match="Failed to establish connection"):
        await get_session_from_context("test_function", context, "test:session:id")

    mock_registry.get.assert_called_once_with("test:session:id")
    mock_session_manager.get.assert_called_once()


# ===========================================================================
# error_response tests
# ===========================================================================


def test_error_response_structure():
    result = error_response("something went wrong")
    assert result == {
        "success": False,
        "error": "something went wrong",
        "isError": True,
    }


def test_error_response_success_is_false():
    assert error_response("x")["success"] is False


def test_error_response_is_error_is_true():
    assert error_response("x")["isError"] is True


def test_error_response_preserves_message():
    msg = "Session 'foo' not found"
    assert error_response(msg)["error"] == msg


def test_error_response_empty_message():
    result = error_response("")
    assert result["error"] == ""
    assert result["success"] is False
    assert result["isError"] is True


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

    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )

    session, error = await get_enterprise_session(
        "test_function", context, "test-session-id"
    )

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

    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )

    session, error = await get_enterprise_session(
        "test_function", context, "test-session-id"
    )

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

    context = MockContext(
        {
            "registry": mock_registry,
            "config_manager": MagicMock(),
        }
    )

    session, error = await get_enterprise_session(
        "test_function", context, "test-session-id"
    )

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
    result = format_meta_table_result(
        arrow_table, "daily_prices", namespace="market_data"
    )

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
# build_table_data_response tests
# ===========================================================================


def _make_data_arrow_table() -> pyarrow.Table:
    """Small arrow table with two typed columns for data-response tests."""
    return pyarrow.table(
        {
            "id": pyarrow.array([1, 2, 3], type=pyarrow.int64()),
            "name": pyarrow.array(["a", "b", "c"], type=pyarrow.string()),
        }
    )


def test_build_table_data_response_schema_and_rowcount():
    """Response includes typed schema and accurate row_count."""
    arrow_table = _make_data_arrow_table()
    result = build_table_data_response(arrow_table, is_complete=True, format="json-row")

    assert result["success"] is True
    assert result["is_complete"] is True
    assert result["row_count"] == 3
    assert result["schema"] == [
        {"name": "id", "type": "int64"},
        {"name": "name", "type": "string"},
    ]
    # Actual format is resolved by format_table_data and echoed back.
    assert isinstance(result["format"], str)
    assert "data" in result


def test_build_table_data_response_is_complete_false_preserved():
    """is_complete=False flows through unchanged."""
    arrow_table = _make_data_arrow_table()
    result = build_table_data_response(
        arrow_table, is_complete=False, format="json-row"
    )
    assert result["is_complete"] is False


def test_build_table_data_response_omits_optional_fields_by_default():
    """Without table_name/namespace, neither key appears in the response."""
    arrow_table = _make_data_arrow_table()
    result = build_table_data_response(arrow_table, is_complete=True, format="json-row")
    assert "table_name" not in result
    assert "namespace" not in result


def test_build_table_data_response_includes_table_name_when_provided():
    """table_name is echoed into the response when passed."""
    arrow_table = _make_data_arrow_table()
    result = build_table_data_response(
        arrow_table, is_complete=True, format="json-row", table_name="prices"
    )
    assert result["table_name"] == "prices"
    assert "namespace" not in result


def test_build_table_data_response_includes_namespace_when_provided():
    """namespace is echoed into the response (catalog-table path) when passed."""
    arrow_table = _make_data_arrow_table()
    result = build_table_data_response(
        arrow_table,
        is_complete=True,
        format="json-row",
        table_name="prices",
        namespace="market_data",
    )
    assert result["namespace"] == "market_data"
    assert result["table_name"] == "prices"


def test_build_table_data_response_passes_format_to_formatter():
    """The requested format is forwarded to format_table_data as format_type=."""
    arrow_table = _make_data_arrow_table()
    with patch(
        "deephaven_mcp.mcp_systems_server._tools.shared.format_table_data",
        return_value=("csv", "id,name\n1,a\n"),
    ) as mock_fmt:
        result = build_table_data_response(arrow_table, is_complete=True, format="csv")
    mock_fmt.assert_called_once()
    assert mock_fmt.call_args.kwargs == {"format_type": "csv"}
    assert result["format"] == "csv"
    assert result["data"] == "id,name\n1,a\n"


def test_build_table_data_response_empty_table():
    """Empty arrow table yields row_count=0 and an empty schema is still typed."""
    empty = pyarrow.table({"x": pyarrow.array([], type=pyarrow.int32())})
    result = build_table_data_response(empty, is_complete=True, format="json-row")
    assert result["row_count"] == 0
    assert result["schema"] == [{"name": "x", "type": "int32"}]


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
    assert result == [
        {"token": "[REDACTED]", "id": 1},
        {"token": "[REDACTED]", "id": 2},
    ]


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


@pytest.mark.parametrize(
    "key", ["password", "passwd", "token", "secret", "api_key", "apikey", "api_secret"]
)
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
    result = redact_json_sensitive_fields("not valid json {{")
    assert result == "[UNPARSEABLE]"


def test_redact_json_sensitive_fields_invalid_json_with_sensitive_content():
    result = redact_json_sensitive_fields("not-json password=hunter2 token=abc123")
    assert result == "[UNPARSEABLE]"


def test_redact_json_sensitive_fields_mixed_keys():
    import json

    data = {"username": "admin", "password": "hunter2", "database": "prod"}
    result = redact_json_sensitive_fields(json.dumps(data))
    parsed = json.loads(result)
    assert parsed["password"] == "[REDACTED]"
    assert parsed["username"] == "admin"
    assert parsed["database"] == "prod"
