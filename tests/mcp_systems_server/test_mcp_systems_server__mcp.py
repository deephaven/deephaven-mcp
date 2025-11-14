"""
Tests for the deephaven_mcp.mcp_systems_server server and tools.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

import deephaven_mcp.mcp_systems_server._mcp as mcp_mod
from deephaven_mcp import config
from deephaven_mcp.mcp_systems_server._mcp import (
    _check_session_id_available,
    _check_session_limits,
    _generate_session_name_if_none,
    _get_system_config,
    _normalize_auth_type,
    _resolve_community_session_parameters,
    _resolve_session_parameters,
)
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)


class MockRequestContext:
    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context


class MockContext:
    def __init__(self, lifespan_context):
        self.request_context = MockRequestContext(lifespan_context)


def create_mock_instance_tracker():
    """Create a mock InstanceTracker for tests."""
    mock_tracker = MagicMock()
    mock_tracker.instance_id = "test-instance-id"
    mock_tracker.track_python_process = AsyncMock()
    mock_tracker.untrack_python_process = AsyncMock()
    return mock_tracker


# ===== Test Helper Functions =====


def create_mock_arrow_meta_table(
    schema_data: list[dict], schema_fields: list | None = None
) -> MagicMock:
    """
    Create a mock PyArrow meta table for testing schema operations.

    This helper eliminates duplication across schema-related tests by providing
    a consistent way to create mock meta tables that match the behavior expected
    by _format_meta_table_result().

    Args:
        schema_data: List of dicts representing metadata rows (e.g., column info)
        schema_fields: Optional list of mock field objects for the schema.
                      If None, creates default Name/DataType fields.

    Returns:
        MagicMock: A mock PyArrow Table with to_pylist(), __len__(), and schema
    """
    mock_arrow_meta = MagicMock()
    mock_arrow_meta.to_pylist = MagicMock(return_value=schema_data)
    mock_arrow_meta.__len__ = MagicMock(return_value=len(schema_data))

    if schema_fields:
        mock_arrow_meta.schema = schema_fields
    else:
        # Default schema fields that match typical Deephaven meta tables
        field1 = MagicMock()
        field1.name = "Name"
        field1.type = "string"

        field2 = MagicMock()
        field2.name = "DataType"
        field2.type = "string"

        mock_arrow_meta.schema = [field1, field2]

    return mock_arrow_meta


def create_mock_catalog_schema_function(schema_data_map, error_tables=None):
    """
    Create a mock function for queries.get_catalog_meta_table that returns different
    schemas based on table name.

    This helper eliminates duplication in catalog schema tests by providing a
    flexible way to mock different table schemas and error conditions.

    Args:
        schema_data_map: Dict mapping table_name -> schema_data (list of dicts)
        error_tables: Optional set of table names that should raise exceptions

    Returns:
        Callable: A function that can be used as side_effect for get_catalog_meta_table
    """
    error_tables = error_tables or set()

    def mock_get_catalog_meta_table(session, namespace, table_name):
        if table_name in error_tables:
            raise Exception(f"Table '{table_name}' not found in catalog")

        schema_data = schema_data_map.get(
            table_name, [{"Name": "Col1", "DataType": "int"}]  # Default schema
        )
        return create_mock_arrow_meta_table(schema_data)

    return mock_get_catalog_meta_table


# === Helper Function Tests ===


@pytest.mark.asyncio
async def test_get_session_from_context_success():
    """Test _get_session_from_context successfully retrieves a session."""
    from deephaven_mcp.mcp_systems_server._mcp import _get_session_from_context

    # Create mock session
    mock_session = MagicMock()

    # Create mock session manager
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    # Create mock session registry
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    # Create context with registry
    context = MockContext({"session_registry": mock_registry})

    # Call the helper
    result = await _get_session_from_context(
        "test_function", context, "test:session:id"
    )

    # Verify the session was returned
    assert result is mock_session

    # Verify the registry was accessed correctly
    mock_registry.get.assert_called_once_with("test:session:id")
    mock_session_manager.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_session_from_context_session_not_found():
    """Test _get_session_from_context raises KeyError when session not found."""
    from deephaven_mcp.mcp_systems_server._mcp import _get_session_from_context

    # Create mock session registry that raises KeyError
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    # Create context with registry
    context = MockContext({"session_registry": mock_registry})

    # Call the helper and expect KeyError
    with pytest.raises(KeyError, match="Session not found"):
        await _get_session_from_context("test_function", context, "nonexistent:session")

    # Verify the registry was accessed
    mock_registry.get.assert_called_once_with("nonexistent:session")


@pytest.mark.asyncio
async def test_get_session_from_context_session_connection_fails():
    """Test _get_session_from_context propagates exception when session.get() fails."""
    from deephaven_mcp.mcp_systems_server._mcp import _get_session_from_context

    # Create mock session manager that fails on get()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(
        side_effect=Exception("Failed to establish connection")
    )

    # Create mock session registry
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    # Create context with registry
    context = MockContext({"session_registry": mock_registry})

    # Call the helper and expect Exception
    with pytest.raises(Exception, match="Failed to establish connection"):
        await _get_session_from_context("test_function", context, "test:session:id")

    # Verify both registry and manager were accessed
    mock_registry.get.assert_called_once_with("test:session:id")
    mock_session_manager.get.assert_called_once()


# === refresh ===


def test_run_script_reads_script_from_file():
    mock_session = MagicMock()
    mock_session.run_script = AsyncMock()
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)
    mock_registry = AsyncMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext(
        {
            "session_registry": mock_registry,
            "config_manager": AsyncMock(),
        }
    )

    file_content = "print('hello')"

    class DummyFile:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def read(self):
            return file_content

    with patch("aiofiles.open", return_value=DummyFile()):
        result = asyncio.run(
            mcp_mod.session_script_run(
                context, session_id="test_worker", script=None, script_path="dummy.py"
            )
        )
        assert result["success"] is True
        mock_session.run_script.assert_called_once_with(file_content)


@pytest.mark.asyncio
async def test_mcp_reload_missing_context_keys():
    # context missing session_registry
    config_manager = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    context = MockContext(
        {"config_manager": config_manager, "refresh_lock": refresh_lock}
    )
    result = await mcp_mod.mcp_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "session_registry" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_lock_error():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(side_effect=Exception("lock error"))
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.mcp_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "lock error" in result["error"]


# Suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket")
@pytest.mark.asyncio
async def test_mcp_reload_success():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock()
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.mcp_reload(context)
    assert result == {"success": True}
    config_manager.clear_config_cache.assert_awaited_once()
    session_registry.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_details_logs_version_info():
    """Test that session_details logs programming language and Deephaven versions when available."""
    # Import the function
    import enum

    from deephaven_mcp.mcp_systems_server._mcp import session_details
    from deephaven_mcp.resource_manager._manager import ResourceLivenessStatus
    from deephaven_mcp.resource_manager._registry_combined import (
        CombinedSessionRegistry,
    )

    # Create mocks
    context = MagicMock()
    session_id = "test-session"
    session = AsyncMock()

    # Setup session registry and session manager
    session_registry = MagicMock(spec=CombinedSessionRegistry)
    mgr = AsyncMock()

    # Configure session manager with required properties
    mgr.is_alive = AsyncMock(return_value=True)
    mgr.system_type = MagicMock()
    mgr.system_type.name = "COMMUNITY"
    mgr.source = "test-source"
    mgr.name = "test"

    # Mock liveness status
    status_mock = MagicMock(spec=enum.Enum)
    status_mock.name = "ONLINE"
    mgr.liveness_status = AsyncMock(return_value=(status_mock, ""))

    # Configure the session object with programming_language
    session.programming_language = "python"

    # Setup mgr.get to return our session
    mgr.get = AsyncMock(return_value=session)

    # Configure session registry to return our manager
    session_registry.get = AsyncMock(return_value=mgr)

    # Setup context.request_context.lifespan_context properly
    request_context = MagicMock()
    request_context.lifespan_context = {"session_registry": session_registry}
    context.request_context = request_context

    # Mock the queries module to return version information
    mock_queries = MagicMock()
    mock_queries.get_programming_language_version = AsyncMock(return_value="3.9.7")
    mock_queries.get_dh_versions = AsyncMock(return_value=("0.24.0", None))

    # Use a logger mock to verify debug logs
    mock_logger = MagicMock()

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries",
            mock_queries,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp._LOGGER",
            mock_logger,
        ),
    ):
        # Call the function
        result = await session_details(context, session_id, attempt_to_connect=True)

        # Verify the function returned successfully
        assert result["success"] is True
        assert "session" in result
        assert result["session"]["programming_language"] == "python"
        assert result["session"]["programming_language_version"] == "3.9.7"
        assert result["session"]["deephaven_community_version"] == "0.24.0"

        # Verify that the debug log messages were called (lines 447 and 458)
        mock_logger.debug.assert_any_call(
            f"[mcp_systems_server:session_details] Session '{session_id}' programming_language_version: 3.9.7"
        )
        mock_logger.debug.assert_any_call(
            f"[mcp_systems_server:session_details] Session '{session_id}' versions: community=0.24.0, enterprise=None"
        )


# === Additional coverage tests for create/delete enterprise session ===


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_name_no_username_and_language_transformer():
    """Covers auto-generated name without username (mcp-worker-...), language transformer execution, and creation_function."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "no-user-system": {
            "connection_json_url": "https://example.com/iris/connection.json",
            "auth_type": "password",
            # Intentionally omit 'username' to exercise the no-username branch
            "password": "pass",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "heap_size_gb": 2.0,
                    "auto_delete_timeout": 600,
                    "server": "server-east-1",
                    "engine": "DeephavenCommunity",
                },
            },
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    with patch("deephaven_mcp.mcp_systems_server._mcp.datetime") as mock_datetime:
        mock_datetime.now().strftime.return_value = "20241126-1500"

        # Enterprise factory chain
        mock_enterprise_registry = MagicMock()
        mock_factory_manager = MagicMock()
        mock_factory = MagicMock()
        mock_session = MagicMock()
        mock_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
        mock_factory_manager.get = AsyncMock(return_value=mock_factory)
        # Set up the factory mock to capture configuration_transformer calls
        captured_config_transformer = None

        def capture_transformer(*args, **kwargs):
            nonlocal captured_config_transformer
            captured_config_transformer = kwargs.get("configuration_transformer")
            return mock_session

        mock_factory.connect_to_new_worker = AsyncMock(side_effect=capture_transformer)

        # Mock the session registry operations
        mock_registry.get = AsyncMock(side_effect=KeyError("Session not found"))
        mock_registry.add_session = AsyncMock()
        mock_registry.count_added_sessions = AsyncMock(return_value=0)

        context = MockContext(
            {"config_manager": mock_config_manager, "session_registry": mock_registry}
        )

        # Use a non-Python programming language to exercise configuration_transformer
        result = await mcp_mod.session_enterprise_create(
            context,
            "no-user-system",
            None,
            programming_language="Groovy",
        )

        assert result["success"] is True
        # Name should be generated without username prefix
        assert result["session_name"] == "mcp-session-20241126-1500"

        # Verify the factory was called with a configuration_transformer for non-Python language
        mock_factory.connect_to_new_worker.assert_called_once()
        assert captured_config_transformer is not None

        # Test the language transformer to cover lines 1669-1670
        mock_config = MagicMock()
        result_config = captured_config_transformer(mock_config)
        assert result_config is mock_config
        assert mock_config.scriptLanguage == "Groovy"

        # Verify session was added using add_session method - check the call was made
        session_id = f"enterprise:no-user-system:mcp-session-20241126-1500"
        mock_registry.add_session.assert_called_once()
        call_args = mock_registry.add_session.call_args
        session_manager = call_args[0][0]  # First (and only) argument is the manager
        assert session_manager.full_name == session_id
        returned_session = await session_manager._creation_function(
            "no-user-system", "mcp-session-20241126-1500"
        )
        assert returned_session is mock_session


@pytest.mark.asyncio
async def test_session_enterprise_delete_removal_missing_in_registry():
    """Covers branch where pop returns None (lines 1959-1960)."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=mcp_mod.EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys": {"session_creation": {"max_concurrent_sessions": 5}}}

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    # Mock remove_session to return None (simulating session not found in registry)
    mock_registry.remove_session = AsyncMock(return_value=None)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await mcp_mod.session_enterprise_delete(context, "sys", "s1")

    assert result["success"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_cleanup_created_sessions_empty():
    """Test session removal - session tracking now handled by registry automatically."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=mcp_mod.EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys2": {"session_creation": {"max_concurrent_sessions": 5}}}

    # Mock remove_session to return the manager (simulating successful removal)
    full_id = "enterprise:sys2:solo"
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await mcp_mod.session_enterprise_delete(context, "sys2", "solo")

    assert result["success"] is True
    # Session tracking is now handled internally by the registry


@pytest.mark.asyncio
async def test_session_enterprise_delete_registry_pop_raises_error():
    """Covers error path on removal (lines 1973-1977)."""

    class BadItems:
        def pop(self, *args, **kwargs):
            raise RuntimeError("pop failed")

    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=mcp_mod.EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys3": {"session_creation": {"max_concurrent_sessions": 5}}}
    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove_session = AsyncMock(
        side_effect=Exception("Simulated registry error")
    )

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await mcp_mod.session_enterprise_delete(context, "sys3", "s3")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Failed to remove session" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_delete_outer_exception_logger_info_raises():
    """Force outer exception handler (lines 1991-1998) by making _LOGGER.info raise."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()
    mock_session_manager = MagicMock(spec=mcp_mod.EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    enterprise_config = {"sys4": {"session_creation": {"max_concurrent_sessions": 5}}}
    full_id = "enterprise:sys4:s4"
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    # Only raise on the second info() call (the first is before the try block)
    call_counter = {"n": 0}

    def info_side_effect(*args, **kwargs):
        call_counter["n"] += 1
        if call_counter["n"] == 2:
            raise Exception("log fail")
        return None

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    with patch(
        "deephaven_mcp.mcp_systems_server._mcp._LOGGER.info",
        side_effect=info_side_effect,
    ):
        result = await mcp_mod.session_enterprise_delete(context, "sys4", "s4")

    assert result["success"] is False
    assert result["isError"] is True
    assert "log fail" in result["error"]


@pytest.mark.asyncio
async def test_mcp_reload_failure():
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()
    refresh_lock.__aenter__ = AsyncMock(return_value=None)
    refresh_lock.__aexit__ = AsyncMock(return_value=None)
    config_manager.clear_config_cache = AsyncMock(side_effect=RuntimeError("fail"))
    session_registry.close = AsyncMock()
    context = MockContext(
        {
            "config_manager": config_manager,
            "session_registry": session_registry,
            "refresh_lock": refresh_lock,
        }
    )
    result = await mcp_mod.mcp_reload(context)
    assert result["success"] is False
    assert result["isError"] is True
    assert "fail" in result["error"]


# === table_schemas ===


@pytest.mark.asyncio
async def test_session_tables_schema_empty_table_names():
    session_registry = AsyncMock()

    class DummySession:
        tables = ["t1"]

        def open_table(self, name):
            class MetaTable:
                def to_arrow(self):
                    class Arrow:
                        def to_pylist(self):
                            return [{"Name": name, "DataType": "int"}]

                    return Arrow()

            class Table:
                meta_table = MetaTable()

            return Table()

    # Mock session manager behavior
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.session_tables_schema(
        context, session_id="worker", table_names=[]
    )
    assert isinstance(res, dict)
    assert res == {"success": True, "schemas": [], "count": 0}


@pytest.mark.asyncio
async def test_session_tables_schema_interface_contract():
    """Ensure session.tables() is properly mocked as an async method to match real interface"""

    # This test validates that our mocks match the real session interface
    class ValidSession:
        async def tables(self):
            return ["test_table"]

    # Verify the mock has the correct async interface
    session = ValidSession()
    assert callable(session.tables)
    assert asyncio.iscoroutinefunction(session.tables)

    # Test that it works in the actual function
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    # Mock the queries.get_meta_table to avoid complexity
    class MockArrowTable:
        def to_pylist(self):
            return [{"Name": "test_table", "DataType": "string"}]

    mock_get_meta_table = AsyncMock(return_value=MockArrowTable())

    context = MockContext({"session_registry": session_registry})

    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        result = await mcp_mod.session_tables_schema(
            context, session_id="worker", table_names=None
        )

    assert result["success"] is True
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["table"] == "test_table"


@pytest.mark.asyncio
async def test_session_tables_schema_would_catch_original_bug():
    """Test that validates the original bug pattern would be caught"""

    # This simulates what the original buggy code was trying to do
    class BuggySession:
        # Simulating the broken pattern: tables as property instead of method
        tables = ["table1", "table2"]

        def open_table(self, name):
            pass

    buggy_session = BuggySession()

    # This should demonstrate the original error pattern
    with pytest.raises(TypeError, match="'list' object is not callable"):
        # This is what the original buggy code was effectively doing
        await buggy_session.tables()  # This should fail since tables is a list, not callable

    # Also verify the list() pattern that was in the original bug
    # This would work (incorrectly) with the old buggy approach
    assert list(buggy_session.tables) == ["table1", "table2"]

    # But this is what we need (and what our fix does)
    class CorrectSession:
        async def tables(self):
            return ["table1", "table2"]

    correct_session = CorrectSession()
    result = await correct_session.tables()
    assert result == ["table1", "table2"]


@pytest.mark.asyncio
async def test_session_tables_schema_no_tables():
    session_registry = AsyncMock()

    class DummySession:
        async def tables(self):
            return []

        def open_table(self, name):
            raise Exception("Should not be called")

    # Mock session manager behavior
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())
    session_registry.get = AsyncMock(return_value=mock_session_manager)
    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.session_tables_schema(
        context, session_id="worker", table_names=None
    )
    assert isinstance(res, dict)
    assert res == {"success": True, "schemas": [], "count": 0}


@pytest.mark.asyncio
async def test_session_tables_schema_success():
    # Create a consistent class-based mock session
    class DummySession:
        async def tables(self):
            return ["table1"]

    # Create a mock for queries.get_meta_table that returns proper schema data
    class MockArrowTable:
        def __init__(self):
            # Mock schema with field objects
            class MockField:
                def __init__(self, name, type_str):
                    self.name = name
                    self.type = type_str

            self.schema = [MockField("Name", "string"), MockField("DataType", "string")]

        def __len__(self):
            return 1  # One column in the table

        def to_pylist(self):
            return [{"Name": "col1", "DataType": "int"}]

    mock_get_meta_table = AsyncMock(return_value=MockArrowTable())

    # Set up the session manager mock
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    # Set up the session registry mock
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )

    # Patch queries.get_meta_table to return our mock data
    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        # Call session_tables_schema with a specific table name
        result = await mcp_mod.session_tables_schema(
            context, session_id="test-worker", table_names=["table1"]
        )

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify the result - now expects full metadata in 'data' field
    assert isinstance(result, dict)
    assert result["success"] is True
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][0]["table"] == "table1"
    assert "data" in result["schemas"][0]
    assert result["schemas"][0]["data"][0]["Name"] == "col1"
    assert result["schemas"][0]["data"][0]["DataType"] == "int"
    assert "meta_columns" in result["schemas"][0]
    assert result["schemas"][0]["row_count"] == 1


@pytest.mark.asyncio
async def test_session_tables_schema_all_tables():
    # Create a mock session with two tables
    dummy_session = MagicMock()
    dummy_session.tables = AsyncMock(return_value=["t1", "t2"])

    # Set up side_effect for queries.get_meta_table to handle multiple calls
    # Will return different data based on which table is requested
    def get_meta_table_side_effect(session, table_name):
        class MockArrowTable:
            def __init__(self, tname):
                class MockField:
                    def __init__(self, name, type_str):
                        self.name = name
                        self.type = type_str

                self.schema = [
                    MockField("Name", "string"),
                    MockField("DataType", "string"),
                ]
                self._table_name = tname

            def __len__(self):
                return 1

            def to_pylist(self):
                return [{"Name": f"col_{self._table_name}", "DataType": "int"}]

        return MockArrowTable(table_name)

    # Create the mock with side_effect to handle different tables
    mock_get_meta_table = AsyncMock(side_effect=get_meta_table_side_effect)

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )

    # Patch queries.get_meta_table to return our mock data
    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        # Call table_schemas with no table_names to test getting all tables
        result = await mcp_mod.session_tables_schema(context, session_id="worker")

    # Should return results for both tables in the dummy_session.tables list
    assert isinstance(result, dict)
    assert result["success"] is True
    assert len(result["schemas"]) == 2
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][1]["success"] is True
    assert result["schemas"][0]["table"] in ["t1", "t2"]
    assert result["schemas"][1]["table"] in ["t1", "t2"]
    assert result["schemas"][0]["table"] != result["schemas"][1]["table"]
    # Check for full metadata in 'data' field
    assert "data" in result["schemas"][0]
    assert "data" in result["schemas"][1]
    assert result["schemas"][0]["data"][0]["Name"] in ["col_t1", "col_t2"]
    assert result["schemas"][1]["data"][0]["Name"] in ["col_t1", "col_t2"]
    assert result["schemas"][0]["data"][0]["DataType"] == "int"
    assert result["schemas"][1]["data"][0]["DataType"] == "int"


@pytest.mark.asyncio
async def test_session_tables_schema_schema_key_error():
    # Create our mock session
    dummy_session = MagicMock()
    dummy_session.tables = AsyncMock(return_value=["table1"])

    # Create a mock for queries.get_meta_table that returns data with missing required keys
    class MockArrowTable:
        def __init__(self):
            class MockField:
                def __init__(self, name, type_str):
                    self.name = name
                    self.type = type_str

            self.schema = [MockField("foo", "string")]

        def __len__(self):
            return 1

        def to_pylist(self):
            # Missing 'Name' and 'DataType' keys, which should trigger an error
            return [{"foo": "bar"}]

    # Set up the get_meta_table mock
    mock_get_meta_table = AsyncMock(return_value=MockArrowTable())

    # Set up session manager mock
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    # Set up session registry mock
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )

    # Patch queries.get_meta_table to return our mock
    with patch("deephaven_mcp.queries.get_session_meta_table", mock_get_meta_table):
        # Call session_tables_schema with a specific table name
        result = await mcp_mod.session_tables_schema(
            context, session_id="test-worker", table_names=["table1"]
        )

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-worker")
    mock_session_manager.get.assert_awaited_once()

    # With the new implementation using to_pylist() directly, any data format is accepted
    # The function no longer validates specific keys - it returns whatever metadata is present
    assert isinstance(result, dict)
    assert result["success"] is True  # Overall operation succeeded
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["success"] is True  # Individual table succeeded
    assert result["schemas"][0]["table"] == "table1"
    # The data is returned as-is from to_pylist(), even with non-standard keys
    assert "data" in result["schemas"][0]
    assert result["schemas"][0]["data"][0]["foo"] == "bar"


@pytest.mark.asyncio
async def test_session_tables_schema_session_error():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(session_id) - set to fail here
    # 3. session = await session_manager.get()

    # Set up session_registry to throw an exception when get() is called
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(side_effect=Exception("fail"))

    context = MockContext(
        {
            "session_registry": session_registry,
        }
    )
    res = await mcp_mod.session_tables_schema(
        context, session_id="worker", table_names=["t1"]
    )
    assert isinstance(res, dict)
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]


# === list_tables ===


@pytest.mark.asyncio
async def test_session_tables_list_success_multiple_tables():
    """Test session_tables_list with multiple tables."""

    # Create a mock session with multiple tables
    class DummySession:
        async def tables(self):
            return ["trades", "quotes", "orders"]

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await mcp_mod.session_tables_list(context, session_id="test-session")

    # Verify correct session access pattern
    session_registry.get.assert_awaited_once_with("test-session")
    mock_session_manager.get.assert_awaited_once()

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["session_id"] == "test-session"
    assert result["table_names"] == ["trades", "quotes", "orders"]
    assert result["count"] == 3


@pytest.mark.asyncio
async def test_session_tables_list_success_empty_session():
    """Test session_tables_list with no tables in session."""

    # Create a mock session with no tables
    class DummySession:
        async def tables(self):
            return []

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await mcp_mod.session_tables_list(context, session_id="empty-session")

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["session_id"] == "empty-session"
    assert result["table_names"] == []
    assert result["count"] == 0


@pytest.mark.asyncio
async def test_session_tables_list_invalid_session_id():
    """Test session_tables_list with invalid session_id."""
    # Set up session registry to raise error on get
    session_registry = MagicMock()
    session_registry.get = AsyncMock(
        side_effect=Exception("Session not found: invalid-session")
    )

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await mcp_mod.session_tables_list(context, session_id="invalid-session")

    # Verify error response
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["isError"] is True
    assert "Session not found" in result["error"]


@pytest.mark.asyncio
async def test_session_tables_list_session_connection_failure():
    """Test session_tables_list when session connection fails."""
    # Set up session manager to raise error on get
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(side_effect=Exception("Connection failed"))

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await mcp_mod.session_tables_list(context, session_id="test-session")

    # Verify error response
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["isError"] is True
    assert "Connection failed" in result["error"]


@pytest.mark.asyncio
async def test_session_tables_list_session_tables_method_failure():
    """Test session_tables_list when session.tables() method fails."""

    # Create a mock session where tables() raises an error
    class DummySession:
        async def tables(self):
            raise Exception("Failed to retrieve table list")

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=DummySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await mcp_mod.session_tables_list(context, session_id="test-session")

    # Verify error response
    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["isError"] is True
    assert "Failed to retrieve table list" in result["error"]


@pytest.mark.asyncio
async def test_session_tables_list_community_session():
    """Test session_tables_list works with community sessions."""

    # Create a mock community session
    class CommunitySession:
        async def tables(self):
            return ["table1", "table2"]

    # Set up session manager and registry mocks
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=CommunitySession())

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call list_tables
    result = await mcp_mod.session_tables_list(
        context, session_id="community:local:test"
    )

    # Verify the result
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["session_id"] == "community:local:test"
    assert result["table_names"] == ["table1", "table2"]
    assert result["count"] == 2


# === run_script ===


@pytest.mark.asyncio
async def test_session_script_run_both_script_and_path():
    # Both script and script_path provided, should prefer script
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()

    # Create a session mock with run_script method
    session = MagicMock()
    session.run_script = AsyncMock(return_value=None)

    # Set up session manager to return the session
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=session)

    # Set up session registry to return the manager
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await mcp_mod.session_script_run(
        context, session_id="foo", script="print('hi')", script_path="/tmp/fake.py"
    )
    assert result["success"] is True
    assert session.run_script.call_count >= 1
    session.run_script.assert_any_call("print('hi')")


@pytest.mark.asyncio
async def test_session_script_run_missing_session():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(session_id) - fails here
    # 3. session = await session_manager.get()

    # Set up session_registry to throw an exception when get() is called
    session_registry = AsyncMock()
    session_registry.get = AsyncMock(side_effect=Exception("no session"))

    context = MockContext({"session_registry": session_registry})
    result = await mcp_mod.session_script_run(
        context, session_id=None, script="print('hi')"
    )
    assert result["success"] is False
    assert result["isError"] is True
    assert "no session" in result["error"]


@pytest.mark.asyncio
async def test_session_script_run_both_none():
    # Following the pattern in _mcp.py:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()

    # This test shouldn't get as far as session creation since both script and script_path are None
    # But we still set up the mocks correctly
    session = AsyncMock()
    mock_session_manager = AsyncMock()
    mock_session_manager.get = AsyncMock(return_value=session)

    session_registry = AsyncMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await mcp_mod.session_script_run(context, session_id="foo")
    assert result["success"] is False
    assert result["isError"] is True
    assert "Must provide either script or script_path" in result["error"]


@pytest.mark.asyncio
async def test_app_lifespan_yields_context_and_cleans_up():
    class DummyServer:
        name = "dummy-server"

    # Import the app_lifespan function
    from deephaven_mcp.mcp_systems_server._mcp import app_lifespan

    # Create mocks
    config_manager = AsyncMock()
    session_registry = AsyncMock()
    refresh_lock = AsyncMock()

    # Configure necessary mocks for the app_lifespan function to work
    config_manager.get_config = AsyncMock(return_value={})
    session_registry.initialize = AsyncMock()
    session_registry.close = AsyncMock()

    # Use a comprehensive patching approach to handle all dependencies
    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.ConfigManager",
            return_value=config_manager,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.CombinedSessionRegistry",
            return_value=session_registry,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.asyncio.Lock",
            return_value=refresh_lock,
        ),
        # Mock get_config_path to avoid environment variable dependency
        patch(
            "deephaven_mcp.config.get_config_path",
            return_value="/mock/config/path.json",
        ),
        # Mock load_and_validate_config to avoid file system dependency
        patch(
            "deephaven_mcp.config.load_and_validate_config",
            AsyncMock(return_value={}),
        ),
    ):
        server = DummyServer()
        async with app_lifespan(server) as context:
            # Just check that the keys exist in the context
            assert "config_manager" in context
            assert "session_registry" in context
            assert "refresh_lock" in context
        session_registry.close.assert_awaited_once()


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_success():
    # Main success test for run_script
    class DummySession:
        called = None

        @staticmethod
        async def run_script(script):
            DummySession.called = script
            return None

    # Set up the session registry pattern correctly
    dummy_session = DummySession()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    result = await mcp_mod.session_script_run(
        context, session_id="worker", script="print(1)"
    )

    # Check correct session access pattern
    session_registry.get.assert_awaited_once_with("worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify results
    assert result["success"] is True
    assert DummySession.called == "print(1)"


@pytest.mark.asyncio
async def test_session_script_run_no_script():
    mock_session_manager = MagicMock()
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.session_script_run(context, session_id="worker")

    # No calls to session_registry should be made since validation fails first
    session_registry.get.assert_not_awaited()

    # Verify error message
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path." in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets and event loops, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets or event loops are created or left open). This is required for Python 3.12+ and some CI environments.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_neither_script_nor_path():
    # Test validation that requires either script or script_path
    # This should fail before any session_registry calls
    mock_session_manager = MagicMock()
    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": session_registry})

    # Call with neither script nor script_path
    res = await mcp_mod.session_script_run(
        context, session_id="worker", script=None, script_path=None
    )

    # No calls to session_registry should be made since validation fails first
    session_registry.get.assert_not_awaited()

    # Verify error message
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path." in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_session_error():
    # Run with fake session registry that errors on get
    # so that we hit the exception branch in run_script
    session_registry = MagicMock()
    session_registry.get = AsyncMock(side_effect=Exception("fail"))

    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.session_script_run(
        context, session_id="worker", script="print(1)"
    )

    # Verify the session registry was called with the correct session id
    session_registry.get.assert_awaited_once_with("worker")

    # Verify error response
    assert res["success"] is False
    assert res["isError"] is True
    assert "fail" in res["error"]


# Use filterwarnings to suppress ResourceWarning about unclosed sockets, which can be triggered by mocks or library internals in CI
# but are not caused by this test (no real sockets are created or left open). This is required for Python 3.12 and older.
@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:unclosed <socket.socket:ResourceWarning")
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
async def test_session_script_run_script_path():
    # Test run_script with script_path and no script
    script_path = "/tmp/test.py"
    script_content = "print('loaded from file')"

    # Mock aiofiles.open properly as a context manager
    # This is the key part: We need a regular MagicMock that returns context manager methods
    mock_file_cm = MagicMock()
    mock_file_cm.__aenter__ = AsyncMock(
        return_value=MagicMock(read=AsyncMock(return_value=script_content))
    )
    mock_file_cm.__aexit__ = AsyncMock(return_value=None)

    mock_open = MagicMock(return_value=mock_file_cm)

    # Create a simple mock session class
    class DummySession:
        called = None

        @staticmethod
        async def run_script(script):
            DummySession.called = script
            return None

    # Set up session mocks
    dummy_session = DummySession()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=dummy_session)

    session_registry = MagicMock()
    session_registry.get = AsyncMock(return_value=mock_session_manager)

    # Apply the patches and run the test
    with patch("aiofiles.open", mock_open):
        context = MockContext({"session_registry": session_registry})
        res = await mcp_mod.session_script_run(
            context, session_id="worker", script_path=script_path
        )

    # Verify session registry was called correctly
    session_registry.get.assert_awaited_once_with("worker")
    mock_session_manager.get.assert_awaited_once()

    # Verify file open and script execution
    mock_open.assert_called_once_with(script_path)
    assert DummySession.called == script_content
    assert res["success"] is True


@pytest.mark.asyncio
async def test_session_script_run_script_path_none_error():
    # Test case where neither script nor script_path is provided
    # This should fail with a validation error, not by calling session_registry.get
    session_registry = MagicMock()
    session_registry.get = AsyncMock()

    context = MockContext({"session_registry": session_registry})
    res = await mcp_mod.session_script_run(
        context, session_id="worker", script=None, script_path=None
    )

    # Verify the validation error is returned
    assert res["success"] is False
    assert res["isError"] is True
    assert "Must provide either script or script_path" in res["error"]

    # Verify the session registry was NOT called (validation fails before that)
    session_registry.get.assert_not_awaited()


# === pip_packages ===


@pytest.mark.asyncio
async def test_session_pip_list_success():
    """Test successful retrieval of pip packages."""
    # Set up mock for the Arrow table and data frame
    mock_arrow_table = MagicMock()
    mock_df = MagicMock()
    mock_df.to_dict.side_effect = lambda *args, **kwargs: (
        [
            {"Package": "numpy", "Version": "1.25.0"},
            {"Package": "pandas", "Version": "2.0.1"},
        ]
        if kwargs.get("orient") == "records"
        else []
    )
    mock_arrow_table.to_pandas.return_value = mock_df

    # Mock the query that fetches pip packages
    mock_get_pip_packages_table = AsyncMock(return_value=mock_arrow_table)

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await mcp_mod.session_pip_list(context, session_id="test_worker")

        # Check correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("test_worker")
        mock_session_manager.get.assert_awaited_once()

        # Verify results
        assert result["success"] is True
        assert len(result["result"]) == 2
        assert result["result"][0]["package"] == "numpy"
        assert result["result"][0]["version"] == "1.25.0"


@pytest.mark.asyncio
async def test_session_pip_list_empty():
    """Test pip_packages with an empty table."""
    # Set up mock for the Arrow table and data frame with empty results
    mock_arrow_table = MagicMock()
    mock_df = MagicMock()
    mock_df.to_dict.side_effect = lambda *args, **kwargs: []
    mock_arrow_table.to_pandas.return_value = mock_df
    mock_get_pip_packages_table = AsyncMock(return_value=mock_arrow_table)

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {"session_registry": mock_session_registry, "config_manager": AsyncMock()}
        )
        result = await mcp_mod.session_pip_list(context, session_id="test_worker")

    # Verify results
    assert result["success"] is True
    assert result["result"] == []

    # Check correct session access pattern
    mock_session_registry.get.assert_awaited_once_with("test_worker")
    mock_session_manager.get.assert_awaited_once()
    mock_get_pip_packages_table.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_pip_list_malformed_data():
    """Test pip_packages with malformed data."""
    # Set up mock for the Arrow table and data frame with malformed results
    mock_arrow_table = MagicMock()
    mock_df = MagicMock()
    mock_df.to_dict.side_effect = lambda *args, **kwargs: (
        [{"badkey": 1}] if kwargs.get("orient") == "records" else []
    )  # missing 'Package' and 'Version'
    mock_arrow_table.to_pandas.return_value = mock_df
    mock_get_pip_packages_table = AsyncMock(return_value=mock_arrow_table)

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await mcp_mod.session_pip_list(context, session_id="test_worker")

    # Verify results
    assert result["success"] is False
    assert result["isError"] is True
    assert "Malformed package data" in result["error"]

    # Check correct session access pattern
    mock_session_registry.get.assert_awaited_once_with("test_worker")
    mock_session_manager.get.assert_awaited_once()
    mock_get_pip_packages_table.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_pip_list_error():
    """Test pip_packages with an error."""
    # Mock the query that fetches pip packages to throw an exception
    mock_get_pip_packages_table = AsyncMock(side_effect=Exception("Table error"))

    # Set up the session registry pattern:
    # 1. session_registry = context["session_registry"]
    # 2. session_manager = await session_registry.get(worker_name)
    # 3. session = await session_manager.get()
    mock_session = MagicMock()
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await mcp_mod.session_pip_list(context, session_id="test_worker")

        # Verify results
        assert result["success"] is False
        assert result["isError"] is True
        assert "Table error" in result["error"]

        # Check correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("test_worker")
        mock_session_manager.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_pip_list_session_not_found():
    """Test pip_packages when the session is not found."""
    mock_get_pip_packages_table = AsyncMock(return_value=MagicMock())

    # Set up session_registry to fail when get() is called
    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(side_effect=ValueError("Worker not found"))

    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        mock_get_pip_packages_table,
    ):
        context = MockContext(
            {
                "session_registry": mock_session_registry,
                "config_manager": AsyncMock(),
                "instance_tracker": create_mock_instance_tracker(),
            }
        )
        result = await mcp_mod.session_pip_list(
            context, session_id="nonexistent_worker"
        )
        assert result["success"] is False
        assert "Worker not found" in result["error"]
        assert result["isError"] is True

        # Verify correct session access pattern
        mock_session_registry.get.assert_awaited_once_with("nonexistent_worker")


# === enterprise_systems_status tests ===


@pytest.mark.asyncio
async def test_enterprise_systems_status_success():
    """Test successful retrieval of enterprise systems status."""
    # Mock factory with liveness_status and is_alive methods
    mock_factory1 = AsyncMock()
    mock_factory1.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, "System is healthy")
    )
    mock_factory1.is_alive = AsyncMock(return_value=True)

    mock_factory2 = AsyncMock()
    mock_factory2.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.OFFLINE, "System is not responding")
    )
    mock_factory2.is_alive = AsyncMock(return_value=False)

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(
        return_value={"system1": mock_factory1, "system2": mock_factory2}
    )

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={
            "enterprise": {
                "systems": {
                    "system1": {"url": "http://example.com", "api_key": "secret_key"},
                    "system2": {
                        "url": "http://example2.com",
                        "password": "secret_password",
                    },
                }
            }
        }
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function to match the actual implementation
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config"
    ) as mock_redact:
        # Configure the mock to replace only password with [REDACTED]
        def redact_config(config):
            result = config.copy()
            if "password" in result:
                result["password"] = "[REDACTED]"
            return result

        mock_redact.side_effect = redact_config
        # Call the function with default parameters
        result = await mcp_mod.enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is True
    assert len(result["systems"]) == 2

    # Check system1
    system1 = next(s for s in result["systems"] if s["name"] == "system1")
    assert system1["liveness_status"] == "ONLINE"
    assert system1["liveness_detail"] == "System is healthy"
    assert system1["is_alive"] is True
    assert system1["config"]["url"] == "http://example.com"
    assert system1["config"]["api_key"] == "secret_key"

    # Check system2
    system2 = next(s for s in result["systems"] if s["name"] == "system2")
    assert system2["liveness_status"] == "OFFLINE"
    assert system2["liveness_detail"] == "System is not responding"
    assert system2["is_alive"] is False
    assert system2["config"]["url"] == "http://example2.com"
    assert system2["config"]["password"] == "[REDACTED]"

    # Verify liveness_status was called with attempt_to_connect=False
    mock_factory1.liveness_status.assert_called_once_with(ensure_item=False)
    mock_factory2.liveness_status.assert_called_once_with(ensure_item=False)


@pytest.mark.asyncio
async def test_enterprise_systems_status_with_attempt_to_connect():
    """Test enterprise systems status with attempt_to_connect=True."""
    # Mock factory with liveness_status and is_alive methods
    mock_factory = AsyncMock()
    mock_factory.liveness_status = AsyncMock(
        return_value=(ResourceLivenessStatus.ONLINE, None)
    )
    mock_factory.is_alive = AsyncMock(return_value=True)

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(return_value={"system1": mock_factory})

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {"system1": {}}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config",
        return_value={},
    ):
        # Call the function with attempt_to_connect=True
        result = await mcp_mod.enterprise_systems_status(
            context, attempt_to_connect=True
        )

        # Verify the result
        assert result["success"] is True
        assert len(result["systems"]) == 1

        # Check system1
        system1 = result["systems"][0]
        assert system1["name"] == "system1"
        assert system1["liveness_status"] == "ONLINE"
        assert "liveness_detail" not in system1  # No detail was provided
        assert system1["is_alive"] is True

        # Verify liveness_status was called with attempt_to_connect=True
        mock_factory.liveness_status.assert_called_once_with(ensure_item=True)


@pytest.mark.asyncio
async def test_enterprise_systems_status_no_systems():
    """Test enterprise systems status with no systems available."""
    # Mock enterprise registry with no systems
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(return_value={})

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await mcp_mod.enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is True
    assert len(result["systems"]) == 0


@pytest.mark.asyncio
async def test_enterprise_systems_status_all_status_types():
    """Test enterprise systems status with all possible status types."""
    # Create a mock factory for each status type
    factories = {}
    status_details = {
        "online_system": (ResourceLivenessStatus.ONLINE, "System is healthy"),
        "offline_system": (ResourceLivenessStatus.OFFLINE, "System is not responding"),
        "unauthorized_system": (
            ResourceLivenessStatus.UNAUTHORIZED,
            "Authentication failed",
        ),
        "misconfigured_system": (
            ResourceLivenessStatus.MISCONFIGURED,
            "Invalid configuration",
        ),
        "unknown_system": (ResourceLivenessStatus.UNKNOWN, "Unknown error occurred"),
    }

    for name, (status, detail) in status_details.items():
        mock_factory = AsyncMock()
        mock_factory.liveness_status = AsyncMock(return_value=(status, detail))
        mock_factory.is_alive = AsyncMock(
            return_value=(status == ResourceLivenessStatus.ONLINE)
        )
        factories[name] = mock_factory

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(return_value=factories)

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )

    # Mock config manager with empty configs
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {name: {} for name in factories}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config",
        return_value={},
    ):
        # Call the function
        result = await mcp_mod.enterprise_systems_status(context)

        # Verify the result
        assert result["success"] is True
        assert len(result["systems"]) == 5

        # Check each system has the correct status and detail
        for name, (status, detail) in status_details.items():
            system = next(s for s in result["systems"] if s["name"] == name)
            assert system["liveness_status"] == status.name
            assert system["liveness_detail"] == detail
            assert system["is_alive"] == (status == ResourceLivenessStatus.ONLINE)


@pytest.mark.asyncio
async def test_enterprise_systems_status_config_error():
    """Test enterprise systems status when config retrieval fails."""
    # Mock session registry
    mock_session_registry = MagicMock()
    mock_enterprise_registry = AsyncMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )

    # Mock config manager that raises an exception
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(side_effect=Exception("Config error"))

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await mcp_mod.enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is False
    assert result["isError"] is True
    assert "Config error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_registry_error():
    """Test enterprise systems status when registry retrieval fails."""
    # Mock enterprise registry that raises an exception
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all.side_effect = Exception("Registry error")

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await mcp_mod.enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is False
    assert result["isError"] is True
    assert "Registry error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_liveness_error():
    """Test enterprise systems status when liveness_status raises an exception."""
    # Mock factory with liveness_status that raises an exception
    mock_factory = AsyncMock()
    mock_factory.liveness_status.side_effect = Exception("Liveness error")
    mock_factory.is_alive.return_value = False

    # Mock enterprise registry
    mock_enterprise_registry = AsyncMock()
    mock_enterprise_registry.get_all = AsyncMock(return_value={"system1": mock_factory})

    # Mock session registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"factories": {"system1": {}}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Mock the redact function
    with patch(
        "deephaven_mcp.config._enterprise_system.redact_enterprise_system_config",
        return_value={},
    ):
        # Call the function
        result = await mcp_mod.enterprise_systems_status(context)

        # Verify the result
        assert result["success"] is False
        assert result["isError"] is True
        assert "Liveness error" in result["error"]


@pytest.mark.asyncio
async def test_enterprise_systems_status_no_enterprise_registry():
    """Test enterprise systems status when enterprise_registry is None."""
    # Mock session registry with None enterprise registry
    mock_session_registry = MagicMock()
    mock_session_registry.enterprise_registry = AsyncMock(return_value=AsyncMock())
    mock_session_registry.enterprise_registry.return_value.get_all = AsyncMock(
        return_value={}
    )

    # Mock config manager
    mock_config_manager = AsyncMock()
    mock_config_manager.get_config = AsyncMock(
        return_value={"enterprise": {"systems": {}}}
    )

    # Create context
    context = MockContext(
        {
            "session_registry": mock_session_registry,
            "config_manager": mock_config_manager,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Call the function
    result = await mcp_mod.enterprise_systems_status(context)

    # Verify the result
    assert result["success"] is True
    assert len(result["systems"]) == 0


# === list_sessions and get_session_details tests ===


@pytest.mark.asyncio
async def test_sessions_list_success():
    """Test sessions_list with multiple sessions of different types."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session managers
    mock_session_mgr1 = AsyncMock()
    mock_session_mgr1.system_type.name = "COMMUNITY"
    mock_session_mgr1.source = "source1"
    mock_session_mgr1.name = "session1"

    mock_session_mgr2 = AsyncMock()
    mock_session_mgr2.system_type.name = "ENTERPRISE"
    mock_session_mgr2.source = "source2"
    mock_session_mgr2.name = "session2"

    mock_registry.get_all.return_value = {
        "session1": mock_session_mgr1,
        "session2": mock_session_mgr2,
    }

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.sessions_list(mock_context)

    # Verify results
    assert result["success"] is True
    assert len(result["sessions"]) == 2

    # Check first session
    session1 = next(s for s in result["sessions"] if s["session_id"] == "session1")
    assert session1["type"] == "COMMUNITY"
    assert session1["source"] == "source1"
    assert session1["session_name"] == "session1"
    assert "available" not in session1  # Should not check availability

    # Check second session
    session2 = next(s for s in result["sessions"] if s["session_id"] == "session2")
    assert session2["type"] == "ENTERPRISE"
    assert session2["source"] == "source2"
    assert session2["session_name"] == "session2"
    assert "available" not in session2  # Should not check availability


@pytest.mark.asyncio
async def test_sessions_list_with_unknown_type():
    """Test sessions_list with a session that has no system_type attribute."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create a mock session manager with no system_type
    mock_session_mgr = AsyncMock()
    mock_session_mgr.system_type = None
    mock_session_mgr.source = "source"
    mock_session_mgr.name = "session"

    mock_registry.get_all.return_value = {"session": mock_session_mgr}

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.sessions_list(mock_context)

    # Verify results
    assert result["success"] is True
    assert len(result["sessions"]) == 1
    # Check that we have an error entry for this session since system_type is None
    assert result["sessions"][0]["session_id"] == "session"
    assert "error" in result["sessions"][0]


@pytest.mark.asyncio
async def test_sessions_list_with_processing_error():
    """Test sessions_list when processing a session raises an exception."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create a session manager that will cause an exception during processing
    mock_session_mgr = AsyncMock()
    # Configure system_type.name to raise an exception when accessed
    mock_system_type = MagicMock()
    type(mock_system_type).name = PropertyMock(
        side_effect=Exception("Processing error")
    )
    mock_session_mgr.system_type = mock_system_type

    mock_registry.get_all.return_value = {"session": mock_session_mgr}

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.sessions_list(mock_context)

    # Verify results
    assert result["success"] is True
    assert len(result["sessions"]) == 1
    assert "error" in result["sessions"][0]
    assert result["sessions"][0]["session_id"] == "session"


@pytest.mark.asyncio
async def test_sessions_list_registry_error():
    """Test sessions_list when the session registry raises an exception."""
    # Mock context with registry that raises an exception
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context.__getitem__.side_effect = Exception(
        "Registry error"
    )

    # Call function
    result = await mcp_mod.sessions_list(mock_context)

    # Verify results
    assert result["success"] is False


@pytest.mark.asyncio
async def test_session_details_session_not_found():
    """Test session_details for a non-existent session."""
    # Mock session registry
    mock_registry = AsyncMock()
    mock_registry.get.side_effect = Exception("Session not found")

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.session_details(mock_context, "nonexistent")

    # Verify results
    assert result["success"] is False
    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_details_with_session_error():
    """Test session_details when getting the session raises an exception."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session manager that raises an exception when liveness_status is called
    mock_session_mgr = AsyncMock()
    mock_system_type = MagicMock()
    mock_system_type.name = "COMMUNITY"
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.source = "source1"
    mock_session_mgr.name = "session1"
    # Set is_alive to raise an exception
    mock_session_mgr.is_alive = AsyncMock(side_effect=Exception("Session error"))
    mock_session_mgr.liveness_status.side_effect = Exception("Liveness status error")

    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.session_details(mock_context, "session1")

    # Verify results
    assert result["success"] is True
    assert "session" in result
    assert result["session"]["available"] is False


@pytest.mark.asyncio
async def test_session_details_with_processing_error():
    """Test session_details when processing a session raises an exception."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create a session manager that will cause an exception during processing
    mock_session_mgr = AsyncMock()
    # Configure system_type.name to raise an exception when accessed
    mock_system_type = MagicMock()
    type(mock_system_type).name = PropertyMock(
        side_effect=Exception("Processing error")
    )
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.is_alive = AsyncMock(return_value=True)
    # Mock liveness_status to return a tuple of (status, detail) as expected by the implementation
    mock_status = MagicMock()
    mock_status.name = "ONLINE"
    mock_session_mgr.liveness_status.return_value = (
        mock_status,
        "All systems operational",
    )

    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.session_details(mock_context, "session1")

    # Verify results
    assert result["success"] is False
    assert "error" in result
    assert "Processing error" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_details_registry_error():
    """Test session_details when the session registry raises an exception."""
    # Mock context with registry that raises an exception
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context.__getitem__.side_effect = Exception(
        "Registry error"
    )

    # Call function
    result = await mcp_mod.session_details(mock_context, "session1")

    # Verify results
    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_details_success_with_programming_language():
    """Test session_details for an existing session with programming_language property."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session with programming_language
    mock_session = MagicMock()
    mock_session.programming_language = "python"

    # Create mock session manager
    mock_session_mgr = AsyncMock()
    mock_system_type = MagicMock()
    mock_system_type.name = "COMMUNITY"
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.source = "source1"
    mock_session_mgr.name = "session1"
    mock_session_mgr.is_alive = AsyncMock(return_value=True)
    mock_session_mgr.get = AsyncMock(return_value=mock_session)
    # Mock liveness_status to return a tuple of (status, detail) as expected by the implementation
    mock_status = MagicMock()
    mock_status.name = "ONLINE"
    mock_session_mgr.liveness_status.return_value = (
        mock_status,
        "All systems operational",
    )

    # Set up registry to return our mock session manager
    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.session_details(
        mock_context, "session1", attempt_to_connect=True
    )

    # Verify results
    assert result["success"] is True
    assert "session" in result
    assert result["session"]["session_id"] == "session1"
    assert result["session"]["type"] == "COMMUNITY"
    assert result["session"]["source"] == "source1"
    assert result["session"]["session_name"] == "session1"
    assert result["session"]["available"] is True
    assert result["session"]["liveness_status"] == "ONLINE"
    assert result["session"]["programming_language"] == "python"
    assert result["session"]["liveness_detail"] == "All systems operational"


@pytest.mark.asyncio
async def test_session_details_success_without_programming_language():
    """Test session_details for an existing session without programming_language property."""
    # Mock session registry
    mock_registry = AsyncMock()

    # Create mock session without programming_language attribute
    mock_session = MagicMock(spec=[])

    # Create mock session manager
    mock_session_mgr = AsyncMock()
    mock_system_type = MagicMock()
    mock_system_type.name = "COMMUNITY"
    mock_session_mgr.system_type = mock_system_type
    mock_session_mgr.source = "source1"
    mock_session_mgr.name = "session1"
    mock_session_mgr.is_alive = AsyncMock(return_value=True)
    mock_session_mgr.get = AsyncMock(return_value=mock_session)
    # Mock liveness_status to return a tuple of (status, detail) as expected by the implementation
    mock_status = MagicMock()
    mock_status.name = "ONLINE"
    mock_session_mgr.liveness_status.return_value = (
        mock_status,
        "All systems operational",
    )

    # Set up registry to return our mock session manager
    mock_registry.get.return_value = mock_session_mgr

    # Mock context
    mock_context = MagicMock()
    mock_context.request_context.lifespan_context = {"session_registry": mock_registry}

    # Call function
    result = await mcp_mod.session_details(
        mock_context, "session1", attempt_to_connect=True
    )

    # Verify results
    assert result["success"] is True
    assert "session" in result
    assert result["session"]["session_id"] == "session1"
    assert result["session"]["type"] == "COMMUNITY"
    assert result["session"]["source"] == "source1"
    assert result["session"]["session_name"] == "session1"
    assert result["session"]["available"] is True
    assert result["session"]["liveness_status"] == "ONLINE"
    assert "programming_language" not in result["session"]
    assert result["session"]["liveness_detail"] == "All systems operational"


# === Helper Functions ===


def test_check_response_size_acceptable():
    """Test _check_response_size with acceptable size."""
    result = mcp_mod._check_response_size("test_table", 1000000)  # 1MB
    assert result is None


def test_check_response_size_warning_threshold():
    """Test _check_response_size with size above warning threshold."""
    with patch("deephaven_mcp.mcp_systems_server._mcp._LOGGER") as mock_logger:
        result = mcp_mod._check_response_size("test_table", 10000000)  # 10MB
        assert result is None
        mock_logger.warning.assert_called_once()
        assert "Large response (~10.0MB)" in mock_logger.warning.call_args[0][0]


def test_check_response_size_over_limit():
    """Test _check_response_size with size over maximum limit."""
    result = mcp_mod._check_response_size("test_table", 60000000)  # 60MB
    assert result == {
        "success": False,
        "error": "Response would be ~60.0MB (max 50MB). Please reduce max_rows.",
        "isError": True,
    }


# === get_table_data ===


@pytest.mark.asyncio
async def test_session_table_data_success_default_params():
    """Test get_table_data with default parameters."""
    # Mock context and session registry
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock arrow table (small size to trigger markdown-kv format)
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=50)  # Small size -> markdown-kv
    mock_field1 = MagicMock()
    mock_field1.name = "col1"
    mock_field1.type = "int64"
    mock_field2 = MagicMock()
    mock_field2.name = "col2"
    mock_field2.type = "string"
    mock_arrow_table.schema = [mock_field1, mock_field2]
    mock_arrow_table.column_names = ["col1", "col2"]

    # Mock batch for formatters
    mock_batch = MagicMock()
    mock_batch.to_pylist.return_value = [
        {"col1": 1, "col2": "a"},
        {"col1": 2, "col2": "b"},
        {"col1": 3, "col2": "c"},
    ]
    mock_arrow_table.to_batches.return_value = [mock_batch]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, True)

        result = await mcp_mod.session_table_data(context, "session1", "table1")

        assert result["success"] is True
        assert result["table_name"] == "table1"
        assert (
            result["format"] == "markdown-table"
        )  # Default format changed to markdown-table
        assert result["row_count"] == 50
        assert result["is_complete"] is True
        assert "schema" in result
        assert "data" in result
        assert isinstance(result["data"], str)  # markdown-table returns string
        assert "|" in result["data"]  # markdown-table uses pipe delimiters

        # Verify queries.get_table was called with correct parameters
        mock_get_table.assert_called_once_with(
            mock_session, "table1", max_rows=1000, head=True
        )


@pytest.mark.asyncio
async def test_session_table_data_success_custom_params():
    """Test get_table_data with custom parameters."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock arrow table
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=50)
    mock_field = MagicMock()
    mock_field.name = "col1"
    mock_field.type = "int64"
    mock_arrow_table.schema = [mock_field]
    mock_arrow_table.to_pylist.return_value = [{"col1": 1}, {"col1": 2}]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, False)

        result = await mcp_mod.session_table_data(
            context, "session1", "table1", max_rows=50, head=False, format="json-row"
        )

        assert result["success"] is True
        assert result["format"] == "json-row"
        assert result["is_complete"] is False

        mock_get_table.assert_called_once_with(
            mock_session, "table1", max_rows=50, head=False
        )


@pytest.mark.asyncio
async def test_session_table_data_success_full_table():
    """Test get_table_data with max_rows=None for full table."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock arrow table (large size to trigger CSV format)
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=15000)  # >10000 rows -> csv
    mock_field = MagicMock()
    mock_field.name = "col1"
    mock_field.type = "int64"
    mock_arrow_table.schema = [mock_field]

    # Mock CSV output for large table
    with (
        patch("deephaven_mcp.formatters._csv.io.BytesIO") as mock_bytesio,
        patch("deephaven_mcp.formatters._csv.csv.write_csv") as mock_write_csv,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_table"
        ) as mock_get_table,
    ):

        mock_output = MagicMock()
        mock_output.getvalue.return_value = b"col1\n1\n2\n3"
        mock_bytesio.return_value = mock_output

        mock_get_table.return_value = (mock_arrow_table, True)

        result = await mcp_mod.session_table_data(
            context, "session1", "table1", max_rows=None
        )

        assert result["success"] is True
        assert (
            result["format"] == "markdown-table"
        )  # Default format is now markdown-table
        assert result["is_complete"] is True

        mock_get_table.assert_called_once_with(
            mock_session, "table1", max_rows=None, head=True
        )


@pytest.mark.asyncio
async def test_session_table_data_invalid_format():
    """Test get_table_data with invalid format parameter."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock arrow table
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=10)
    mock_field = MagicMock()
    mock_field.name = "col1"
    mock_field.type = "int64"
    mock_arrow_table.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, True)

        result = await mcp_mod.session_table_data(
            context, "session1", "table1", format="invalid"
        )

        assert result["success"] is False
        assert "Invalid format 'invalid'" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_table_data_size_limit_exceeded():
    """Test get_table_data when response size exceeds limit."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Test values to trigger size limit (large table * many columns = large estimated size)
    large_row_count = 1_000_000  # Large number of rows to trigger size limit
    many_columns = 100  # Large number of columns to trigger size limit

    # Mock arrow table with large estimated size
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=large_row_count)
    mock_arrow_table.schema = [MagicMock() for _ in range(many_columns)]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_table"
    ) as mock_get_table:
        mock_get_table.return_value = (mock_arrow_table, True)

        result = await mcp_mod.session_table_data(context, "session1", "table1")

        assert result["success"] is False
        assert "max 50MB" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_table_data_session_not_found():
    """Test get_table_data when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await mcp_mod.session_table_data(context, "invalid_session", "table1")

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_table_data_table_not_found():
    """Test get_table_data when table is not found."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_table"
    ) as mock_get_table:
        mock_get_table.side_effect = Exception("Table 'invalid_table' not found")

        result = await mcp_mod.session_table_data(context, "session1", "invalid_table")

        assert result["success"] is False
        assert "Table 'invalid_table' not found" in result["error"]
        assert result["isError"] is True


# === get_table_meta tests removed ===
# The get_table_meta function has been merged into session_tables_schema
# All functionality is now tested by session_tables_schema tests


# === create_enterprise_session ===


@pytest.mark.asyncio
async def test_session_enterprise_create_success_with_defaults():
    """Test session_enterprise_create with config defaults."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    # Mock enterprise systems config
    enterprise_config = {
        "prod-system": {
            "connection_json_url": "https://prod.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "admin",
            "password": "secret",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "heap_size_gb": 8.0,
                    "auto_delete_timeout": 3600,
                    "server": "server-east-1",
                    "engine": "DeephavenCommunity",
                },
            },
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry and factories
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    mock_registry.enterprise_registry = AsyncMock(return_value=mock_enterprise_registry)
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

    # Mock no existing workers (under limit)
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.get = AsyncMock(
        side_effect=KeyError("Session not found")
    )  # No conflict
    mock_registry.add_session = AsyncMock()
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_create(
        context, "prod-system", "test-worker"
    )

    assert result["success"] is True
    assert result["session_id"] == "enterprise:prod-system:test-worker"
    assert result["system_name"] == "prod-system"
    assert result["session_name"] == "test-worker"
    assert result["configuration"]["heap_size_gb"] == 8.0
    assert result["configuration"]["auto_delete_timeout"] == 3600
    assert result["configuration"]["server"] == "server-east-1"
    assert result["configuration"]["engine"] == "DeephavenCommunity"

    # Verify worker was created with correct parameters
    mock_factory.connect_to_new_worker.assert_called_once_with(
        name="test-worker",
        heap_size_gb=8.0,
        auto_delete_timeout=3600,
        server="server-east-1",
        engine="DeephavenCommunity",
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=60,
        configuration_transformer=None,
        session_arguments=None,
    )

    # Verify session was added to registry
    # Verify add_session was called with manager only
    mock_registry.add_session.assert_called_once()
    call_args = mock_registry.add_session.call_args
    session_manager = call_args[0][0]  # Manager is the only argument
    assert session_manager.full_name == "enterprise:prod-system:test-worker"


@pytest.mark.asyncio
async def test_session_enterprise_create_success_with_overrides():
    """Test session_enterprise_create with parameter overrides."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    # Mock enterprise systems config with defaults
    enterprise_config = {
        "prod-system": {
            "connection_json_url": "https://prod.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "admin",
            "password": "secret",
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {"heap_size_gb": 4.0, "auto_delete_timeout": 1800},
            },
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry and factories
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    mock_registry.enterprise_registry = AsyncMock(return_value=mock_enterprise_registry)
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.get = AsyncMock(
        side_effect=KeyError("Session not found")
    )  # No conflict
    mock_registry.add_session = AsyncMock()
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_create(
        context,
        "prod-system",
        "custom-worker",
        heap_size_gb=16.0,
        auto_delete_timeout=7200,
        server="server-west-1",
        engine="DeephavenEnterprise",
    )

    assert result["success"] is True
    assert result["configuration"]["heap_size_gb"] == 16.0  # Override
    assert result["configuration"]["auto_delete_timeout"] == 7200  # Override
    assert result["configuration"]["server"] == "server-west-1"  # Override
    assert result["configuration"]["engine"] == "DeephavenEnterprise"  # Override

    mock_factory.connect_to_new_worker.assert_called_once_with(
        name="custom-worker",
        heap_size_gb=16.0,
        auto_delete_timeout=7200,
        server="server-west-1",
        engine="DeephavenEnterprise",
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=60,
        configuration_transformer=None,
        session_arguments=None,
    )


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_generate_name():
    """Test session_enterprise_create auto-generates worker name when None."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "test",
            "password": "test",
            "session_creation": {"max_concurrent_sessions": 3},
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    with patch("deephaven_mcp.mcp_systems_server._mcp.datetime") as mock_datetime:
        mock_datetime.now().strftime.return_value = "20241126-1430"

        # Mock session registry and factories
        mock_enterprise_registry = MagicMock()
        mock_factory_manager = MagicMock()
        mock_factory = MagicMock()
        mock_session = MagicMock()

        mock_registry.enterprise_registry = AsyncMock(
            return_value=mock_enterprise_registry
        )
        mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
        mock_factory_manager.get = AsyncMock(return_value=mock_factory)
        mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)

        mock_registry.get_all = AsyncMock(return_value={})
        mock_registry.get = AsyncMock(
            side_effect=KeyError("Session not found")
        )  # No conflict
        mock_registry.add_session = AsyncMock()
        mock_registry.count_added_sessions = AsyncMock(return_value=0)

        context = MockContext(
            {"config_manager": mock_config_manager, "session_registry": mock_registry}
        )

        result = await mcp_mod.session_enterprise_create(context, "test-system")

        assert result["success"] is True
        assert result["session_name"] == "mcp-test-20241126-1430"
        assert result["session_id"] == "enterprise:test-system:mcp-test-20241126-1430"


@pytest.mark.asyncio
async def test_session_enterprise_create_system_not_found():
    """Test session_enterprise_create when enterprise system not found."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    # Provide empty enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_create(
        context, "nonexistent-system", "worker"
    )

    assert result["success"] is False
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_max_workers_exceeded():
    """Test session_enterprise_create when max concurrent workers limit exceeded."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "limited-system": {
            "connection_json_url": "https://limited.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 2},  # Low limit for testing
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock registry to return 2 existing sessions (at limit)
    mock_registry.count_added_sessions = AsyncMock(return_value=2)

    # Mock session registry get to simulate existing sessions for counting
    async def mock_session_get(session_id):
        if session_id in [
            "enterprise:limited-system:worker1",
            "enterprise:limited-system:worker2",
        ]:
            return MagicMock(spec=mcp_mod.EnterpriseSessionManager)
        elif session_id == "enterprise:limited-system:worker3":
            raise KeyError("Session not found")  # New session doesn't exist yet
        else:
            raise KeyError("Session not found")

    mock_registry.get = AsyncMock(side_effect=mock_session_get)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_create(
        context, "limited-system", "worker3"
    )

    assert result["success"] is False
    assert "Max concurrent sessions (2) reached" in result["error"]
    assert result["isError"] is True

    # No cleanup needed - session tracking handled by registry


@pytest.mark.asyncio
async def test_session_enterprise_create_session_conflict():
    """Test session_enterprise_create when session ID already exists."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "conflict-system": {
            "connection_json_url": "https://conflict.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 5},
        }
    }

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry to return existing session
    mock_existing_session = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_existing_session)
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_create(
        context, "conflict-system", "existing-worker"
    )

    assert result["success"] is False
    assert (
        "Session 'enterprise:conflict-system:existing-worker' already exists"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_factory_creation_failure():
    """Test session_enterprise_create when worker creation fails."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "failing-system": {
            "connection_json_url": "https://failing.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 5},
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry - no conflict
    mock_registry.get = AsyncMock(side_effect=KeyError("No session found"))
    mock_registry.get_all = AsyncMock(return_value={})
    mock_registry.count_added_sessions = AsyncMock(return_value=0)

    # Mock factory that fails during worker creation
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()

    mock_registry.enterprise_registry = AsyncMock(return_value=mock_enterprise_registry)
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(
        side_effect=Exception("Resource exhausted")
    )

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_create(
        context, "failing-system", "failing-worker"
    )

    assert result["success"] is False
    assert "Resource exhausted" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_create_disabled_by_zero_max_workers():
    """Test session_enterprise_create when worker creation is disabled (max_concurrent_sessions = 0)."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "disabled-system": {
            "connection_json_url": "https://disabled.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
            "session_creation": {"max_concurrent_sessions": 0},  # Disabled
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_create(
        context, "disabled-system", "test-worker"
    )

    assert result["success"] is False
    assert "Session creation is disabled" in result["error"]
    assert result["isError"] is True


# === delete_enterprise_session ===


@pytest.mark.asyncio
async def test_session_enterprise_delete_success():
    """Test session_enterprise_delete successful deletion."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock existing enterprise session manager
    mock_session_manager = MagicMock(spec=mcp_mod.EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock()

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_delete(
        context, "test-system", "test-worker"
    )

    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:test-worker"
    assert result["system_name"] == "test-system"
    assert result["session_name"] == "test-worker"

    # Verify session was closed and removed
    mock_session_manager.close.assert_called_once()
    # Verify remove_session was called
    mock_registry.remove_session.assert_called_once_with(
        "enterprise:test-system:test-worker"
    )


@pytest.mark.asyncio
async def test_session_enterprise_delete_system_not_found():
    """Test session_enterprise_delete when enterprise system not found."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    # No enterprise systems

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_delete(
        context, "nonexistent-system", "worker"
    )

    assert result["success"] is False
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_session_not_found():
    """Test session_enterprise_delete when session not found."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_delete(
        context, "test-system", "nonexistent-worker"
    )

    assert result["success"] is False
    assert (
        "Session 'enterprise:test-system:nonexistent-worker' not found"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_not_enterprise_session():
    """Test session_enterprise_delete when session is not an EnterpriseSessionManager."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock non-enterprise session manager
    mock_session_manager = MagicMock()  # Not an EnterpriseSessionManager

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_delete(
        context, "test-system", "wrong-type-worker"
    )

    assert result["success"] is False
    assert "is not an enterprise session" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_enterprise_delete_close_failure_continues():
    """Test session_enterprise_delete continues removal even if close fails."""
    mock_registry = MagicMock()
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.example.com/iris/connection.json",
            "auth_type": "password",
            "username": "user",
            "password": "pass",
        }
    }

    # Mock session manager that fails to close
    mock_session_manager = MagicMock(spec=mcp_mod.EnterpriseSessionManager)
    mock_session_manager.close = AsyncMock(side_effect=Exception("Close failed"))

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_registry.remove_session = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {"config_manager": mock_config_manager, "session_registry": mock_registry}
    )

    result = await mcp_mod.session_enterprise_delete(
        context, "test-system", "failing-close-worker"
    )

    # Should succeed despite close failure
    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:failing-close-worker"

    # Verify session was still removed from registry
    # Verify remove_session was called even after close failure
    mock_registry.remove_session.assert_called_once_with(
        "enterprise:test-system:failing-close-worker"
    )


# === Helper function tests ===


def test_resolve_session_parameters():
    """Test _resolve_session_parameters helper function."""
    defaults = {
        "heap_size_gb": 4.0,
        "auto_delete_timeout": 1800,
        "server": "default-server",
        "engine": "DeephavenCommunity",
        "extra_jvm_args": ["-Xmx1g"],
        "extra_environment_vars": ["ENV=test"],
        "admin_groups": ["admins"],
        "viewer_groups": ["viewers"],
        "timeout_seconds": 120,
        "session_arguments": {"key": "value"},
        "programming_language": "Python",
    }

    # Test with all parameters provided (should override defaults)
    result = mcp_mod._resolve_session_parameters(
        heap_size_gb=8.0,
        auto_delete_timeout=3600,
        server="custom-server",
        engine="CustomEngine",
        extra_jvm_args=["-Xmx2g"],
        extra_environment_vars=["ENV=prod"],
        admin_groups=["custom-admins"],
        viewer_groups=["custom-viewers"],
        timeout_seconds=240,
        session_arguments={"custom": "args"},
        programming_language="Groovy",
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 8.0
    assert result["auto_delete_timeout"] == 3600
    assert result["server"] == "custom-server"
    assert result["engine"] == "CustomEngine"
    assert result["extra_jvm_args"] == ["-Xmx2g"]
    assert result["extra_environment_vars"] == ["ENV=prod"]
    assert result["admin_groups"] == ["custom-admins"]
    assert result["viewer_groups"] == ["custom-viewers"]
    assert result["timeout_seconds"] == 240
    assert result["session_arguments"] == {"custom": "args"}
    assert result["programming_language"] == "Groovy"

    # Test with no parameters provided (should use defaults)
    result = mcp_mod._resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 4.0
    assert result["auto_delete_timeout"] == 1800
    assert result["server"] == "default-server"
    assert result["engine"] == "DeephavenCommunity"
    assert result["extra_jvm_args"] == ["-Xmx1g"]
    assert result["extra_environment_vars"] == ["ENV=test"]
    assert result["admin_groups"] == ["admins"]
    assert result["viewer_groups"] == ["viewers"]
    assert result["timeout_seconds"] == 120
    assert result["session_arguments"] == {"key": "value"}
    assert result["programming_language"] == "Python"

    # Test with mixed parameters (some provided, some defaults)
    result = mcp_mod._resolve_session_parameters(
        heap_size_gb=16.0,  # Override
        auto_delete_timeout=None,  # Use default
        server="override-server",  # Override
        engine=None,  # Use default
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 16.0
    assert result["auto_delete_timeout"] == 1800
    assert result["server"] == "override-server"
    assert result["engine"] == "DeephavenCommunity"

    # Test with empty defaults (should use built-in defaults)
    result = mcp_mod._resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults={},
    )

    assert result["heap_size_gb"] is None
    assert result["auto_delete_timeout"] is None
    assert result["server"] is None
    assert result["engine"] == "DeephavenCommunity"  # Built-in default
    assert result["extra_jvm_args"] is None
    assert result["extra_environment_vars"] is None
    assert result["admin_groups"] is None
    assert result["viewer_groups"] is None
    assert result["timeout_seconds"] == 60  # Built-in default
    assert result["session_arguments"] is None
    assert result["programming_language"] == "Python"  # Built-in default


# === create_enterprise_session tests ===


@pytest.mark.asyncio
async def test_session_enterprise_create_success():
    """Test successful enterprise session creation."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_factory_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    # Configure the chain of mocks
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_factory_registry
    )
    mock_enterprise_factory_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    # Mock session registry get to raise KeyError for non-existent sessions (indicates session doesn't exist yet)
    mock_session_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    # Mock config
    enterprise_config = {
        "test-system": {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {"heap_size_gb": 4.0, "programming_language": "Python"},
            },
            "username": "testuser",
        }
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    mock_config_manager.get_config = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Clear any existing tracking
    mcp_mod._created_sessions = {}

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await mcp_mod.session_enterprise_create(
        context,
        system_name="test-system",
        session_name="test-session",
        heap_size_gb=8.0,
        programming_language="Groovy",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:test-session"
    assert result["system_name"] == "test-system"
    assert result["session_name"] == "test-session"

    # Verify session was added to registry
    mock_session_registry.add_session.assert_called_once()
    call_args = mock_session_registry.add_session.call_args
    session_manager = call_args[0][0]  # Manager is the only argument
    assert session_manager.full_name == "enterprise:test-system:test-session"

    # Session tracking is now verified through registry methods
    # Verify session was added (tracked automatically by add_session)


@pytest.mark.asyncio
async def test_session_enterprise_create_auto_generated_name():
    """Test enterprise session creation with auto-generated session name."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_factory_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_session = MagicMock()

    # Configure the chain of mocks
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_factory_registry
    )
    mock_enterprise_factory_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.connect_to_new_worker = AsyncMock(return_value=mock_session)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    # Mock session registry get to raise KeyError for non-existent sessions (indicates session doesn't exist yet)
    mock_session_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    # Mock config with username
    enterprise_config = {
        "test-system": {
            "session_creation": {"max_concurrent_sessions": 5, "defaults": {}},
            "username": "alice",
        }
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    mock_config_manager.get_config = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Clear any existing tracking
    mcp_mod._created_sessions = {}

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await mcp_mod.session_enterprise_create(
        context,
        system_name="test-system",
        session_name=None,  # This should trigger auto-generation
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"].startswith("enterprise:test-system:mcp-alice-")
    assert result["system_name"] == "test-system"
    assert result["session_name"].startswith("mcp-alice-")

    # Clean up
    mcp_mod._created_sessions = {}


@pytest.mark.asyncio
async def test_session_enterprise_create_max_sessions_reached():
    """Test enterprise session creation when max concurrent sessions reached."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config with low max limit
    enterprise_config = {
        "test-system": {
            "session_creation": {"max_concurrent_sessions": 2, "defaults": {}}
        }
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    mock_config_manager.get_config = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Mock registry to return 2 existing sessions (at limit)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    # Mock the session registry to return sessions for count validation
    async def mock_get(session_id):
        if session_id in [
            "enterprise:test-system:session1",
            "enterprise:test-system:session2",
        ]:
            return MagicMock()
        raise KeyError(f"Session {session_id} not found")

    mock_session_registry.get = AsyncMock(side_effect=mock_get)

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await mcp_mod.session_enterprise_create(
        context, system_name="test-system", session_name="test-session"
    )

    # Verify failure due to max sessions reached
    assert result["success"] is False
    assert result["isError"] is True
    assert "Max concurrent sessions (2) reached" in result["error"]

    # Clean up
    mcp_mod._created_sessions = {}


@pytest.mark.asyncio
async def test_session_enterprise_create_disabled():
    """Test enterprise session creation when session creation is disabled."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config with session creation disabled
    enterprise_config = {
        "test-system": {
            "session_creation": {
                "max_concurrent_sessions": 0,  # Disabled
                "defaults": {},
            }
        }
    }

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Provide nested enterprise systems config via async get_config()
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    result = await mcp_mod.session_enterprise_create(
        context, system_name="test-system", session_name="test-session"
    )

    # Verify failure due to disabled session creation
    assert result["success"] is False
    assert result["isError"] is True
    assert "Session creation is disabled" in result["error"]


@pytest.mark.asyncio
async def test_session_enterprise_create_system_not_found_v2():
    """Test enterprise session creation with non-existent system."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Provide empty systems via async get_config()
    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    result = await mcp_mod.session_enterprise_create(
        context, system_name="nonexistent-system", session_name="test-session"
    )

    # Verify failure due to system not found
    assert result["success"] is False
    assert result["isError"] is True
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]


# === delete_enterprise_session tests ===


@pytest.mark.asyncio
async def test_session_enterprise_delete_success_v2():
    """Test successful enterprise session deletion."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_session_manager = MagicMock(spec=mcp_mod.EnterpriseSessionManager)

    # Mock config
    enterprise_config = {
        "test-system": {"session_creation": {"max_concurrent_sessions": 5}}
    }

    def mock_get_config_section(manager, section):
        if section == "enterprise_sessions":
            return enterprise_config
        return {}

    # Mock session registry
    mock_session_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.close = AsyncMock()
    mock_session_registry.remove_session = AsyncMock(return_value=mock_session_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Session tracking is now handled by registry - no manual setup needed

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    result = await mcp_mod.session_enterprise_delete(
        context, system_name="test-system", session_name="test-session"
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "enterprise:test-system:test-session"
    assert result["system_name"] == "test-system"
    assert result["session_name"] == "test-session"

    # Verify session was removed from registry
    mock_session_registry.remove_session.assert_called_once_with(
        "enterprise:test-system:test-session"
    )

    # Session tracking cleanup is now handled automatically by remove_session()


@pytest.mark.asyncio
async def test_session_enterprise_delete_not_found():
    """Test enterprise session deletion when session doesn't exist."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config
    enterprise_config = {
        "test-system": {"session_creation": {"max_concurrent_sessions": 5}}
    }
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    # Mock session registry to return KeyError for non-existent session
    mock_session_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_enterprise_delete(
        context, system_name="test-system", session_name="nonexistent-session"
    )

    # Verify failure due to session not found
    assert result["success"] is False
    assert result["isError"] is True
    assert (
        "Session 'enterprise:test-system:nonexistent-session' not found"
        in result["error"]
    )


@pytest.mark.asyncio
async def test_session_enterprise_delete_system_not_found_v2():
    """Test enterprise session deletion with non-existent system."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # No systems configured
    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_enterprise_delete(
        context, system_name="nonexistent-system", session_name="test-session"
    )

    # Verify failure due to system not found
    assert result["success"] is False
    assert result["isError"] is True
    assert "Enterprise system 'nonexistent-system' not found" in result["error"]


# === Helper Function Tests ===


@pytest.mark.asyncio
async def test_check_session_limits_disabled():
    """Test _check_session_limits when sessions are disabled (max_sessions = 0)."""
    mock_session_registry = MagicMock()

    result = await _check_session_limits(mock_session_registry, "test-system", 0)

    assert result is not None
    assert (
        result["error"]
        == "Session creation is disabled for system 'test-system' (max_concurrent_sessions = 0)"
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_check_session_limits_under_limit():
    """Test _check_session_limits when under the session limit."""
    mock_session_registry = MagicMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    result = await _check_session_limits(mock_session_registry, "test-system", 5)

    assert result is None  # No error when under limit
    mock_session_registry.count_added_sessions.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_session_limits_at_limit():
    """Test _check_session_limits when at the session limit."""
    mock_session_registry = MagicMock()
    mock_session_registry.count_added_sessions = AsyncMock(return_value=5)

    result = await _check_session_limits(mock_session_registry, "test-system", 5)

    assert result is not None
    assert (
        result["error"]
        == "Max concurrent sessions (5) reached for system 'test-system'"
    )
    assert result["isError"] is True
    mock_session_registry.count_added_sessions.assert_awaited_once()


def test_generate_session_name_if_none_with_name():
    """Test _generate_session_name_if_none when session_name is provided."""
    system_config = {"username": "testuser"}

    result = _generate_session_name_if_none(system_config, "provided-name")

    assert result == "provided-name"


def test_generate_session_name_if_none_with_username():
    """Test _generate_session_name_if_none when no name provided but username exists."""
    system_config = {"username": "testuser"}

    with patch("deephaven_mcp.mcp_systems_server._mcp.datetime") as mock_datetime:
        mock_datetime.now().strftime.return_value = "20240101-1200"
        result = _generate_session_name_if_none(system_config, None)

    assert result == "mcp-testuser-20240101-1200"


def test_generate_session_name_if_none_without_username():
    """Test _generate_session_name_if_none when no name or username provided."""
    system_config = {}  # No username

    with patch("deephaven_mcp.mcp_systems_server._mcp.datetime") as mock_datetime:
        mock_datetime.now().strftime.return_value = "20240101-1200"
        result = _generate_session_name_if_none(system_config, None)

    assert result == "mcp-session-20240101-1200"


@pytest.mark.asyncio
async def test_check_session_id_available_success():
    """Test _check_session_id_available when session ID is available."""
    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    result = await _check_session_id_available(mock_session_registry, "test-session-id")

    assert result is None  # No error when session doesn't exist


@pytest.mark.asyncio
async def test_check_session_id_available_conflict():
    """Test _check_session_id_available when session ID already exists."""
    mock_session_registry = MagicMock()
    mock_session_registry.get = AsyncMock(return_value=MagicMock())  # Session exists

    result = await _check_session_id_available(
        mock_session_registry, "existing-session-id"
    )

    assert result is not None
    assert result["error"] == "Session 'existing-session-id' already exists"
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_get_system_config_success():
    """Test _get_system_config when system exists in configuration."""
    mock_config_manager = MagicMock()

    enterprise_config = {
        "test-system": {
            "connection_json_url": "https://test.com",
            "auth_type": "password",
        }
    }

    # New config access path uses async get_config() with nested structure
    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    system_config, error_response = await _get_system_config(
        "test_function", mock_config_manager, "test-system"
    )

    assert error_response is None
    assert system_config == enterprise_config["test-system"]


@pytest.mark.asyncio
async def test_get_system_config_system_not_found():
    """Test _get_system_config when system doesn't exist in configuration."""
    mock_config_manager = MagicMock()

    enterprise_config = {"other-system": {}}

    full_config = {"enterprise": {"systems": enterprise_config}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    system_config, error_response = await _get_system_config(
        "test_function", mock_config_manager, "nonexistent-system"
    )

    assert system_config == {}
    assert error_response is not None
    assert (
        error_response["error"]
        == "Enterprise system 'nonexistent-system' not found in configuration"
    )
    assert error_response["isError"] is True


@pytest.mark.asyncio
async def test_get_system_config_handles_keyerror_from_get_config_section():
    """Covers the KeyError except block when extracting nested enterprise systems config."""
    mock_config_manager = MagicMock()
    mock_config_manager.get_config = AsyncMock(return_value={"some": "config"})

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.get_config_section",
        side_effect=KeyError(),
    ):
        system_config, error_response = await _get_system_config(
            "test_function", mock_config_manager, "missing-system"
        )

    assert system_config == {}
    assert error_response is not None
    assert "missing-system" in error_response["error"]
    assert error_response["isError"] is True


@pytest.mark.asyncio
async def test_get_system_config_empty_config():
    """Test _get_system_config when enterprise config is empty."""
    mock_config_manager = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    system_config, error_response = await _get_system_config(
        "test_function", mock_config_manager, "test-system"
    )

    assert system_config == {}
    assert error_response is not None
    assert (
        error_response["error"]
        == "Enterprise system 'test-system' not found in configuration"
    )
    assert error_response["isError"] is True


def test_resolve_session_parameters_with_defaults():
    """Test _resolve_session_parameters using configuration defaults."""
    defaults = {
        "heap_size_gb": 8.0,
        "auto_delete_timeout": 3600,
        "server": "default-server",
        "programming_language": "Python",
    }

    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=None,
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=None,
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 8.0
    assert result["auto_delete_timeout"] == 3600
    assert result["server"] == "default-server"
    assert result["engine"] == "DeephavenCommunity"  # Default when not specified
    assert result["programming_language"] == "Python"


def test_resolve_session_parameters_with_overrides():
    """Test _resolve_session_parameters with parameter overrides."""
    defaults = {
        "heap_size_gb": 8.0,
        "auto_delete_timeout": 3600,
        "programming_language": "Python",
    }

    result = _resolve_session_parameters(
        heap_size_gb=16.0,  # Override
        auto_delete_timeout=7200,  # Override
        server="custom-server",  # Override
        engine="CustomEngine",  # Override
        extra_jvm_args=["-Xms4g"],
        extra_environment_vars=["VAR=value"],
        admin_groups=["admins"],
        viewer_groups=["viewers"],
        timeout_seconds=300.0,
        session_arguments={"arg": "value"},
        programming_language="Groovy",  # Override
        defaults=defaults,
    )

    assert result["heap_size_gb"] == 16.0
    assert result["auto_delete_timeout"] == 7200
    assert result["server"] == "custom-server"
    assert result["engine"] == "CustomEngine"
    assert result["extra_jvm_args"] == ["-Xms4g"]
    assert result["extra_environment_vars"] == ["VAR=value"]
    assert result["admin_groups"] == ["admins"]
    assert result["viewer_groups"] == ["viewers"]
    assert result["timeout_seconds"] == 300.0
    assert result["session_arguments"] == {"arg": "value"}
    assert result["programming_language"] == "Groovy"


def test_resolve_session_parameters_zero_values():
    """Test _resolve_session_parameters handles zero values correctly."""
    defaults = {
        "auto_delete_timeout": 3600,
        "timeout_seconds": 120.0,
    }

    result = _resolve_session_parameters(
        heap_size_gb=None,
        auto_delete_timeout=0,  # Explicitly set to 0
        server=None,
        engine=None,
        extra_jvm_args=None,
        extra_environment_vars=None,
        admin_groups=None,
        viewer_groups=None,
        timeout_seconds=0.0,  # Explicitly set to 0.0
        session_arguments=None,
        programming_language=None,
        defaults=defaults,
    )

    assert result["auto_delete_timeout"] == 0  # Should use explicit 0, not default
    assert result["timeout_seconds"] == 0.0  # Should use explicit 0.0, not default


# ===== catalog_tables tests =====


@pytest.mark.asyncio
async def test_catalog_tables_success_no_filters():
    """Test catalog_tables with no filters and default max_rows."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=100)
    mock_catalog_table.nbytes = 5000  # Small size, under limit
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_field2 = MagicMock()
    mock_field2.name = "TableName"
    mock_field2.type = "string"
    mock_catalog_table.schema = [mock_field1, mock_field2]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = (
                "json-row",
                [{"Namespace": "ns1", "TableName": "t1"}],
            )

            result = await mcp_mod.catalog_tables_list(
                context, "enterprise:prod:analytics"
            )

            assert result["success"] is True
            assert result["session_id"] == "enterprise:prod:analytics"
            assert result["format"] == "json-row"
            assert result["row_count"] == 100
            assert result["is_complete"] is True
            assert len(result["columns"]) == 2
            assert result["data"] == [{"Namespace": "ns1", "TableName": "t1"}]

            mock_get_catalog.assert_called_once_with(
                mock_session, max_rows=10000, filters=None, distinct_namespaces=False
            )


@pytest.mark.asyncio
async def test_catalog_tables_success_with_filters():
    """Test catalog with filters applied."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=50)
    mock_catalog_table.nbytes = 2500
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_catalog_table.schema = [mock_field1]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [{"Namespace": "market_data"}])

            filters = ["Namespace = `market_data`", "TableName.contains(`price`)"]
            result = await mcp_mod.catalog_tables_list(
                context, "enterprise:prod:analytics", filters=filters
            )

            assert result["success"] is True
            assert result["row_count"] == 50
            assert result["is_complete"] is True

            mock_get_catalog.assert_called_once_with(
                mock_session, max_rows=10000, filters=filters, distinct_namespaces=False
            )


@pytest.mark.asyncio
async def test_catalog_tables_success_csv_format():
    """Test catalog with CSV format."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=10)
    mock_catalog_table.nbytes = 500
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_catalog_table.schema = [mock_field1]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("csv", "Namespace\nmarket_data\n")

            result = await mcp_mod.catalog_tables_list(
                context, "enterprise:prod:analytics", format="csv"
            )

            assert result["success"] is True
            assert result["format"] == "csv"
            assert isinstance(result["data"], str)

            mock_format.assert_called_once_with(mock_catalog_table, "csv")


@pytest.mark.asyncio
async def test_catalog_tables_incomplete_results():
    """Test catalog when results are incomplete (truncated by max_rows)."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=1000)
    mock_catalog_table.nbytes = 50000
    mock_field1 = MagicMock()
    mock_field1.name = "Namespace"
    mock_field1.type = "string"
    mock_catalog_table.schema = [mock_field1]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, False)  # Incomplete

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [])

            result = await mcp_mod.catalog_tables_list(
                context, "enterprise:prod:analytics", max_rows=1000
            )

            assert result["success"] is True
            assert result["is_complete"] is False  # Truncated
            assert result["row_count"] == 1000


@pytest.mark.asyncio
async def test_catalog_tables_not_enterprise_session():
    """Test catalog with non-enterprise session (should fail)."""
    from deephaven_mcp._exceptions import UnsupportedOperationError

    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = UnsupportedOperationError(
            "get_catalog_table only supports enterprise (Core+) sessions"
        )

        result = await mcp_mod.catalog_tables_list(context, "community:local:test")

        assert result["success"] is False
        assert "enterprise" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_session_not_found():
    """Test catalog when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await mcp_mod.catalog_tables_list(context, "invalid_session")

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_invalid_filter():
    """Test catalog with invalid filter syntax."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = RuntimeError("Invalid filter syntax")

        result = await mcp_mod.catalog_tables_list(
            context, "enterprise:prod:analytics", filters=["InvalidFilter!!!"]
        )

        assert result["success"] is False
        assert "Invalid filter syntax" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_invalid_format():
    """Test catalog with invalid format parameter."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=10)
    mock_catalog_table.nbytes = 500

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.side_effect = ValueError("Unsupported format: invalid")

            result = await mcp_mod.catalog_tables_list(
                context, "enterprise:prod:analytics", format="invalid"
            )

            assert result["success"] is False
            assert "Unsupported format" in result["error"]
            assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_tables_size_limit_exceeded():
    """Test catalog when response size exceeds limit."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock catalog arrow table with size exceeding limit
    mock_catalog_table = MagicMock()
    mock_catalog_table.__len__ = MagicMock(return_value=1000000)
    mock_catalog_table.nbytes = 60_000_000  # 60MB, exceeds 50MB limit

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, False)

        result = await mcp_mod.catalog_tables_list(context, "enterprise:prod:analytics")

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


# ===== catalog_namespaces tests =====


@pytest.mark.asyncio
async def test_catalog_namespaces_success_no_filters():
    """Test catalog_namespaces with no filters and default max_rows."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=25)
    namespaces_table_mock.nbytes = 1000  # Small size, under limit
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [{"Namespace": "market_data"}])

            result = await mcp_mod.catalog_namespaces_list(
                context, "enterprise:prod:analytics"
            )

            assert result["success"] is True
            assert result["session_id"] == "enterprise:prod:analytics"
            assert result["format"] == "json-row"
            assert result["row_count"] == 25
            assert result["is_complete"] is True
            assert len(result["columns"]) == 1
            assert result["data"] == [{"Namespace": "market_data"}]

            mock_get_namespaces.assert_called_once_with(
                mock_session, max_rows=1000, filters=None, distinct_namespaces=True
            )


@pytest.mark.asyncio
async def test_catalog_namespaces_success_with_filters():
    """Test catalog_namespaces with filters applied."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=5)
    namespaces_table_mock.nbytes = 500
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [{"Namespace": "market_data"}])

            filters = ["TableName.contains(`daily`)"]
            result = await mcp_mod.catalog_namespaces_list(
                context, "enterprise:prod:analytics", filters=filters
            )

            assert result["success"] is True
            assert result["row_count"] == 5
            assert result["is_complete"] is True

            mock_get_namespaces.assert_called_once_with(
                mock_session, max_rows=1000, filters=filters, distinct_namespaces=True
            )


@pytest.mark.asyncio
async def test_catalog_namespaces_success_csv_format():
    """Test catalog_namespaces with CSV format."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=10)
    namespaces_table_mock.nbytes = 500
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("csv", "Namespace\nmarket_data\n")

            result = await mcp_mod.catalog_namespaces_list(
                context, "enterprise:prod:analytics", format="csv"
            )

            assert result["success"] is True
            assert result["format"] == "csv"
            assert isinstance(result["data"], str)

            mock_format.assert_called_once_with(namespaces_table_mock, "csv")


@pytest.mark.asyncio
async def test_catalog_namespaces_incomplete_results():
    """Test catalog_namespaces when results are incomplete (truncated by max_rows)."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=500)
    namespaces_table_mock.nbytes = 25000
    mock_field = MagicMock()
    mock_field.name = "Namespace"
    mock_field.type = "string"
    namespaces_table_mock.schema = [mock_field]

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, False)  # Incomplete

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.return_value = ("json-row", [])

            result = await mcp_mod.catalog_namespaces_list(
                context, "enterprise:prod:analytics", max_rows=500
            )

            assert result["success"] is True
            assert result["is_complete"] is False  # Truncated
            assert result["row_count"] == 500


@pytest.mark.asyncio
async def test_catalog_namespaces_not_enterprise_session():
    """Test catalog_namespaces with non-enterprise session (should fail)."""
    from deephaven_mcp._exceptions import UnsupportedOperationError

    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.side_effect = UnsupportedOperationError(
            "get_catalog_namespaces only supports enterprise (Core+) sessions"
        )

        result = await mcp_mod.catalog_namespaces_list(context, "community:local:test")

        assert result["success"] is False
        assert "enterprise" in result["error"].lower()
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_session_not_found():
    """Test catalog_namespaces when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await mcp_mod.catalog_namespaces_list(context, "invalid_session")

    assert result["success"] is False
    assert "Session not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_invalid_format():
    """Test catalog_namespaces with invalid format parameter."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=10)
    namespaces_table_mock.nbytes = 500

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, True)

        with patch(
            "deephaven_mcp.mcp_systems_server._mcp.format_table_data"
        ) as mock_format:
            mock_format.side_effect = ValueError("Unsupported format: invalid")

            result = await mcp_mod.catalog_namespaces_list(
                context, "enterprise:prod:analytics", format="invalid"
            )

            assert result["success"] is False
            assert "Unsupported format" in result["error"]
            assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_namespaces_size_limit_exceeded():
    """Test catalog_namespaces when response size exceeds limit."""
    mock_registry = MagicMock()
    mock_session_manager = MagicMock()
    mock_session = MagicMock()

    mock_registry.get = AsyncMock(return_value=mock_session_manager)
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    context = MockContext({"session_registry": mock_registry})

    # Mock namespaces arrow table with size exceeding limit
    namespaces_table_mock = MagicMock()
    namespaces_table_mock.__len__ = MagicMock(return_value=100000)
    namespaces_table_mock.nbytes = 60_000_000  # 60MB, exceeds 50MB limit

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_namespaces:
        mock_get_namespaces.return_value = (namespaces_table_mock, False)

        result = await mcp_mod.catalog_namespaces_list(
            context, "enterprise:prod:analytics"
        )

        assert result["success"] is False
        assert "50MB" in result["error"] or "max" in result["error"].lower()
        assert result["isError"] is True


# === catalog_tables_schema ===


@pytest.mark.asyncio
async def test_catalog_tables_schema_success_with_namespace():
    """Test catalog_schemas with namespace filter."""
    from deephaven_mcp.client import CorePlusSession

    # Create mock CorePlusSession
    mock_session = MagicMock(spec=CorePlusSession)

    # Mock catalog table data
    catalog_data = [
        {"Namespace": "market_data", "TableName": "daily_prices"},
        {"Namespace": "market_data", "TableName": "quotes"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function
    schema_map = {
        "daily_prices": [
            {"Name": "Date", "DataType": "LocalDate"},
            {"Name": "Price", "DataType": "double"},
        ],
        "quotes": [
            {"Name": "Date", "DataType": "LocalDate"},
            {"Name": "Price", "DataType": "double"},
        ],
    }
    mock_get_catalog_meta = create_mock_catalog_schema_function(schema_map)

    # Set up session manager and registry
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    # Mock both queries functions
    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await mcp_mod.catalog_tables_schema(
            context, "enterprise:prod:analytics", namespace="market_data"
        )

    # Verify result
    assert result["success"] is True
    assert result["count"] == 2
    assert result["is_complete"] is True
    assert len(result["schemas"]) == 2

    # Check first schema - now expects full metadata in 'data' field
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][0]["namespace"] == "market_data"
    assert result["schemas"][0]["table"] == "daily_prices"
    assert "data" in result["schemas"][0]
    assert len(result["schemas"][0]["data"]) == 2  # 2 columns
    assert "meta_columns" in result["schemas"][0]
    assert "row_count" in result["schemas"][0]

    # Check second schema
    assert result["schemas"][1]["success"] is True
    assert result["schemas"][1]["namespace"] == "market_data"
    assert result["schemas"][1]["table"] == "quotes"
    assert "data" in result["schemas"][1]


@pytest.mark.asyncio
async def test_catalog_tables_schema_success_with_table_names():
    """Test catalog_schemas with specific table_names filter."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Catalog will be filtered by table_names at the catalog level
    # So it should only return the requested table
    catalog_data = [
        {"Namespace": "market_data", "TableName": "quotes"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function (default schema for all tables)
    mock_get_catalog_meta = create_mock_catalog_schema_function({})

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await mcp_mod.catalog_tables_schema(
            context,
            "enterprise:prod:analytics",
            table_names=["quotes"],  # Only request quotes
        )

        # Verify the filter was passed correctly to get_catalog_table
        call_args = mock_get_catalog.call_args
        assert call_args[1]["filters"] == ["TableName in `quotes`"]

    # Should only return 1 schema (quotes)
    assert result["success"] is True
    assert result["count"] == 1
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["table"] == "quotes"


@pytest.mark.asyncio
async def test_catalog_tables_schema_max_tables_limit():
    """Test catalog_schemas respects max_tables limit."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Create 150 catalog entries, but get_catalog_table will only return 50 due to max_rows=50
    catalog_data = [
        {"Namespace": "market_data", "TableName": f"table_{i}"}
        for i in range(50)  # Only 50 entries returned due to max_rows limit
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function (default schema for all tables)
    mock_get_catalog_meta = create_mock_catalog_schema_function({})

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (
            mock_catalog_table,
            False,
        )  # is_complete=False because truncated
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await mcp_mod.catalog_tables_schema(
            context,
            "enterprise:prod:analytics",
            max_tables=50,  # Limit to 50
        )

    # Should only return 50 schemas
    assert result["success"] is True
    assert result["count"] == 50
    assert result["is_complete"] is False  # Truncated
    assert len(result["schemas"]) == 50


@pytest.mark.asyncio
async def test_catalog_tables_schema_not_enterprise_session():
    """Test catalog_schemas fails with non-enterprise session."""
    # Create a non-CorePlusSession mock
    mock_session = MagicMock()  # Not CorePlusSession

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    result = await mcp_mod.catalog_tables_schema(
        context, "community:local:test", namespace="market_data"
    )

    # Should fail with error about enterprise-only
    assert result["success"] is False
    assert result["isError"] is True
    assert "enterprise" in result["error"].lower() or "Core+" in result["error"]


@pytest.mark.asyncio
async def test_catalog_tables_schema_mixed_success_failure():
    """Test catalog_schemas with some tables succeeding and some failing."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    catalog_data = [
        {"Namespace": "market_data", "TableName": "good_table"},
        {"Namespace": "market_data", "TableName": "bad_table"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function - fail for bad_table
    mock_get_catalog_meta = create_mock_catalog_schema_function(
        schema_data_map={}, error_tables={"bad_table"}
    )

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await mcp_mod.catalog_tables_schema(
            context, "enterprise:prod:analytics"
        )

    # Overall operation should succeed
    assert result["success"] is True
    assert result["count"] == 2

    # First table should succeed
    assert result["schemas"][0]["success"] is True
    assert result["schemas"][0]["table"] == "good_table"

    # Second table should fail
    assert result["schemas"][1]["success"] is False
    assert result["schemas"][1]["table"] == "bad_table"
    assert result["schemas"][1]["isError"] is True
    assert "not found" in result["schemas"][1]["error"].lower()


@pytest.mark.asyncio
async def test_catalog_tables_schema_session_not_found():
    """Test catalog_schemas when session is not found."""
    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(side_effect=Exception("Session not found"))

    context = MockContext({"session_registry": mock_registry})

    result = await mcp_mod.catalog_tables_schema(context, "enterprise:prod:nonexistent")

    assert result["success"] is False
    assert result["isError"] is True
    assert "Session not found" in result["error"]


@pytest.mark.asyncio
async def test_catalog_tables_schema_catalog_retrieval_error():
    """Test catalog_schemas when catalog table retrieval fails."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    # Mock queries.get_catalog_table to raise error
    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.side_effect = Exception("Catalog access denied")

        result = await mcp_mod.catalog_tables_schema(
            context, "enterprise:prod:analytics"
        )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Catalog access denied" in result["error"]


@pytest.mark.asyncio
async def test_catalog_tables_schema_with_filters():
    """Test catalog_schemas with custom filters."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Catalog will be pre-filtered by queries.get_catalog_table
    catalog_data = [
        {"Namespace": "market_data", "TableName": "daily_prices"},
    ]

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    # Use helper to create mock schema function with specific schema for daily_prices
    schema_map = {"daily_prices": [{"Name": "Price", "DataType": "double"}]}
    mock_get_catalog_meta = create_mock_catalog_schema_function(schema_map)

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
        ) as mock_get_catalog,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_meta_table"
        ) as mock_get_schema,
    ):
        mock_get_catalog.return_value = (mock_catalog_table, True)
        mock_get_schema.side_effect = mock_get_catalog_meta

        result = await mcp_mod.catalog_tables_schema(
            context,
            "enterprise:prod:analytics",
            filters=["TableName.contains(`price`)"],
        )

        # Verify filters were passed to get_catalog_table
        call_args = mock_get_catalog.call_args
        assert call_args[1]["filters"] == ["TableName.contains(`price`)"]

    assert result["success"] is True
    assert result["count"] == 1
    assert result["schemas"][0]["table"] == "daily_prices"


@pytest.mark.asyncio
async def test_catalog_tables_schema_empty_catalog():
    """Test catalog_schemas when catalog has no matching tables."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)

    # Empty catalog
    catalog_data = []

    mock_catalog_table = MagicMock()
    mock_catalog_table.to_pylist = MagicMock(return_value=catalog_data)

    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"
    ) as mock_get_catalog:
        mock_get_catalog.return_value = (mock_catalog_table, True)

        result = await mcp_mod.catalog_tables_schema(
            context, "enterprise:prod:analytics", namespace="nonexistent"
        )

    assert result["success"] is True
    assert result["count"] == 0
    assert result["is_complete"] is True
    assert len(result["schemas"]) == 0


# ===== _get_enterprise_session tests =====


@pytest.mark.asyncio
async def test_get_enterprise_session_success():
    """Test _get_enterprise_session with a valid CorePlusSession."""
    from deephaven_mcp.client import CorePlusSession
    from deephaven_mcp.mcp_systems_server._mcp import _get_enterprise_session

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    session, error = await _get_enterprise_session(
        "test_function", context, "test-session-id"
    )

    assert session is mock_session  # Returns the validated session
    assert error is None


@pytest.mark.asyncio
async def test_get_enterprise_session_not_enterprise():
    """Test _get_enterprise_session with a non-enterprise session."""
    from deephaven_mcp.client import BaseSession
    from deephaven_mcp.mcp_systems_server._mcp import _get_enterprise_session

    mock_session = MagicMock(spec=BaseSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    session, error = await _get_enterprise_session(
        "test_function", context, "test-session-id"
    )

    assert session is None
    assert error is not None
    assert error["success"] is False
    assert "test_function only works with enterprise (Core+) sessions" in error["error"]
    assert "test-session-id" in error["error"]
    assert error["isError"] is True


# ===== catalog_table_sample tests =====


@pytest.mark.asyncio
async def test_catalog_table_sample_success():
    """Test catalog_table_sample with successful data retrieval."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    # Mock arrow table with data
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=10)
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=3)
    mock_arrow_table.to_pydict = MagicMock(
        return_value={
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
            "col3": [4.5, 5.5, 6.5],
        }
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, True)

        result = await mcp_mod.catalog_table_sample(
            context, "enterprise:prod:analytics", "public", "users", max_rows=10
        )

    assert result["success"] is True
    assert result["row_count"] == 10
    assert result["is_complete"] is True
    assert (
        result["format"] == "markdown-table"
    )  # Default format changed to markdown-table
    assert result["namespace"] == "public"
    assert result["table_name"] == "users"
    assert "data" in result


@pytest.mark.asyncio
async def test_catalog_table_sample_with_format():
    """Test catalog_table_sample with different format."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=5)
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=2)
    mock_arrow_table.to_pydict = MagicMock(
        return_value={"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    )

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, False)

        result = await mcp_mod.catalog_table_sample(
            context,
            "enterprise:prod:analytics",
            "analytics",
            "events",
            max_rows=5,
            format="markdown-table",
        )

    assert result["success"] is True
    assert result["format"] == "markdown-table"
    assert result["is_complete"] is False


@pytest.mark.asyncio
async def test_catalog_table_sample_not_enterprise_session():
    """Test catalog_table_sample with non-enterprise session."""
    from deephaven_mcp.client import BaseSession

    mock_session = MagicMock(spec=BaseSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    result = await mcp_mod.catalog_table_sample(
        context, "enterprise:prod:analytics", "public", "users"
    )

    assert result["success"] is False
    assert "only works with enterprise (Core+) sessions" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_table_sample_exception():
    """Test catalog_table_sample with exception during data retrieval."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.side_effect = Exception("Database connection failed")

        result = await mcp_mod.catalog_table_sample(
            context, "enterprise:prod:analytics", "public", "users"
        )

    assert result["success"] is False
    assert "Database connection failed" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_catalog_table_sample_response_too_large():
    """Test catalog_table_sample when response size exceeds limit."""
    from deephaven_mcp.client import CorePlusSession

    mock_session = MagicMock(spec=CorePlusSession)
    mock_session_manager = MagicMock()
    mock_session_manager.get = AsyncMock(return_value=mock_session)

    mock_registry = MagicMock()
    mock_registry.get = AsyncMock(return_value=mock_session_manager)

    context = MockContext({"session_registry": mock_registry})

    # Mock arrow table with huge data that exceeds size limit
    mock_arrow_table = MagicMock()
    mock_arrow_table.__len__ = MagicMock(return_value=100000)  # 100k rows
    mock_arrow_table.schema = MagicMock()
    mock_arrow_table.schema.__len__ = MagicMock(return_value=100)  # 100 columns

    with patch(
        "deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table_data"
    ) as mock_get_data:
        mock_get_data.return_value = (mock_arrow_table, True)

        result = await mcp_mod.catalog_table_sample(
            context, "enterprise:prod:analytics", "public", "huge_table"
        )

    assert result["success"] is False
    assert "max 50MB" in result["error"]
    assert "reduce max_rows" in result["error"]
    assert result["isError"] is True


# =============================================================================
# Tests for session_community_create and session_community_delete
# (Consolidated from test_session_community_tools.py)
# =============================================================================


@pytest.mark.asyncio
async def test_session_community_create_success():
    """Test successful community session creation."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config
    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "launch_method": "docker",
                "auth_type": "PSK",
                "heap_size_gb": 4.0,
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get_all = AsyncMock(return_value={})

    # Mock launcher
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test_token"
    )
    mock_launched_session.container_id = "test_container"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
        ),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_create(
            context,
            session_name="test-session",
        )

        # Verify success
        assert result["success"] is True
        assert result["session_id"] == "community:dynamic:test-session"
        assert result["session_name"] == "test-session"
        assert result["port"] == 10000
        assert "connection_url" in result

        # Verify session was added to registry
        mock_session_registry.add_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_create_not_configured():
    """Test community session creation when not configured."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # No session_creation config
    full_config = {"community": {}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await mcp_mod.session_community_create(
        context,
        session_name="test-session",
    )

    # Verify error
    assert result["success"] is False
    assert "not configured" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_sessions_disabled():
    """Test community session creation when max_concurrent_sessions is 0 (disabled)."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 0,  # Disabled
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get_all = AsyncMock(return_value={})

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test_token"
    )
    mock_launched_session.container_id = "test_container"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session,
            "wait_until_ready",
            new=AsyncMock(return_value=True),
        ),
    ):
        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_create(
            context,
            session_name="test-session",
        )

        # Should succeed - limit is disabled so no limit check
        assert result["success"] is True
        assert result["session_id"] == "community:dynamic:test-session"
        # count_added_sessions should NOT have been called since limit is disabled
        mock_session_registry.count_added_sessions.assert_not_called()


@pytest.mark.asyncio
async def test_session_community_create_max_sessions_reached():
    """Test community session creation when max sessions reached."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 2,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=2)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await mcp_mod.session_community_create(
        context,
        session_name="test-session",
    )

    # Verify error
    assert result["success"] is False
    assert "Session limit reached" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_launch_failure():
    """Test community session creation when launch fails."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get_all = AsyncMock(return_value={})

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
            return_value="token",
        ),
    ):

        mock_launch_session.side_effect = Exception("Launch failed")

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_create(
            context,
            session_name="test-session",
        )

        # Verify error
        assert result["success"] is False
        assert "Launch failed" in result["error"]
        assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_success():
    """Test successful community session deletion."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock launched session (Docker by default)
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.launch_method = "docker"

    # Create a mock dynamic session manager
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:dynamic:test-session"
    mock_manager._name = "test-session"
    mock_manager.source = "dynamic"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove_session = AsyncMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await mcp_mod.session_community_delete(
        context,
        session_name="test-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:dynamic:test-session"
    assert result["session_name"] == "test-session"

    # Verify session was closed and removed
    mock_manager.close.assert_called_once()
    mock_session_registry.remove_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_python_session():
    """Test deleting a python-launched session to cover untrack_python_process call."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_instance_tracker = create_mock_instance_tracker()

    # Create a mock python-launched session
    mock_launched_session = MagicMock(spec=PythonLaunchedSession)
    mock_launched_session.launch_method = "python"

    # Create a mock python-launched session manager
    mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
    mock_manager.full_name = "community:dynamic:python-session"
    mock_manager._name = "python-session"
    mock_manager.source = "dynamic"
    mock_manager.system_type = SystemType.COMMUNITY
    mock_manager.launched_session = mock_launched_session
    mock_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_manager)
    mock_session_registry.remove_session = AsyncMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": mock_instance_tracker,
        }
    )

    result = await mcp_mod.session_community_delete(
        context,
        session_name="python-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:dynamic:python-session"

    # Verify untrack_python_process was called (line 4197)
    mock_instance_tracker.untrack_python_process.assert_called_once_with(
        "python-session"
    )

    # Verify session was closed and removed
    mock_manager.close.assert_called_once()
    mock_session_registry.remove_session.assert_called_once()


@pytest.mark.asyncio
async def test_session_community_delete_not_found():
    """Test community session deletion when session not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_session_registry.get = AsyncMock(side_effect=KeyError("Not found"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await mcp_mod.session_community_delete(
        context,
        session_name="nonexistent",
    )

    # Verify error
    assert result["success"] is False
    assert "not found" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_not_dynamic():
    """Test community session deletion when session is not dynamic."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock static session manager (not dynamic)
    mock_manager = MagicMock()
    mock_manager.full_name = "community:static:test-session"
    mock_manager.source = "static"  # Not dynamic!
    mock_manager.system_type = SystemType.COMMUNITY

    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    result = await mcp_mod.session_community_delete(
        context,
        session_name="test-session",
    )

    # Verify error
    assert result["success"] is False
    assert "Only dynamically created sessions" in result["error"]
    assert result["isError"] is True


# =============================================================================
# Complete coverage tests for session_community_create and session_community_delete
# (Consolidated from test_session_community_mcp_complete.py)
# =============================================================================


class TestSessionCommunityCreateComplete:
    """Complete tests for session_community_create edge cases."""

    @pytest.mark.asyncio
    async def test_create_with_auth_token_parameter(self):
        """Test lines 3740: auth_token parameter takes precedence."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "auth_token": "default_token",
                    "auth_token_env_var": "SOME_VAR",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = (
            "http://localhost:10000/?authToken=test_token"
        )
        mock_launched_session.container_id = "test"
        mock_launched_session.auth_type = "psk"
        mock_launched_session.auth_token = "test_token"

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
                return_value=10000,
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=True),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await mcp_mod.session_community_create(
                context,
                session_name="test-session",
                auth_token="explicit_token",  # This should take precedence
            )

            assert result["success"] is True
            # Verify explicit token was used
            launch_call = mock_launch_session.call_args
            assert launch_call[1]["auth_token"] == "explicit_token"

    @pytest.mark.asyncio
    async def test_create_with_auth_token_env_var_set(self):
        """Test lines 3742-3746: auth_token_env_var when env var exists."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "auth_token_env_var": "TEST_AUTH_TOKEN",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = (
            "http://localhost:10000/?authToken=test_token"
        )
        mock_launched_session.container_id = "test"
        mock_launched_session.auth_type = "psk"
        mock_launched_session.auth_token = "test_token"

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
                return_value=10000,
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=True),
            ),
            patch.dict(os.environ, {"TEST_AUTH_TOKEN": "env_token_value"}),
        ):

            mock_launch_session.return_value = mock_launched_session

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await mcp_mod.session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is True
            # Verify env var token was used
            launch_call = mock_launch_session.call_args
            assert launch_call[1]["auth_token"] == "env_token_value"

    @pytest.mark.asyncio
    async def test_create_with_auth_token_from_defaults(self):
        """Test line 3750: auth_token from defaults."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "auth_token": "default_token",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = (
            "http://localhost:10000/?authToken=test_token"
        )
        mock_launched_session.container_id = "test"
        mock_launched_session.auth_type = "psk"
        mock_launched_session.auth_token = "test_token"

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
                return_value=10000,
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=True),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await mcp_mod.session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is True
            # Verify default token was used
            launch_call = mock_launch_session.call_args
            assert launch_call[1]["auth_token"] == "default_token"

    @pytest.mark.asyncio
    async def test_create_session_already_exists(self):
        """Test lines 3766-3770: session ID already exists."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {},
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        # Session already exists
        mock_session_registry.get_all = AsyncMock(
            return_value={"community:dynamic:test-session": MagicMock()}
        )

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_create(
            context, session_name="test-session"
        )

        assert result["success"] is False
        assert "already exists" in result["error"]
        assert result["isError"] is True

    @pytest.mark.asyncio
    async def test_create_health_check_timeout_with_cleanup(self):
        """Test lines 3819-3832: health check timeout with successful cleanup."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {},
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = (
            "http://localhost:10000/?authToken=test_token"
        )
        mock_launched_session.container_id = "test"
        mock_launched_session.auth_type = "psk"
        mock_launched_session.auth_token = "test_token"

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
                return_value=10000,
            ),
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
                return_value="token",
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=False),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session
            mock_launched_session.stop = AsyncMock()  # Cleanup succeeds

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await mcp_mod.session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is False
            assert "failed to start" in result["error"].lower()
            assert result["isError"] is True
            # Verify cleanup was attempted
            mock_launched_session.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_with_python_launch_method(self):
        """Test lines 3891-3892: python launch method sets process_id."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "launch_method": "python",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_launched_session = MagicMock(spec=PythonLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "python"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = "http://localhost:10000"
        mock_launched_session.process = mock_process
        mock_launched_session.auth_type = "anonymous"
        mock_launched_session.auth_token = None

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
                return_value=10000,
            ),
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
                return_value="token",
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=True),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await mcp_mod.session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is True
            assert result["process_id"] == 12345


class TestSessionCommunityDeleteComplete:
    """Complete tests for session_community_delete edge cases."""

    @pytest.mark.asyncio
    async def test_delete_non_community_session(self):
        """Test lines 4005-4009: trying to delete non-community session."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        mock_manager = MagicMock()
        mock_manager.full_name = "enterprise:system:test-session"
        mock_manager._name = "test-session"
        mock_manager.source = "dynamic"
        mock_manager.system_type = SystemType.ENTERPRISE  # Not COMMUNITY

        mock_session_registry.get = AsyncMock(return_value=mock_manager)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_delete(
            context, session_name="test-session"
        )

        assert result["success"] is False
        assert "not a community session" in result["error"]
        assert result["isError"] is True

    @pytest.mark.asyncio
    async def test_delete_close_fails_but_continues(self):
        """Test lines 4034-4047: close fails but removal continues."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.launch_method = "docker"

        mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
        mock_manager.full_name = "community:dynamic:test-session"
        mock_manager._name = "test-session"
        mock_manager.source = "dynamic"
        mock_manager.system_type = SystemType.COMMUNITY
        mock_manager.launched_session = mock_launched_session
        mock_manager.close = AsyncMock(side_effect=Exception("Close failed"))

        mock_session_registry.get = AsyncMock(return_value=mock_manager)
        mock_session_registry.remove_session = AsyncMock(return_value=mock_manager)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_delete(
            context, session_name="test-session"
        )

        # Should still succeed despite close failure
        assert result["success"] is True
        # Verify removal was still attempted
        mock_session_registry.remove_session.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_removal_fails(self):
        """Test lines 4055-4060: removal from registry fails."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.launch_method = "docker"

        mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
        mock_manager.full_name = "community:dynamic:test-session"
        mock_manager._name = "test-session"
        mock_manager.source = "dynamic"
        mock_manager.system_type = SystemType.COMMUNITY
        mock_manager.launched_session = mock_launched_session
        mock_manager.close = AsyncMock()

        mock_session_registry.get = AsyncMock(return_value=mock_manager)
        mock_session_registry.remove_session = AsyncMock(
            side_effect=Exception("Removal failed")
        )

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_delete(
            context, session_name="test-session"
        )

        assert result["success"] is False
        assert "Failed to remove session" in result["error"]
        assert result["isError"] is True

    @pytest.mark.asyncio
    async def test_delete_unexpected_exception(self):
        """Test lines 4075-4081: unexpected exception during delete."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        # Make get() raise an unexpected exception
        mock_session_registry.get = AsyncMock(
            side_effect=RuntimeError("Unexpected error")
        )

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_delete(
            context, session_name="test-session"
        )

        assert result["success"] is False
        assert "Unexpected error" in result["error"]
        assert result["isError"] is True


class TestRemainingEdgeCases:
    """Tests for remaining edge cases."""

    @pytest.mark.asyncio
    async def test_create_with_pip_and_process_id(self):
        """Test line 995: process_id in session details."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "launch_method": "python",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_process = MagicMock()
        mock_process.pid = 99999
        mock_launched_session = MagicMock(spec=PythonLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "python"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = "http://localhost:10000"
        mock_launched_session.process = mock_process
        mock_launched_session.auth_type = "anonymous"
        mock_launched_session.auth_token = None

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
                return_value=10000,
            ),
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
                return_value="token",
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=True),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await mcp_mod.session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is True
            assert result["process_id"] == 99999

    @pytest.mark.asyncio
    async def test_create_auth_token_env_var_not_found(self):
        """Test that error is raised when auth_token_env_var is configured but env var not found."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {
                    "auth_token_env_var": "NONEXISTENT_VAR",
                },
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.add_session = AsyncMock()
        mock_session_registry.get_all = AsyncMock(return_value={})

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        with patch.dict(os.environ, {}, clear=True):  # Empty environment
            result = await mcp_mod.session_community_create(
                context, session_name="test-session"
            )

            # Should fail because explicitly configured env var is not set
            assert result["success"] is False
            assert "NONEXISTENT_VAR" in result["error"]
            assert "not set" in result["error"]
            assert result["isError"] is True

    @pytest.mark.asyncio
    async def test_create_cleanup_fails_on_timeout(self):
        """Test lines 3824-3825: cleanup fails after health check timeout."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        community_config = {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {},
            }
        }

        full_config = {"community": community_config}
        mock_config_manager.get_config = AsyncMock(return_value=full_config)
        mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
        mock_session_registry.get_all = AsyncMock(return_value={})

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = (
            "http://localhost:10000/?authToken=test_token"
        )
        mock_launched_session.container_id = "test"
        mock_launched_session.auth_type = "psk"
        mock_launched_session.auth_token = "test_token"

        with (
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.launch_session"
            ) as mock_launch_session,
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
                return_value=10000,
            ),
            patch(
                "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
                return_value="token",
            ),
            patch.object(
                mock_launched_session,
                "wait_until_ready",
                new=AsyncMock(return_value=False),
            ),
        ):

            mock_launch_session.return_value = mock_launched_session
            mock_launched_session.stop = AsyncMock(
                side_effect=Exception("Cleanup failed")
            )

            context = MockContext(
                {
                    "config_manager": mock_config_manager,
                    "session_registry": mock_session_registry,
                    "instance_tracker": create_mock_instance_tracker(),
                }
            )

            result = await mcp_mod.session_community_create(
                context, session_name="test-session"
            )

            assert result["success"] is False
            # Verify cleanup was attempted even though it failed
            mock_launched_session.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_removal_returns_none(self):
        """Test lines 4044-4047: removal returns None (not found)."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.launch_method = "docker"

        mock_manager = MagicMock(spec=DynamicCommunitySessionManager)
        mock_manager.full_name = "community:dynamic:test-session"
        mock_manager._name = "test-session"
        mock_manager.source = "dynamic"
        mock_manager.system_type = SystemType.COMMUNITY
        mock_manager.launched_session = mock_launched_session
        mock_manager.close = AsyncMock()

        mock_session_registry.get = AsyncMock(return_value=mock_manager)
        mock_session_registry.remove_session = AsyncMock(return_value=None)  # Not found

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_community_delete(
            context, session_name="test-session"
        )

        # Should still succeed even though removal returned None
        assert result["success"] is True


# =============================================================================
# Tests for session_details with dynamic community sessions
# (Consolidated from test_session_details_dynamic.py)
# =============================================================================


class TestSessionDetailsDynamicCommunity:
    """Test session_details with dynamic community sessions."""

    @pytest.mark.asyncio
    async def test_session_details_with_all_dynamic_fields(self):
        """Test lines 975-998: all dynamic session fields present."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        # Create a mock DynamicCommunitySessionManager with all fields
        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10000
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10000"
        mock_launched_session.connection_url_with_auth = (
            "http://localhost:10000/?authToken=abc123"
        )
        mock_launched_session.container_id = "de18601a1657"
        mock_launched_session.auth_type = "psk"
        mock_launched_session.auth_token = "abc123"

        session_config = {
            "host": "localhost",
            "port": 10000,
            "auth_type": "PSK",
        }

        # Create actual manager instance
        manager = DynamicCommunitySessionManager(
            name="test-session",
            config=session_config,
            launched_session=mock_launched_session,
        )

        mock_session_registry.get = AsyncMock(return_value=manager)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_details(
            context, session_id="community:dynamic:test-session"
        )

        # Verify all dynamic fields were added
        assert result["success"] is True
        session_info = result["session"]
        assert "connection_url" in session_info
        # Note: connection_url_with_auth removed from to_dict() for security
        assert session_info["auth_type"] == "PSK"
        assert session_info["launch_method"] == "docker"
        assert session_info["port"] == 10000
        assert session_info["container_id"] == "de18601a1657"

    @pytest.mark.asyncio
    async def test_session_details_with_python_process_id(self):
        """Test lines 994-997: process_id field for python launch method."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        # Create a python-launched session with process
        mock_process = MagicMock()
        mock_process.pid = 54321

        mock_launched_session = MagicMock(spec=PythonLaunchedSession)
        mock_launched_session.port = 10001
        mock_launched_session.launch_method = "python"
        mock_launched_session.connection_url = "http://localhost:10001"
        mock_launched_session.connection_url_with_auth = "http://localhost:10001"
        mock_launched_session.process = mock_process
        mock_launched_session.auth_type = "anonymous"
        mock_launched_session.auth_token = None

        session_config = {
            "host": "localhost",
            "port": 10001,
            "auth_type": "anonymous",
        }

        # Create actual manager instance
        manager = DynamicCommunitySessionManager(
            name="pip-session",
            config=session_config,
            launched_session=mock_launched_session,
        )

        mock_session_registry.get = AsyncMock(return_value=manager)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_details(
            context, session_id="community:dynamic:pip-session"
        )

        # Verify process_id was added
        assert result["success"] is True
        session_info = result["session"]
        assert session_info["launch_method"] == "python"
        assert session_info["process_id"] == 54321
        assert (
            "container_id" not in session_info
        )  # Should not have container_id for pip

    @pytest.mark.asyncio
    async def test_session_details_with_partial_fields(self):
        """Test lines 975-998: only some dynamic fields present."""
        mock_config_manager = MagicMock()
        mock_session_registry = MagicMock()

        # Create a session with minimal fields
        mock_launched_session = MagicMock(spec=DockerLaunchedSession)
        mock_launched_session.port = 10002
        mock_launched_session.launch_method = "docker"
        mock_launched_session.connection_url = "http://localhost:10002"
        mock_launched_session.connection_url_with_auth = "http://localhost:10002"
        mock_launched_session.container_id = "minimal123"
        mock_launched_session.auth_type = "anonymous"
        mock_launched_session.auth_token = None

        session_config = {
            "host": "localhost",
            "port": 10002,
            "auth_type": "anonymous",
        }

        # Create actual manager instance
        manager = DynamicCommunitySessionManager(
            name="minimal-session",
            config=session_config,
            launched_session=mock_launched_session,
        )

        mock_session_registry.get = AsyncMock(return_value=manager)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_details(
            context, session_id="community:dynamic:minimal-session"
        )

        # Verify fields that should be present
        assert result["success"] is True
        session_info = result["session"]
        assert "connection_url" in session_info
        assert session_info["launch_method"] == "docker"
        assert session_info["port"] == 10002
        assert session_info["container_id"] == "minimal123"


@pytest.mark.asyncio
async def test_session_community_create_case_insensitive_params():
    """Test that launch_method, programming_language, and auth_type are case-insensitive."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config with session creation enabled
    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    # Test case: Mixed case parameters should be normalized
    # Docker + Python + PSK with various casings
    test_cases = [
        ("Docker", "Python", "PSK"),  # Title case
        ("DOCKER", "PYTHON", "psk"),  # Various cases
        ("docker", "python", "Psk"),  # Lower + title
        ("PIP", None, "anonymous"),  # Pip with anonymous (upper + lower)
        ("Pip", None, "ANONYMOUS"),  # Pip with anonymous (title + upper)
    ]

    for launch_method, prog_lang, auth_type in test_cases:
        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        # This should NOT raise validation errors - parameters should be normalized
        # We expect it to fail later (e.g., Docker not available), but NOT on parameter validation
        result = await mcp_mod.session_community_create(
            context,
            session_name=f"test-{launch_method.lower()}",
            launch_method=launch_method,
            programming_language=prog_lang,
            auth_type=auth_type,
        )

        # If it fails on validation (not Docker/pip issues), test fails
        if not result["success"]:
            error = result.get("error", "")
            # These are validation errors we DON'T want to see (means normalization failed)
            assert (
                "'programming_language' parameter only applies to docker" not in error
            ), f"Case normalization failed for {launch_method=}, {prog_lang=}"
            # Other errors (like Docker not available) are OK for this test


@pytest.mark.asyncio
async def test_session_community_create_validates_programming_language_with_python():
    """Test that programming_language parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: programming_language only for docker
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        programming_language="Python",  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'programming_language' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_image_with_python():
    """Test that docker_image parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_image only for docker
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_image="ghcr.io/deephaven/server:custom",  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_image' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_memory_limit_with_python():
    """Test that docker_memory_limit_gb parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_memory_limit_gb only for docker
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_memory_limit_gb=8.0,  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_memory_limit_gb' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_cpu_limit_with_python():
    """Test that docker_cpu_limit parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_cpu_limit only for docker
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_cpu_limit=2.0,  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_cpu_limit' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_docker_volumes_with_python():
    """Test that docker_volumes parameter raises error with python launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: docker_volumes only for docker
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-invalid",
        launch_method="python",
        docker_volumes=["/data:/opt/data:ro"],  # Not valid with python!
    )

    assert result["success"] is False
    assert (
        "'docker_volumes' parameter only applies to docker launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_python_venv_path_with_docker():
    """Test that python_venv_path parameter raises error with docker launch method."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: python_venv_path only for python
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-invalid",
        launch_method="docker",
        python_venv_path="/path/to/custom/venv",  # Not valid with docker!
    )

    assert result["success"] is False
    assert (
        "'python_venv_path' parameter only applies to python launch method"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_create_validates_mutually_exclusive_params():
    """Test that programming_language and docker_image cannot both be specified."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(
        return_value={
            "community": {
                "session_creation": {
                    "defaults": {},
                    "max_concurrent_sessions": 5,
                }
            }
        }
    )

    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should raise validation error: can't specify both
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-invalid",
        launch_method="docker",
        programming_language="Python",
        docker_image="ghcr.io/deephaven/server:custom",
    )

    assert result["success"] is False
    assert (
        "Cannot specify both 'programming_language' and 'docker_image'"
        in result["error"]
    )
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_dynamic_community_session_has_correct_source():
    """Test that DynamicCommunitySessionManager has source='dynamic'."""
    from unittest.mock import MagicMock

    from deephaven_mcp.resource_manager import (
        DockerLaunchedSession,
        DynamicCommunitySessionManager,
        SystemType,
    )

    # Create a mock launched session
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = "http://localhost:10000"

    # Create DynamicCommunitySessionManager
    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    # Verify source is "dynamic"
    assert (
        manager.source == "dynamic"
    ), f"Expected source='dynamic', got source='{manager.source}'"

    # Verify system_type is COMMUNITY
    assert manager.system_type == SystemType.COMMUNITY

    # Verify full_name format is correct
    assert manager.full_name == "community:dynamic:test-session"

    # Verify name
    assert manager.name == "test-session"


@pytest.mark.asyncio
async def test_session_community_delete_validates_source():
    """Test that session_community_delete only allows deletion of dynamic sessions."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod
    from deephaven_mcp.resource_manager import SystemType

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock session manager with source="community" (static session from config)
    mock_static_manager = MagicMock()
    mock_static_manager.full_name = "community:community:local"
    mock_static_manager.system_type = SystemType.COMMUNITY
    mock_static_manager.source = "community"  # NOT "dynamic"

    mock_session_registry.get = AsyncMock(return_value=mock_static_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Attempt to delete static session
    result = await mcp_mod.session_community_delete(
        context,
        session_name="local",
    )

    # Verify error - cannot delete static sessions
    assert result["success"] is False
    assert "not a dynamically created session" in result["error"]
    assert "source: 'community'" in result["error"]
    assert result["isError"] is True


@pytest.mark.asyncio
async def test_session_community_delete_allows_dynamic_sessions():
    """Test that session_community_delete allows deletion of dynamic sessions."""
    from deephaven_mcp.mcp_systems_server import _mcp as mcp_mod
    from deephaven_mcp.resource_manager import SystemType

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a mock dynamic session manager with source="dynamic"
    mock_dynamic_manager = MagicMock()
    mock_dynamic_manager.full_name = "community:dynamic:test-session"
    mock_dynamic_manager.system_type = SystemType.COMMUNITY
    mock_dynamic_manager.source = "dynamic"  # Correct source
    mock_dynamic_manager.close = AsyncMock()

    mock_session_registry.get = AsyncMock(return_value=mock_dynamic_manager)
    mock_session_registry.get_all = AsyncMock(
        return_value=["community:dynamic:test-session"]
    )
    mock_session_registry.remove_session = AsyncMock(return_value=mock_dynamic_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Delete dynamic session
    result = await mcp_mod.session_community_delete(
        context,
        session_name="test-session",
    )

    # Verify success
    assert result["success"] is True
    assert result["session_id"] == "community:dynamic:test-session"

    # Verify close and remove_session were called
    mock_dynamic_manager.close.assert_called_once()
    mock_session_registry.remove_session.assert_called_once_with(
        "community:dynamic:test-session"
    )


# ===== Programming Language and Docker Image Resolution Tests =====


@pytest.mark.asyncio
async def test_session_community_create_explicit_docker_image():
    """Test coverage for line 3830: explicit docker_image parameter override."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get_all = AsyncMock(return_value={})

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test_token"
    )
    mock_launched_session.container_id = "test"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
        ),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        # Use explicit docker_image (power user override)
        result = await mcp_mod.session_community_create(
            context,
            session_name="test-session",
            docker_image="ghcr.io/deephaven/custom-server:v1.2.3",
        )

        assert result["success"] is True
        # Verify launch_session was called with custom image
        call_kwargs = mock_launch_session.call_args.kwargs
        assert call_kwargs["docker_image"] == "ghcr.io/deephaven/custom-server:v1.2.3"


@pytest.mark.asyncio
async def test_session_community_create_groovy_programming_language():
    """Test coverage for lines 3836-3837: Groovy programming language parameter."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get_all = AsyncMock(return_value={})

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test_token"
    )
    mock_launched_session.container_id = "test"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
        ),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        # Use Groovy programming language
        result = await mcp_mod.session_community_create(
            context,
            session_name="test-session",
            programming_language="Groovy",
        )

        assert result["success"] is True
        # Verify launch_session was called with Groovy image (slim variant)
        call_kwargs = mock_launch_session.call_args.kwargs
        assert "slim" in call_kwargs["docker_image"]  # Groovy uses server-slim


@pytest.mark.asyncio
async def test_session_community_create_unsupported_programming_language():
    """Test coverage for lines 3839-3843: unsupported programming language error."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {},
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get_all = AsyncMock(return_value={})

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Use unsupported programming language
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-session",
        programming_language="JavaScript",  # Invalid!
    )

    assert result["success"] is False
    assert "Unsupported programming_language" in result["error"]
    assert "JavaScript" in result["error"]
    assert "Python" in result["error"] and "Groovy" in result["error"]


@pytest.mark.asyncio
async def test_session_community_create_groovy_from_config_defaults():
    """Test coverage for lines 3849-3850: Groovy as config default."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "programming_language": "Groovy",  # Set Groovy as default
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.add_session = AsyncMock()
    mock_session_registry.get_all = AsyncMock(return_value={})

    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test_token"
    )
    mock_launched_session.container_id = "test"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test_token"

    with (
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.launch_session"
        ) as mock_launch_session,
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.find_available_port",
            return_value=10000,
        ),
        patch(
            "deephaven_mcp.mcp_systems_server._mcp.generate_auth_token",
            return_value="test_token",
        ),
        patch.object(
            mock_launched_session, "wait_until_ready", new=AsyncMock(return_value=True)
        ),
    ):

        mock_launch_session.return_value = mock_launched_session

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        # Don't specify programming_language - should use config default (Groovy)
        result = await mcp_mod.session_community_create(
            context,
            session_name="test-session",
        )

        assert result["success"] is True
        # Verify launch_session was called with Groovy image from config
        call_kwargs = mock_launch_session.call_args.kwargs
        assert "slim" in call_kwargs["docker_image"]  # Groovy uses slim image


@pytest.mark.asyncio
async def test_session_community_create_invalid_config_programming_language():
    """Test coverage for lines 3853-3857: invalid programming language in config."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "programming_language": "Ruby",  # Invalid in config!
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get_all = AsyncMock(return_value={})

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should fail with invalid config language error
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-session",
    )

    assert result["success"] is False
    assert "Invalid programming_language in config" in result["error"]
    assert "Ruby" in result["error"]
    assert "Python" in result["error"] and "Groovy" in result["error"]


@pytest.mark.asyncio
async def test_session_community_create_missing_auth_token_env_var():
    """Test that missing auth_token_env_var returns configuration error."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    community_config = {
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "launch_method": "docker",
                "auth_type": "PSK",
                "auth_token_env_var": "MISSING_ENV_VAR",  # This env var is not set
            },
        }
    }

    full_config = {"community": community_config}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)
    mock_session_registry.count_added_sessions = AsyncMock(return_value=0)
    mock_session_registry.get_all = AsyncMock(return_value={})

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )

    # Should return error when env var is not set
    result = await mcp_mod.session_community_create(
        context,
        session_name="test-session",
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "MISSING_ENV_VAR" in result["error"]
    assert "not set" in result["error"]


@pytest.mark.asyncio
async def test_session_details_to_dict_exception():
    """Test coverage for lines 1021-1022: exception when to_dict() fails."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Create a real DynamicCommunitySessionManager instance
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test"
    )
    mock_launched_session.container_id = "abc123"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.auth_token = "test"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    # Mock to_dict() to raise an exception
    with patch.object(
        manager, "to_dict", side_effect=RuntimeError("Simulated failure in to_dict")
    ):
        mock_session_registry.get = AsyncMock(return_value=manager)

        context = MockContext(
            {
                "config_manager": mock_config_manager,
                "session_registry": mock_session_registry,
                "instance_tracker": create_mock_instance_tracker(),
            }
        )

        result = await mcp_mod.session_details(
            context, session_id="community:dynamic:test-session"
        )

        # Should still succeed despite to_dict() exception
        assert result["success"] is True
        session_info = result["session"]

        # Basic session info should be present
        assert session_info["session_id"] == "community:dynamic:test-session"
        assert session_info["type"] == "COMMUNITY"
        assert session_info["source"] == "dynamic"
        assert session_info["session_name"] == "test-session"

        # Dynamic fields from to_dict() should NOT be present (because it failed)
        # These would normally be added by to_dict() if it succeeded
        assert "connection_url" not in session_info  # This comes from to_dict()
        assert "port" not in session_info  # This comes from to_dict()
        assert "launch_method" not in session_info  # This comes from to_dict()


# ===== session_community_credentials Tests =====


@pytest.mark.asyncio
async def test_session_community_credentials_disabled_by_default():
    """Test that credential retrieval is disabled by default (mode='none')."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Config without security section (defaults to mode='none')
    config = {
        "community": {
            "session_creation": {
                "max_concurrent_sessions": 5,
                "defaults": {},
            }
        }
    }

    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]
    assert "security" in result["error"]
    assert "credential_retrieval_mode" in result["error"]
    assert "deephaven_mcp.json" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_explicit_none():
    """Test that credential retrieval respects explicit 'none' mode."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Config with explicit mode='none'
    config = {"security": {"community": {"credential_retrieval_mode": "none"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_dynamic_success():
    """Test successful credential retrieval for dynamic session with mode='dynamic_only'."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Config with mode='dynamic_only'
    config = {"security": {"community": {"credential_retrieval_mode": "dynamic_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a mock DynamicCommunitySessionManager
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.auth_token = "test_auth_token_123"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test_auth_token_123"
    )
    mock_launched_session.container_id = "test_container_id"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is True
    assert result["connection_url"] == "http://localhost:10000"
    assert (
        result["connection_url_with_auth"]
        == "http://localhost:10000/?authToken=test_auth_token_123"
    )
    assert result["auth_token"] == "test_auth_token_123"
    assert result["auth_type"] == "PSK"
    assert "error" not in result
    assert "isError" not in result


@pytest.mark.asyncio
async def test_session_community_credentials_anonymous_auth():
    """Test credential retrieval with anonymous auth (no token)."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a mock session with anonymous auth (no token)
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.auth_token = None
    mock_launched_session.auth_type = "anonymous"
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = "http://localhost:10000"
    mock_launched_session.container_id = "test_container_id"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is True
    assert result["auth_token"] == ""  # Empty string for None
    assert result["auth_type"] == "ANONYMOUS"


@pytest.mark.asyncio
async def test_session_community_credentials_no_config():
    """Test when community config is empty."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Empty config - should default to disabled
    config = {}
    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Credential retrieval is disabled" in result["error"]
    assert "mode='none'" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_session_not_found():
    """Test when session does not exist."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Session not found
    mock_session_registry.get = AsyncMock(side_effect=KeyError("Session not found"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:nonexistent"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Session 'community:dynamic:nonexistent' not found" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_not_dynamic_session():
    """Test when session is not a DynamicCommunitySessionManager."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Return a different type of manager (not DynamicCommunitySessionManager)
    mock_manager = MagicMock()
    mock_manager.__class__.__name__ = "StaticCommunitySessionManager"
    mock_session_registry.get = AsyncMock(return_value=mock_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:static-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "not a community session" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_static_session():
    """Test credential retrieval for static community session with mode='static_only'."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "static_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a static session manager
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "static_token_123",
        "auth_type": "PSK",
    }

    manager = StaticCommunitySessionManager(name="local-dev", config=static_config)

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:config:local-dev"
    )

    assert result["success"] is True
    assert result["connection_url"] == "http://localhost:10000"
    assert (
        result["connection_url_with_auth"]
        == "http://localhost:10000/?authToken=static_token_123"
    )
    assert result["auth_token"] == "static_token_123"
    assert result["auth_type"] == "PSK"
    assert "error" not in result
    assert "isError" not in result


@pytest.mark.asyncio
async def test_session_community_credentials_static_session_anonymous():
    """Test credential retrieval for static community session with anonymous auth."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a static session manager with anonymous auth (no token)
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "",  # Empty token for anonymous
        "auth_type": "anonymous",
    }

    manager = StaticCommunitySessionManager(name="local-dev-anon", config=static_config)

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:config:local-dev-anon"
    )

    assert result["success"] is True
    assert result["connection_url"] == "http://localhost:10000"
    assert (
        result["connection_url_with_auth"] == "http://localhost:10000"
    )  # No auth query param
    assert result["auth_token"] == ""  # Empty string
    assert result["auth_type"] == "ANONYMOUS"
    assert "error" not in result
    assert "isError" not in result


@pytest.mark.asyncio
async def test_session_community_credentials_invalid_session_id():
    """Test when session_id has invalid format."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="enterprise:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Invalid session_id" in result["error"]
    assert "community:dynamic:" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_exception_handling():
    """Test exception handling in session_community_credentials."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Make get_config raise an exception
    mock_config_manager.get_config = AsyncMock(
        side_effect=RuntimeError("Unexpected config error")
    )

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "Unexpected config error" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_dynamic_only_denies_static():
    """Test that mode='dynamic_only' denies static session credentials."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "dynamic_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a static session manager
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "static_token_123",
        "auth_type": "PSK",
    }

    manager = StaticCommunitySessionManager(name="local-dev", config=static_config)

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:config:local-dev"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "static sessions is disabled" in result["error"]
    assert "dynamic_only" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_static_only_denies_dynamic():
    """Test that mode='static_only' denies dynamic session credentials."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "static_only"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Create a mock DynamicCommunitySessionManager
    mock_launched_session = MagicMock(spec=DockerLaunchedSession)
    mock_launched_session.auth_token = "test_auth_token_123"
    mock_launched_session.auth_type = "psk"
    mock_launched_session.port = 10000
    mock_launched_session.launch_method = "docker"
    mock_launched_session.connection_url = "http://localhost:10000"
    mock_launched_session.connection_url_with_auth = (
        "http://localhost:10000/?authToken=test_auth_token_123"
    )
    mock_launched_session.container_id = "test_container_id"

    manager = DynamicCommunitySessionManager(
        name="test-session",
        config={"host": "localhost", "port": 10000},
        launched_session=mock_launched_session,
    )

    mock_session_registry.get = AsyncMock(return_value=manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:dynamic:test-session"
    )

    assert result["success"] is False
    assert result["isError"] is True
    assert "dynamic sessions is disabled" in result["error"]
    assert "static_only" in result["error"]


@pytest.mark.asyncio
async def test_session_community_credentials_all_allows_both():
    """Test that mode='all' allows both dynamic and static session credentials."""
    from deephaven_mcp.resource_manager._manager import StaticCommunitySessionManager

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    config = {"security": {"community": {"credential_retrieval_mode": "all"}}}

    mock_config_manager.get_config = AsyncMock(return_value=config)

    # Test with static session
    static_config = {
        "server": "http://localhost:10000",
        "auth_token": "static_token_123",
        "auth_type": "PSK",
    }

    static_manager = StaticCommunitySessionManager(
        name="local-dev", config=static_config
    )

    mock_session_registry.get = AsyncMock(return_value=static_manager)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await mcp_mod.session_community_credentials(
        context, session_id="community:config:local-dev"
    )

    assert result["success"] is True
    assert result["auth_token"] == "static_token_123"


# ===== Test _normalize_auth_type Helper Function =====


def test_normalize_auth_type_psk_uppercase():
    """Test PSK shorthand normalization - uppercase."""
    result, error = _normalize_auth_type("PSK")
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_psk_lowercase():
    """Test PSK shorthand normalization - lowercase."""
    result, error = _normalize_auth_type("psk")
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_psk_mixedcase():
    """Test PSK shorthand normalization - mixed case."""
    result, error = _normalize_auth_type("Psk")
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_anonymous_uppercase():
    """Test Anonymous shorthand normalization - uppercase."""
    result, error = _normalize_auth_type("ANONYMOUS")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_anonymous_lowercase():
    """Test Anonymous shorthand normalization - lowercase."""
    result, error = _normalize_auth_type("anonymous")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_anonymous_proper_case():
    """Test Anonymous shorthand normalization - proper case."""
    result, error = _normalize_auth_type("Anonymous")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_basic_rejected():
    """Test that Basic auth is rejected for dynamic sessions."""
    result, error = _normalize_auth_type("Basic")
    assert error is not None
    assert "Basic authentication is not supported for dynamic sessions" in error
    assert "requires database setup" in error


def test_normalize_auth_type_basic_lowercase_rejected():
    """Test that Basic auth (lowercase) is rejected."""
    result, error = _normalize_auth_type("basic")
    assert error is not None
    assert "Basic authentication is not supported" in error


def test_normalize_auth_type_basic_uppercase_rejected():
    """Test that Basic auth (uppercase) is rejected."""
    result, error = _normalize_auth_type("BASIC")
    assert error is not None
    assert "Basic authentication is not supported" in error


def test_normalize_auth_type_psk_handler_wrong_case_rejected():
    """Test that the Deephaven PSK handler with incorrect case is rejected."""
    result, error = _normalize_auth_type(
        "IO.DEEPHAVEN.AUTHENTICATION.PSK.PSKAUTHENTICATIONHANDLER"
    )
    assert error is not None
    assert "Deephaven PSK handler with incorrect case" in error
    assert "io.deephaven.authentication.psk.PskAuthenticationHandler" in error


def test_normalize_auth_type_whitespace_rejected():
    """Test that auth_type with whitespace is rejected."""
    result, error = _normalize_auth_type(" PSK")
    assert error is not None
    assert "whitespace" in error


def test_normalize_auth_type_trailing_whitespace_rejected():
    """Test that auth_type with trailing whitespace is rejected."""
    result, error = _normalize_auth_type("PSK ")
    assert error is not None
    assert "whitespace" in error


def test_normalize_auth_type_full_class_name_preserved():
    """Test that correct full class name is preserved."""
    result, error = _normalize_auth_type(
        "io.deephaven.authentication.psk.PskAuthenticationHandler"
    )
    assert error is None
    assert result == "io.deephaven.authentication.psk.PskAuthenticationHandler"


def test_normalize_auth_type_custom_authenticator_preserved():
    """Test that custom authenticator class names are preserved."""
    result, error = _normalize_auth_type("com.example.CustomAuthenticator")
    assert error is None
    assert result == "com.example.CustomAuthenticator"


def test_normalize_auth_type_no_dots_preserved():
    """Test that values without dots (non-class names) are preserved."""
    result, error = _normalize_auth_type("CustomAuth")
    assert error is None
    assert result == "CustomAuth"


def test_normalize_auth_type_anonymous_mixedcase():
    """Test Anonymous shorthand normalization - various mixed cases."""
    result, error = _normalize_auth_type("AnOnYmOuS")
    assert error is None
    assert result == "Anonymous"


def test_normalize_auth_type_uppercase_custom_authenticator_allowed():
    """Test that custom authenticators with uppercase names are allowed."""
    result, error = _normalize_auth_type("COM.MYCOMPANY.CUSTOMAUTH")
    assert error is None
    assert result == "COM.MYCOMPANY.CUSTOMAUTH"


def test_resolve_community_session_parameters_invalid_auth_type():
    """Test _resolve_community_session_parameters with invalid auth_type returns error."""
    # Call with invalid auth_type (Basic is not supported for dynamic sessions)
    resolved_params, error = _resolve_community_session_parameters(
        launch_method=None,
        programming_language=None,
        auth_type="Basic",  # This should trigger validation error
        auth_token=None,
        heap_size_gb=None,
        extra_jvm_args=None,
        environment_vars=None,
        docker_image=None,
        docker_memory_limit_gb=None,
        docker_cpu_limit=None,
        docker_volumes=None,
        python_venv_path=None,
        defaults={},
    )
    
    # Should return empty dict and error dict
    assert resolved_params == {}
    assert error is not None
    assert error["success"] is False
    assert error["isError"] is True
    assert "Invalid auth_type" in error["error"]
    assert "Basic authentication is not supported" in error["error"]
