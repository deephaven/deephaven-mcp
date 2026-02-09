"""
Tests for deephaven_mcp.mcp_systems_server._tools.pq.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from conftest import MockContext, create_mock_instance_tracker

# Test-specific helper functions (only used in this file)
def create_mock_pq_info(serial, name, state="RUNNING", heap_size=8.0):
    """Helper to create mock PQ info object.

    Creates a mock PersistentQueryInfoMessage for testing.
    Protobuf docs: https://docs.deephaven.io/protodoc/20240517/#io.deephaven.proto.persistent_query.PersistentQueryInfoMessage

    Note: Python protobuf keeps camelCase field names from .proto files (e.g., heapSizeGb not heap_size_gb)
    """
    mock_pq_info = MagicMock()
    # Basic config fields
    mock_pq_info.config.pb.serial = serial
    mock_pq_info.config.pb.version = 1
    mock_pq_info.config.pb.name = name
    mock_pq_info.config.pb.owner = "test_user"
    mock_pq_info.config.pb.enabled = True
    # Resource fields
    mock_pq_info.config.pb.heapSizeGb = heap_size
    mock_pq_info.config.pb.bufferPoolToHeapRatio = 0.0
    mock_pq_info.config.pb.detailedGCLoggingEnabled = False
    mock_pq_info.config.pb.extraJvmArguments = []
    mock_pq_info.config.pb.extraEnvironmentVariables = []
    mock_pq_info.config.pb.classPathAdditions = []
    # Execution fields
    mock_pq_info.config.pb.serverName = ""
    mock_pq_info.config.pb.adminGroups = []
    mock_pq_info.config.pb.viewerGroups = []

    # restartUsers - handled by RestartUsersEnum.Name() which is patched in tests
    mock_pq_info.config.pb.restartUsers = 0
    # Script fields
    mock_pq_info.config.pb.scriptCode = ""
    mock_pq_info.config.pb.scriptPath = ""
    mock_pq_info.config.pb.scriptLanguage = "Python"
    mock_pq_info.config.pb.configurationType = "Script"
    mock_pq_info.config.pb.typeSpecificFieldsJson = ""
    # Scheduling/timeout
    mock_pq_info.config.pb.scheduling = []
    mock_pq_info.config.pb.timeoutNanos = 0
    # Advanced config
    mock_pq_info.config.pb.jvmProfile = ""
    mock_pq_info.config.pb.pythonControl = ""
    mock_pq_info.config.pb.genericWorkerControl = ""
    # Metadata fields
    mock_pq_info.config.pb.lastModifiedByAuthenticated = ""
    mock_pq_info.config.pb.lastModifiedByEffective = ""
    mock_pq_info.config.pb.lastModifiedTimeNanos = 0
    mock_pq_info.config.pb.createdTimeNanos = 0
    mock_pq_info.config.pb.completedStatus = ""
    mock_pq_info.config.pb.expirationTimeNanos = 0
    # Kubernetes/worker fields
    mock_pq_info.config.pb.kubernetesControl = ""
    mock_pq_info.config.pb.workerKind = "DeephavenCommunity"
    mock_pq_info.config.pb.replicaCount = 0
    mock_pq_info.config.pb.spareCount = 0
    mock_pq_info.config.pb.assignmentPolicy = ""
    mock_pq_info.config.pb.assignmentPolicyParams = ""
    mock_pq_info.config.pb.additionalMemoryGb = 0.0
    # State - primary instance with ALL 25 protobuf fields
    # status is accessed via wrapper's status.name property
    mock_pq_info.state.status.name = state
    mock_pq_info.state.pb.serial = serial
    mock_pq_info.state.pb.version = 1
    mock_pq_info.state.pb.initializationStartNanos = (
        1734467100000000000 if state in ["RUNNING", "INITIALIZING"] else 0
    )
    mock_pq_info.state.pb.initializationCompleteNanos = (
        1734467150000000000 if state in ["RUNNING", "INITIALIZING"] else 0
    )
    mock_pq_info.state.pb.lastUpdateNanos = 1734467200000000000
    mock_pq_info.state.pb.dispatcherHost = (
        "dispatcher.example.com" if state in ["RUNNING", "INITIALIZING"] else ""
    )
    mock_pq_info.state.pb.tableGroups = []
    mock_pq_info.state.pb.scopeTypes = []
    mock_pq_info.state.pb.connectionDetails = None
    mock_pq_info.state.pb.exceptionDetails = None
    mock_pq_info.state.pb.typeSpecificStateJson = ""
    mock_pq_info.state.pb.lastAuthenticatedUser = ""
    mock_pq_info.state.pb.lastEffectiveUser = ""
    mock_pq_info.state.pb.scriptLoaderStateJson = ""
    mock_pq_info.state.pb.hasProgress = False
    mock_pq_info.state.pb.progressValue = 0
    mock_pq_info.state.pb.progressMessage = ""
    mock_pq_info.state.pb.engineVersion = ""
    mock_pq_info.state.pb.dispatcherPort = (
        8080 if state in ["RUNNING", "INITIALIZING"] else 0
    )
    mock_pq_info.state.pb.shouldStopNanos = 0
    mock_pq_info.state.pb.numFailures = 0
    mock_pq_info.state.pb.lastFailureTimeNanos = 0
    mock_pq_info.state.pb.replicaSlot = 0
    mock_pq_info.state.pb.statusDetails = ""

    # Replicas list (empty by default)
    mock_pq_info.replicas = []

    # Spares list (empty by default)
    mock_pq_info.spares = []

    return mock_pq_info





from deephaven_mcp.mcp_systems_server._tools.pq import (
    _format_column_definition,
    _format_connection_details,
    _format_exception_details,
    _format_exported_object_info,
    _format_named_string_list,
    _format_pq_config,
    _format_pq_replicas,
    _format_pq_spares,
    _format_pq_state,
    _format_table_definition,
    _format_worker_protocol,
    _parse_pq_id,
    _validate_and_parse_pq_ids,
    _validate_max_concurrent,
    _validate_timeout,
    pq_create,
    pq_delete,
    pq_details,
    pq_list,
    pq_modify,
    pq_name_to_id,
    pq_restart,
    pq_start,
    pq_stop,
)
from deephaven_mcp import config
from deephaven_mcp.resource_manager import (
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    PythonLaunchedSession,
    ResourceLivenessStatus,
    SystemType,
)


@pytest.mark.asyncio
async def test_pq_name_to_id_success():
    """Test successful PQ name to ID conversion."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller
    mock_controller.get_serial_for_name = AsyncMock(return_value=12345)

    # Mock config
    full_config = {"enterprise": {"systems": {"prod": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_name_to_id(
        context, system_name="prod", pq_name="analytics"
    )

    assert result["success"] is True
    assert result["pq_id"] == "enterprise:prod:12345"
    assert result["serial"] == 12345
    assert result["name"] == "analytics"
    assert result["system_name"] == "prod"
    mock_controller.get_serial_for_name.assert_called_once_with("analytics")



@pytest.mark.asyncio
async def test_pq_name_to_id_not_found():
    """Test pq_name_to_id when PQ name not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller
    mock_controller.get_serial_for_name = AsyncMock(
        side_effect=KeyError("PQ not found")
    )

    # Mock config
    full_config = {"enterprise": {"systems": {"prod": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_name_to_id(
        context, system_name="prod", pq_name="nonexistent"
    )

    assert result["success"] is False
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_name_to_id_system_not_found():
    """Test pq_name_to_id when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    # Mock config with no system
    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_name_to_id(
        context, system_name="nonexistent", pq_name="analytics"
    )

    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_name_to_id_exception():
    """Test pq_name_to_id when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_name_to_id(
        context, system_name="prod", pq_name="analytics"
    )

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@patch("deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum")
def test_format_pq_config(mock_restart_enum):
    """Test _format_pq_config helper function with all field transformations."""
    # Set up RestartUsersEnum.Name() to return proper names
    mock_restart_enum.Name.side_effect = lambda x: {
        0: "RU_UNSPECIFIED",
        1: "RU_ADMIN",
        2: "RU_ALL",
    }.get(x, f"RU_UNKNOWN_{x}")

    # Create mock config with all fields populated
    mock_config = MagicMock()
    mock_pb = MagicMock()

    # Basic fields
    mock_pb.serial = 12345
    mock_pb.version = 5
    mock_pb.name = "test_query"
    mock_pb.owner = "test_owner"
    mock_pb.enabled = True

    # Resource fields
    mock_pb.heapSizeGb = 8.0
    mock_pb.bufferPoolToHeapRatio = 0.25
    mock_pb.detailedGCLoggingEnabled = True

    # List fields
    mock_pb.extraJvmArguments = ["-XX:+UseG1GC", "-Xmx8g"]
    mock_pb.extraEnvironmentVariables = ["VAR1=value1", "VAR2=value2"]
    mock_pb.classPathAdditions = ["/custom/libs"]

    # Execution fields
    mock_pb.serverName = "QueryServer_1"  # Non-empty -> kept
    mock_pb.adminGroups = ["admins", "data-team"]
    mock_pb.viewerGroups = ["analysts"]

    # restartUsers - RestartUsersEnum.Name() is patched at function level
    # Value 1 = RU_ADMIN in our mock
    mock_pb.restartUsers = 1

    # Script fields
    mock_pb.scriptCode = ""  # Empty -> None
    mock_pb.scriptPath = "/scripts/test.py"
    mock_pb.scriptLanguage = "Python"
    mock_pb.configurationType = "Script"
    mock_pb.typeSpecificFieldsJson = ""  # Empty -> None

    # Scheduling/timeout
    mock_pb.scheduling = ["SchedulerType=Daily", "StartTime=08:00:00"]
    mock_pb.timeoutNanos = 300000000000

    # Advanced config
    mock_pb.jvmProfile = "large-memory"

    # Metadata fields
    mock_pb.lastModifiedByAuthenticated = "admin_user"
    mock_pb.lastModifiedByEffective = "admin_user"
    mock_pb.lastModifiedTimeNanos = 1734467200000000000
    mock_pb.completedStatus = ""  # Empty -> None
    mock_pb.expirationTimeNanos = 0  # 0 -> None

    # Kubernetes/worker fields
    mock_pb.kubernetesControl = ""  # Empty -> None
    mock_pb.workerKind = "DeephavenCommunity"
    mock_pb.createdTimeNanos = 1734380800000000000
    mock_pb.replicaCount = 2
    mock_pb.spareCount = 1
    mock_pb.assignmentPolicy = "RoundRobin"
    mock_pb.assignmentPolicyParams = ""  # Empty -> None
    mock_pb.additionalMemoryGb = 2.0
    mock_pb.pythonControl = ""  # Empty -> None
    mock_pb.genericWorkerControl = ""  # Empty -> None

    mock_config.pb = mock_pb

    # Call the helper
    result = _format_pq_config(mock_config)

    # Verify all 38 fields are present
    assert len(result) == 38

    # Basic fields
    assert result["serial"] == 12345
    assert result["version"] == 5
    assert result["name"] == "test_query"
    assert result["owner"] == "test_owner"
    assert result["enabled"] is True

    # Resource fields
    assert result["heap_size_gb"] == 8.0
    assert result["buffer_pool_to_heap_ratio"] == 0.25
    assert result["detailed_gc_logging_enabled"] is True

    # List conversions
    assert result["extra_jvm_arguments"] == ["-XX:+UseG1GC", "-Xmx8g"]
    assert result["extra_environment_variables"] == ["VAR1=value1", "VAR2=value2"]
    assert result["class_path_additions"] == ["/custom/libs"]
    assert result["admin_groups"] == ["admins", "data-team"]
    assert result["viewer_groups"] == ["analysts"]
    assert result["scheduling"] == ["SchedulerType=Daily", "StartTime=08:00:00"]

    # Execution fields
    assert result["server_name"] == "QueryServer_1"
    assert result["restart_users"] == "RU_ADMIN"

    # Script fields
    assert result["script_code"] is None  # Empty -> None
    assert result["script_path"] == "/scripts/test.py"
    assert result["script_language"] == "Python"
    assert result["configuration_type"] == "Script"
    assert result["type_specific_fields_json"] is None  # Empty -> None

    # Timeout
    assert result["timeout_nanos"] == 300000000000

    # Advanced config
    assert result["jvm_profile"] == "large-memory"

    # Metadata fields
    assert result["last_modified_by_authenticated"] == "admin_user"
    assert result["last_modified_by_effective"] == "admin_user"
    assert result["last_modified_time_nanos"] == 1734467200000000000
    assert result["completed_status"] is None  # Empty -> None
    assert result["expiration_time_nanos"] is None  # 0 -> None

    # Kubernetes/worker fields
    assert result["kubernetes_control"] is None  # Empty -> None
    assert result["worker_kind"] == "DeephavenCommunity"
    assert result["created_time_nanos"] == 1734380800000000000
    assert result["replica_count"] == 2
    assert result["spare_count"] == 1
    assert result["assignment_policy"] == "RoundRobin"
    assert result["assignment_policy_params"] is None  # Empty -> None
    assert result["additional_memory_gb"] == 2.0
    assert result["python_control"] is None  # Empty -> None
    assert result["generic_worker_control"] is None  # Empty -> None



@patch("deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum")
def test_format_pq_config_unknown_enum_fallback(mock_restart_enum):
    """Test _format_pq_config handles unknown enum values gracefully (version mismatch)."""
    # Simulate server sending an enum value unknown to client's proto definition
    mock_restart_enum.Name.side_effect = ValueError(
        "Enum has no name defined for value 99"
    )

    mock_config = MagicMock()
    mock_pb = MagicMock()
    mock_pb.serial = 12345
    mock_pb.version = 1
    mock_pb.name = "test"
    mock_pb.owner = "owner"
    mock_pb.enabled = True
    mock_pb.heapSizeGb = 8.0
    mock_pb.bufferPoolToHeapRatio = 0.0
    mock_pb.detailedGCLoggingEnabled = False
    mock_pb.extraJvmArguments = []
    mock_pb.extraEnvironmentVariables = []
    mock_pb.classPathAdditions = []
    mock_pb.serverName = ""
    mock_pb.adminGroups = []
    mock_pb.viewerGroups = []
    mock_pb.restartUsers = 99  # Unknown enum value from newer server
    mock_pb.scriptCode = ""
    mock_pb.scriptPath = ""
    mock_pb.scriptLanguage = "Python"
    mock_pb.configurationType = "Script"
    mock_pb.typeSpecificFieldsJson = ""
    mock_pb.scheduling = []
    mock_pb.timeoutNanos = 0
    mock_pb.jvmProfile = ""
    mock_pb.lastModifiedByAuthenticated = ""
    mock_pb.lastModifiedByEffective = ""
    mock_pb.lastModifiedTimeNanos = 0
    mock_pb.completedStatus = ""
    mock_pb.expirationTimeNanos = 0
    mock_pb.kubernetesControl = ""
    mock_pb.workerKind = "DeephavenCommunity"
    mock_pb.createdTimeNanos = 0
    mock_pb.replicaCount = 0
    mock_pb.spareCount = 0
    mock_pb.assignmentPolicy = ""
    mock_pb.assignmentPolicyParams = ""
    mock_pb.additionalMemoryGb = 0.0
    mock_pb.pythonControl = ""
    mock_pb.genericWorkerControl = ""
    mock_config.pb = mock_pb

    result = _format_pq_config(mock_config)

    # Should return fallback string for unknown enum
    assert result["restart_users"] == "UNKNOWN_RESTART_USERS_99"



@patch("deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum", None)
def test_format_pq_config_no_restart_enum():
    """Test _format_pq_config when RestartUsersEnum is None (missing enterprise package)."""
    mock_config = MagicMock()
    mock_pb = MagicMock()

    # Set minimal required fields
    mock_pb.serial = 12345
    mock_pb.version = 1
    mock_pb.name = "test"
    mock_pb.owner = "owner"
    mock_pb.enabled = True
    mock_pb.heapSizeGb = 8.0
    mock_pb.bufferPoolToHeapRatio = 0.0
    mock_pb.detailedGCLoggingEnabled = False
    mock_pb.extraJvmArguments = []
    mock_pb.extraEnvironmentVariables = []
    mock_pb.classPathAdditions = []
    mock_pb.serverName = ""
    mock_pb.adminGroups = []
    mock_pb.viewerGroups = []
    mock_pb.restartUsers = 1
    mock_pb.scriptCode = ""
    mock_pb.scriptPath = ""
    mock_pb.scriptLanguage = "Python"
    mock_pb.configurationType = "Script"
    mock_pb.typeSpecificFieldsJson = ""
    mock_pb.scheduling = []
    mock_pb.timeoutNanos = 0
    mock_pb.jvmProfile = ""
    mock_pb.lastModifiedByAuthenticated = ""
    mock_pb.lastModifiedByEffective = ""
    mock_pb.lastModifiedTimeNanos = 0
    mock_pb.completedStatus = ""
    mock_pb.expirationTimeNanos = 0
    mock_pb.kubernetesControl = ""
    mock_pb.workerKind = "DeephavenCommunity"
    mock_pb.createdTimeNanos = 0
    mock_pb.replicaCount = 0
    mock_pb.spareCount = 0
    mock_pb.assignmentPolicy = ""
    mock_pb.assignmentPolicyParams = ""
    mock_pb.additionalMemoryGb = 0.0
    mock_pb.pythonControl = ""
    mock_pb.genericWorkerControl = ""

    mock_config.pb = mock_pb

    result = _format_pq_config(mock_config)

    # When RestartUsersEnum is None, restart_users should be str(numeric_value)
    assert result["restart_users"] == "1"
    assert result["serial"] == 12345



@pytest.mark.asyncio
async def test_pq_restart_multiple():
    """Test pq_restart with multiple PQs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_pq_info_1 = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_pq_info_2 = create_mock_pq_info(67890, "reporting", "RUNNING", 8.0)

    mock_controller.get = AsyncMock(side_effect=[mock_pq_info_1, mock_pq_info_2])
    mock_controller.restart_query = AsyncMock(return_value=None)

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    assert result["success"] is True
    assert len(result["results"]) == 2
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "RUNNING"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["success"] is True
    assert result["results"][1]["name"] == "reporting"
    assert result["results"][1]["state"] == "RUNNING"
    assert result["results"][1]["error"] is None
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 2
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Restarted 2 of 2 PQ(s)"



@pytest.mark.asyncio
async def test_pq_restart_partial_failure():
    """Test pq_restart with one success and one failure."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_pq_info_1 = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)

    async def mock_restart_side_effect(serials, timeout):
        if serials == [67890]:
            raise Exception("PQ cannot be restarted")

    mock_controller.get = AsyncMock(return_value=mock_pq_info_1)
    mock_controller.restart_query = AsyncMock(side_effect=mock_restart_side_effect)

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    assert result["success"] is True
    assert len(result["results"]) == 2
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "RUNNING"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["success"] is False
    assert result["results"][1]["name"] is None
    assert "cannot be restarted" in result["results"][1]["error"]
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 1
    assert result["message"] == "Restarted 1 of 2 PQ(s), 1 failed"



@pytest.mark.asyncio
async def test_pq_delete_partial_failure():
    """Test pq_delete with one success and one failure."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_pq_info_1 = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_pq_info_2 = create_mock_pq_info(67890, "reporting", "STOPPED", 8.0)

    call_count = [0]

    async def mock_get_side_effect(serial, timeout_seconds=0):
        call_count[0] += 1
        if call_count[0] == 1:
            return mock_pq_info_1
        else:
            return mock_pq_info_2

    async def mock_delete_side_effect(serial):
        if serial == 67890:
            raise Exception("PQ not found")

    mock_controller.get = AsyncMock(side_effect=mock_get_side_effect)
    mock_controller.delete_query = AsyncMock(side_effect=mock_delete_side_effect)

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    assert result["success"] is True
    assert len(result["results"]) == 2
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["success"] is False
    assert result["results"][1]["name"] is None
    assert "PQ not found" in result["results"][1]["error"]
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 1
    assert result["message"] == "Deleted 1 of 2 PQ(s), 1 failed"



@pytest.mark.asyncio
async def test_pq_start_partial_failure():
    """Test pq_start with one success and one failure."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_pq_info_1 = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)

    async def mock_start_side_effect(serial, timeout):
        if serial == 67890:
            raise Exception("Timeout waiting for PQ to start")

    mock_controller.start_and_wait = AsyncMock(side_effect=mock_start_side_effect)
    mock_controller.get = AsyncMock(return_value=mock_pq_info_1)

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    assert result["success"] is True
    assert len(result["results"]) == 2
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "RUNNING"
    assert result["results"][0]["session_id"] == "enterprise:test-system:analytics"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["success"] is False
    assert result["results"][1]["name"] is None
    assert "Timeout" in result["results"][1]["error"]
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 1
    assert result["message"] == "Started 1 of 2 PQ(s), 1 failed"



@pytest.mark.asyncio
async def test_pq_stop_partial_failure():
    """Test pq_stop with one success and one failure."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_pq_info = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)

    async def mock_stop_side_effect(serials, timeout):
        if serials == [67890]:
            raise Exception("PQ already stopped")

    mock_controller.stop_query = AsyncMock(side_effect=mock_stop_side_effect)
    mock_controller.get = AsyncMock(return_value=mock_pq_info)

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    assert result["success"] is True
    assert len(result["results"]) == 2
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "STOPPED"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["success"] is False
    assert result["results"][1]["name"] is None
    assert "already stopped" in result["results"][1]["error"]
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 1
    assert result["message"] == "Stopped 1 of 2 PQ(s), 1 failed"



def test_format_named_string_list():
    """Test _format_named_string_list formats NamedStringList correctly."""
    mock_nsl = MagicMock()
    mock_nsl.name = "my_group"
    mock_nsl.values = ["value1", "value2", "value3"]

    result = _format_named_string_list(mock_nsl)

    assert result["name"] == "my_group"
    assert result["values"] == ["value1", "value2", "value3"]



def test_format_named_string_list_empty():
    """Test _format_named_string_list handles empty values list."""
    mock_nsl = MagicMock()
    mock_nsl.name = "empty_group"
    mock_nsl.values = []

    result = _format_named_string_list(mock_nsl)

    assert result["name"] == "empty_group"
    assert result["values"] == []



def test_format_worker_protocol():
    """Test _format_worker_protocol formats WorkerProtocolMessage correctly."""
    mock_wp = MagicMock()
    mock_wp.name = "grpc"
    mock_wp.port = 9000

    result = _format_worker_protocol(mock_wp)

    assert result["name"] == "grpc"
    assert result["port"] == 9000



def test_format_worker_protocol_with_zero_port():
    """Test _format_worker_protocol handles zero port value."""
    mock_wp = MagicMock()
    mock_wp.name = "http"
    mock_wp.port = 0

    result = _format_worker_protocol(mock_wp)

    assert result["name"] == "http"
    assert result["port"] == 0



def test_format_column_definition():
    """Test _format_column_definition formats ColumnDefinitionMessage correctly."""
    mock_col = MagicMock()
    mock_col.name = "my_column"
    mock_col.dataType = "int64"
    mock_col.componentType = "java.lang.Long"
    mock_col.columnType = 1
    mock_col.isVarSizeString = False
    mock_col.encoding = 2
    mock_col.codec = "lz4"
    mock_col.codecArgs = "level=5"
    mock_col.objectWidthBytes = 8

    result = _format_column_definition(mock_col)

    assert result["name"] == "my_column"
    assert result["data_type"] == "int64"
    assert result["component_type"] == "java.lang.Long"
    assert result["column_type"] == 1
    assert result["is_var_size_string"] is False
    assert result["encoding"] == 2
    assert result["codec"] == "lz4"
    assert result["codec_args"] == "level=5"
    assert result["object_width_bytes"] == 8



def test_format_column_definition_with_empty_values():
    """Test _format_column_definition handles empty values correctly."""
    mock_col = MagicMock()
    mock_col.name = "sparse_column"
    mock_col.dataType = ""
    mock_col.componentType = ""
    mock_col.columnType = 0
    mock_col.isVarSizeString = True
    mock_col.encoding = 0
    mock_col.codec = ""
    mock_col.codecArgs = ""
    mock_col.objectWidthBytes = 0

    result = _format_column_definition(mock_col)

    assert result["name"] == "sparse_column"
    assert result["data_type"] is None
    assert result["component_type"] is None
    assert result["column_type"] is None
    assert result["is_var_size_string"] is True
    assert result["encoding"] is None
    assert result["codec"] is None
    assert result["codec_args"] is None
    assert result["object_width_bytes"] is None



def test_format_table_definition():
    """Test _format_table_definition formats TableDefinitionMessage correctly."""
    mock_col = MagicMock()
    mock_col.name = "id"
    mock_col.dataType = "int"
    mock_col.componentType = ""
    mock_col.columnType = 1
    mock_col.isVarSizeString = False
    mock_col.encoding = 0
    mock_col.codec = ""
    mock_col.codecArgs = ""
    mock_col.objectWidthBytes = 0

    mock_td = MagicMock()
    mock_td.namespace = "my_namespace"
    mock_td.tableName = "my_table"
    mock_td.columns = [mock_col]
    mock_td.storageType = 1

    result = _format_table_definition(mock_td)

    assert result["namespace"] == "my_namespace"
    assert result["table_name"] == "my_table"
    assert result["storage_type"] == 1
    assert len(result["columns"]) == 1
    assert result["columns"][0]["name"] == "id"



def test_format_table_definition_with_empty_values():
    """Test _format_table_definition handles empty values correctly."""
    mock_td = MagicMock()
    mock_td.namespace = ""
    mock_td.tableName = ""
    mock_td.columns = []
    mock_td.storageType = 0

    result = _format_table_definition(mock_td)

    assert result["namespace"] is None
    assert result["table_name"] is None
    assert result["columns"] == []
    assert result["storage_type"] is None



@patch("deephaven_mcp.mcp_systems_server._tools.pq.ExportedObjectTypeEnum")
def test_format_exported_object_info(mock_exported_enum):
    """Test _format_exported_object_info formats ExportedObjectInfoMessage correctly."""
    mock_exported_enum.Name.side_effect = lambda x: {1: "EOT_TABLE"}.get(
        x, f"EOT_UNKNOWN_{x}"
    )

    mock_obj = MagicMock()
    mock_obj.name = "my_table"
    mock_obj.type = 1
    mock_obj.tableDefinition = None
    mock_obj.originalType = "io.deephaven.db.tables.Table"

    result = _format_exported_object_info(mock_obj)

    assert result["name"] == "my_table"
    assert result["type"] == "EOT_TABLE"
    assert result["table_definition"] is None
    assert result["original_type"] == "io.deephaven.db.tables.Table"



@patch("deephaven_mcp.mcp_systems_server._tools.pq.ExportedObjectTypeEnum")
def test_format_exported_object_info_with_table_definition(mock_exported_enum):
    """Test _format_exported_object_info includes nested tableDefinition."""
    mock_exported_enum.Name.side_effect = lambda x: {1: "EOT_TABLE"}.get(
        x, f"EOT_UNKNOWN_{x}"
    )

    mock_col = MagicMock()
    mock_col.name = "id"
    mock_col.dataType = "int"
    mock_col.componentType = ""
    mock_col.columnType = 0
    mock_col.isVarSizeString = False
    mock_col.encoding = 0
    mock_col.codec = ""
    mock_col.codecArgs = ""
    mock_col.objectWidthBytes = 0

    mock_td = MagicMock()
    mock_td.namespace = "ns"
    mock_td.tableName = "tbl"
    mock_td.columns = [mock_col]
    mock_td.storageType = 1

    mock_obj = MagicMock()
    mock_obj.name = "my_table"
    mock_obj.type = 1
    mock_obj.tableDefinition = mock_td
    mock_obj.originalType = ""

    result = _format_exported_object_info(mock_obj)

    assert result["name"] == "my_table"
    assert result["type"] == "EOT_TABLE"
    assert result["table_definition"] is not None
    assert result["table_definition"]["namespace"] == "ns"
    assert result["original_type"] is None



def test_format_connection_details():
    """Test _format_connection_details formats ProcessorConnectionDetailsMessage correctly."""
    mock_protocol = MagicMock()
    mock_protocol.name = "grpc"
    mock_protocol.port = 9000

    mock_cd = MagicMock()
    mock_cd.protocols = [mock_protocol]
    mock_cd.workerName = "worker-1"
    mock_cd.processInfoId = "pid-123"
    mock_cd.processorHost = "host.example.com"
    mock_cd.envoyPrefix = "/envoy"
    mock_cd.grpcUrl = "grpc://localhost:10000"
    mock_cd.staticUrl = "http://static.example.com"
    mock_cd.enterpriseWebSocketUrl = "wss://ws.example.com"

    result = _format_connection_details(mock_cd)

    assert result["protocols"] == [{"name": "grpc", "port": 9000}]
    assert result["worker_name"] == "worker-1"
    assert result["process_info_id"] == "pid-123"
    assert result["processor_host"] == "host.example.com"
    assert result["envoy_prefix"] == "/envoy"
    assert result["grpc_url"] == "grpc://localhost:10000"
    assert result["static_url"] == "http://static.example.com"
    assert result["enterprise_web_socket_url"] == "wss://ws.example.com"



def test_format_connection_details_with_empty_values():
    """Test _format_connection_details handles empty values correctly."""
    mock_cd = MagicMock()
    mock_cd.protocols = []
    mock_cd.workerName = ""
    mock_cd.processInfoId = ""
    mock_cd.processorHost = ""
    mock_cd.envoyPrefix = ""
    mock_cd.grpcUrl = ""
    mock_cd.staticUrl = ""
    mock_cd.enterpriseWebSocketUrl = ""

    result = _format_connection_details(mock_cd)

    assert result["protocols"] == []
    assert result["worker_name"] is None
    assert result["process_info_id"] is None
    assert result["processor_host"] is None
    assert result["envoy_prefix"] is None
    assert result["grpc_url"] is None
    assert result["static_url"] is None
    assert result["enterprise_web_socket_url"] is None



def test_format_exception_details():
    """Test _format_exception_details formats ExceptionDetailsMessage correctly."""
    mock_ed = MagicMock()
    mock_ed.errorMessage = "Something went wrong"
    mock_ed.stackTrace = "at line 1\nat line 2"
    mock_ed.shortCauses = "Error cause"

    result = _format_exception_details(mock_ed)

    assert result["error_message"] == "Something went wrong"
    assert result["stack_trace"] == "at line 1\nat line 2"
    assert result["short_causes"] == "Error cause"



def test_format_exception_details_with_empty_values():
    """Test _format_exception_details handles empty values correctly."""
    mock_ed = MagicMock()
    mock_ed.errorMessage = ""
    mock_ed.stackTrace = ""
    mock_ed.shortCauses = ""

    result = _format_exception_details(mock_ed)

    assert result["error_message"] is None
    assert result["stack_trace"] is None
    assert result["short_causes"] is None



def test_format_pq_state_with_none():
    """Test _format_pq_state returns None when state is None."""
    result = _format_pq_state(None)
    assert result is None



@patch("deephaven_mcp.mcp_systems_server._tools.pq.ExportedObjectTypeEnum")
def test_format_pq_state_unknown_enum_fallback(mock_exported_enum):
    """Test _format_pq_state handles unknown enum values gracefully (version mismatch)."""
    # Simulate server sending an enum value unknown to client's proto definition
    mock_exported_enum.Name.side_effect = ValueError(
        "Enum has no name defined for value 10"
    )

    mock_state = MagicMock()
    mock_pb = MagicMock()
    mock_state.status.name = "RUNNING"
    mock_pb.serial = 12345
    mock_pb.version = 1
    mock_pb.initializationStartNanos = 0
    mock_pb.initializationCompleteNanos = 0
    mock_pb.lastUpdateNanos = 0
    mock_pb.dispatcherHost = ""
    mock_pb.tableGroups = []

    # scopeTypes with unknown enum value
    # ExportedObjectInfoMessage has: name, type (enum), tableDefinition, originalType
    mock_obj = MagicMock()
    mock_obj.name = "unknown_object"
    mock_obj.type = 10  # Unknown enum value from newer server
    mock_obj.tableDefinition = None
    mock_obj.originalType = "some.unknown.Type"
    mock_pb.scopeTypes = [mock_obj]

    mock_pb.connectionDetails = None
    mock_pb.exceptionDetails = None
    mock_pb.typeSpecificStateJson = ""
    mock_pb.lastAuthenticatedUser = ""
    mock_pb.lastEffectiveUser = ""
    mock_pb.scriptLoaderStateJson = ""
    mock_pb.hasProgress = False
    mock_pb.progressValue = 0
    mock_pb.progressMessage = ""
    mock_pb.engineVersion = ""
    mock_pb.dispatcherPort = 0
    mock_pb.shouldStopNanos = 0
    mock_pb.numFailures = 0
    mock_pb.lastFailureTimeNanos = 0
    mock_pb.replicaSlot = 0
    mock_pb.statusDetails = ""
    mock_state.pb = mock_pb

    result = _format_pq_state(mock_state)

    # Should return fallback string for unknown enum
    assert result["scope_types"] == [
        {
            "name": "unknown_object",
            "type": "UNKNOWN_EXPORTED_TYPE_10",
            "table_definition": None,
            "original_type": "some.unknown.Type",
        }
    ]



@patch("deephaven_mcp.mcp_systems_server._tools.pq.ExportedObjectTypeEnum")
def test_format_pq_state_with_table_definition(mock_exported_enum):
    """Test _format_pq_state properly formats tableDefinition in scopeTypes."""
    mock_exported_enum.Name.side_effect = lambda x: {1: "EOT_TABLE"}.get(
        x, f"EOT_UNKNOWN_{x}"
    )

    mock_state = MagicMock()
    mock_pb = MagicMock()
    mock_state.status.name = "RUNNING"
    mock_pb.serial = 12345
    mock_pb.version = 1
    mock_pb.initializationStartNanos = 0
    mock_pb.initializationCompleteNanos = 0
    mock_pb.lastUpdateNanos = 0
    mock_pb.dispatcherHost = ""
    mock_pb.tableGroups = []

    # scopeTypes with tableDefinition
    # TableDefinitionMessage has: namespace, tableName, columns (repeated), storageType
    mock_col = MagicMock()
    mock_col.name = "id"
    mock_col.dataType = "int"
    mock_col.componentType = ""
    mock_col.columnType = 1
    mock_col.isVarSizeString = False
    mock_col.encoding = 0
    mock_col.codec = ""
    mock_col.codecArgs = ""
    mock_col.objectWidthBytes = 0

    mock_table_def = MagicMock()
    mock_table_def.namespace = "my_namespace"
    mock_table_def.tableName = "my_table"
    mock_table_def.columns = [mock_col]
    mock_table_def.storageType = 1

    mock_obj = MagicMock()
    mock_obj.name = "table_with_def"
    mock_obj.type = 1
    mock_obj.tableDefinition = mock_table_def
    mock_obj.originalType = "io.deephaven.db.tables.Table"
    mock_pb.scopeTypes = [mock_obj]

    mock_pb.connectionDetails = None
    mock_pb.exceptionDetails = None
    mock_pb.typeSpecificStateJson = ""
    mock_pb.lastAuthenticatedUser = ""
    mock_pb.lastEffectiveUser = ""
    mock_pb.scriptLoaderStateJson = ""
    mock_pb.hasProgress = False
    mock_pb.progressValue = 0
    mock_pb.progressMessage = ""
    mock_pb.engineVersion = ""
    mock_pb.dispatcherPort = 0
    mock_pb.shouldStopNanos = 0
    mock_pb.numFailures = 0
    mock_pb.lastFailureTimeNanos = 0
    mock_pb.replicaSlot = 0
    mock_pb.statusDetails = ""
    mock_state.pb = mock_pb

    result = _format_pq_state(mock_state)

    # Verify tableDefinition is properly formatted
    assert len(result["scope_types"]) == 1
    scope_type = result["scope_types"][0]
    assert scope_type["name"] == "table_with_def"
    assert scope_type["type"] == "EOT_TABLE"
    assert scope_type["original_type"] == "io.deephaven.db.tables.Table"

    table_def = scope_type["table_definition"]
    assert table_def["namespace"] == "my_namespace"
    assert table_def["table_name"] == "my_table"
    assert table_def["storage_type"] == 1
    assert len(table_def["columns"]) == 1

    col = table_def["columns"][0]
    assert col["name"] == "id"
    assert col["data_type"] == "int"
    assert col["component_type"] is None
    assert col["column_type"] == 1
    assert col["is_var_size_string"] is False



@patch("deephaven_mcp.mcp_systems_server._tools.pq.ExportedObjectTypeEnum")
def test_format_pq_state_with_all_fields(mock_exported_enum):
    """Test _format_pq_state extracts all 25 fields from protobuf."""
    # Set up ExportedObjectTypeEnum.Name() to return proper names
    mock_exported_enum.Name.side_effect = lambda x: {1: "EOT_TABLE"}.get(
        x, f"EOT_UNKNOWN_{x}"
    )

    mock_state = MagicMock()
    mock_pb = MagicMock()

    # Set all 25 protobuf fields
    mock_pb.serial = 12345
    mock_pb.version = 1

    # Set up status using wrapper's status.name property
    mock_state.status.name = "RUNNING"
    mock_pb.initializationStartNanos = 1734467100000000000
    mock_pb.initializationCompleteNanos = 1734467150000000000
    mock_pb.lastUpdateNanos = 1734467200000000000
    mock_pb.dispatcherHost = "dispatcher.example.com"

    # tableGroups - repeated NamedStringList
    # NamedStringList has: name (string), values (repeated string)
    mock_group = MagicMock()
    mock_group.name = "group1"
    mock_group.values = ["table1", "table2"]
    mock_pb.tableGroups = [mock_group]

    # scopeTypes - repeated ExportedObjectInfoMessage
    # ExportedObjectTypeEnum.Name() is patched at the test function level
    # ExportedObjectInfoMessage has: name, type (enum), tableDefinition, originalType
    mock_obj1 = MagicMock()
    mock_obj1.name = "table1"
    mock_obj1.type = (
        1  # enum as int - will be converted via ExportedObjectTypeEnum.Name()
    )
    mock_obj1.tableDefinition = None
    mock_obj1.originalType = "io.deephaven.db.tables.Table"

    mock_obj2 = MagicMock()
    mock_obj2.name = "table2"
    mock_obj2.type = 1  # enum as int
    mock_obj2.tableDefinition = None
    mock_obj2.originalType = ""
    mock_pb.scopeTypes = [mock_obj1, mock_obj2]

    # connectionDetails - optional ProcessorConnectionDetailsMessage
    # Uses protocols (repeated WorkerProtocolMessage)
    # WorkerProtocolMessage has: name (string), port (int32)
    # ProcessorConnectionDetailsMessage has: protocols, workerName, processInfoId, processorHost, envoyPrefix, grpcUrl, staticUrl, enterpriseWebSocketUrl
    mock_protocol = MagicMock()
    mock_protocol.name = "grpc"
    mock_protocol.port = 9000
    mock_pb.connectionDetails = MagicMock()
    mock_pb.connectionDetails.protocols = [mock_protocol]
    mock_pb.connectionDetails.workerName = "worker-1"
    mock_pb.connectionDetails.processInfoId = "pid-123"
    mock_pb.connectionDetails.processorHost = "processor.example.com"
    mock_pb.connectionDetails.envoyPrefix = "/envoy"
    mock_pb.connectionDetails.grpcUrl = "grpc://localhost:10000"
    mock_pb.connectionDetails.staticUrl = "http://static.example.com"
    mock_pb.connectionDetails.enterpriseWebSocketUrl = "wss://ws.example.com"

    # exceptionDetails - optional ExceptionDetailsMessage
    # ExceptionDetailsMessage has: errorMessage, stackTrace, shortCauses
    mock_pb.exceptionDetails = MagicMock()
    mock_pb.exceptionDetails.errorMessage = "RuntimeError: Test error"
    mock_pb.exceptionDetails.stackTrace = "at line 1\nat line 2"
    mock_pb.exceptionDetails.shortCauses = "Test error"

    mock_pb.typeSpecificStateJson = '{"key": "value"}'
    mock_pb.lastAuthenticatedUser = "user1"
    mock_pb.lastEffectiveUser = "user1-effective"
    mock_pb.scriptLoaderStateJson = '{"loader": "state"}'
    mock_pb.hasProgress = True
    mock_pb.progressValue = 75
    mock_pb.progressMessage = "Processing..."
    mock_pb.engineVersion = "1.2.3"
    mock_pb.dispatcherPort = 8080
    mock_pb.shouldStopNanos = 1734467300000000000
    mock_pb.numFailures = 2
    mock_pb.lastFailureTimeNanos = 1734467250000000000
    mock_pb.replicaSlot = 1
    mock_pb.statusDetails = "Running normally"

    mock_state.pb = mock_pb

    result = _format_pq_state(mock_state)

    # Verify all 25 fields
    assert result["serial"] == 12345
    assert result["version"] == 1
    assert result["status"] == "RUNNING"
    assert result["initialization_start_nanos"] == 1734467100000000000
    assert result["initialization_complete_nanos"] == 1734467150000000000
    assert result["last_update_nanos"] == 1734467200000000000
    assert result["dispatcher_host"] == "dispatcher.example.com"
    # table_groups now returns list of dicts with name and values
    assert result["table_groups"] == [
        {"name": "group1", "values": ["table1", "table2"]}
    ]
    assert result["scope_types"] == [
        {
            "name": "table1",
            "type": "EOT_TABLE",
            "table_definition": None,
            "original_type": "io.deephaven.db.tables.Table",
        },
        {
            "name": "table2",
            "type": "EOT_TABLE",
            "table_definition": None,
            "original_type": None,
        },
    ]
    # connection_details now has protocols list and all 8 fields
    assert result["connection_details"]["protocols"] == [{"name": "grpc", "port": 9000}]
    assert result["connection_details"]["worker_name"] == "worker-1"
    assert result["connection_details"]["process_info_id"] == "pid-123"
    assert result["connection_details"]["processor_host"] == "processor.example.com"
    assert result["connection_details"]["envoy_prefix"] == "/envoy"
    assert result["connection_details"]["grpc_url"] == "grpc://localhost:10000"
    assert result["connection_details"]["static_url"] == "http://static.example.com"
    assert (
        result["connection_details"]["enterprise_web_socket_url"]
        == "wss://ws.example.com"
    )
    # exception_details now has stack_trace
    assert result["exception_details"] == {
        "error_message": "RuntimeError: Test error",
        "stack_trace": "at line 1\nat line 2",
        "short_causes": "Test error",
    }
    assert result["type_specific_state_json"] == '{"key": "value"}'
    assert result["last_authenticated_user"] == "user1"
    assert result["last_effective_user"] == "user1-effective"
    assert result["script_loader_state_json"] == '{"loader": "state"}'
    assert result["has_progress"] is True
    assert result["progress_value"] == 75
    assert result["progress_message"] == "Processing..."
    assert result["engine_version"] == "1.2.3"
    assert result["dispatcher_port"] == 8080
    assert result["should_stop_nanos"] == 1734467300000000000
    assert result["num_failures"] == 2
    assert result["last_failure_time_nanos"] == 1734467250000000000
    assert result["replica_slot"] == 1
    assert result["status_details"] == "Running normally"



def test_format_pq_state_with_empty_connection_details():
    """Test _format_pq_state handles empty protocols list in connectionDetails."""
    mock_state = MagicMock()
    mock_pb = MagicMock()

    mock_pb.serial = 99999
    mock_pb.version = 2
    mock_pb.status.name = "FAILED"
    mock_pb.initializationStartNanos = 0
    mock_pb.initializationCompleteNanos = 0
    mock_pb.lastUpdateNanos = 1734467200000000000
    mock_pb.dispatcherHost = ""
    mock_pb.tableGroups = []
    mock_pb.scopeTypes = []

    # connectionDetails exists but protocols list is empty
    # Use correct camelCase field names from protobuf
    mock_connection = MagicMock()
    mock_connection.protocols = []  # Empty protocols list
    mock_connection.workerName = ""
    mock_connection.processInfoId = ""
    mock_connection.processorHost = ""
    mock_connection.envoyPrefix = ""
    mock_connection.grpcUrl = ""
    mock_connection.staticUrl = ""
    mock_connection.enterpriseWebSocketUrl = ""
    mock_pb.connectionDetails = mock_connection

    # exceptionDetails with empty values
    mock_exception = MagicMock()
    mock_exception.errorMessage = ""
    mock_exception.stackTrace = ""
    mock_exception.shortCauses = ""
    mock_pb.exceptionDetails = mock_exception

    mock_pb.typeSpecificStateJson = ""
    mock_pb.lastAuthenticatedUser = ""
    mock_pb.lastEffectiveUser = ""
    mock_pb.scriptLoaderStateJson = ""
    mock_pb.hasProgress = False
    mock_pb.progressValue = 0
    mock_pb.progressMessage = ""
    mock_pb.engineVersion = ""
    mock_pb.dispatcherPort = 0
    mock_pb.shouldStopNanos = 0
    mock_pb.numFailures = 0
    mock_pb.lastFailureTimeNanos = 0
    mock_pb.replicaSlot = 0
    mock_pb.statusDetails = ""

    mock_state.pb = mock_pb

    result = _format_pq_state(mock_state)

    assert result is not None
    assert result["serial"] == 99999
    # Empty protocols list means protocols is empty list
    assert result["connection_details"]["protocols"] == []
    assert result["connection_details"]["worker_name"] is None
    assert result["exception_details"] == {
        "error_message": None,
        "stack_trace": None,
        "short_causes": None,
    }



def test_format_pq_replicas_empty():
    """Test _format_pq_replicas returns empty list for empty input."""
    result = _format_pq_replicas([])
    assert result == []



def test_format_pq_replicas_with_data():
    """Test _format_pq_replicas formats replica states correctly."""
    mock_replica1 = MagicMock()
    mock_replica1.pb.serial = 12345
    mock_replica1.pb.version = 1

    # Set up status using wrapper's status.name property
    mock_replica1.status.name = "RUNNING"
    mock_replica1.pb.initializationStartNanos = 1734467100000000000
    mock_replica1.pb.initializationCompleteNanos = 1734467150000000000
    mock_replica1.pb.lastUpdateNanos = 1734467200000000000
    mock_replica1.pb.dispatcherHost = "dispatcher.example.com"
    mock_replica1.pb.tableGroups = []
    mock_replica1.pb.scopeTypes = []
    mock_replica1.pb.connectionDetails = None
    mock_replica1.pb.exceptionDetails = None
    mock_replica1.pb.typeSpecificStateJson = ""
    mock_replica1.pb.lastAuthenticatedUser = ""
    mock_replica1.pb.lastEffectiveUser = ""
    mock_replica1.pb.scriptLoaderStateJson = ""
    mock_replica1.pb.hasProgress = False
    mock_replica1.pb.progressValue = 0
    mock_replica1.pb.progressMessage = ""
    mock_replica1.pb.engineVersion = ""
    mock_replica1.pb.dispatcherPort = 8080
    mock_replica1.pb.shouldStopNanos = 0
    mock_replica1.pb.numFailures = 0
    mock_replica1.pb.lastFailureTimeNanos = 0
    mock_replica1.pb.replicaSlot = 1
    mock_replica1.pb.statusDetails = ""

    result = _format_pq_replicas([mock_replica1])

    assert len(result) == 1
    assert result[0]["serial"] == 12345
    assert result[0]["replica_slot"] == 1
    assert result[0]["status"] == "RUNNING"



def test_format_pq_replicas_filters_none():
    """Test _format_pq_replicas filters out None entries."""
    mock_replica = MagicMock()
    mock_replica.pb.serial = 12345
    mock_replica.pb.version = 1

    # Set up status using wrapper's status.name property
    mock_replica.status.name = "RUNNING"
    mock_replica.pb.initializationStartNanos = 0
    mock_replica.pb.initializationCompleteNanos = 0
    mock_replica.pb.lastUpdateNanos = 0
    mock_replica.pb.dispatcherHost = ""
    mock_replica.pb.tableGroups = []
    mock_replica.pb.scopeTypes = []
    mock_replica.pb.connectionDetails = None
    mock_replica.pb.exceptionDetails = None
    mock_replica.pb.typeSpecificStateJson = ""
    mock_replica.pb.lastAuthenticatedUser = ""
    mock_replica.pb.lastEffectiveUser = ""
    mock_replica.pb.scriptLoaderStateJson = ""
    mock_replica.pb.hasProgress = False
    mock_replica.pb.progressValue = 0
    mock_replica.pb.progressMessage = ""
    mock_replica.pb.engineVersion = ""
    mock_replica.pb.dispatcherPort = 0
    mock_replica.pb.shouldStopNanos = 0
    mock_replica.pb.numFailures = 0
    mock_replica.pb.lastFailureTimeNanos = 0
    mock_replica.pb.replicaSlot = 0
    mock_replica.pb.statusDetails = ""
    mock_replica.pb.killTime = 0
    mock_replica.pb.assignedWorkerGroupId = 0
    mock_replica.pb.configId = ""

    result = _format_pq_replicas([mock_replica, None])
    assert len(result) == 1



def test_format_pq_spares_empty():
    """Test _format_pq_spares returns empty list for empty input."""
    result = _format_pq_spares([])
    assert result == []



def test_format_pq_spares_with_data():
    """Test _format_pq_spares formats spare states correctly."""
    mock_spare = MagicMock()
    mock_spare.pb.serial = 12345
    mock_spare.pb.version = 1

    # Set up status using wrapper's status.name property
    mock_spare.status.name = "INITIALIZING"
    mock_spare.pb.initializationStartNanos = 1734467150000000000
    mock_spare.pb.initializationCompleteNanos = 1734467200000000000
    mock_spare.pb.lastUpdateNanos = 1734467250000000000
    mock_spare.pb.dispatcherHost = "dispatcher.example.com"
    mock_spare.pb.tableGroups = []
    mock_spare.pb.scopeTypes = []
    mock_spare.pb.connectionDetails = None
    mock_spare.pb.exceptionDetails = None
    mock_spare.pb.typeSpecificStateJson = ""
    mock_spare.pb.lastAuthenticatedUser = ""
    mock_spare.pb.lastEffectiveUser = ""
    mock_spare.pb.scriptLoaderStateJson = ""
    mock_spare.pb.hasProgress = False
    mock_spare.pb.progressValue = 0
    mock_spare.pb.progressMessage = ""
    mock_spare.pb.engineVersion = ""
    mock_spare.pb.dispatcherPort = 8080
    mock_spare.pb.shouldStopNanos = 0
    mock_spare.pb.numFailures = 0
    mock_spare.pb.lastFailureTimeNanos = 0
    mock_spare.pb.replicaSlot = 2
    mock_spare.pb.statusDetails = ""

    result = _format_pq_spares([mock_spare])

    assert len(result) == 1
    assert result[0]["serial"] == 12345
    assert result[0]["status"] == "INITIALIZING"
    assert result[0]["replica_slot"] == 2



def test_format_pq_spares_filters_none():
    """Test _format_pq_spares filters out None entries."""
    mock_spare = MagicMock()
    mock_spare.pb.serial = 12345
    mock_spare.pb.version = 1

    # Set up status using wrapper's status.name property
    mock_spare.status.name = "INITIALIZING"
    mock_spare.pb.initializationStartNanos = 0
    mock_spare.pb.initializationCompleteNanos = 0
    mock_spare.pb.lastUpdateNanos = 0
    mock_spare.pb.dispatcherHost = ""
    mock_spare.pb.tableGroups = []
    mock_spare.pb.scopeTypes = []
    mock_spare.pb.connectionDetails = None
    mock_spare.pb.exceptionDetails = None
    mock_spare.pb.typeSpecificStateJson = ""
    mock_spare.pb.lastAuthenticatedUser = ""
    mock_spare.pb.lastEffectiveUser = ""
    mock_spare.pb.scriptLoaderStateJson = ""
    mock_spare.pb.hasProgress = False
    mock_spare.pb.progressValue = 0
    mock_spare.pb.progressMessage = ""
    mock_spare.pb.engineVersion = ""
    mock_spare.pb.dispatcherPort = 0
    mock_spare.pb.shouldStopNanos = 0
    mock_spare.pb.numFailures = 0
    mock_spare.pb.lastFailureTimeNanos = 0
    mock_spare.pb.replicaSlot = 0
    mock_spare.pb.statusDetails = ""
    mock_spare.pb.flightSessionId = ""
    mock_spare.pb.sessionToken = ""
    mock_spare.pb.tokenExpirationTime = 0
    mock_spare.pb.queryInfoJson = ""
    mock_spare.pb.tempQueryId = 0
    mock_spare.pb.totalMemoryMB = 0
    mock_spare.pb.grpcAddress = ""
    mock_spare.pb.flightAddress = ""
    mock_spare.pb.httpPort = 0
    mock_spare.pb.lastActivityTime = 0
    mock_spare.pb.assignedDispatcherId = 0
    mock_spare.pb.killTime = 0
    mock_spare.pb.assignedWorkerGroupId = 0
    mock_spare.pb.configId = ""

    result = _format_pq_spares([None, mock_spare])
    assert len(result) == 1



@pytest.mark.asyncio
async def test_pq_list_success():
    """Test successful PQ listing."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Create mock PQ map
    mock_pq_map = {
        12345: create_mock_pq_info(12345, "analytics", "RUNNING", 8.0),
        12346: create_mock_pq_info(12346, "reporting", "STOPPED", 4.0),
    }
    mock_controller.map = AsyncMock(return_value=mock_pq_map)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_list(context, system_name="test-system")

    # Verify success
    assert result["success"] is True
    assert result["system_name"] == "test-system"
    assert len(result["pqs"]) == 2

    # Verify PQ1 summary data (trimmed response - no full config/state_details)
    pq1 = result["pqs"][0]
    assert pq1["pq_id"] == "enterprise:test-system:12345"
    assert pq1["serial"] == 12345
    assert pq1["name"] == "analytics"
    assert pq1["status"] == "RUNNING"
    assert pq1["enabled"] is True
    assert pq1["owner"] == "test_user"
    assert pq1["heap_size_gb"] == 8.0
    assert pq1["worker_kind"] == "DeephavenCommunity"
    assert pq1["configuration_type"] == "Script"
    assert pq1["script_language"] == "Python"
    assert pq1["server_name"] is None  # Empty string -> None
    assert pq1["admin_groups"] == []
    assert pq1["viewer_groups"] == []
    assert pq1["is_scheduled"] is False
    assert pq1["num_failures"] == 0
    assert "session_id" in pq1  # Running PQ should have session_id
    assert (
        pq1["session_id"] == "enterprise:test-system:analytics"
    )  # session_id uses name

    # Verify trimmed response does NOT include full config/state_details/replicas/spares
    assert "config" not in pq1
    assert "state_details" not in pq1
    assert "replicas" not in pq1
    assert "spares" not in pq1

    # Verify PQ2 summary data
    pq2 = result["pqs"][1]
    assert pq2["pq_id"] == "enterprise:test-system:12346"
    assert pq2["name"] == "reporting"
    assert pq2["status"] == "STOPPED"
    assert pq2["enabled"] is True
    assert pq2["owner"] == "test_user"
    assert pq2["heap_size_gb"] == 4.0
    assert pq2["worker_kind"] == "DeephavenCommunity"
    assert pq2["configuration_type"] == "Script"
    assert pq2["script_language"] == "Python"
    assert "session_id" not in pq2  # Stopped PQ should not have session_id



@pytest.mark.asyncio
async def test_pq_list_system_not_found():
    """Test pq_list when system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_list(context, system_name="nonexistent")

    assert result["success"] is False
    assert "not found" in result["error"]



@pytest.mark.asyncio
async def test_pq_list_exception():
    """Test pq_list when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_list(context, system_name="prod")

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@pytest.mark.asyncio
@patch("deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum")
@patch("deephaven_mcp.mcp_systems_server._tools.pq.ExportedObjectTypeEnum")
async def test_pq_details_success_by_name(mock_exported_enum, mock_restart_enum):
    """Test successful PQ details retrieval using pq_id."""
    # Set up enum mocks
    mock_restart_enum.Name.side_effect = lambda x: {0: "RU_ADMIN", 1: "RU_ALL"}.get(
        x, f"RU_UNKNOWN_{x}"
    )
    mock_exported_enum.Name.side_effect = lambda x: {1: "EOT_TABLE"}.get(
        x, f"EOT_UNKNOWN_{x}"
    )

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller.map() to return PQ map (pq_details uses map() to ensure subscription is ready)
    mock_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: mock_pq_info})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_details(context, pq_id="enterprise:test-system:12345")

    # Verify success
    assert result["success"] is True
    assert result["pq_id"] == "enterprise:test-system:12345"
    assert result["serial"] == 12345
    assert result["name"] == "analytics"
    assert result["state"] == "RUNNING"
    assert "session_id" in result
    assert result["session_id"] == "enterprise:test-system:analytics"

    # Verify comprehensive config fields from PersistentQueryConfigMessage
    config = result["config"]
    # Basic fields
    assert config["serial"] == 12345
    assert config["version"] == 1
    assert config["owner"] == "test_user"
    assert config["enabled"] is True
    # Resource fields
    assert config["heap_size_gb"] == 8.0
    assert config["buffer_pool_to_heap_ratio"] == 0.0
    assert config["detailed_gc_logging_enabled"] is False
    assert config["worker_kind"] == "DeephavenCommunity"
    # Script fields
    assert config["script_language"] == "Python"
    assert config["configuration_type"] == "Script"
    assert config["type_specific_fields_json"] is None
    assert config["server_name"] is None
    assert config["script_path"] is None
    assert config["script_code"] is None
    # Advanced config
    assert config["jvm_profile"] is None
    assert config["python_control"] is None
    assert config["generic_worker_control"] is None
    assert config["extra_jvm_arguments"] == []
    assert config["extra_environment_variables"] == []
    assert config["class_path_additions"] == []
    # Scheduling
    assert config["scheduling"] == []
    assert config["timeout_nanos"] is None
    # Access control
    assert config["admin_groups"] == []
    assert config["viewer_groups"] == []
    assert config["restart_users"] == "RU_ADMIN"
    # Metadata
    assert config["last_modified_by_authenticated"] is None
    assert config["last_modified_by_effective"] is None
    assert config["last_modified_time_nanos"] is None
    assert config["created_time_nanos"] is None
    assert config["completed_status"] is None
    assert config["expiration_time_nanos"] is None
    # Kubernetes/worker
    assert config["kubernetes_control"] is None
    assert config["replica_count"] == 0
    assert config["spare_count"] == 0
    assert config["assignment_policy"] is None
    assert config["assignment_policy_params"] is None
    assert config["additional_memory_gb"] == 0.0

    # Verify state_details - ALL 25 fields
    state_details = result["state_details"]
    assert state_details is not None
    assert state_details["serial"] == 12345
    assert state_details["version"] == 1
    assert state_details["status"] == "RUNNING"
    assert state_details["initialization_start_nanos"] == 1734467100000000000
    assert state_details["initialization_complete_nanos"] == 1734467150000000000
    assert state_details["last_update_nanos"] == 1734467200000000000
    assert state_details["dispatcher_host"] == "dispatcher.example.com"
    assert state_details["dispatcher_port"] == 8080
    assert state_details["table_groups"] == []
    assert state_details["scope_types"] == []
    assert state_details["connection_details"] is None
    assert state_details["exception_details"] is None
    assert state_details["type_specific_state_json"] is None
    assert state_details["last_authenticated_user"] is None
    assert state_details["last_effective_user"] is None
    assert state_details["script_loader_state_json"] is None
    assert state_details["has_progress"] is False
    assert state_details["progress_value"] == 0
    assert state_details["progress_message"] is None
    assert state_details["engine_version"] is None
    assert state_details["should_stop_nanos"] is None
    assert state_details["num_failures"] == 0
    assert state_details["last_failure_time_nanos"] is None
    assert state_details["replica_slot"] == 0
    assert state_details["status_details"] is None

    # Verify replicas and spares are empty lists
    assert result["replicas"] == []
    assert result["spares"] == []



@pytest.mark.asyncio
async def test_pq_details_success_by_serial():
    """Test successful PQ details retrieval for stopped PQ."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller.map() to return PQ map (pq_details uses map() to ensure subscription is ready)
    mock_pq_info = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: mock_pq_info})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_details(context, pq_id="enterprise:test-system:12345")

    # Verify success
    assert result["success"] is True
    assert result["pq_id"] == "enterprise:test-system:12345"
    assert result["serial"] == 12345
    assert result["name"] == "analytics"
    assert result["state"] == "STOPPED"
    assert "session_id" not in result  # Stopped PQ shouldn't have session_id



@pytest.mark.asyncio
async def test_pq_details_not_found():
    """Test pq_details when PQ not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller.map() to return empty map (PQ not found)
    mock_controller.map = AsyncMock(return_value={})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_details(context, pq_id="enterprise:test-system:99999")

    assert result["success"] is False
    assert "error" in result



@pytest.mark.asyncio
async def test_pq_details_invalid_pq_id():
    """Test pq_details with invalid pq_id format."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_details(context, pq_id="invalid:format")

    assert result["success"] is False
    assert "Invalid pq_id format" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_details_system_not_found():
    """Test pq_details when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_details(context, pq_id="enterprise:nonexistent:12345")

    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_details_exception():
    """Test pq_details when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_details(context, pq_id="enterprise:prod:12345")

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_details_not_found_by_serial():
    """Test pq_details when PQ not found by serial."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller to return None for non-existent PQ
    mock_controller.map = AsyncMock(return_value={})

    full_config = {"enterprise": {"systems": {"prod": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_details(context, pq_id="enterprise:prod:99999")

    assert result["success"] is False
    assert "error" in result



@pytest.mark.asyncio
async def test_pq_create_success():
    """Test successful PQ creation."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_config = MagicMock()
    mock_config.pb = MagicMock()  # Add pb attribute for scriptLanguage setting
    mock_controller.make_pq_config = AsyncMock(return_value=mock_config)
    mock_controller.add_query = AsyncMock(return_value=12345)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_create(
        context,
        system_name="test-system",
        pq_name="new-pq",
        heap_size_gb=8.0,
    )

    # Verify success
    assert result["success"] is True
    assert result["pq_id"] == "enterprise:test-system:12345"
    assert result["serial"] == 12345
    assert result["name"] == "new-pq"
    assert result["state"] == "UNINITIALIZED"



@pytest.mark.asyncio
async def test_pq_create_success_groovy():
    """Test successful PQ creation with Groovy programming language."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_config = MagicMock()
    mock_config.pb = MagicMock()
    mock_controller.make_pq_config = AsyncMock(return_value=mock_config)
    mock_controller.add_query = AsyncMock(return_value=12345)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_create(
        context,
        system_name="test-system",
        pq_name="new-pq",
        heap_size_gb=8.0,
        programming_language="groovy",
    )

    # Verify success
    assert result["success"] is True
    assert result["pq_id"] == "enterprise:test-system:12345"
    assert result["serial"] == 12345
    assert result["name"] == "new-pq"
    assert result["state"] == "UNINITIALIZED"



@pytest.mark.asyncio
async def test_pq_create_invalid_language():
    """Test pq_create with invalid programming language."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)

    full_config = {"enterprise": {"systems": {"prod": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_create(
        context,
        system_name="prod",
        pq_name="new-pq",
        heap_size_gb=8.0,
        programming_language="JavaScript",
    )

    assert result["success"] is False
    assert "Invalid programming_language" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_create_system_not_found():
    """Test pq_create when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_create(
        context, system_name="nonexistent", pq_name="new-pq", heap_size_gb=8.0
    )

    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_create_exception():
    """Test pq_create when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_create(
        context, system_name="prod", pq_name="new-pq", heap_size_gb=8.0
    )

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



def test_validate_and_parse_pq_ids_single():
    """Test _validate_and_parse_pq_ids with single pq_id."""
    parsed_pqs, system_name, error = _validate_and_parse_pq_ids(
        "enterprise:test-system:12345"
    )

    assert error is None
    assert len(parsed_pqs) == 1
    assert parsed_pqs[0][0] == "enterprise:test-system:12345"
    assert parsed_pqs[0][1] == 12345
    assert system_name == "test-system"



def test_validate_and_parse_pq_ids_multiple():
    """Test _validate_and_parse_pq_ids with multiple pq_ids."""
    parsed_pqs, system_name, error = _validate_and_parse_pq_ids(
        ["enterprise:test-system:12345", "enterprise:test-system:67890"]
    )

    assert error is None
    assert len(parsed_pqs) == 2
    assert parsed_pqs[0][0] == "enterprise:test-system:12345"
    assert parsed_pqs[1][0] == "enterprise:test-system:67890"
    assert system_name == "test-system"



def test_validate_and_parse_pq_ids_empty_list():
    """Test _validate_and_parse_pq_ids with empty list."""
    parsed_pqs, system_name, error = _validate_and_parse_pq_ids([])

    assert parsed_pqs is None
    assert system_name is None
    assert error == "At least one pq_id must be provided"



def test_validate_and_parse_pq_ids_different_systems():
    """Test _validate_and_parse_pq_ids with pq_ids from different systems."""
    parsed_pqs, system_name, error = _validate_and_parse_pq_ids(
        ["enterprise:system1:12345", "enterprise:system2:67890"]
    )

    assert parsed_pqs is None
    assert system_name is None
    assert "system1" in error
    assert "system2" in error



def test_validate_and_parse_pq_ids_invalid_format():
    """Test _validate_and_parse_pq_ids with invalid pq_id format."""
    parsed_pqs, system_name, error = _validate_and_parse_pq_ids(
        "invalid-format"
    )

    assert parsed_pqs is None
    assert system_name is None
    assert "Invalid pq_id" in error
    assert "invalid-format" in error



@pytest.mark.asyncio
async def test_pq_delete_success_by_name():
    """Test successful PQ deletion using pq_id."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_controller.delete_query = AsyncMock()
    mock_pq_info = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_controller.get = AsyncMock(return_value=mock_pq_info)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(context, pq_id="enterprise:test-system:12345")

    # Verify success - new results structure
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 1
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["error"] is None
    assert result["summary"]["total"] == 1
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Deleted 1 PQ(s)"
    mock_controller.delete_query.assert_called_once_with(12345)



@pytest.mark.asyncio
async def test_pq_delete_success_custom_timeout():
    """Test successful PQ deletion with custom timeout."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_controller.delete_query = AsyncMock()
    mock_pq_info = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_controller.get = AsyncMock(return_value=mock_pq_info)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(
        context, pq_id="enterprise:test-system:12345", timeout_seconds=20
    )

    # Verify success
    assert result["success"] is True
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"

    # Verify controller.get was called with custom timeout
    mock_controller.get.assert_called_once_with(12345, timeout_seconds=20)
    mock_controller.delete_query.assert_called_once_with(12345)



@pytest.mark.asyncio
async def test_pq_delete_invalid_pq_id():
    """Test pq_delete with invalid pq_id format."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(context, pq_id="invalid:format")

    assert result["success"] is False
    assert "Invalid pq_id format" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_delete_system_not_found():
    """Test pq_delete when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(context, pq_id="enterprise:nonexistent:12345")

    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_delete_exception():
    """Test pq_delete when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(context, pq_id="enterprise:prod:12345")

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_delete_multiple():
    """Test successful deletion of multiple PQs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_controller.delete_query = AsyncMock()
    mock_pq_info1 = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_pq_info2 = create_mock_pq_info(67890, "reporting", "STOPPED", 16.0)
    # First two calls for getting names before deletion, next two would be after but we don't need them
    mock_controller.get = AsyncMock(side_effect=[mock_pq_info1, mock_pq_info2])

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    # Verify success
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 2
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["pq_id"] == "enterprise:test-system:67890"
    assert result["results"][1]["serial"] == 67890
    assert result["results"][1]["success"] is True
    assert result["results"][1]["name"] == "reporting"
    assert result["results"][1]["error"] is None
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 2
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Deleted 2 PQ(s)"
    # Verify delete_query was called for each serial
    assert mock_controller.delete_query.call_count == 2



@pytest.mark.asyncio
async def test_pq_delete_different_systems_error():
    """Test error when trying to delete PQs from different systems."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(
        context, pq_id=["enterprise:system1:12345", "enterprise:system2:67890"]
    )

    # Verify error
    assert result["success"] is False
    assert "All pq_ids must be from the same system" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_delete_empty_list():
    """Test error when trying to delete with empty pq_id list."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(context, pq_id=[])

    # Verify error
    assert result["success"] is False
    assert "At least one pq_id must be provided" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_delete_negative_timeout():
    """Test pq_delete with negative timeout triggers validation error."""
    context = MockContext(
        {"config_manager": MagicMock(), "session_registry": MagicMock()}
    )

    result = await pq_delete(
        context, "enterprise:system1:12345", timeout_seconds=-1
    )

    assert result["success"] is False
    assert "timeout_seconds must be non-negative" in result["error"]
    assert "got -1" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_delete_zero_max_concurrent():
    """Test pq_delete with max_concurrent=0 triggers validation error."""
    context = MockContext(
        {"config_manager": MagicMock(), "session_registry": MagicMock()}
    )

    result = await pq_delete(
        context, "enterprise:system1:12345", max_concurrent=0
    )

    assert result["success"] is False
    assert "max_concurrent must be at least 1" in result["error"]
    assert "got 0" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_modify_success():
    """Test successful PQ modification without restart."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info - this config will be modified in-place
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    current_pq_info.config.pb.scriptCode = "print('old')"

    # Mock controller methods - map() returns dict of {serial: pq_info}
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})
    mock_controller.modify_query = AsyncMock()

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        heap_size_gb=16.0,
        restart=False,
    )

    # Verify success
    assert result["success"] is True
    assert result["pq_id"] == "enterprise:test-system:12345"
    assert result["serial"] == 12345
    assert result["name"] == "analytics"
    assert result["restarted"] is False
    assert "modified successfully" in result["message"]

    # Verify modify_query was called with the existing config (now modified) and restart=False
    mock_controller.modify_query.assert_called_once()
    call_args = mock_controller.modify_query.call_args
    assert (
        call_args[0][0] == current_pq_info.config
    )  # First positional arg is the config
    assert call_args[1]["restart"] is False
    # Verify heap was actually modified in the existing config
    assert current_pq_info.config.pb.heapSizeGb == 16.0



@pytest.mark.asyncio
async def test_pq_modify_with_restart():
    """Test successful PQ modification with restart."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info - config will be modified in-place
    current_pq_info = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)

    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})
    mock_controller.modify_query = AsyncMock()

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        pq_name="analytics_renamed",
        restart=True,
    )

    # Verify success
    assert result["success"] is True
    assert result["serial"] == 12345
    assert result["name"] == "analytics_renamed"
    assert result["restarted"] is True
    assert "restarted" in result["message"]

    # Verify modify_query was called with existing config and restart=True
    call_args = mock_controller.modify_query.call_args
    assert call_args[0][0] == current_pq_info.config  # Existing config was passed
    assert call_args[1]["restart"] is True
    # Verify name was actually modified in the existing config
    assert current_pq_info.config.pb.name == "analytics_renamed"



@pytest.mark.asyncio
async def test_pq_modify_script_path():
    """Test pq_modify with script_path (without script_body) for coverage."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})
    mock_controller.modify_query = AsyncMock()

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        script_path="/path/to/script.py",
        restart=False,
    )

    # Verify success
    assert result["success"] is True
    # Verify script_path was set and script_body cleared
    assert current_pq_info.config.pb.scriptPath == "/path/to/script.py"
    assert current_pq_info.config.pb.scriptCode == ""



@pytest.mark.asyncio
async def test_pq_modify_mutually_exclusive_scripts():
    """Test pq_modify with both script_body and script_path (mutually exclusive)."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        script_body="print('inline')",
        script_path="/path/to/script.py",
    )

    # Verify error
    assert result["success"] is False
    assert "mutually exclusive" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_modify_invalid_pq_id():
    """Test pq_modify with invalid pq_id format."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="invalid:format",
        heap_size_gb=16.0,
    )

    assert result["success"] is False
    assert "Invalid pq_id" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_modify_system_not_found():
    """Test pq_modify when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:nonexistent:12345",
        heap_size_gb=16.0,
    )

    assert result["success"] is False
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_modify_pq_not_found():
    """Test pq_modify when PQ serial not found in controller map."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller.map() to return empty dict (PQ doesn't exist)
    mock_controller.map = AsyncMock(return_value={})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:99999",
        heap_size_gb=16.0,
    )

    assert result["success"] is False
    assert "not found" in result["error"]
    assert "99999" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_modify_invalid_language():
    """Test pq_modify with invalid programming language."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        programming_language="JavaScript",
    )

    assert result["success"] is False
    assert "Invalid programming_language" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_modify_no_changes():
    """Test pq_modify with no parameters provided (should error)."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Call with no modification parameters (only pq_id and restart)
    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        restart=False,
    )

    # Verify error for no changes
    assert result["success"] is False
    assert "No changes specified" in result["error"]
    assert result["isError"] is True
    # Verify modify_query was NOT called
    mock_controller.modify_query.assert_not_called()



@pytest.mark.asyncio
@patch("deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum")
async def test_pq_modify_all_parameters(mock_restart_enum):
    """Test pq_modify with all parameters to achieve full coverage."""
    # Mock RestartUsersEnum.Value() to return numeric enum value
    mock_restart_enum.Value.return_value = 1  # RU_ADMIN = 1

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info
    current_pq_info = create_mock_pq_info(12345, "old_name", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})
    mock_controller.modify_query = AsyncMock()

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Call with all possible parameters
    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        pq_name="new_name",
        script_body="print('test')",
        programming_language="Python",
        configuration_type="Script",
        enabled=False,
        schedule=["0 0 * * *"],
        server="server1",
        engine="DeephavenCommunity",
        jvm_profile="default",
        extra_jvm_args=["-Xmx4g"],
        extra_class_path=["/path/to/lib.jar"],
        python_virtual_environment="/path/to/venv",
        extra_environment_vars=["VAR1=value1"],
        init_timeout_nanos=60000000000,
        auto_delete_timeout=3600,
        admin_groups=["admins"],
        viewer_groups=["viewers"],
        restart_users="RU_ADMIN",
        restart=False,
    )

    # Verify success
    assert result["success"] is True
    assert result["name"] == "new_name"

    # Verify all fields were modified
    config_pb = current_pq_info.config.pb
    assert config_pb.name == "new_name"
    assert config_pb.scriptCode == "print('test')"
    assert config_pb.scriptPath == ""
    assert config_pb.scriptLanguage == "Python"
    assert config_pb.configurationType == "Script"
    assert config_pb.enabled is False
    assert list(config_pb.scheduling) == ["0 0 * * *"]
    assert config_pb.serverName == "server1"
    assert config_pb.workerKind == "DeephavenCommunity"
    assert config_pb.jvmProfile == "default"
    assert list(config_pb.extraJvmArguments) == ["-Xmx4g"]
    assert list(config_pb.classPathAdditions) == ["/path/to/lib.jar"]
    assert config_pb.pythonControl == "/path/to/venv"
    assert list(config_pb.extraEnvironmentVariables) == ["VAR1=value1"]
    assert config_pb.timeoutNanos == 60000000000
    assert config_pb.expirationTimeNanos == 3600000000000  # 3600 seconds * 1e9
    assert list(config_pb.adminGroups) == ["admins"]
    assert list(config_pb.viewerGroups) == ["viewers"]
    # restart_users should be converted to numeric enum value (1 = RU_ADMIN)
    assert config_pb.restartUsers == 1
    mock_restart_enum.Value.assert_called_once_with("RU_ADMIN")



@pytest.mark.asyncio
async def test_pq_modify_clear_auto_delete_timeout():
    """Test pq_modify with auto_delete_timeout=0 to make query permanent."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info with existing timeout
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    current_pq_info.config.pb.expirationTimeNanos = (
        3600000000000  # Currently has 1 hour timeout
    )
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})
    mock_controller.modify_query = AsyncMock()

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Call with auto_delete_timeout=0 to clear expiration (make permanent)
    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        auto_delete_timeout=0,
        restart=False,
    )

    # Verify success
    assert result["success"] is True
    assert result["message"] == "PQ 'analytics' modified successfully"

    # Verify modify_query was called
    mock_controller.modify_query.assert_called_once()

    # Verify expirationTimeNanos was set to 0 (permanent)
    config_pb = current_pq_info.config.pb
    assert config_pb.expirationTimeNanos == 0



@pytest.mark.asyncio
@patch("deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum", None)
async def test_pq_modify_restart_users_enum_not_available():
    """Test pq_modify when RestartUsersEnum is None (enterprise package not installed)."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        restart_users="RU_ADMIN",
    )

    assert result["success"] is False
    assert "Core+ features are not available" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
@patch("deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum")
async def test_pq_modify_invalid_restart_users_value(mock_restart_enum):
    """Test pq_modify with invalid restart_users value to cover ValueError path."""
    # Mock RestartUsersEnum.Value() to raise ValueError for invalid value
    mock_restart_enum.Value.side_effect = ValueError("unknown enum value")
    mock_restart_enum.keys.return_value = [
        "RU_ADMIN",
        "RU_ADMIN_AND_VIEWERS",
        "RU_VIEWERS_WHEN_DOWN",
        "RU_UNSPECIFIED",
    ]

    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock current PQ info
    current_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.map = AsyncMock(return_value={12345: current_pq_info})

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:test-system:12345",
        restart_users="INVALID_VALUE",
    )

    assert result["success"] is False
    assert "Invalid restart_users" in result["error"]
    assert "INVALID_VALUE" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_modify_exception():
    """Test pq_modify when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_modify(
        context,
        pq_id="enterprise:prod:12345",
        heap_size_gb=16.0,
    )

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_start_success():
    """Test successful PQ start using pq_id."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods (no more get_serial_for_name)
    mock_controller.start_and_wait = AsyncMock()
    mock_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.get = AsyncMock(return_value=mock_pq_info)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(context, pq_id="enterprise:test-system:12345")

    # Verify success - new results structure
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 1
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "RUNNING"
    assert result["results"][0]["session_id"] == "enterprise:test-system:analytics"
    assert result["results"][0]["error"] is None
    assert result["summary"]["total"] == 1
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Started 1 PQ(s)"
    # Verify controller.start_and_wait was called with correct timeout
    mock_controller.start_and_wait.assert_called_once_with(12345, 30)



@pytest.mark.asyncio
async def test_pq_start_invalid_pq_id():
    """Test pq_start with invalid pq_id format."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(context, pq_id="invalid:format")

    assert result["success"] is False
    assert "Invalid pq_id format" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_start_system_not_found():
    """Test pq_start when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(context, pq_id="enterprise:nonexistent:12345")

    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_start_exception():
    """Test pq_start when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(context, pq_id="enterprise:prod:12345")

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_start_multiple():
    """Test successful start of multiple PQs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_controller.start_and_wait = AsyncMock()
    mock_pq_info1 = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_pq_info2 = create_mock_pq_info(67890, "reporting", "RUNNING", 16.0)
    mock_controller.get = AsyncMock(side_effect=[mock_pq_info1, mock_pq_info2])

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    # Verify success
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 2
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "RUNNING"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["pq_id"] == "enterprise:test-system:67890"
    assert result["results"][1]["serial"] == 67890
    assert result["results"][1]["success"] is True
    assert result["results"][1]["name"] == "reporting"
    assert result["results"][1]["state"] == "RUNNING"
    assert result["results"][1]["error"] is None
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 2
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Started 2 PQ(s)"



@pytest.mark.asyncio
async def test_pq_start_different_systems_error():
    """Test error when trying to start PQs from different systems."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(
        context, pq_id=["enterprise:system1:12345", "enterprise:system2:67890"]
    )

    # Verify error
    assert result["success"] is False
    assert "All pq_ids must be from the same system" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_start_empty_list():
    """Test error when trying to start with empty pq_id list."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_start(context, pq_id=[])

    # Verify error
    assert result["success"] is False
    assert "At least one pq_id must be provided" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_stop_success():
    """Test successful PQ stop using pq_id with default timeout."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods - now uses stop_query
    mock_controller.stop_query = AsyncMock()
    mock_pq_info = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_controller.get = AsyncMock(return_value=mock_pq_info)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(context, pq_id="enterprise:test-system:12345")

    # Verify success - new results structure
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 1
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "STOPPED"
    assert result["results"][0]["error"] is None
    assert result["summary"]["total"] == 1
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Stopped 1 PQ(s)"
    mock_controller.stop_query.assert_called_once_with([12345], 30)



@pytest.mark.asyncio
async def test_pq_stop_success_custom_timeout():
    """Test successful PQ stop using pq_id with custom timeout."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods - now uses stop_query
    mock_controller.stop_query = AsyncMock()
    mock_pq_info = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_controller.get = AsyncMock(return_value=mock_pq_info)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(
        context, pq_id="enterprise:test-system:12345", timeout_seconds=60
    )

    # Verify success - new results structure
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 1
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "STOPPED"
    assert result["results"][0]["error"] is None
    assert result["summary"]["total"] == 1
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Stopped 1 PQ(s)"
    mock_controller.stop_query.assert_called_once_with([12345], 60)



@pytest.mark.asyncio
async def test_pq_stop_empty_list():
    """Test pq_stop with empty pq_id list."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(context, pq_id=[])

    assert result["success"] is False
    assert "At least one pq_id must be provided" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_stop_invalid_pq_id_in_list():
    """Test pq_stop with invalid pq_id in list."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(
        context, pq_id=["enterprise:prod:12345", "invalid:format"]
    )

    assert result["success"] is False
    assert "Invalid pq_id format" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_stop_system_not_found():
    """Test pq_stop when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(context, pq_id="enterprise:nonexistent:12345")

    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_stop_exception():
    """Test pq_stop when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(context, pq_id="enterprise:prod:12345")

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_restart_success():
    """Test successful PQ restart using pq_id."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods (no more get_serial_for_name)
    mock_controller.restart_query = AsyncMock()
    mock_pq_info = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_controller.get = AsyncMock(return_value=mock_pq_info)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(context, pq_id="enterprise:test-system:12345")

    # Verify success - new results structure
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 1
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "RUNNING"
    assert result["results"][0]["error"] is None
    assert result["summary"]["total"] == 1
    assert result["summary"]["succeeded"] == 1
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Restarted 1 PQ(s)"
    mock_controller.restart_query.assert_called_once_with([12345], 30)



@pytest.mark.asyncio
async def test_pq_restart_empty_list():
    """Test pq_restart with empty pq_id list."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(context, pq_id=[])

    assert result["success"] is False
    assert "At least one pq_id must be provided" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_restart_invalid_pq_id_in_list():
    """Test pq_restart with invalid pq_id in list."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(
        context, pq_id=["enterprise:prod:12345", "invalid:format"]
    )

    assert result["success"] is False
    assert "Invalid pq_id format" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_restart_system_not_found():
    """Test pq_restart when enterprise system not found."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    full_config = {"enterprise": {"systems": {}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(context, pq_id="enterprise:nonexistent:12345")

    assert "error" in result
    assert "not found" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_restart_exception():
    """Test pq_restart when exception occurs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    mock_config_manager.get_config = AsyncMock(side_effect=RuntimeError("Config error"))

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(context, pq_id="enterprise:prod:12345")

    assert result["success"] is False
    assert "error" in result
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_stop_multiple():
    """Test successful stop of multiple PQs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_controller.stop_query = AsyncMock()
    mock_pq_info1 = create_mock_pq_info(12345, "analytics", "STOPPED", 8.0)
    mock_pq_info2 = create_mock_pq_info(67890, "reporting", "STOPPED", 16.0)
    mock_controller.get = AsyncMock(side_effect=[mock_pq_info1, mock_pq_info2])

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    # Verify success
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 2
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "STOPPED"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["pq_id"] == "enterprise:test-system:67890"
    assert result["results"][1]["serial"] == 67890
    assert result["results"][1]["success"] is True
    assert result["results"][1]["name"] == "reporting"
    assert result["results"][1]["state"] == "STOPPED"
    assert result["results"][1]["error"] is None
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 2
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Stopped 2 PQ(s)"
    # Best-effort calls stop_query for each PQ individually
    assert mock_controller.stop_query.call_count == 2



@pytest.mark.asyncio
async def test_pq_restart_multiple():
    """Test successful restart of multiple PQs."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock controller methods
    mock_controller.restart_query = AsyncMock()
    mock_pq_info1 = create_mock_pq_info(12345, "analytics", "RUNNING", 8.0)
    mock_pq_info2 = create_mock_pq_info(67890, "reporting", "RUNNING", 16.0)
    mock_controller.get = AsyncMock(side_effect=[mock_pq_info1, mock_pq_info2])

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(
        context,
        pq_id=["enterprise:test-system:12345", "enterprise:test-system:67890"],
    )

    # Verify success
    assert result["success"] is True
    assert "results" in result
    assert len(result["results"]) == 2
    assert result["results"][0]["pq_id"] == "enterprise:test-system:12345"
    assert result["results"][0]["serial"] == 12345
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "analytics"
    assert result["results"][0]["state"] == "RUNNING"
    assert result["results"][0]["error"] is None
    assert result["results"][1]["pq_id"] == "enterprise:test-system:67890"
    assert result["results"][1]["serial"] == 67890
    assert result["results"][1]["success"] is True
    assert result["results"][1]["name"] == "reporting"
    assert result["results"][1]["state"] == "RUNNING"
    assert result["results"][1]["error"] is None
    assert result["summary"]["total"] == 2
    assert result["summary"]["succeeded"] == 2
    assert result["summary"]["failed"] == 0
    assert result["message"] == "Restarted 2 PQ(s)"
    # Best-effort calls restart_query for each PQ individually
    assert mock_controller.restart_query.call_count == 2



@pytest.mark.asyncio
async def test_pq_stop_different_systems_error():
    """Test error when trying to stop PQs from different systems."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(
        context, pq_id=["enterprise:system1:12345", "enterprise:system2:67890"]
    )

    # Verify error
    assert result["success"] is False
    assert "All pq_ids must be from the same system" in result["error"]
    assert result["isError"] is True



@pytest.mark.asyncio
async def test_pq_restart_different_systems_error():
    """Test error when trying to restart PQs from different systems."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_restart(
        context, pq_id=["enterprise:system1:12345", "enterprise:system2:67890"]
    )

    # Verify error
    assert result["success"] is False
    assert "All pq_ids must be from the same system" in result["error"]
    assert result["isError"] is True



def test_parse_pq_id_invalid_format():
    """Test _parse_pq_id with invalid format (not enough parts)."""
    with pytest.raises(ValueError, match="Invalid pq_id format"):
        _parse_pq_id("invalid:format")



def test_parse_pq_id_invalid_prefix():
    """Test _parse_pq_id with non-enterprise prefix."""
    with pytest.raises(ValueError, match="Invalid pq_id format"):
        _parse_pq_id("community:system:12345")



def test_parse_pq_id_invalid_serial():
    """Test _parse_pq_id with non-integer serial."""
    with pytest.raises(ValueError, match="Serial must be an integer"):
        _parse_pq_id("enterprise:system:not_a_number")



def test_parse_pq_id_success():
    """Test successful _parse_pq_id."""
    system_name, serial = _parse_pq_id("enterprise:prod:12345")
    assert system_name == "prod"
    assert serial == 12345



def test_validate_timeout_excessive(caplog):
    """Test _validate_timeout with timeout exceeding safe limit."""
    import logging

    caplog.set_level(logging.WARNING)
    result = _validate_timeout(400, "test_function")
    assert result == 400
    assert "exceeds recommended MCP limit" in caplog.text



def test_validate_timeout_normal():
    """Test _validate_timeout with normal timeout."""
    result = _validate_timeout(30, "test_function")
    assert result == 30



def test_validate_timeout_zero():
    """Test _validate_timeout with timeout=0 (fire-and-forget)."""
    result = _validate_timeout(0, "test_function")
    assert result == 0



def test_validate_timeout_negative():
    """Test _validate_timeout with negative timeout raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        _validate_timeout(-1, "test_function")

    assert "timeout_seconds must be non-negative" in str(exc_info.value)
    assert "got -1" in str(exc_info.value)
    assert "Use timeout_seconds=0 for fire-and-forget" in str(exc_info.value)



def test_validate_max_concurrent_zero():
    """Test _validate_max_concurrent with zero raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        _validate_max_concurrent(0, "test_function")

    assert "max_concurrent must be at least 1" in str(exc_info.value)
    assert "got 0" in str(exc_info.value)



def test_validate_max_concurrent_negative():
    """Test _validate_max_concurrent with negative value raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        _validate_max_concurrent(-5, "test_function")

    assert "max_concurrent must be at least 1" in str(exc_info.value)
    assert "got -5" in str(exc_info.value)



def test_validate_max_concurrent_valid():
    """Test _validate_max_concurrent with valid values returns the value."""
    assert _validate_max_concurrent(1, "test_function") == 1
    assert _validate_max_concurrent(20, "test_function") == 20
    assert _validate_max_concurrent(100, "test_function") == 100



@pytest.mark.asyncio
async def test_pq_delete_parallel_execution_with_semaphore():
    """Test that pq_delete executes operations in parallel with semaphore limiting."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Track execution order and timing
    execution_log = []

    async def mock_delete_with_delay(serial):
        execution_log.append(f"delete_start_{serial}")
        await asyncio.sleep(0.01)  # Simulate work
        execution_log.append(f"delete_end_{serial}")

    async def mock_get_with_delay(serial, timeout_seconds=0):
        execution_log.append(f"get_{serial}")
        await asyncio.sleep(0.001)  # Small delay
        return create_mock_pq_info(serial, f"pq_{serial}", "STOPPED")

    mock_controller.delete_query = AsyncMock(side_effect=mock_delete_with_delay)
    mock_controller.get = AsyncMock(side_effect=mock_get_with_delay)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Test with 3 PQs and max_concurrent=2
    result = await pq_delete(
        context,
        pq_id=[
            "enterprise:test-system:1",
            "enterprise:test-system:2",
            "enterprise:test-system:3",
        ],
        max_concurrent=2,
    )

    # Verify success
    assert result["success"] is True
    assert len(result["results"]) == 3
    assert all(r["success"] for r in result["results"])

    # Verify operations executed (all 3 should have been processed)
    assert mock_controller.delete_query.call_count == 3

    # Verify parallel execution occurred - with max_concurrent=2,
    # we should see overlapping execution (not strictly sequential)
    # The first two should start before the third one
    assert "delete_start_1" in execution_log
    assert "delete_start_2" in execution_log

    # Due to semaphore limit of 2, the third operation should start
    # only after one of the first two finishes
    delete_start_3_index = execution_log.index("delete_start_3")
    # At least one of the first two should have ended before the third starts
    ends_before_third = [
        i
        for i, log in enumerate(execution_log)
        if (log == "delete_end_1" or log == "delete_end_2") and i < delete_start_3_index
    ]
    assert len(ends_before_third) > 0, "Semaphore should limit concurrency to 2"



@pytest.mark.asyncio
async def test_pq_delete_handles_unexpected_exception():
    """Test that pq_delete handles unexpected exceptions with return_exceptions=True."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # First PQ succeeds, second raises unexpected exception, third succeeds
    mock_pq_info1 = create_mock_pq_info(1, "pq1", "STOPPED")
    mock_pq_info3 = create_mock_pq_info(3, "pq3", "STOPPED")

    get_call_count = 0

    async def mock_get_side_effect(serial, timeout_seconds=0):
        nonlocal get_call_count
        get_call_count += 1
        if get_call_count == 1:
            return mock_pq_info1
        elif get_call_count == 2:
            # Simulate unexpected exception during get
            raise RuntimeError("Unexpected database error")
        else:
            return mock_pq_info3

    mock_controller.get = AsyncMock(side_effect=mock_get_side_effect)
    mock_controller.delete_query = AsyncMock()

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_delete(
        context,
        pq_id=[
            "enterprise:test-system:1",
            "enterprise:test-system:2",
            "enterprise:test-system:3",
        ],
    )

    # Should still succeed overall (best-effort)
    assert result["success"] is True
    assert len(result["results"]) == 3

    # First PQ should succeed
    assert result["results"][0]["success"] is True
    assert result["results"][0]["name"] == "pq1"

    # Second PQ should have error from the exception
    assert result["results"][1]["success"] is False
    assert "RuntimeError" in result["results"][1]["error"]
    assert "Unexpected database error" in result["results"][1]["error"]

    # Third PQ should succeed
    assert result["results"][2]["success"] is True
    assert result["results"][2]["name"] == "pq3"

    # Summary should reflect 2 successes and 1 failure
    assert result["summary"]["succeeded"] == 2
    assert result["summary"]["failed"] == 1



@pytest.mark.asyncio
async def test_pq_start_parallel_execution():
    """Test that pq_start executes operations in parallel."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Track concurrent execution
    active_operations = []
    max_concurrent_observed = 0

    async def mock_start_and_wait(serial, timeout):
        active_operations.append(serial)
        nonlocal max_concurrent_observed
        max_concurrent_observed = max(max_concurrent_observed, len(active_operations))
        await asyncio.sleep(0.01)
        active_operations.remove(serial)

    mock_controller.start_and_wait = AsyncMock(side_effect=mock_start_and_wait)
    mock_controller.get = AsyncMock(
        side_effect=lambda s, timeout_seconds=0: create_mock_pq_info(
            s, f"pq_{s}", "RUNNING"
        )
    )

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Test with 5 PQs and default max_concurrent (20)
    result = await pq_start(
        context,
        pq_id=[f"enterprise:test-system:{i}" for i in range(1, 6)],
    )

    # Verify success
    assert result["success"] is True
    assert len(result["results"]) == 5
    assert all(r["success"] for r in result["results"])

    # Verify parallel execution - with 5 operations and limit of 20,
    # we should see multiple operations running concurrently
    assert max_concurrent_observed > 1, "Operations should run in parallel"



@pytest.mark.asyncio
async def test_pq_stop_parallel_with_mixed_results():
    """Test pq_stop parallel execution with mixed success/failure."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # First and third succeed, second fails
    async def mock_stop_side_effect(serials, timeout):
        # serials is a list with one element
        serial = serials[0]
        if serial == 2:
            raise TimeoutError("PQ did not stop in time")

    async def mock_get_side_effect(serial, timeout_seconds=0):
        # This is only called after successful stops
        return create_mock_pq_info(serial, f"pq_{serial}", "STOPPED")

    mock_controller.stop_query = AsyncMock(side_effect=mock_stop_side_effect)
    mock_controller.get = AsyncMock(side_effect=mock_get_side_effect)

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_stop(
        context,
        pq_id=[
            "enterprise:test-system:1",
            "enterprise:test-system:2",
            "enterprise:test-system:3",
        ],
    )

    # Should succeed overall (best-effort)
    assert result["success"] is True
    assert len(result["results"]) == 3

    # Check individual results
    assert result["results"][0]["success"] is True
    assert result["results"][1]["success"] is False
    assert "TimeoutError" in result["results"][1]["error"]
    assert result["results"][2]["success"] is True

    # Summary
    assert result["summary"]["succeeded"] == 2
    assert result["summary"]["failed"] == 1



@pytest.mark.asyncio
async def test_pq_restart_parallel_execution():
    """Test pq_restart parallel execution."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    # Setup mock chain
    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    # Mock successful restarts
    mock_controller.restart_query = AsyncMock()
    mock_controller.get = AsyncMock(
        side_effect=lambda s, timeout_seconds=0: create_mock_pq_info(
            s, f"pq_{s}", "RUNNING"
        )
    )

    # Mock config
    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Test with 4 PQs
    result = await pq_restart(
        context,
        pq_id=[f"enterprise:test-system:{i}" for i in range(1, 5)],
        max_concurrent=2,
    )

    # Verify success
    assert result["success"] is True
    assert len(result["results"]) == 4
    assert all(r["success"] for r in result["results"])
    assert result["summary"]["succeeded"] == 4
    assert result["summary"]["failed"] == 0

    # Verify all restart_query calls were made
    assert mock_controller.restart_query.call_count == 4



@pytest.mark.asyncio
async def test_pq_delete_exception_escapes_to_gather(monkeypatch):
    """Test pq_delete handles raw Exception objects from asyncio.gather."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_controller.delete_query = AsyncMock()
    mock_controller.get = AsyncMock(
        side_effect=lambda s, timeout_seconds=0: create_mock_pq_info(
            s, f"pq_{s}", "STOPPED"
        )
    )

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Monkeypatch asyncio.gather to inject a raw exception into the results
    original_gather = asyncio.gather

    async def patched_gather(*args, **kwargs):
        results = await original_gather(*args, **kwargs)
        # Replace second result with a raw Exception to trigger isinstance branch
        results_list = list(results)
        if len(results_list) > 1:
            results_list[1] = RuntimeError("Injected exception for testing")
        return results_list

    monkeypatch.setattr(asyncio, "gather", patched_gather)

    result = await pq_delete(
        context,
        pq_id=[
            "enterprise:test-system:1",
            "enterprise:test-system:2",
            "enterprise:test-system:3",
        ],
    )

    assert result["success"] is True
    assert len(result["results"]) == 3
    assert result["results"][0]["success"] is True
    assert result["results"][1]["success"] is False
    assert "Unexpected error" in result["results"][1]["error"]
    assert "RuntimeError" in result["results"][1]["error"]
    assert result["results"][2]["success"] is True



@pytest.mark.asyncio
async def test_pq_start_exception_escapes_to_gather(monkeypatch):
    """Test pq_start handles raw Exception objects from asyncio.gather."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_controller.start_and_wait = AsyncMock()
    mock_controller.get = AsyncMock(
        side_effect=lambda s, timeout_seconds=0: create_mock_pq_info(
            s, f"pq_{s}", "RUNNING"
        )
    )

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Monkeypatch asyncio.gather to inject a raw exception
    original_gather = asyncio.gather

    async def patched_gather(*args, **kwargs):
        results = await original_gather(*args, **kwargs)
        results_list = list(results)
        if len(results_list) > 1:
            results_list[1] = ValueError("Injected exception for testing")
        return results_list

    monkeypatch.setattr(asyncio, "gather", patched_gather)

    result = await pq_start(
        context,
        pq_id=[
            "enterprise:test-system:1",
            "enterprise:test-system:2",
            "enterprise:test-system:3",
        ],
    )

    assert result["success"] is True
    assert len(result["results"]) == 3
    assert result["results"][1]["success"] is False
    assert "Unexpected error" in result["results"][1]["error"]
    assert "ValueError" in result["results"][1]["error"]



@pytest.mark.asyncio
async def test_pq_stop_exception_escapes_to_gather(monkeypatch):
    """Test pq_stop handles raw Exception objects from asyncio.gather."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_controller.stop_query = AsyncMock()
    mock_controller.get = AsyncMock(
        side_effect=lambda s, timeout_seconds=0: create_mock_pq_info(
            s, f"pq_{s}", "STOPPED"
        )
    )

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Monkeypatch asyncio.gather to inject a raw exception
    original_gather = asyncio.gather

    async def patched_gather(*args, **kwargs):
        results = await original_gather(*args, **kwargs)
        results_list = list(results)
        if len(results_list) > 1:
            results_list[1] = TypeError("Injected exception for testing")
        return results_list

    monkeypatch.setattr(asyncio, "gather", patched_gather)

    result = await pq_stop(
        context,
        pq_id=[
            "enterprise:test-system:1",
            "enterprise:test-system:2",
            "enterprise:test-system:3",
        ],
    )

    assert result["success"] is True
    assert len(result["results"]) == 3
    assert result["results"][1]["success"] is False
    assert "Unexpected error" in result["results"][1]["error"]
    assert "TypeError" in result["results"][1]["error"]



@pytest.mark.asyncio
async def test_pq_restart_exception_escapes_to_gather(monkeypatch):
    """Test pq_restart handles raw Exception objects from asyncio.gather."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()
    mock_enterprise_registry = MagicMock()
    mock_factory_manager = MagicMock()
    mock_factory = MagicMock()
    mock_controller = MagicMock()

    mock_session_registry.enterprise_registry = AsyncMock(
        return_value=mock_enterprise_registry
    )
    mock_enterprise_registry.get = AsyncMock(return_value=mock_factory_manager)
    mock_factory_manager.get = AsyncMock(return_value=mock_factory)
    mock_factory.controller_client = mock_controller

    mock_controller.restart_query = AsyncMock()
    mock_controller.get = AsyncMock(
        side_effect=lambda s, timeout_seconds=0: create_mock_pq_info(
            s, f"pq_{s}", "RUNNING"
        )
    )

    full_config = {"enterprise": {"systems": {"test-system": {}}}}
    mock_config_manager.get_config = AsyncMock(return_value=full_config)

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    # Monkeypatch asyncio.gather to inject a raw exception
    original_gather = asyncio.gather

    async def patched_gather(*args, **kwargs):
        results = await original_gather(*args, **kwargs)
        results_list = list(results)
        if len(results_list) > 1:
            results_list[1] = OSError("Injected exception for testing")
        return results_list

    monkeypatch.setattr(asyncio, "gather", patched_gather)

    result = await pq_restart(
        context,
        pq_id=[
            "enterprise:test-system:1",
            "enterprise:test-system:2",
            "enterprise:test-system:3",
        ],
    )

    assert result["success"] is True
    assert len(result["results"]) == 3
    assert result["results"][1]["success"] is False
    assert "Unexpected error" in result["results"][1]["error"]
    assert "OSError" in result["results"][1]["error"]



@pytest.mark.asyncio
async def test_pq_create_script_body_and_path_mutually_exclusive():
    """Test pq_create rejects both script_body and script_path being specified."""
    mock_config_manager = MagicMock()
    mock_session_registry = MagicMock()

    context = MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": mock_session_registry,
        }
    )

    result = await pq_create(
        context,
        system_name="test-system",
        pq_name="test-pq",
        heap_size_gb=8.0,
        script_body="print('hello')",
        script_path="/path/to/script.py",
    )

    assert result["success"] is False
    assert "mutually exclusive" in result["error"]
    assert result["isError"] is True
