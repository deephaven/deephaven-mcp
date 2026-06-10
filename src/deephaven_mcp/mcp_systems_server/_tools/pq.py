"""Persistent Query (PQ) MCP Tools - Enterprise Core+ PQ Management.

Provides MCP tools for managing Deephaven Enterprise (Core+) persistent queries:
- pq_name_to_id: Convert PQ names to IDs
- pq_list: List all persistent queries
- pq_details: Get detailed information about specific PQs
- pq_create: Create new persistent queries
- pq_delete: Delete persistent queries
- pq_modify: Modify existing PQ configurations
- pq_start: Start stopped persistent queries
- pq_stop: Stop running persistent queries
- pq_restart: Restart persistent queries

These tools require Deephaven Enterprise (Core+) and are not available in Community.
"""

import asyncio
import logging
from typing import TYPE_CHECKING, Annotated, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from deephaven_mcp._exceptions import (
    InternalError,
    InvalidSessionNameError,
)
from deephaven_mcp.client import (
    PQ_STATES,
    CorePlusControllerClient,
    CorePlusQueryConfig,
    CorePlusQuerySerial,
    CorePlusQueryState,
    CorePlusQueryStatus,
)
from deephaven_mcp.mcp_systems_server._tools.shared import (
    get_enterprise_registry,
    get_enterprise_settings,
    parse_pq_id,
    redact_json_sensitive_fields,
    resolve_pq_ids_to_single_system,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    SystemType,
)

if TYPE_CHECKING:
    from deephaven.proto.table_pb2 import (
        ColumnDefinitionMessage,
        ExportedObjectInfoMessage,
        TableDefinitionMessage,
    )
    from deephaven_enterprise.proto.common_pb2 import (
        ExceptionDetailsMessage,
    )
    from deephaven_enterprise.proto.controller_common_pb2 import (
        NamedStringList,
    )
    from deephaven_enterprise.proto.persistent_query_pb2 import (
        ExportedObjectTypeEnum,
        ProcessorConnectionDetailsMessage,
        RestartUsersEnum,
        WorkerProtocolMessage,
    )

# Runtime sentinels for the optional protobuf enums. ``ExportedObjectTypeEnum``
# and ``RestartUsersEnum`` are also imported under TYPE_CHECKING above so that
# annotations like ``RestartUsersEnum.ValueType`` resolve for mypy/pyright.
try:
    from deephaven_enterprise.proto.persistent_query_pb2 import (
        ExportedObjectTypeEnum,
        RestartUsersEnum,
    )
except (
    ImportError
):  # pragma: no cover - only reached when the enterprise package is absent
    ExportedObjectTypeEnum = None  # type: ignore[misc,assignment]
    RestartUsersEnum = None  # type: ignore[misc,assignment]


_LOGGER = logging.getLogger(__name__)

# Matches deephaven.constants.NULL_LONG / Java Long.MIN_VALUE. Defined locally because
# deephaven.constants requires a live JVM which this server never starts.
_NULL_LONG = -9223372036854775808


# =============================================================================
# Persistent Query (PQ) Management Tools
# =============================================================================


def _parse_pq_id(pq_id: str) -> tuple[str, CorePlusQuerySerial]:
    """Parse a pq_id and return its (system_name, serial) tuple.

    The multiplexed systems server hosts multiple enterprise systems
    in a single process, so ``pq_id`` carries the system name as its
    first segment. The previous ``enterprise:`` prefix has been
    dropped because the type segment is redundant for an identifier
    that is always enterprise-scoped.

    Args:
        pq_id (str): PQ identifier in format ``'<system_name>:<serial>'``.

    Returns:
        tuple[str, CorePlusQuerySerial]: ``(system_name, serial)``.

    Raises:
        ValueError: If ``pq_id`` is not exactly two non-empty
            colon-separated segments or if the serial is not a
            non-negative integer. Raised as ``ValueError`` for
            backwards compatibility with existing ``except ValueError``
            sites in this module; callers should re-wrap into the
            tool error response.
    """
    try:
        system_name, serial_int = parse_pq_id(pq_id)
    except InvalidSessionNameError as exc:
        raise ValueError(str(exc)) from None
    return system_name, CorePlusQuerySerial(serial_int)


def _make_pq_id(serial: CorePlusQuerySerial, system_name: str) -> str:
    """Construct a pq_id from serial number.

    Args:
        serial (CorePlusQuerySerial): PQ serial number.
        system_name (str): Enterprise system name.

    Returns:
        str: PQ identifier in format ``'<system_name>:<serial>'``.
    """
    return f"{system_name}:{serial}"


def _validate_max_concurrent(max_concurrent: int, function_name: str) -> int:
    """Validate max_concurrent is valid for parallel operations.

    Args:
        max_concurrent (int): Maximum number of concurrent operations (must be >= 1).
        function_name (str): Name of calling function, included in the error message for context.

    Returns:
        int: The validated max_concurrent value

    Raises:
        ValueError: If max_concurrent is less than 1
    """
    if max_concurrent < 1:
        raise ValueError(
            f"[{function_name}] max_concurrent must be at least 1, got {max_concurrent}. "
            f"Use a positive integer to control parallelism (e.g., 20 for moderate concurrency)."
        )
    return max_concurrent


def _format_pq_config(config: CorePlusQueryConfig) -> dict[str, object]:
    """Format PersistentQueryConfigMessage into MCP-compatible dictionary.

    Extracts ALL 38 fields from PersistentQueryConfigMessage protobuf and formats them
    for MCP API responses. Applies consistent field naming (snake_case) and converts
    empty/zero values to None for optional fields to produce cleaner JSON.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.persistent_query.PersistentQueryConfigMessage

    Field transformations:
    - Empty strings → None for optional string fields
    - Zero values → None for optional timestamp fields
    - Repeated fields → Python lists
    - Enum values → Stringified
    - camelCase protobuf names → snake_case API names
    - ``script_code`` and ``script_path`` are a protobuf ``oneof`` (``scriptData``); at most
      one will be non-None at any time

    Args:
        config (CorePlusQueryConfig): Wrapper around PersistentQueryConfigMessage protobuf

    Returns:
        dict[str, object]: All 38 config fields formatted for MCP API, with optional fields
            converted to None when empty
    """
    pb = config.pb

    # Get restartUsers enum name using protobuf enum class Name() method
    # Handle unknown enum values (server may have newer proto than client)
    restart_users = pb.restartUsers
    if RestartUsersEnum is not None:
        try:
            restart_users_str = RestartUsersEnum.Name(restart_users)
        except ValueError:
            restart_users_str = f"UNKNOWN_RESTART_USERS_{restart_users}"
    else:
        restart_users_str = str(restart_users)

    return {
        "serial": pb.serial,
        "version": pb.version,
        "name": pb.name,
        "owner": pb.owner,
        "enabled": pb.enabled,
        "heap_size_gb": pb.heapSizeGb,
        "buffer_pool_to_heap_ratio": pb.bufferPoolToHeapRatio,
        "detailed_gc_logging_enabled": pb.detailedGCLoggingEnabled,
        "extra_jvm_arguments": list(pb.extraJvmArguments),
        "extra_environment_variables": list(pb.extraEnvironmentVariables),
        "class_path_additions": list(pb.classPathAdditions),
        "server_name": pb.serverName if pb.serverName else None,
        "admin_groups": list(pb.adminGroups),
        "viewer_groups": list(pb.viewerGroups),
        "restart_users": restart_users_str,
        "script_code": pb.scriptCode if pb.scriptCode else None,
        "script_path": pb.scriptPath if pb.scriptPath else None,
        "script_language": pb.scriptLanguage,
        "configuration_type": pb.configurationType,
        "type_specific_fields_json": redact_json_sensitive_fields(
            pb.typeSpecificFieldsJson
        ),
        "scheduling": list(pb.scheduling),
        "timeout_nanos": pb.timeoutNanos if pb.timeoutNanos else None,
        "jvm_profile": pb.jvmProfile if pb.jvmProfile else None,
        "last_modified_by_authenticated": (
            pb.lastModifiedByAuthenticated if pb.lastModifiedByAuthenticated else None
        ),
        "last_modified_by_effective": (
            pb.lastModifiedByEffective if pb.lastModifiedByEffective else None
        ),
        "last_modified_time_nanos": (
            pb.lastModifiedTimeNanos if pb.lastModifiedTimeNanos else None
        ),
        "completed_status": pb.completedStatus if pb.completedStatus else None,
        "expiration_time_nanos": (
            pb.expirationTimeNanos if pb.expirationTimeNanos else None
        ),
        "kubernetes_control": pb.kubernetesControl if pb.kubernetesControl else None,
        "worker_kind": pb.workerKind,
        "created_time_nanos": (
            pb.createdTimeNanos
            if (pb.createdTimeNanos and pb.createdTimeNanos != _NULL_LONG)
            else None
        ),
        "replica_count": pb.replicaCount,
        "spare_count": pb.spareCount,
        "assignment_policy": pb.assignmentPolicy if pb.assignmentPolicy else None,
        "assignment_policy_params": (
            pb.assignmentPolicyParams if pb.assignmentPolicyParams else None
        ),
        "additional_memory_gb": pb.additionalMemoryGb,
        "python_control": pb.pythonControl if pb.pythonControl else None,
        "generic_worker_control": (
            pb.genericWorkerControl if pb.genericWorkerControl else None
        ),
    }


def _format_named_string_list(nsl: "NamedStringList") -> dict[str, object]:
    """Format NamedStringList protobuf into dict.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.controller_common.NamedStringList

    NamedStringList fields (2 total):
    - name (string)
    - values (repeated string)

    Args:
        nsl (NamedStringList): NamedStringList protobuf object

    Returns:
        dict[str, object]: Formatted named string list with snake_case keys
    """
    return {
        "name": nsl.name,
        "values": list(nsl.values),
    }


def _format_column_definition(col: "ColumnDefinitionMessage") -> dict[str, object]:
    """Format ColumnDefinitionMessage protobuf into dict.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.common.ColumnDefinitionMessage

    ColumnDefinitionMessage fields (9 total):
    - name (string)
    - dataType (string)
    - componentType (string)
    - columnType (ColumnTypeEnum)
    - isVarSizeString (bool)
    - encoding (EncodingTypeEnum)
    - codec (string)
    - codecArgs (string)
    - objectWidthBytes (int32)

    Args:
        col (ColumnDefinitionMessage): ColumnDefinitionMessage protobuf object

    Returns:
        dict[str, object]: Formatted column definition with snake_case keys
    """
    return {
        "name": col.name,
        "data_type": col.dataType or None,
        "component_type": col.componentType or None,
        "column_type": col.columnType if col.columnType else None,
        "is_var_size_string": col.isVarSizeString,
        "encoding": col.encoding if col.encoding else None,
        "codec": col.codec or None,
        "codec_args": col.codecArgs or None,
        "object_width_bytes": col.objectWidthBytes if col.objectWidthBytes else None,
    }


def _format_table_definition(td: "TableDefinitionMessage") -> dict[str, object]:
    """Format TableDefinitionMessage protobuf into dict.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.common.TableDefinitionMessage

    TableDefinitionMessage fields (4 total):
    - namespace (string, optional)
    - tableName (string, optional)
    - columns (repeated ColumnDefinitionMessage)
    - storageType (StorageTypeEnum, optional)

    Args:
        td (TableDefinitionMessage): TableDefinitionMessage protobuf object

    Returns:
        dict[str, object]: Formatted table definition with snake_case keys
    """
    columns = [_format_column_definition(col) for col in td.columns]
    return {
        "namespace": td.namespace or None,
        "table_name": td.tableName or None,
        "columns": columns,
        "storage_type": td.storageType if td.storageType else None,
    }


def _format_exported_object_info(obj: "ExportedObjectInfoMessage") -> dict[str, object]:
    """Format ExportedObjectInfoMessage protobuf into dict.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.persistent_query.ExportedObjectInfoMessage

    ExportedObjectInfoMessage fields (4 total):
    - name (string)
    - type (ExportedObjectTypeEnum)
    - tableDefinition (TableDefinitionMessage)
    - originalType (string)

    Args:
        obj (ExportedObjectInfoMessage): ExportedObjectInfoMessage protobuf object

    Returns:
        dict[str, object]: Formatted exported object info with snake_case keys
    """
    # Get enum name using protobuf enum class Name() method
    # Handle unknown enum values (server may have newer proto than client)
    obj_type = obj.type
    if obj_type is not None and ExportedObjectTypeEnum is not None:
        try:
            obj_type = ExportedObjectTypeEnum.Name(obj_type)
        except ValueError:
            obj_type = f"UNKNOWN_EXPORTED_TYPE_{obj_type}"

    # Format tableDefinition if present
    table_def = (
        _format_table_definition(obj.tableDefinition) if obj.tableDefinition else None
    )

    return {
        "name": obj.name,
        "type": obj_type,
        "table_definition": table_def,
        "original_type": obj.originalType or None,
    }


def _format_worker_protocol(wp: "WorkerProtocolMessage") -> dict[str, object]:
    """Format WorkerProtocolMessage protobuf into dict.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.persistent_query.WorkerProtocolMessage

    WorkerProtocolMessage fields (2 total):
    - name (string)
    - port (int32)

    Args:
        wp (WorkerProtocolMessage): WorkerProtocolMessage protobuf object

    Returns:
        dict[str, object]: Formatted worker protocol with snake_case keys
    """
    return {
        "name": wp.name,
        "port": wp.port,
    }


def _format_connection_details(
    cd: "ProcessorConnectionDetailsMessage",
) -> dict[str, object]:
    """Format ProcessorConnectionDetailsMessage protobuf into dict.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.persistent_query.ProcessorConnectionDetailsMessage

    ProcessorConnectionDetailsMessage fields (8 total):
    - protocols (repeated WorkerProtocolMessage)
    - workerName (string)
    - processInfoId (string)
    - processorHost (string)
    - envoyPrefix (string)
    - grpcUrl (string)
    - staticUrl (string)
    - enterpriseWebSocketUrl (string)

    Args:
        cd (ProcessorConnectionDetailsMessage): ProcessorConnectionDetailsMessage protobuf object

    Returns:
        dict[str, object]: Formatted connection details with snake_case keys
    """
    protocols = [_format_worker_protocol(p) for p in cd.protocols]
    return {
        "protocols": protocols,
        "worker_name": cd.workerName or None,
        "process_info_id": cd.processInfoId or None,
        "processor_host": cd.processorHost or None,
        "envoy_prefix": cd.envoyPrefix or None,
        "grpc_url": cd.grpcUrl or None,
        "static_url": cd.staticUrl or None,
        "enterprise_web_socket_url": cd.enterpriseWebSocketUrl or None,
    }


def _format_exception_details(ed: "ExceptionDetailsMessage") -> dict[str, object]:
    """Format ExceptionDetailsMessage protobuf into dict.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.common.ExceptionDetailsMessage

    ExceptionDetailsMessage fields (3 total):
    - errorMessage (string)
    - stackTrace (string)
    - shortCauses (string)

    Args:
        ed (ExceptionDetailsMessage): ExceptionDetailsMessage protobuf object

    Returns:
        dict[str, object]: Formatted exception details with snake_case keys
    """
    return {
        "error_message": ed.errorMessage or None,
        "stack_trace": ed.stackTrace or None,
        "short_causes": ed.shortCauses or None,
    }


def _format_pq_state(state: CorePlusQueryState | None) -> dict[str, object] | None:
    """Format PersistentQueryStateMessage into MCP-compatible dictionary.

    Extracts ALL 25 fields from PersistentQueryStateMessage protobuf and formats them
    for MCP API responses. Applies consistent field naming (snake_case) and converts
    empty/zero values to None for optional fields to produce cleaner JSON. Returns None
    if state is not available.

    Protobuf reference:
    https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.persistent_query.PersistentQueryStateMessage

    PersistentQueryStateMessage fields (25 total):
    - serial (int64)
    - version (int64)
    - status (PersistentQueryStatusEnum)
    - initializationStartNanos (int64)
    - initializationCompleteNanos (int64)
    - lastUpdateNanos (int64)
    - dispatcherHost (string)
    - tableGroups (repeated NamedStringList)
    - scopeTypes (repeated ExportedObjectInfoMessage)
    - connectionDetails (ProcessorConnectionDetailsMessage, optional)
    - exceptionDetails (ExceptionDetailsMessage, optional)
    - typeSpecificStateJson (string, Encoded JSON)
    - lastAuthenticatedUser (string)
    - lastEffectiveUser (string)
    - scriptLoaderStateJson (string, optional, Encoded JSON)
    - hasProgress (bool)
    - progressValue (int32)
    - progressMessage (string)
    - engineVersion (string)
    - dispatcherPort (int32)
    - shouldStopNanos (int64)
    - numFailures (int32)
    - lastFailureTimeNanos (int64)
    - replicaSlot (int32)
    - statusDetails (string)

    Args:
        state (CorePlusQueryState | None): CorePlusQueryState wrapper around PersistentQueryStateMessage protobuf,
                                          or None if no state available

    Returns:
        dict[str, object] | None: All 25 state fields formatted for MCP API, with optional
            fields converted to None when empty, or None if state not available
    """
    if state is None:
        return None

    pb = state.pb

    # Format tableGroups using helper function
    table_groups = [_format_named_string_list(g) for g in pb.tableGroups]

    # Format scopeTypes using helper function
    scope_types = [_format_exported_object_info(obj) for obj in pb.scopeTypes]

    # Format connectionDetails using helper function
    connection_details = (
        _format_connection_details(pb.connectionDetails)
        if pb.connectionDetails
        else None
    )

    # Format exceptionDetails using helper function
    exception_details = (
        _format_exception_details(pb.exceptionDetails) if pb.exceptionDetails else None
    )

    # Use the wrapper's status property which properly converts enum to name via ControllerClient
    status_str = state.status.name

    result = {
        "serial": pb.serial,
        "version": pb.version,
        "status": status_str,
        "initialization_start_nanos": pb.initializationStartNanos or None,
        "initialization_complete_nanos": pb.initializationCompleteNanos or None,
        "last_update_nanos": pb.lastUpdateNanos or None,
        "dispatcher_host": pb.dispatcherHost or None,
        "table_groups": table_groups,
        "scope_types": scope_types,
        "connection_details": connection_details,
        "exception_details": exception_details,
        "type_specific_state_json": redact_json_sensitive_fields(
            pb.typeSpecificStateJson
        ),
        "last_authenticated_user": pb.lastAuthenticatedUser or None,
        "last_effective_user": pb.lastEffectiveUser or None,
        "script_loader_state_json": pb.scriptLoaderStateJson or None,
        "has_progress": pb.hasProgress,
        "progress_value": pb.progressValue,
        "progress_message": pb.progressMessage or None,
        "engine_version": pb.engineVersion or None,
        "dispatcher_port": pb.dispatcherPort or None,
        "should_stop_nanos": pb.shouldStopNanos or None,
        "num_failures": pb.numFailures,
        "last_failure_time_nanos": pb.lastFailureTimeNanos or None,
        "replica_slot": pb.replicaSlot,
        "status_details": pb.statusDetails or None,
    }

    return result


def _format_pq_states(states: list[CorePlusQueryState]) -> list[dict[str, object]]:
    """Format a list of PersistentQueryStateMessage objects, dropping None entries.

    Used for a PQ's replicas (additional running instances for high availability) and its
    spares (pre-initialized workers ready to take over if the primary fails); both are lists
    of states formatted identically.

    Args:
        states (list[CorePlusQueryState]): CorePlusQueryState wrappers to format. None
            entries are tolerated and dropped from the result.

    Returns:
        list[dict[str, object]]: Formatted state dictionaries (25 fields each) with None
            entries removed; empty list if no states are provided.
    """
    formatted = [_format_pq_state(state) for state in states]
    return [f for f in formatted if f is not None]


async def _setup_batch_pq_operation(
    context: Context,
    pq_id: str | list[str],
    function_name: str,
    max_concurrent: int,
) -> tuple[
    list[tuple[str, CorePlusQuerySerial]] | None,
    CorePlusControllerClient | None,
    int | None,
    str,
    dict[str, object] | None,
]:
    """Set up common infrastructure for batch PQ operations.

    Validates pq_ids and parameters and returns controller client.
    Consolidates validation and setup boilerplate across pq_delete, pq_start, pq_stop, pq_restart.

    Args:
        context (Context): MCP context object
        pq_id (str | list[str]): Single pq_id string or list of pq_id strings
        function_name (str): Name of calling function, used in log messages and error strings
        max_concurrent (int): Maximum concurrent operations (must be >= 1)

    Returns:
        tuple: (parsed_pqs, controller, validated_max_concurrent, system_name, error_response)
               On success: (parsed_list, controller_client, max_concurrent_int, system_name, None)
               On failure: (None, None, None, system_name, {"success": False, "error": "...", "isError": True})

    Example::

        parsed_pqs, controller, max_conc, system_name, error = await _setup_batch_pq_operation(...)
        if error:
            return error
        # Type narrowing: all returned values except error are non-None here
    """
    # Validate max_concurrent before touching the registry so a bad value
    # surfaces a clean error even when the pq_ids are also malformed.
    try:
        validated_max_concurrent = _validate_max_concurrent(
            max_concurrent, function_name
        )
    except ValueError as e:
        return (
            None,
            None,
            None,
            "",
            {"success": False, "error": str(e), "isError": True},
        )

    # Parse pq_ids; system_name is derived from them.
    parsed_pqs, system_name, parse_error = _validate_and_parse_pq_ids(pq_id)
    if parse_error:
        return (
            None,
            None,
            None,
            "",
            {"success": False, "error": parse_error, "isError": True},
        )
    # parse_error is None implies system_name is set
    if system_name is None:
        raise InternalError(
            "Internal invariant violated: _validate_and_parse_pq_ids returned "
            "system_name=None without an error."
        )

    # Resolve registry for the system named in the pq_ids.
    try:
        session_registry = get_enterprise_registry(context, system_name)
    except InvalidSessionNameError as e:
        return (
            None,
            None,
            None,
            system_name,
            {"success": False, "error": str(e), "isError": True},
        )

    # Type narrowing: when parse_error is None, parsed_pqs is guaranteed non-None
    parsed_pqs = cast(list[tuple[str, CorePlusQuerySerial]], parsed_pqs)

    factory_manager = session_registry.factory_manager
    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Connecting to enterprise factory"
    )
    factory = await factory_manager.get()
    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Connected to enterprise factory"
    )

    # Get controller client
    controller = factory.controller_client

    return (
        parsed_pqs,
        controller,
        validated_max_concurrent,
        system_name,
        None,
    )


def _validate_and_parse_pq_ids(
    pq_id: str | list[str],
) -> tuple[
    list[tuple[str, CorePlusQuerySerial]] | None,
    str | None,
    str | None,
]:
    """Validate and parse pq_id(s) for batch operations.

    Thin adapter over
    :func:`deephaven_mcp.mcp_systems_server._tools.shared.resolve_pq_ids_to_single_system`
    that preserves the batch-tool return convention (parsed list,
    system name, optional error string). The shared helper guarantees
    every id parses cleanly and all of them name the same enterprise
    system; mixed-system batches are rejected up front so the caller
    cannot accidentally fan a batch out across systems.

    Args:
        pq_id (str | list[str]): Single pq_id string or list of pq_id
            strings in format ``'<system_name>:<serial>'``.

    Returns:
        tuple[list[tuple[str, CorePlusQuerySerial]] | None, str | None, str | None]:
            ``(parsed_pqs, system_name, error_message)``.

            - On success: ``([(pq_id, serial), ...], system_name, None)``.
            - On failure: ``(None, None, error_string)``.
    """
    pq_ids = [pq_id] if isinstance(pq_id, str) else list(pq_id)

    if not pq_ids:
        return (None, None, "At least one pq_id must be provided")

    try:
        system_name, serials = resolve_pq_ids_to_single_system(pq_ids)
    except InvalidSessionNameError as exc:
        return (None, None, str(exc))

    parsed_pqs: list[tuple[str, CorePlusQuerySerial]] = [
        (pid, CorePlusQuerySerial(serial))
        for pid, serial in zip(pq_ids, serials, strict=True)
    ]
    return (parsed_pqs, system_name, None)


def _pq_state_category(state_name: str) -> str:
    """Return the PQ_STATES category for a state name; INVALID if unrecognized."""
    return PQ_STATES.get(state_name, "INVALID")


def _add_session_id_if_running(
    result_dict: dict[str, object],
    status: CorePlusQueryStatus | None,
    pq_name: str,
    system_name: str,
) -> None:
    """Add ``session_id`` to ``result_dict`` if the PQ is RUNNING, EXECUTING, or INITIALIZING.

    ``session_id`` is added when ``status.is_running`` (covers RUNNING and EXECUTING) or
    ``status.is_initializing`` (covers INITIALIZING). In all other states no ``session_id``
    key is written to ``result_dict``.

    Args:
        result_dict (dict[str, object]): Result dict to mutate in place.
        status (CorePlusQueryStatus | None): Current PQ status. ``None`` is a no-op.
        pq_name (str): PQ name used to construct the session_id.
        system_name (str): Enterprise system name used to construct the session_id.
    """
    if status is not None and (status.is_running or status.is_initializing):
        session_id = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, system_name, pq_name
        )
        result_dict["session_id"] = session_id


async def pq_name_to_id(
    context: Context,
    system: str,
    pq_name: str,
) -> dict:
    """MCP Tool: Convert PQ name to pq_id format.

    Looks up a persistent query by name and returns its pq_id (the canonical
    identifier used by all other PQ management tools). Use this when you know
    the human-readable PQ name but need the pq_id for other operations.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Serial numbers are system-assigned unique integer identifiers
    - pq_id is the canonical string format: '<system_name>:<serial>'
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use this tool when you know the PQ name but need the pq_id
    - The returned pq_id can be used with pq_details, pq_start, pq_stop, etc.
    - This tool performs a network lookup to find the serial number
    - If the PQ doesn't exist, you'll get an error

    Args:
        context (Context): MCP context object
        system (str): Enterprise system name as listed by ``list_systems``
        pq_name (str): Human-readable name of the persistent query

    Returns:
        dict: Success response:
        {
            "success": True,
            "pq_id": "prod:12345",
            "serial": 12345,
            "name": "analytics_worker",
            "system_name": "prod"
        }

        dict: Error response:
        {
            "success": False,
            "error": "PQ 'nonexistent' not found on system 'prod'",
            "isError": True
        }
    """
    result: dict[str, object] = {"success": False}
    system_name = system

    try:
        session_registry = get_enterprise_registry(context, system)
        _LOGGER.info(
            f"[mcp_systems_server:pq_name_to_id] Invoked: system_name={system_name!r}, pq_name={pq_name!r}"
        )
        factory_manager = session_registry.factory_manager
        _LOGGER.debug(
            "[mcp_systems_server:pq_name_to_id] Connecting to enterprise factory"
        )
        factory = await factory_manager.get()
        _LOGGER.debug(
            "[mcp_systems_server:pq_name_to_id] Connected to enterprise factory"
        )

        # Get controller client
        controller = factory.controller_client

        # Look up serial by name
        try:
            _LOGGER.debug(
                f"[mcp_systems_server:pq_name_to_id] Looking up serial for PQ '{pq_name}' on system '{system_name}'"
            )
            serial = await controller.get_serial_for_name(pq_name)
        except Exception as e:
            error_msg = f"PQ '{pq_name}' not found on system '{system_name}': {e}"
            _LOGGER.warning(f"[mcp_systems_server:pq_name_to_id] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Success - construct pq_id
        pq_id = _make_pq_id(serial, system_name)
        result = {
            "success": True,
            "pq_id": pq_id,
            "serial": serial,
            "name": pq_name,
            "system_name": system_name,
        }

        _LOGGER.info(
            f"[mcp_systems_server:pq_name_to_id] Converted PQ '{pq_name}' to pq_id '{pq_id}' (serial: {serial})"
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_name_to_id] Failed to convert PQ name '{pq_name}' on system '{system_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to convert PQ name '{pq_name}' to ID on system '{system_name}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result


async def pq_list(
    context: Context,
    system: str,
) -> dict:
    """MCP Tool: List all persistent queries (PQs) on an enterprise system.

    Returns a summary list of all persistent queries managed by the specified enterprise
    system's controller, including key fields for filtering and identification.
    Use pq_details to get full configuration and state information for a specific PQ.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use this to discover all PQs on a system before performing operations
    - Each PQ includes a pq_id that can be used with pq_details, pq_start, pq_stop, etc.
    - PQ state vocabulary — ACTIVE: RUNNING, EXECUTING;
      TRANSITIONAL (do not branch on a specific value): UNINITIALIZED, CONNECTING,
      AUTHENTICATING, ACQUIRING_WORKER, INITIALIZING, STOPPING, DISCONNECTED;
      TERMINAL (stable until user action): STOPPED, FAILED, KILLED, COMPLETED, ERROR;
      STOPPED and FAILED can be restarted; INVALID sentinel: UNSPECIFIED
    - Each PQ entry includes a status_category field (ACTIVE/TRANSITIONAL/TERMINAL/INVALID)
      so you can branch on category without memorizing which states fall in each group
    - Never write `if status == "RUNNING"` — use `if status_category == "ACTIVE"` instead
    - session_id field only present when status is RUNNING, EXECUTING, or INITIALIZING
    - Filter results by status, owner, worker_kind, configuration_type, or script_language
    - Use pq_details(pq_id) to get full configuration and state for a specific PQ
    - Empty pqs list is valid - indicates no PQs configured on the system
    - num_failures is the cumulative lifetime failure count for the PQ (not reset on restart)

    Args:
        context (Context): MCP context object
        system (str): Enterprise system name as listed by ``list_systems``

    Returns:
        dict: Success response:
        {
            "success": True,
            "system_name": "prod_cluster",
            "pqs": [
                {
                    "pq_id": "prod_cluster:12345",
                    "serial": 12345,
                    "name": "analytics_worker",
                    "status": "RUNNING",
                    "status_category": "ACTIVE",  # ACTIVE | TRANSITIONAL | TERMINAL | INVALID
                    "enabled": True,
                    "owner": "admin_user",
                    "heap_size_gb": 8.0,
                    "worker_kind": "DeephavenCommunity",
                    "configuration_type": "Script",
                    "script_language": "Python",
                    "server_name": "QueryServer_1",
                    "admin_groups": ["admins", "data-team"],
                    "viewer_groups": ["analysts"],
                    "is_scheduled": True,
                    "num_failures": 0,
                    "session_id": "enterprise:prod_cluster:analytics_worker"  # Only when RUNNING, EXECUTING, or INITIALIZING
                }
            ]
        }

        Error response:
        {
            "success": False,
            "error": "Enterprise system 'prod' not found in configuration",
            "isError": True
        }
    """
    result: dict[str, object] = {"success": False}
    system_name = system

    try:
        session_registry = get_enterprise_registry(context, system)
        _LOGGER.info(
            f"[mcp_systems_server:pq_list] Invoked: system_name={system_name!r}"
        )
        factory_manager = session_registry.factory_manager
        _LOGGER.debug("[mcp_systems_server:pq_list] Connecting to enterprise factory")
        factory = await factory_manager.get()
        _LOGGER.debug("[mcp_systems_server:pq_list] Connected to enterprise factory")

        # Get controller client
        controller = factory.controller_client

        # Get all PQs from controller
        _LOGGER.debug(
            f"[mcp_systems_server:pq_list] Fetching PQ map from controller for system '{system_name}'"
        )
        pq_map = await controller.map()
        _LOGGER.debug(
            f"[mcp_systems_server:pq_list] Received {len(pq_map)} PQ(s) from controller for system '{system_name}'"
        )

        # Format PQ list with trimmed summary fields
        # NOTE: pq_info is PersistentQueryInfoMessage
        # Protobuf docs: https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.persistent_query.PersistentQueryInfoMessage
        # Full details available via pq_details tool
        pqs = []
        for serial, pq_info in pq_map.items():
            config_pb = pq_info.config.pb
            state_pb = pq_info.state.pb if pq_info.state else None
            pq_name = config_pb.name
            pq_id = _make_pq_id(serial, system_name)
            status_obj = pq_info.state.status if pq_info.state else None
            status = status_obj.name if status_obj is not None else "UNKNOWN"

            pq_data = {
                "pq_id": pq_id,
                "serial": serial,
                "name": pq_name,
                "status": status,
                "status_category": _pq_state_category(status),
                "enabled": config_pb.enabled,
                "owner": config_pb.owner,
                "heap_size_gb": config_pb.heapSizeGb,
                "worker_kind": config_pb.workerKind,
                "configuration_type": config_pb.configurationType,
                "script_language": config_pb.scriptLanguage,
                "server_name": config_pb.serverName or None,
                "admin_groups": list(config_pb.adminGroups),
                "viewer_groups": list(config_pb.viewerGroups),
                "is_scheduled": bool(config_pb.scheduling),
                "num_failures": state_pb.numFailures if state_pb else 0,
            }

            # Add session_id if PQ is running (session_id uses name, not serial)
            _add_session_id_if_running(pq_data, status_obj, pq_name, system_name)

            pqs.append(pq_data)

        _LOGGER.info(
            f"[mcp_systems_server:pq_list] Found {len(pqs)} PQs on system '{system_name}'"
        )

        result.update(
            {
                "success": True,
                "system_name": system_name,
                "pqs": pqs,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_list] Failed to list PQs on system '{system_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to list PQs on system '{system_name}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result


async def pq_details(
    context: Context,
    pq_id: str,
) -> dict:
    """MCP Tool: Get detailed information about a persistent query.

    Retrieves comprehensive details about a specific PQ including its full
    configuration, current state, resource allocation, permissions, and
    session connection details if running.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use pq_id from pq_list to identify the PQ
    - If you only have a PQ name, use pq_name_to_id to look up the pq_id first
    - session_id only present when state is RUNNING, EXECUTING, or INITIALIZING
    - Worker host available at state_details.connection_details.processor_host
    - Worker port available at state_details.connection_details.protocols[*].port
    - connection_details is null when PQ is not running
    - Use session_id with other session tools to interact with a running PQ
    - null script_path in config means inline script_code is used (or vice versa)
    - Empty arrays ([]) indicate optional features are disabled (scheduling, admin_groups, etc.)
    - jvm_profile null means default JVM settings are used
    - replicas array contains state of all active replicas (load-balanced instances)
    - spares array contains state of spare instances ready to replace failed replicas
    - num_failures in state_details is the cumulative lifetime failure count

    Args:
        context (Context): MCP context object
        pq_id (str): PQ identifier in format '<system_name>:<serial>'

    Returns:
        dict: Success response with comprehensive PQ information:
        {
            "success": True,
            "pq_id": "prod:12345",
            "serial": 12345,
            "name": "analytics_worker",
            "state": "RUNNING",
            "session_id": "enterprise:prod:analytics_worker",
            "config": {
                "serial": 12345,
                "version": 5,
                "name": "analytics_worker",
                "owner": "admin_user",
                "enabled": true,
                "heap_size_gb": 8.0,
                "buffer_pool_to_heap_ratio": 0.5,
                "detailed_gc_logging_enabled": false,
                "extra_jvm_arguments": ["-XX:+UseG1GC"],
                "extra_environment_variables": ["VAR1=value1"],
                "class_path_additions": ["/custom/libs"],
                "server_name": "QueryServer_1",
                "admin_groups": ["admins", "data-team"],
                "viewer_groups": ["analysts"],
                "restart_users": "RU_ADMIN",
                "script_code": null,
                "script_path": "/scripts/analytics.py",
                "script_language": "Python",
                "configuration_type": "Script",
                "type_specific_fields_json": null,
                "scheduling": ["SchedulerType=Daily", "StartTime=08:00:00"],
                "timeout_nanos": 300000000000,
                "jvm_profile": "large-memory",
                "last_modified_by_authenticated": "admin_user",
                "last_modified_by_effective": "admin_user",
                "last_modified_time_nanos": 1734467200000000000,
                "completed_status": null,
                "expiration_time_nanos": null,
                "kubernetes_control": null,
                "worker_kind": "DeephavenCommunity",
                "created_time_nanos": 1734380800000000000,
                "replica_count": 2,
                "spare_count": 1,
                "assignment_policy": "RoundRobin",
                "assignment_policy_params": null,
                "additional_memory_gb": 2.0,
                "python_control": "analytics-env",
                "generic_worker_control": null
            },
            "state_details": {
                "serial": 12345,
                "version": 5,
                "status": "RUNNING",
                "initialization_start_nanos": 1734467100000000000,
                "initialization_complete_nanos": 1734467110000000000,
                "last_update_nanos": 1734467200000000000,
                "dispatcher_host": "dispatcher.example.com",
                "table_groups": [],
                "scope_types": [],
                "connection_details": {
                    "protocols": [{"name": "grpc", "port": 10000}],
                    "worker_name": "worker-01",
                    "process_info_id": "pid-12345",
                    "processor_host": "worker-01.example.com",
                    "envoy_prefix": null,
                    "grpc_url": "grpc://worker-01.example.com:10000",
                    "static_url": null,
                    "enterprise_web_socket_url": null
                },
                "exception_details": null,
                "type_specific_state_json": null,
                "last_authenticated_user": "admin_user",
                "last_effective_user": "admin_user",
                "script_loader_state_json": null,
                "has_progress": false,
                "progress_value": 0,
                "progress_message": null,
                "engine_version": "0.35.0",
                "dispatcher_port": 8080,
                "should_stop_nanos": null,
                "num_failures": 0,
                "last_failure_time_nanos": null,
                "replica_slot": 0,
                "status_details": null
            },
            "replicas": [
                {
                    "serial": 12345,
                    "version": 5,
                    "status": "RUNNING",
                    "initialization_start_nanos": 1734467100000000000,
                    "initialization_complete_nanos": 1734467110000000000,
                    "last_update_nanos": 1734467200000000000,
                    "dispatcher_host": "dispatcher.example.com",
                    "table_groups": [],
                    "scope_types": [],
                    "connection_details": null,
                    "exception_details": null,
                    "type_specific_state_json": null,
                    "last_authenticated_user": "admin_user",
                    "last_effective_user": "admin_user",
                    "script_loader_state_json": null,
                    "has_progress": false,
                    "progress_value": 0,
                    "progress_message": null,
                    "engine_version": "0.35.0",
                    "dispatcher_port": 8080,
                    "should_stop_nanos": null,
                    "num_failures": 0,
                    "last_failure_time_nanos": null,
                    "replica_slot": 1,
                    "status_details": null
                }
            ],
            "spares": [
                {
                    "serial": 12345,
                    "version": 5,
                    "status": "INITIALIZING",
                    "initialization_start_nanos": 1734467150000000000,
                    "initialization_complete_nanos": null,
                    "last_update_nanos": 1734467200000000000,
                    "dispatcher_host": "dispatcher.example.com",
                    "table_groups": [],
                    "scope_types": [],
                    "connection_details": null,
                    "exception_details": null,
                    "type_specific_state_json": null,
                    "last_authenticated_user": "admin_user",
                    "last_effective_user": "admin_user",
                    "script_loader_state_json": null,
                    "has_progress": true,
                    "progress_value": 42,
                    "progress_message": "Loading script",
                    "engine_version": "0.35.0",
                    "dispatcher_port": 8080,
                    "should_stop_nanos": null,
                    "num_failures": 0,
                    "last_failure_time_nanos": null,
                    "replica_slot": 0,
                    "status_details": null
                }
            ]
        }

        dict: Error response:
        {
            "success": False,
            "error": "PQ 'nonexistent' not found",
            "isError": True
        }
    """
    _LOGGER.info(f"[mcp_systems_server:pq_details] Invoked: pq_id={pq_id!r}")

    result: dict[str, object] = {"success": False}

    try:
        # Early validation: parse pq_id to fail fast on invalid format
        try:
            system_name, serial = _parse_pq_id(pq_id)
        except ValueError as e:
            result["error"] = str(e)
            result["isError"] = True
            return result

        try:
            session_registry = get_enterprise_registry(context, system_name)
        except InvalidSessionNameError as e:
            result["error"] = str(e)
            result["isError"] = True
            return result

        factory_manager = session_registry.factory_manager
        _LOGGER.debug(
            "[mcp_systems_server:pq_details] Connecting to enterprise factory"
        )
        factory = await factory_manager.get()
        _LOGGER.debug("[mcp_systems_server:pq_details] Connected to enterprise factory")

        # Get controller client
        controller = factory.controller_client

        # Get all PQs from controller (ensures subscription is ready)
        # Then extract the specific PQ by serial
        _LOGGER.debug("[mcp_systems_server:pq_details] Fetching PQ map from controller")
        pq_map = await controller.map()
        _LOGGER.debug(
            f"[mcp_systems_server:pq_details] Received {len(pq_map)} PQ(s) from controller"
        )

        if serial not in pq_map:
            error_msg = f"PQ with serial {serial} not found"
            _LOGGER.warning(f"[mcp_systems_server:pq_details] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        pq_info = pq_map[serial]

        # Format response
        # NOTE: pq_info is PersistentQueryInfoMessage
        # Protobuf docs: https://docs.deephaven.io/protodoc/latest/#io.deephaven.proto.persistent_query.PersistentQueryInfoMessage
        pq_name = pq_info.config.pb.name
        status_obj = pq_info.state.status if pq_info.state else None
        state_name = status_obj.name if status_obj is not None else "UNKNOWN"

        pq_data = {
            "success": True,
            "pq_id": pq_id,
            "serial": serial,
            "name": pq_name,
            "state": state_name,
            "config": _format_pq_config(pq_info.config),
            "state_details": _format_pq_state(pq_info.state),
            "replicas": _format_pq_states(pq_info.replicas),
            "spares": _format_pq_states(pq_info.spares),
        }

        # Add session_id if running (session_id uses name, not serial)
        _add_session_id_if_running(pq_data, status_obj, pq_name, system_name)

        _LOGGER.info(
            f"[mcp_systems_server:pq_details] Retrieved details for PQ '{pq_name}' (serial: {serial})"
        )

        result.update(pq_data)

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_details] Failed to get PQ details: {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to get PQ details for '{pq_id}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result


async def pq_create(
    context: Context,
    system: str,
    pq_name: str,
    heap_size_gb: float | int,
    script_body: str | None = None,
    script_path: str | None = None,
    programming_language: str = "Python",
    configuration_type: str = "Script",
    enabled: bool = True,
    schedule: list[str] | None = None,
    server: str | None = None,
    engine: str = "DeephavenCommunity",
    jvm_profile: str | None = None,
    extra_jvm_args: list[str] | None = None,
    extra_class_path: list[str] | None = None,
    python_virtual_environment: str | None = None,
    extra_environment_vars: list[str] | None = None,
    init_timeout_nanos: int | None = None,
    auto_delete_timeout: int | None = None,
    admin_groups: list[str] | None = None,
    viewer_groups: list[str] | None = None,
    restart_users: str | None = None,
    owner: str | None = None,
) -> dict:
    """MCP Tool: Create a new persistent query on an enterprise system.

    Creates a PQ configuration and adds it to the controller.

    Scheduler semantics (``auto_delete_timeout`` and ``schedule`` are mutually exclusive —
    supplying both returns an error, because ``auto_delete_timeout`` installs its own scheduler):
        - No ``schedule`` and ``auto_delete_timeout`` is ``None`` (default) or ``0``: a
          continuous (permanent) scheduler with ``SchedulingDisabled=false`` and
          ``RestartWhenRunning=Yes``, which causes the controller to begin acquiring a worker
          immediately after creation (``ACQUIRING_WORKER`` → ``RUNNING`` with no explicit
          ``pq_start`` call).
        - No ``schedule`` and ``auto_delete_timeout`` is a positive integer: a temporary
          scheduler that auto-deletes the PQ after that many seconds of inactivity.
        - ``schedule=[...]`` (non-empty list): the caller-supplied list **replaces** the
          scheduling block wholesale. No default keys are merged in — the caller is
          responsible for including ``SchedulerType`` and any other required entries.
        - ``schedule=[]``: the caller is explicitly requesting no scheduling. The
          scheduling list is cleared; the server decides whether to accept or reject a
          PQ with no scheduling entries.

    Creating a quiescent PQ (created but not auto-starting):
        Two supported recipes, each with different semantics:
        - ``enabled=False``: the PQ is marked disabled. The controller will not acquire
          a worker. Subsequent ``pq_start`` typically requires enabling the PQ first.
        - ``schedule=[...]`` with ``SchedulingDisabled=true``: the PQ has a valid,
          disabled scheduler. Manual ``pq_start`` / ``pq_stop`` / ``pq_restart`` work
          normally; only the automatic scheduler trigger is suppressed. The Core+ client
          library publishes a canonical "disabled daily" scheduler that can be used
          here; see its ``generate_disabled_scheduler`` helper for the full list of
          entries.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Permanent PQs (``auto_delete_timeout=None``, the default) auto-start after creation
      via the default continuous scheduler. See "Scheduler semantics" above. Do not call
      ``pq_start`` on a freshly-created permanent PQ unless you have overridden the
      default — it will already be acquiring a worker. Use ``pq_details`` to observe the
      state transition.
    - Returns pq_id and serial number for use with other PQ management tools
    - programming_language is case-insensitive: "Python"/"python" or "Groovy"/"groovy"
    - auto_delete_timeout=None (default) or 0 creates a permanent PQ; a positive value creates a
      temporary PQ deleted after that many seconds of inactivity
    - auto_delete_timeout installs its own scheduler, so it is mutually exclusive with schedule;
      supplying both returns an error
    - owner=None (default) makes the PQ owned by the authenticated user; set it to assign a
      different owner (e.g. a service account). Setting another user as owner may require
      server-side permission and is rejected by the controller if you lack it.
    - Specify code via script_body (inline) OR script_path (Git) - specifying both causes error
    - Omit both script_body and script_path to create empty interactive session
    - configuration_type="RunAndDone" for batch jobs that execute once and stop
    - configuration_type="Script" (default) for long-running interactive sessions
    - schedule parameter enables automated start/stop - see detailed format below
    - All list parameters (schedule, admin_groups, etc.) accept empty list [] or None

    Script Source Options (mutually exclusive):
    - script_body: Inline Python/Groovy code as a string. Use for simple scripts or dynamic code generation.
    - script_path: Path to script in controller's Git repository (e.g., "IrisQueries/groovy/analytics.groovy"). Use for version-controlled scripts.
    - Both None: Creates empty interactive session where code is entered manually after starting.
    - Both specified: Returns validation error - only one source allowed.

    Configuration Types:
    - "Script": Standard live interactive query (default, runs continuously)
    - "RunAndDone": Batch query that executes once and terminates automatically
    - Other types exist (Merge, Import, etc.) but are specialized

    Scheduling Format (list of "Key=Value" strings):
    - SchedulerType: Use fully qualified Java class name (required whenever the list
      is non-empty)
    - Time format: HH:MM:SS (24-hour) for all time fields
    - TimeZone: Standard timezone identifiers (e.g., "America/New_York", "UTC")
    - See "Scheduler semantics" above for how ``None`` / ``[]`` / non-empty list are
      interpreted on create.

    Daily Scheduler:
      ["SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerDaily",
       "StartTime=08:00:00", "StopTime=18:00:00", "TimeZone=America/New_York"]
      - Required: SchedulerType, StartTime, StopTime
      - Optional: TimeZone (defaults to server timezone)

    Continuous Scheduler:
      ["SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerContinuous"]
      - Required: SchedulerType only
      - Runs continuously without stop times

    Monthly Scheduler:
      ["SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerMonthly",
       "DayOfMonth=1", "StartTime=00:00:00", "TimeZone=UTC"]
      - Required: SchedulerType, DayOfMonth, StartTime
      - Optional: TimeZone, StopTime
      - DayOfMonth: 1-31 (or last day if month has fewer days)

    Restart Permissions:
    - "RU_ADMIN": Only administrators can restart (most restrictive)
    - "RU_ADMIN_AND_VIEWERS": Both admins and viewers can restart
    - "RU_VIEWERS_WHEN_DOWN": Admins always; viewers only when query is down

    Args:
        context (Context): MCP context object
        system (str): Enterprise system name as listed by ``list_systems``
        pq_name (str): Human-readable name for the PQ
        heap_size_gb (float | int): JVM heap size in GB (e.g., 8.0 or 16)
        script_body (str | None): Inline script code to execute (mutually exclusive with script_path)
        script_path (str | None): Path to script in Git repository (mutually exclusive with script_body)
        programming_language (str): Script language - "Python" or "Groovy", case-insensitive (default: "Python")
        configuration_type (str): Query type - "Script" (live) or "RunAndDone" (batch), default: "Script"
        enabled (bool): Whether query can be executed (default: True)
        schedule (list[str] | None): Scheduling config as ["Key=Value", ...] (e.g., ["SchedulerType=...", "StartTime=08:00:00"])
        server (str | None): Specific server to run on (None = controller chooses)
        engine (str): Worker engine type (default: "DeephavenCommunity")
        jvm_profile (str | None): Named JVM profile from controller config (e.g., "large-memory")
        extra_jvm_args (list[str] | None): Additional JVM arguments
        extra_class_path (list[str] | None): Additional classpath entries to prepend (e.g., ["/opt/libs/custom.jar"])
        python_virtual_environment (str | None): Named Python venv for Core+ workers
        extra_environment_vars (list[str] | None): Environment variables as ["KEY=value", ...]
        init_timeout_nanos (int | None): Initialization timeout in nanoseconds
        auto_delete_timeout (int | None): Seconds of inactivity before auto-deletion. None (default) and 0 = permanent; positive = temporary
        admin_groups (list[str] | None): Groups with admin access
        viewer_groups (list[str] | None): Groups with viewer access
        restart_users (str | None): Who can restart - "RU_ADMIN", "RU_ADMIN_AND_VIEWERS", "RU_VIEWERS_WHEN_DOWN"
        owner (str | None): User to set as the PQ owner. None (default) leaves the owner as the authenticated user.

    Returns:
        dict: Success response. The ``"state"`` field is a fixed placeholder
        (``"UNINITIALIZED"``) emitted at the moment the controller accepts the
        ``add_query`` call; it does **not** reflect the live state of the PQ at
        response time. In particular, for a permanent PQ created with the default
        continuous scheduler the controller typically begins acquiring a worker
        immediately, so by the time the caller reads this field the real state may
        already be ``ACQUIRING_WORKER``, ``INITIALIZING``, or ``RUNNING``. Call
        ``pq_details`` with the returned ``pq_id`` to observe the actual live state.

        {
            "success": True,
            "pq_id": "prod:12345",
            "serial": 12345,
            "name": "analytics_worker",
            "state": "UNINITIALIZED",
            "message": "PQ created successfully"
        }

        dict: Error response:
        {
            "success": False,
            "error": "Failed to create PQ: ...",
            "isError": True
        }
    """
    result: dict[str, object] = {"success": False}
    system_name = "<unknown>"

    try:
        # Early validation: mutually exclusive arguments.
        if script_body is not None and script_path is not None:
            result["error"] = (
                "script_body and script_path are mutually exclusive. "
                "Specify one or the other, not both."
            )
            result["isError"] = True
            return result
        if auto_delete_timeout is not None and schedule is not None:
            result["error"] = (
                "auto_delete_timeout and schedule are mutually exclusive. "
                "auto_delete_timeout installs its own scheduler."
            )
            result["isError"] = True
            return result

        session_registry = get_enterprise_registry(context, system)
        system_name = system
        _LOGGER.info(
            f"[mcp_systems_server:pq_create] Invoked: system_name={system_name!r}, "
            f"pq_name={pq_name!r}, heap_size_gb={heap_size_gb}"
        )
        factory_manager = session_registry.factory_manager
        _LOGGER.debug("[mcp_systems_server:pq_create] Connecting to enterprise factory")
        factory = await factory_manager.get()
        _LOGGER.debug("[mcp_systems_server:pq_create] Connected to enterprise factory")

        # Get controller client
        controller = factory.controller_client

        # Create PQ configuration (the client normalizes programming_language)
        pq_config = await controller.make_pq_config(
            name=pq_name,
            heap_size_gb=heap_size_gb,
            script_body=script_body,
            script_path=script_path,
            programming_language=programming_language,
            configuration_type=configuration_type,
            enabled=enabled,
            schedule=schedule,
            server=server,
            engine=engine,
            jvm_profile=jvm_profile,
            extra_jvm_args=extra_jvm_args,
            extra_class_path=extra_class_path,
            python_virtual_environment=python_virtual_environment,
            extra_environment_vars=extra_environment_vars,
            init_timeout_nanos=init_timeout_nanos,
            auto_delete_timeout=auto_delete_timeout,
            admin_groups=admin_groups,
            viewer_groups=viewer_groups,
            restart_users=restart_users,
            owner=owner,
        )

        # Add the PQ to controller
        _LOGGER.debug(
            f"[mcp_systems_server:pq_create] Submitting PQ '{pq_name}' to controller on system '{system_name}'"
        )
        serial = await controller.add_query(pq_config)
        _LOGGER.debug(
            f"[mcp_systems_server:pq_create] Controller accepted PQ '{pq_name}' with serial {serial}"
        )

        # Construct pq_id (serial-based)
        pq_id = _make_pq_id(serial, system_name)

        _LOGGER.info(
            f"[mcp_systems_server:pq_create] Created PQ '{pq_name}' with serial {serial}, pq_id='{pq_id}'"
        )

        result.update(
            {
                "success": True,
                "pq_id": pq_id,
                "serial": serial,
                "name": pq_name,
                "state": "UNINITIALIZED",
                "message": f"PQ '{pq_name}' created successfully with serial {serial}",
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_create] Failed to create PQ '{pq_name}': {e!r}",
            exc_info=True,
        )
        # Provide descriptive error message with exception type
        error_msg = str(e) if str(e) else repr(e)
        result["error"] = (
            f"Failed to create PQ '{pq_name}': {type(e).__name__}: {error_msg}"
        )
        result["isError"] = True

    return result


async def pq_delete(
    context: Context,
    pq_id: str | list[str],
    max_concurrent: Annotated[int | None, Field(gt=0)] = None,
) -> dict:
    """MCP Tool: Delete one or more persistent queries.

    Permanently removes one or more PQs from the controller. If any PQ is running,
    it will be stopped first. This operation cannot be undone.

    **Batch Support**: This operation supports batch execution for efficiency.
    Pass a single pq_id string or a list of pq_id strings.

    **Best-Effort Execution**: Each PQ is deleted independently. If some deletions fail,
    successful deletions are still completed and reported. Check individual item success
    status in the results.

    **Important**: All pq_ids must be from the same enterprise system - mixing systems returns an error.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use pq_id from pq_list to identify PQs
    - If you only have PQ names, use pq_name_to_id to look up the pq_ids first
    - Single PQ: pass string "enterprise:system:12345"
    - Multiple PQs: pass list ["enterprise:system:12345", "enterprise:system:67890"]
    - Best-effort: partial success is possible, check summary and individual results
    - Each result item has same fields: pq_id, serial, success, name, error
    - Note: Results do NOT include session_id field (PQ is deleted and has no session)
    - If success=True: name has value, error is None
    - If success=False: name is None, error has message
    - Operation is irreversible - confirm before deleting
    - Running PQs will be stopped automatically before deletion

    Args:
        context (Context): MCP context object
        pq_id (str | list[str]): PQ identifier or list of identifiers in format '<system_name>:<serial>'
        max_concurrent (int): Maximum concurrent delete operations (default: 20).
                              Must be a positive integer (> 0).

    Returns:
        dict: Response with per-item results:
        {
            "success": True,
            "results": [
                {
                    "pq_id": "prod:12345",
                    "serial": 12345,
                    "success": True,
                    "name": "analytics_worker",
                    "error": None
                },
                {
                    "pq_id": "prod:67890",
                    "serial": 67890,
                    "success": False,
                    "name": None,
                    "error": "PQ not found"
                }
            ],
            "summary": {"total": 2, "succeeded": 1, "failed": 1},
            "message": "Deleted 1 of 2 PQ(s), 1 failed"
        }

        dict: System error response (operation didn't execute):
        {
            "success": False,
            "error": "Invalid parameters",
            "isError": True
        }
    """
    if max_concurrent is None:
        max_concurrent = get_enterprise_settings(
            context
        ).pq_tools.default_max_concurrent
    _LOGGER.info(f"[mcp_systems_server:pq_delete] Invoked: pq_id={pq_id!r}")

    result: dict[str, object] = {"success": False}

    try:
        # Common setup and validation for batch operations
        (
            parsed_pqs,
            controller,
            validated_max_concurrent,
            _system_name,
            setup_error,
        ) = await _setup_batch_pq_operation(context, pq_id, "pq_delete", max_concurrent)
        if setup_error:
            return setup_error

        # Type narrowing: when setup_error is None, all values are guaranteed non-None
        parsed_pqs = cast(list[tuple[str, CorePlusQuerySerial]], parsed_pqs)
        controller = cast(CorePlusControllerClient, controller)
        validated_max_concurrent = cast(int, validated_max_concurrent)

        # Process each PQ with controlled parallelism (best-effort)
        # Note: Controller API supports batch deletion, but we process with parallel
        # individual calls to provide granular per-item success/failure reporting
        # for AI agents while maintaining performance
        _LOGGER.info(
            f"[mcp_systems_server:pq_delete] Processing {len(parsed_pqs)} PQ(s) "
            f"with max_concurrent={validated_max_concurrent}"
        )

        async def delete_single_pq(
            pid: str, serial: CorePlusQuerySerial
        ) -> dict[str, object]:
            """Delete a single PQ and return result dict."""
            item_result: dict[str, object] = {
                "pq_id": pid,
                "serial": serial,
                "success": False,
                "name": None,
                "error": None,
            }

            try:
                # Get name before deletion
                pq_info = await controller.get(serial)
                pq_name = pq_info.config.pb.name

                # Delete the PQ
                await controller.delete_query(serial)

                # Success
                item_result["success"] = True
                item_result["name"] = pq_name
                _LOGGER.debug(
                    f"[mcp_systems_server:pq_delete] Successfully deleted PQ {pid}"
                )

            except Exception as e:
                # Failure - record error
                item_result["error"] = (
                    f"{type(e).__name__}: {str(e) if str(e) else repr(e)}"
                )
                _LOGGER.warning(
                    f"[mcp_systems_server:pq_delete] Failed to delete PQ {pid}: {item_result['error']}"
                )

            return item_result

        # Use semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(validated_max_concurrent)

        async def delete_with_limit(
            pid: str, serial: CorePlusQuerySerial
        ) -> dict[str, object]:
            """Delete with concurrency limit."""
            async with semaphore:
                return await delete_single_pq(pid, serial)

        # Execute all deletions in parallel with concurrency control
        # return_exceptions=True ensures one failure doesn't cancel other operations
        raw_results = await asyncio.gather(
            *[delete_with_limit(pid, serial) for pid, serial in parsed_pqs],
            return_exceptions=True,
        )

        # Handle any unexpected exceptions that weren't caught in the operation functions
        results: list[dict[str, object]] = []
        for i, r in enumerate(raw_results):
            if isinstance(r, Exception):
                # Unexpected exception - convert to error dict
                pid, serial = parsed_pqs[i]
                results.append(
                    {
                        "pq_id": pid,
                        "serial": serial,
                        "success": False,
                        "name": None,
                        "error": f"Unexpected error: {type(r).__name__}: {r}",
                    }
                )
            else:
                # Normal dict result from operation function
                results.append(cast(dict[str, object], r))

        # Calculate summary
        succeeded = sum(1 for r in results if r["success"])
        failed = len(results) - succeeded

        # Build message
        if failed == 0:
            message = f"Deleted {succeeded} PQ(s)"
        else:
            message = f"Deleted {succeeded} of {len(results)} PQ(s), {failed} failed"

        _LOGGER.info(
            f"[mcp_systems_server:pq_delete] {message}: "
            f"succeeded={[r['pq_id'] for r in results if r['success']]}, "
            f"failed={[r['pq_id'] for r in results if not r['success']]}"
        )

        # Always return consistent format
        result.update(
            {
                "success": True,
                "results": results,
                "summary": {
                    "total": len(results),
                    "succeeded": succeeded,
                    "failed": failed,
                },
                "message": message,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_delete] Failed to delete PQ(s): {e!r}",
            exc_info=True,
        )
        result["error"] = f"Failed to delete PQ(s): {type(e).__name__}: {e}"
        result["isError"] = True

    return result


async def pq_modify(
    context: Context,
    pq_id: str,
    restart: bool = False,
    pq_name: str | None = None,
    heap_size_gb: float | int | None = None,
    script_body: str | None = None,
    script_path: str | None = None,
    programming_language: str | None = None,
    configuration_type: str | None = None,
    enabled: bool | None = None,
    schedule: list[str] | None = None,
    server: str | None = None,
    engine: str | None = None,
    jvm_profile: str | None = None,
    extra_jvm_args: list[str] | None = None,
    extra_class_path: list[str] | None = None,
    python_virtual_environment: str | None = None,
    extra_environment_vars: list[str] | None = None,
    init_timeout_nanos: int | None = None,
    auto_delete_timeout: int | None = None,
    admin_groups: list[str] | None = None,
    viewer_groups: list[str] | None = None,
    restart_users: str | None = None,
    owner: str | None = None,
) -> dict:
    """MCP Tool: Modify an existing persistent query configuration.

    Updates a PQ's configuration by merging provided parameters with the current config.
    Only specified (non-None) parameters are updated - all others remain unchanged.
    Changes can be applied to PQs in any state (RUNNING, STOPPED, etc.).

    Scheduler semantics (``auto_delete_timeout`` and ``schedule`` are mutually exclusive —
    supplying both returns an error, because ``auto_delete_timeout`` installs its own scheduler;
    use ``auto_delete_timeout`` to switch a PQ between permanent and temporary, or ``schedule``
    for a custom scheduler):
        - ``auto_delete_timeout=0``: installs the continuous (permanent) scheduler and clears
          the auto-delete grace period. ``auto_delete_timeout=<positive>``: installs the
          temporary scheduler with that idle timeout. ``auto_delete_timeout=None`` (default):
          leaves scheduling and auto-delete untouched.
        - ``schedule=None`` (default): the existing scheduling on the PQ is preserved
          unchanged.
        - ``schedule=[]``: the caller is explicitly clearing the scheduling. The PQ's
          scheduling list is sent empty to the server; the server decides whether to
          accept or reject a PQ with no scheduling entries.
        - ``schedule=[...]`` (non-empty list): the caller-supplied list **replaces** the
          scheduling block wholesale. No existing entries are merged in — the caller is
          responsible for re-specifying every key they want to keep (including
          ``SchedulerType``, ``TimeZone``, flags like ``SchedulingDisabled``, etc.). To
          tweak a single key, first call ``pq_details`` to read the current
          ``config.scheduling`` list, modify the entries you want, and pass the full
          modified list here.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Only specify parameters you want to change - all params are optional
    - List fields (extra_jvm_args, schedule, etc.) completely REPLACE the existing list
    - restart=True applies changes immediately by restarting the PQ
    - restart=False (default): config is saved but the running worker keeps executing the previous
      script/config. If you modify script_body, heap_size_gb, or other runtime settings on a
      RUNNING PQ without restart=True, the response will include a "warning" field — always check
      for it and call pq_restart to apply the changes.
    - Can modify RUNNING PQs but be cautious - restart=True will disrupt active sessions
    - Use pq_details first to see current config before modifying

    Parameter Behaviors:
    - pq_name: Renames the PQ (does not affect serial number or pq_id)
    - heap_size_gb: Changes JVM heap allocation (requires restart to apply)
    - script_body/script_path: Mutually exclusive - specifying one clears the other
    - programming_language: "Python" or "Groovy" (case-insensitive)
    - configuration_type: "Script" (interactive) or "RunAndDone" (batch)
    - enabled: Whether PQ can be executed (true/false)
    - schedule: List of "Key=Value" strings for scheduling (replaces entire schedule)
    - List fields: Completely replace existing lists (not append/merge)

    Restart Behavior:
    - restart=True: PQ is stopped and restarted immediately, applying all changes
    - restart=False: Changes are saved but PQ continues running with old config until manually restarted
    - Note: Even with restart=False, some changes won't apply until next restart

    Args:
        context (Context): MCP context object
        pq_id (str): PQ identifier in format '<system_name>:<serial>'
        restart (bool): Restart PQ to apply changes immediately (default: False)
        pq_name (str | None): New name for the PQ
        heap_size_gb (float | int | None): JVM heap size in GB (e.g., 8.0 or 16)
        script_body (str | None): Inline script code (mutually exclusive with script_path)
        script_path (str | None): Path to script in Git repository (mutually exclusive with script_body)
        programming_language (str | None): "Python" or "Groovy", case-insensitive
        configuration_type (str | None): "Script" (live) or "RunAndDone" (batch)
        enabled (bool | None): Whether query can be executed
        schedule (list[str] | None): Scheduling config as ["Key=Value", ...] (replaces current)
        server (str | None): Specific server to run on
        engine (str | None): Worker engine type (default: "DeephavenCommunity")
        jvm_profile (str | None): Named JVM profile from controller config
        extra_jvm_args (list[str] | None): Additional JVM arguments (replaces current)
        extra_class_path (list[str] | None): Additional classpath entries (replaces current)
        python_virtual_environment (str | None): Named Python venv for Core+ workers
        extra_environment_vars (list[str] | None): Environment variables as ["KEY=value", ...] (replaces current)
        init_timeout_nanos (int | None): Initialization timeout in nanoseconds
        auto_delete_timeout (int | None): Seconds of inactivity before auto-deletion. None = no change, 0 = permanent (auto-delete disabled), positive integer = timeout in seconds
        admin_groups (list[str] | None): Groups with admin access (replaces current)
        viewer_groups (list[str] | None): Groups with viewer access (replaces current)
        restart_users (str | None): Who can restart - "RU_ADMIN", "RU_ADMIN_AND_VIEWERS", "RU_VIEWERS_WHEN_DOWN"
        owner (str | None): New query owner. None = no change. Reassigning ownership may require server-side permission.

    Returns:
        dict: Success response. The ``"warning"`` field is only present when the PQ is
        currently RUNNING and runtime-affecting settings (script, heap, JVM args, etc.)
        were changed with ``restart=False`` — the running worker still has the previous
        config. Call ``pq_restart`` to apply when you see it.

        {
            "success": True,
            "pq_id": "prod:12345",
            "serial": 12345,
            "name": "analytics_worker",
            "restarted": False,
            "message": "PQ modified successfully",
            "warning": "Config saved but the PQ is still running the previous configuration. ..."
        }

        dict: Error response:
        {
            "success": False,
            "error": "Failed to modify PQ: ...",
            "isError": True
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:pq_modify] Invoked: pq_id={pq_id!r}, restart={restart}"
    )

    result: dict[str, object] = {"success": False}
    system_name = "<unknown>"

    try:
        # Early validation: mutually exclusive arguments.
        if script_body is not None and script_path is not None:
            result["error"] = (
                "script_body and script_path are mutually exclusive. "
                "Specify one or the other, not both."
            )
            result["isError"] = True
            return result
        if auto_delete_timeout is not None and schedule is not None:
            result["error"] = (
                "auto_delete_timeout and schedule are mutually exclusive. "
                "auto_delete_timeout installs its own scheduler."
            )
            result["isError"] = True
            return result

        # Parse pq_id to get serial and the enterprise system it belongs to.
        try:
            system_name, serial = _parse_pq_id(pq_id)
        except ValueError as e:
            result["error"] = f"Invalid pq_id '{pq_id}': {type(e).__name__}: {e}"
            result["isError"] = True
            return result

        try:
            session_registry = get_enterprise_registry(context, system_name)
        except InvalidSessionNameError as e:
            result["error"] = str(e)
            result["isError"] = True
            return result

        factory_manager = session_registry.factory_manager
        _LOGGER.debug("[mcp_systems_server:pq_modify] Connecting to enterprise factory")
        factory = await factory_manager.get()
        _LOGGER.debug("[mcp_systems_server:pq_modify] Connected to enterprise factory")

        # Get controller client
        controller = factory.controller_client

        # Get all PQs from controller (ensures subscription is ready)
        # Then extract the specific PQ by serial
        _LOGGER.debug("[mcp_systems_server:pq_modify] Fetching PQ map from controller")
        pq_map = await controller.map()
        _LOGGER.debug(
            f"[mcp_systems_server:pq_modify] Received {len(pq_map)} PQ(s) from controller"
        )

        if serial not in pq_map:
            error_msg = f"PQ with serial {serial} not found"
            _LOGGER.warning(f"[mcp_systems_server:pq_modify] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Get current PQ info and config
        pq_info = pq_map[serial]
        config = pq_info.config

        # Apply configuration modifications via the shared client-layer applier.
        has_changes = controller.update_pq_config(
            config,
            pq_name=pq_name,
            heap_size_gb=heap_size_gb,
            script_body=script_body,
            script_path=script_path,
            programming_language=programming_language,
            configuration_type=configuration_type,
            enabled=enabled,
            schedule=schedule,
            server=server,
            engine=engine,
            jvm_profile=jvm_profile,
            extra_jvm_args=extra_jvm_args,
            extra_class_path=extra_class_path,
            python_virtual_environment=python_virtual_environment,
            extra_environment_vars=extra_environment_vars,
            init_timeout_nanos=init_timeout_nanos,
            auto_delete_timeout=auto_delete_timeout,
            admin_groups=admin_groups,
            viewer_groups=viewer_groups,
            restart_users=restart_users,
            owner=owner,
        )

        # Only modify if changes were made
        if not has_changes:
            result["error"] = (
                "No changes specified - at least one parameter must be provided"
            )
            result["isError"] = True
            return result

        # Modify the PQ with the updated existing config
        await controller.modify_query(config, restart=restart)

        _LOGGER.info(
            f"[mcp_systems_server:pq_modify] Modified PQ serial={serial}, name='{config.pb.name}', restart={restart}"
        )

        result.update(
            {
                "success": True,
                "pq_id": pq_id,
                "serial": serial,
                "name": config.pb.name,
                "restarted": restart,
                "message": f"PQ '{config.pb.name}' modified successfully"
                + (" and restarted" if restart else ""),
            }
        )

        if (
            not restart
            and pq_info.state is not None
            and pq_info.state.status.is_running
            and any(
                v is not None
                for v in (
                    script_body,
                    script_path,
                    heap_size_gb,
                    extra_jvm_args,
                    extra_class_path,
                    jvm_profile,
                    python_virtual_environment,
                    programming_language,
                )
            )
        ):
            result["warning"] = (
                "Config saved but the PQ is still running the previous configuration. "
                "Runtime changes (script, heap, JVM args, etc.) require a restart to take effect. "
                "Call pq_restart to apply."
            )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_modify] Failed to modify PQ: {e!r}",
            exc_info=True,
        )
        error_msg = str(e) if str(e) else repr(e)
        result["error"] = (
            f"Failed to modify PQ '{pq_id}': {type(e).__name__}: {error_msg}"
        )
        result["isError"] = True

    return result


async def _pq_start_single(
    controller: CorePlusControllerClient,
    system_name: str,
    pid: str,
    serial: CorePlusQuerySerial,
    wait: bool,
) -> dict[str, object]:
    """Start a single PQ and return the per-item result dict.

    Extracted from :func:`pq_start` to keep the outer function below
    the project's cyclomatic-complexity ceiling.
    """
    item_result: dict[str, object] = {
        "pq_id": pid,
        "serial": serial,
        "success": False,
        "name": None,
        "state": None,
        "state_category": None,
        "session_id": None,
        "error": None,
    }

    try:
        # Check current state before attempting start
        current_info = await controller.get(serial)
        if current_info.state and current_info.state.status.is_running:
            item_result["error"] = "Cannot start a PQ that is already RUNNING"
            _LOGGER.warning(
                f"[mcp_systems_server:pq_start] PQ {pid} is already RUNNING, skipping start"
            )
            return item_result

        # Start the PQ
        _LOGGER.debug(
            f"[mcp_systems_server:pq_start] Calling start_and_wait for PQ {pid} (wait={wait})"
        )
        await controller.start_and_wait(serial, wait=wait)
        _LOGGER.debug(
            f"[mcp_systems_server:pq_start] start_and_wait completed for PQ {pid}"
        )

        # Get updated info
        pq_info = await controller.get(serial)
        pq_name = pq_info.config.pb.name
        status_obj = pq_info.state.status if pq_info.state else None
        state_name = status_obj.name if status_obj is not None else "UNKNOWN"

        # Success
        item_result["success"] = True
        item_result["name"] = pq_name
        item_result["state"] = state_name
        item_result["state_category"] = _pq_state_category(state_name)

        # Add session_id if running (session_id uses name, not serial)
        _add_session_id_if_running(item_result, status_obj, pq_name, system_name)

        _LOGGER.debug(f"[mcp_systems_server:pq_start] Successfully started PQ {pid}")

    except Exception as e:
        # Failure - record error
        item_result["error"] = f"{type(e).__name__}: {str(e) if str(e) else repr(e)}"
        _LOGGER.warning(
            f"[mcp_systems_server:pq_start] Failed to start PQ {pid}: {item_result['error']}"
        )

    return item_result


async def pq_start(
    context: Context,
    pq_id: str | list[str],
    wait: bool = True,
    max_concurrent: Annotated[int | None, Field(gt=0)] = None,
) -> dict:
    """MCP Tool: Start one or more persistent queries.

    Starts one or more stopped or newly created PQs, waiting for them to transition to RUNNING state.

    **Batch Support**: This operation supports batch execution for efficiency.
    Pass a single pq_id string or a list of pq_id strings.

    **Best-Effort Execution**: Each PQ is started independently. If some starts fail,
    successful starts are still completed and reported. Check individual item success
    status in the results.

    **Important**: All pq_ids must be from the same enterprise system - mixing systems returns error.

    **Critical for AI Agents**:
    - When ``wait=True`` (default), the timeout duration is operator-controlled via
      ``enterprise/settings.json: timeouts.client.pq_state_change_timeout_seconds``.
    - If the timeout is reached, the call returns failure BUT the PQ keeps starting in background.
    - After failures, use pq_details to check if PQs eventually reached RUNNING state.
    - Initialization time varies: simple sessions ~5-15s, large heap/complex scripts ~30-60s.
    - Set ``wait=False`` to fire-and-forget (submit start request and return immediately).

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use pq_id from pq_list to identify PQs
    - If you only have PQ names, use pq_name_to_id to look up the pq_ids first
    - Single PQ: pass string "enterprise:system:12345"
    - Multiple PQs: pass list ["enterprise:system:12345", "enterprise:system:67890"]
    - Best-effort: partial success is possible, check summary and individual results
    - Each result item has same fields: pq_id, serial, success, name, state, state_category, session_id, error
    - If success=True: name, state, and state_category have values; session_id is populated
      when state is RUNNING, EXECUTING, or INITIALIZING and omitted otherwise; error is None
    - If success=False: name/state/state_category/session_id are None, error has message
    - Cannot start a PQ that is already RUNNING - will be marked as failed
    - Can start a STOPPED or FAILED PQ - this is a normal operation
    - state_category == "TRANSITIONAL" (e.g. state CONNECTING or INITIALIZING) is a valid
      success outcome when ``wait=False`` or when the operator-configured wait was short —
      success=True means the start was accepted and the PQ was last seen in a non-failed
      state, NOT that it is fully RUNNING; use pq_details to confirm RUNNING if needed.
    - Branch on `state_category == "ACTIVE"` rather than `state == "RUNNING"`

    Args:
        context (Context): MCP context object
        pq_id (str | list[str]): PQ identifier or list of identifiers in format '<system_name>:<serial>'
        wait (bool): When True (default), wait for the PQ to reach RUNNING (or another
                     terminal state) using the operator-configured wait duration
                     (``enterprise/settings.json: timeouts.client.pq_state_change_timeout_seconds``).
                     When False, fire-and-forget: submit the start request and return
                     immediately without waiting.
        max_concurrent (int): Maximum concurrent start operations (default: 20).
                              Must be a positive integer (> 0).

    Returns:
        dict: Response with per-item results:
        {
            "success": True,
            "results": [
                {
                    "pq_id": "prod:12345",
                    "serial": 12345,
                    "success": True,
                    "name": "analytics_worker",
                    "state": "RUNNING",
                    "state_category": "ACTIVE",  # ACTIVE | TRANSITIONAL | TERMINAL | INVALID
                    "session_id": "enterprise:prod:analytics_worker",
                    "error": None
                },
                {
                    "pq_id": "prod:67890",
                    "serial": 67890,
                    "success": False,
                    "name": None,
                    "state": None,
                    "state_category": None,
                    "session_id": None,
                    "error": "Timeout waiting for PQ to start"
                }
            ],
            "summary": {"total": 2, "succeeded": 1, "failed": 1},
            "message": "Started 1 of 2 PQ(s), 1 failed"
        }

        dict: System error response (operation didn't execute):
        {
            "success": False,
            "error": "Invalid parameters",
            "isError": True
        }
    """
    if max_concurrent is None:
        max_concurrent = get_enterprise_settings(
            context
        ).pq_tools.default_max_concurrent
    _LOGGER.info(f"[mcp_systems_server:pq_start] Invoked: pq_id={pq_id!r}, wait={wait}")

    result: dict[str, object] = {"success": False}

    try:
        # Common setup and validation for batch operations
        (
            parsed_pqs,
            controller,
            validated_max_concurrent,
            system_name,
            setup_error,
        ) = await _setup_batch_pq_operation(context, pq_id, "pq_start", max_concurrent)
        if setup_error:
            return setup_error

        # Type narrowing: when setup_error is None, all values are guaranteed non-None
        parsed_pqs = cast(list[tuple[str, CorePlusQuerySerial]], parsed_pqs)
        controller = cast(CorePlusControllerClient, controller)
        validated_max_concurrent = cast(int, validated_max_concurrent)

        # Process each PQ with controlled parallelism (best-effort)
        # Note: Controller start_and_wait() only accepts single serial (no batch support)
        # We process with parallel individual calls to provide granular per-item
        # success/failure reporting for AI agents while maintaining performance
        _LOGGER.info(
            f"[mcp_systems_server:pq_start] Processing {len(parsed_pqs)} PQ(s) "
            f"with max_concurrent={validated_max_concurrent}, wait={wait}"
        )

        # Use semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(validated_max_concurrent)

        async def start_with_limit(
            pid: str, serial: CorePlusQuerySerial
        ) -> dict[str, object]:
            """Start with concurrency limit."""
            async with semaphore:
                return await _pq_start_single(
                    controller, system_name, pid, serial, wait
                )

        # Execute all starts in parallel with concurrency control
        # return_exceptions=True ensures one failure doesn't cancel other operations
        raw_results = await asyncio.gather(
            *[start_with_limit(pid, serial) for pid, serial in parsed_pqs],
            return_exceptions=True,
        )

        # Handle any unexpected exceptions that weren't caught in the operation functions
        results: list[dict[str, object]] = []
        for i, r in enumerate(raw_results):
            if isinstance(r, Exception):
                # Unexpected exception - convert to error dict
                pid, serial = parsed_pqs[i]
                results.append(
                    {
                        "pq_id": pid,
                        "serial": serial,
                        "success": False,
                        "name": None,
                        "state": None,
                        "state_category": None,
                        "session_id": None,
                        "error": f"Unexpected error: {type(r).__name__}: {r}",
                    }
                )
            else:
                # Normal dict result from operation function
                results.append(cast(dict[str, object], r))

        # Calculate summary
        succeeded = sum(1 for r in results if r["success"])
        failed = len(results) - succeeded

        # Build message
        if failed == 0:
            message = f"Started {succeeded} PQ(s)"
        else:
            message = f"Started {succeeded} of {len(results)} PQ(s), {failed} failed"

        _LOGGER.info(
            f"[mcp_systems_server:pq_start] {message}: "
            f"succeeded={[r['pq_id'] for r in results if r['success']]}, "
            f"failed={[r['pq_id'] for r in results if not r['success']]}"
        )

        # Always return consistent format
        result.update(
            {
                "success": True,
                "results": results,
                "summary": {
                    "total": len(results),
                    "succeeded": succeeded,
                    "failed": failed,
                },
                "message": message,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_start] Failed to start PQ: {e!r}",
            exc_info=True,
        )
        result["error"] = f"Failed to start PQ(s): {type(e).__name__}: {e}"
        result["isError"] = True

    return result


async def pq_stop(
    context: Context,
    pq_id: str | list[str],
    wait: bool = True,
    max_concurrent: Annotated[int | None, Field(gt=0)] = None,
) -> dict:
    """MCP Tool: Stop one or more running persistent queries.

    Stops one or more running PQs, waiting for them to transition to STOPPED state.

    **Batch Support**: This operation supports batch execution for efficiency.
    Pass a single pq_id string or a list of pq_id strings.

    **Best-Effort Execution**: Each PQ is stopped independently. If some stops fail,
    successful stops are still completed and reported. Check individual item success
    status in the results.

    **Important**: All pq_ids must be from the same enterprise system - mixing systems returns error.

    **Important**: When ``wait=True`` (default), the timeout duration is operator-controlled
    via ``enterprise/settings.json: timeouts.client.pq_state_change_timeout_seconds``. If reached,
    the call returns failure BUT the PQ keeps stopping in background. Use pq_details to
    check current state after failures. Set ``wait=False`` for fire-and-forget.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use pq_id from pq_list to identify PQs
    - If you only have PQ names, use pq_name_to_id to look up the pq_ids first
    - Single PQ: pass string "enterprise:system:12345"
    - Multiple PQs: pass list ["enterprise:system:12345", "enterprise:system:67890"]
    - Best-effort: partial success is possible, check summary and individual results
    - Each result item has same fields: pq_id, serial, success, name, state, error
    - If success=True: name, state have values, error is None
    - If success=False: name/state are None, error has message
    - Note: Results do NOT include session_id field (PQ is stopped and has no active session)
    - Cannot stop a PQ that is already STOPPED - will be marked as failed
    - Stopping preserves PQ configuration - use pq_start to run again
    - Stopping is graceful - allows scripts to finish current operations

    Args:
        context (Context): MCP context object
        pq_id (str | list[str]): PQ identifier or list of identifiers in format '<system_name>:<serial>'
        wait (bool): When True (default), wait for the PQ to reach a terminal state using
                     the operator-configured wait duration
                     (``enterprise/settings.json: timeouts.client.pq_state_change_timeout_seconds``).
                     When False, fire-and-forget: submit the stop request and return
                     immediately without waiting.
        max_concurrent (int): Maximum concurrent stop operations (default: 20).
                              Must be a positive integer (> 0).

    Returns:
        dict: Response with per-item results:
        {
            "success": True,
            "results": [
                {
                    "pq_id": "prod:12345",
                    "serial": 12345,
                    "success": True,
                    "name": "analytics_worker",
                    "state": "STOPPED",
                    "error": None
                },
                {
                    "pq_id": "prod:67890",
                    "serial": 67890,
                    "success": False,
                    "name": None,
                    "state": None,
                    "error": "Timeout waiting for PQ to stop"
                }
            ],
            "summary": {"total": 2, "succeeded": 1, "failed": 1},
            "message": "Stopped 1 of 2 PQ(s), 1 failed"
        }

        dict: System error response (operation didn't execute):
        {
            "success": False,
            "error": "Invalid parameters",
            "isError": True
        }
    """
    if max_concurrent is None:
        max_concurrent = get_enterprise_settings(
            context
        ).pq_tools.default_max_concurrent
    _LOGGER.info(f"[mcp_systems_server:pq_stop] Invoked: pq_id={pq_id!r}, wait={wait}")

    result: dict[str, object] = {"success": False}

    try:
        # Common setup and validation for batch operations
        (
            parsed_pqs,
            controller,
            validated_max_concurrent,
            _system_name,
            setup_error,
        ) = await _setup_batch_pq_operation(context, pq_id, "pq_stop", max_concurrent)
        if setup_error:
            return setup_error

        # Type narrowing: when setup_error is None, all values are guaranteed non-None
        parsed_pqs = cast(list[tuple[str, CorePlusQuerySerial]], parsed_pqs)
        controller = cast(CorePlusControllerClient, controller)
        validated_max_concurrent = cast(int, validated_max_concurrent)

        # Process each PQ with controlled parallelism (best-effort)
        # Note: Controller stop_query() supports batch, but we process with parallel
        # individual calls to provide granular per-item success/failure reporting
        # for AI agents while maintaining performance
        _LOGGER.info(
            f"[mcp_systems_server:pq_stop] Processing {len(parsed_pqs)} PQ(s) "
            f"with max_concurrent={validated_max_concurrent}, wait={wait}"
        )

        async def stop_single_pq(
            pid: str, serial: CorePlusQuerySerial
        ) -> dict[str, object]:
            """Stop a single PQ and return result dict."""
            item_result: dict[str, object] = {
                "pq_id": pid,
                "serial": serial,
                "success": False,
                "name": None,
                "state": None,
                "error": None,
            }

            try:
                # Stop the PQ
                _LOGGER.debug(
                    f"[mcp_systems_server:pq_stop] Calling stop_query for PQ {pid} (wait={wait})"
                )
                await controller.stop_query([serial], wait=wait)
                _LOGGER.debug(
                    f"[mcp_systems_server:pq_stop] stop_query completed for PQ {pid}"
                )

                # Get updated info
                pq_info = await controller.get(serial)
                pq_name = pq_info.config.pb.name
                state_name = pq_info.state.status.name if pq_info.state else "UNKNOWN"

                # Success
                item_result["success"] = True
                item_result["name"] = pq_name
                item_result["state"] = state_name

                _LOGGER.debug(
                    f"[mcp_systems_server:pq_stop] Successfully stopped PQ {pid}"
                )

            except Exception as e:
                # Failure - record error
                item_result["error"] = (
                    f"{type(e).__name__}: {str(e) if str(e) else repr(e)}"
                )
                _LOGGER.warning(
                    f"[mcp_systems_server:pq_stop] Failed to stop PQ {pid}: {item_result['error']}"
                )

            return item_result

        # Use semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(validated_max_concurrent)

        async def stop_with_limit(
            pid: str, serial: CorePlusQuerySerial
        ) -> dict[str, object]:
            """Stop with concurrency limit."""
            async with semaphore:
                return await stop_single_pq(pid, serial)

        # Execute all stops in parallel with concurrency control
        # return_exceptions=True ensures one failure doesn't cancel other operations
        raw_results = await asyncio.gather(
            *[stop_with_limit(pid, serial) for pid, serial in parsed_pqs],
            return_exceptions=True,
        )

        # Handle any unexpected exceptions that weren't caught in the operation functions
        results: list[dict[str, object]] = []
        for i, r in enumerate(raw_results):
            if isinstance(r, Exception):
                # Unexpected exception - convert to error dict
                pid, serial = parsed_pqs[i]
                results.append(
                    {
                        "pq_id": pid,
                        "serial": serial,
                        "success": False,
                        "name": None,
                        "state": None,
                        "error": f"Unexpected error: {type(r).__name__}: {r}",
                    }
                )
            else:
                # Normal dict result from operation function
                results.append(cast(dict[str, object], r))

        # Calculate summary
        succeeded = sum(1 for r in results if r["success"])
        failed = len(results) - succeeded

        # Build message
        if failed == 0:
            message = f"Stopped {succeeded} PQ(s)"
        else:
            message = f"Stopped {succeeded} of {len(results)} PQ(s), {failed} failed"

        _LOGGER.info(
            f"[mcp_systems_server:pq_stop] {message}: "
            f"succeeded={[r['pq_id'] for r in results if r['success']]}, "
            f"failed={[r['pq_id'] for r in results if not r['success']]}"
        )

        # Always return consistent format
        result.update(
            {
                "success": True,
                "results": results,
                "summary": {
                    "total": len(results),
                    "succeeded": succeeded,
                    "failed": failed,
                },
                "message": message,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_stop] Failed to stop PQ: {e!r}",
            exc_info=True,
        )
        result["error"] = f"Failed to stop PQ(s): {type(e).__name__}: {e}"
        result["isError"] = True

    return result


async def pq_restart(
    context: Context,
    pq_id: str | list[str],
    wait: bool = True,
    max_concurrent: Annotated[int | None, Field(gt=0)] = None,
) -> dict:
    """MCP Tool: Restart one or more persistent queries.

    Restarts PQs using their original configurations, stopping them first if currently running.
    More efficient than delete + recreate for the same configuration.

    **Batch Support**: This operation supports batch execution for efficiency.
    Pass a single pq_id string or a list of pq_id strings.

    **Best-Effort Execution**: Each PQ is restarted independently. If some restarts fail,
    successful restarts are still completed and reported. Check individual item success
    status in the results.

    **Important**: All pq_ids must be from the same enterprise system - mixing systems returns error.

    **Important**: When ``wait=True`` (default), the timeout duration is operator-controlled
    via ``enterprise/settings.json: timeouts.client.pq_state_change_timeout_seconds``. If reached,
    the call returns failure BUT the PQ keeps restarting in background. Use pq_details to
    check current state after failures. Set ``wait=False`` for fire-and-forget.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'PQ' is shorthand for Persistent Query
    - Persistent Queries are recipes that create and manage worker sessions
    - A running PQ creates a session that can be connected to
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use pq_id from pq_list to identify PQs
    - If you only have PQ names, use pq_name_to_id to look up the pq_ids first
    - Best-effort: partial success is possible, check summary and individual results
    - Each result item has same fields: pq_id, serial, success, name, state, state_category, session_id, error
    - If success=True: name, state, and state_category have values; session_id is populated
      when state is RUNNING, EXECUTING, or INITIALIZING and omitted otherwise; error is None
    - If success=False: name/state/state_category/session_id are None, error has message
    - Works for stopped, failed, or completed PQs
    - Preserves PQ serial numbers and configurations
    - More efficient than deleting and recreating
    - state_category == "TRANSITIONAL" (e.g. state CONNECTING or INITIALIZING) is a valid
      success outcome when ``wait=False`` or when the operator-configured wait was short —
      success=True means the restart was accepted and the PQ was last seen in a non-failed
      state, NOT that it is fully RUNNING; use pq_details to confirm RUNNING if needed.
    - Branch on `state_category == "ACTIVE"` rather than `state == "RUNNING"`

    Args:
        context (Context): MCP context object
        pq_id (str | list[str]): PQ identifier or list of identifiers in format '<system_name>:<serial>'
        wait (bool): When True (default), wait for the PQ to reach RUNNING using the
                     operator-configured wait duration
                     (``enterprise/settings.json: timeouts.client.pq_state_change_timeout_seconds``).
                     When False, fire-and-forget: submit the restart request and return
                     immediately without waiting.
        max_concurrent (int): Maximum concurrent restart operations (default: 20).
                              Must be a positive integer (> 0).

    Returns:
        dict: Response with per-item results:
        {
            "success": True,
            "results": [
                {
                    "pq_id": "prod:12345",
                    "serial": 12345,
                    "success": True,
                    "name": "analytics_worker",
                    "state": "CONNECTING",          # may be RUNNING if fully started within timeout
                    "state_category": "TRANSITIONAL",  # ACTIVE | TRANSITIONAL | TERMINAL | INVALID
                    "session_id": None,             # present only when RUNNING, EXECUTING, or INITIALIZING
                    "error": None
                },
                {
                    "pq_id": "prod:67890",
                    "serial": 67890,
                    "success": False,
                    "name": None,
                    "state": None,
                    "state_category": None,
                    "session_id": None,
                    "error": "Timeout waiting for PQ to restart"
                }
            ],
            "summary": {"total": 2, "succeeded": 1, "failed": 1},
            "message": "Restarted 1 of 2 PQ(s), 1 failed"
        }

        dict: System error response (operation didn't execute):
        {
            "success": False,
            "error": "Invalid parameters",
            "isError": True
        }
    """
    if max_concurrent is None:
        max_concurrent = get_enterprise_settings(
            context
        ).pq_tools.default_max_concurrent
    _LOGGER.info(
        f"[mcp_systems_server:pq_restart] Invoked: pq_id={pq_id!r}, wait={wait}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Common setup and validation for batch operations
        (
            parsed_pqs,
            controller,
            validated_max_concurrent,
            system_name,
            setup_error,
        ) = await _setup_batch_pq_operation(
            context, pq_id, "pq_restart", max_concurrent
        )
        if setup_error:
            return setup_error

        # Type narrowing: when setup_error is None, all values are guaranteed non-None
        parsed_pqs = cast(list[tuple[str, CorePlusQuerySerial]], parsed_pqs)
        controller = cast(CorePlusControllerClient, controller)
        validated_max_concurrent = cast(int, validated_max_concurrent)

        # Process each PQ with controlled parallelism (best-effort)
        # Note: Controller restart_query() supports batch, but we process with parallel
        # individual calls to provide granular per-item success/failure reporting
        # for AI agents while maintaining performance
        _LOGGER.info(
            f"[mcp_systems_server:pq_restart] Processing {len(parsed_pqs)} PQ(s) "
            f"with max_concurrent={validated_max_concurrent}, wait={wait}"
        )

        async def restart_single_pq(
            pid: str, serial: CorePlusQuerySerial
        ) -> dict[str, object]:
            """Restart a single PQ and return result dict."""
            item_result: dict[str, object] = {
                "pq_id": pid,
                "serial": serial,
                "success": False,
                "name": None,
                "state": None,
                "state_category": None,
                "session_id": None,
                "error": None,
            }

            try:
                # Restart the PQ (and wait if requested)
                _LOGGER.debug(
                    f"[mcp_systems_server:pq_restart] Calling restart_query for PQ {pid} (wait={wait})"
                )
                await controller.restart_query([serial], wait=wait)
                _LOGGER.debug(
                    f"[mcp_systems_server:pq_restart] restart_query completed for PQ {pid}"
                )

                # Get updated info
                pq_info = await controller.get(serial)
                pq_name = pq_info.config.pb.name
                status_obj = pq_info.state.status if pq_info.state else None
                state_name = status_obj.name if status_obj is not None else "UNKNOWN"

                # Success
                item_result["success"] = True
                item_result["name"] = pq_name
                item_result["state"] = state_name
                item_result["state_category"] = _pq_state_category(state_name)

                # Add session_id if running (session_id uses name, not serial)
                _add_session_id_if_running(
                    item_result, status_obj, pq_name, system_name
                )

                _LOGGER.debug(
                    f"[mcp_systems_server:pq_restart] Successfully restarted PQ {pid}"
                )

            except Exception as e:
                # Failure - record error
                item_result["error"] = (
                    f"{type(e).__name__}: {str(e) if str(e) else repr(e)}"
                )
                _LOGGER.warning(
                    f"[mcp_systems_server:pq_restart] Failed to restart PQ {pid}: {item_result['error']}"
                )

            return item_result

        # Use semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(validated_max_concurrent)

        async def restart_with_limit(
            pid: str, serial: CorePlusQuerySerial
        ) -> dict[str, object]:
            """Restart with concurrency limit."""
            async with semaphore:
                return await restart_single_pq(pid, serial)

        # Execute all restarts in parallel with concurrency control
        # return_exceptions=True ensures one failure doesn't cancel other operations
        raw_results = await asyncio.gather(
            *[restart_with_limit(pid, serial) for pid, serial in parsed_pqs],
            return_exceptions=True,
        )

        # Handle any unexpected exceptions that weren't caught in the operation functions
        results: list[dict[str, object]] = []
        for i, r in enumerate(raw_results):
            if isinstance(r, Exception):
                # Unexpected exception - convert to error dict
                pid, serial = parsed_pqs[i]
                results.append(
                    {
                        "pq_id": pid,
                        "serial": serial,
                        "success": False,
                        "name": None,
                        "state": None,
                        "state_category": None,
                        "session_id": None,
                        "error": f"Unexpected error: {type(r).__name__}: {r}",
                    }
                )
            else:
                # Normal dict result from operation function
                results.append(cast(dict[str, object], r))

        # Calculate summary
        succeeded = sum(1 for r in results if r["success"])
        failed = len(results) - succeeded

        # Build message
        if failed == 0:
            message = f"Restarted {succeeded} PQ(s)"
        else:
            message = f"Restarted {succeeded} of {len(results)} PQ(s), {failed} failed"

        _LOGGER.info(
            f"[mcp_systems_server:pq_restart] {message}: "
            f"succeeded={[r['pq_id'] for r in results if r['success']]}, "
            f"failed={[r['pq_id'] for r in results if not r['success']]}"
        )

        # Always return consistent format
        result.update(
            {
                "success": True,
                "results": results,
                "summary": {
                    "total": len(results),
                    "succeeded": succeeded,
                    "failed": failed,
                },
                "message": message,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:pq_restart] Failed to restart PQ: {e!r}",
            exc_info=True,
        )
        result["error"] = f"Failed to restart PQ(s): {type(e).__name__}: {e}"
        result["isError"] = True

    return result


def register_tools(server: FastMCP) -> None:
    """Register all persistent query (PQ) tools with the given FastMCP server.

    These tools are specific to the DHE server and should NOT be registered
    on the DHC server.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(pq_name_to_id)
    server.tool()(pq_list)
    server.tool()(pq_details)
    server.tool()(pq_create)
    server.tool()(pq_delete)
    server.tool()(pq_modify)
    server.tool()(pq_start)
    server.tool()(pq_stop)
    server.tool()(pq_restart)
