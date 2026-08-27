"""Shared utilities — internal helper functions for the multiplexed systems-server tools.

Provides common helpers used across the MCP tool modules:

- Lifespan-context accessors:
  :func:`get_lifespan_context`, :func:`get_registry`,
  :func:`get_multi_config`, :func:`get_community_settings`,
  :func:`get_enterprise_settings`, :func:`get_community_registry`,
  :func:`get_enterprise_registry`.
- ID parsers and formatters: :func:`parse_pq_id`, :func:`make_pq_id`
  (use :meth:`QualifiedSessionId.from_str` directly for session ids).
- Session retrieval: :func:`get_session_from_context`,
  :func:`get_enterprise_session`, :func:`get_system_session`.
- Response helpers: :func:`error_response`, :func:`check_response_size`,
  :func:`build_table_data_response`, :func:`format_schema_result`.
- Parameter guards: :func:`validate_programming_language`.
- Partial-result formatting: :func:`format_partial_result`.
- JSON redaction: :func:`redact_json_sensitive_fields`.

This module is internal — none of its functions are MCP tools.

Per-request authentication has been removed: every tool reads its
already-validated configuration and credentials from the lifespan
context produced by
:func:`deephaven_mcp.mcp_systems_server._lifespan.make_lifespan`.
"""

from __future__ import annotations

import json
import logging
from typing import NamedTuple, assert_never

import pyarrow
from mcp.server.fastmcp import Context

from deephaven_mcp._exceptions import (
    CommunityNotConfiguredError,
    EnterpriseNotConfiguredError,
    InternalError,
    InvalidSessionNameError,
    SessionCreationError,
    UnsupportedOperationError,
)
from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.config.schema import (
    CommunitySettings,
    EnterpriseSettings,
    ResponseLimits,
)
from deephaven_mcp.config.tree import ConfigTree
from deephaven_mcp.formatters import TableFormat, format_table_data
from deephaven_mcp.mcp_systems_server._lifespan import LifespanContext
from deephaven_mcp.resource_manager import (
    CommunitySessionRegistry,
    EnterpriseSessionRegistry,
    InitializationPhase,
    MultiSystemRegistry,
    QualifiedSessionId,
    SessionId,
    SystemType,
)
from deephaven_mcp.sessions import VALID_PROGRAMMING_LANGUAGES

__all__ = [
    "ParsedPqId",
    "SystemAccess",
    "build_table_data_response",
    "check_response_size",
    "check_session_limit",
    "error_response",
    "format_partial_result",
    "format_schema_result",
    "get_community_registry",
    "get_community_settings",
    "get_enterprise_registry",
    "get_enterprise_session",
    "get_enterprise_settings",
    "get_lifespan_context",
    "get_multi_config",
    "get_registry",
    "get_response_limits",
    "get_session_from_context",
    "get_wcd_system_session",
    "make_pq_id",
    "parse_pq_id",
    "redact_json_sensitive_fields",
    "resolve_pq_ids_to_single_system",
    "validate_programming_language",
]

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Standard response shapes
# ---------------------------------------------------------------------------


def error_response(msg: str) -> dict[str, object]:
    """Return a standard MCP tool error response dict.

    Args:
        msg (str): Human-readable error message.

    Returns:
        dict[str, object]: ``{"success": False, "error": msg, "isError": True}``.
    """
    return {"success": False, "error": msg, "isError": True}


def validate_programming_language(programming_language: str, tool_name: str) -> None:
    """Reject a ``programming_language`` value outside the closed vocabulary.

    Runtime guard for untyped callers of the session-creation tools;
    typed callers are constrained by
    :data:`deephaven_mcp.sessions.ProgrammingLanguage`, and MCP traffic
    is validated against the advertised schema before it reaches the
    tool body.

    Args:
        programming_language (str): Candidate value to check against
            the exact-case vocabulary ("Python" or "Groovy").
        tool_name (str): Tool name used in the log prefix
            (e.g. ``"session_community_create"``).

    Raises:
        SessionCreationError: If ``programming_language`` is not
            "Python" or "Groovy".
    """
    if programming_language not in VALID_PROGRAMMING_LANGUAGES:
        valid_options = ", ".join(f"'{v}'" for v in sorted(VALID_PROGRAMMING_LANGUAGES))
        error_msg = f"Invalid programming_language '{programming_language}'. Valid options: {valid_options}."
        _LOGGER.error(f"[mcp_systems_server:{tool_name}] {error_msg}")
        raise SessionCreationError(error_msg)


def format_partial_result(
    phase: InitializationPhase,
    init_errors: dict[str, str],
) -> dict[str, object] | None:
    """Describe an incomplete result for a discovery-spanning tool.

    Tools whose result spans enterprise systems (``sessions_list``,
    ``enterprise_systems_status``) attach the returned block to their
    ``success=True`` payload when the result may be missing data. Pure
    formatting — callers obtain ``phase`` and ``init_errors`` from the same
    atomic snapshot (e.g. via :meth:`MultiSystemRegistry.get_all`).

    Args:
        phase (InitializationPhase): Current enterprise-discovery phase.
        init_errors (dict[str, str]): Factory name → error description.

    Returns:
        dict[str, object] | None: ``None`` when the result is complete (``phase``
            is ``COMPLETED`` with no errors). Otherwise a ``partial_result``
            block: ``phase`` (the machine-readable :class:`InitializationPhase`
            value, e.g. ``"loading"``), ``detail`` (a human-readable message),
            and ``errors`` (factory name → message, only when present).
    """
    if phase == InitializationPhase.COMPLETED and not init_errors:
        return None
    match phase:
        case InitializationPhase.FAILED:
            detail = (
                "Enterprise session discovery failed critically (e.g. canceled "
                "during shutdown). The registry may have partial or no data."
            )
        case InitializationPhase.NOT_STARTED | InitializationPhase.PARTIAL:
            detail = (
                "Enterprise session discovery has not yet started. Some sessions "
                "or systems may not yet be visible."
            )
        case InitializationPhase.LOADING:
            detail = (
                "Enterprise session discovery is actively running. Some sessions "
                "or systems may not yet be visible."
            )
        case InitializationPhase.COMPLETED:
            detail = "Some enterprise systems had connection issues during discovery."
        case _ as unexpected:
            assert_never(unexpected)
    block: dict[str, object] = {"phase": phase.value, "detail": detail}
    if init_errors:
        block["errors"] = init_errors
    return block


# ---------------------------------------------------------------------------
# Lifespan-context accessors
# ---------------------------------------------------------------------------


def get_lifespan_context(context: Context) -> LifespanContext:
    """Return the :class:`LifespanContext` attached to the MCP request.

    Args:
        context (Context): The MCP context object.

    Returns:
        LifespanContext: The frozen dataclass yielded by
            :func:`deephaven_mcp.mcp_systems_server._lifespan.make_lifespan`.
    """
    return context.request_context.lifespan_context  # type: ignore[no-any-return]


def get_registry(context: Context) -> MultiSystemRegistry:
    """Return the multi-system registry for this server process.

    Args:
        context (Context): The MCP context object.

    Returns:
        MultiSystemRegistry: The composite registry that fans out to
            community and enterprise child registries. Per-request
            authentication has been removed; the registry was wired
            during startup with credentials read from the configuration
            tree.
    """
    return get_lifespan_context(context).registry


def get_multi_config(context: Context) -> ConfigTree:
    """Return the validated multi-system configuration loaded at startup.

    Args:
        context (Context): The MCP context object.

    Returns:
        ConfigTree: The same dataclass returned by
            :meth:`ConfigTreeLoader.get_config` during the
            lifespan startup.
    """
    return get_lifespan_context(context).multi_config


def get_enterprise_settings(context: Context) -> EnterpriseSettings:
    """Return the validated enterprise settings model.

    Args:
        context (Context): The MCP context object.

    Returns:
        EnterpriseSettings: The validated ``enterprise/settings.json``
            model (timeouts, evictor knobs, ``pq_tools``, ...).

    Raises:
        EnterpriseNotConfiguredError: If no Enterprise system is
            configured (``multi_config.enterprise is None``). Enterprise
            tools register unconditionally, so this is a foreseeable
            user-correctable condition (configure an Enterprise system),
            surfaced as a clean error rather than an internal one.
    """
    enterprise = get_multi_config(context).enterprise
    if enterprise is None:
        raise EnterpriseNotConfiguredError(
            "No Enterprise (Core+) system is configured on this server."
        )
    return enterprise.settings


def get_community_settings(context: Context) -> CommunitySettings:
    """Return the validated community settings model.

    Args:
        context (Context): The MCP context object.

    Returns:
        CommunitySettings: The validated ``community/settings.json``
            model (security, session creation defaults, ...).

    Raises:
        CommunityNotConfiguredError: If no Community sessions are
            configured (``multi_config.community is None``). Community
            tools register unconditionally, so this is a foreseeable
            user-correctable condition (configure a Community session),
            surfaced as a clean error rather than an internal one.
    """
    community = get_multi_config(context).community
    if community is None:
        raise CommunityNotConfiguredError(
            "No Community sessions are configured on this server."
        )
    return community.settings


def get_community_registry(context: Context) -> CommunitySessionRegistry:
    """Return the community child registry, or raise when absent.

    Args:
        context (Context): The MCP context object.

    Returns:
        CommunitySessionRegistry: The community child of the
            multi-system registry.

    Raises:
        CommunityNotConfiguredError: If no Community sessions are
            configured. Community tools register unconditionally, so this
            is a foreseeable user-correctable condition surfaced as a
            clean error rather than an internal one.
    """
    registry = get_registry(context).community
    if registry is None:
        raise CommunityNotConfiguredError(
            "No Community sessions are configured on this server."
        )
    return registry


def get_enterprise_registry(context: Context, system: str) -> EnterpriseSessionRegistry:
    """Return the enterprise child registry for ``system``.

    Args:
        context (Context): The MCP context object.
        system (str): Enterprise system name (the ``system_name`` field
            in the system's config file, equivalent to its filename
            stem).

    Returns:
        EnterpriseSessionRegistry: The configured enterprise registry.

    Raises:
        InvalidSessionNameError: If ``system`` is not a configured
            enterprise system. The exception lists the configured
            systems so tools can surface a useful error to the caller.
    """
    enterprise = get_registry(context).enterprise_systems
    registry = enterprise.get(system)
    if registry is None:
        known = sorted(enterprise.keys())
        raise InvalidSessionNameError(
            f"Enterprise system {system!r} is not configured. Known "
            f"enterprise systems: {known}."
        )
    return registry


# ---------------------------------------------------------------------------
# Identifier parsers
# ---------------------------------------------------------------------------


class ParsedPqId(NamedTuple):
    """Structured result of :func:`parse_pq_id`.

    Backward compatibility: unpacks positionally as a 2-tuple
    ``(system_name, serial)``.

    Attributes:
        system_name (str): Enterprise system that owns the PQ.
        serial (int): Non-negative integer serial assigned by the
            controller.
    """

    system_name: str
    serial: int


def parse_pq_id(pq_id: str) -> ParsedPqId:
    """Parse an enterprise persistent-query identifier.

    A persistent-query identifier is the fully qualified session id of
    the PQ: ``"enterprise:<system_name>:<serial>"`` where ``serial`` is
    a non-negative integer assigned by the controller. It is the same
    string the session tools report for a running PQ, so ids can be
    passed between the ``pq`` and ``session`` tool families verbatim.

    Args:
        pq_id (str): The persistent-query identifier to parse.

    Returns:
        ParsedPqId: ``(system_name, serial)``. Also unpacks as a
            plain 2-tuple for callers using positional unpacking.

    Raises:
        InvalidSessionNameError: If ``pq_id`` is not a valid fully
            qualified session id, if its type segment is not
            ``"enterprise"``, or if the trailing segment is not a
            non-negative integer serial.
    """
    qsid = QualifiedSessionId.from_str(pq_id)
    if qsid.system_type is not SystemType.ENTERPRISE:
        raise InvalidSessionNameError(
            f"Persistent-query id {pq_id!r} must be enterprise-scoped "
            f"('enterprise:<system_name>:<serial>'); got type "
            f"{qsid.system_type.value!r}."
        )
    try:
        serial = int(qsid.session_id)
    except ValueError as exc:
        raise InvalidSessionNameError(
            f"Persistent-query id {pq_id!r} has non-integer serial "
            f"{str(qsid.session_id)!r}; expected a non-negative integer."
        ) from exc
    return ParsedPqId(system_name=qsid.system_name, serial=serial)


def make_pq_id(system_name: str, serial: int) -> str:
    """Format a persistent-query identifier from its components.

    The inverse of :func:`parse_pq_id`: renders the fully qualified
    session id ``"enterprise:<system_name>:<serial>"`` for the PQ.

    Args:
        system_name (str): Enterprise system that owns the PQ.
        serial (int): Non-negative integer serial assigned by the
            controller.

    Returns:
        str: The identifier, e.g. ``"enterprise:prod:12345"``.

    Raises:
        InvalidSessionNameError: If ``system_name`` is not a valid
            resource name or ``serial`` is negative.
    """
    return str(
        QualifiedSessionId(
            SystemType.ENTERPRISE, system_name, SessionId.from_int(serial)
        )
    )


def resolve_pq_ids_to_single_system(
    pq_ids: list[str],
) -> tuple[str, list[int]]:
    """Parse a batch of PQ ids and require all of them to share one system.

    Persistent-query batch tools (``pq_start``, ``pq_stop``,
    ``pq_restart``, ``pq_delete``) operate on a single enterprise
    system per call. This helper centralizes the parse-and-uniformity
    check so each batch tool can route to a single child registry.

    Args:
        pq_ids (list[str]): One or more persistent-query identifiers,
            each of the form ``"enterprise:<system_name>:<serial>"``.

    Returns:
        tuple[str, list[int]]: ``(system_name, serials)`` where every
            id parsed cleanly and all of them named the same system.
            ``serials`` is positionally aligned with ``pq_ids``.

    Raises:
        InvalidSessionNameError: If ``pq_ids`` is empty, if any id
            fails :func:`parse_pq_id`, or if the ids name more than
            one enterprise system.
    """
    if not pq_ids:
        raise InvalidSessionNameError("At least one persistent-query id is required.")
    parsed = [parse_pq_id(pq_id) for pq_id in pq_ids]
    systems = {p.system_name for p in parsed}
    if len(systems) > 1:
        raise InvalidSessionNameError(
            "All persistent-query ids in a batch must name the same "
            f"enterprise system; got {sorted(systems)!r}."
        )
    (system_name,) = systems
    return system_name, [p.serial for p in parsed]


# ---------------------------------------------------------------------------
# Session retrieval
# ---------------------------------------------------------------------------


class SystemAccess(NamedTuple):
    """A system's shared WebClientData session and the identity it acts as."""

    session: CorePlusSession
    """Live session on the system's ``WebClientData`` persistent query."""

    operate_as: str
    """Identity whose ACLs per-user widget tables are built with."""


async def get_session_from_context(
    function_name: str, context: Context, session_id: str
) -> BaseSession:
    """Get an active session from the MCP context.

    Looks the session up by routing through
    :class:`MultiSystemRegistry`. Community sessions
    (``community:*:*``) and enterprise sessions
    (``enterprise:<system>:*``) are both supported in a single call.

    Args:
        function_name (str): Name of calling function for logging
            purposes.
        context (Context): The MCP context object containing the
            lifespan context.
        session_id (str): Fully qualified session id (e.g.
            ``"community:community:local"`` or
            ``"enterprise:prod:my-pq"``).

    Returns:
        BaseSession: The active session connection.

    Raises:
        InternalError: If the registry is missing from the lifespan
            context.
        InvalidSessionNameError: If ``session_id`` does not route to a
            configured child registry.
        RegistryItemNotFoundError: If ``session_id`` is not present in
            the routed registry.
        Exception: Any other exception raised by the registry lookup or
            the session connection propagates unchanged.
    """
    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Accessing session registry "
        f"from context"
    )
    registry = get_registry(context)

    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Retrieving session manager "
        f"for '{session_id}'"
    )
    session_manager = await registry.get(QualifiedSessionId.from_str(session_id))

    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Establishing session "
        f"connection for '{session_id}'"
    )
    session: BaseSession = await session_manager.get()

    _LOGGER.info(
        f"[mcp_systems_server:{function_name}] Session established for "
        f"'{session_id}'"
    )
    return session


async def get_enterprise_session(
    function_name: str, context: Context, session_id: str
) -> CorePlusSession:
    """Get and validate an enterprise (Core+) session from context.

    Args:
        function_name (str): Name of calling function for logging and
            error messages.
        context (Context): The MCP context object.
        session_id (str): Session id (e.g. ``"enterprise:prod:analytics"``).

    Returns:
        CorePlusSession: The active enterprise session connection.

    Raises:
        UnsupportedOperationError: If ``session_id`` resolves to a
            session that is not an enterprise (Core+) session.
        InternalError: If the registry is missing from the lifespan
            context.
        InvalidSessionNameError: If ``session_id`` does not route to a
            configured child registry.
        RegistryItemNotFoundError: If ``session_id`` is not present in
            the routed registry.
        Exception: Any other exception raised by the registry lookup or
            the session connection propagates unchanged.
    """
    session = await get_session_from_context(function_name, context, session_id)

    if not isinstance(session, CorePlusSession):
        error_msg = (
            f"{function_name} only works with enterprise (Core+) sessions, "
            f"but session '{session_id}' is {type(session).__name__}"
        )
        _LOGGER.error(f"[mcp_systems_server:{function_name}] {error_msg}")
        raise UnsupportedOperationError(error_msg)

    return session


async def get_wcd_system_session(
    function_name: str, context: Context, system: str
) -> SystemAccess:
    """Get the shared WebClientData session and operate-as identity for a system.

    Routes to the system's :class:`EnterpriseSessionRegistry` and returns its
    cached ``WebClientData`` session together with the identity that session
    authenticated as. Use this for system-scoped reads served by the
    ``WebClientData`` table-factory widget — the widget builds each table for
    a named user, so both halves are needed.

    Args:
        function_name (str): Name of calling function for logging.
        context (Context): The MCP context object.
        system (str): Enterprise system name (the ``system_name`` field in
            the system's config file).

    Returns:
        SystemAccess: The live ``WebClientData`` session and the operate-as
            identity to name in widget requests.

    Raises:
        InvalidSessionNameError: If ``system`` is not a configured
            enterprise system.
        InternalError: If the registry is missing from the lifespan context,
            or the controller reports no effective user.
        Exception: Any exception raised while connecting to
            ``WebClientData`` propagates unchanged; a system where that
            persistent query is not running cannot serve these reads.
    """
    _LOGGER.debug(
        f"[mcp_systems_server:{function_name}] Acquiring WebClientData session "
        f"for system '{system}'"
    )
    registry = get_enterprise_registry(context, system)
    session = await registry.web_client_data_session()
    operate_as = await registry.effective_user()
    _LOGGER.info(
        f"[mcp_systems_server:{function_name}] WebClientData session established "
        f"for system '{system}' as user '{operate_as}'"
    )
    return SystemAccess(session=session, operate_as=operate_as)


# ---------------------------------------------------------------------------
# Response size guards
# ---------------------------------------------------------------------------


def get_response_limits(context: Context, session_id: str) -> ResponseLimits:
    """Return the validated :class:`ResponseLimits` for ``session_id``'s system.

    Routes to ``multi_config.enterprise.settings.response_limits`` for
    enterprise sessions and ``multi_config.community.settings.response_limits``
    for community sessions, parsing ``session_id`` to determine which.

    Args:
        context (Context): The MCP context object.
        session_id (str): Fully qualified session id used to determine
            which section's limits apply.

    Returns:
        ResponseLimits: The validated per-system response-limit block.

    Raises:
        InvalidSessionNameError: If ``session_id`` does not parse as a
            fully qualified session identifier.
        InternalError: If the section that owns ``session_id`` has no
            configuration loaded (a tool was registered without its
            underlying configuration block), or if ``qsid.system_type``
            is not a known :class:`SystemType` member (indicates the
            enum gained a value the router has not been taught about).
    """
    qsid = QualifiedSessionId.from_str(session_id)
    if qsid.system_type is SystemType.ENTERPRISE:
        return get_enterprise_settings(context).response_limits
    if qsid.system_type is SystemType.COMMUNITY:
        return get_community_settings(context).response_limits
    raise InternalError(
        f"[get_response_limits] Unhandled SystemType {qsid.system_type!r}; "
        f"this router must be extended whenever SystemType gains a member."
    )


def check_response_size(
    table_name: str, estimated_size: int, limits: ResponseLimits
) -> dict | None:
    """Check if estimated response size is within acceptable limits.

    Args:
        table_name (str): Name of the table being processed.
        estimated_size (int): Estimated response size in bytes.
        limits (ResponseLimits): Operator-tunable thresholds (read
            from the per-section ``response_limits`` config block).

    Returns:
        dict | None: ``None`` if the size is acceptable; otherwise a
            structured error dict produced by :func:`error_response`.

    Side Effects:
        Logs a warning when ``estimated_size`` exceeds
        ``limits.warning_response_bytes`` even if the response is
        allowed.
    """
    if estimated_size > limits.warning_response_bytes:
        _LOGGER.warning(
            f"[mcp_systems_server:check_response_size] Large response "
            f"(~{estimated_size/1_000_000:.1f}MB) for table '{table_name}'. "
            "Consider reducing max_rows for better performance."
        )

    if estimated_size > limits.max_response_bytes:
        return error_response(
            f"Response would be ~{estimated_size/1_000_000:.1f}MB "
            f"(max {limits.max_response_bytes/1_000_000:.0f}MB). "
            "Please reduce max_rows."
        )

    return None


# ---------------------------------------------------------------------------
# Session-limit guard
# ---------------------------------------------------------------------------


async def check_session_limit(
    session_registry: CommunitySessionRegistry | EnterpriseSessionRegistry,
    system_type: SystemType,
    system_name: str,
    max_sessions: int | None,
    function_name: str,
    error_message_template: str,
) -> dict | None:
    """Check whether ``system_name`` is under its concurrent-session cap.

    Counts dynamically added sessions for ``(system_type, system_name)``
    via :meth:`count_added_sessions` and compares the count against
    ``max_sessions``. A cap of ``None`` disables the check.

    Args:
        session_registry (CommunitySessionRegistry | EnterpriseSessionRegistry):
            The child registry that owns the session bucket being counted.
        system_type (SystemType): Owning system type (community or
            enterprise) used to filter ``_added_session_ids``.
        system_name (str): Owning system name used to filter
            ``_added_session_ids``.
        max_sessions (int | None): Maximum concurrent dynamically added
            sessions allowed. ``None`` disables the cap (unbounded).
        function_name (str): Calling tool name used in the log prefix.
        error_message_template (str): Format string substituted with
            ``current`` and ``max`` keyword arguments when the cap is
            reached (e.g. ``"Session limit reached: {current}/{max}"``).
            The formatted message is logged at warning level and
            returned in the error response.

    Returns:
        dict | None: ``None`` when the cap is disabled or the count is
            below ``max_sessions``; otherwise a structured error dict
            produced by :func:`error_response`.
    """
    if max_sessions is None:
        return None
    current_count = await session_registry.count_added_sessions(
        system_type, system_name
    )
    if current_count >= max_sessions:
        error_message = error_message_template.format(
            current=current_count, max=max_sessions
        )
        _LOGGER.warning(f"[mcp_systems_server:{function_name}] {error_message}")
        return error_response(error_message)
    return None


# ---------------------------------------------------------------------------
# Table response shapers
# ---------------------------------------------------------------------------


def format_schema_result(
    arrow_meta_table: pyarrow.Table,
    id: str,
    table_name: str,
    namespace: str | None = None,
) -> dict:
    """Format a PyArrow meta table into a lean single-table schema result.

    A "meta table" in Deephaven is a table that describes another
    table's structure: each row represents one column from the original
    table.

    Each ``schema`` entry is a snake_case dict with ``name`` and ``type``
    always present, plus one sparse key carried only when meaningful:
    ``column_type`` (the meta table's ``ColumnType``, e.g. ``"Partitioning"``
    or ``"Grouping"``; omitted for ``"Normal"`` columns).

    Args:
        arrow_meta_table (pyarrow.Table): The PyArrow meta table.
        id (str): Fully qualified session id to echo back.
        table_name (str): Name of the table being described.
        namespace (str | None): Optional namespace for catalog tables.

    Returns:
        dict: ``{"success": True, "id": ..., "namespace": ... (if provided),
            "table_name": ..., "schema": [...], "column_count": ...}``. The
            ``type`` values are Deephaven type names from the meta table
            (e.g. ``"java.lang.String"``, ``"int"``), not PyArrow names.

    Raises:
        KeyError: If a meta-table row lacks the ``Name`` or ``DataType``
            column (malformed meta table); callers convert this to a
            structured error response.
    """
    meta_rows = arrow_meta_table.to_pylist()
    schema: list[dict[str, object]] = []
    for row in meta_rows:
        column: dict[str, object] = {
            "name": row["Name"],
            "type": row["DataType"],
        }
        column_type = row.get("ColumnType")
        if column_type and column_type != "Normal":
            column["column_type"] = column_type
        schema.append(column)
    result: dict[str, object] = {"success": True, "id": id}
    if namespace is not None:
        result["namespace"] = namespace
    result["table_name"] = table_name
    result["schema"] = schema
    result["column_count"] = len(meta_rows)
    return result


def build_table_data_response(
    arrow_table: pyarrow.Table,
    is_complete: bool,
    format: TableFormat,
    id: str,
    table_name: str | None = None,
    namespace: str | None = None,
) -> dict:
    """Build a standardized table-data response with schema and metadata.

    Args:
        arrow_table (pyarrow.Table): The Arrow table containing the
            data.
        is_complete (bool): Whether the entire table was retrieved
            (``False`` if truncated by ``max_rows``).
        format (TableFormat): Desired output format.
        id (str): Fully qualified session id to echo back.
        table_name (str | None): Optional table name to include.
        namespace (str | None): Optional namespace (for catalog tables).

    Returns:
        dict: ``{"success": True, "id": ..., "format": ..., "schema": [...],
            "row_count": ..., "is_complete": ..., "data": ...,
            "table_name": ... (optional), "namespace": ... (optional)}``.
    """
    schema = [
        {"name": field.name, "type": str(field.type)} for field in arrow_table.schema
    ]
    actual_format, formatted_data = format_table_data(arrow_table, format_type=format)
    response: dict[str, object] = {"success": True, "id": id}
    if namespace is not None:
        response["namespace"] = namespace
    if table_name is not None:
        response["table_name"] = table_name
    response["row_count"] = len(arrow_table)
    response["is_complete"] = is_complete
    response["format"] = actual_format
    response["schema"] = schema
    response["data"] = formatted_data
    return response


# ---------------------------------------------------------------------------
# JSON redaction
# ---------------------------------------------------------------------------


_SENSITIVE_JSON_KEYS: frozenset[str] = frozenset(
    {"password", "passwd", "token", "secret", "api_key", "apikey", "api_secret"}
)
"""JSON object keys whose values are redacted in nested-JSON tool output."""


def _redact_recursive(obj: object) -> object:
    """Recursively redact values of sensitive keys in a parsed JSON structure."""
    if isinstance(obj, dict):
        return {
            k: (REDACTED if k.lower() in _SENSITIVE_JSON_KEYS else _redact_recursive(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact_recursive(item) for item in obj]
    return obj


def redact_json_sensitive_fields(json_str: str | None) -> str | None:
    """Parse a JSON string and redact values whose keys match known-sensitive names.

    Args:
        json_str (str | None): The JSON string to scan, or ``None``.

    Returns:
        str | None: ``None`` for empty/``None`` input. ``"[UNPARSEABLE]"``
            (with a warning log) when the string is not valid JSON.
            Otherwise a re-serialized JSON string with sensitive values
            replaced by ``[REDACTED]``.
    """
    if not json_str:
        return None
    try:
        parsed = json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        _LOGGER.warning(
            "[mcp_systems_server:redact_json_sensitive_fields] type_specific "
            "JSON field is not valid JSON; content suppressed"
        )
        return "[UNPARSEABLE]"
    return json.dumps(_redact_recursive(parsed))
