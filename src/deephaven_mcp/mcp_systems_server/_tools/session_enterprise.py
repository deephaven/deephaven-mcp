"""Enterprise Session MCP Tools - Create and Manage Enterprise Sessions.

Provides MCP tools for managing Deephaven Enterprise (Core+) sessions:
- enterprise_systems_status: Get status of configured Enterprise systems
- session_enterprise_create: Create new Enterprise sessions on configured systems
- session_enterprise_delete: Delete Enterprise sessions

These tools require Deephaven Enterprise (Core+) and are not available in Community.
"""

import logging
from datetime import datetime
from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp._exceptions import InvalidSessionNameError, RegistryItemNotFoundError
from deephaven_mcp._pydantic import dump_redacted
from deephaven_mcp.auth.credentials import PasswordCredentials
from deephaven_mcp.client import CorePlusQueryConfig, CorePlusSession
from deephaven_mcp.mcp_systems_server._tools.shared import (
    check_session_limit,
    error_response,
    format_partial_result,
    get_enterprise_registry,
    get_multi_config,
    parse_session_id,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    EnterpriseSessionManager,
    EnterpriseSessionRegistry,
    InitializationPhase,
    SystemType,
    least_advanced_phase,
)
from deephaven_mcp.sessions import (
    EnterpriseSessionCreationDefaults,
    EnterpriseSystemConfig,
)

_LOGGER = logging.getLogger(__name__)


async def _collect_one_enterprise_system_status(
    context: Context, system: str, attempt_to_connect: bool
) -> tuple[dict[str, object], dict[str, str], InitializationPhase]:
    """Build the per-system status dict + per-system init state.

    Returns a 3-tuple ``(system_info, init_errors, init_phase)``. The
    caller (:func:`enterprise_systems_status`) merges these across all
    requested systems.
    """
    session_registry = get_enterprise_registry(context, system)
    multi_config = get_multi_config(context)
    snapshot = await session_registry.get_all()
    factory_manager = session_registry.factory_manager
    status_enum, liveness_detail = await factory_manager.liveness_status(
        ensure_item=attempt_to_connect
    )
    is_alive = await factory_manager.is_alive()

    if multi_config.enterprise is None or system not in multi_config.enterprise.systems:
        raise InvalidSessionNameError(
            f"Enterprise system {system!r} is not configured."
        )
    redacted_config = dump_redacted(multi_config.enterprise.systems[system])

    system_info: dict[str, object] = {
        "name": session_registry.system_name,
        "liveness_status": status_enum.name,
        "is_alive": is_alive,
        "config": redacted_config,
    }
    if liveness_detail is not None:
        system_info["liveness_detail"] = liveness_detail
    return system_info, snapshot.initialization_errors, snapshot.initialization_phase


async def enterprise_systems_status(
    context: Context,
    system: str | None = None,
    attempt_to_connect: bool = False,
) -> dict:
    """MCP Tool: Get the status and configuration details of this enterprise system.

    This tool provides comprehensive status information about the single enterprise system managed by
    this MCP server instance. It returns detailed health status using the ResourceLivenessStatus
    classification system, along with explanatory details and configuration information (with sensitive
    fields redacted for security).

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    The tool supports two operational modes:
    1. Default mode (attempt_to_connect=False): Quick status check of existing connections
       - Fast response time, minimal resource usage
       - Suitable for dashboards, monitoring, and non-critical status checks
       - Will report systems as OFFLINE if no connection exists

    2. Connection verification mode (attempt_to_connect=True): Active connection attempt
       - Attempts to establish connections to verify actual availability
       - Higher latency but more accurate status reporting
       - Suitable for troubleshooting and pre-flight checks before critical operations
       - May create new connections if none exist

    Status Classification:
      - "ONLINE": System is healthy and ready for operational use
      - "OFFLINE": System is unresponsive, failed health checks, or not connected
      - "UNAUTHORIZED": Authentication or authorization failures prevent access
      - "MISCONFIGURED": Configuration errors prevent proper system operation
      - "UNKNOWN": Unexpected errors occurred during status determination

    AI Agent Usage:
    - Use attempt_to_connect=False (default) for quick status checks
    - Use attempt_to_connect=True to actively verify system connectivity
    - Check 'systems' array in response for individual system status
    - Use each system's 'liveness_detail' field for troubleshooting connection issues
    - Configuration details are included but sensitive fields are redacted
    - If 'partial_result' is present, this report may be incomplete: check its 'phase'
      ('loading'/'partial' → discovery still running, retry later; 'failed' → report)
      and 'errors' for which enterprise systems could not be reached

    Args:
        context (Context): The MCP context object.
        system (str | None, optional): Single enterprise system name to
            report on. ``None`` (default) aggregates every configured
            enterprise system.
        attempt_to_connect (bool, optional): If True, actively attempts to connect to each system
            to verify its status. This provides more accurate results but increases latency.
            Default is False (only checks existing connections for faster response).

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if retrieval succeeded, False otherwise.
            - 'systems' (list[dict]): List of system info dicts. Each contains:
                - 'name' (str): System name identifier
                - 'liveness_status' (str): ResourceLivenessStatus ("ONLINE", "OFFLINE", "UNAUTHORIZED", "MISCONFIGURED", "UNKNOWN")
                - 'liveness_detail' (str, optional): Explanation message for the status, useful for troubleshooting
                - 'is_alive' (bool): Simple boolean indicating if the system is responsive
                - 'config' (dict): System configuration with sensitive fields redacted
            - 'partial_result' (dict, optional): Present only when this report may be
                incomplete — enterprise session discovery is still in progress or some
                systems failed. Contains:
                - 'phase' (str): Machine-readable discovery phase — one of 'not_started',
                    'partial', 'loading', 'completed', 'failed'. Use it to decide whether
                    to retry (still loading) or report (failed).
                - 'detail' (str): Human-readable description of why the report is partial.
                - 'errors' (dict[str, str], optional): Present when one or more enterprise systems
                    had connection errors during initial discovery. Keys are factory names, values
                    are error descriptions.
            - 'error' (str, optional): Error message if retrieval failed.
            - 'isError' (bool, optional): Present and True if this is an error response.

    Example Successful Response:
        {
            'success': True,
            'systems': [
                {
                    'name': 'prod-system',
                    'liveness_status': 'ONLINE',
                    'liveness_detail': 'Connection established successfully',
                    'is_alive': True,
                    'config': {'host': 'prod.example.com', 'port': 10000, 'auth_type': 'anonymous'}
                }
            ]
        }

    Example Partial Result (discovery still in progress):
        {
            'success': True,
            'systems': [
                {
                    'name': 'prod-system',
                    'liveness_status': 'ONLINE',
                    'liveness_detail': 'Connection established successfully',
                    'is_alive': True,
                    'config': {'host': 'prod.example.com', 'port': 10000, 'auth_type': 'anonymous'}
                }
            ],
            'partial_result': {
                'phase': 'loading',
                'detail': 'Enterprise session discovery is actively running. Some sessions or systems may not yet be visible.',
            },
        }

    Example Error Response:
        {'success': False, 'error': 'Failed to retrieve systems status', 'isError': True}

    Performance Considerations:
        - With attempt_to_connect=False: Typically completes in milliseconds
        - With attempt_to_connect=True: May take seconds due to connection operations
    """
    _LOGGER.info(
        f"[mcp_systems_server:enterprise_systems_status] Invoked: system={system!r}"
    )
    try:
        if system is None:
            # Aggregate every configured enterprise system.
            multi_config = get_multi_config(context)
            if multi_config.enterprise is None:
                return {"success": True, "systems": []}
            target_systems = list(multi_config.enterprise.systems.keys())
        else:
            target_systems = [system]

        systems_info: list[dict[str, object]] = []
        merged_errors: dict[str, str] = {}
        # The merged phase is the *least advanced* across the surveyed
        # systems so an aggregated call surfaces in-flight or failed
        # discovery rather than masking it behind a completed sibling.
        # Reuse the central fold from MultiSystemRegistry so this tool
        # and ``MultiSystemRegistry.get_all`` stay in lockstep — in
        # particular, FAILED outranks every other phase so a single
        # failed system always surfaces in the aggregated response.
        phases: list[InitializationPhase] = []
        for sys_name in target_systems:
            (
                system_info,
                init_errors,
                init_phase,
            ) = await _collect_one_enterprise_system_status(
                context, sys_name, attempt_to_connect
            )
            systems_info.append(system_info)
            # Single-system queries preserve the legacy un-namespaced
            # error keys; aggregated calls namespace with the system
            # name so duplicate source tags don't collide.
            if len(target_systems) == 1:
                merged_errors.update(init_errors)
            else:
                for source, err in init_errors.items():
                    merged_errors[f"{sys_name}:{source}"] = err
            phases.append(init_phase)

        merged_phase = least_advanced_phase(phases)
        response: dict[str, object] = {"success": True, "systems": systems_info}
        partial = format_partial_result(merged_phase, merged_errors)
        if partial:
            response["partial_result"] = partial
        return response
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:enterprise_systems_status] Failed: {e!r}",
            exc_info=True,
        )
        return error_response(str(e))


def _env_vars_to_list(env_vars: dict[str, str] | None) -> list[str] | None:
    """Convert an ``environment_vars`` mapping to the controller wire format.

    The Deephaven Enterprise controller accepts environment variables
    as a list of ``"NAME=value"`` strings; the MCP-side schema and
    tool surface use a more idiomatic ``dict[str, str]``. This helper
    is the call-site adapter at the controller boundary.

    Args:
        env_vars (dict[str, str] | None): Environment variables keyed
            by name. ``None`` is propagated through unchanged.

    Returns:
        list[str] | None: ``["NAME=value", ...]`` or ``None``.
    """
    if env_vars is None:
        return None
    return [f"{name}={value}" for name, value in env_vars.items()]


async def _check_session_limit(
    session_registry: EnterpriseSessionRegistry, max_sessions: int | None
) -> dict | None:
    """Check if enterprise session creation is within the configured cap.

    Args:
        session_registry (EnterpriseSessionRegistry): The enterprise child
            registry whose dynamically added sessions are being counted.
        max_sessions (int | None): Maximum concurrent dynamically added
            enterprise sessions allowed for this system. ``None``
            disables the cap (unbounded).

    Returns:
        dict | None: ``None`` if the cap is disabled or not yet reached;
            otherwise a structured error dict produced by
            :func:`error_response`.
    """
    system_name = session_registry.system_name
    return await check_session_limit(
        session_registry,
        SystemType.ENTERPRISE,
        system_name,
        max_sessions,
        "_check_session_limit",
        f"Max concurrent sessions ({{max}}) reached for system '{system_name}'",
    )


def _generate_session_name_if_none(
    system_config: EnterpriseSystemConfig, session_name: str | None
) -> str:
    """Generate a session name if none provided.

    Args:
        system_config (EnterpriseSystemConfig): The validated enterprise
            system declaration. When the configured credentials carry a
            ``username`` (i.e. password auth) it is embedded into the
            generated name to make logs easier to attribute.
        session_name (str | None): Provided session name or None

    Returns:
        str: Either the provided session_name or auto-generated name
    """
    if session_name is not None:
        return session_name

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    creds = system_config.credentials
    username = creds.username if isinstance(creds, PasswordCredentials) else None
    if username:
        generated = f"mcp-{username}-{timestamp}"
    else:
        generated = f"mcp-session-{timestamp}"

    _LOGGER.debug(
        f"[mcp_systems_server:_generate_session_name_if_none] Auto-generated session name: {generated}"
    )
    return generated


async def _check_session_id_available(
    session_registry: EnterpriseSessionRegistry, session_id: str
) -> dict | None:
    """Check if session ID is available (not already in use).

    Called during session creation to prevent duplicate session IDs.
    This ensures each session has a unique identifier in the registry.

    Args:
        session_registry (EnterpriseSessionRegistry): The session registry to check
        session_id (str): The session ID to check for availability

    Returns:
        dict | None: Error response dict if session exists, None if available
    """
    try:
        await session_registry.get(session_id)
        # If we got here, session already exists
        error_msg = f"Session '{session_id}' already exists"
        _LOGGER.error(f"[mcp_systems_server:_check_session_id_available] {error_msg}")
        return error_response(error_msg)
    except RegistryItemNotFoundError:
        return None  # Good - session doesn't exist yet


async def session_enterprise_create(
    context: Context,
    system: str,
    session_name: str | None = None,
    heap_size_gb: float | int | None = None,
    programming_language: str | None = None,
    auto_delete_timeout: int | None = None,
    server: str | None = None,
    engine: str | None = None,
    extra_jvm_args: list[str] | None = None,
    environment_vars: dict[str, str] | None = None,
    admin_groups: list[str] | None = None,
    viewer_groups: list[str] | None = None,
    session_arguments: dict[str, Any] | None = None,
) -> dict:
    """MCP Tool: Create a new enterprise session with configurable parameters.

    Creates a new enterprise session on the specified enterprise system and registers it in the
    session registry for future use. The session is configured using provided parameters or defaults
    from the enterprise system configuration.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    Parameter Resolution Priority (highest to lowest):
    1. Tool parameters provided in this function call
    2. Enterprise system session_creation defaults from configuration
    3. Deephaven server built-in defaults

    AI Agent Usage:
    - Use this tool only when you need to create a new session
    - Check 'success' field and use returned 'session_id' for subsequent operations
    - Sessions have resource limits and may auto-delete after timeout periods
    - Use session_enterprise_delete tool to clean up when done

    Args:
        context (Context): The MCP context object.
        system (str): Enterprise system name as listed by ``list_systems``
            on which to create the session.
        session_name (str | None): Name for the new session. If None, auto-generates
            a timestamp-based name like "mcp-{username}-20241126-1130".
        heap_size_gb (float | int | None): JVM heap size in gigabytes (e.g., 8 or 2.5 for -Xmx8g or -Xmx2.5g). If None, uses
            config default or Deephaven default.
        programming_language (str | None): Programming language for the session.
            Supported values: "Python" (default) or "Groovy". If None, uses config default or "Python".
        auto_delete_timeout (int | None): Seconds of inactivity before automatic session deletion.
            If None, uses config default or the Deephaven server built-in default.
        server (str | None): Specific server to run session on.
            If None, uses config default or lets Deephaven auto-select.
        engine (str | None): Engine type for the session.
            If None, uses config default or "DeephavenCommunity".
        extra_jvm_args (list[str] | None): Additional JVM arguments for the session.
            If None, uses config default or standard JVM settings.
        environment_vars (dict[str, str] | None): Environment variables for the session,
            keyed by variable name. If None, uses config default environment.
        admin_groups (list[str] | None): User groups with administrative permissions on the session.
            If None, uses config default or creator-only access.
        viewer_groups (list[str] | None): User groups with read-only access to session.
            If None, uses config default or creator-only access.
        session_arguments (dict[str, Any] | None): Additional arguments for pydeephaven.Session constructor.
            If None, uses config default or standard session settings.

    Returns:
        dict: Structured response with session creation details.

        The ``session_id`` in the response has the format
        ``"enterprise:{system_name}:{session_name}"``, where ``{system_name}`` is this
        server's configured system name.  This format ensures globally unique IDs when
        multiple enterprise MCP servers run simultaneously (one per system), and allows
        each server to validate that an incoming ``session_id`` belongs to its own system.
        Pass the returned ``session_id`` verbatim to all subsequent tools
        (``session_details``, ``script_run``, ``table_*``, ``catalog_*``, ``session_enterprise_delete``).

        Success response:
        {
            "success": True,
            "session_id": "enterprise:prod-system:analytics-session-001",
            "system_name": "prod-system",
            "session_name": "analytics-session-001",
            "configuration": {
                "heap_size_gb": 8.0,
                "auto_delete_timeout": 3600,
                "server": "server-east-1",
                "engine": "DeephavenCommunity"
            }
        }

        Error response:
        {
            "success": False,
            "error": "Max concurrent sessions (5) reached for system 'prod-system'",
            "isError": True
        }

    Validation and Safety:
        - Verifies enterprise system exists and is accessible
        - Checks max_concurrent_sessions limit from configuration
        - Ensures no session ID conflicts in registry
        - Authenticates with enterprise system before creation
        - Provides detailed error messages for troubleshooting

    Common Error Scenarios:
        - Session limit reached: "Max concurrent sessions (N) reached"
        - Name conflict: "Session 'enterprise:sys:name' already exists"
        - Authentication failure: "Failed to authenticate with enterprise system"
        - Resource exhaustion: "Insufficient resources to create session"
        - Network issues: "Failed to connect to enterprise system"

    Example Usage:
        # Create session with auto-generated name and all defaults
        Tool: session_enterprise_create
        Parameters: {}

        # Create session with custom name
        Tool: session_enterprise_create
        Parameters: {
            "session_name": "my-analysis-session"
        }

        # Create session with custom heap size and timeout
        Tool: session_enterprise_create
        Parameters: {
            "session_name": "large-data-session",
            "heap_size_gb": 16.0,
            "auto_delete_timeout": 3600
        }

        # Create Groovy session with custom JVM args
        Tool: session_enterprise_create
        Parameters: {
            "programming_language": "Groovy",
            "extra_jvm_args": ["-Xmx8g", "-XX:+UseG1GC"]
        }

        # Create session with environment variables
        Tool: session_enterprise_create
        Parameters: {
            "environment_vars": {"VAR1": "/mnt/data", "VAR2": "DEBUG"}
        }

        # Create session with specific server and permissions
        Tool: session_enterprise_create
        Parameters: {
            "server": "server-east-1",
            "admin_groups": ["data-engineers"],
            "viewer_groups": ["analysts", "data-scientists"]
        }
    """
    result: dict[str, object] = {"success": False}
    system_name = system  # alias used in legacy log/error strings below

    try:
        # Get config and session registry
        multi_config = get_multi_config(context)
        session_registry = get_enterprise_registry(context, system)
        _LOGGER.info(
            f"[mcp_systems_server:session_enterprise_create] Invoked: "
            f"system_name={system_name!r}, session_name={session_name!r}, "
            f"heap_size_gb={heap_size_gb}, auto_delete_timeout={auto_delete_timeout}, "
            f"server={server!r}, engine={engine!r}, "
            f"extra_jvm_args={extra_jvm_args}, environment_vars={environment_vars}, "
            f"admin_groups={admin_groups}, viewer_groups={viewer_groups}, "
            f"session_arguments={session_arguments}, "
            f"programming_language={programming_language}"
        )

        # Get enterprise system configuration from the lifespan-loaded multi-config.
        if (
            multi_config.enterprise is None
            or system_name not in multi_config.enterprise.systems
        ):
            error_msg = f"Enterprise system {system_name!r} is not configured."
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_create] {error_msg}")
            result.update(error_response(error_msg))
            return result
        # Read the typed enterprise system declaration directly.
        system_config = multi_config.enterprise.systems[system_name]
        session_creation_config = system_config.session_creation
        if session_creation_config is None:
            error_msg = f"Enterprise session creation not configured for system '{system_name}'. Add a 'session_creation' section to the configuration."
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_create] {error_msg}")
            result.update(error_response(error_msg))
            return result
        max_sessions = session_creation_config.max_concurrent_sessions

        # Check session limits (both enabled and count)
        limit_err = await _check_session_limit(session_registry, max_sessions)
        if limit_err:
            result.update(limit_err)
            return result

        # Generate session name if not provided
        session_name = _generate_session_name_if_none(system_config, session_name)

        # Create session ID and check for conflicts
        session_id = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, system_name, session_name
        )
        id_err = await _check_session_id_available(session_registry, session_id)
        if id_err:
            result.update(id_err)
            return result

        # Resolve configuration parameters (defaults guaranteed by config validation)
        defaults = session_creation_config.defaults
        resolved_config = _resolve_session_parameters(
            heap_size_gb,
            auto_delete_timeout,
            server,
            engine,
            extra_jvm_args,
            environment_vars,
            admin_groups,
            viewer_groups,
            session_arguments,
            programming_language,
            defaults,
        )

        _LOGGER.debug(
            f"[mcp_systems_server:session_enterprise_create] Resolved configuration: {resolved_config}"
        )

        # Get the factory manager directly and create session
        factory_manager = session_registry.factory_manager
        factory = await factory_manager.get()

        # Create configuration transformer based on programming language
        configuration_transformer = None
        programming_lang = resolved_config["programming_language"]
        if programming_lang and programming_lang.lower() != "python":

            def language_transformer(
                config: CorePlusQueryConfig,
            ) -> CorePlusQueryConfig:
                config.pb.scriptLanguage = programming_lang
                return config

            configuration_transformer = language_transformer

        _LOGGER.debug(
            f"[mcp_systems_server:session_enterprise_create] Creating session with parameters: "
            f"name={session_name}, heap_size_gb={resolved_config['heap_size_gb']}, "
            f"auto_delete_timeout={resolved_config['auto_delete_timeout']}, "
            f"server={resolved_config['server']}, engine={resolved_config['engine']}, "
            f"programming_language={programming_lang}"
        )

        # Create the session
        session = await factory.connect_to_new_worker(
            name=session_name,
            heap_size_gb=resolved_config["heap_size_gb"],
            auto_delete_timeout=resolved_config["auto_delete_timeout"],
            server=resolved_config["server"],
            engine=resolved_config["engine"],
            extra_jvm_args=resolved_config["extra_jvm_args"],
            extra_environment_vars=resolved_config[
                "extra_environment_vars"
            ],  # controller wire format: ["NAME=value", ...]
            admin_groups=resolved_config["admin_groups"],
            viewer_groups=resolved_config["viewer_groups"],
            configuration_transformer=configuration_transformer,
            session_arguments=resolved_config["session_arguments"],
        )

        # Create an EnterpriseSessionManager and add to registry
        async def creation_function(source: str, name: str) -> CorePlusSession:
            return session

        enterprise_session_manager = EnterpriseSessionManager(
            system=system_name,
            name=session_name,
            creation_function=creation_function,
        )
        session_id = enterprise_session_manager.full_name

        # Add to session registry
        await session_registry.add_session(enterprise_session_manager)

        _LOGGER.info(
            f"[mcp_systems_server:session_enterprise_create] Successfully created session "
            f"'{session_name}' on system '{system_name}' with session ID '{session_id}'"
        )

        result.update(
            {
                "success": True,
                "session_id": session_id,
                "system_name": system_name,
                "session_name": session_name,
                "configuration": {
                    "heap_size_gb": resolved_config["heap_size_gb"],
                    "auto_delete_timeout": resolved_config["auto_delete_timeout"],
                    "server": resolved_config["server"],
                    "engine": resolved_config["engine"],
                },
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_enterprise_create] Failed to create session "
            f"'{session_name}' on system '{system_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to create enterprise session '{session_name}' on system '{system_name}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result


def _resolve_session_parameters(
    heap_size_gb: float | int | None,
    auto_delete_timeout: int | None,
    server: str | None,
    engine: str | None,
    extra_jvm_args: list[str] | None,
    environment_vars: dict[str, str] | None,
    admin_groups: list[str] | None,
    viewer_groups: list[str] | None,
    session_arguments: dict[str, Any] | None,
    programming_language: str | None,
    defaults: EnterpriseSessionCreationDefaults,
) -> dict:
    """Resolve session parameters with priority: tool param -> typed config default.

    Args:
        heap_size_gb (float | int | None): Tool parameter value for JVM heap size in GB (e.g., 8 or 2.5).
        auto_delete_timeout (int | None): Tool parameter value for session timeout in seconds.
        server (str | None): Tool parameter value for target server.
        engine (str | None): Tool parameter value for engine type.
        extra_jvm_args (list[str] | None): Tool parameter value for additional JVM arguments.
        environment_vars (dict[str, str] | None): Tool parameter value for environment
            variables (mapping form; the controller adapter converts to ``["NAME=value", ...]``).
        admin_groups (list[str] | None): Tool parameter value for admin user groups.
        viewer_groups (list[str] | None): Tool parameter value for viewer user groups.
        session_arguments (dict[str, Any] | None): Tool parameter value for pydeephaven.Session constructor.
        programming_language (str | None): Tool parameter value for session language ("Python" or "Groovy").
        defaults (EnterpriseSessionCreationDefaults): Typed defaults
            block from the enterprise system's ``session_creation``
            config; every field carries its schema-level default.

    Returns:
        dict: Resolved configuration with all parameters using priority order.
    """
    return {
        "heap_size_gb": heap_size_gb or defaults.heap_size_gb,
        "auto_delete_timeout": (
            auto_delete_timeout
            if auto_delete_timeout is not None
            else defaults.auto_delete_timeout
        ),
        "server": server or defaults.server,
        "engine": engine or defaults.engine,
        "extra_jvm_args": extra_jvm_args or defaults.extra_jvm_args,
        "extra_environment_vars": _env_vars_to_list(
            environment_vars or defaults.environment_vars
        ),
        "admin_groups": admin_groups or defaults.admin_groups,
        "viewer_groups": viewer_groups or defaults.viewer_groups,
        "session_arguments": session_arguments or defaults.session_arguments,
        "programming_language": programming_language or defaults.programming_language,
    }


async def session_enterprise_delete(
    context: Context,
    session_id: str,
) -> dict:
    """MCP Tool: Delete an existing enterprise session.

    Removes an enterprise session from this enterprise system and removes it from the
    session registry. The session becomes inaccessible for future operations.

    Session ID Format:
        Session IDs have the format ``"enterprise:{system_name}:{session_name}"``.
        The ``{system_name}`` component is the server's configured system name, embedded
        in the ID when the session was created.  Each enterprise MCP server instance
        manages exactly one system, identified by its ``system_name``.  This server
        validates that the ``{system_name}`` in the provided ``session_id`` matches its own
        configured ``system_name``; passing an ID from a different enterprise server
        returns a clear error rather than a confusing "not found".

        Use the ``session_id`` returned by ``session_enterprise_create`` or
        ``sessions_list`` verbatim — do not construct or modify it manually.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use this tool to clean up sessions when no longer needed
    - Pass the ``session_id`` exactly as returned by ``session_enterprise_create`` or ``sessions_list``
    - Check 'success' field to verify deletion completed
    - This operation is irreversible - deleted sessions cannot be recovered
    - Session will no longer be accessible via other MCP tools after deletion

    Args:
        context (Context): The MCP context object.
        session_id (str): Full session identifier in format
            ``"enterprise:{system_name}:{session_name}"``.  Must be an existing session
            created on this server's configured system.  Passing an ID from a different
            enterprise server returns a validation error.

    Returns:
        dict: Structured response with deletion details.

        Success response:
        {
            "success": True,
            "session_id": "enterprise:prod-system:analytics-session-001",
            "system_name": "prod-system",
            "session_name": "analytics-session-001"
        }

        Error response (not found):
        {
            "success": False,
            "error": "Session 'enterprise:prod-system:nonexistent-session' not found",
            "isError": True
        }

        Error response (wrong server):
        {
            "success": False,
            "error": "Session 'enterprise:dev:session-1' belongs to system 'dev', but this server manages 'prod'",
            "isError": True
        }

    Validation and Safety:
        - Validates session_id format (must be "enterprise:{system_name}:{session_name}")
        - Validates that the system_name component matches this server's configured system
        - Checks that the specified session exists in registry
        - Properly closes the session before removal
        - Removes session from registry to prevent future access
        - Provides detailed error messages for troubleshooting

    Common Error Scenarios:
        - Invalid format: "Invalid session_id format: ..."
        - Wrong type: "Session 'community:...' is not an enterprise session"
        - Wrong server: "Session 'enterprise:dev:s1' belongs to system 'dev', but this server manages 'prod'"
        - Session not found: "Session 'enterprise:sys:session' not found"
        - Already deleted: "Session 'enterprise:sys:session' not found"
        - Close failure: "Failed to close session"
        - Registry error: "Failed to remove session from registry"

    Note:
        - This operation is irreversible - deleted sessions cannot be recovered
        - Any running queries or tables in the session will be lost
        - Other connections to the same session will lose access
        - Use with caution in production environments
    """
    result: dict[str, object] = {"success": False}
    session_name = (
        session_id  # fallback for outer except; overwritten after parse_full_name
    )
    system_name: str = (
        ""  # fallback for outer except; overwritten after registry lookup
    )

    try:
        # Parse session_id to determine which enterprise system it belongs to.
        try:
            system_type, source, session_name = parse_session_id(session_id)
        except InvalidSessionNameError as e:
            error_msg = f"Invalid session_id format: {e}"
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        if system_type != SystemType.ENTERPRISE:
            error_msg = (
                f"Session '{session_id}' is not an enterprise session "
                f"(type: '{system_type.value}')"
            )
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Get session registry for the system named in the id.
        try:
            session_registry = get_enterprise_registry(context, source)
        except InvalidSessionNameError as e:
            error_msg = str(e)
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result
        system_name = source
        _LOGGER.info(
            f"[mcp_systems_server:session_enterprise_delete] Invoked: "
            f"system_name={system_name!r}, session_id={session_id!r}"
        )

        _LOGGER.debug(
            f"[mcp_systems_server:session_enterprise_delete] Looking for session '{session_id}'"
        )

        # Check if session exists in registry
        try:
            session_manager = await session_registry.get(session_id)
        except RegistryItemNotFoundError as e:
            error_msg = f"Session '{session_id}' not found: {e}"
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Verify it's an EnterpriseSessionManager (safety check)
        if not isinstance(session_manager, EnterpriseSessionManager):
            error_msg = f"Session '{session_id}' is not an enterprise session"
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.debug(
            f"[mcp_systems_server:session_enterprise_delete] Found enterprise session manager for '{session_id}'"
        )

        # Close the session if it's active
        try:
            _LOGGER.debug(
                f"[mcp_systems_server:session_enterprise_delete] Closing session '{session_id}'"
            )
            await session_manager.close()
            _LOGGER.debug(
                f"[mcp_systems_server:session_enterprise_delete] Successfully closed session '{session_id}'"
            )
        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:session_enterprise_delete] Failed to close session '{session_id}': {e}"
            )
            # Continue with removal even if close failed

        # Remove from session registry
        try:
            removed_manager = await session_registry.remove(session_id)
            if removed_manager is None:
                error_msg = (
                    f"Session '{session_id}' was not found in registry during removal"
                )
                _LOGGER.warning(
                    f"[mcp_systems_server:session_enterprise_delete] {error_msg}"
                )
            else:
                _LOGGER.debug(
                    f"[mcp_systems_server:session_enterprise_delete] Removed session '{session_id}' from registry"
                )

        except Exception as e:
            error_msg = f"Failed to remove session '{session_id}' from registry: {e}"
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.info(
            f"[mcp_systems_server:session_enterprise_delete] Successfully deleted session "
            f"'{session_name}' from system '{system_name}' (session ID: '{session_id}')"
        )

        result.update(
            {
                "success": True,
                "session_id": session_id,
                "system_name": system_name,
                "session_name": session_name,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_enterprise_delete] Failed to delete session "
            f"'{session_name}' from system '{system_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to delete enterprise session '{session_name}' from system '{system_name}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result


def register_tools(server: FastMCP) -> None:
    """Register all Enterprise session tools with the given FastMCP server.

    Registers ``enterprise_systems_status`` (which returns an empty system
    list when no Enterprise system is configured) plus the Enterprise session
    create/delete tools (which return a clean "not configured" error in that
    case). Every tool module registers unconditionally; tools self-report
    applicability rather than being gated by configuration.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(enterprise_systems_status)
    server.tool()(session_enterprise_create)
    server.tool()(session_enterprise_delete)
