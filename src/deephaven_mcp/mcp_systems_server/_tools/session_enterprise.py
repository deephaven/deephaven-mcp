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
from deephaven_mcp.client import CorePlusSession
from deephaven_mcp.client._protobuf import CorePlusQueryConfig
from deephaven_mcp.config import (
    ConfigManager,
    redact_enterprise_system_config,
)
from deephaven_mcp.mcp_systems_server._tools.session import (
    DEFAULT_MAX_CONCURRENT_SESSIONS,
    DEFAULT_PROGRAMMING_LANGUAGE,
)
from deephaven_mcp.mcp_systems_server._tools.shared import (
    format_initialization_status,
)
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    EnterpriseSessionManager,
    SystemType,
)
from deephaven_mcp.resource_manager._registry_enterprise import (
    EnterpriseSessionRegistry,
)

_LOGGER = logging.getLogger(__name__)


# Enterprise session creation defaults
DEFAULT_ENGINE = "DeephavenCommunity"
"""Default engine type for enterprise sessions when not specified in config."""


DEFAULT_TIMEOUT_SECONDS = 60
"""Default timeout for enterprise session operations when not specified in config."""


async def enterprise_systems_status(
    context: Context, attempt_to_connect: bool = False
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
    - Use 'detail' field for troubleshooting connection issues
    - Configuration details are included but sensitive fields are redacted

    Args:
        context (Context): The MCP context object.
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
            - 'initialization' (dict, optional): Present when enterprise session discovery is
                still in progress or completed with errors. Contains:
                - 'status' (str): Human-readable description of the initialization state.
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

    Example Error Response:
        {'success': False, 'error': 'Failed to retrieve systems status', 'isError': True}

    Performance Considerations:
        - With attempt_to_connect=False: Typically completes in milliseconds
        - With attempt_to_connect=True: May take seconds due to connection operations
    """
    _LOGGER.info("[mcp_systems_server:enterprise_systems_status] Invoked.")
    try:
        session_registry: EnterpriseSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]
        # Get atomic snapshot for initialization state
        snapshot = await session_registry.get_all()

        # Get the single factory manager directly
        factory_manager = session_registry.factory_manager

        # Use liveness_status() for detailed health information
        status_enum, liveness_detail = await factory_manager.liveness_status(
            ensure_item=attempt_to_connect
        )
        liveness_status = status_enum.name

        # Also get simple is_alive boolean
        is_alive = await factory_manager.is_alive()

        # Flat config is the system config directly
        raw_config = await config_manager.get_config()
        redacted_config = redact_enterprise_system_config(raw_config)

        system_info: dict[str, object] = {
            "name": session_registry.system_name,
            "liveness_status": liveness_status,
            "is_alive": is_alive,
            "config": redacted_config,
        }

        if liveness_detail is not None:
            system_info["liveness_detail"] = liveness_detail

        response: dict[str, object] = {"success": True, "systems": [system_info]}

        # Surface initialization status from the registry snapshot
        init_info = format_initialization_status(
            snapshot.initialization_phase, snapshot.initialization_errors
        )
        if init_info:
            response["initialization"] = init_info

        return response
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:enterprise_systems_status] Failed: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}


async def _check_session_limit(
    session_registry: EnterpriseSessionRegistry, max_sessions: int
) -> dict | None:
    """Check if session creation is allowed and within limits.

    Args:
        session_registry (EnterpriseSessionRegistry): The session registry
        max_sessions (int): Maximum concurrent sessions allowed

    Returns:
        dict | None: Error response dict if not allowed, None if allowed
    """
    system_name = session_registry.system_name

    # Check if session creation is disabled
    if max_sessions == 0:
        error_msg = f"Session creation is disabled for system '{system_name}' (max_concurrent_sessions = 0)"
        _LOGGER.error(f"[mcp_systems_server:_check_session_limit] {error_msg}")
        return {"success": False, "error": error_msg, "isError": True}

    # Check if current session count would exceed the limit
    current_session_count = await session_registry.count_added_sessions(
        SystemType.ENTERPRISE, system_name
    )
    if current_session_count >= max_sessions:
        error_msg = f"Max concurrent sessions ({max_sessions}) reached for system '{system_name}'"
        _LOGGER.error(f"[mcp_systems_server:_check_session_limit] {error_msg}")
        return {"success": False, "error": error_msg, "isError": True}

    return None


def _generate_session_name_if_none(
    system_config: dict, session_name: str | None
) -> str:
    """Generate a session name if none provided.

    Args:
        system_config (dict): Enterprise system configuration dict
        session_name (str | None): Provided session name or None

    Returns:
        str: Either the provided session_name or auto-generated name
    """
    if session_name is not None:
        return session_name

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    username = system_config.get("username")
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
        return {"error": error_msg, "isError": True}
    except RegistryItemNotFoundError:
        return None  # Good - session doesn't exist yet


async def session_enterprise_create(
    context: Context,
    session_name: str | None = None,
    heap_size_gb: float | int | None = None,
    programming_language: str | None = None,
    auto_delete_timeout: int | None = None,
    server: str | None = None,
    engine: str | None = None,
    extra_jvm_args: list[str] | None = None,
    extra_environment_vars: list[str] | None = None,
    admin_groups: list[str] | None = None,
    viewer_groups: list[str] | None = None,
    timeout_seconds: float | None = None,
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
        extra_environment_vars (list[str] | None): Environment variables for the session in format
            ["NAME=value", ...]. If None, uses config default environment.
        admin_groups (list[str] | None): User groups with administrative permissions on the session.
            If None, uses config default or creator-only access.
        viewer_groups (list[str] | None): User groups with read-only access to session.
            If None, uses config default or creator-only access.
        timeout_seconds (float | None): Maximum time in seconds to wait for session startup.
            If None, uses config default or 60 seconds.
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
            "extra_environment_vars": ["VAR1=/mnt/data", "VAR2=DEBUG"]
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

    try:
        # Get config and session registry
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]
        session_registry: EnterpriseSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        system_name = session_registry.system_name
        _LOGGER.info(
            f"[mcp_systems_server:session_enterprise_create] Invoked: "
            f"system_name={system_name!r}, session_name={session_name!r}, "
            f"heap_size_gb={heap_size_gb}, auto_delete_timeout={auto_delete_timeout}, "
            f"server={server!r}, engine={engine!r}, "
            f"extra_jvm_args={extra_jvm_args}, extra_environment_vars={extra_environment_vars}, "
            f"admin_groups={admin_groups}, viewer_groups={viewer_groups}, "
            f"timeout_seconds={timeout_seconds}, session_arguments={session_arguments}, "
            f"programming_language={programming_language}"
        )

        # Get enterprise system configuration (flat config returned directly)
        system_config = await config_manager.get_config()
        session_creation_config = system_config.get("session_creation", {})
        max_sessions = session_creation_config.get(
            "max_concurrent_sessions", DEFAULT_MAX_CONCURRENT_SESSIONS
        )

        # Check session limits (both enabled and count)
        error_response = await _check_session_limit(session_registry, max_sessions)
        if error_response:
            result.update(error_response)
            return result

        # Generate session name if not provided
        session_name = _generate_session_name_if_none(system_config, session_name)

        # Create session ID and check for conflicts
        session_id = BaseItemManager.make_full_name(
            SystemType.ENTERPRISE, system_name, session_name
        )
        error_response = await _check_session_id_available(session_registry, session_id)
        if error_response:
            result.update(error_response)
            return result

        # Resolve configuration parameters
        defaults = session_creation_config.get("defaults", {})
        resolved_config = _resolve_session_parameters(
            heap_size_gb,
            auto_delete_timeout,
            server,
            engine,
            extra_jvm_args,
            extra_environment_vars,
            admin_groups,
            viewer_groups,
            timeout_seconds,
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
            extra_environment_vars=resolved_config["extra_environment_vars"],
            admin_groups=resolved_config["admin_groups"],
            viewer_groups=resolved_config["viewer_groups"],
            timeout_seconds=resolved_config["timeout_seconds"],
            configuration_transformer=configuration_transformer,
            session_arguments=resolved_config["session_arguments"],
        )

        # Create an EnterpriseSessionManager and add to registry
        async def creation_function(source: str, name: str) -> CorePlusSession:
            return session

        enterprise_session_manager = EnterpriseSessionManager(
            source=system_name,
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
    extra_environment_vars: list[str] | None,
    admin_groups: list[str] | None,
    viewer_groups: list[str] | None,
    timeout_seconds: float | None,
    session_arguments: dict[str, Any] | None,
    programming_language: str | None,
    defaults: dict,
) -> dict:
    """Resolve session parameters with priority: tool param -> config default -> API default.

    Args:
        heap_size_gb (float | int | None): Tool parameter value for JVM heap size in GB (e.g., 8 or 2.5).
        auto_delete_timeout (int | None): Tool parameter value for session timeout in seconds.
        server (str | None): Tool parameter value for target server.
        engine (str | None): Tool parameter value for engine type.
        extra_jvm_args (list[str] | None): Tool parameter value for additional JVM arguments.
        extra_environment_vars (list[str] | None): Tool parameter value for environment variables.
        admin_groups (list[str] | None): Tool parameter value for admin user groups.
        viewer_groups (list[str] | None): Tool parameter value for viewer user groups.
        timeout_seconds (float | None): Tool parameter value for session startup timeout.
        session_arguments (dict[str, Any] | None): Tool parameter value for pydeephaven.Session constructor.
        programming_language (str | None): Tool parameter value for session language ("Python" or "Groovy").
        defaults (dict): Configuration defaults dictionary from session_creation config.

    Returns:
        dict: Resolved configuration with all parameters using priority order.
    """
    return {
        "heap_size_gb": heap_size_gb or defaults.get("heap_size_gb"),
        "auto_delete_timeout": (
            auto_delete_timeout
            if auto_delete_timeout is not None
            else defaults.get("auto_delete_timeout")
        ),
        "server": server or defaults.get("server"),
        "engine": engine or defaults.get("engine", DEFAULT_ENGINE),
        "extra_jvm_args": extra_jvm_args or defaults.get("extra_jvm_args"),
        "extra_environment_vars": extra_environment_vars
        or defaults.get("extra_environment_vars"),
        "admin_groups": admin_groups or defaults.get("admin_groups"),
        "viewer_groups": viewer_groups or defaults.get("viewer_groups"),
        "timeout_seconds": (
            timeout_seconds
            if timeout_seconds is not None
            else defaults.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        ),
        "session_arguments": session_arguments or defaults.get("session_arguments"),
        "programming_language": programming_language
        or defaults.get("programming_language", DEFAULT_PROGRAMMING_LANGUAGE),
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
        ``session_list`` verbatim — do not construct or modify it manually.

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
    - Pass the ``session_id`` exactly as returned by ``session_enterprise_create`` or ``session_list``
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
        # Get session registry
        session_registry: EnterpriseSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        system_name = session_registry.system_name
        _LOGGER.info(
            f"[mcp_systems_server:session_enterprise_delete] Invoked: "
            f"system_name={system_name!r}, session_id={session_id!r}"
        )

        # Parse and validate the session_id
        try:
            system_type_str, source, session_name = BaseItemManager.parse_full_name(
                session_id
            )
        except InvalidSessionNameError as e:
            error_msg = f"Invalid session_id format: {e}"
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        if system_type_str != SystemType.ENTERPRISE.value:
            error_msg = f"Session '{session_id}' is not an enterprise session (type: '{system_type_str}')"
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        if source != system_name:
            error_msg = (
                f"Session '{session_id}' belongs to system '{source}', "
                f"but this server manages '{system_name}'"
            )
            _LOGGER.error(f"[mcp_systems_server:session_enterprise_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

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
            removed_manager = await session_registry.remove_session(session_id)
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
    """Register all enterprise session tools with the given FastMCP server.

    These tools are specific to the DHE server and should NOT be registered
    on the DHC server.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(enterprise_systems_status)
    server.tool()(session_enterprise_create)
    server.tool()(session_enterprise_delete)
