"""
Community Session MCP Tools - Create and Manage Community Sessions.

Provides MCP tools for managing Deephaven Community sessions:
- session_community_create: Create new Community sessions (Docker or Python)
- session_community_delete: Delete Community sessions
- session_community_credentials: Get connection credentials for Community sessions

These tools work with Deephaven Community (Core) sessions only.
"""

import logging
import os
from typing import Any, Literal, cast

from mcp.server.fastmcp import Context

from deephaven_mcp._exceptions import CommunitySessionConfigurationError
from deephaven_mcp.config import ConfigManager
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CombinedSessionRegistry,
    CommunitySessionManager,
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    LaunchedSession,
    PythonLaunchedSession,
    SystemType,
    find_available_port,
    generate_auth_token,
    launch_session,
)
from deephaven_mcp.resource_manager._instance_tracker import InstanceTracker

from deephaven_mcp.mcp_systems_server._tools.mcp_server import (
    mcp_server,
)
from deephaven_mcp.mcp_systems_server._tools.session import (
    DEFAULT_MAX_CONCURRENT_SESSIONS,
    DEFAULT_PROGRAMMING_LANGUAGE,
)

_LOGGER = logging.getLogger(__name__)


# Community session creation defaults
DEFAULT_LAUNCH_METHOD = "docker"
"""Default launch method for community sessions when not specified in config."""


DEFAULT_AUTH_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"
"""Default authentication type for community sessions when not specified in config."""


DEFAULT_DOCKER_IMAGE_PYTHON = "ghcr.io/deephaven/server:latest"
"""Docker image for Python community sessions."""


DEFAULT_DOCKER_IMAGE_GROOVY = "ghcr.io/deephaven/server-slim:latest"
"""Docker image for Groovy community sessions."""


DEFAULT_HEAP_SIZE_GB = 4.0
"""Default JVM heap size in GB for community sessions when not specified in config."""


DEFAULT_STARTUP_TIMEOUT_SECONDS = 60
"""Default maximum time to wait for session startup when not specified in config."""


DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS = 2
"""Default time between health checks during startup when not specified in config."""


DEFAULT_STARTUP_RETRIES = 3
"""Default number of connection attempts per health check when not specified in config."""



# =============================================================================
# Community Session Management Tools
# =============================================================================


async def _get_session_creation_config(
    config_manager: ConfigManager,
) -> tuple[dict, int, dict | None]:
    """Get and validate session creation configuration.

    Returns:
        Tuple of (defaults_dict, max_concurrent_sessions, error_dict).
        On error, error_dict is set and other values are empty.
    """
    config_data = await config_manager.get_config()
    community_config = config_data.get("community", {})
    session_creation_config = community_config.get("session_creation")

    if not session_creation_config:
        error_msg = "Community session creation not configured in deephaven_mcp.json"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        return {}, 0, {"success": False, "error": error_msg, "isError": True}

    defaults = session_creation_config.get("defaults", {})
    max_concurrent_sessions = session_creation_config.get(
        "max_concurrent_sessions", DEFAULT_MAX_CONCURRENT_SESSIONS
    )

    return defaults, max_concurrent_sessions, None




async def _check_session_limit(
    session_registry: CombinedSessionRegistry,
    max_concurrent_sessions: int,
) -> dict | None:
    """Check if session limit has been reached.

    Returns:
        Error dict if limit reached, None if limit not reached or disabled.
    """
    if max_concurrent_sessions <= 0:
        return None

    current_count = await session_registry.count_added_sessions(
        SystemType.COMMUNITY, ""
    )
    if current_count >= max_concurrent_sessions:
        error_msg = f"Session limit reached: {current_count}/{max_concurrent_sessions} sessions active"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        return {"success": False, "error": error_msg, "isError": True}

    return None




def _validate_launch_method_params(
    launch_method: str,
    programming_language: str | None,
    docker_image: str | None,
    docker_memory_limit_gb: float | None,
    docker_cpu_limit: float | None,
    docker_volumes: list[str] | None,
    python_venv_path: str | None,
) -> dict | None:
    """Validate that method-specific parameters are only used with their respective launch methods.

    Ensures docker-only parameters are not used with python launch method,
    python-only parameters are not used with docker launch method, and
    mutually exclusive parameters are not used together.

    Args:
        launch_method (str): Launch method ("docker" or "python").
        programming_language (str | None): Docker-only parameter.
        docker_image (str | None): Docker-only parameter.
        docker_memory_limit_gb (float | None): Docker-only parameter.
        docker_cpu_limit (float | None): Docker-only parameter.
        docker_volumes (list[str] | None): Docker-only parameter.
        python_venv_path (str | None): Python-only parameter.

    Returns:
        dict | None: Error dict with 'success', 'error', 'isError' keys if validation fails,
            None if validation passes.
    """
    # Docker-only parameters
    docker_only_params = [
        ("programming_language", programming_language),
        ("docker_image", docker_image),
        ("docker_memory_limit_gb", docker_memory_limit_gb),
        ("docker_cpu_limit", docker_cpu_limit),
        ("docker_volumes", docker_volumes),
    ]

    for param_name, param_value in docker_only_params:
        if param_value and launch_method != "docker":
            error_msg = f"'{param_name}' parameter only applies to docker launch method, not '{launch_method}'"
            _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
            return {"success": False, "error": error_msg, "isError": True}

    # Python-only parameters
    if python_venv_path and launch_method != "python":
        error_msg = f"'python_venv_path' parameter only applies to python launch method, not '{launch_method}'"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        return {"success": False, "error": error_msg, "isError": True}

    # Check mutual exclusivity
    if programming_language and docker_image:
        error_msg = "Cannot specify both 'programming_language' and 'docker_image' - use one or the other"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        return {"success": False, "error": error_msg, "isError": True}

    return None




def _resolve_docker_image(
    programming_language: str | None,
    docker_image: str | None,
    defaults: dict,
) -> tuple[str, dict | None]:
    """Resolve docker image from programming language or explicit image parameter.

    This function implements the following priority for Docker image selection:
    1. Explicit docker_image parameter (highest priority)
    2. Auto-select based on programming_language parameter
    3. Auto-select based on programming_language from config defaults
    4. Use docker_image from config defaults (if language-based selection not applicable)

    Args:
        programming_language (str | None): Programming language ("Python" or "Groovy"), or None
        docker_image (str | None): Explicit Docker image name, or None for auto-selection
        defaults (dict): Configuration defaults that may contain 'programming_language' or 'docker_image'

    Returns:
        tuple[str, dict | None]: Two-element tuple:
            - First element: Resolved Docker image name (empty string on error)
            - Second element: Error dict with 'success', 'error', 'isError' keys, or None on success

    Note:
        Returns error if programming_language (param or config) is not "Python" or "Groovy" (case-insensitive).
    """
    if docker_image:
        return docker_image, None

    if programming_language:
        lang_lower = programming_language.lower()
        if lang_lower == "python":
            return DEFAULT_DOCKER_IMAGE_PYTHON, None
        elif lang_lower == "groovy":
            return DEFAULT_DOCKER_IMAGE_GROOVY, None
        else:
            error_msg = f"Unsupported programming_language: '{programming_language}'. Must be 'Python' or 'Groovy'"
            _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
            return "", {"success": False, "error": error_msg, "isError": True}

    # Use config defaults
    resolved_lang = defaults.get("programming_language", DEFAULT_PROGRAMMING_LANGUAGE)
    lang_lower = resolved_lang.lower()

    if lang_lower == "python":
        return defaults.get("docker_image", DEFAULT_DOCKER_IMAGE_PYTHON), None
    elif lang_lower == "groovy":
        return defaults.get("docker_image", DEFAULT_DOCKER_IMAGE_GROOVY), None
    else:
        error_msg = f"Invalid programming_language in config: '{resolved_lang}'. Must be 'Python' or 'Groovy'"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        return "", {"success": False, "error": error_msg, "isError": True}




def _resolve_community_session_parameters(
    launch_method: str | None,
    programming_language: str | None,
    auth_type: str | None,
    auth_token: str | None,
    heap_size_gb: float | int | None,
    extra_jvm_args: list[str] | None,
    environment_vars: dict[str, str] | None,
    docker_image: str | None,
    docker_memory_limit_gb: float | None,
    docker_cpu_limit: float | None,
    docker_volumes: list[str] | None,
    python_venv_path: str | None,
    defaults: dict,
) -> tuple[dict[str, Any], dict | None]:
    """Resolve all community session creation parameters from tool args, config defaults, and hardcoded defaults.

    This function implements the parameter resolution priority: tool parameter > config default > hardcoded default.
    It validates parameters, normalizes values, and returns a complete set of resolved parameters for session creation.

    Args:
        launch_method (str | None): Launch method ("docker" or "python"), or None to use default
        programming_language (str | None): Programming language ("Python" or "Groovy"), or None to use default
        auth_type (str | None): Authentication type (shorthand or full class name), or None to use default
        auth_token (str | None): Authentication token, or None to auto-generate for PSK auth
        heap_size_gb (float | int | None): JVM heap size in GB (e.g., 4 or 2.5), or None to use default
        extra_jvm_args (list[str] | None): Additional JVM arguments, or None to use default
        environment_vars (dict[str, str] | None): Environment variables, or None to use default
        docker_image (str | None): Docker image name (docker only), or None to auto-select based on language
        docker_memory_limit_gb (float | None): Docker memory limit in GB (docker only), or None for no limit
        docker_cpu_limit (float | None): Docker CPU limit (docker only), or None for no limit
        docker_volumes (list[str] | None): Docker volume mounts (docker only), or None to use default
        python_venv_path (str | None): Python venv path (python only), or None to use default
        defaults (dict): Configuration defaults from deephaven_mcp.json

    Returns:
        tuple[dict[str, Any], dict | None]: Two-element tuple:
            - First element: Dict of resolved parameters with keys:
                - launch_method (str): Resolved launch method (lowercase)
                - programming_language (str): Resolved programming language
                - auth_type (str): Normalized auth type (full class name)
                - auth_token (str | None): Resolved or generated auth token
                - auto_generated_token (bool): True if token was auto-generated
                - heap_size_gb (float | int): Resolved heap size
                - docker_image (str): Resolved docker image (empty for python launch)
                - docker_memory_limit_gb (float | None): Resolved memory limit
                - docker_cpu_limit (float | None): Resolved CPU limit
                - docker_volumes (list[str]): Resolved volume mounts
                - python_venv_path (str | None): Resolved venv path
                - extra_jvm_args (list[str]): Resolved JVM args
                - environment_vars (dict[str, str]): Resolved environment variables
                - startup_timeout_seconds (int): Resolved startup timeout
                - startup_check_interval_seconds (float): Resolved check interval
                - startup_retries (int): Resolved retry count
            - Second element: Error dict with 'success', 'error', 'isError' keys, or None on success
    """
    # Resolve launch method and auth type
    resolved_launch_method = (
        launch_method or defaults.get("launch_method", DEFAULT_LAUNCH_METHOD)
    ).lower()
    # Normalize auth_type to full class name for Deephaven client compatibility
    raw_auth_type = auth_type or defaults.get("auth_type", DEFAULT_AUTH_TYPE)
    resolved_auth_type, auth_error = _normalize_auth_type(raw_auth_type)
    if auth_error:
        return {}, {
            "success": False,
            "error": f"Invalid auth_type: {auth_error}",
            "isError": True,
        }

    # Validate method-specific parameters
    validation_error = _validate_launch_method_params(
        resolved_launch_method,
        programming_language,
        docker_image,
        docker_memory_limit_gb,
        docker_cpu_limit,
        docker_volumes,
        python_venv_path,
    )
    if validation_error:
        return {}, validation_error

    # Resolve programming_language for both launch methods
    # This determines both the Docker image selection (for docker) and the session's
    # programming_language property (via session_type in session config)
    resolved_programming_language = programming_language or defaults.get(
        "programming_language", DEFAULT_PROGRAMMING_LANGUAGE
    )

    # Resolve docker image (only for docker launch method)
    if resolved_launch_method == "docker":
        resolved_docker_image, image_error = _resolve_docker_image(
            programming_language, docker_image, defaults
        )
        if image_error:
            return {}, image_error
    else:
        # For python launch, ensure no docker image is set
        resolved_docker_image = ""

    # Resolve heap size
    resolved_heap_size_gb = heap_size_gb or defaults.get(
        "heap_size_gb", DEFAULT_HEAP_SIZE_GB
    )

    # Resolve startup parameters from config or defaults (not exposed as tool parameters)
    resolved_startup_timeout = defaults.get(
        "startup_timeout_seconds", DEFAULT_STARTUP_TIMEOUT_SECONDS
    )
    resolved_startup_interval = defaults.get(
        "startup_check_interval_seconds", DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS
    )
    resolved_startup_retries = defaults.get("startup_retries", DEFAULT_STARTUP_RETRIES)

    # Resolve optional parameters based on launch method
    if resolved_launch_method == "docker":
        resolved_docker_memory_limit = docker_memory_limit_gb or defaults.get(
            "docker_memory_limit_gb"
        )
        resolved_docker_cpu_limit = docker_cpu_limit or defaults.get("docker_cpu_limit")
        resolved_docker_volumes = docker_volumes or defaults.get("docker_volumes", [])
        resolved_python_venv_path = None
    else:  # python
        resolved_docker_memory_limit = None
        resolved_docker_cpu_limit = None
        resolved_docker_volumes = []
        resolved_python_venv_path = python_venv_path or defaults.get("python_venv_path")

    resolved_extra_jvm_args = extra_jvm_args or defaults.get("extra_jvm_args", [])
    resolved_environment_vars = environment_vars or defaults.get("environment_vars", {})

    # Resolve auth token
    resolved_auth_token, auto_generated_token = _resolve_auth_token(
        resolved_auth_type, auth_token, defaults
    )

    return {
        "launch_method": resolved_launch_method,
        "programming_language": resolved_programming_language,
        "auth_type": resolved_auth_type,
        "auth_token": resolved_auth_token,
        "auto_generated_token": auto_generated_token,
        "heap_size_gb": resolved_heap_size_gb,
        "docker_image": resolved_docker_image,
        "docker_memory_limit_gb": resolved_docker_memory_limit,
        "docker_cpu_limit": resolved_docker_cpu_limit,
        "docker_volumes": resolved_docker_volumes,
        "python_venv_path": resolved_python_venv_path,
        "extra_jvm_args": resolved_extra_jvm_args,
        "environment_vars": resolved_environment_vars,
        "startup_timeout_seconds": resolved_startup_timeout,
        "startup_check_interval_seconds": resolved_startup_interval,
        "startup_retries": resolved_startup_retries,
    }, None




def _normalize_auth_type(auth_type: str) -> tuple[str, str | None]:
    """Normalize shorthand auth types to full Deephaven class names.

    Dynamic community sessions only support PSK and Anonymous authentication.
    Basic auth requires database setup and is not suitable for dynamic sessions.

    Validation Rules:
    - Rejects leading/trailing whitespace
    - Rejects "Basic" authentication (case-insensitive)
    - Detects and rejects incorrectly-cased Deephaven PSK handler
    - Normalizes "PSK" → "io.deephaven.authentication.psk.PskAuthenticationHandler"
    - Normalizes "ANONYMOUS" → "Anonymous" (canonical case)
    - Preserves custom authenticator class names exactly as provided

    Args:
        auth_type (str): Authentication type, either shorthand ("PSK", "Anonymous")
            or full class name.

    Returns:
        tuple[str, str | None]: (normalized_auth_type, error_message).
            - On success: (normalized_value, None)
            - On failure: ("", error_message_string)
    """
    # Check for whitespace first (applies to all auth_type values)
    if auth_type != auth_type.strip():
        return (
            "",
            f"Invalid auth_type '{auth_type}': contains leading or trailing whitespace.",
        )

    auth_type_upper = auth_type.upper()

    # Normalize shorthand to full class names (only PSK and Anonymous for dynamic sessions)
    if auth_type_upper == "PSK":
        return "io.deephaven.authentication.psk.PskAuthenticationHandler", None
    elif auth_type_upper == "ANONYMOUS":
        return "Anonymous", None
    elif auth_type_upper == "BASIC":
        return (
            "",
            "Basic authentication is not supported for dynamic sessions (requires database setup). Use 'PSK' or 'Anonymous'.",
        )

    # Check if it looks like the Deephaven PSK handler but with wrong case
    if (
        "." in auth_type
        and auth_type.upper()
        == "IO.DEEPHAVEN.AUTHENTICATION.PSK.PSKAUTHENTICATIONHANDLER"
    ):
        if auth_type != "io.deephaven.authentication.psk.PskAuthenticationHandler":
            return (
                "",
                f"Invalid auth_type '{auth_type}': appears to be the Deephaven PSK handler with incorrect case. Use 'io.deephaven.authentication.psk.PskAuthenticationHandler' or shorthand 'PSK'.",
            )

    # Already a valid value ("Anonymous", correct PSK handler, or custom authenticator) - preserve exact case
    return auth_type, None




def _resolve_auth_token(
    auth_type: str,
    auth_token: str | None,
    defaults: dict,
) -> tuple[str | None, bool]:
    """Resolve authentication token, auto-generating if needed.

    Args:
        auth_type (str): Normalized authentication type (should be full class name from _normalize_auth_type).
        auth_token (str | None): Explicit auth token parameter, or None.
        defaults (dict): Configuration defaults dictionary that may contain 'auth_token_env_var' or 'auth_token'.

    Returns:
        tuple[str | None, bool]: (resolved_token, was_auto_generated).
            - (None, False) if auth_type doesn't require a token
            - (token_string, False) if token was provided or from config
            - (token_string, True) if token was auto-generated

    Raises:
        CommunitySessionConfigurationError: If auth_token_env_var is configured but the environment variable is not set.
    """
    # Check if auth type requires a PSK token (must be exact match for full class name)
    if auth_type != "io.deephaven.authentication.psk.PskAuthenticationHandler":
        return None, False

    # Check explicit parameter
    if auth_token:
        return auth_token, False

    # Check environment variable from config
    if "auth_token_env_var" in defaults:
        env_var = defaults["auth_token_env_var"]
        token = os.environ.get(env_var)
        if token:
            return token, False
        # If auth_token_env_var is explicitly configured but not set, this is an error
        error_msg = f"Environment variable '{env_var}' specified in auth_token_env_var is not set"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        raise CommunitySessionConfigurationError(error_msg)

    # Check config default
    if "auth_token" in defaults:
        return defaults["auth_token"], False

    # Auto-generate
    token = generate_auth_token()
    _LOGGER.debug(
        "[mcp_systems_server:session_community_create] Auto-generated auth token"
    )
    return token, True




async def _register_session_manager(
    session_name: str,
    session_id: str,
    port: int,
    programming_language: str,
    resolved_auth_type: str,
    resolved_auth_token: str | None,
    launched_session: DockerLaunchedSession | PythonLaunchedSession,
    session_registry: CombinedSessionRegistry,
    instance_tracker: InstanceTracker,
) -> None:
    """Create session manager object and register it in the session registry.

    This helper function creates a DynamicCommunitySessionManager with the appropriate
    configuration and registers it in the combined session registry. It also tracks
    Python-launched processes for orphan cleanup.

    Args:
        session_name (str): Simple session name (not full session_id)
        session_id (str): Full session identifier in format "community:dynamic:{session_name}"
        port (int): Port number where the session is listening
        programming_language (str): Programming language for the session (e.g., "Python", "Groovy")
        resolved_auth_type (str): Normalized authentication type (full class name)
        resolved_auth_token (str | None): Authentication token if applicable
        launched_session (DockerLaunchedSession | PythonLaunchedSession): The launched session object
        session_registry (CombinedSessionRegistry): Registry to add the session to
        instance_tracker (InstanceTracker): Tracker for orphan process cleanup
    """
    # Create session configuration
    # Note: session_type must be lowercase to match CoreSession.from_config expectations
    session_config = {
        "host": "localhost",
        "port": port,
        "auth_type": resolved_auth_type,
        "session_type": programming_language.lower(),  # CoreSession uses this for programming_language property
    }
    if resolved_auth_token:
        session_config["auth_token"] = resolved_auth_token

    # Create manager
    session_manager = DynamicCommunitySessionManager(
        name=session_name,
        config=session_config,
        launched_session=launched_session,
    )

    # Track python process if applicable
    if isinstance(launched_session, PythonLaunchedSession):
        await instance_tracker.track_python_process(
            session_name, launched_session.process.pid
        )

    # Add to registry
    await session_registry.add_session(session_manager)
    _LOGGER.info(
        f"[mcp_systems_server:session_community_create] Successfully created and registered session '{session_id}'"
    )




async def _launch_process_and_wait_for_ready(
    session_name: str,
    resolved_launch_method: Literal["docker", "python"],
    resolved_auth_token: str | None,
    resolved_heap_size_gb: float | int,
    resolved_extra_jvm_args: list[str],
    resolved_environment_vars: dict[str, str],
    resolved_docker_image: str,
    resolved_docker_memory_limit: float | None,
    resolved_docker_cpu_limit: float | None,
    resolved_docker_volumes: list[str],
    resolved_python_venv_path: str | None,
    resolved_startup_timeout: int,
    resolved_startup_interval: float,
    resolved_startup_retries: int,
    instance_tracker: InstanceTracker,
) -> tuple[
    DockerLaunchedSession | PythonLaunchedSession | None, int | None, dict | None
]:
    """Launch Docker container or Python process and wait for health check.

    Finds an available port, launches the session using the specified method,
    and waits for it to become ready via HTTP health checks.

    Args:
        session_name (str): Name for the session.
        resolved_launch_method (Literal["docker", "python"]): Launch method.
        resolved_auth_token (str | None): PSK authentication token, or None for anonymous.
        resolved_heap_size_gb (float | int): JVM heap size in gigabytes (e.g., 4 or 2.5).
        resolved_extra_jvm_args (list[str]): Additional JVM arguments.
        resolved_environment_vars (dict[str, str]): Environment variables.
        resolved_docker_image (str): Docker image (used only if docker launch).
        resolved_docker_memory_limit (float | None): Docker memory limit in GB (docker only).
        resolved_docker_cpu_limit (float | None): Docker CPU limit (docker only).
        resolved_docker_volumes (list[str]): Docker volume mounts (docker only).
        resolved_python_venv_path (str | None): Python venv path (python only).
        resolved_startup_timeout (int): Health check timeout in seconds.
        resolved_startup_interval (float): Health check interval in seconds.
        resolved_startup_retries (int): Max retries per health check.
        instance_tracker (InstanceTracker): Tracker for orphan cleanup.

    Returns:
        tuple[LaunchedSession | None, int | None, dict | None]: Tuple of
            (launched_session, port, error_dict). On success, error_dict is None.
            On failure, launched_session and port may be None.
    """
    port = find_available_port()
    _LOGGER.debug(
        f"[mcp_systems_server:session_community_create] Assigned port {port} to session '{session_name}'"
    )

    _LOGGER.info(
        f"[mcp_systems_server:session_community_create] Launching {resolved_launch_method} session '{session_name}' on port {port}"
    )

    launched_session = await launch_session(
        launch_method=resolved_launch_method,
        session_name=session_name,
        port=port,
        auth_token=resolved_auth_token,
        heap_size_gb=resolved_heap_size_gb,
        extra_jvm_args=resolved_extra_jvm_args,
        environment_vars=resolved_environment_vars,
        docker_image=resolved_docker_image,
        docker_memory_limit_gb=resolved_docker_memory_limit,
        docker_cpu_limit=resolved_docker_cpu_limit,
        docker_volumes=resolved_docker_volumes,
        python_venv_path=resolved_python_venv_path,
        instance_id=instance_tracker.instance_id,
    )

    _LOGGER.info(
        f"[mcp_systems_server:session_community_create] Waiting for session '{session_name}' to be ready"
    )
    is_ready = await launched_session.wait_until_ready(
        timeout_seconds=resolved_startup_timeout,
        check_interval_seconds=resolved_startup_interval,
        max_retries=resolved_startup_retries,
    )

    if not is_ready:
        _LOGGER.error(
            f"[mcp_systems_server:session_community_create] Session '{session_name}' failed to start within {resolved_startup_timeout}s"
        )
        try:
            await launched_session.stop()
        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_create] Failed to cleanup failed session: {e}"
            )

        error_msg = f"Session failed to start within {resolved_startup_timeout} seconds"
        return None, None, {"success": False, "error": error_msg, "isError": True}

    return launched_session, port, None




def _build_success_response(
    session_id: str,
    session_name: str,
    connection_url: str,
    resolved_auth_type: str,
    resolved_launch_method: str,
    port: int,
    launched_session: LaunchedSession,
) -> dict:
    """Build the success response dict for session creation.

    Returns:
        Success response dict with session details.
    """
    result = {
        "success": True,
        "session_id": session_id,
        "session_name": session_name,
        "connection_url": connection_url,
        "auth_type": resolved_auth_type,
        "launch_method": resolved_launch_method,
        "port": port,
    }

    # Add launch-method-specific details
    if resolved_launch_method == "docker":
        docker_session = cast(DockerLaunchedSession, launched_session)
        result["container_id"] = docker_session.container_id
    elif resolved_launch_method == "python":
        python_session = cast(PythonLaunchedSession, launched_session)
        result["process_id"] = python_session.process.pid

    return result




def _log_auto_generated_credentials(
    session_name: str,
    port: int,
    connection_url: str,
    auth_token: str,
) -> None:
    """Log auto-generated credentials prominently for user access."""
    _LOGGER.warning("=" * 70)
    _LOGGER.warning(
        f"🔑 Session '{session_name}' Created - Browser Access Information:"
    )
    _LOGGER.warning(f"   Port: {port}")
    _LOGGER.warning(f"   Base URL: {connection_url}")
    _LOGGER.warning(f"   Auth Token: {auth_token}")
    _LOGGER.warning(f"   Browser URL: {connection_url}/?psk={auth_token}")
    _LOGGER.warning("")
    _LOGGER.warning(
        "   To retrieve credentials via MCP tool, enable credential_retrieval_enabled"
    )
    _LOGGER.warning("   in your deephaven_mcp.json configuration.")
    _LOGGER.warning("=" * 70)




@mcp_server.tool()
async def session_community_create(
    context: Context,
    session_name: str,
    launch_method: str | None = None,
    programming_language: str | None = None,
    auth_type: str | None = None,
    auth_token: str | None = None,
    heap_size_gb: float | int | None = None,
    extra_jvm_args: list[str] | None = None,
    environment_vars: dict[str, str] | None = None,
    docker_image: str | None = None,
    docker_memory_limit_gb: float | None = None,
    docker_cpu_limit: float | None = None,
    docker_volumes: list[str] | None = None,
    python_venv_path: str | None = None,
) -> dict:
    """
    MCP Tool: Create a new dynamically launched Deephaven Community session.

    Creates a new Deephaven Community session by launching it via Docker or Python-launched
    Deephaven. The session is registered in the MCP server and will be automatically
    cleaned up when the MCP server shuts down.

    Launch Method Requirements:
    - Docker: Requires Docker daemon running (default method)
    - Python: Requires deephaven-server package installed (pip install deephaven-server)

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - 'COMMUNITY' sessions run Deephaven Community (also called 'Core')
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use this tool to create temporary Deephaven sessions for data analysis
    - Sessions are automatically cleaned up when MCP server shuts down
    - Check 'success' field to verify creation completed
    - Use 'connection_url' or 'connection_url_with_auth' to connect to the session
    - Save the 'session_id' to reference the session in other MCP tools
    - IMPORTANT: Created sessions consume system resources (memory, CPU, ports)
    - Delete sessions when done using session_community_delete

    Args:
        context (Context): The MCP context object.
        session_name (str): Unique name for the session. Must not conflict with existing sessions.
            Will be used to create session_id in format "community:dynamic:{session_name}".
        launch_method (str | None): How to launch the session ("docker" or "python", case-insensitive).
            - "docker": Uses Docker containers (requires Docker daemon running)
            - "python": Uses Python-launched deephaven-server (requires: pip install deephaven-server)
            Defaults to configuration value or "docker".
        programming_language (str | None): Programming language ("Python" or "Groovy", case-insensitive).
            Only applies to docker launch method - raises error if used with python launch.
            Automatically selects the appropriate Docker image:
            - "Python" → ghcr.io/deephaven/server:latest
            - "Groovy" → ghcr.io/deephaven/server-slim:latest
            Defaults to configuration value or "Python".
            Cannot be specified together with docker_image (mutually exclusive).
        auth_type (str | None): Authentication type ("PSK" or "Anonymous", case-insensitive).
            - "PSK": Pre-shared key authentication (recommended for security)
            - "Anonymous": No authentication required (less secure)
            Note: Basic authentication is not supported for dynamic sessions (requires database setup).
            Shorthand values are normalized to full Java class names internally.
            Defaults to "io.deephaven.authentication.psk.PskAuthenticationHandler".
        auth_token (str | None): Pre-shared key for PSK authentication.
            If None and auth_type is PSK, a cryptographically secure token will be auto-generated.
            Auto-generated tokens are logged at WARNING level and included in response with
            connection_url_with_auth for easy access.
        docker_image (str | None): Custom Docker image to use (docker launch only).
            For advanced users who want to use a custom image instead of standard Python/Groovy images.
            Cannot be specified together with programming_language (mutually exclusive).
            If neither docker_image nor programming_language is specified, defaults to Python image.
            Raises error if used with python launch method.
        docker_memory_limit_gb (float | None): Container memory limit in GB (docker only).
            Raises error if used with python launch method.
        docker_cpu_limit (float | None): Container CPU limit in cores (docker only).
            Raises error if used with python launch method.
        docker_volumes (list[str] | None): Volume mounts in format ["host:container:mode"] (docker only).
            Raises error if used with python launch method.
        python_venv_path (str | None): Path to custom Python venv directory (python only).
            If provided, uses the deephaven installation from that venv (e.g., "/path/to/venv").
            If None (default), uses the same venv as the MCP server.
            Raises error if used with docker launch method.
        heap_size_gb (float | int | None): JVM heap size in gigabytes (e.g., 4 or 2.5 for -Xmx4g or -Xmx2.5g).
            Applies to both docker and python launches.
            Defaults to configuration value or 4.
        extra_jvm_args (list[str] | None): Additional JVM arguments as list of strings.
        environment_vars (dict[str, str] | None): Environment variables to set in the session.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if creation succeeded, False if error occurred
            - 'session_id' (str): Full identifier in format "community:dynamic:{session_name}"
            - 'session_name' (str): Simple name provided by user
            - 'connection_url' (str): Base HTTP URL without authentication
            - 'auth_type' (str): Normalized authentication type as full class name
                (e.g., "io.deephaven.authentication.psk.PskAuthenticationHandler", "Anonymous", "Basic")
            - 'launch_method' (str): "docker" or "python" (normalized to lowercase)
            - 'port' (int): Port number where session is listening
            - 'container_id' (str, optional): Docker container ID (only for docker launch)
            - 'process_id' (int, optional): Process ID of deephaven server (only for python launch)
            - 'error' (str, optional): Error message if creation failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response

        Security Note:
            - auth_token and connection_url_with_auth are NOT included for security
            - Auto-generated tokens are logged to console (similar to Jupyter)
            - Use session_community_credentials tool to retrieve credentials programmatically
              (requires credential_retrieval_enabled=true in configuration)

        Example Success Response (docker):
        {
            "success": True,
            "session_id": "community:dynamic:my-session",
            "session_name": "my-session",
            "connection_url": "http://localhost:45123",
            "auth_type": "io.deephaven.authentication.psk.PskAuthenticationHandler",
            "launch_method": "docker",
            "port": 45123,
            "container_id": "a1b2c3d4..."
        }

        Example Success Response (python):
        {
            "success": True,
            "session_id": "community:dynamic:my-session",
            "session_name": "my-session",
            "connection_url": "http://localhost:45123",
            "auth_type": "io.deephaven.authentication.psk.PskAuthenticationHandler",
            "launch_method": "python",
            "port": 45123,
            "process_id": 98765
        }

        Example Error Response:
        {
            "success": False,
            "error": "Session limit reached: 5/5 sessions active",
            "isError": True
        }

    Validation and Safety:
        - Checks session creation is enabled in configuration
        - Enforces max_concurrent_sessions limit
        - Validates session name doesn't conflict with existing sessions
        - Auto-generates secure auth tokens if not provided
        - Waits for session to be ready before returning
        - Logs auth token with WARNING level for user visibility
        - Registers session in registry for lifecycle management

    Common Error Scenarios:
        - Session creation not configured: "Community session creation not configured in deephaven_mcp.json"
        - Session limit reached: "Session limit reached: X/Y sessions active"
        - Docker param with python: "'programming_language' parameter only applies to docker launch method, not 'python'"
        - Docker image with python: "'docker_image' parameter only applies to docker launch method, not 'python'"
        - Docker resource with python: "'docker_memory_limit_gb' parameter only applies to docker launch method, not 'python'"
        - Docker resource with python: "'docker_cpu_limit' parameter only applies to docker launch method, not 'python'"
        - Docker resource with python: "'docker_volumes' parameter only applies to docker launch method, not 'python'"
        - Invalid parameters: "Cannot specify both 'programming_language' and 'docker_image' - use one or the other"
        - Unsupported language: "Unsupported programming_language: '{language}'. Must be 'Python' or 'Groovy'"
        - Invalid config language: "Invalid programming_language in config: '{language}'. Must be 'Python' or 'Groovy'"
        - Name conflict: "Session 'community:dynamic:{name}' already exists in registry"
        - Startup timeout: "Session failed to start within {timeout} seconds"

    Note:
        - Created sessions are automatically cleaned up on MCP server shutdown
        - Sessions consume system resources - delete when no longer needed
        - Auto-generated auth tokens are logged to console at WARNING level
        - For browser access, copy the URL from console logs or use session_community_credentials tool
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_community_create] Invoked: session_name={session_name!r}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Get config and session registry
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        # Get and validate configuration
        defaults, max_concurrent_sessions, config_error = (
            await _get_session_creation_config(config_manager)
        )
        if config_error:
            return config_error

        # Check session limit
        limit_error = await _check_session_limit(
            session_registry, max_concurrent_sessions
        )
        if limit_error:
            return limit_error

        # Resolve all session parameters
        params, params_error = _resolve_community_session_parameters(
            launch_method,
            programming_language,
            auth_type,
            auth_token,
            heap_size_gb,
            extra_jvm_args,
            environment_vars,
            docker_image,
            docker_memory_limit_gb,
            docker_cpu_limit,
            docker_volumes,
            python_venv_path,
            defaults,
        )
        if params_error:
            return params_error

        # Extract resolved parameters
        resolved_launch_method = params["launch_method"]
        resolved_programming_language = params["programming_language"]
        resolved_auth_type = params["auth_type"]
        resolved_auth_token = params["auth_token"]
        auto_generated_token = params["auto_generated_token"]
        resolved_heap_size_gb = params["heap_size_gb"]
        resolved_docker_image = params["docker_image"]
        resolved_docker_memory_limit = params["docker_memory_limit_gb"]
        resolved_docker_cpu_limit = params["docker_cpu_limit"]
        resolved_docker_volumes = params["docker_volumes"]
        resolved_python_venv_path = params["python_venv_path"]
        resolved_extra_jvm_args = params["extra_jvm_args"]
        resolved_environment_vars = params["environment_vars"]
        resolved_startup_timeout = params["startup_timeout_seconds"]
        resolved_startup_interval = params["startup_check_interval_seconds"]
        resolved_startup_retries = params["startup_retries"]

        # Check for session name conflicts
        session_id = BaseItemManager.make_full_name(
            SystemType.COMMUNITY, "dynamic", session_name
        )
        if session_id in await session_registry.get_all():
            error_msg = f"Session '{session_id}' already exists in registry"
            _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.info(
            f"[mcp_systems_server:session_community_create] Creating session '{session_name}' "
            f"(method: {resolved_launch_method}, language: {resolved_programming_language}, auth: {resolved_auth_type})"
        )

        # Get instance tracker from context for orphan tracking
        instance_tracker: InstanceTracker = context.request_context.lifespan_context[
            "instance_tracker"
        ]

        # Launch session and wait for readiness
        launched_session, port, launch_error = await _launch_process_and_wait_for_ready(
            session_name,
            cast(Literal["docker", "python"], resolved_launch_method),
            resolved_auth_token,
            resolved_heap_size_gb,
            resolved_extra_jvm_args,
            resolved_environment_vars,
            resolved_docker_image,
            resolved_docker_memory_limit,
            resolved_docker_cpu_limit,
            resolved_docker_volumes,
            resolved_python_venv_path,
            resolved_startup_timeout,
            resolved_startup_interval,
            resolved_startup_retries,
            instance_tracker,
        )
        if launch_error or launched_session is None or port is None:
            return launch_error or {
                "success": False,
                "error": "Session launch failed",
                "isError": True,
            }

        # Create and register session manager
        await _register_session_manager(
            session_name,
            session_id,
            port,
            resolved_programming_language,
            resolved_auth_type,
            resolved_auth_token,
            launched_session,
            session_registry,
            instance_tracker,
        )

        # Log auto-generated credentials prominently
        if auto_generated_token and resolved_auth_token:
            _log_auto_generated_credentials(
                session_name,
                port,
                launched_session.connection_url,
                resolved_auth_token,
            )

        # Build and return success response
        return _build_success_response(
            session_id,
            session_name,
            launched_session.connection_url,
            resolved_auth_type,
            resolved_launch_method,
            port,
            launched_session,
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_community_create] Failed to create session '{session_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to create community session '{session_name}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result




@mcp_server.tool()
async def session_community_delete(
    context: Context,
    session_name: str,
) -> dict:
    """
    MCP Tool: Delete a dynamically created Deephaven Community session.

    Deletes a community session that was created via session_community_create.
    This stops the underlying Docker container or pip process and removes the
    session from the registry.

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - 'COMMUNITY' sessions run Deephaven Community (also called 'Core')
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage:
    - Use this tool to clean up sessions when no longer needed to free resources
    - Always check 'success' field first to verify deletion completed
    - This operation is irreversible - deleted sessions cannot be recovered
    - Only dynamically created sessions (source='dynamic') can be deleted
    - Static sessions from configuration cannot be deleted (will return error)
    - After successful deletion, session_id will no longer be valid for other MCP tools
    - Deletion stops the Docker container or kills the pip process

    Args:
        context (Context): The MCP context object.
        session_name (str): Name of the session to delete (without "community:dynamic:" prefix).
            Must be a dynamically created session from session_community_create.
            Static sessions from configuration files cannot be deleted.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if deletion succeeded, False if error occurred
            - 'session_id' (str): Full identifier in format "community:dynamic:{session_name}"
            - 'session_name' (str): Simple name provided by user
            - 'error' (str, optional): Error message if deletion failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response

        Example Success Response:
        {
            "success": True,
            "session_id": "community:dynamic:my-session",
            "session_name": "my-session"
        }

        Example Error Response:
        {
            "success": False,
            "error": "Session 'community:dynamic:nonexistent' not found",
            "isError": True
        }

    Validation and Safety:
        - Verifies session exists in registry
        - Checks that session is dynamically created (source='dynamic')
        - Properly closes the session connection
        - Stops the underlying Docker container or pip process
        - Removes session from registry to prevent future access
        - Provides detailed error messages for troubleshooting

    Common Error Scenarios:
        - Session not found: "Session 'community:dynamic:{name}' not found"
        - Not a community session: "Session '{session_id}' is not a community session"
        - Not a dynamic session: "Session '{session_id}' is not a dynamically created session (source: '{source}'). Only dynamically created sessions can be deleted."
        - Already deleted: "Session 'community:dynamic:{name}' not found"
        - Cleanup failure: "Failed to close session '{session_id}': {error}"
        - Registry removal failure: "Failed to remove session '{session_id}' from registry: {error}"

    Note:
        - This operation is irreversible - deleted sessions cannot be recovered
        - Any running queries or tables in the session will be lost
        - The Docker container or pip process will be terminated
        - Use with caution - ensure you have saved any important data
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_community_delete] Invoked: session_name={session_name!r}"
    )

    result: dict[str, object] = {"success": False}

    try:
        # Get session registry
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        # Create expected session ID for dynamic sessions
        session_id = BaseItemManager.make_full_name(
            SystemType.COMMUNITY, "dynamic", session_name
        )

        _LOGGER.debug(
            f"[mcp_systems_server:session_community_delete] Looking for session '{session_id}'"
        )

        # Check if session exists in registry
        try:
            session_manager = await session_registry.get(session_id)
        except KeyError:
            error_msg = f"Session '{session_id}' not found"
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Verify it's a dynamic community session
        if session_manager.system_type != SystemType.COMMUNITY:
            error_msg = f"Session '{session_id}' is not a community session"
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        if session_manager.source != "dynamic":
            error_msg = (
                f"Session '{session_id}' is not a dynamically created session "
                f"(source: '{session_manager.source}'). Only dynamically created sessions can be deleted."
            )
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.debug(
            f"[mcp_systems_server:session_community_delete] Found dynamic community session manager for '{session_id}'"
        )

        # Untrack python process if applicable (before closing)
        instance_tracker: InstanceTracker = context.request_context.lifespan_context[
            "instance_tracker"
        ]
        if isinstance(session_manager, DynamicCommunitySessionManager):
            if isinstance(session_manager.launched_session, PythonLaunchedSession):
                await instance_tracker.untrack_python_process(session_name)

        # Close the session (this will also stop the Docker container/python process)
        try:
            _LOGGER.debug(
                f"[mcp_systems_server:session_community_delete] Closing session '{session_id}'"
            )
            await session_manager.close()
            _LOGGER.debug(
                f"[mcp_systems_server:session_community_delete] Successfully closed session '{session_id}'"
            )
        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_delete] Failed to close session '{session_id}': {e}"
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
                    f"[mcp_systems_server:session_community_delete] {error_msg}"
                )
            else:
                _LOGGER.debug(
                    f"[mcp_systems_server:session_community_delete] Removed session '{session_id}' from registry"
                )

        except Exception as e:
            error_msg = f"Failed to remove session '{session_id}' from registry: {e}"
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.info(
            f"[mcp_systems_server:session_community_delete] Successfully deleted session "
            f"'{session_name}' (session ID: '{session_id}')"
        )

        result.update(
            {
                "success": True,
                "session_id": session_id,
                "session_name": session_name,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_community_delete] Failed to delete session '{session_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to delete community session '{session_name}': {type(e).__name__}: {e}"
        )
        result["isError"] = True

    return result




@mcp_server.tool()
async def session_community_credentials(
    context: Context,
    session_id: str,
) -> dict:
    """
    SECURITY SENSITIVE: Retrieve connection credentials for browser access.

    Returns authentication credentials for connecting to a Deephaven Community session
    via web browser. This tool exposes sensitive credentials and should only be called
    when the user explicitly needs browser access.

    IMPORTANT: This tool is DISABLED by default for security. To enable, add to your
    deephaven_mcp.json configuration:

    {
      "security": {
        "community": {
          "credential_retrieval_mode": "dynamic_only"  // or "all", "static_only"
        }
      }
    }

    Valid credential_retrieval_mode values:
    - "none": Disabled (secure default)
    - "dynamic_only": Only auto-generated tokens (dynamic sessions)
    - "static_only": Only pre-configured tokens (static sessions)
    - "all": Both dynamic and static session credentials

    Terminology Note:
    - 'Session' and 'worker' are interchangeable terms - both refer to a running Deephaven instance
    - 'Deephaven Community' and 'Deephaven Core' are interchangeable names for the same product
    - 'Deephaven Enterprise', 'Deephaven Core+', and 'Deephaven CorePlus' are interchangeable names for the same product
    - In Deephaven, "schema" and "meta table" refer to the same concept - the table's column definitions including names, types, and properties.
    - In Deephaven, "catalog" and "database" are interchangeable terms - the catalog is the database of available tables.
    - 'DHC' is shorthand for Deephaven Community (also called 'Core')
    - 'DHE' is shorthand for Deephaven Enterprise (also called 'Core+')

    AI Agent Usage Guidelines:
    - **When to Call**: Only when user explicitly requests browser access, connection URL,
      or credentials. Do not call proactively or for informational purposes.
    - **Credential Handling**: Never cache, log, or display credentials in plain text unless
      user specifically asks. Treat auth_token as sensitive data.
    - **Error Handling**: If tool returns disabled error, inform user that credential
      retrieval is disabled for security and guide them to enable it in configuration.
    - **Session Types**: Works for both static (config-based) and dynamic (on-demand) sessions,
      but access is controlled by credential_retrieval_mode setting.
    - **Mode Selection Guidance**:
        * "dynamic_only": Recommended for development - allows retrieving auto-generated tokens
        * "static_only": For controlled environments with pre-configured credentials
        * "all": Maximum flexibility but requires careful security consideration
        * "none": Default - no credential retrieval allowed (most secure)

    Security Note:
    - Credentials are returned in plain text
    - All calls are logged for security audit
    - Only use for legitimate browser access needs
    - Disabled by default - must be explicitly enabled in configuration

    Args:
        context (Context): MCP context provided by the MCP framework
        session_id (str): Session ID in format "community:source:name" where:
            - source="config" for static (configuration-based) sessions
            - source="dynamic" for dynamic (on-demand created) sessions
            - name is the unique session identifier within that source
            Examples: "community:config:local-dev", "community:dynamic:my-session"

    Returns:
        dict: Response structure varies based on success/failure:

        On Success (success=True):
            - success (bool): Always True
            - auth_type (str): Authentication type - "PSK" (pre-shared key) or "ANONYMOUS"
            - auth_token (str): Authentication token string. For PSK auth, contains the token value.
                For ANONYMOUS auth, returns empty string "". Never None.
            - connection_url (str): Base server URL without authentication parameters.
                Format: "http://host:port" or "https://host:port"
                Example: "http://localhost:45123"
            - connection_url_with_auth (str): Complete browser-ready URL including auth token if applicable.
                For PSK: Base URL + "/?psk={token}"
                For ANONYMOUS: Same as connection_url (no auth parameter needed)

        On Failure (success=False):
            - success (bool): Always False
            - error (str): Human-readable error message explaining the failure
            - isError (bool): Always True to indicate error condition

    Example Success Response (PSK Authentication):
        {
            "success": True,
            "auth_type": "PSK",
            "auth_token": "abc123xyz789...",
            "connection_url": "http://localhost:45123",
            "connection_url_with_auth": "http://localhost:45123/?psk=abc123xyz789"
        }

    Example Success Response (ANONYMOUS Authentication):
        {
            "success": True,
            "auth_type": "ANONYMOUS",
            "auth_token": "",
            "connection_url": "http://localhost:45123",
            "connection_url_with_auth": "http://localhost:45123"
        }

    Example Disabled Response:
        {
            "success": False,
            "error": "Credential retrieval is disabled (mode='none'). To enable, configure in deephaven_mcp.json...",
            "isError": True
        }

    Example Session Not Found Response:
        {
            "success": False,
            "error": "Session 'community:config:my-session' not found: ...",
            "isError": True
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_community_credentials] Invoked for session_id: {session_id}"
    )

    try:
        # Get config manager from context
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]

        # Check credential retrieval mode from security config
        config = await config_manager.get_config()
        security_config = config.get("security", {})
        security_community_config = security_config.get("community", {})
        credential_retrieval_mode = security_community_config.get(
            "credential_retrieval_mode", "none"
        )

        # Validate session_id format - must be a community session
        if not session_id.startswith("community:"):
            return {
                "success": False,
                "error": f"Invalid session_id '{session_id}'. This tool only works for community sessions (format: 'community:config:name' or 'community:dynamic:name')",
                "isError": True,
            }

        # Check if credential retrieval is disabled globally (mode='none')
        if credential_retrieval_mode == "none":
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_credentials] DENIED: Credential retrieval disabled (mode='none') for session_id '{session_id}'"
            )
            return {
                "success": False,
                "error": (
                    "Credential retrieval is disabled (mode='none'). To enable, configure in deephaven_mcp.json:\n\n"
                    "Available modes:\n"
                    '  - "none": Disable all credential retrieval (secure default)\n'
                    '  - "dynamic_only": Allow only auto-generated session credentials\n'
                    '  - "static_only": Allow only pre-configured session credentials\n'
                    '  - "all": Allow all credential retrieval\n\n'
                    "Configuration example:\n"
                    "{\n"
                    '  "security": {\n'
                    '    "community": {\n'
                    '      "credential_retrieval_mode": "dynamic_only"\n'
                    "    }\n"
                    "  }\n"
                    "}\n\n"
                    "Documentation: https://github.com/deephaven/deephaven-mcp/"
                ),
                "isError": True,
            }

        # Get session registry and session manager
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )

        try:
            mgr = await session_registry.get(session_id)
        except Exception as e:
            return {
                "success": False,
                "error": f"Session '{session_id}' not found: {str(e)}",
                "isError": True,
            }

        # Verify it's a community session manager
        if not isinstance(mgr, CommunitySessionManager):
            return {
                "success": False,
                "error": f"Session '{session_id}' is not a community session",
                "isError": True,
            }

        # Determine session type
        is_dynamic = isinstance(mgr, DynamicCommunitySessionManager)
        is_static = not is_dynamic

        # Check mode-specific permissions
        if credential_retrieval_mode == "dynamic_only" and is_static:
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_credentials] DENIED: Static session credential retrieval disabled (mode='dynamic_only') for session_id '{session_id}'"
            )
            return {
                "success": False,
                "error": (
                    f"Credential retrieval for static sessions is disabled. Current mode: 'dynamic_only'. "
                    f"Session '{session_id}' is a static (config-based) session. "
                    f"To retrieve static session credentials, set security.community.credential_retrieval_mode to 'all' or 'static_only' in deephaven_mcp.json."
                ),
                "isError": True,
            }
        elif credential_retrieval_mode == "static_only" and is_dynamic:
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_credentials] DENIED: Dynamic session credential retrieval disabled (mode='static_only') for session_id '{session_id}'"
            )
            return {
                "success": False,
                "error": (
                    f"Credential retrieval for dynamic sessions is disabled. Current mode: 'static_only'. "
                    f"Session '{session_id}' is a dynamic (on-demand) session. "
                    f"To retrieve dynamic session credentials, set security.community.credential_retrieval_mode to 'all' or 'dynamic_only' in deephaven_mcp.json."
                ),
                "isError": True,
            }

        # Credential retrieval is allowed - proceed
        session_type_str = "dynamic" if is_dynamic else "static"
        _LOGGER.warning(
            f"[mcp_systems_server:session_community_credentials] SECURITY: Credential retrieval ALLOWED (mode='{credential_retrieval_mode}', type='{session_type_str}') for session_id '{session_id}'"
        )

        # Get credentials based on session type
        if is_dynamic:
            # Dynamic session - get from launched_session
            dynamic_mgr = cast(DynamicCommunitySessionManager, mgr)
            auth_token = (
                dynamic_mgr.launched_session.auth_token
                if dynamic_mgr.launched_session.auth_token
                else ""
            )
            connection_url = dynamic_mgr.connection_url
            connection_url_with_auth = dynamic_mgr.connection_url_with_auth
            auth_type = dynamic_mgr.launched_session.auth_type.upper()
        else:
            # Static session - get from config
            server = mgr._config.get("server", "")
            auth_token = mgr._config.get("auth_token", "")
            auth_type = mgr._config.get("auth_type", "ANONYMOUS").upper()

            # Build connection URL with auth if token exists
            connection_url = server
            if auth_token:
                connection_url_with_auth = f"{server}/?psk={auth_token}"
            else:
                connection_url_with_auth = server

        result = {
            "success": True,
            "auth_type": auth_type,
            "auth_token": auth_token,
            "connection_url": connection_url,
            "connection_url_with_auth": connection_url_with_auth,
        }

        _LOGGER.warning(
            f"[mcp_systems_server:session_community_credentials] SECURITY: Credentials retrieved for session_id '{session_id}'"
        )

        return result

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_community_credentials] Failed: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}
