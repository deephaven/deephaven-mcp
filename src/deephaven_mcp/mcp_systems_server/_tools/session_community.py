"""Community Session MCP Tools - Create and Manage Community Sessions.

Provides MCP tools for managing Deephaven Community sessions:
- session_community_create: Create new Community sessions (Docker or Python)
- session_community_delete: Delete Community sessions
- session_community_credentials: Get connection credentials for Community sessions

These tools work with Deephaven Community (Core) sessions only.
"""

import logging
from dataclasses import dataclass
from typing import Any, cast, get_args

from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp._exception_utils import exception_summary
from deephaven_mcp._exceptions import (
    InternalError,
    InvalidSessionNameError,
    RegistryItemNotFoundError,
    SessionCreationError,
    SessionLaunchError,
)
from deephaven_mcp._names import validate_resource_name
from deephaven_mcp.auth.credentials import (
    AnonymousCredentials,
    CredentialsUnion,
    CustomTokenCredentials,
    PasswordCredentials,
    PSKCredentials,
)
from deephaven_mcp.config.schema import (
    CommunitySessionCreationDefaults,
    CommunitySettings,
    LaunchMethod,
)
from deephaven_mcp.mcp_systems_server._tools.shared import (
    check_session_limit,
    error_response,
    get_community_registry,
    get_community_settings,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionManager,
    CommunitySessionRegistry,
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    InstanceTracker,
    LaunchedSession,
    PythonLaunchedSession,
    QualifiedSessionId,
    SessionId,
    SessionOrigin,
    StaticCommunitySessionManager,
    SystemType,
    find_available_port,
    generate_auth_token,
    launch_session,
)
from deephaven_mcp.sessions import CommunitySessionConfig

_LOGGER = logging.getLogger(__name__)

_VALID_LAUNCH_METHODS: frozenset[str] = frozenset(get_args(LaunchMethod))
"""Runtime membership set for ``LaunchMethod``, derived via ``typing.get_args``."""

_PSK_AUTH_HANDLER = "io.deephaven.authentication.psk.PskAuthenticationHandler"
"""Fully-qualified class name (FQCN) of the Deephaven worker-side Java
authentication handler for Pre-Shared Key (PSK) auth.

This is the value passed to a launched community worker so it knows
which Java auth handler to instantiate. It is also used internally as
the canonical ``auth_type`` string for PSK sessions (the single source
of truth used by :func:`_credentials_to_auth_type`,
:func:`_resolve_auth_token`, and :func:`_register_session_manager`).
"""

_ANONYMOUS_AUTH_HANDLER = "Anonymous"
"""Sentinel ``auth_type`` string for anonymous (no-credential) workers.

Unlike :data:`_PSK_AUTH_HANDLER`, this is not a Java FQCN — Deephaven
treats the literal string ``"Anonymous"`` as the marker for the
anonymous authentication path. Used as the canonical ``auth_type``
value for sessions backed by :class:`AnonymousCredentials`.
"""


# ==============================================================================
# Community Session Management Tools
# =============================================================================


def _get_session_creation_config(
    settings: CommunitySettings,
) -> tuple[CommunitySessionCreationDefaults, int | None]:
    """Extract the typed session-creation defaults from ``CommunitySettings``.

    Args:
        settings: The validated :class:`CommunitySettings` model.

    Returns:
        Tuple of (defaults_model, max_concurrent_sessions).
        ``max_concurrent_sessions`` is ``None`` when the operator
        disabled the cap (unbounded).

    Raises:
        SessionCreationError: If ``community/settings.json`` has no
            ``session_creation`` block.
    """
    session_creation = settings.session_creation
    if session_creation is None:
        error_msg = (
            "Community session creation not configured in "
            "community/settings.json (missing 'session_creation' block)."
        )
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        raise SessionCreationError(error_msg)

    return (
        session_creation.defaults,
        session_creation.max_concurrent_sessions,
    )


async def _check_session_limit(
    session_registry: CommunitySessionRegistry,
    max_concurrent_sessions: int | None,
) -> dict | None:
    """Check if community session limit has been reached.

    Args:
        session_registry (CommunitySessionRegistry): The community child
            registry whose dynamically added sessions are being counted.
        max_concurrent_sessions (int | None): Maximum concurrent
            dynamically added community sessions allowed. ``None``
            disables the cap (unbounded).

    Returns:
        dict | None: ``None`` if the cap is disabled or not yet reached;
            otherwise a structured error dict produced by
            :func:`error_response`.
    """
    return await check_session_limit(
        session_registry,
        SystemType.COMMUNITY,
        SystemType.COMMUNITY.value,
        max_concurrent_sessions,
        "session_community_create",
        "Session limit reached: {current}/{max} sessions active",
    )


def _validate_launch_method_params(
    launch_method: LaunchMethod,
    programming_language: str | None,
    docker_image: str | None,
    docker_memory_limit_gb: float | None,
    docker_cpu_limit: float | None,
    docker_volumes: list[str] | None,
    python_venv_path: str | None,
) -> None:
    """Validate that method-specific parameters are only used with their respective launch methods.

    Ensures docker-only parameters are not used with python launch method,
    python-only parameters are not used with docker launch method, and
    mutually exclusive parameters are not used together.

    Args:
        launch_method (LaunchMethod): Launch method ("docker" or "python").
        programming_language (str | None): Docker-only parameter.
        docker_image (str | None): Docker-only parameter.
        docker_memory_limit_gb (float | None): Docker-only parameter.
        docker_cpu_limit (float | None): Docker-only parameter.
        docker_volumes (list[str] | None): Docker-only parameter.
        python_venv_path (str | None): Python-only parameter.

    Raises:
        SessionCreationError: If a parameter is used with the wrong
            launch method, or mutually exclusive parameters are
            combined.
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
            raise SessionCreationError(error_msg)

    # Python-only parameters
    if python_venv_path and launch_method != "python":
        error_msg = f"'python_venv_path' parameter only applies to python launch method, not '{launch_method}'"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        raise SessionCreationError(error_msg)

    # Check mutual exclusivity
    if programming_language and docker_image:
        error_msg = "Cannot specify both 'programming_language' and 'docker_image' - use one or the other"
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        raise SessionCreationError(error_msg)


def _resolve_docker_image(
    programming_language: str | None,
    docker_image: str | None,
    defaults: CommunitySessionCreationDefaults,
) -> str:
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
        str: Resolved Docker image name.

    Raises:
        SessionCreationError: If ``programming_language`` is not
            "Python" or "Groovy" (case-insensitive).
    """
    if docker_image:
        return docker_image

    if programming_language:
        lang_lower = programming_language.lower()
        if lang_lower == "python":
            return defaults.docker.images.python
        elif lang_lower == "groovy":
            return defaults.docker.images.groovy
        else:
            error_msg = f"Unsupported programming_language: '{programming_language}'. Must be 'Python' or 'Groovy'"
            _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
            raise SessionCreationError(error_msg)

    # Use config defaults (programming_language is a Literal
    # validated by Pydantic, so it is always "Python" or "Groovy").
    if defaults.programming_language == "Python":
        return defaults.docker.images.python
    return defaults.docker.images.groovy


@dataclass(frozen=True)
class _ResolvedSessionParams:
    """Fully resolved community session creation parameters."""

    launch_method: LaunchMethod
    """Resolved launch method."""

    programming_language: str
    """Resolved programming language."""

    auth_type: str
    """Worker-side auth handler FQCN, or the anonymous sentinel."""

    auth_token: str | None
    """Resolved or auto-generated auth token; None for anonymous."""

    auto_generated_token: bool
    """True if the auth token was auto-generated."""

    heap_size_gb: float | int
    """JVM heap size in gigabytes."""

    docker_image: str
    """Resolved docker image; empty string for python launch."""

    docker_memory_limit_gb: float | None
    """Docker memory limit in GB, or None for no limit."""

    docker_cpu_limit: float | None
    """Docker CPU limit in cores, or None for no limit."""

    docker_volumes: list[str]
    """Docker volume mounts; empty for python launch."""

    python_venv_path: str | None
    """Python venv path; None for docker launch."""

    extra_jvm_args: list[str]
    """Additional JVM arguments."""

    environment_vars: dict[str, str]
    """Environment variables for the worker process."""

    startup_timeout_seconds: float
    """Health-check timeout in seconds."""

    startup_check_interval_seconds: float
    """Health-check poll interval in seconds."""

    startup_retries: int
    """Maximum retries per health check."""


def _resolve_community_session_parameters(
    launch_method: LaunchMethod | None,
    programming_language: str | None,
    auth_token: str | None,
    heap_size_gb: float | int | None,
    extra_jvm_args: list[str] | None,
    environment_vars: dict[str, str] | None,
    docker_image: str | None,
    docker_memory_limit_gb: float | None,
    docker_cpu_limit: float | None,
    docker_volumes: list[str] | None,
    python_venv_path: str | None,
    defaults: CommunitySessionCreationDefaults,
) -> _ResolvedSessionParams:
    """Resolve all community session creation parameters from tool args, config defaults, and hardcoded defaults.

    This function implements the parameter resolution priority: tool parameter > config default > hardcoded default.
    It validates parameters, normalizes values, and returns a complete set of resolved parameters for session creation.

    The worker-side authentication handler (``auth_type``) is derived
    from ``defaults.credentials`` via :func:`_credentials_to_auth_type`
    rather than exposed as an independent knob; see that function's
    docstring for the rationale.

    Args:
        launch_method (LaunchMethod | None): Launch method ("docker" or "python"), or None to use default
        programming_language (str | None): Programming language ("Python" or "Groovy"), or None to use default
        auth_token (str | None): Authentication token, or None to auto-generate for PSK auth
        heap_size_gb (float | int | None): JVM heap size in GB (e.g., 4 or 2.5), or None to use default
        extra_jvm_args (list[str] | None): Additional JVM arguments, or None to use default
        environment_vars (dict[str, str] | None): Environment variables, or None to use default
        docker_image (str | None): Docker image name (docker only), or None to auto-select based on language
        docker_memory_limit_gb (float | None): Docker memory limit in GB (docker only), or None for no limit
        docker_cpu_limit (float | None): Docker CPU limit (docker only), or None for no limit
        docker_volumes (list[str] | None): Docker volume mounts (docker only), or None to use default
        python_venv_path (str | None): Python venv path (python only), or None to use default
        defaults (dict): Configuration defaults from ``community/settings.json`` (in your configuration directory)

    Returns:
        _ResolvedSessionParams: The fully resolved creation parameters.

    Raises:
        SessionCreationError: If the launch method is unrecognized, the
            configured credentials kind is unsupported for dynamic
            workers, a parameter is used with the wrong launch method,
            or the programming language is unsupported.
    """
    # Resolve and validate launch method (runtime validation retained for
    # untyped callers; typed callers are constrained by LaunchMethod)
    method_str = (launch_method or defaults.launch_method).lower()
    if method_str not in _VALID_LAUNCH_METHODS:
        valid_options = ", ".join(f"'{m}'" for m in sorted(_VALID_LAUNCH_METHODS))
        error_msg = (
            f"Invalid launch_method '{method_str}'. Valid options: {valid_options}."
        )
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
        raise SessionCreationError(error_msg)
    # Safe: membership in the get_args-derived set proves method_str is a
    # LaunchMethod member.
    resolved_launch_method = cast(LaunchMethod, method_str)

    # Resolve auth type
    # Derive worker-side auth handler FQCN from credentials kind (single
    # source of truth; no parallel auth_type knob to drift out of sync).
    resolved_auth_type = _credentials_to_auth_type(defaults.credentials)

    # Validate method-specific parameters
    _validate_launch_method_params(
        resolved_launch_method,
        programming_language,
        docker_image,
        docker_memory_limit_gb,
        docker_cpu_limit,
        docker_volumes,
        python_venv_path,
    )

    # Resolve programming_language for both launch methods
    # This determines both the Docker image selection (for docker) and the
    # session's programming_language property (forwarded to the session config).
    resolved_programming_language = (
        programming_language or defaults.programming_language
    )

    # Resolve docker image (only for docker launch method)
    if resolved_launch_method == "docker":
        resolved_docker_image = _resolve_docker_image(
            programming_language, docker_image, defaults
        )
    else:
        # For python launch, ensure no docker image is set
        resolved_docker_image = ""

    # Resolve heap size
    resolved_heap_size_gb = heap_size_gb or defaults.heap_size_gb

    # Resolve startup parameters from typed defaults (not exposed as tool parameters)
    resolved_startup_timeout = defaults.startup_timeout_seconds
    resolved_startup_interval = defaults.startup_check_interval_seconds
    resolved_startup_retries = defaults.startup_retries

    # Resolve optional parameters based on launch method
    if resolved_launch_method == "docker":
        resolved_docker_memory_limit = (
            docker_memory_limit_gb or defaults.docker.memory_limit_gb
        )
        resolved_docker_cpu_limit = docker_cpu_limit or defaults.docker.cpu_limit
        resolved_docker_volumes = docker_volumes or (defaults.docker.volumes or [])
        resolved_python_venv_path = None
    else:  # python
        resolved_docker_memory_limit = None
        resolved_docker_cpu_limit = None
        resolved_docker_volumes = []
        resolved_python_venv_path = python_venv_path or defaults.python.venv_path

    resolved_extra_jvm_args = extra_jvm_args or (defaults.extra_jvm_args or [])
    resolved_environment_vars = environment_vars or (defaults.environment_vars or {})

    # Resolve auth token
    resolved_auth_token, auto_generated_token = _resolve_auth_token(
        resolved_auth_type, auth_token, defaults
    )

    return _ResolvedSessionParams(
        launch_method=resolved_launch_method,
        programming_language=resolved_programming_language,
        auth_type=resolved_auth_type,
        auth_token=resolved_auth_token,
        auto_generated_token=auto_generated_token,
        heap_size_gb=resolved_heap_size_gb,
        docker_image=resolved_docker_image,
        docker_memory_limit_gb=resolved_docker_memory_limit,
        docker_cpu_limit=resolved_docker_cpu_limit,
        docker_volumes=resolved_docker_volumes,
        python_venv_path=resolved_python_venv_path,
        extra_jvm_args=resolved_extra_jvm_args,
        environment_vars=resolved_environment_vars,
        startup_timeout_seconds=resolved_startup_timeout,
        startup_check_interval_seconds=resolved_startup_interval,
        startup_retries=resolved_startup_retries,
    )


def _credentials_to_auth_type(
    credentials: CredentialsUnion | None,
) -> str:
    """Derive the worker-side Java auth handler FQCN from typed credentials.

    The handler class the worker is launched with is uniquely
    determined by the credentials kind: PSK → PSK handler, anonymous
    → Anonymous, custom → the FQCN carried on the credential itself.
    Deriving the handler this way (instead of exposing a parallel
    ``auth_type`` knob) is the single-source-of-truth fix for the
    "credentials and handler disagree" footgun that an independent
    field would create.

    ``None`` falls back to PSK — the historical default for
    dynamically-launched community workers — with the token resolved
    by :func:`_resolve_auth_token`.

    Args:
        credentials (CredentialsUnion | None): Typed credentials from
            ``community/settings.json``'s ``session_creation.defaults``.

    Returns:
        str: The worker-side auth handler FQCN (or the anonymous
            sentinel).

    Raises:
        SessionCreationError: If the credentials kind cannot back a
            dynamically-launched worker. Today that is
            :class:`PasswordCredentials` — freshly launched workers
            have no pre-configured user database for Basic auth to
            validate against (static community sessions and
            enterprise systems still accept password credentials) —
            or an unrecognized credentials class.
    """
    if credentials is None or isinstance(credentials, PSKCredentials):
        return _PSK_AUTH_HANDLER
    if isinstance(credentials, AnonymousCredentials):
        return _ANONYMOUS_AUTH_HANDLER
    if isinstance(credentials, CustomTokenCredentials):
        return credentials.auth_type
    if isinstance(credentials, PasswordCredentials):
        raise SessionCreationError(
            "Basic authentication is not supported for dynamically-"
            "launched workers because they have no pre-configured "
            "user database for Basic to validate against. Use PSK or "
            "anonymous credentials here, or declare a static session "
            "under community/sessions/ if you have a pre-existing "
            "worker with Basic auth set up."
        )
    raise SessionCreationError(
        f"Unsupported credentials kind for dynamic community session: "
        f"{type(credentials).__name__}"
    )


def _resolve_auth_token(
    auth_type: str,
    auth_token: str | None,
    defaults: CommunitySessionCreationDefaults,
) -> tuple[str | None, bool]:
    """Resolve authentication token, auto-generating if needed.

    Args:
        auth_type (str): Normalized authentication type (should be full class name from _normalize_auth_type).
        auth_token (str | None): Explicit auth token parameter, or None.
        defaults (CommunitySessionCreationDefaults): Typed defaults from
            ``community/settings.json``. The typed ``credentials`` field
            carries the resolved PSK token (via ``SecretStr``) when
            the JSON declared one.

    Returns:
        tuple[str | None, bool]: (resolved_token, was_auto_generated).
            - (None, False) if auth_type doesn't require a token
            - (token_string, False) if token was provided or from config
            - (token_string, True) if token was auto-generated
    """
    # Only the PSK handler requires (and consumes) a token.
    if auth_type != _PSK_AUTH_HANDLER:
        return None, False

    # Check explicit parameter
    if auth_token:
        return auth_token, False

    # Pull the resolved token from the typed credentials, if any.
    # Env-var indirection in the JSON has already been collapsed at
    # validation time (the templating engine resolved ${env:VAR} into
    # the literal value before the model was built).
    creds = defaults.credentials
    if isinstance(creds, PSKCredentials):
        token = creds.token.get_secret_value()
        if token:
            return token, False

    # Auto-generate
    token = generate_auth_token()
    _LOGGER.debug(
        "[mcp_systems_server:session_community_create] Auto-generated auth token"
    )
    return token, True


async def _register_session_manager(
    session_name: str,
    port: int,
    programming_language: str,
    resolved_auth_type: str,
    resolved_auth_token: str | None,
    launched_session: DockerLaunchedSession | PythonLaunchedSession,
    session_registry: CommunitySessionRegistry,
    instance_tracker: InstanceTracker,
) -> str:
    """Build a session config and register a dynamic community session.

    The registry owns :class:`~deephaven_mcp.client.CommunityTimeouts`
    and constructs the manager itself; this helper only assembles the
    wire-format :class:`CommunitySessionConfig`, hands it to the
    registry, and tracks the Python launcher for orphan cleanup.

    Args:
        session_name (str): Simple session name (not full id).
        port (int): Port number where the session is listening.
        programming_language (str): Programming language for the session
            (``"Python"`` or ``"Groovy"``).
        resolved_auth_type (str): Normalized authentication type
            (full class name).
        resolved_auth_token (str | None): Authentication token if
            applicable.
        launched_session (DockerLaunchedSession | PythonLaunchedSession):
            The launched session object.
        session_registry (CommunitySessionRegistry): Registry that
            constructs the manager (using its own timeouts) and stores it.
        instance_tracker (InstanceTracker): Tracker for orphan-process
            cleanup. Python launches are recorded here.

    Returns:
        str: The canonical ``id`` (``"community:community:<session_name>"``)
            assigned by the registry.
    """
    # Build a typed CommunitySessionConfig describing how to connect to
    # the launched session. Authentication lives entirely inside
    # ``auth.credentials``; the model unwraps that to the typed
    # ``credentials`` field.
    credentials_block: dict[str, Any]
    if resolved_auth_type == _PSK_AUTH_HANDLER:
        credentials_block = {
            "type": "psk",
            "token": resolved_auth_token or "",
        }
    elif resolved_auth_type == _ANONYMOUS_AUTH_HANDLER:
        credentials_block = {"type": "anonymous"}
    else:
        # Any other authenticator class name is forwarded as a custom
        # auth handler; the dynamic launcher does not generate this
        # path today but the schema supports it for forward compat.
        credentials_block = {
            "type": "custom",
            "auth_type": resolved_auth_type,
            "auth_token": resolved_auth_token or "",
        }
    session_config = CommunitySessionConfig.model_validate(
        {
            "name": session_name,
            "host": "localhost",
            "port": port,
            "programming_language": programming_language,
            "auth": {"credentials": credentials_block},
        }
    )

    # Registry owns the timeouts and constructs the manager. The
    # community SessionId is just ``session_name`` itself.
    manager = await session_registry.add_dynamic_session(
        name=session_name,
        session_config=session_config,
        launched_session=launched_session,
    )
    id = manager.qualified_session_id

    # Track python process if applicable
    if isinstance(launched_session, PythonLaunchedSession):
        await instance_tracker.track_python_process(
            session_name, launched_session.process.pid
        )

    _LOGGER.info(
        f"[mcp_systems_server:session_community_create] Successfully created and registered session '{id}'"
    )
    return str(id)


async def _launch_process_and_wait_for_ready(
    session_name: str,
    resolved_launch_method: LaunchMethod,
    resolved_auth_token: str | None,
    resolved_heap_size_gb: float | int,
    resolved_extra_jvm_args: list[str],
    resolved_environment_vars: dict[str, str],
    resolved_docker_image: str,
    resolved_docker_memory_limit: float | None,
    resolved_docker_cpu_limit: float | None,
    resolved_docker_volumes: list[str],
    resolved_python_venv_path: str | None,
    resolved_startup_timeout: float,
    resolved_startup_interval: float,
    resolved_startup_retries: int,
    instance_tracker: InstanceTracker,
) -> tuple[DockerLaunchedSession | PythonLaunchedSession, int]:
    """Launch Docker container or Python process and wait for health check.

    Finds an available port, launches the session using the specified method,
    and waits for it to become ready via HTTP health checks.

    Args:
        session_name (str): Name for the session.
        resolved_launch_method (LaunchMethod): Launch method.
        resolved_auth_token (str | None): PSK authentication token, or None for anonymous.
        resolved_heap_size_gb (float | int): JVM heap size in gigabytes (e.g., 4 or 2.5).
        resolved_extra_jvm_args (list[str]): Additional JVM arguments.
        resolved_environment_vars (dict[str, str]): Environment variables.
        resolved_docker_image (str): Docker image (used only if docker launch).
        resolved_docker_memory_limit (float | None): Docker memory limit in GB (docker only).
        resolved_docker_cpu_limit (float | None): Docker CPU limit (docker only).
        resolved_docker_volumes (list[str]): Docker volume mounts (docker only).
        resolved_python_venv_path (str | None): Python venv path (python only).
        resolved_startup_timeout (float): Health check timeout in seconds.
        resolved_startup_interval (float): Health check interval in seconds.
        resolved_startup_retries (int): Max retries per health check.
        instance_tracker (InstanceTracker): Tracker for orphan cleanup.

    Returns:
        tuple[DockerLaunchedSession | PythonLaunchedSession, int]: The
            launched session and its assigned port.

    Raises:
        SessionLaunchError: If the session does not pass its health
            check within the startup timeout (the launched process is
            stopped best-effort first), or if the launcher itself
            fails to start the session.
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
            f"[mcp_systems_server:session_community_create] Session '{session_name}' failed to start within {resolved_startup_timeout}s. "
            f"Increase community/settings.json: session_creation.defaults.startup_timeout_seconds."
        )
        try:
            await launched_session.stop()
        except Exception as e:
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_create] Failed to cleanup failed session: {e}"
            )

        raise SessionLaunchError(
            f"Session failed to start within {resolved_startup_timeout} seconds. "
            f"To allow more time, increase community/settings.json: "
            f"session_creation.defaults.startup_timeout_seconds in the operator config."
        )

    return launched_session, port


def _build_success_response(
    id: str,
    session_name: str,
    connection_url: str,
    resolved_auth_type: str,
    resolved_launch_method: LaunchMethod,
    port: int,
    launched_session: LaunchedSession,
) -> dict:
    """Build the success response dict for session creation.

    Returns:
        Success response dict with session details.

    Raises:
        InternalError: If ``launched_session`` is neither a
            :class:`DockerLaunchedSession` nor a
            :class:`PythonLaunchedSession`.
    """
    result = {
        "success": True,
        "id": id,
        "session_name": session_name,
        "connection_url": connection_url,
        "auth_type": resolved_auth_type,
        "launch_method": resolved_launch_method,
        "port": port,
    }

    # Add launch-method-specific details
    if isinstance(launched_session, DockerLaunchedSession):
        result["container_id"] = launched_session.container_id
    elif isinstance(launched_session, PythonLaunchedSession):
        result["process_id"] = launched_session.process.pid
    else:
        raise InternalError(
            f"Unhandled LaunchedSession subtype "
            f"{type(launched_session).__name__}; _build_success_response "
            f"must be extended whenever a launch method is added."
        )

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
    _LOGGER.warning(
        "   in your community/settings.json in your configuration directory."
    )
    _LOGGER.warning("=" * 70)


async def _check_display_name_conflict_fast(
    session_registry: CommunitySessionRegistry,
    session_name: str,
) -> dict | None:
    """Fast-fail pre-check for display-name conflicts before launching a worker.

    The community :class:`SessionId` is the session name itself, so the
    canonical ``qualified_session_id`` is deterministic from ``session_name`` alone.
    A single ``registry.get`` against that ``qualified_session_id`` catches a
    duplicate in O(1) without launching anything.

    This is best-effort — ``CommunitySessionRegistry.add_dynamic_session``
    repeats the check under its lock and is the atomic source of truth.

    Args:
        session_registry: The community session registry.
        session_name: The requested display name.

    Returns:
        Error dict if a same-named session already exists, or ``None``
        to proceed with creation.
    """
    id = QualifiedSessionId(
        SystemType.COMMUNITY,
        SystemType.COMMUNITY.value,
        SessionId(session_name),
    )
    try:
        await session_registry.get(id)
    except RegistryItemNotFoundError:
        return None
    error_msg = f"A community session named {session_name!r} already exists"
    _LOGGER.error(f"[mcp_systems_server:session_community_create] {error_msg}")
    return error_response(error_msg)


async def session_community_create(
    context: Context,
    session_name: str,
    launch_method: LaunchMethod | None = None,
    programming_language: str | None = None,
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
    """MCP Tool: Create a new dynamically launched Deephaven Community session.

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
    - Save the 'id' to reference the session in other MCP tools
    - IMPORTANT: Created sessions consume system resources (memory, CPU, ports)
    - Delete sessions when done using session_community_delete

    Args:
        context (Context): The MCP context object.
        session_name (str): Unique display name for the session. Must not conflict with
            existing session display names. It is also the hash input that the registry
            uses as the session's :class:`SessionId`. The final ``id``
            returned to the caller has the form ``"community:community:<session_name>"``.
        launch_method (LaunchMethod | None): How to launch the session ("docker" or "python").
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
        auth_token (str | None): Pre-shared key for PSK authentication.
            The worker-side authentication handler is derived from
            ``community/settings.json``'s
            ``session_creation.defaults.auth.credentials`` block: PSK
            credentials → PSK handler, anonymous credentials →
            Anonymous handler, custom credentials → the FQCN carried
            on the credential. When the resolved handler is PSK and
            ``auth_token`` is omitted, a cryptographically secure
            token is auto-generated and logged at WARNING level.
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
        heap_size_gb (float | int | None): JVM heap size in gigabytes.
            Applies to both docker and python launches. Integer values use the ``g``
            suffix (e.g. ``4`` → ``-Xmx4g``); float values with a decimal part are
            converted to megabytes because the JVM does not accept decimal ``g``
            values (e.g. ``2.5`` → ``-Xmx2560m``).
            Defaults to configuration value or 4.
        extra_jvm_args (list[str] | None): Additional JVM arguments as list of strings.
        environment_vars (dict[str, str] | None): Environment variables to set in the session.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if creation succeeded, False if error occurred
            - 'id' (str): Full identifier in format ``"community:community:<session_name>"``.
            - 'session_name' (str): Display name provided by user
            - 'connection_url' (str): Base HTTP URL without authentication
            - 'auth_type' (str): Normalized authentication type as full class name
                (e.g., "io.deephaven.authentication.psk.PskAuthenticationHandler", "Anonymous")
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
            "id": "community:community:my-session",
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
            "id": "community:community:my-session",
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
        - Rejects duplicate display names (case-sensitive)
        - Auto-generates secure auth tokens if not provided
        - Waits for session to be ready before returning
        - Logs auth token with WARNING level for user visibility
        - Registers session in registry for lifecycle management

    Common Error Scenarios:
        - Session creation not configured: "Community session creation not configured in community/settings.json"
        - Session limit reached: "Session limit reached: X/Y sessions active"
        - Docker param with python: "'programming_language' parameter only applies to docker launch method, not 'python'"
        - Docker image with python: "'docker_image' parameter only applies to docker launch method, not 'python'"
        - Docker resource with python: "'docker_memory_limit_gb' parameter only applies to docker launch method, not 'python'"
        - Docker resource with python: "'docker_cpu_limit' parameter only applies to docker launch method, not 'python'"
        - Docker resource with python: "'docker_volumes' parameter only applies to docker launch method, not 'python'"
        - Invalid parameters: "Cannot specify both 'programming_language' and 'docker_image' - use one or the other"
        - Unsupported language: "Unsupported programming_language: '{language}'. Must be 'Python' or 'Groovy'"
        - Invalid config language: "Invalid programming_language in config: '{language}'. Must be 'Python' or 'Groovy'"
        - Display-name conflict: "A community session named '{name}' already exists"
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
        # Validate session_name up front: it must be a shell- and id-safe
        # identifier because it doubles as a Docker container name,
        # Python process tag, and the hash input for the SessionId.
        # Any failure here is a user-facing input error and must surface
        # before we touch the registry or spawn any process.
        try:
            validate_resource_name(session_name, field="session_name")
        except InvalidSessionNameError as e:
            _LOGGER.error(f"[mcp_systems_server:session_community_create] {e}")
            return error_response(str(e))

        # Get config and session registry
        settings = get_community_settings(context)
        session_registry = get_community_registry(context)

        # Get and validate configuration
        defaults, max_concurrent_sessions = _get_session_creation_config(settings)

        # Check session limit
        limit_error = await _check_session_limit(
            session_registry, max_concurrent_sessions
        )
        if limit_error:
            return limit_error

        # Resolve all session parameters
        params = _resolve_community_session_parameters(
            launch_method,
            programming_language,
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

        # Extract resolved parameters
        resolved_launch_method = params.launch_method
        resolved_programming_language = params.programming_language
        resolved_auth_type = params.auth_type
        resolved_auth_token = params.auth_token
        auto_generated_token = params.auto_generated_token
        resolved_heap_size_gb = params.heap_size_gb
        resolved_docker_image = params.docker_image
        resolved_docker_memory_limit = params.docker_memory_limit_gb
        resolved_docker_cpu_limit = params.docker_cpu_limit
        resolved_docker_volumes = params.docker_volumes
        resolved_python_venv_path = params.python_venv_path
        resolved_extra_jvm_args = params.extra_jvm_args
        resolved_environment_vars = params.environment_vars
        resolved_startup_timeout = params.startup_timeout_seconds
        resolved_startup_interval = params.startup_check_interval_seconds
        resolved_startup_retries = params.startup_retries

        # Fast-fail display-name conflict check (best-effort; the atomic
        # guarantee is in ``add_dynamic_session``).
        conflict_error = await _check_display_name_conflict_fast(
            session_registry, session_name
        )
        if conflict_error:
            return conflict_error

        _LOGGER.info(
            f"[mcp_systems_server:session_community_create] Creating session '{session_name}' "
            f"(method: {resolved_launch_method}, language: {resolved_programming_language}, auth: {resolved_auth_type})"
        )

        # Get instance tracker from context for orphan tracking.
        instance_tracker: InstanceTracker = (
            context.request_context.lifespan_context.instance_tracker
        )

        # Launch session and wait for readiness
        launched_session, port = await _launch_process_and_wait_for_ready(
            session_name,
            resolved_launch_method,
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

        # Create and register session manager. The registry assigns the
        # SessionId; we read the full canonical id back.
        id = await _register_session_manager(
            session_name,
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
            id,
            session_name,
            launched_session.connection_url,
            resolved_auth_type,
            resolved_launch_method,
            port,
            launched_session,
        )

    except SessionCreationError as e:
        # Raised by the config / parameter-resolution / launch helpers
        # (SessionLaunchError is a subclass). The message is already
        # user-facing; surface it without additional wrapping.
        _LOGGER.error(f"[mcp_systems_server:session_community_create] {e}")
        return error_response(str(e))
    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_community_create] Failed to create session '{session_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to create community session '{session_name}': {exception_summary(e)}"
        )
        result["isError"] = True

    return result


async def _delete_session_resources(
    id: str,
    session_name: str,
    session_manager: CommunitySessionManager,
    session_registry: CommunitySessionRegistry,
    instance_tracker: InstanceTracker,
) -> None:
    """Untrack, close, and remove a community session from the registry.

    Performs the three cleanup steps required to fully delete a session:

    1. **Untrack** the Python process from the instance tracker, if the session
       is a :class:`DynamicCommunitySessionManager` backed by a
       :class:`PythonLaunchedSession`.
    2. **Close** the session (stops the Docker container or Python process).
       Close failure is non-fatal: it is logged as a warning and cleanup
       continues so that the registry entry is always removed.
    3. **Remove** the session from the registry.  If ``remove`` raises,
       the exception propagates to the caller.

    Args:
        id (str): Fully qualified id, e.g. ``"community:community:my_worker"``.
        session_name (str): Display name carried on the manager. For community
            sessions this equals the trailing segment of ``id``. Used for
            Python process untracking.
        session_manager (CommunitySessionManager): The session manager retrieved from
            the registry.
        session_registry (CommunitySessionRegistry): Registry to remove the session from.
        instance_tracker (InstanceTracker): Tracker used to unregister Python processes.

    Raises:
        Exception: Propagates any exception raised by ``session_registry.remove``.
            Close failure is swallowed and logged; removal failure is fatal.
    """
    if isinstance(session_manager, DynamicCommunitySessionManager):
        if isinstance(session_manager.launched_session, PythonLaunchedSession):
            await instance_tracker.untrack_python_process(session_name)

    try:
        _LOGGER.debug(
            f"[mcp_systems_server:session_community_delete] Closing session '{id}'"
        )
        await session_manager.close()
        _LOGGER.debug(
            f"[mcp_systems_server:session_community_delete] Successfully closed session '{id}'"
        )
    except Exception as e:
        _LOGGER.warning(
            f"[mcp_systems_server:session_community_delete] Failed to close session '{id}': {e}"
        )
        # Continue with removal even if close failed

    removed_manager = await session_registry.remove(QualifiedSessionId.from_str(id))
    if removed_manager is None:
        _LOGGER.warning(
            f"[mcp_systems_server:session_community_delete] Session '{id}' was not found in registry during removal"
        )
    else:
        _LOGGER.debug(
            f"[mcp_systems_server:session_community_delete] Removed session '{id}' from registry"
        )


async def session_community_delete(
    context: Context,
    id: str,
) -> dict:
    """MCP Tool: Delete a dynamically created Deephaven Community session.

    Deletes a community session that was created via session_community_create.
    This stops the underlying Docker container or Python process and removes the
    session from the registry.

    Session ID Format:
        Community session IDs have the format ``"community:community:<session_name>"``
        — the community :class:`SessionId` is the session name itself.
        Use the ``id`` returned by ``session_community_create`` or ``sessions_list``
        verbatim — do not construct or modify it manually.
        Only dynamically created sessions (``origin="dynamic"``) can be deleted; passing
        a static session ID (``origin="static"``) returns a clear error.

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
    - Pass the ``id`` exactly as returned by ``session_community_create`` or ``sessions_list``
    - Always check 'success' field first to verify deletion completed
    - This operation is irreversible - deleted sessions cannot be recovered
    - Only dynamically created sessions (origin='dynamic') can be deleted
    - Static sessions from configuration cannot be deleted (will return error)
    - After successful deletion, id will no longer be valid for other MCP tools
    - Deletion stops the Docker container or terminates the Python process

    Args:
        context (Context): The MCP context object.
        id (str): Fully qualified id in the form
            ``"community:community:<session_name>"``. Must be a dynamically created
            session from ``session_community_create``. Static sessions from configuration
            files cannot be deleted.

    Returns:
        dict: Structured result object with keys:
            - 'success' (bool): True if deletion succeeded, False if error occurred
            - 'id' (str): Full identifier in format
                ``"community:community:<session_name>"``.
            - 'session_name' (str): Display name carried on the manager.
            - 'error' (str, optional): Error message if deletion failed. Omitted on success.
            - 'isError' (bool, optional): Present and True if this is an error response

        Example Success Response:
        {
            "success": True,
            "id": "community:community:my-session",
            "session_name": "my-session"
        }

        Example Error Response:
        {
            "success": False,
            "error": "Session 'community:community:my-session' not found",
            "isError": True
        }

    Validation and Safety:
        - Verifies session exists in registry
        - Checks that session is dynamically created (origin='dynamic')
        - Properly closes the session connection
        - Stops the underlying Docker container or pip process
        - Removes session from registry to prevent future access
        - Provides detailed error messages for troubleshooting

    Common Error Scenarios:
        - Session not found: "Session '{id}' not found"
        - Not a community session: "Session '{id}' is not a community session"
        - Not a dynamic session: "Session '{id}' is not a dynamically created session (origin: '{origin}'). Only dynamically created sessions can be deleted."
        - Already deleted: "Session '{id}' not found"
        - Cleanup failure: "Failed to close session '{id}': {error}"
        - Registry removal failure: "Failed to remove session '{id}' from registry: {error}"

    Note:
        - This operation is irreversible - deleted sessions cannot be recovered
        - Any running queries or tables in the session will be lost
        - The Docker container or pip process will be terminated
        - Use with caution - ensure you have saved any important data
    """
    _LOGGER.info(f"[mcp_systems_server:session_community_delete] Invoked: id={id!r}")

    result: dict[str, object] = {"success": False}
    # Display name is unknown until we look up the manager; the id
    # itself is the most informative fallback for the outer exception handler.
    session_name: str = id

    try:
        # Get session registry
        session_registry = get_community_registry(context)

        # Parse and validate the id. Wrap once and reuse the typed
        # value for the registry lookup below.
        try:
            qsid = QualifiedSessionId.from_str(id)
        except InvalidSessionNameError as e:
            error_msg = f"Invalid id format: {e}"
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        if qsid.system_type is not SystemType.COMMUNITY:
            error_msg = f"Session '{id}' is not a community session (type: '{qsid.system_type.value}')"
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        _LOGGER.debug(
            f"[mcp_systems_server:session_community_delete] Looking for session '{id}'"
        )

        # Check if session exists in registry
        try:
            session_manager = await session_registry.get(qsid)
        except RegistryItemNotFoundError as e:
            error_msg = f"Session '{id}' not found: {e}"
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Only dynamically created community sessions are deletable. The
        # registry is community-only, so every manager returned here is a
        # ``CommunitySessionManager`` with a non-``None`` ``origin``; the
        # ``isinstance`` guard exists for the static type checker.
        if (
            not isinstance(session_manager, CommunitySessionManager)
            or session_manager.origin is not SessionOrigin.DYNAMIC
        ):
            origin_str = (
                session_manager.origin.value
                if isinstance(session_manager, CommunitySessionManager)
                else "unknown"
            )
            error_msg = (
                f"Session '{id}' is not a dynamically created session "
                f"(origin: '{origin_str}'). Only dynamically created sessions can be deleted."
            )
            _LOGGER.error(f"[mcp_systems_server:session_community_delete] {error_msg}")
            result["error"] = error_msg
            result["isError"] = True
            return result

        # Capture the manager's display name for human-facing logs and the
        # success payload. Reading after the type guard above keeps mypy happy.
        session_name = session_manager.name

        _LOGGER.debug(
            f"[mcp_systems_server:session_community_delete] Found dynamic community session manager for '{id}'"
        )

        instance_tracker: InstanceTracker = (
            context.request_context.lifespan_context.instance_tracker
        )
        await _delete_session_resources(
            id,
            session_name,
            session_manager,
            session_registry,
            instance_tracker,
        )

        _LOGGER.info(
            f"[mcp_systems_server:session_community_delete] Successfully deleted session "
            f"'{session_name}' (session ID: '{id}')"
        )

        result.update(
            {
                "success": True,
                "id": id,
                "session_name": session_name,
            }
        )

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_community_delete] Failed to delete session '{session_name}': {e!r}",
            exc_info=True,
        )
        result["error"] = (
            f"Failed to delete community session '{session_name}': {exception_summary(e)}"
        )
        result["isError"] = True

    return result


def _static_credentials_view(
    mgr: StaticCommunitySessionManager,
) -> tuple[str, str, str, str]:
    """Return ``(auth_type, auth_token, connection_url, connection_url_with_auth)``.

    Reads directly from the typed ``CommunitySessionConfig`` carried by
    the static session manager. Authentication shape:

    - ``PSKCredentials`` → ``auth_type='PSK'`` and the resolved token.
    - ``CustomTokenCredentials`` → the custom Java handler class name
      and its opaque token.
    - ``AnonymousCredentials`` → ``auth_type='ANONYMOUS'`` and an empty
      token.
    - Anything else (``PasswordCredentials``, ``PrivateKeyCredentials``)
      returns an empty token to avoid leaking secrets; community workers
      do not use these.
    """
    session_cfg = mgr.session_config
    host = session_cfg.host or ""
    port = session_cfg.port
    scheme = "https" if session_cfg.tls is not None else "http"
    server = f"{scheme}://{host}:{port}" if host else ""
    creds = session_cfg.credentials
    if isinstance(creds, PSKCredentials):
        auth_token = creds.token.get_secret_value()
        auth_type = "PSK"
    elif isinstance(creds, CustomTokenCredentials):
        auth_token = creds.auth_token.get_secret_value()
        auth_type = creds.auth_type
    elif isinstance(creds, AnonymousCredentials):
        auth_token = ""
        auth_type = "ANONYMOUS"
    else:
        auth_token = ""
        auth_type = type(creds).__name__.upper()
    connection_url = server
    connection_url_with_auth = f"{server}/?psk={auth_token}" if auth_token else server
    return auth_type, auth_token, connection_url, connection_url_with_auth


async def session_community_credentials(
    context: Context,
    id: str,
) -> dict:
    """SECURITY SENSITIVE: Retrieve connection credentials for browser access.

    Returns authentication credentials for connecting to a Deephaven Community session
    via web browser. This tool exposes sensitive credentials and should only be called
    when the user explicitly needs browser access.

    IMPORTANT: This tool is DISABLED by default for security. To enable, add to your
    ``community/settings.json`` (in your configuration directory):

    {
      "security": {
        "credential_retrieval_mode": "dynamic_only"  // or "all", "static_only"
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
        id (str): Fully qualified id in the canonical form
            ``"community:community:<session_name>"``. Both static (configured) and
            dynamic (runtime-created) sessions share this prefix; use
            ``sessions_list`` and check the ``origin`` field to distinguish
            them when needed.
            Example: ``"community:community:my-session"``

    Returns:
        dict: Response structure varies based on success/failure:

        On Success (success=True):
            - success (bool): Always True
            - id (str): The fully qualified session id, echoed back
            - auth_type (str): Authentication type string (uppercased). For dynamic sessions,
                derived from the launched session's auth type (e.g., "PSK", "ANONYMOUS").
                For static sessions, the raw config ``auth_type`` value uppercased
                (e.g., "IO.DEEPHAVEN.AUTHENTICATION.PSK.PSKAUTHENTICATIONHANDLER", "ANONYMOUS").
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
            "id": "community:community:my-session",
            "auth_type": "PSK",
            "auth_token": "abc123xyz789...",
            "connection_url": "http://localhost:45123",
            "connection_url_with_auth": "http://localhost:45123/?psk=abc123xyz789"
        }

    Example Success Response (ANONYMOUS Authentication):
        {
            "success": True,
            "id": "community:community:my-session",
            "auth_type": "ANONYMOUS",
            "auth_token": "",
            "connection_url": "http://localhost:45123",
            "connection_url_with_auth": "http://localhost:45123"
        }

    Example Disabled Response:
        {
            "success": False,
            "error": "Credential retrieval is disabled (mode='none'). To enable, configure security.credential_retrieval_mode in community/settings.json...",
            "isError": True
        }

    Example Session Not Found Response:
        {
            "success": False,
            "error": "Session 'community:community:my-session' not found: ...",
            "isError": True
        }
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_community_credentials] Invoked for id: {id}"
    )

    try:
        # Read security settings from the lifespan-loaded community config.
        settings = get_community_settings(context)
        credential_retrieval_mode = (
            settings.security.credential_retrieval_mode
            if settings.security is not None
            else "none"
        )

        # Validate id format - must be a community session
        if not id.startswith("community:"):
            return error_response(
                f"Invalid id '{id}'. This tool only works for "
                f"community sessions (format: 'community:community:<session_name>')."
            )

        # Check if credential retrieval is disabled globally (mode='none')
        if credential_retrieval_mode == "none":
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_credentials] DENIED: Credential retrieval disabled (mode='none') for id '{id}'"
            )
            return error_response(
                "Credential retrieval is disabled (mode='none'). To enable, set security.credential_retrieval_mode in community/settings.json (in your configuration directory):\n\n"
                "Available modes:\n"
                '  - "none": Disable all credential retrieval (secure default)\n'
                '  - "dynamic_only": Allow only auto-generated session credentials\n'
                '  - "static_only": Allow only pre-configured session credentials\n'
                '  - "all": Allow all credential retrieval\n\n'
                "Configuration example:\n"
                "{\n"
                '  "security": {\n'
                '    "credential_retrieval_mode": "dynamic_only"\n'
                "  }\n"
                "}\n\n"
                "Documentation: https://github.com/deephaven/deephaven-mcp/"
            )

        # Get session registry and session manager
        session_registry = get_community_registry(context)

        try:
            mgr = await session_registry.get(QualifiedSessionId.from_str(id))
        except RegistryItemNotFoundError as e:
            return error_response(f"Session '{id}' not found: {e}")

        # Verify it's a community session manager
        if not isinstance(mgr, CommunitySessionManager):
            return error_response(f"Session '{id}' is not a community session")

        # Determine session type and extract its credentials view. Unknown
        # subtypes must fail loudly here, before any mode check can
        # misclassify them as static.
        if isinstance(mgr, DynamicCommunitySessionManager):
            # Dynamic session - get from launched_session
            is_dynamic = True
            auth_token = (
                mgr.launched_session.auth_token
                if mgr.launched_session.auth_token
                else ""
            )
            connection_url = mgr.connection_url
            connection_url_with_auth = mgr.connection_url_with_auth
            auth_type = mgr.launched_session.auth_type.upper()
        elif isinstance(mgr, StaticCommunitySessionManager):
            # Static session - reads directly from the typed declaration
            # carried on the manager (no legacy ``_config`` dict here).
            is_dynamic = False
            (
                auth_type,
                auth_token,
                connection_url,
                connection_url_with_auth,
            ) = _static_credentials_view(mgr)
        else:
            raise InternalError(
                f"Unhandled CommunitySessionManager subtype "
                f"{type(mgr).__name__}; session_community_credentials must be "
                f"extended whenever a manager subtype is added."
            )
        is_static = not is_dynamic

        # Check mode-specific permissions
        if credential_retrieval_mode == "dynamic_only" and is_static:
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_credentials] DENIED: Static session credential retrieval disabled (mode='dynamic_only') for id '{id}'"
            )
            return error_response(
                f"Credential retrieval for static sessions is disabled. Current mode: 'dynamic_only'. "
                f"Session '{id}' is a static (config-based) session. "
                f"To retrieve static session credentials, set security.credential_retrieval_mode to 'all' or 'static_only' in community/settings.json (in your configuration directory)."
            )
        elif credential_retrieval_mode == "static_only" and is_dynamic:
            _LOGGER.warning(
                f"[mcp_systems_server:session_community_credentials] DENIED: Dynamic session credential retrieval disabled (mode='static_only') for id '{id}'"
            )
            return error_response(
                f"Credential retrieval for dynamic sessions is disabled. Current mode: 'static_only'. "
                f"Session '{id}' is a dynamic (on-demand) session. "
                f"To retrieve dynamic session credentials, set security.credential_retrieval_mode to 'all' or 'dynamic_only' in community/settings.json (in your configuration directory)."
            )

        # Credential retrieval is allowed - proceed
        session_type_str = "dynamic" if is_dynamic else "static"
        _LOGGER.warning(
            f"[mcp_systems_server:session_community_credentials] SECURITY: Credential retrieval ALLOWED (mode='{credential_retrieval_mode}', type='{session_type_str}') for id '{id}'"
        )

        result = {
            "success": True,
            "id": id,
            "auth_type": auth_type,
            "auth_token": auth_token,
            "connection_url": connection_url,
            "connection_url_with_auth": connection_url_with_auth,
        }

        _LOGGER.warning(
            f"[mcp_systems_server:session_community_credentials] SECURITY: Credentials retrieved for id '{id}'"
        )

        return result

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_community_credentials] Failed: {e!r}",
            exc_info=True,
        )
        return error_response(str(e))


def register_tools(server: FastMCP) -> None:
    """Register all community session tools with the given FastMCP server.

    These tools are specific to the DHC server and should NOT be registered
    on the DHE server.

    Args:
        server (FastMCP): The server to register tools with.
    """
    server.tool()(session_community_create)
    server.tool()(session_community_delete)
    server.tool()(session_community_credentials)
