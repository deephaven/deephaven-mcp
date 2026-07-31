"""Schemas and loader for the ``community/`` section of the MCP config tree.

Contains:

- :class:`CommunitySettings` and its nested sub-models
  (:class:`CommunitySecurity`,
  :class:`CommunitySessionCreation`,
  :class:`CommunitySessionCreationDefaults`) - the Pydantic schema
  for ``community/settings.json``.
- :class:`CommunityConfig` - the umbrella that
  :class:`~deephaven_mcp.config.tree.ConfigTreeLoader`
  produces after loading ``community/settings.json`` and every
  ``community/sessions/<name>.json`` file.
- :func:`load_community` - the section loader the manager invokes.

The per-session declaration type itself
(:class:`~deephaven_mcp.sessions.CommunitySessionConfig`) lives in
:mod:`deephaven_mcp.sessions` - it is a domain value type, produced
both by this loader and by runtime callers
(:mod:`deephaven_mcp.mcp_systems_server._tools.session_community`).
"""

from __future__ import annotations

__all__ = [
    "CommunityConfig",
    "CommunitySecurity",
    "CommunitySessionCreation",
    "CommunitySessionCreationDefaults",
    "CommunitySettings",
    "CommunityTimeouts",
    "DockerImages",
    "DockerLaunchOptions",
    "LaunchMethod",
    "ProgrammingLanguage",
    "PythonLaunchOptions",
    "load_community",
]

import logging
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field

from deephaven_mcp._pydantic import (
    RedactableSchema,
    StrictSchema,
)
from deephaven_mcp.client._timeouts import CommunityClientTimeouts
from deephaven_mcp.config._loaders import load_named_json, load_named_json_with_stem
from deephaven_mcp.resource_manager._evictor import EvictionTimeouts
from deephaven_mcp.sessions import (
    AuthConfig,
    CommunitySessionConfig,
    LaunchMethod,
    ProgrammingLanguage,
)

from ._response_limits import ResponseLimits

_LOGGER = logging.getLogger(__name__)


class CommunityTimeouts(StrictSchema):
    """All operator-tunable duration knobs for the community section.

    Single umbrella for every duration the operator may tune in
    ``community/settings.json``. Two typed sub-blocks split by
    consumer:

    - :attr:`client`: deadlines the Deephaven client library applies
      to outbound community RPCs.
    - :attr:`eviction`: MCP-side idle-session eviction policy.
    """

    client: CommunityClientTimeouts = Field(default_factory=CommunityClientTimeouts)
    """Client-layer timeouts for outbound Deephaven Community RPCs.
    Defaults to a default-constructed
    :class:`CommunityClientTimeouts` when absent from the JSON."""

    eviction: EvictionTimeouts = Field(default_factory=EvictionTimeouts)
    """MCP-side idle-session eviction policy for the community
    registry. Defaults to a default-constructed
    :class:`EvictionTimeouts` when absent from the JSON."""


class CommunitySecurity(StrictSchema):
    """``security`` sub-block of ``community/settings.json``."""

    credential_retrieval_mode: Literal["none", "dynamic_only", "static_only", "all"] = (
        "dynamic_only"
    )
    """Policy controlling whether the systems server exposes its
    community session credentials to MCP clients through the
    credential-retrieval tool. ``"none"`` disables retrieval;
    ``"dynamic_only"`` permits retrieval for runtime-launched
    sessions; ``"static_only"`` permits retrieval for sessions
    configured under ``community.sessions``; ``"all"`` permits both.

    Defaults to ``"dynamic_only"``: a dynamic worker's token was minted
    for a session the caller just created, while operator-authored
    static credentials stay withheld until an operator opts in."""


class DockerImages(StrictSchema):
    """Per-language default Docker images for dynamic community workers.

    Each language has its own default image reference. Operators
    override one without losing the other's default; the schema
    enforces ``extra="forbid"`` so unknown languages cannot be added
    without coordinated launcher / Literal updates.
    """

    python: str = "ghcr.io/deephaven/server:latest"
    """Docker image used when ``launch_method=="docker"`` and the
    resolved ``programming_language`` is ``"Python"``. Override to pin
    a specific tag or use a private registry."""

    groovy: str = "ghcr.io/deephaven/server-slim:latest"
    """Docker image used when ``launch_method=="docker"`` and the
    resolved ``programming_language`` is ``"Groovy"``."""


class DockerLaunchOptions(StrictSchema):
    """Docker-specific defaults for dynamic community workers.

    Only consulted when the resolved ``launch_method`` is
    ``"docker"``. The block is always present (default-constructed)
    so an operator may flip ``launch_method`` per call without
    rewriting ``community/settings.json``.
    """

    images: DockerImages = Field(default_factory=DockerImages)
    """Per-language image defaults. The launcher picks
    ``images.python`` or ``images.groovy`` based on the resolved
    ``programming_language``. The MCP tool's per-call ``docker_image``
    parameter overrides this."""

    memory_limit_gb: Annotated[float | None, Field(default=None, gt=0)] = None
    """Optional per-container memory limit in gigabytes (Docker
    ``--memory``). ``None`` applies no Docker-level limit (the
    worker's JVM is still bounded by ``heap_size_gb``)."""

    cpu_limit: Annotated[float | None, Field(default=None, gt=0)] = None
    """Optional per-container CPU limit in cores (Docker ``--cpus``).
    ``None`` applies no Docker-level CPU limit."""

    volumes: list[str] | None = None
    """Optional list of bind-mount specifications (each in Docker's
    ``host:container[:opts]`` form) to mount into the worker
    container. ``None`` mounts nothing extra."""


class PythonLaunchOptions(StrictSchema):
    """Python-launch-method defaults for dynamic community workers.

    Only consulted when the resolved ``launch_method`` is
    ``"python"``. The block is always present (default-constructed)
    so an operator may flip ``launch_method`` per call without
    rewriting ``community/settings.json``.
    """

    venv_path: str | None = None
    """Optional host-side Python virtualenv path used to locate the
    ``deephaven`` executable. ``None`` falls back to the MCP server's
    own venv. Has no effect under ``launch_method=="docker"``."""


class CommunitySessionCreationDefaults(RedactableSchema):
    """``session_creation.defaults`` sub-block for community sessions.

    Default settings applied when MCP creates a new dynamic community
    session and the request did not override the corresponding field.
    Mode-specific defaults live in nested blocks (``docker``,
    ``python``); both blocks are always present so operators can flip
    ``launch_method`` per call without rewriting the defaults. The
    optional ``auth`` block carries the default bearer material for
    those sessions.
    """

    launch_method: LaunchMethod = "docker"
    """How dynamic community sessions are launched. ``"docker"`` starts
    a fresh Deephaven server container per session (configured by the
    ``docker`` block); ``"python"`` starts the server in-process via
    the host venv at ``python.venv_path``."""

    docker: DockerLaunchOptions = Field(default_factory=DockerLaunchOptions)
    """Docker-specific defaults. Consulted only when the resolved
    ``launch_method`` is ``"docker"``; otherwise its fields are
    ignored. The block is always present (default-constructed)."""

    python: PythonLaunchOptions = Field(default_factory=PythonLaunchOptions)
    """Python-launch-method defaults. Consulted only when the resolved
    ``launch_method`` is ``"python"``; otherwise its fields are
    ignored. The block is always present (default-constructed)."""

    auth: AuthConfig | None = None
    """Optional ``auth`` block whose credentials are applied to
    dynamic community sessions when the request does not override
    them. ``None`` means the request must always supply its own
    credentials."""

    programming_language: ProgrammingLanguage = "Python"
    """Default scripting language for dynamic community sessions:
    exactly ``"Python"`` or ``"Groovy"``. Selects between Python and
    Groovy worker images / venv layouts."""

    heap_size_gb: Annotated[float, Field(gt=0)] = 4.0
    """JVM heap size in gigabytes for the worker process. Increase
    for memory-intensive analytics workloads."""

    extra_jvm_args: list[str] | None = None
    """Optional additional JVM arguments appended to the worker
    startup command (e.g. ``["-Dfoo=bar", "-Xms2g"]``). ``None`` adds
    no extra args."""

    environment_vars: dict[str, str] | None = None
    """Optional environment variables set in the worker process,
    keyed by variable name. ``None`` sets no extra env vars."""

    startup_timeout_seconds: Annotated[float, Field(gt=0)] = 60.0
    """How long (seconds) to wait for a dynamic community worker to
    become reachable before declaring startup failed. Increase when
    Docker image pulls or JVM cold-starts are slow."""

    startup_check_interval_seconds: Annotated[float, Field(gt=0)] = 2.0
    """Polling interval (seconds) used during the startup wait. Each
    poll probes the worker for readiness; smaller values detect
    success faster at the cost of more probes."""

    startup_retries: Annotated[int, Field(ge=0)] = 3
    """Number of times the launcher retries worker creation after a
    failure before giving up. ``0`` disables retry; the launcher
    fails on the first error."""


class CommunitySessionCreation(StrictSchema):
    """``session_creation`` sub-block of ``community/settings.json``."""

    max_concurrent_sessions: Annotated[int | None, Field(ge=1)] = 5
    """Cap on the number of concurrent dynamic community sessions
    MCP may run. ``None`` disables the cap (unbounded). Must be a
    positive integer when set."""

    defaults: CommunitySessionCreationDefaults = Field(
        default_factory=CommunitySessionCreationDefaults
    )
    """Per-session defaults applied to new dynamic sessions. Default-
    constructed (carrying the per-field defaults) when the JSON omits
    this block."""


class CommunitySettings(StrictSchema):
    """Validated contents of ``community/settings.json``.

    Every top-level key is optional. Numeric timer fields receive
    package-wide defaults defined at the field declaration site when
    absent, so callers can always read the field directly and get
    the effective value without further default resolution.
    """

    security: CommunitySecurity = Field(default_factory=CommunitySecurity)
    """Community-wide security policy. Omitting the block is the same as
    writing it empty: each field takes its own default."""

    session_creation: CommunitySessionCreation | None = None
    """Optional defaults for dynamically-created community sessions.
    ``None`` means MCP cannot create new community sessions; only
    pre-declared ones in ``community/sessions/`` are usable."""

    timeouts: CommunityTimeouts = Field(default_factory=CommunityTimeouts)
    """All operator-tunable duration knobs for the community section,
    grouped under :attr:`~CommunityTimeouts.client` (outbound RPC
    deadlines) and :attr:`~CommunityTimeouts.eviction` (MCP-side
    idle-session sweeper). Defaults to a default-constructed
    :class:`CommunityTimeouts` when absent from the JSON."""

    response_limits: ResponseLimits = Field(default_factory=ResponseLimits)
    """Operator-tunable thresholds for the tool-side response-size
    guard applied when a community tool projects how large a
    serialized response will be. See
    :class:`deephaven_mcp.config.schema._response_limits.ResponseLimits`."""


class CommunityConfig(StrictSchema):
    """Validated community configuration block.

    Sibling of
    :class:`~deephaven_mcp.config.schema.EnterpriseConfig`.
    Duration knobs exposed via ``settings.timeouts`` always carry
    effective values (per-file overrides if present, project-wide
    defaults otherwise), so consumers can read
    ``community.settings.timeouts.eviction.session_idle_timeout_seconds``
    directly.
    """

    settings: CommunitySettings
    """Parsed ``community/settings.json`` (defaulted to a default-
    constructed instance when the file is absent)."""

    sessions: dict[str, CommunitySessionConfig]
    """Validated per-session configurations, keyed by session name
    (filename stem). Empty dict when the ``community/sessions/``
    directory is absent or empty."""


async def load_community(config_dir: Path) -> CommunityConfig | None:
    """Load and validate the community section if any community files exist.

    Args:
        config_dir (Path): The audited configuration root.

    Returns:
        CommunityConfig | None: ``None`` when both
            ``community/settings.json`` is absent and
            ``community/sessions/`` is empty or missing.

    Raises:
        ConfigurationError: When any community file fails validation.
    """
    section_dir = config_dir / "community"
    settings_path = section_dir / "settings.json"
    sessions_dir = section_dir / "sessions"

    settings_present = settings_path.is_file()
    settings: CommunitySettings
    if settings_present:
        settings = await load_named_json(
            CommunitySettings,
            path=settings_path,
            config_dir=config_dir,
            error_label="community settings",
            log_label="_community:community/settings.json",
            logger=_LOGGER,
        )
    else:
        settings = CommunitySettings()

    sessions: dict[str, CommunitySessionConfig] = {}
    if sessions_dir.is_dir():
        for path in sorted(sessions_dir.glob("*.json")):
            session = await load_named_json_with_stem(
                CommunitySessionConfig,
                path=path,
                config_dir=config_dir,
                error_label=f"community session '{path.stem}'",
                log_label=f"_community:community/sessions/{path.name}",
                logger=_LOGGER,
            )
            sessions[session.name] = session

    if not settings_present and not sessions:
        return None
    return CommunityConfig(settings=settings, sessions=sessions)
