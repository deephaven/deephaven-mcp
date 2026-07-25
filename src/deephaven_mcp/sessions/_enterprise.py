"""Declaration value type for one Deephaven Enterprise system.

:class:`EnterpriseSystemConfig` is the **domain value type** that
describes how to connect to one Deephaven Enterprise system: the
``connection.json`` URL, the bearer credentials, and any
session-creation defaults. It is *not* the live
:class:`deephaven_mcp.client.CorePlusSessionFactory` itself, and it
is *not* inherently a file-format schema, though today it is the
typed result of loading one ``enterprise/systems/<name>.json`` file.

Schema (all fields at the top level)::

    {
        "system_name": "prod",
        "connection_json_url": "https://dhe.example.com/iris/connection.json",
        "auth": {
            "credentials": {
                "type": "password",
                "username": "alice",
                "password": "${env:DH_ENTERPRISE_PROD_PASSWORD}"
            }
        },
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4, "programming_language": "Python"}
        }
    }

Required top-level fields: ``system_name``, ``connection_json_url``,
``auth``. Optional: ``session_creation``.

The per-system connection timeout was retired in favor of the
shared ``enterprise/settings.json: timeouts.client.session_connect_timeout_seconds``;
factory construction reads the global value directly.
Idle/sweep timers live on ``enterprise/settings.json`` (apply to
every enterprise system uniformly) rather than per-system; see
:class:`EnterpriseSettings`.

Enterprise systems do **not** accept a ``tls`` block. The Deephaven
Enterprise SessionManager fetches its truststore via the
``connection.json``'s ``truststore_url`` and has no per-system mTLS
client-cert support; the model rejects ``tls`` with a pointed error.
"""

from __future__ import annotations

__all__ = [
    "EnterpriseSessionCreation",
    "EnterpriseSessionCreationDefaults",
    "EnterpriseSystemConfig",
]

from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from deephaven_mcp._pydantic import (
    RedactableSchema,
    StrictSchema,
    reconcile_filename_stem,
)

from ._auth import AuthConfig
from ._types import ProgrammingLanguage


class EnterpriseSessionCreationDefaults(StrictSchema):
    """Defaults applied when creating enterprise sessions.

    All fields are optional; each carries a schema-level default so
    operators may omit any subset.
    """

    heap_size_gb: Annotated[float, Field(gt=0)] = 4.0
    """JVM heap size in gigabytes allocated to the worker. Increase
    for memory-intensive workloads. Mirrors the community-side default
    on :class:`CommunitySessionCreationDefaults`."""

    auto_delete_timeout: Annotated[int | None, Field(default=None, gt=0)] = None
    """Optional worker auto-delete timeout (seconds of inactivity
    before the upstream controller tears the worker down). ``None``
    creates a permanent persistent query (no auto-delete). A positive
    integer creates a temporary persistent query that is torn down
    after that many seconds of idle time."""

    server: str | None = None
    """Optional name of the upstream Deephaven Enterprise server pool
    to schedule workers on. ``None`` lets the controller pick from
    its default pool."""

    engine: Literal["DeephavenCommunity", "DeephavenEnterprise"] = "DeephavenCommunity"
    """Worker engine kind. ``"DeephavenCommunity"`` runs the
    community-edition engine inside the enterprise harness;
    ``"DeephavenEnterprise"`` selects the enterprise engine. Operators
    typically only override when they need enterprise-only features.
    Typos at the JSON layer fail at config-load time."""

    extra_jvm_args: list[str] | None = None
    """Optional additional JVM arguments appended to the worker's
    startup command (e.g. ``["-Dfoo=bar", "-Xms2g"]``). ``None`` adds
    no extra args."""

    environment_vars: dict[str, str] | None = None
    """Optional environment variables set in the worker process,
    keyed by variable name. ``None`` sets no extra env vars. The
    enterprise session-creation tool converts this mapping to
    ``["NAME=value", ...]`` entries; the client layer converts those
    to the controller's alternating key/value wire format at call
    time."""

    admin_groups: list[str] | None = None
    """Optional list of group names granted admin access to workers
    created with these defaults. ``None`` falls back to the
    controller's default ACL."""

    viewer_groups: list[str] | None = None
    """Optional list of group names granted viewer access to workers
    created with these defaults. ``None`` falls back to the
    controller's default ACL."""

    session_arguments: dict[str, Any] | None = None
    """Optional free-form key/value session arguments forwarded
    verbatim to the upstream controller (typically read by the
    worker's user code). ``None`` sends no extra arguments."""

    programming_language: ProgrammingLanguage = "Python"
    """Scripting language the worker exposes to clients: exactly
    ``"Python"`` or ``"Groovy"``. Affects the set of available APIs
    but not the JVM/engine choice (which is controlled by
    ``engine``)."""


class EnterpriseSessionCreation(StrictSchema):
    """``session_creation`` block on an enterprise system config."""

    max_concurrent_sessions: Annotated[int | None, Field(ge=1)] = 5
    """Per-system cap on the number of concurrent active sessions MCP
    may run against this enterprise system. ``None`` disables the cap
    (unbounded). Must be a positive integer when set.

    The cap is per-system; there is no aggregate enterprise-wide cap.
    Two systems each at their default cap of ``5`` allow ten total
    concurrent sessions; the effective total is the sum of the
    per-system values."""

    defaults: EnterpriseSessionCreationDefaults = Field(
        default_factory=EnterpriseSessionCreationDefaults
    )
    """Per-session defaults applied when MCP launches a new worker
    against this system. Default-constructed (carrying the per-field
    defaults) when the JSON omits this block."""


class EnterpriseSystemConfig(RedactableSchema):
    """Validated declaration of one Deephaven Enterprise system."""

    name: str = Field(json_schema_extra={"non_wire": True})
    """System name. Not a wire field: the loader injects the filename
    stem of the source JSON file, cross-checked against any
    ``system_name`` field declared inside that file."""

    connection_json_url: str
    """URL of the Enterprise ``connection.json`` document. The upstream
    SessionManager fetches this URL to discover endpoints and the
    truststore for outbound TLS."""

    auth: AuthConfig
    """Authentication details for connecting to the Enterprise system."""

    session_creation: EnterpriseSessionCreation | None = None
    """Optional ``session_creation`` block. ``None`` means MCP cannot
    create new workers on this system; only pre-existing PQs are
    discoverable."""

    @model_validator(mode="before")
    @classmethod
    def _reject_tls_and_reconcile_name(cls, data: Any) -> Any:
        """Reject ``tls`` and cross-check ``system_name`` against the filename stem.

        Args:
            data (Any): The raw mapping passed to
                :meth:`model_validate`. Returned unchanged when not
                a dict.

        Returns:
            Any: A new mapping with ``system_name`` removed in favor
                of the caller-supplied ``name`` (the filename stem).

        Raises:
            ValueError: When ``tls`` is present, ``system_name``
                disagrees with the filename stem, or ``name`` is
                absent.
        """
        if isinstance(data, dict) and "tls" in data:
            raise ValueError(
                "'tls' is not supported on enterprise systems. Deephaven "
                "Enterprise SessionManager fetches its truststore via the "
                "connection.json's 'truststore_url' and has no per-system "
                "mTLS client-certificate support. Remove the 'tls' block."
            )
        return reconcile_filename_stem(
            data,
            declared_field="system_name",
            model_label="EnterpriseSystemConfig",
        )
