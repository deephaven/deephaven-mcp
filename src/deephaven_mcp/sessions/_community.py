"""Declaration value type for one static Deephaven Community session.

:class:`CommunitySessionConfig` is the **domain value type** that
describes how to connect to one community session: host, port,
optional transport TLS, and required outbound credentials. It is
*not* the live session itself — see
:class:`deephaven_mcp.client.CoreSession` for that.

The same class is produced from two sources:

1. The on-disk loader
   (:func:`deephaven_mcp.config.schema._community.load_community`),
   which validates a JSON file's contents (with filename-stem
   cross-checks via the ``model_validator(mode="before")`` below).
2. The dynamic-session tool
   (:mod:`deephaven_mcp.mcp_systems_server._tools.session_community`),
   which constructs an instance directly from caller-supplied
   parameters for runtime-launched sessions.

Schema::

    {
        "session_name": "local",
        "host": "localhost",
        "port": 10000,
        "tls": {
            "root_certs": "${file:/etc/ssl/dh-ca.pem}",
            "client_certificate": {
                "cert_chain": "${file:/etc/ssl/client.pem}",
                "private_key": "${file:/etc/ssl/client.key}"
            }
        },
        "programming_language": "Python",
        "never_timeout": false,
        "auth": {
            "credentials": {
                "type": "psk",
                "token": "${env:DH_COMMUNITY_LOCAL_PSK}"
            }
        }
    }
"""

from __future__ import annotations

__all__ = [
    "CommunitySessionConfig",
]

from typing import Annotated, Any

from pydantic import Field, field_validator, model_validator

from deephaven_mcp._names import validate_resource_name
from deephaven_mcp._pydantic import (
    RedactableSchema,
    reconcile_filename_stem,
)
from deephaven_mcp.auth.tls import TlsConfig

from ._auth import AuthConfig
from ._types import ProgrammingLanguage


class CommunitySessionConfig(RedactableSchema):
    """Validated configuration for one static community session.

    Both wire-format schema and runtime type: parsing a session file
    produces an instance that the registry passes directly to the
    session manager.
    """

    name: str = Field(json_schema_extra={"non_wire": True})
    """Session name. Not a wire field: the loader injects the filename
    stem of the source JSON file, cross-checked against any
    ``session_name`` field declared inside that file."""

    host: str | None = None
    """Optional Deephaven Community server hostname. ``None`` falls
    back to the upstream client's default (typically ``localhost``)."""

    port: Annotated[int, Field(gt=0, lt=65536)] | None = None
    """Optional Deephaven Community server port (1-65535). ``None``
    falls back to the upstream client's default (typically
    ``10000``)."""

    programming_language: ProgrammingLanguage | None = None
    """Optional scripting language for the worker session: exactly
    ``"Python"`` or ``"Groovy"`` (title-case is the canonical wire
    form). ``None`` falls back to the upstream client's default."""

    never_timeout: bool | None = None
    """Optional flag disabling the worker-side session idle timeout.
    ``None`` leaves the upstream default in place."""

    tls: TlsConfig | None = None
    """Pre-resolved transport-layer TLS material parsed from the
    optional ``tls`` block. ``None`` when the session config did not
    include a ``tls`` key (plaintext); a populated
    :class:`~deephaven_mcp.auth.tls.TlsConfig` otherwise. Presence is
    what enables TLS, not the value of any individual sub-field."""

    auth: AuthConfig
    """Authentication details for connecting to the Community server."""

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        """Reject names that can't round-trip through ``qualified_session_id``.

        The community :class:`SessionId` is the session name itself, so
        the name must conform to the resource-name character class
        (ASCII alphanumerics plus ``_``, ``.``, ``-``; starting
        alphanumeric; non-empty). Catches bad filename stems at
        config-load time before they reach the registry.
        """
        validate_resource_name(value, field="name")
        return value

    @model_validator(mode="before")
    @classmethod
    def _reconcile_name(cls, data: Any) -> Any:
        """Cross-check ``session_name`` against the filename stem."""
        return reconcile_filename_stem(
            data,
            declared_field="session_name",
            model_label="CommunitySessionConfig",
        )
