"""Configuration handling for the Deephaven MCP enterprise server.

Validates and redacts flat enterprise config files and provides the
:class:`EnterpriseServerConfigManager` used by ``dh-mcp-enterprise-server``.

Enterprise config file format (flat — all fields at top level)::

    {
        "system_name": "prod",
        "connection_json_url": "https://dhe.example.com/iris/connection.json",
        "auth": {
            "backends": ["password", "private_key"],
            "allow_effective_user": false
        },
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4, "programming_language": "Python"}
        }
    }

Top-level schema:

- **Required**: ``system_name``, ``connection_json_url``, ``auth``.
- **Optional**: ``session_creation``, ``connection_timeout``,
  ``session_idle_timeout_seconds``,
  ``session_idle_sweep_interval_seconds``.

The ``auth`` block (required):

- ``backends`` (list[str], required, non-empty): a subset of
  ``{"password", "private_key"}``. Identifies which authentication
  backends the server will mount in front of its streamable-HTTP app.
- ``allow_effective_user`` (bool, optional, default ``False``): when
  ``True``, the password backend honors the optional
  ``X-Deephaven-Effective-User`` HTTP header. Only valid when
  ``"password"`` is in ``backends``.

Per-request authentication
--------------------------
The enterprise MCP server no longer accepts credentials in its config
file. Every request must carry the caller's credentials in the
``X-Deephaven-*`` HTTP headers; the auth middleware exchanges them for a
:class:`~deephaven_mcp.auth.credentials.PasswordCredentials` or
:class:`~deephaven_mcp.auth.credentials.PrivateKeyCredentials`, which is later exchanged for
an authenticated ``CorePlusSessionFactory``.

Unknown fields at every level are rejected.

Public API (re-exported via :mod:`deephaven_mcp.config`):

- :class:`EnterpriseServerConfigManager`
- :func:`validate_enterprise_config`
- :func:`redact_enterprise_config`
- :data:`DEFAULT_CONNECTION_TIMEOUT_SECONDS`

Also exported from this module (imported directly, not via
:mod:`deephaven_mcp.config`):

- :data:`SUPPORTED_AUTH_BACKENDS`
"""

__all__ = [
    "DEFAULT_CONNECTION_TIMEOUT_SECONDS",
    "EnterpriseServerConfigManager",
    "SUPPORTED_AUTH_BACKENDS",
    "redact_enterprise_config",
    "validate_enterprise_config",
]

import logging
from typing import Any

from deephaven_mcp._exceptions import ConfigurationError

from ._base import (
    ConfigManager,
    _get_config_path,
    _load_and_validate_config,
    _log_config_summary,
)
from ._validators import (
    validate_allowed_fields,
    validate_field_type,
    validate_non_negative_int,
    validate_optional_positive_number,
)

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10.0
"""Default timeout in seconds for establishing connections to enterprise systems."""

SUPPORTED_AUTH_BACKENDS: frozenset[str] = frozenset({"password", "private_key"})
"""The set of values allowed in ``auth.backends``."""

_REQUIRED_TOP_LEVEL_FIELDS: dict[str, type | tuple[type, ...]] = {
    "system_name": str,
    "connection_json_url": str,
    "auth": dict,
}
"""Required top-level fields and their expected types."""

_OPTIONAL_TOP_LEVEL_FIELDS: dict[str, type | tuple[type, ...]] = {
    "session_creation": dict,
    "connection_timeout": (int, float),
    "session_idle_timeout_seconds": (int, float),
    "session_idle_sweep_interval_seconds": (int, float),
}
"""Optional top-level fields and their expected types."""

_ALLOWED_TOP_LEVEL_FIELDS: dict[str, type | tuple[type, ...]] = {
    **_REQUIRED_TOP_LEVEL_FIELDS,
    **_OPTIONAL_TOP_LEVEL_FIELDS,
}
"""Union of required and optional top-level fields, used for unknown-field
rejection."""

_ALLOWED_AUTH_FIELDS: dict[str, type | tuple[type, ...]] = {
    "backends": list,
    "allow_effective_user": bool,
}
"""Allowed fields inside the ``auth`` block."""

_ALLOWED_SESSION_CREATION_FIELDS: dict[str, type | tuple[type, ...]] = {
    "max_concurrent_sessions": int,
    "defaults": dict,
}
"""Allowed top-level fields in ``session_creation``."""

_ALLOWED_SESSION_CREATION_DEFAULTS: dict[str, type | tuple[type, ...]] = {
    "heap_size_gb": (int, float),
    "auto_delete_timeout": int,
    "server": str,
    "engine": str,
    "extra_jvm_args": list,
    "extra_environment_vars": list,
    "admin_groups": list,
    "viewer_groups": list,
    "timeout_seconds": (int, float),
    "session_arguments": dict,
    "programming_language": str,
}
"""Allowed fields inside ``session_creation.defaults``. ``heap_size_gb`` is
required when ``session_creation.defaults`` is present; the rest are
optional."""


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def redact_enterprise_config(system_config: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of ``system_config`` safe for logging.

    The current enterprise config schema carries no secret material —
    credentials are delivered per-request via HTTP headers, never via
    the config file — so this function returns a plain shallow copy.
    The function is kept (rather than removed) as a stable redaction
    surface: future schema changes that introduce sensitive fields can
    be redacted here without forcing every caller to learn the new
    shape.

    The returned dictionary is a shallow copy; the input
    ``system_config`` is never mutated.

    Args:
        system_config (dict[str, Any]): The enterprise system
            configuration dictionary.

    Returns:
        dict[str, Any]: A shallow copy of ``system_config``.
    """
    return system_config.copy()


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _validate_top_level_fields(system_name: str, config: dict[str, Any]) -> None:
    """Validate base + optional top-level fields and reject unknowns.

    Args:
        system_name (str): The ``system_name`` value from ``config``,
            used only in error messages.
        config (dict[str, Any]): The enterprise configuration dictionary.

    Raises:
        ConfigurationError: If a required base field is missing or has
            the wrong type, or if any unknown top-level field is
            present.
    """
    for field_name, expected_type in _REQUIRED_TOP_LEVEL_FIELDS.items():
        if field_name not in config:
            msg = (
                f"Required field '{field_name}' missing in enterprise system "
                f"'{system_name}'."
            )
            _LOGGER.error(f"[config:_validate_top_level_fields] {msg}")
            raise ConfigurationError(msg)
        validate_field_type(
            f"enterprise system '{system_name}'",
            field_name,
            config[field_name],
            expected_type,
        )

    validate_allowed_fields(
        f"enterprise system '{system_name}'",
        config,
        _ALLOWED_TOP_LEVEL_FIELDS,
    )


def _validate_auth_block(system_name: str, auth: dict[str, Any]) -> None:
    """Validate the required ``auth`` block.

    Enforces:

    - ``auth`` only contains keys in :data:`_ALLOWED_AUTH_FIELDS`.
    - ``auth.backends`` is a non-empty list of unique strings, each in
      :data:`SUPPORTED_AUTH_BACKENDS`.
    - ``auth.allow_effective_user`` (when present) is a ``bool`` and may
      only be ``True`` when ``"password"`` is in ``auth.backends``.

    Args:
        system_name (str): The ``system_name`` value, used only in error
            messages.
        auth (dict[str, Any]): The ``auth`` block from the enterprise
            configuration dictionary; already validated to be a ``dict``.

    Raises:
        ConfigurationError: For any violation of the rules above.
    """
    context = f"auth for enterprise system '{system_name}'"
    validate_allowed_fields(context, auth, _ALLOWED_AUTH_FIELDS)

    if "backends" not in auth:
        msg = (
            f"Required field 'backends' missing in {context}. Provide a "
            f"non-empty list whose elements are drawn from "
            f"{sorted(SUPPORTED_AUTH_BACKENDS)}."
        )
        _LOGGER.error(f"[config:_validate_auth_block] {msg}")
        raise ConfigurationError(msg)

    backends = auth["backends"]
    validate_field_type(context, "backends", backends, list)
    if len(backends) == 0:
        msg = f"'backends' for {context} must be a non-empty list."
        _LOGGER.error(f"[config:_validate_auth_block] {msg}")
        raise ConfigurationError(msg)
    if len(set(backends)) != len(backends):
        msg = f"'backends' for {context} must not contain duplicate entries."
        _LOGGER.error(f"[config:_validate_auth_block] {msg}")
        raise ConfigurationError(msg)
    for entry in backends:
        if not isinstance(entry, str):
            msg = (
                f"'backends' for {context} must contain only strings; "
                f"got element of type {type(entry).__name__}."
            )
            _LOGGER.error(f"[config:_validate_auth_block] {msg}")
            raise ConfigurationError(msg)
        if entry not in SUPPORTED_AUTH_BACKENDS:
            msg = (
                f"'backends' for {context} contains unsupported entry "
                f"'{entry}'; allowed values are "
                f"{sorted(SUPPORTED_AUTH_BACKENDS)}."
            )
            _LOGGER.error(f"[config:_validate_auth_block] {msg}")
            raise ConfigurationError(msg)

    allow_effective_user = auth.get("allow_effective_user", False)
    if "allow_effective_user" in auth:
        validate_field_type(context, "allow_effective_user", allow_effective_user, bool)
    if allow_effective_user and "password" not in backends:
        msg = (
            f"'allow_effective_user' for {context} can only be true when "
            f"'password' is included in 'backends'."
        )
        _LOGGER.error(f"[config:_validate_auth_block] {msg}")
        raise ConfigurationError(msg)


def _validate_session_creation(system_name: str, config: dict[str, Any]) -> None:
    """Validate the optional ``session_creation`` section.

    If absent, passes silently. When present:

    - ``session_creation`` may contain only ``max_concurrent_sessions``
      and ``defaults``.
    - ``max_concurrent_sessions`` must be a non-negative int.
    - ``defaults`` is required and must be a dict.
    - ``defaults.heap_size_gb`` is required.
    - Other ``defaults.*`` fields are optional but must match the
      allowed schema; unknowns are rejected.

    Args:
        system_name (str): The ``system_name`` value, used only in error
            messages.
        config (dict[str, Any]): The enterprise configuration dictionary
            (already validated at the top level).

    Raises:
        ConfigurationError: If ``session_creation`` contains unknown
            fields, ``max_concurrent_sessions`` is invalid, ``defaults``
            is missing or contains unknown fields, or
            ``defaults.heap_size_gb`` is missing.
    """
    session_creation = config.get("session_creation")
    if session_creation is None:
        return

    context = f"session_creation for enterprise system '{system_name}'"
    validate_allowed_fields(context, session_creation, _ALLOWED_SESSION_CREATION_FIELDS)

    if "max_concurrent_sessions" in session_creation:
        validate_non_negative_int(
            "max_concurrent_sessions",
            session_creation["max_concurrent_sessions"],
        )

    defaults = session_creation.get("defaults")
    if defaults is None:
        msg = (
            f"'session_creation.defaults' is required for enterprise system "
            f"'{system_name}' but is missing."
        )
        _LOGGER.error(f"[config:_validate_session_creation] {msg}")
        raise ConfigurationError(msg)

    defaults_context = (
        f"session_creation.defaults for enterprise system '{system_name}'"
    )
    validate_allowed_fields(
        defaults_context, defaults, _ALLOWED_SESSION_CREATION_DEFAULTS
    )

    if "heap_size_gb" not in defaults:
        msg = (
            f"'session_creation.defaults.heap_size_gb' is required for "
            f"enterprise system '{system_name}' but is missing."
        )
        _LOGGER.error(f"[config:_validate_session_creation] {msg}")
        raise ConfigurationError(msg)


# ---------------------------------------------------------------------------
# Top-level enterprise config validation
# ---------------------------------------------------------------------------


def validate_enterprise_config(config: Any) -> dict[str, Any]:
    """Validate a flat enterprise server configuration.

    Required fields:
        - ``system_name`` (str)
        - ``connection_json_url`` (str)
        - ``auth`` (dict): see :func:`_validate_auth_block`.

    Optional fields:
        - ``connection_timeout`` (int|float > 0)
        - ``session_idle_timeout_seconds`` (int|float > 0)
        - ``session_idle_sweep_interval_seconds`` (int|float > 0)
        - ``session_creation`` (dict): when present,
          ``defaults.heap_size_gb`` is required.

    Unknown fields at every level are rejected.

    Args:
        config (Any): The configuration to validate; must be a ``dict``
            for validation to succeed.

    Returns:
        dict[str, Any]: The same ``config`` object, unchanged, after
            successful validation. Returning the object (rather than
            ``None``) matches the validator signature expected by
            :func:`deephaven_mcp.config._base._load_and_validate_config`.

    Raises:
        ConfigurationError: For any validation failure.
    """
    _LOGGER.debug(
        "[config:validate_enterprise_config] Validating enterprise server config"
    )
    if not isinstance(config, dict):
        msg = (
            f"Enterprise system configuration must be a dictionary, but got "
            f"{type(config).__name__}."
        )
        _LOGGER.error(f"[config:validate_enterprise_config] {msg}")
        raise ConfigurationError(msg)

    # We need system_name for error context; validate it exists and is a str
    # before doing anything else. _validate_top_level_fields also does this,
    # but using the raw config value here gives clearer error messages when
    # system_name itself is missing.
    system_name_raw = config.get("system_name", "<unset>")
    system_name = system_name_raw if isinstance(system_name_raw, str) else "<invalid>"

    _validate_top_level_fields(system_name, config)
    _validate_auth_block(system_name, config["auth"])

    validate_optional_positive_number(config, "connection_timeout")
    validate_optional_positive_number(config, "session_idle_timeout_seconds")
    validate_optional_positive_number(config, "session_idle_sweep_interval_seconds")

    _validate_session_creation(system_name, config)

    _LOGGER.debug(
        f"[config:validate_enterprise_config] Enterprise system "
        f"'{system_name}' validation passed"
    )
    return config


async def _load_and_validate_enterprise_config(config_path: str) -> dict[str, Any]:
    """Load, parse, and validate the flat enterprise configuration from a JSON/JSON5 file."""
    return await _load_and_validate_config(
        config_path,
        validate_enterprise_config,
        "_load_and_validate_enterprise_config",
    )


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


class EnterpriseServerConfigManager(ConfigManager):
    """ConfigManager for the DHE MCP server (``dh-mcp-enterprise-server``).

    Reads a *flat* enterprise config file where the system fields sit at
    the top level (no system-name nesting). Validates the config as a
    single enterprise system and returns it directly.
    """

    async def get_config(self) -> dict[str, Any]:
        """Load and validate the flat enterprise config file (coroutine-safe).

        Returns:
            dict[str, Any]: The flat enterprise system config dict
                (fields at top level).

        Raises:
            RuntimeError: If no config path is provided and
                ``DH_MCP_CONFIG_FILE`` is unset.
            ConfigurationError: If the file cannot be read or fails
                validation.
        """
        _LOGGER.debug(
            "[EnterpriseServerConfigManager:get_config] Loading enterprise "
            "server configuration..."
        )
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug(
                    "[EnterpriseServerConfigManager:get_config] Using cached "
                    "configuration."
                )
                return self._cache

            resolved_path = (
                self._config_path
                if self._config_path is not None
                else _get_config_path()
            )
            flat_config = await _load_and_validate_enterprise_config(resolved_path)
            self._cache = flat_config
            _log_config_summary(
                flat_config,
                label="EnterpriseServerConfigManager:get_config",
                redactor=redact_enterprise_config,
            )
            _LOGGER.info(
                "[EnterpriseServerConfigManager:get_config] Enterprise "
                "configuration loaded successfully."
            )
            return flat_config

    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Inject a configuration dictionary into the cache (for testing).

        ``config`` is passed through :func:`validate_enterprise_config`
        before being cached, fulfilling the parent class's contract that
        subclasses must validate against their schema. Intended only for
        unit tests that need to seed a manager with a specific
        configuration without touching the filesystem.

        Args:
            config (dict[str, Any]): A raw configuration dictionary to
                validate and cache.

        Raises:
            ConfigurationError: If ``config`` fails enterprise schema
                validation.
        """
        async with self._lock:
            self._cache = validate_enterprise_config(config)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def get_enterprise_auth_backends(config: dict[str, Any]) -> list[str]:
    """Return the configured ``auth.backends`` list.

    Convenience accessor used by server startup to construct middleware
    backends without re-implementing the schema lookup.

    Args:
        config (dict[str, Any]): A validated enterprise configuration
            dictionary.

    Returns:
        list[str]: The configured backends, in declaration order.
    """
    return list(config["auth"]["backends"])


def get_enterprise_allow_effective_user(config: dict[str, Any]) -> bool:
    """Return the configured ``auth.allow_effective_user`` flag.

    Args:
        config (dict[str, Any]): A validated enterprise configuration
            dictionary.

    Returns:
        bool: ``True`` if ``auth.allow_effective_user`` is set to
            ``True``; ``False`` otherwise (including when the field is
            absent).
    """
    return bool(config["auth"].get("allow_effective_user", False))
