"""Configuration handling for the Deephaven MCP enterprise server.

Validates and redacts flat enterprise config files and provides the
:class:`EnterpriseServerConfigManager` used by ``dh-mcp-enterprise-server``.

Enterprise config file format (flat — all fields at top level)::

    {
        "system_name": "prod",
        "connection_json_url": "https://dhe.example.com/iris/connection.json",
        "auth_type": "password",
        "username": "user",
        "password_env_var": "DHE_PASSWORD",
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {"heap_size_gb": 4, "programming_language": "Python"}
        }
    }

Top-level schema:

- **Required**: ``system_name``, ``connection_json_url``, ``auth_type``.
- **Auth-specific** (required per ``auth_type``):

  * ``"password"``: ``username`` plus exactly one of ``password`` or
    ``password_env_var``.
  * ``"private_key"``: ``private_key_path``.

- **Optional**: ``session_creation``, ``connection_timeout``,
  ``mcp_session_idle_timeout_seconds``.

Supported ``auth_type`` values: ``"password"``, ``"private_key"``. Unknown
fields at every level are rejected.

Public API (re-exported via :mod:`deephaven_mcp.config`):

- :class:`EnterpriseServerConfigManager`
- :func:`validate_enterprise_config`
- :func:`redact_enterprise_config`
- :data:`DEFAULT_CONNECTION_TIMEOUT_SECONDS`
"""

__all__ = [
    "EnterpriseServerConfigManager",
    "validate_enterprise_config",
    "redact_enterprise_config",
    "DEFAULT_CONNECTION_TIMEOUT_SECONDS",
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
    validate_mutually_exclusive,
    validate_non_negative_int,
    validate_optional_positive_number,
)

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10.0
"""Default timeout in seconds for establishing connections to enterprise systems."""

_BASE_ENTERPRISE_FIELDS: dict[str, type | tuple[type, ...]] = {
    "system_name": str,
    "connection_json_url": str,
    "auth_type": str,
}
"""Base fields that every enterprise config must contain."""

_OPTIONAL_ENTERPRISE_FIELDS: dict[str, type | tuple[type, ...]] = {
    "session_creation": dict,
    "connection_timeout": (int, float),
    "mcp_session_idle_timeout_seconds": (int, float),
}
"""Optional top-level fields and their expected types."""

_AUTH_SPECIFIC_FIELDS: dict[str, dict[str, type | tuple[type, ...]]] = {
    "password": {
        "username": str,
        "password": str,
        "password_env_var": str,
    },
    "private_key": {
        "private_key_path": str,
    },
}
"""Per-``auth_type`` field schemas. Requirements and mutual-exclusivity are
enforced separately in :func:`_validate_auth_type_logic`."""

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
required when ``session_creation.defaults`` is present; the rest are optional."""


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def redact_enterprise_config(system_config: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``system_config`` with the ``password`` field redacted.

    Only the literal ``password`` field is redacted. Other auth-adjacent
    fields are intentionally preserved: ``password_env_var`` holds the *name*
    of an environment variable (not the secret itself), and
    ``private_key_path`` is a filesystem path. Neither reveals secret material
    on its own, so both are safe to include in log output.

    The returned dictionary is a shallow copy; the input ``system_config`` is
    never mutated.

    Args:
        system_config (dict[str, Any]): The enterprise system configuration
            dictionary.

    Returns:
        dict[str, Any]: A new dictionary with ``password`` replaced by
            ``"[REDACTED]"`` if present; all other fields preserved unchanged.
    """
    out = system_config.copy()
    if "password" in out:
        out["password"] = "[REDACTED]"  # noqa: S105
    return out


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _validate_top_level_fields(
    system_name: str, config: dict[str, Any]
) -> tuple[str, dict[str, type | tuple[type, ...]]]:
    """Validate base + optional + auth-specific top-level fields.

    Also determines the ``auth_type`` and returns the merged schema of all
    allowed top-level fields for that auth type.

    Args:
        system_name (str): The ``system_name`` value from ``config``, used
            only in error messages.
        config (dict[str, Any]): The enterprise configuration dictionary.

    Returns:
        tuple[str, dict[str, type | tuple[type, ...]]]: A tuple of
            ``(auth_type, merged_allowed_fields_schema)`` where the schema
            unions the base, optional, and auth-specific field tables.

    Raises:
        ConfigurationError: If a required base field is missing or has the
            wrong type, if ``auth_type`` is not one of the supported values,
            or if any unknown top-level field is present.
    """
    # 1. Required base fields must be present and correctly typed.
    for field_name, expected_type in _BASE_ENTERPRISE_FIELDS.items():
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

    # 2. Validate auth_type value and select the matching auth-specific schema.
    auth_type = config["auth_type"]
    if auth_type not in _AUTH_SPECIFIC_FIELDS:
        msg = (
            f"'auth_type' for enterprise system '{system_name}' must be one "
            f"of {sorted(_AUTH_SPECIFIC_FIELDS.keys())}, but got "
            f"'{auth_type}'."
        )
        _LOGGER.error(f"[config:_validate_top_level_fields] {msg}")
        raise ConfigurationError(msg)

    # 3. Build merged allowed set for strict unknown-field rejection.
    allowed = {
        **_BASE_ENTERPRISE_FIELDS,
        **_OPTIONAL_ENTERPRISE_FIELDS,
        **_AUTH_SPECIFIC_FIELDS[auth_type],
    }
    validate_allowed_fields(f"enterprise system '{system_name}'", config, allowed)

    return auth_type, allowed


def _validate_auth_type_logic(
    system_name: str, config: dict[str, Any], auth_type: str
) -> None:
    """Enforce auth-type-specific required fields and mutual-exclusivity rules.

    For ``auth_type == "password"``: ``username`` is required, and exactly
    one of ``password`` or ``password_env_var`` must be provided. For
    ``auth_type == "private_key"``: ``private_key_path`` is required.

    Args:
        system_name (str): The ``system_name`` value, used only in error
            messages.
        config (dict[str, Any]): The enterprise configuration dictionary.
        auth_type (str): The already-validated ``auth_type`` value (one of
            the keys in :data:`_AUTH_SPECIFIC_FIELDS`).

    Raises:
        ConfigurationError: If any auth-type-specific required field is
            missing, or if ``password`` and ``password_env_var`` are both
            specified.
    """
    context = f"enterprise system '{system_name}'"
    if auth_type == "password":
        if "username" not in config:
            msg = f"{context} with auth_type 'password' must define 'username'."
            _LOGGER.error(f"[config:_validate_auth_type_logic] {msg}")
            raise ConfigurationError(msg)
        validate_mutually_exclusive(context, config, "password", "password_env_var")
        if "password" not in config and "password_env_var" not in config:
            msg = (
                f"{context} with auth_type 'password' must define 'password' "
                f"or 'password_env_var'."
            )
            _LOGGER.error(f"[config:_validate_auth_type_logic] {msg}")
            raise ConfigurationError(msg)
    elif auth_type == "private_key":
        if "private_key_path" not in config:
            msg = (
                f"{context} with auth_type 'private_key' must define "
                f"'private_key_path'."
            )
            _LOGGER.error(f"[config:_validate_auth_type_logic] {msg}")
            raise ConfigurationError(msg)


def _validate_session_creation(system_name: str, config: dict[str, Any]) -> None:
    """Validate the optional ``session_creation`` section.

    If absent, passes silently. When present:

    - ``session_creation`` may contain only ``max_concurrent_sessions`` and
      ``defaults``.
    - ``max_concurrent_sessions`` must be a non-negative int.
    - ``defaults`` is required and must be a dict.
    - ``defaults.heap_size_gb`` is required.
    - Other ``defaults.*`` fields are optional but must match the allowed
      schema; unknowns are rejected.

    Args:
        system_name (str): The ``system_name`` value, used only in error
            messages.
        config (dict[str, Any]): The enterprise configuration dictionary
            (already validated at the top level).

    Raises:
        ConfigurationError: If ``session_creation`` contains unknown fields,
            ``max_concurrent_sessions`` is invalid, ``defaults`` is missing
            or contains unknown fields, or ``defaults.heap_size_gb`` is
            missing.
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
        - ``auth_type`` (str): ``"password"`` or ``"private_key"``

    Authentication requirements:
        - ``password``: requires ``username`` and exactly one of ``password``
          or ``password_env_var``.
        - ``private_key``: requires ``private_key_path``.

    Optional fields:
        - ``connection_timeout`` (int|float > 0)
        - ``mcp_session_idle_timeout_seconds`` (int|float > 0)
        - ``session_creation`` (dict): when present, ``defaults.heap_size_gb``
          is required.

    Unknown fields at every level are rejected.

    Args:
        config (Any): The configuration to validate; must be a ``dict`` for
            validation to succeed.

    Returns:
        dict[str, Any]: The same ``config`` object, unchanged, after
            successful validation. Returning the object (rather than ``None``)
            matches the validator signature expected by
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

    auth_type, _allowed = _validate_top_level_fields(system_name, config)
    _validate_auth_type_logic(system_name, config, auth_type)

    validate_optional_positive_number(config, "connection_timeout")
    validate_optional_positive_number(config, "mcp_session_idle_timeout_seconds")

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

    Reads a *flat* enterprise config file where the system fields sit at the
    top level (no system-name nesting). Validates the config as a single
    enterprise system and returns it directly.
    """

    async def get_config(self) -> dict[str, Any]:
        """Load and validate the flat enterprise config file (coroutine-safe).

        Returns:
            dict[str, Any]: The flat enterprise system config dict (fields at
                top level).

        Raises:
            RuntimeError: If no config path is provided and
                ``DH_MCP_CONFIG_FILE`` is unset.
            ConfigurationError: If the file cannot be read or fails validation.
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

        ``config`` is passed through :func:`validate_enterprise_config` before
        being cached, fulfilling the parent class's contract that subclasses
        must validate against their schema. Intended only for unit tests that
        need to seed a manager with a specific configuration without touching
        the filesystem.

        Args:
            config (dict[str, Any]): A raw configuration dictionary to
                validate and cache.

        Raises:
            ConfigurationError: If ``config`` fails enterprise schema
                validation.
        """
        async with self._lock:
            self._cache = validate_enterprise_config(config)
