"""Configuration handling for the Deephaven MCP community server.

Validates and redacts community config files and provides the
:class:`CommunityServerConfigManager` used by ``dh-mcp-community-server``.

Community config file format (flat — all keys at top level)::

    {
        "security": {"credential_retrieval_mode": "dynamic_only"},
        "sessions": {
            "local": {"host": "localhost", "port": 10000, "auth_type": "PSK", "auth_token": "..."}
        },
        "session_creation": {"defaults": {"launch_method": "python"}},
        "mcp_session_idle_timeout_seconds": 3600
    }

Valid top-level keys: ``security``, ``sessions``, ``session_creation``,
``mcp_session_idle_timeout_seconds``. Unknown keys at any level are rejected.

Public API (re-exported via :mod:`deephaven_mcp.config`):

- :class:`CommunityServerConfigManager`
- :func:`validate_community_config`
- :func:`validate_community_session_config`
- :func:`redact_community_config`
- :func:`redact_community_session_config`

All validation errors raise :class:`ConfigurationError` with
descriptive messages. Sensitive fields (auth tokens, binary TLS material) are
redacted from log output.
"""

__all__ = [
    "CommunityServerConfigManager",
    "validate_community_config",
    "validate_community_session_config",
    "redact_community_config",
    "redact_community_session_config",
]

import copy
import logging
import types
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
    validate_mutually_exclusive,
    validate_non_negative_int,
    validate_optional_positive_number,
    validate_optional_string_dict,
    validate_optional_string_list,
    validate_positive_number,
)

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_KNOWN_AUTH_TYPES: set[str] = {
    "PSK",
    "Anonymous",
    "Basic",
    "io.deephaven.authentication.psk.PskAuthenticationHandler",
}
"""Commonly known ``auth_type`` values. Custom authenticators are also valid
but will trigger a warning."""

_ALLOWED_COMMUNITY_SESSION_FIELDS: dict[str, type | tuple[type, ...]] = {
    "host": str,
    "port": int,
    "auth_type": str,
    "auth_token": str,
    "auth_token_env_var": str,
    "never_timeout": bool,
    "session_type": str,
    "use_tls": bool,
    "tls_root_certs": (str, types.NoneType),
    "client_cert_chain": (str, types.NoneType),
    "client_private_key": (str, types.NoneType),
}
"""Allowed per-session fields and their expected types."""

_VALID_CREDENTIAL_RETRIEVAL_MODES: set[str] = {
    "none",
    "dynamic_only",
    "static_only",
    "all",
}
"""Valid values for ``security.credential_retrieval_mode``."""

_ALLOWED_SECURITY_FIELDS: dict[str, type | tuple[type, ...]] = {
    "credential_retrieval_mode": str,
}
"""Allowed fields in the ``security`` section."""

_ALLOWED_LAUNCH_METHODS: set[str] = {"docker", "python"}
"""Allowed values for ``session_creation.defaults.launch_method``."""

_ALLOWED_SESSION_CREATION_FIELDS: dict[str, type | tuple[type, ...]] = {
    "max_concurrent_sessions": int,
    "defaults": dict,
}
"""Allowed top-level fields in ``session_creation``."""

_ALLOWED_SESSION_CREATION_DEFAULTS: dict[str, type | tuple[type, ...]] = {
    "launch_method": str,
    "auth_type": str,
    "auth_token": (str, types.NoneType),
    "auth_token_env_var": (str, types.NoneType),
    "programming_language": str,
    "docker_image": str,
    "docker_memory_limit_gb": (float, int, types.NoneType),
    "docker_cpu_limit": (float, int, types.NoneType),
    "docker_volumes": list,
    "python_venv_path": (str, types.NoneType),
    "heap_size_gb": (int, float),
    "extra_jvm_args": list,
    "environment_vars": dict,
    "startup_timeout_seconds": (float, int),
    "startup_check_interval_seconds": (float, int),
    "startup_retries": int,
}
"""Allowed fields inside ``session_creation.defaults``."""

_ALLOWED_TOP_LEVEL_FIELDS: dict[str, type | tuple[type, ...]] = {
    "security": dict,
    "sessions": dict,
    "session_creation": dict,
    "mcp_session_idle_timeout_seconds": (int, float),
}
"""Allowed top-level keys in a community config file."""


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def redact_community_session_config(
    session_config: dict[str, Any], redact_binary_values: bool = True
) -> dict[str, Any]:
    """Return a copy of ``session_config`` with sensitive fields redacted.

    - ``auth_token`` is redacted when truthy.
    - ``tls_root_certs``, ``client_cert_chain``, ``client_private_key`` are
      redacted only when the value is binary (``bytes``/``bytearray``) and
      ``redact_binary_values`` is ``True``. String values (e.g., filesystem
      paths) are preserved as-is.

    The returned dictionary is a shallow copy; nested containers are not
    deep-copied, but the redaction sites above write scalar replacements so
    no mutation leaks back to the caller's dictionary.

    Args:
        session_config (dict[str, Any]): The per-session configuration
            dictionary.
        redact_binary_values (bool): If ``False``, skip binary TLS field
            redaction. Defaults to ``True``.

    Returns:
        dict[str, Any]: A new dictionary with the same structure; sensitive
            fields replaced with the string ``"[REDACTED]"`` where applicable.
    """
    out = dict(session_config)
    if out.get("auth_token"):
        out["auth_token"] = "[REDACTED]"  # noqa: S105
    if redact_binary_values:
        for key in ("tls_root_certs", "client_cert_chain", "client_private_key"):
            value = out.get(key)
            if value and isinstance(value, bytes | bytearray):
                out[key] = "[REDACTED]"
    return out


def _redact_session_creation_config(
    session_creation_config: dict[str, Any],
) -> dict[str, Any]:
    """Return a deep copy of ``session_creation_config`` with ``defaults.auth_token`` redacted."""
    out = copy.deepcopy(session_creation_config)
    defaults = out.get("defaults")
    if isinstance(defaults, dict) and "auth_token" in defaults:
        defaults["auth_token"] = "[REDACTED]"  # noqa: S105
    return out


def redact_community_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return a deep copy of ``config`` with every sensitive field redacted.

    Walks the two subsections that can contain secrets and applies the
    matching per-unit redactor:

    - ``sessions.<name>`` entries are redacted via
      :func:`redact_community_session_config` (with binary TLS material also
      redacted).
    - ``session_creation.defaults.auth_token``, if present, is replaced with
      ``"[REDACTED]"``.

    All other sections (e.g., ``security``, ``mcp_session_idle_timeout_seconds``)
    are passed through unchanged. The input ``config`` is never mutated.

    Args:
        config (dict[str, Any]): The full community configuration dictionary.

    Returns:
        dict[str, Any]: A deep copy of ``config`` safe to include in log
            output.
    """
    out = copy.deepcopy(config)
    sessions = out.get("sessions")
    if isinstance(sessions, dict):
        out["sessions"] = {
            name: redact_community_session_config(cfg) if isinstance(cfg, dict) else cfg
            for name, cfg in sessions.items()
        }
    session_creation = out.get("session_creation")
    if isinstance(session_creation, dict):
        out["session_creation"] = _redact_session_creation_config(session_creation)
    return out


# ---------------------------------------------------------------------------
# Validation — security
# ---------------------------------------------------------------------------


def _validate_security_config(security_config: Any) -> None:
    """Validate the optional ``security`` section.

    Args:
        security_config (Any): The ``security`` value from the config (already
            confirmed to be a dict by :func:`validate_community_config`).

    Raises:
        ConfigurationError: If an unknown field is present, or
            ``credential_retrieval_mode`` is not a valid enum value.
    """
    validate_allowed_fields(
        "'security' section", security_config, _ALLOWED_SECURITY_FIELDS
    )

    mode = security_config.get("credential_retrieval_mode")
    if mode is not None and mode not in _VALID_CREDENTIAL_RETRIEVAL_MODES:
        valid = ", ".join(sorted(_VALID_CREDENTIAL_RETRIEVAL_MODES))
        msg = (
            f"'security.credential_retrieval_mode' must be one of "
            f"[{valid}], got '{mode}'."
        )
        _LOGGER.error(f"[config:_validate_security_config] {msg}")
        raise ConfigurationError(msg)


# ---------------------------------------------------------------------------
# Validation — sessions
# ---------------------------------------------------------------------------


def validate_community_session_config(session_name: str, config_item: Any) -> None:
    """Validate a single community session's configuration dictionary.

    Checks type, allowed fields, and auth-related rules (mutual exclusivity of
    ``auth_token`` and ``auth_token_env_var``; warning for unknown
    ``auth_type`` values).

    Args:
        session_name (str): The session name (used in error messages).
        config_item (Any): The session configuration to validate. Must be a
            ``dict`` for validation to succeed; any other type raises.

    Raises:
        ConfigurationError: If the configuration is invalid.
    """
    context = f"session '{session_name}'"
    if not isinstance(config_item, dict):
        msg = f"{context} must be a dictionary, got " f"{type(config_item).__name__}."
        _LOGGER.error(f"[config:validate_community_session_config] {msg}")
        raise ConfigurationError(msg)

    validate_allowed_fields(context, config_item, _ALLOWED_COMMUNITY_SESSION_FIELDS)
    validate_mutually_exclusive(
        context, config_item, "auth_token", "auth_token_env_var"
    )

    auth_type = config_item.get("auth_type")
    if auth_type is not None and auth_type not in _KNOWN_AUTH_TYPES:
        _LOGGER.warning(
            f"[config:validate_community_session_config] {context} "
            f"uses auth_type='{auth_type}' which is not a commonly known "
            f"value. Known values: {sorted(_KNOWN_AUTH_TYPES)}. Custom "
            f"authenticators are also valid."
        )


def _validate_sessions_config(sessions_map: Any) -> None:
    """Validate the optional ``sessions`` section (a dict of session configs)."""
    if not isinstance(sessions_map, dict):
        msg = f"'sessions' must be a dictionary, got " f"{type(sessions_map).__name__}."
        _LOGGER.error(f"[config:_validate_sessions_config] {msg}")
        raise ConfigurationError(msg)
    for session_name, session_cfg in sessions_map.items():
        validate_community_session_config(session_name, session_cfg)


# ---------------------------------------------------------------------------
# Validation — session_creation
# ---------------------------------------------------------------------------


def _validate_session_creation_defaults(defaults: dict[str, Any]) -> None:
    """Validate the ``defaults`` sub-section of ``session_creation``."""
    context = "'session_creation.defaults'"
    validate_allowed_fields(context, defaults, _ALLOWED_SESSION_CREATION_DEFAULTS)
    validate_mutually_exclusive(context, defaults, "auth_token", "auth_token_env_var")

    launch_method = defaults.get("launch_method")
    if launch_method is not None and launch_method not in _ALLOWED_LAUNCH_METHODS:
        msg = (
            f"'session_creation.defaults.launch_method' must be one of "
            f"{sorted(_ALLOWED_LAUNCH_METHODS)}, got '{launch_method}'."
        )
        _LOGGER.error(f"[config:_validate_session_creation_defaults] {msg}")
        raise ConfigurationError(msg)

    auth_type = defaults.get("auth_type")
    if auth_type is not None and auth_type not in _KNOWN_AUTH_TYPES:
        _LOGGER.warning(
            f"[config:_validate_session_creation_defaults] "
            f"session_creation.defaults uses auth_type='{auth_type}' which "
            f"is not a commonly known value. Known values: "
            f"{sorted(_KNOWN_AUTH_TYPES)}. Custom authenticators are also valid."
        )

    validate_optional_positive_number(defaults, "heap_size_gb")
    validate_optional_positive_number(defaults, "docker_memory_limit_gb")
    validate_optional_positive_number(defaults, "docker_cpu_limit")
    validate_optional_positive_number(defaults, "startup_timeout_seconds")
    validate_optional_positive_number(defaults, "startup_check_interval_seconds")
    validate_optional_string_list(defaults, "docker_volumes")
    validate_optional_string_list(defaults, "extra_jvm_args")
    validate_optional_string_dict(defaults, "environment_vars")
    if "startup_retries" in defaults:
        validate_non_negative_int("startup_retries", defaults["startup_retries"])


def _validate_session_creation_config(session_creation_config: Any) -> None:
    """Validate the optional ``session_creation`` section."""
    if not isinstance(session_creation_config, dict):
        msg = (
            f"'session_creation' must be a dictionary, got "
            f"{type(session_creation_config).__name__}."
        )
        _LOGGER.error(f"[config:_validate_session_creation_config] {msg}")
        raise ConfigurationError(msg)

    validate_allowed_fields(
        "'session_creation' section",
        session_creation_config,
        _ALLOWED_SESSION_CREATION_FIELDS,
    )

    if "max_concurrent_sessions" in session_creation_config:
        validate_non_negative_int(
            "max_concurrent_sessions",
            session_creation_config["max_concurrent_sessions"],
        )

    if "defaults" in session_creation_config:
        _validate_session_creation_defaults(session_creation_config["defaults"])


# ---------------------------------------------------------------------------
# Top-level community config validation
# ---------------------------------------------------------------------------


def validate_community_config(config: Any) -> dict[str, Any]:
    """Validate a community configuration dictionary.

    Ensures only the allowed top-level keys are present with correct types,
    then delegates to the section-specific validators for each present section.

    Args:
        config (Any): The parsed configuration; must be a ``dict`` for
            validation to succeed.

    Returns:
        dict[str, Any]: The same ``config`` object, unchanged, after
            successful validation. Returning the object (rather than ``None``)
            matches the validator signature expected by
            :func:`deephaven_mcp.config._base._load_and_validate_config`.

    Raises:
        ConfigurationError: If ``config`` is not a dict, an unknown top-level
            key is present, or any section fails its schema.
    """
    if not isinstance(config, dict):
        msg = (
            f"Community configuration must be a dictionary, got "
            f"{type(config).__name__}."
        )
        _LOGGER.error(f"[config:validate_community_config] {msg}")
        raise ConfigurationError(msg)

    # Top-level: reject unknowns, check types.
    validate_allowed_fields(
        "community configuration", config, _ALLOWED_TOP_LEVEL_FIELDS
    )

    if "security" in config:
        _validate_security_config(config["security"])
    if "sessions" in config:
        _validate_sessions_config(config["sessions"])
    if "session_creation" in config:
        _validate_session_creation_config(config["session_creation"])
    if "mcp_session_idle_timeout_seconds" in config:
        validate_positive_number(
            "mcp_session_idle_timeout_seconds",
            config["mcp_session_idle_timeout_seconds"],
        )

    _LOGGER.info("[config:validate_community_config] Configuration validation passed.")
    return config


async def _load_and_validate_community_config(config_path: str) -> dict[str, Any]:
    """Load, parse, and validate the community configuration from a JSON/JSON5 file."""
    return await _load_and_validate_config(
        config_path,
        validate_community_config,
        "_load_and_validate_community_config",
    )


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


class CommunityServerConfigManager(ConfigManager):
    """ConfigManager for the DHC MCP server (``dh-mcp-community-server``).

    Reads a community config file. The format uses ``sessions``,
    ``session_creation``, ``security``, and ``mcp_session_idle_timeout_seconds``
    as optional top-level keys; validation enforces the community schema.
    """

    async def get_config(self) -> dict[str, Any]:
        """Load and validate the community config file (coroutine-safe).

        Returns:
            dict[str, Any]: The validated community configuration dictionary.

        Raises:
            RuntimeError: If no config path is provided and
                ``DH_MCP_CONFIG_FILE`` is unset.
            ConfigurationError: If the file cannot be read or fails validation.
        """
        _LOGGER.debug(
            "[CommunityServerConfigManager:get_config] Loading Deephaven MCP "
            "application configuration..."
        )
        async with self._lock:
            if self._cache is not None:
                _LOGGER.debug(
                    "[CommunityServerConfigManager:get_config] Using cached "
                    "configuration."
                )
                return self._cache

            resolved_path = (
                self._config_path
                if self._config_path is not None
                else _get_config_path()
            )
            validated = await _load_and_validate_community_config(resolved_path)
            self._cache = validated
            _log_config_summary(
                validated,
                label="CommunityServerConfigManager:get_config",
                redactor=redact_community_config,
            )
            _LOGGER.info(
                "[CommunityServerConfigManager:get_config] Community "
                "configuration loaded successfully."
            )
            return self._cache

    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Inject a configuration dictionary into the cache (for testing).

        ``config`` is passed through :func:`validate_community_config` before
        being cached, fulfilling the parent class's contract that subclasses
        must validate against their schema. Intended only for unit tests that
        need to seed a manager with a specific configuration without touching
        the filesystem.

        Args:
            config (dict[str, Any]): A raw configuration dictionary to
                validate and cache.

        Raises:
            ConfigurationError: If ``config`` fails community schema
                validation.
        """
        async with self._lock:
            self._cache = validate_community_config(config)
