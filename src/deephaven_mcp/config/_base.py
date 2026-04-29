"""Shared base infrastructure for Deephaven MCP configuration management.

This module provides the abstract :class:`ConfigManager` base class and shared
file-loading utilities used by both the community and enterprise server config managers.

Concrete subclasses live in their respective modules:

- :class:`~deephaven_mcp.config.community.CommunityServerConfigManager` in ``community.py``
- :class:`~deephaven_mcp.config.enterprise.EnterpriseServerConfigManager` in ``enterprise.py``

Private module-level helpers used by the concrete subclasses:

- :func:`_get_config_path`: resolve path from ``DH_MCP_CONFIG_FILE``.
- :func:`_load_and_validate_config`: combined load + validate with error wrapping.
- :func:`_log_config_summary`: pretty-prints the (optionally redacted) config to the log.

And one internal helper used only within this module:

- :func:`_load_config_from_file`: async JSON/JSON5 read with error wrapping
  (called by :func:`_load_and_validate_config`).
"""

__all__ = [
    "ConfigManager",
    "CONFIG_ENV_VAR",
    "DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS",
]

import abc
import asyncio
import logging
import os
from collections.abc import Callable
from typing import Any, cast

import aiofiles
import json5

from deephaven_mcp._exceptions import ConfigurationError

_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "DH_MCP_CONFIG_FILE"
"""Name of the environment variable specifying the path to the Deephaven MCP config file."""

DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS: float = 3600.0
"""Default MCP session idle timeout in seconds (1 hour).

After this many seconds of inactivity from an MCP client, its per-session
Deephaven registry is closed by the TTL sweeper.  Overridable per-server via the
``mcp_session_idle_timeout_seconds`` config file key.
"""


class ConfigManager(abc.ABC):
    """Abstract base class for Deephaven MCP configuration managers.

    Provides the common interface and shared infrastructure for coroutine-safe,
    cached configuration loading. Concrete subclasses implement config-format-specific
    loading and validation logic.

    Subclasses:
        - :class:`~deephaven_mcp.config.community.CommunityServerConfigManager`: Loads community-format config files.
        - :class:`~deephaven_mcp.config.enterprise.EnterpriseServerConfigManager`: Loads flat enterprise-format config files.

    Common features:
        - **Coroutine-safe**: Uses asyncio.Lock to prevent concurrent loads.
        - **Caching**: Loads configuration once; subsequent calls return the cached value.
        - **Cache control**: :meth:`clear_config_cache` forces reload on next access.
    """

    def __init__(self, config_path: str | None = None) -> None:
        """Initialize a new ConfigManager instance.

        Sets up the internal configuration cache and an asyncio.Lock for coroutine safety.

        Args:
            config_path (str | None): Optional explicit path to the configuration file.
                If provided, this takes precedence over the ``DH_MCP_CONFIG_FILE`` environment
                variable. If ``None`` (default), the environment variable is used.
        """
        self._config_path = config_path
        self._cache: dict[str, Any] | None = None
        self._lock = asyncio.Lock()

    async def clear_config_cache(self) -> None:
        """Clear the cached Deephaven configuration (coroutine-safe).

        Forces the next configuration access to reload from disk. Useful for tests
        or when the config file has changed.
        """
        _LOGGER.debug(
            "[ConfigManager:clear_config_cache] Clearing Deephaven configuration cache..."
        )
        async with self._lock:
            self._cache = None

        _LOGGER.debug("[ConfigManager:clear_config_cache] Configuration cache cleared.")

    @abc.abstractmethod
    async def get_config(self) -> dict[str, Any]:
        """Load and return the validated configuration (coroutine-safe).

        Subclasses must implement format-specific loading and validation. The
        implementation is expected to:

        - Return the cached dict on subsequent calls (the cache is cleared by
          :meth:`clear_config_cache`).
        - Validate the configuration against the subclass's schema before
          caching and returning it.
        - Hold ``self._lock`` while mutating ``self._cache`` to ensure
          coroutine-safety.

        Returns:
            dict[str, Any]: The validated configuration dictionary.

        Raises:
            RuntimeError: If no config path was provided to ``__init__`` and
                the ``DH_MCP_CONFIG_FILE`` environment variable is unset.
            ConfigurationError: If the file cannot be read, parsed, or fails
                schema validation.
        """
        ...

    async def get_mcp_session_idle_timeout_seconds(self) -> float:
        """Return the MCP session idle timeout in seconds.

        Reads the optional ``mcp_session_idle_timeout_seconds`` key from the
        loaded configuration and returns it as a float.  If the key is absent,
        :data:`DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS` is returned.

        The value is guaranteed to be positive because the concrete config
        validators reject non-positive values during :meth:`get_config`.

        Returns:
            float: Idle timeout in seconds. Always positive.

        Raises:
            RuntimeError: Propagated from :meth:`get_config` when the config
                path is unresolvable.
            ConfigurationError: Propagated from :meth:`get_config` when the
                file cannot be read, parsed, or validated.
        """
        config = await self.get_config()
        return float(
            config.get(
                "mcp_session_idle_timeout_seconds",
                DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS,
            )
        )

    @abc.abstractmethod
    async def _set_config_cache(self, config: dict[str, Any]) -> None:
        """PRIVATE: Inject a configuration dictionary into the cache (for testing).

        Intended only for unit tests that need to seed a manager with a
        specific configuration without touching the filesystem. Subclasses
        must validate ``config`` against their schema before caching it so
        that downstream ``get_config`` calls observe the same invariants as
        after a real file load.

        Args:
            config (dict[str, Any]): A raw configuration dictionary to validate
                and cache.

        Raises:
            ConfigurationError: If ``config`` fails the subclass's schema
                validation.
        """
        ...


async def _load_config_from_file(config_path: str) -> dict[str, Any]:
    """Load and parse a Deephaven MCP configuration file (JSON or JSON5) using async I/O.

    Uses aiofiles for non-blocking file reads, ensuring the event loop is not blocked
    during file I/O operations. Parsing is performed with ``json5.loads``, so both
    standard JSON and JSON5 (comments, trailing commas, unquoted keys, etc.) are
    accepted. All file-access and parsing errors are caught and wrapped as
    ConfigurationError with descriptive messages.

    Args:
        config_path (str): The absolute or relative path to the configuration JSON/JSON5 file.

    Returns:
        dict[str, Any]: The parsed configuration cast as a dictionary. No runtime check
            is performed that the parsed root is actually a mapping; callers (typically
            the validator passed to :func:`_load_and_validate_config`) are responsible
            for verifying the top-level structure.

    Raises:
        ConfigurationError: Wraps any of the following underlying failures:
            - File not found (:class:`FileNotFoundError`)
            - Permission denied (:class:`PermissionError`)
            - Invalid JSON/JSON5 syntax (:class:`ValueError`, raised by ``json5.loads``)
            - Any other unexpected error during file read or parsing
              (caught via a broad ``Exception`` handler)
    """
    try:
        async with aiofiles.open(config_path) as f:
            content = await f.read()
        return cast(dict[str, Any], json5.loads(content))
    except FileNotFoundError:
        _LOGGER.error(
            f"[_load_config_from_file] Configuration file not found: {config_path}"
        )
        raise ConfigurationError(
            f"Configuration file not found: {config_path}"
        ) from None
    except PermissionError:
        _LOGGER.error(
            f"[_load_config_from_file] Permission denied when trying to read configuration file: {config_path}"
        )
        raise ConfigurationError(
            f"Permission denied when trying to read configuration file: {config_path}"
        ) from None
    except ValueError as e:
        _LOGGER.error(
            f"[_load_config_from_file] Invalid JSON/JSON5 in configuration file {config_path}: {e}"
        )
        raise ConfigurationError(
            f"Invalid JSON/JSON5 in configuration file {config_path}: {e}"
        ) from e
    except Exception as e:
        _LOGGER.error(
            f"[_load_config_from_file] Unexpected error reading configuration file {config_path}: {e}"
        )
        raise ConfigurationError(
            f"Unexpected error loading or parsing config file {config_path}: {e}"
        ) from e


def _get_config_path() -> str:
    """Retrieve the configuration file path from the DH_MCP_CONFIG_FILE environment variable.

    Returns:
        str: The raw value of ``DH_MCP_CONFIG_FILE``, which should be an
            absolute or relative path to a Deephaven MCP configuration
            JSON/JSON5 file. No existence or format check is performed here;
            those happen in :func:`_load_config_from_file`.

    Raises:
        RuntimeError: If the DH_MCP_CONFIG_FILE environment variable is not set.
    """
    if CONFIG_ENV_VAR not in os.environ:
        _LOGGER.error(
            f"[_get_config_path] Environment variable {CONFIG_ENV_VAR} is not set."
        )
        raise RuntimeError(f"Environment variable {CONFIG_ENV_VAR} is not set.")
    config_path = os.environ[CONFIG_ENV_VAR]
    _LOGGER.info(
        f"[_get_config_path] Environment variable {CONFIG_ENV_VAR} is set to: {config_path}"
    )
    return config_path


async def _load_and_validate_config(
    config_path: str,
    validator: Callable[[dict[str, Any]], dict[str, Any]],
    caller: str,
) -> dict[str, Any]:
    """Load a config file and run a validator; wrap any error as ConfigurationError.

    Args:
        config_path (str): Path to the JSON/JSON5 config file.
        validator (Callable[[dict[str, Any]], dict[str, Any]]): Function that validates and returns the parsed dict.
        caller (str): Caller label used in error log messages when loading or validation fails.

    Returns:
        dict[str, Any]: The fully validated configuration dictionary.

    Raises:
        ConfigurationError: For any failure during loading or validation.
    """
    try:
        data = await _load_config_from_file(config_path)
        return validator(data)
    except Exception as e:
        _LOGGER.error(f"[{caller}] Error loading configuration file {config_path}: {e}")
        raise ConfigurationError(f"Error loading configuration file: {e}") from e


def _log_config_summary(
    config: dict[str, Any],
    label: str = "ConfigManager:get_config",
    redactor: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> None:
    """Log a summary of the loaded Deephaven MCP configuration.

    Always emits an INFO "Configuration summary:" header, followed by the
    configuration body. The body is pretty-printed (indented, key-sorted)
    JSON produced by ``json5.dumps`` and logged at INFO level. If a
    ``redactor`` callable is supplied, it is applied to the config first so
    that sensitive fields (auth tokens, passwords, private keys, etc.) can be
    replaced with ``"[REDACTED]"`` before logging; if ``redactor`` is
    ``None`` the configuration is logged as-is with no redaction. If JSON
    serialization fails (``TypeError`` or ``ValueError``), a WARNING is
    emitted describing the failure and the (optionally redacted) config is
    then logged at INFO level as its Python ``dict`` representation instead.

    Args:
        config (dict[str, Any]): The loaded and validated configuration dictionary.
        label (str): Log prefix label identifying the caller. Real callers
            should pass a subclass-specific label such as
            ``"CommunityServerConfigManager:get_config"`` or
            ``"EnterpriseServerConfigManager:get_config"``; the default
            ``"ConfigManager:get_config"`` is a generic placeholder since
            :class:`ConfigManager` is abstract and cannot be instantiated
            directly.
        redactor (Callable[[dict[str, Any]], dict[str, Any]] | None): Optional function to
            redact sensitive fields before logging. If ``None``, the config is logged
            without redaction.
    """
    _LOGGER.info(f"[{label}] Configuration summary:")

    redacted_config = redactor(config) if redactor is not None else config

    try:
        formatted_config = json5.dumps(redacted_config, indent=2, sort_keys=True)
        _LOGGER.info(f"[{label}] Loaded configuration:\n{formatted_config}")
    except (TypeError, ValueError) as e:
        _LOGGER.warning(f"[{label}] Failed to format config as JSON: {e}")
        _LOGGER.info(f"[{label}] Loaded configuration: {redacted_config}")
