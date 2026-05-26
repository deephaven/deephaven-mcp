"""General-purpose configuration primitives for Deephaven MCP servers.

This package owns the reusable plumbing that any MCP server in the
project can use to load a JSON5-on-disk configuration directory:

- :func:`verify_config_directory_permissions` — strict POSIX /
  best-effort Windows permission audit of a configuration directory.
- :func:`default_config_dir` — platform-default configuration
  directory (``~/.deephaven/ai/config`` on POSIX,
  ``%APPDATA%/Deephaven/ai/config`` on Windows).
- :data:`CONFIG_DIR_ENV_VAR` — name of the environment variable that
  overrides :func:`default_config_dir`.
- :class:`~deephaven_mcp._exceptions.ConfigurationError` — re-export
  so callers that handle config-loading failures can import the
  exception without reaching into ``deephaven_mcp._exceptions``.

Two more module-level primitives live alongside but are not surfaced
in ``__all__`` because their signatures are intentionally still
project-private:

- :func:`deephaven_mcp.config._file_loader.load_config_from_file` —
  async JSON5 reader + templating + ``ConfigurationError`` wrapping.
- :mod:`deephaven_mcp.config._templating` — placeholder engine
  resolving ``${env:VAR}`` / ``${env:VAR:-default}`` / ``${file:PATH}``
  inside a parsed JSON tree.

The systems-server-specific schema models (``ServerConfig``,
``CommunitySettings``, ``EnterpriseSystemConfig`` umbrellas, the
``MultiSystemConfigManager`` orchestrator, etc.) live in
:mod:`deephaven_mcp.mcp_systems_server.config`; per-session and
per-system declaration types live in :mod:`deephaven_mcp.sessions`.
"""

__all__ = [
    "CONFIG_DIR_ENV_VAR",
    "ConfigurationError",
    "default_config_dir",
    "resolve_config_dir",
    "verify_config_directory_permissions",
]

from deephaven_mcp._exceptions import ConfigurationError

from ._config_dir import CONFIG_DIR_ENV_VAR, default_config_dir, resolve_config_dir
from ._dir_permissions import verify_config_directory_permissions
