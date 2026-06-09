"""General-purpose configuration primitives for Deephaven MCP servers.

This package owns the reusable plumbing that any MCP server in the
project can use to locate, audit, and load a JSON5-on-disk
configuration tree.

User-data-root resolution
-------------------------

Every directory the MCP server family reads from or writes to lives
under a single user-data root (``~/.deephaven/ai`` on POSIX;
``%APPDATA%/Deephaven/ai`` on Windows). One environment variable —
:data:`DATA_DIR_ENV_VAR` (``DH_MCP_DATA_DIR``) — overrides that root,
and per-subdir resolvers compose paths under whatever root that
function returns:

- :func:`resolve_data_root` — env override or platform default; the
  *only* function that consults the env var.
- :func:`resolve_config_dir` — read-only configuration tree consumed
  by :class:`~deephaven_mcp.mcp_systems_server.config.ConfigTreeLoader`.
- :func:`resolve_runtime_dir` — mutable per-user state owned by the
  running daemon (registry, lock, log).
- :func:`daemon_dir` — the ``daemon/`` subdirectory under the
  runtime root; deterministic and platform-independent given a
  resolved runtime directory.

Per-subdir env vars (``DH_MCP_CONFIG_DIR``, ``DH_MCP_RUNTIME_DIR``)
are intentionally absent: a single root knob serves the operator use
cases (containers, chroots, custom install layouts), and per-subdir
overrides are still available via explicit :class:`~pathlib.Path`
arguments to the resolvers (CLI flags, test fixtures).

Audit + loading primitives
--------------------------

- :func:`verify_config_directory_permissions` — strict POSIX /
  best-effort Windows permission audit of a configuration directory.
- :func:`harden_private_dir` — idempotent ``mkdir + chmod 0o700`` for
  any per-user-private directory in this project (the runtime
  directory and the daemon subdirectory beneath it).
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
``ConfigTreeLoader`` orchestrator, etc.) live in
:mod:`deephaven_mcp.mcp_systems_server.config`; per-session and
per-system declaration types live in :mod:`deephaven_mcp.sessions`.
"""

__all__ = [
    "DATA_DIR_ENV_VAR",
    "ConfigurationError",
    "daemon_dir",
    "harden_private_dir",
    "resolve_config_dir",
    "resolve_data_root",
    "resolve_runtime_dir",
    "verify_config_directory_permissions",
]

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._platform.dir_permissions import harden_private_dir

from ._config_dir import resolve_config_dir
from ._data_root import DATA_DIR_ENV_VAR, resolve_data_root
from ._dir_permissions import verify_config_directory_permissions
from ._runtime_dir import daemon_dir, resolve_runtime_dir
