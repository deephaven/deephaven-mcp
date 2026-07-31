"""General-purpose configuration primitives for Deephaven MCP servers.

This package owns the reusable plumbing that any MCP server in the
project can use to locate, audit, and load a JSON5-on-disk
configuration tree.

User-data-root resolution
-------------------------

Every directory the MCP server family reads from or writes to lives
under a single user-data root (``~/.deephaven/ai`` on POSIX;
``%APPDATA%/Deephaven/ai`` on Windows). One environment variable —
:data:`DATA_DIR_ENV_VAR` (``DH_AI_DATA_DIR``) — overrides that root,
and per-subdir resolvers compose paths under whatever root that
function returns:

- :func:`resolve_data_root` — env override or platform default; the
  *only* function that consults the env var.
- :func:`resolve_config_dir` — read-only configuration tree consumed
  by :class:`~deephaven_mcp.config.tree.ConfigTreeLoader`.
- :func:`resolve_runtime_dir` — mutable per-user state owned by the
  running daemon (registry, lock, log).
- :func:`daemon_dir` — the ``daemon/`` subdirectory under the
  runtime root; deterministic and platform-independent given a
  resolved runtime directory.
- :func:`instances_dir` — the ``instances/`` subdirectory under the
  runtime root, holding the instance tracker's per-process metadata
  files; deterministic and platform-independent given a resolved
  runtime directory.

Per-subdir env vars (``DH_AI_CONFIG_DIR``, ``DH_AI_RUNTIME_DIR``)
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

Authoring tier
--------------

The write side of the package, consumed by the ``dhcli config``
authoring commands (``cli/_commands/config.py``). Like the loading
primitives above, these modules are intentionally project-private —
importable across the project but not part of the package's exported
surface:

- :mod:`deephaven_mcp.config._field_path` —
  :class:`~deephaven_mcp.config._field_path.FieldPath`, the dotted
  logical-path type every other authoring module speaks.
- :mod:`deephaven_mcp.config._file_kinds` — registry binding each of
  the six configuration file kinds to its path prefix and schema.
- :mod:`deephaven_mcp.config._logical_paths` — resolves a logical
  path to a file (plus field within it) or a section scoping several.
- :mod:`deephaven_mcp.config._fields` — get/set/unset of a field
  within one file's raw wire-format dict.
- :mod:`deephaven_mcp.config._settable_fields` — schema-derived
  inventory of settable paths (``dhcli config keys``).
- :mod:`deephaven_mcp.config._store` — validated, atomic file I/O for
  one configuration directory.

Schema tier
-----------

The Pydantic section schemas (``ServerConfig``, ``CliConfig``,
``CommunityConfig``, ``EnterpriseConfig`` and their nested models)
live in :mod:`deephaven_mcp.config.schema`, and the aggregator that
composes them into a validated tree
(:class:`~deephaven_mcp.config.tree.ConfigTree`,
:class:`~deephaven_mcp.config.tree.ConfigTreeLoader`) lives in
:mod:`deephaven_mcp.config.tree`. These are deliberately *not*
re-exported here: importing :mod:`deephaven_mcp.config` for the path
and audit primitives stays cheap and never pulls in the schema graph.
Per-session and per-system declaration types live in
:mod:`deephaven_mcp.sessions`.
"""

__all__ = [
    "DATA_DIR_ENV_VAR",
    "ConfigurationError",
    "daemon_dir",
    "harden_private_dir",
    "instances_dir",
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
from ._runtime_dir import daemon_dir, instances_dir, resolve_runtime_dir
