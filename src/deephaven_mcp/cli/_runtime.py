"""Resolved runtime context shared by every ``dh-mcp`` subcommand.

Configuration is loaded **once, eagerly, on every invocation**. Any
malformed file under the configuration directory fails fast with
:class:`CliError(CONFIG_INVALID)` — there is no two-phase split, no
"command runs against partially-broken config" mode. The user fixes
the offending file (the error message names it) and retries.

The single :class:`Runtime` aggregates the resolved paths, the
validated :class:`ConfigTree`, and the daemon-directory handle.
:func:`load_runtime` is the only entry point: every subcommand
either takes a :class:`Runtime` injected by ``click.pass_obj`` or
the ``--help`` / ``introspect`` paths short-circuit before the load
runs.

Read sites:

- ``runtime.config.cli`` — the effective :class:`CliConfig`
  (``cli.json`` value with top-level CLI flag overrides applied).
- ``runtime.config.server`` / ``.community`` / ``.enterprise`` —
  the systems-server sections, each ``None`` when the matching
  file or subtree is absent.
- ``runtime.config_dir`` / ``.runtime_dir`` — resolved paths.
- ``runtime.daemon_dir`` — typed handle to ``runtime_dir/daemon/``.

Recovery from a broken configuration tree happens by editing the
file the error message points at; ``--help`` and
``dh-mcp introspect`` continue to work without config because
:mod:`deephaven_mcp.cli._main` skips the load on those paths.
"""

from __future__ import annotations

__all__ = ["Runtime", "load_runtime"]

import logging
from dataclasses import dataclass
from pathlib import Path

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.config import (
    harden_private_dir,
    resolve_config_dir,
    resolve_runtime_dir,
)
from deephaven_mcp.config.tree import ConfigTree, ConfigTreeLoader
from deephaven_mcp.daemon_registry import DaemonDirectory

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Runtime:
    """Resolved runtime context for a single ``dh-mcp`` invocation."""

    config_dir: Path
    """The resolved configuration directory."""

    runtime_dir: Path
    """The resolved, mode-0o700 runtime directory. Always created."""

    config: ConfigTree
    """Validated parse of the entire configuration directory tree.

    All four sections (``server``, ``cli``, ``community``,
    ``enterprise``) are either populated or explicitly ``None`` per
    their schema. ``config.cli`` is always populated and reflects
    the on-disk ``cli.json`` value with top-level CLI flag overrides
    (``-o``, ``--timeout``, ``--no-auto-start``) applied by
    :func:`load_runtime`."""

    daemon_dir: DaemonDirectory
    """Typed handle to ``runtime_dir/daemon/`` exposing the registry
    / lock / log artifact paths and atomic registry CRUD."""


async def load_runtime(
    *,
    config_dir_override: Path | None = None,
    runtime_dir_override: Path | None = None,
    cli_overrides: dict[str, object] | None = None,
) -> Runtime:
    """Resolve paths, validate the entire config tree, return a :class:`Runtime`.

    Eager and total: every section under the configuration directory
    is parsed and validated up front. Any malformed file raises
    :class:`CliError(CONFIG_INVALID)`. There is no recovery mode —
    callers that want the CLI usable without a valid configuration
    must short-circuit *before* calling this function (the
    ``--help`` and ``introspect`` paths in :mod:`deephaven_mcp.cli._main`
    are the canonical examples).

    The runtime directory is created if absent and tightened to mode
    ``0o700``; this happens before the configuration audit so a
    fresh install with no ``config_dir`` still gets a usable
    ``runtime_dir``. The configuration directory's permission audit
    runs inside :class:`ConfigTreeLoader.initialize`.

    CLI flag overrides supplied via ``cli_overrides`` are applied to
    the loaded ``config.cli`` after validation. Each key in the dict
    must be a sub-section name on :class:`CliConfig` whose value is
    a :class:`CliConfig` sub-model produced by
    :mod:`deephaven_mcp.cli._main` from the parsed flag values.

    Args:
        config_dir_override (Path | None): Optional explicit config
            directory. ``None`` falls back to
            ``$DH_MCP_DATA_DIR/config`` (or the platform default
            user-data root's ``config`` subdirectory).
        runtime_dir_override (Path | None): Optional explicit
            runtime directory. ``None`` falls back to
            ``$DH_MCP_DATA_DIR/runtime`` (or the platform default
            user-data root's ``runtime`` subdirectory).
        cli_overrides (dict[str, object] | None): Optional sub-model
            replacements applied to ``config.cli`` after loading.
            ``None`` (or an empty dict) leaves the on-disk value
            unchanged.

    Returns:
        Runtime: The frozen, fully-validated runtime context.

    Raises:
        CliError: With :attr:`ErrorCode.CONFIG_INVALID` when the
            permission audit fails or any configuration file is
            malformed.
    """
    config_dir = resolve_config_dir(config_dir_override)
    runtime_dir = resolve_runtime_dir(runtime_dir_override)
    harden_private_dir(runtime_dir)

    try:
        tree = await ConfigTreeLoader(config_dir=config_dir).initialize()
    except ConfigurationError as exc:
        raise CliError(str(exc), code=ErrorCode.CONFIG_INVALID) from exc

    if cli_overrides:
        tree = tree.model_copy(
            update={"cli": tree.cli.model_copy(update=cli_overrides)}
        )

    _LOGGER.debug(
        f"[_runtime:load_runtime] Runtime loaded "
        f"config_dir={config_dir} runtime_dir={runtime_dir}"
    )
    return Runtime(
        config_dir=config_dir,
        runtime_dir=runtime_dir,
        config=tree,
        daemon_dir=DaemonDirectory.for_runtime_dir(runtime_dir),
    )
