"""Resolved runtime context shared by every ``dhcli`` subcommand.

Configuration is loaded **once per invocation, just before the leaf
command's body runs**. Any malformed file under the configuration
directory fails fast with :class:`CliError(CONFIG_INVALID)` before
any subcommand body executes — there is no two-phase split, no
"command runs against partially-broken config" mode. The user fixes
the offending file (the error message names it) and retries.

The load is deferred, not eager-at-the-root, so click's own eager
``--help`` and ``--agents`` callbacks (which fire while the leaf's
arguments are being parsed, *before* the leaf body is invoked) never
touch configuration at all. The root callback in
:mod:`deephaven_mcp.cli._main` stores a cheap :class:`RuntimeSpec`
(the load recipe: directory overrides + CLI flag overrides) on
``ctx.obj``;
:meth:`~deephaven_mcp.cli._help.HelpfulCommand.invoke` swaps it for a
fully-loaded :class:`Runtime` via :meth:`RuntimeSpec.resolve` right
before the body runs. Commands declared ``needs_runtime=False`` (the
``agents`` verbs) skip the swap and never load configuration.

The single :class:`Runtime` aggregates the resolved paths, the
validated :class:`ConfigTree`, and the daemon-directory handle.
:func:`load_runtime` is the only loader entry point: every subcommand
body takes a :class:`Runtime` injected by ``click.pass_obj``.

Read sites:

- ``runtime.config.cli`` — the effective :class:`CliConfig`
  (``cli.json`` value with top-level CLI flag overrides applied).
- ``runtime.config.server`` / ``.community`` / ``.enterprise`` —
  the systems-server sections, each ``None`` when the matching
  file or subtree is absent.
- ``runtime.config_dir`` / ``.runtime_dir`` — resolved paths.
- ``runtime.daemon_dir`` — typed handle to ``runtime_dir/daemon/``.

Recovery from a broken configuration tree happens by editing the
file the error message points at; ``--help``, the ``--agents``
flag, and ``dhcli agents`` continue to work without config
because they exit before any command body — and therefore before
the load — runs.
"""

from __future__ import annotations

__all__ = ["Runtime", "RuntimeSpec", "apply_cli_overrides", "load_runtime"]

import logging
from dataclasses import dataclass
from pathlib import Path

from deephaven_mcp._dictutil import deep_merge
from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.config import (
    harden_private_dir,
    resolve_config_dir,
    resolve_runtime_dir,
)
from deephaven_mcp.config.schema import CliConfig
from deephaven_mcp.config.tree import ConfigTree, ConfigTreeLoader
from deephaven_mcp.daemon_registry import DaemonDirectory

_LOGGER = logging.getLogger(__name__)


def apply_cli_overrides(cli: CliConfig, cli_overrides: dict[str, object]) -> CliConfig:
    """Return ``cli`` with per-invocation flag overrides merged in.

    ``cli_overrides`` is a nested partial mapping of raw field values
    (e.g. ``{"request": {"timeouts": {"default_seconds": 5}}}``). It is
    deep-merged into ``cli``'s current values and the result is
    re-validated, so sibling fields the overrides do not touch keep
    their existing values.

    Args:
        cli (CliConfig): The loaded ``cli.json`` value.
        cli_overrides (dict[str, object]): Nested partial mapping of raw
            field values whose leaves win over ``cli``'s values.

    Returns:
        CliConfig: A new validated model with the overrides applied.

    Raises:
        pydantic.ValidationError: When the merged mapping fails
            :class:`CliConfig` validation.
    """
    merged = deep_merge(cli.model_dump(), cli_overrides)
    return CliConfig.model_validate(merged)


@dataclass(frozen=True, slots=True)
class RuntimeSpec:
    """Deferred recipe for building a :class:`Runtime`.

    The root callback constructs one per invocation — cheaply, with no
    I/O and no validation — and stores it on ``ctx.obj``.
    :meth:`~deephaven_mcp.cli._help.HelpfulCommand.invoke` calls
    :meth:`resolve` to swap it for the real :class:`Runtime` right
    before a leaf command's body runs. Fields mirror
    :func:`load_runtime`'s keyword parameters.
    """

    config_dir_override: Path | None = None
    """Explicit ``--config-dir`` value, or ``None`` for the default."""

    runtime_dir_override: Path | None = None
    """Explicit ``--runtime-dir`` value, or ``None`` for the default."""

    cli_overrides: dict[str, object] | None = None
    """Nested partial ``cli.json`` overrides from top-level CLI flags
    (``-o``, ``--timeout``, ``--no-auto-start``), or ``None``."""

    no_input: bool = False
    """The root ``--no-input`` flag: when ``True``, commands never
    prompt interactively. Read by the ``needs_runtime=False``
    configuration-authoring verbs (which receive the spec itself);
    not part of the loaded :class:`Runtime`."""

    def resolve(self) -> Runtime:
        """Load, validate, and return the :class:`Runtime` this spec describes.

        Runs :func:`load_runtime` to completion on a fresh event loop
        via :func:`~deephaven_mcp.cli._async.run_async` (the CLI's
        async-to-sync seam); safe here because the caller (click's
        command dispatch) is synchronous and no loop is running. The
        returned :class:`Runtime` holds only paths, validated models,
        and the daemon-directory handle — no loop-bound state — so the
        command body's own event loop (``@run_async``) can use it
        freely.

        Returns:
            Runtime: The frozen, fully-validated runtime context.

        Raises:
            CliError: With :attr:`ErrorCode.CONFIG_INVALID` when the
                permission audit fails or any configuration file is
                malformed.
        """
        return run_async(load_runtime)(
            config_dir_override=self.config_dir_override,
            runtime_dir_override=self.runtime_dir_override,
            cli_overrides=self.cli_overrides,
        )


@dataclass(frozen=True, slots=True)
class Runtime:
    """Resolved runtime context for a single ``dhcli`` invocation."""

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

    Total: every section under the configuration directory is parsed
    and validated in one pass. Any malformed file raises
    :class:`CliError(CONFIG_INVALID)`. There is no recovery mode —
    paths that must stay usable without a valid configuration
    (``--help``, ``--agents``, and the ``agents`` verbs) never reach
    this function because they exit before the leaf-body dispatch
    that triggers the load (see :class:`RuntimeSpec`).

    The runtime directory is created if absent and tightened to mode
    ``0o700``; this happens before the configuration audit so a
    fresh install with no ``config_dir`` still gets a usable
    ``runtime_dir``. The configuration directory's permission audit
    runs inside :class:`ConfigTreeLoader.initialize`.

    CLI flag overrides supplied via ``cli_overrides`` are applied to
    the loaded ``config.cli`` after validation. The dict is a nested
    partial mapping of raw field values (e.g.
    ``{"request": {"timeouts": {"default_seconds": 5}}}``) deep-merged
    into the loaded value and re-validated, so sibling fields the
    flags do not touch keep their on-disk values.

    Args:
        config_dir_override (Path | None): Optional explicit config
            directory. ``None`` falls back to
            ``$DH_AI_DATA_DIR/config`` (or the platform default
            user-data root's ``config`` subdirectory).
        runtime_dir_override (Path | None): Optional explicit
            runtime directory. ``None`` falls back to
            ``$DH_AI_DATA_DIR/runtime`` (or the platform default
            user-data root's ``runtime`` subdirectory).
        cli_overrides (dict[str, object] | None): Optional nested
            partial mapping of raw field values deep-merged into
            ``config.cli`` after loading. ``None`` (or an empty dict)
            leaves the on-disk value unchanged.

    Returns:
        Runtime: The frozen, fully-validated runtime context.

    A zero-system tree loads successfully here: the zero-system
    invariant is enforced only where a system is required (the CLI
    daemon-acquisition path and systems-server startup), so
    system-independent verbs (``docs``, ``daemon stop``, and the
    ``config`` authoring/inspection verbs) work on an empty tree.

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
            update={"cli": apply_cli_overrides(tree.cli, cli_overrides)}
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
