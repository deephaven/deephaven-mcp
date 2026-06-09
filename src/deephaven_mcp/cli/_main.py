"""Entry point for the ``dh-mcp`` CLI.

This module assembles the click command tree, resolves the
:class:`Runtime` once, and dispatches to the noun groups defined in
:mod:`deephaven_mcp.cli._commands`. It is the only module wired to
``[project.scripts]`` in ``pyproject.toml``.

Exit codes:

- ``0`` — success.
- ``2`` — user-facing failure (CLI argument error, daemon down with
  ``--no-auto-start``, MCP request failed, configuration invalid,
  etc.).
- ``3`` — the invoked MCP tool returned ``isError=True``.
"""

from __future__ import annotations

__all__ = ["cli", "main"]

import logging
import sys
from collections.abc import Iterable
from pathlib import Path

import click

from deephaven_mcp._logging import setup_logging
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands.config import config as config_group
from deephaven_mcp.cli._commands.daemon import daemon as daemon_group
from deephaven_mcp.cli._commands.introspect import introspect as introspect_command
from deephaven_mcp.cli._commands.tool import tool as tool_group
from deephaven_mcp.cli._errors import CliError, ErrorCode, render_error
from deephaven_mcp.cli._format import OUTPUT_MODES, OutputMode
from deephaven_mcp.cli._help import build_help
from deephaven_mcp.cli._runtime import load_runtime
from deephaven_mcp.config.schema import CliConfig

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_cli_overrides(
    *,
    template: CliConfig,
    output: OutputMode | None,
    timeout: int | None,
    no_auto_start: bool,
) -> dict[str, object]:
    """Build a ``cli_overrides`` dict for :func:`load_runtime`.

    Each top-level CLI flag maps to a fresh sub-model on the
    matching :class:`CliConfig` section. ``template`` is used only
    to obtain default sub-model instances when no override is
    supplied for that section; passing :class:`CliConfig` directly
    is the canonical caller.
    """
    overrides: dict[str, object] = {}
    if output is not None:
        overrides["output"] = template.output.model_copy(update={"format": output})
    if timeout is not None:
        overrides["request"] = template.request.model_copy(
            update={
                "timeouts": template.request.timeouts.model_copy(
                    update={"default_seconds": timeout}
                )
            }
        )
    if no_auto_start:
        overrides["daemon"] = template.daemon.model_copy(update={"auto_start": False})
    return overrides


def _is_help_invocation() -> bool:
    """Return True when ``--help`` or ``-h`` is a real option in ``sys.argv``.

    Used to short-circuit runtime loading: rendering ``--help`` for a
    subcommand should not require a valid configuration tree.

    Tokens consumed as the value of a value-taking root option
    (``dh-mcp --config-dir --help`` treats ``--help`` as a path) are
    skipped so the help fast-path only fires for genuine help requests.
    """
    value_taking = _value_taking_root_options()
    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in {"--help", "-h"}:
            return True
        if "=" not in tok and tok in value_taking and i + 1 < len(argv):
            i += 2
            continue
        i += 1
    return False


def _verbosity_to_level(verbose: int, quiet: bool) -> int:
    """Map ``-v`` count and ``-q`` flag to a :mod:`logging` level."""
    if quiet:
        return logging.ERROR
    if verbose >= 2:
        return logging.DEBUG
    if verbose >= 1:
        return logging.INFO
    return logging.WARNING


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------


@click.group(
    name="dh-mcp",
    help=build_help(
        summary=(
            "Local CLI for the Deephaven MCP systems server: manage a "
            "per-user daemon and call MCP tools."
        ),
        description=(
            "dh-mcp manages a per-user background daemon that hosts the "
            "Deephaven MCP systems server, then connects to it to inspect "
            "and invoke tools. Getting started: run 'dh-mcp tool list' to "
            "see the available tools (the daemon auto-starts on first use), "
            "then 'dh-mcp tool call NAME' to invoke one. Use the 'daemon' "
            "group to manage the daemon lifecycle (start, stop, status, "
            "restart, reset, logs), 'tool' to list, show, and call MCP "
            "tools, and 'config' to inspect and validate configuration. "
            "Pass --no-auto-start to require an already-running daemon "
            "instead of spawning one. AI agents should run 'dh-mcp "
            "introspect' for a machine-readable manifest of every command, "
            "option, and error code rather than scraping --help."
        ),
        examples=(
            "$ dh-mcp tool list",
            "$ dh-mcp tool call sessions_list --arg type=community",
            "$ dh-mcp daemon status",
            "$ dh-mcp config validate",
            "$ dh-mcp introspect | jq '.commands | keys'",
        ),
    ),
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--config-dir",
    "config_dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Configuration directory. When unset, defaults to the "
        "'config' subdirectory under $DH_MCP_DATA_DIR (or the "
        "platform default user-data root). Must be a directory the "
        "current user owns at mode 0o700."
    ),
)
@click.option(
    "--runtime-dir",
    "runtime_dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Runtime directory for the daemon registry. When unset, "
        "defaults to the 'runtime' subdirectory under "
        "$DH_MCP_DATA_DIR (or the platform default user-data root)."
    ),
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(OUTPUT_MODES),
    envvar="DH_MCP_OUTPUT",
    default=None,
    help=(
        "Output format. Takes precedence over the DH_MCP_OUTPUT "
        "environment variable and the 'output.format' setting in cli.json."
    ),
)
@click.option(
    "--timeout",
    type=int,
    default=None,
    help=(
        "Per-request timeout in seconds. Overrides the "
        "'request.timeouts.default_seconds' setting in cli.json."
    ),
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help=(
        "Increase logging verbosity (-v=INFO, -vv=DEBUG). "
        "Mutually exclusive with -q/--quiet."
    ),
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    default=False,
    help=(
        "Suppress non-error output (root logger at ERROR). "
        "Mutually exclusive with -v/--verbose."
    ),
)
@click.option(
    "--no-auto-start",
    is_flag=True,
    default=False,
    help="If the daemon is not running, fail rather than auto-spawning it.",
)
@click.version_option(
    package_name="deephaven-mcp",
    prog_name="dh-mcp",
    message="%(prog)s %(version)s",
)
@click.pass_context
@run_async
async def cli(
    ctx: click.Context,
    config_dir: Path | None,
    runtime_dir: Path | None,
    output: OutputMode | None,
    timeout: int | None,
    verbose: int,
    quiet: bool,
    no_auto_start: bool,
) -> None:
    """Local CLI entry point — see ``dh-mcp --help`` for the command tree."""
    if verbose and quiet:
        raise CliError(
            "-v/--verbose and -q/--quiet are mutually exclusive.",
            code=ErrorCode.ARG_PARSE_ERROR,
        )
    setup_logging()
    logging.getLogger().setLevel(_verbosity_to_level(verbose, quiet))

    # ``introspect`` walks the command tree and does not need a
    # validated configuration tree; constructing the runtime would
    # force-load config/server JSON files that may not exist when
    # an agent is just learning the surface. The same reasoning
    # applies when the operator is asking for ``--help`` at any
    # depth (``dh-mcp daemon --help``, ``dh-mcp tool call --help``).
    if ctx.invoked_subcommand == "introspect" or _is_help_invocation():
        ctx.obj = None
        return

    overrides = _build_cli_overrides(
        template=CliConfig(),
        output=output,
        timeout=timeout,
        no_auto_start=no_auto_start,
    )
    ctx.obj = await load_runtime(
        config_dir_override=config_dir,
        runtime_dir_override=runtime_dir,
        cli_overrides=overrides or None,
    )


# Register noun groups + meta commands on the root.
cli.add_command(daemon_group)
cli.add_command(tool_group)
cli.add_command(config_group)
cli.add_command(introspect_command)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _output_from_argv(argv: list[str]) -> OutputMode:
    """Recover the active ``-o/--output`` mode from a raw argv list.

    Used by the top-level fallback renderer in :func:`main` after
    ``cli.main`` returned and the live click context is gone. All
    four argv shapes click accepts are recognized:

    - ``-o human`` (short, separate value)
    - ``--output human`` (long, separate value)
    - ``-o=human`` (short, ``=`` form)
    - ``--output=human`` (long, ``=`` form)

    Falls back to ``"human"`` when no recognized override is found
    or the supplied value is not a member of :data:`OUTPUT_MODES`.
    """
    it = iter(argv)
    for token in it:
        candidate: str | None = None
        if token in {"-o", "--output"}:
            candidate = next(it, None)
        elif token.startswith("-o="):
            candidate = token[len("-o=") :]
        elif token.startswith("--output="):
            candidate = token[len("--output=") :]
        if candidate is None:
            continue
        if candidate in OUTPUT_MODES:
            # mypy narrows ``candidate`` to ``OutputMode`` here via
            # the runtime membership check against the
            # ``Literal``-derived tuple — no cast required.
            return candidate
        # Recognized flag but unknown value — fall through to the
        # safe default rather than honoring something the parser
        # would have rejected.
        return "human"
    return "human"


def main(argv: list[str] | None = None) -> None:
    """Console entry point for ``dh-mcp``.

    Args:
        argv (list[str] | None): Optional argument list (used by
            tests). ``None`` defers to :data:`sys.argv` ``[1:]``.
    """
    try:
        cli.main(args=argv, prog_name="dh-mcp", standalone_mode=False)
    except CliError as exc:
        # ``standalone_mode=False`` lets us intercept ``CliError``
        # globally and render it according to the active output
        # mode. The live click context is gone by the time we get
        # here, so we approximate the command path from argv.
        argv_used = argv if argv is not None else sys.argv[1:]
        render_error(
            exc,
            output=_output_from_argv(argv_used),
            command=_argv_command_path(argv_used),
        )
        sys.exit(exc.exit_code)
    except click.exceptions.UsageError as exc:
        # Click prints its own message to stderr; honor its exit code.
        exc.show()
        sys.exit(exc.exit_code)
    except click.exceptions.Abort:  # pragma: no cover - Ctrl-C
        click.echo("Aborted.", err=True)
        sys.exit(130)
    except click.exceptions.Exit as exc:  # pragma: no cover - click internal flow
        sys.exit(exc.exit_code)
    except SystemExit:  # pragma: no cover - re-raise click's own sys.exit
        raise
    except Exception as exc:  # noqa: BLE001 - top-level safety net
        # Unexpected failure: wrap as a CliError so the operator and
        # any agent caller see a structured payload before we exit.
        argv_used = argv if argv is not None else sys.argv[1:]
        render_error(
            CliError(f"Unexpected error: {exc}", code=ErrorCode.INTERNAL_ERROR),
            output=_output_from_argv(argv_used),
            command=_argv_command_path(argv_used),
        )
        sys.exit(2)
    sys.exit(0)


def _value_taking_options(params: Iterable[click.Parameter]) -> frozenset[str]:
    """Spellings of every value-taking ``--flag value`` option in ``params``.

    Positional :class:`click.Argument` instances are excluded because
    they don't carry ``--`` spellings; flag (``is_flag=True``) and
    counter (``count=True``) options are excluded because they
    consume no value. The remaining options contribute their primary
    and secondary option strings.

    Factored to take an iterable so the helper is unit-testable
    against synthetic groups containing positional ``Argument``
    instances — a shape the real ``cli`` group does not (yet) have
    but which the helper must handle correctly to remain forward
    compatible.
    """
    spellings: set[str] = set()
    for param in params:
        if not isinstance(param, click.Option):
            # Positional ``click.Argument``: no ``--`` spellings to
            # contribute; skip without raising.
            continue
        if param.is_flag or param.count:
            continue
        spellings.update(param.opts)
        spellings.update(param.secondary_opts)
    return frozenset(spellings)


def _value_taking_root_options() -> frozenset[str]:
    """Return :func:`_value_taking_options` applied to ``cli.params``.

    Driven from ``cli.params`` so adding or removing a top-level
    option does not desynchronize callers that need the value-taking
    set (``_argv_command_path`` and ``_is_help_invocation``).
    """
    return _value_taking_options(cli.params)


def _argv_command_path(argv: list[str]) -> str:
    """Approximate the dotted command path from a raw argv list.

    Used only in the top-level fallback renderer; the in-context
    renderer derives the path from the live click context.
    """
    value_taking = _value_taking_root_options()
    non_options: list[str] = []
    skip_next = False
    for tok in argv:
        if skip_next:
            skip_next = False
            continue
        if tok.startswith("-"):
            # ``--opt=value`` carries its own value; only the bare
            # ``--opt value`` form requires a one-token lookahead.
            if "=" not in tok and tok in value_taking:
                skip_next = True
            continue
        non_options.append(tok)
    return " ".join(non_options[:2]) or "dh-mcp"


if __name__ == "__main__":  # pragma: no cover
    main()
