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
import os
import sys
from collections.abc import Iterable
from pathlib import Path

import click

from deephaven_mcp._logging import setup_logging
from deephaven_mcp.cli._async import run_async
from deephaven_mcp.cli._commands.catalog import catalog as catalog_group
from deephaven_mcp.cli._commands.config import config as config_group
from deephaven_mcp.cli._commands.daemon import daemon as daemon_group
from deephaven_mcp.cli._commands.docs import docs as docs_group
from deephaven_mcp.cli._commands.introspect import introspect as introspect_group
from deephaven_mcp.cli._commands.pq import pq as pq_group
from deephaven_mcp.cli._commands.session import session as session_group
from deephaven_mcp.cli._commands.system import system as system_group
from deephaven_mcp.cli._commands.table import table as table_group
from deephaven_mcp.cli._commands.tool import tool as tool_group
from deephaven_mcp.cli._errors import CliError, ErrorCode, render_error
from deephaven_mcp.cli._format import (
    DEFAULT_OUTPUT_MODE,
    OUTPUT_ENV_VAR,
    OUTPUT_MODES,
    OutputMode,
)
from deephaven_mcp.cli._help import HelpfulGroup, build_help
from deephaven_mcp.cli._runtime import load_runtime

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_cli_overrides(
    *,
    output: OutputMode | None,
    timeout: int | None,
    no_auto_start: bool,
) -> dict[str, object]:
    """Build a ``cli_overrides`` dict for :func:`load_runtime`.

    Each top-level CLI flag maps to a nested partial dict of raw
    field values, deep-merged into the loaded ``cli.json`` value by
    :func:`load_runtime` so untouched sibling fields (e.g. a
    configured ``docs.url`` or ``daemon.reuse`` policy) survive the
    override. ``--timeout`` covers both the daemon request timeout
    and the docs request timeout: it means "this invocation's
    per-request timeout", whichever server the verb talks to.
    """
    overrides: dict[str, object] = {}
    if output is not None:
        overrides["output"] = {"format": output}
    if timeout is not None:
        overrides["request"] = {"timeouts": {"default_seconds": timeout}}
        overrides["docs"] = {"timeouts": {"request_seconds": timeout}}
    if no_auto_start:
        overrides["daemon"] = {"auto_start": False}
    return overrides


def _argv_has_option(targets: frozenset[str]) -> bool:
    """Return True when any spelling in ``targets`` is a real option in argv.

    Scans ``sys.argv[1:]`` for a bare occurrence of any target spelling.
    Tokens consumed as the value of a value-taking root option
    (``dh-mcp --config-dir --help`` treats ``--help`` as a path) are
    skipped so the match only fires for a genuine option, not a value
    that happens to share the spelling.

    Args:
        targets (frozenset[str]): Option spellings to look for
            (e.g. ``{"--help", "-h"}``).

    Returns:
        bool: True when a target spelling appears as a standalone token.
    """
    value_taking = _value_taking_root_options()
    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in targets:
            return True
        if "=" not in tok and tok in value_taking and i + 1 < len(argv):
            i += 2
            continue
        i += 1
    return False


def _is_help_invocation() -> bool:
    """Return True when ``--help`` or ``-h`` is a real option in ``sys.argv``.

    Used to short-circuit runtime loading: rendering ``--help`` for a
    subcommand should not require a valid configuration tree.
    """
    return _argv_has_option(frozenset({"--help", "-h"}))


def _is_introspect_invocation() -> bool:
    """Return True when ``--introspect`` is a real option in ``sys.argv``.

    The universal ``--introspect`` flag is the machine-readable twin of
    ``--help`` and likewise renders without a valid configuration tree,
    so it short-circuits runtime loading wherever it appears.
    """
    return _argv_has_option(frozenset({"--introspect"}))


def _verbosity_to_level(verbose: int, quiet: bool) -> int:
    """Map ``-v`` count and ``-q`` flag to a :mod:`logging` level."""
    if quiet:
        return logging.ERROR
    if verbose >= 2:
        return logging.DEBUG
    if verbose >= 1:
        return logging.INFO
    return logging.WARNING


_NOISY_DEPENDENCY_LOGGERS: tuple[str, ...] = ("mcp", "httpx", "anyio")
"""Third-party logger names whose routine WARNING records are quieted by default."""


def _quiet_dependency_loggers(verbose: int) -> None:
    """Pin noisy dependency loggers to ERROR unless the user raised verbosity.

    Without ``-v``/``-vv`` (including under ``-q``), each logger named in
    ``_NOISY_DEPENDENCY_LOGGERS`` is set to ``ERROR`` so library internals a
    CLI user cannot act on — such as the mcp client's "Session termination
    failed" notice on teardown — never reach the terminal. With ``-v``/``-vv``
    the loggers are reset to ``NOTSET`` so they follow the root level the
    operator chose.
    """
    level = logging.NOTSET if verbose else logging.ERROR
    for name in _NOISY_DEPENDENCY_LOGGERS:
        logging.getLogger(name).setLevel(level)


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------


@click.group(
    name="dh-mcp",
    cls=HelpfulGroup,
    help=build_help(
        summary=(
            "Local CLI for the Deephaven MCP systems server: manage a "
            "per-user daemon and call MCP tools."
        ),
        description=(
            "dh-mcp manages a per-user background daemon that hosts the "
            "Deephaven MCP systems server, then connects to it to inspect "
            "and invoke tools. Getting started: run 'dh-mcp session list' to "
            "see the available sessions (the daemon auto-starts on first "
            "use), then use a verb like 'dh-mcp table data' to read a table "
            "or 'dh-mcp session exec' to run a script. "
            "Use the 'daemon' group to manage the daemon lifecycle (start, "
            "stop, status, restart, repair, logs); the 'session', 'system', "
            "'table', 'catalog', and 'pq' groups to inspect and "
            "operate sessions and Enterprise resources with first-class "
            "flags; 'tool' to list, show, and call any MCP tool directly; "
            "'docs' to ask the Deephaven documentation assistant a "
            "question; and 'config' to inspect and validate configuration. "
            "Pass --no-auto-start to require an already-running daemon "
            "instead of spawning one. AI agents should run 'dh-mcp "
            "introspect tree' for a machine-readable manifest of every "
            "command, option, and error code rather than scraping --help, "
            "or append --introspect to any command for just its node."
        ),
        examples=(
            "$ dh-mcp tool list",
            "$ dh-mcp tool call sessions_list --arg type=community",
            "$ dh-mcp daemon status",
            "$ dh-mcp config validate",
            "$ dh-mcp introspect tree | jq '.commands | keys'",
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
    envvar=OUTPUT_ENV_VAR,
    default=None,
    # Eager so it is resolved before the eager ``--introspect`` callback,
    # which reads the root output mode (e.g. ``dh-mcp -o json --introspect``).
    is_eager=True,
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
        "'request.timeouts.default_seconds' setting in cli.json (and "
        "'docs.timeouts.request_seconds' for the 'docs' commands)."
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
    _quiet_dependency_loggers(verbose)

    # ``introspect`` walks the command tree and does not need a
    # validated configuration tree; constructing the runtime would
    # force-load config/server JSON files that may not exist when
    # an agent is just learning the surface. The same reasoning
    # applies to the ``--introspect`` flag (its machine twin) at any
    # depth, and to ``--help`` (``dh-mcp daemon --help``, ``dh-mcp tool
    # call --help``).
    if (
        ctx.invoked_subcommand == "introspect"
        or _is_help_invocation()
        or _is_introspect_invocation()
    ):
        ctx.obj = None
        return

    overrides = _build_cli_overrides(
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
cli.add_command(session_group)
cli.add_command(system_group)
cli.add_command(table_group)
cli.add_command(catalog_group)
cli.add_command(pq_group)
cli.add_command(docs_group)
cli.add_command(config_group)
cli.add_command(introspect_group)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _env_output_mode() -> OutputMode | None:
    """Return ``DH_MCP_OUTPUT`` if it names a valid output mode, else ``None``."""
    candidate = os.environ.get(OUTPUT_ENV_VAR)
    if candidate in OUTPUT_MODES:
        # mypy narrows ``candidate`` to ``OutputMode`` via the runtime
        # membership check against the ``Literal``-derived tuple.
        return candidate
    return None


def _output_from_argv(argv: list[str]) -> OutputMode:
    """Recover the active output mode for the fallback error renderer.

    Used by :func:`main` after ``cli.main`` returned and the live click
    context is gone, so the resolution is reconstructed from the raw
    argv plus the environment. Precedence mirrors the live path: an
    explicit ``-o/--output`` flag wins, then ``DH_MCP_OUTPUT``, then
    :data:`DEFAULT_OUTPUT_MODE`. All four argv shapes click accepts are
    recognized:

    - ``-o human`` (short, separate value)
    - ``--output human`` (long, separate value)
    - ``-o=human`` (short, ``=`` form)
    - ``--output=human`` (long, ``=`` form)

    ``cli.json``'s ``output.format`` is not consulted — the config may
    be the very thing that failed to load.
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
        # Recognized flag but unknown value — click rejected the
        # invocation; fall through to env / default rather than honor
        # a value the parser would not accept.
        break
    return _env_output_mode() or DEFAULT_OUTPUT_MODE


def main(argv: list[str] | None = None) -> None:
    """Console entry point for ``dh-mcp``.

    Args:
        argv (list[str] | None): Optional argument list (used by
            tests). ``None`` defers to :data:`sys.argv` ``[1:]``.

    Before delegating to :meth:`click.Group.main`, every recognized
    root-level option in ``argv`` (including occurrences that appear
    *after* the subcommand) is lifted to the front via
    :func:`_lift_root_options`, so ``dh-mcp config show -o json`` is
    accepted identically to ``dh-mcp -o json config show``. See that
    function for the precise contract.
    """
    # Normalize argv exactly once: every downstream reader (cli.main,
    # the error renderer's output-mode probe, the error renderer's
    # command-path probe) sees the same lifted view. The lifter
    # handles the ``None`` fallback to ``sys.argv[1:]`` itself.
    argv_lifted = _lift_root_options(argv)
    try:
        cli.main(args=argv_lifted, prog_name="dh-mcp", standalone_mode=False)
    except CliError as exc:
        # ``standalone_mode=False`` lets us intercept ``CliError``
        # globally and render it according to the active output
        # mode. The live click context is gone by the time we get
        # here, so we approximate the command path from argv.
        render_error(
            exc,
            output=_output_from_argv(argv_lifted),
            command=_argv_command_path(argv_lifted),
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
        render_error(
            CliError(f"Unexpected error: {exc}", code=ErrorCode.INTERNAL_ERROR),
            output=_output_from_argv(argv_lifted),
            command=_argv_command_path(argv_lifted),
        )
        sys.exit(2)
    sys.exit(0)


def _liftable_options(
    params: Iterable[click.Parameter],
) -> tuple[frozenset[str], frozenset[str]]:
    """Bucket ``params`` into value-taking and value-less liftable spellings.

    Args:
        params (Iterable[click.Parameter]): The Click parameters to
            classify. Typically ``cli.params``; the iterable shape
            makes the helper unit-testable against synthetic groups.

    Returns:
        tuple[frozenset[str], frozenset[str]]: A pair
            ``(value_taking, value_less)``. ``value_taking`` holds
            option spellings (long and short) that consume a value via
            a following token; ``value_less`` holds spellings of
            boolean (``is_flag=True``) and counted (``count=True``)
            options that consume no following value. Positional
            :class:`click.Argument` instances are silently skipped.

    ``--help`` / ``-h`` and ``--version`` are intentionally excluded:
    Click resolves these per-command, so ``dh-mcp daemon --help`` is
    expected to render the *daemon* group's help, not the root's;
    lifting them would change that semantics.
    """
    value_taking: set[str] = set()
    value_less: set[str] = set()
    for param in params:
        if not isinstance(param, click.Option):
            # Positional ``click.Argument``: no ``--`` spellings to
            # contribute; skip without raising.
            continue
        spellings = set(param.opts) | set(param.secondary_opts)
        # ``--help`` is wired via ``context_settings``; ``--version``
        # is an eager :class:`click.Option` added by ``version_option``.
        if "--help" in spellings or "-h" in spellings:
            continue
        if "--version" in spellings:
            continue
        if param.is_flag or param.count:
            value_less.update(spellings)
        else:
            value_taking.update(spellings)
    return frozenset(value_taking), frozenset(value_less)


def _liftable_root_options() -> tuple[frozenset[str], frozenset[str]]:
    """Return :func:`_liftable_options` applied to ``cli.params``.

    Driven from ``cli.params`` so adding or removing a top-level
    option does not desynchronize :func:`_lift_root_options`.
    """
    return _liftable_options(cli.params)


def _lift_root_options(argv: list[str] | None = None) -> list[str]:
    """Return ``argv`` with every recognized root option moved to the front.

    Click requires group-level options to precede the subcommand, so
    ``dh-mcp config show --output human`` would otherwise fail with
    ``No such option '--output'``. This rewrite makes option position
    immaterial without subclassing :class:`click.Group` or duplicating
    options onto every subcommand.

    Args:
        argv (list[str] | None): The argument list to rewrite. ``None``
            (the default) falls back to ``sys.argv[1:]``, matching the
            convention of :meth:`click.Group.main`: ``sys.argv[0]`` is
            the program name supplied by the OS and is never part of
            the argument list a parser sees. Tests pass an explicit
            list.

    Returns:
        list[str]: A new list with every lifted root option (and its
            value, for value-taking options) moved to the front,
            followed by the remaining tokens in their original order.

    Algorithm:

    - Single left-to-right pass.
    - Tokens after a literal ``--`` are never touched (POSIX
      end-of-options sentinel).
    - A token matching a value-taking root option in its bare form
      (``--output``, ``-o``) is lifted together with the next token,
      which is its value. If the value is missing (option is the last
      argv element), the option is lifted alone and Click then raises
      its own usage error, identical to today.
    - A token matching a value-taking root option in ``=`` form
      (``--output=human``, ``-o=human``) is lifted as a single token.
      Note: click only accepts the long ``--opt=value`` form; for
      short options it parses ``-oVALUE`` with no separator, so
      ``-o=human`` fails validation at click regardless of position.
      The lifter still recognizes the shape for symmetry; rejection
      happens at click, same as it would without lifting.
    - The attached short-value form (``-ohuman``, which click *does*
      accept as ``-o human``) is **not** lifted: the lexical pass has
      no per-option arity table to know ``-o`` consumes the rest of the
      token, and the trailing chars are not all value-less bundle
      spellings. So ``dh-mcp -ohuman config show`` works but
      ``dh-mcp config show -ohuman`` does not — use the spaced
      (``-o human``) or long ``=`` (``--output=human``) form after the
      subcommand.
    - A token matching a value-less root option (boolean ``is_flag``
      or counter ``count``) is lifted as a single token. Counter
      options preserve repetition (``-v -v -v`` and ``-vvv`` are both
      handled — short-flag bundling is not expanded; Click's own
      parser collapses ``-vvv`` once the token is at the front).

    Lifted tokens preserve their relative order, so
    ``dh-mcp config show -o json --timeout 5`` becomes
    ``dh-mcp -o json --timeout 5 config show``, not the reverse.

    Limitation: the lift is purely lexical — it has no grammar for
    subcommand options, so a token that *equals* a root spelling is
    hoisted even when it is the value of a subcommand option or a
    positional (e.g. ``--jvm-arg --timeout`` would steal ``--timeout``
    for the root). The colliding strings (``-o``, ``--timeout``,
    ``--config-dir``, …) are implausible as real subcommand values, so
    this rarely bites; when it must, guard the value with the POSIX
    ``--`` sentinel — every token after ``--`` is preserved verbatim.
    """
    if argv is None:
        argv = sys.argv[1:]
    value_taking, value_less = _liftable_root_options()
    # Short-flag bundle support: Click accepts ``-vvv`` as three
    # ``-v`` for a counter, and ``-vq`` as ``-v -q`` for two value-less
    # flags. The single chars usable inside a bundle are exactly the
    # value-less short spellings.
    bundle_chars = {opt[1] for opt in value_less if len(opt) == 2 and opt[0] == "-"}
    lifted: list[str] = []
    remaining: list[str] = []
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "--":
            # POSIX end-of-options sentinel: stop lifting; preserve
            # the sentinel and every following token verbatim.
            remaining.extend(argv[i:])
            break
        # ``=`` form: the whole token is one option carrying its value.
        if "=" in tok and tok.startswith("-"):
            prefix = tok.split("=", 1)[0]
            if prefix in value_taking:
                lifted.append(tok)
                i += 1
                continue
        # Bare value-taking form: lift this token *and* the next.
        if tok in value_taking:
            lifted.append(tok)
            if i + 1 < len(argv):
                lifted.append(argv[i + 1])
                i += 2
            else:
                # Missing value — let Click surface its own usage error.
                i += 1
            continue
        # Value-less form (flag or counter).
        if tok in value_less:
            lifted.append(tok)
            i += 1
            continue
        # Short-flag bundle (e.g. ``-vvv``, ``-vq``): a single ``-``
        # followed by characters that are *all* value-less short
        # spellings. Conservative — if any char is not liftable, the
        # token is left in place for Click to handle (or error on).
        if (
            len(tok) > 2
            and tok.startswith("-")
            and not tok.startswith("--")
            and "=" not in tok
            and all(ch in bundle_chars for ch in tok[1:])
        ):
            lifted.append(tok)
            i += 1
            continue
        remaining.append(tok)
        i += 1
    return lifted + remaining


def _value_taking_root_options() -> frozenset[str]:
    """Spellings of every value-taking root option (``--flag value``).

    The value-taking half of :func:`_liftable_root_options`, single-sourced
    through that bucketer so the classification of ``cli.params`` lives in one
    place. Callers that only need to know which root options consume a
    following token — ``_argv_command_path`` and ``_is_help_invocation`` —
    read this.
    """
    return _liftable_root_options()[0]


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
