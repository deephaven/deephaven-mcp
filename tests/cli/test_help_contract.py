"""Contract tests for dhcli help content across the live command tree.

Enforces the ref-cli-help-standards contract on every registered leaf
command, parametrized over the live click tree so a newly-added
command is covered automatically.
"""

from __future__ import annotations

import re

import click
import pytest

from deephaven_mcp.cli._command import HelpfulCommand, HelpfulGroup
from deephaven_mcp.cli._context import (
    TARGET_SELECTION_GUIDANCE,
    TARGET_SELECTION_HINT,
)
from deephaven_mcp.cli._errors import ErrorCode
from deephaven_mcp.cli._help import OutputSpec
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._params import NonBlankPath

# Project-wide convention enforcement over the live click tree, not a mirror
# of one source file (``ref-python-coding-practices`` rule 5).
pytestmark = pytest.mark.guardrail


def _leaf_commands(
    group: click.Group, prefix: tuple[str, ...] = ()
) -> list[tuple[str, click.Command]]:
    """Return ``(path, command)`` for every leaf command under ``group``."""
    leaves: list[tuple[str, click.Command]] = []
    for name, cmd in group.commands.items():
        path = (*prefix, name)
        if isinstance(cmd, click.Group):
            leaves.extend(_leaf_commands(cmd, path))
        else:
            leaves.append((" ".join(path), cmd))
    return leaves


def _all_commands(
    group: click.Group, prefix: tuple[str, ...] = ()
) -> list[tuple[str, click.Command]]:
    """Return ``(path, command)`` for ``group`` and every command beneath it.

    Unlike :func:`_leaf_commands`, this includes the groups (the root
    ``dhcli`` group and each noun group), so checks that apply to every
    command's surfaced strings -- plain-text and option-help presence --
    also cover group-level options such as the global ``-o``/``--timeout``/
    ``-v``/``-q`` flags on the root.
    """
    path = " ".join(prefix) or "dhcli"
    commands: list[tuple[str, click.Command]] = [(path, group)]
    for name, cmd in group.commands.items():
        sub = (*prefix, name)
        if isinstance(cmd, click.Group):
            commands.extend(_all_commands(cmd, sub))
        else:
            commands.append((" ".join(sub), cmd))
    return commands


_LEAVES = _leaf_commands(cli)
_LEAF_IDS = [path for path, _ in _LEAVES]
_ALL = _all_commands(cli)
_ALL_IDS = [path for path, _ in _ALL]


def _see_also(cmd: click.Command) -> tuple[str, ...]:
    """Return the ``See also`` entries a command's ``HelpSpec`` declares.

    Narrows on the concrete command class for the same reason as
    :func:`_declared_error_codes`: a plain ``click.Command`` in this tree
    would be a bug worth surfacing.
    """
    if not isinstance(cmd, HelpfulCommand | HelpfulGroup):
        return ()
    spec = cmd.help_spec
    return () if spec is None or spec.see_also is None else tuple(spec.see_also)


def _declared_error_codes(cmd: click.Command) -> tuple[ErrorCode, ...]:
    """Return the error codes a command's ``HelpSpec`` declares.

    Narrows on the concrete command class rather than probing for the
    attribute: every command in the tree is a ``HelpfulCommand`` or
    ``HelpfulGroup``, and a plain ``click.Command`` appearing here would
    be a bug worth surfacing rather than silently treating as
    code-less.
    """
    if not isinstance(cmd, HelpfulCommand | HelpfulGroup):
        return ()
    spec = cmd.help_spec
    # Both are legitimately absent: a group carries no spec, and a spec's
    # error_codes is None when the command documents no Error codes section.
    return () if spec is None or spec.error_codes is None else tuple(spec.error_codes)


_CONTEXT_LEAVES = [
    (path, cmd)
    for path, cmd in _LEAVES
    if ErrorCode.CONTEXT_NOT_SET in _declared_error_codes(cmd)
]
_CONTEXT_IDS = [path for path, _ in _CONTEXT_LEAVES]

# Pure discovery commands have no command-specific failure mode (they
# cannot raise a CliError), so they document no Error codes section.
# Every operational command must. ``agents command`` is excluded
# because it can raise COMMAND_NOT_FOUND for an unresolvable path.
# ``self completion`` only prints a click-generated script; its sole
# failure mode (bad SHELL) is a click argument-parse error.
# ``context show`` takes no arguments and only reads a file: like every
# other runtime verb it can surface CONFIG_INVALID from the shared
# leaf-boundary load, but that code is universally emittable and so is
# declared by no command (see ``config validate`` for the explicit one).
_NO_ERROR_CODES = {
    "agents tree",
    "agents errors",
    "self completion",
    "config files",
    "config session list",
    "config system list",
    "context show",
}


@pytest.mark.parametrize("path,cmd", _LEAVES, ids=_LEAF_IDS)
def test_leaf_help_has_required_sections(path: str, cmd: click.Command) -> None:
    """Every leaf command's help carries the required contract sections."""
    help_text = cmd.help or ""
    assert help_text, f"{path}: no help text"
    for section in ("Output:", "Examples:", "See also:", "Exit codes:"):
        assert section in help_text, f"{path}: missing {section!r}"
    if path not in _NO_ERROR_CODES:
        assert "Error codes:" in help_text, f"{path}: missing 'Error codes:'"


@pytest.mark.parametrize("path,cmd", _LEAVES, ids=_LEAF_IDS)
def test_leaf_help_documents_positional_arguments(
    path: str, cmd: click.Command
) -> None:
    """A command with a positional argument documents it in an Arguments block."""
    has_argument = any(isinstance(p, click.Argument) for p in cmd.params)
    if has_argument:
        assert "Arguments:" in (cmd.help or ""), f"{path}: undocumented positional"


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_help_is_plain_text(path: str, cmd: click.Command) -> None:
    """Help text carries no backtick markup (literal or RST role).

    Surfaced help is rendered verbatim by click in the terminal, and
    the agents manifest emits the same HelpSpec strings structurally;
    neither interprets markup. Rejecting any backtick catches both
    double-backtick literals and single-backtick RST roles
    (``:func:`x```). Single quotes are the emphasis convention.
    """
    assert "`" not in (cmd.help or ""), f"{path}: backtick markup in help"


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_options_help_is_plain_text(path: str, cmd: click.Command) -> None:
    """Every option's help string carries no backtick markup."""
    for param in cmd.params:
        help_ = getattr(param, "help", None)
        if help_:
            assert "`" not in help_, f"{path}: backtick markup in --{param.name} help"


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_options_have_help(path: str, cmd: click.Command) -> None:
    """Every option carries a non-empty help string (the Options contract)."""
    for param in cmd.params:
        if isinstance(param, click.Option):
            assert param.help, f"{path}: option --{param.name} has no help"


@pytest.mark.parametrize("path,cmd", _CONTEXT_LEAVES, ids=_CONTEXT_IDS)
def test_context_defaultable_command_names_its_discovery_command(
    path: str, cmd: click.Command
) -> None:
    """A command that can take its target from the context says how to see it.

    The sticky context is the one input a command's own argv does not
    reveal, so naming the concept without naming ``dhcli context show``
    leaves a reader -- especially an agent, which cannot inspect the
    terminal -- with a term it cannot act on.
    """
    help_text = cmd.help or ""
    assert "dhcli context show" in help_text, (
        f"{path}: help mentions the sticky context but never names "
        "'dhcli context show'"
    )
    assert any(
        entry.startswith("dhcli context") for entry in _see_also(cmd)
    ), f"{path}: no 'dhcli context' entry in See also"


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_path_options_reject_a_blank_value(path: str, cmd: click.Command) -> None:
    """Every path-valued parameter must use ``NonBlankPath``.

    ``click.Path`` converts ``''`` to ``Path('.')``, so a blank silently
    becomes the current directory: ``--config-dir ''`` read a different
    configuration tree and reported success, and ``--runtime-dir ''``
    pointed the daemon registry at a relative path so ``daemon status``
    reported no daemon while one ran. The post-parse blank guard cannot
    catch it, because ``Path('')`` and ``Path('.')`` are indistinguishable
    and rejecting both would outlaw an explicit ``--config-dir .``.

    ``click.Path`` is the only click type with this flaw -- every other
    (``INT``, ``FLOAT``, ``Choice``, ``IntRange``, ``DateTime``, ``UUID``,
    ``File``) rejects a blank outright, and a bare ``STRING`` keeps it
    blank for ``cli._params.reject_blank_values`` to catch. If a
    custom ``ParamType`` that converts a blank into a meaningful value is
    ever added, extend this guard to cover it.
    """
    offenders = [
        param.name
        for param in cmd.params
        if isinstance(param.type, click.Path)
        and not isinstance(param.type, NonBlankPath)
    ]
    assert not offenders, (
        f"{path}: {offenders} use a bare click.Path, which turns a blank "
        f"into Path('.') silently; use NonBlankPath from cli._params instead"
    )


def _output_spec(cmd: click.Command) -> OutputSpec | None:
    """Return the command's declared output spec, if it carries one."""
    if not isinstance(cmd, HelpfulCommand | HelpfulGroup):
        return None
    return cmd.output_spec


def _examples(cmd: click.Command) -> tuple[str, ...]:
    """Return the ``Examples`` entries a command's ``HelpSpec`` declares.

    Narrows on the concrete command class for the same reason as
    :func:`_see_also`.
    """
    if not isinstance(cmd, HelpfulCommand | HelpfulGroup):
        return ()
    spec = cmd.help_spec
    return () if spec is None or spec.examples is None else tuple(spec.examples)


_STRUCTURED_LEAVES = [
    (path, cmd)
    for path, cmd in _LEAVES
    if (spec := _output_spec(cmd)) is not None and spec.mode in {"object", "list"}
]
_STRUCTURED_IDS = [path for path, _ in _STRUCTURED_LEAVES]


@pytest.mark.parametrize("path,cmd", _STRUCTURED_LEAVES, ids=_STRUCTURED_IDS)
def test_structured_command_has_an_agent_example(path: str, cmd: click.Command) -> None:
    """A command with structured output shows how to consume it programmatically.

    The standard asks for a human example and an agent example. A human
    reads the plain invocation; an agent needs one that pulls a field out
    of the payload, which is also the cheapest proof that the documented
    field names are the real ones. Text-mode commands are exempt -- there
    is no structure to select from.
    """
    assert any(
        "jq" in example or "-o json" in example for example in _examples(cmd)
    ), f"{path}: no agent example (pipe through jq, or show -o json)"


_JQ_CALL = re.compile(
    r"\|\s*jq\s+(?:-\w+\s+)*(?:'(?P<quoted>[^']*)'|(?P<bare>[^\s|]+))"
)
"""One ``| jq`` invocation in an example, filter either quoted or bare."""

_JQ_LEADING_FIELD = re.compile(r"^\.(?P<name>[A-Za-z_][A-Za-z0-9_]*)")
"""A jq filter that opens by selecting one named field off the payload root."""


def _jq_selected_root_fields(example: str) -> list[str]:
    """Return the root field names an example's ``jq`` filters select.

    Each comma-separated term is examined, so the two-field form
    (``jq '.row_count, .is_complete'``) is checked rather than waved
    through. Within a term, only one that *opens* with a ``.field``
    selection is reported -- covering ``.field``, ``.field.nested`` and
    ``.field // "default"``.

    Deliberately narrow elsewhere: a term that pipes into another filter
    or starts with a builtin (``keys``, ``length``, ``.[]``) yields
    nothing, because only the leading root field is checkable against an
    ``OutputSpec``, and a check that reports false positives gets deleted
    by whoever it blocks. Nested keys past the first are not verifiable
    either way -- an ``OutputSpec`` describes a field's subfields in
    prose, not structurally.
    """
    names: list[str] = []
    for match in _JQ_CALL.finditer(example):
        expr = match.group("quoted") or match.group("bare") or ""
        for term in expr.split(","):
            term = term.strip()
            if "|" in term:
                continue
            if leading := _JQ_LEADING_FIELD.match(term):
                names.append(leading.group("name"))
    return names


_OBJECT_LEAVES = [
    (path, cmd)
    for path, cmd in _LEAVES
    if (spec := _output_spec(cmd)) is not None and spec.mode == "object"
]
_OBJECT_IDS = [path for path, _ in _OBJECT_LEAVES]


@pytest.mark.parametrize("path,cmd", _OBJECT_LEAVES, ids=_OBJECT_IDS)
def test_jq_examples_select_declared_output_fields(
    path: str, cmd: click.Command
) -> None:
    """An example's ``jq`` filter names a field the command actually emits.

    An example is the one part of the help a reader will paste verbatim,
    so a filter naming a field the payload does not carry prints ``null``
    and teaches the wrong schema. Pinning the examples to the same
    ``OutputSpec`` the ``Output:`` section renders keeps the two from
    drifting apart. ``list``- and ``text``-mode commands are excluded:
    their payload root is not an object, so a root field name is not the
    right thing to look for.
    """
    spec = _output_spec(cmd)
    assert spec is not None  # guaranteed by _OBJECT_LEAVES
    declared = {field.name for field in spec.fields}
    for example in _examples(cmd):
        for selected in _jq_selected_root_fields(example):
            assert selected in declared, (
                f"{path}: example selects .{selected}, which its OutputSpec "
                f"does not declare (declared: {sorted(declared)})"
            )


@pytest.mark.parametrize("path,cmd", _STRUCTURED_LEAVES, ids=_STRUCTURED_IDS)
def test_structured_output_names_its_fields(path: str, cmd: click.Command) -> None:
    """Structured output is described field by field, not just in prose.

    An agent parses the payload; a note saying "the tool's result
    envelope" tells it nothing it can index by. ``object`` mode must name
    at least one field. ``list`` mode may name none -- an array of bare
    strings has no fields -- but then owes a note saying what the
    elements are.
    """
    spec = _output_spec(cmd)
    assert spec is not None
    if spec.mode == "object":
        assert spec.fields, f"{path}: object output declares no fields"
    elif not spec.fields:
        assert spec.note, (
            f"{path}: list output declares neither fields nor a note, so the "
            "element shape is undocumented"
        )


def _wraps_a_tool(cmd: click.Command) -> bool:
    """Whether the command fronts at least one MCP tool."""
    if not isinstance(cmd, HelpfulCommand | HelpfulGroup):
        return False
    return bool(cmd.wraps_tool or cmd.wraps_tools)


_CREDENTIAL_DISCLOSING_PATHS = frozenset(
    {"session credentials", "session url", "session open"}
)
"""Verbs whose payload embeds a live auth token for the named session.

Not structurally detectable -- they take no ``--yes`` (nothing is
destroyed) yet aiming one at another user's session hands out that
session's credentials, so they belong to the target-sensitive set.
"""

_STATE_LEAVING_PATHS = frozenset({"pq start"})
"""Verbs that leave state on a shared system without confirming first.

Also not structurally detectable: starting someone else's PQ disrupts a
shared system, but nothing is destroyed, so there is no ``--yes`` to key
off. Kept honest by
:func:`test_every_hint_carrying_leaf_is_target_sensitive` rather than by
an enumeration in prose -- add to this set when that test names a new
verb.
"""

_TARGET_SENSITIVE_LEAVES = [
    (path, cmd)
    for path, cmd in _LEAVES
    if _wraps_a_tool(cmd)
    and (
        any(
            isinstance(param, click.Option) and param.name == "yes"
            for param in cmd.params
        )
        or path in _CREDENTIAL_DISCLOSING_PATHS
        or path in _STATE_LEAVING_PATHS
    )
]
_TARGET_SENSITIVE_IDS = [path for path, _ in _TARGET_SENSITIVE_LEAVES]


@pytest.mark.parametrize("path,cmd", _LEAVES, ids=_LEAF_IDS)
def test_every_hint_carrying_leaf_is_target_sensitive(
    path: str, cmd: click.Command
) -> None:
    """A verb warning about target choice is covered by the target checks.

    Carrying ``TARGET_SELECTION_HINT`` is the author's own statement that
    aiming this verb at the wrong resource matters. The target-sensitive
    set is assembled from ``--yes`` plus two hand-maintained sets, so
    without this check a verb that is neither destructive nor
    credential-disclosing -- ``pq start`` was exactly that -- carries the
    hint while escaping every test that verifies the hint is there.
    """
    if TARGET_SELECTION_HINT not in (cmd.help or ""):
        return
    assert path in _TARGET_SENSITIVE_IDS, (
        f"{path} carries the target-selection hint but is not in the "
        "target-sensitive set; add it to _STATE_LEAVING_PATHS (or "
        "_CREDENTIAL_DISCLOSING_PATHS) so the hint is enforced"
    )


_LISTING_PATHS = ("session list", "pq list")
"""The verbs whose output is the candidate set the rule is about."""


@pytest.mark.parametrize(
    "path,cmd", _TARGET_SENSITIVE_LEAVES, ids=_TARGET_SENSITIVE_IDS
)
def test_target_sensitive_command_states_the_target_selection_rule(
    path: str, cmd: click.Command
) -> None:
    """A verb whose target matters says which id it is legitimate to act on.

    Two arms, both requiring a tool binding (a live resource on a shared
    system, not a local file): ``--yes`` marks the verbs that execute or
    destroy, and ``_CREDENTIAL_DISCLOSING_PATHS`` the ones that hand out
    a token. For those, the sticky-context hazard is only half the risk;
    the other half is enumerating with a list verb and acting on
    whatever came back, which is how an agent reaches an unrelated or
    production resource with an id it typed deliberately.

    The verb carries the one-line ``TARGET_SELECTION_HINT``; the full
    ``TARGET_SELECTION_GUIDANCE`` paragraph is stated tree-wide instead
    (see ``test_listing_command_states_the_full_target_selection_rule``),
    because repeating it per verb duplicated ~1.9 KB of agent context.
    Both are single-sourced in ``cli/_context.py`` so the wording cannot
    drift.

    The local config verbs are out of scope: ``config system remove``
    also confirms, but it edits a file in the caller's own
    configuration tree, where there is no cross-user listing to pick a
    victim from.
    """
    assert TARGET_SELECTION_HINT in (cmd.help or ""), (
        f"{path}: target-sensitive verb does not carry TARGET_SELECTION_HINT; "
        "an agent is told to check the context but not which id it may act on"
    )


@pytest.mark.parametrize("path", _LISTING_PATHS)
def test_listing_command_states_the_full_target_selection_rule(path: str) -> None:
    """The listings state the rule in full, since they create the hazard.

    An agent reads the paragraph with the candidate ids in hand, which
    is the moment it decides what to act on -- so this is the one place
    the reasoning is worth its bytes, alongside the agents-manifest
    conventions block.
    """
    cmd = dict(_LEAVES)[path]
    assert TARGET_SELECTION_GUIDANCE in (cmd.help or ""), (
        f"{path}: listing verb does not carry TARGET_SELECTION_GUIDANCE; the "
        "candidate set is disclosed without the rule for choosing from it"
    )


@pytest.mark.parametrize("path,cmd", _CONTEXT_LEAVES, ids=_CONTEXT_IDS)
def test_confirmable_command_declares_operation_canceled(
    path: str, cmd: click.Command
) -> None:
    """``--yes`` and ``operation_canceled`` travel together.

    A verb that can decline to act must document the code it exits with,
    and a verb documenting that code must offer the flag that skips the
    question.
    """
    has_yes = any(
        isinstance(param, click.Option) and param.name == "yes" for param in cmd.params
    )
    declares = ErrorCode.OPERATION_CANCELED in _declared_error_codes(cmd)
    assert has_yes == declares, (
        f"{path}: --yes present={has_yes} but operation_canceled "
        f"declared={declares}; they must agree"
    )


# ---------------------------------------------------------------------------
# Help text points at commands that exist
# ---------------------------------------------------------------------------


def _command_tokens(reference: str) -> tuple[str, ...]:
    """Reduce a ``dhcli ...`` reference to the command tokens it names.

    Drops the program name, any flags, and any metavar or argument
    placeholder. Metavars are uppercase by convention throughout the
    help surface (``ID``, ``SYSTEM``, ``PQ_NAME``), which is what makes
    them separable from verbs without a lookup.
    """
    tokens = reference.split()
    start = 1 if tokens and tokens[0] == "dhcli" else 0
    return tuple(
        token
        for token in tokens[start:]
        if not token.startswith("-") and not token.isupper()
    )


def _resolved_depth(tokens: tuple[str, ...]) -> int:
    """Return how many leading ``tokens`` resolve as a command path."""
    node: click.Command = cli
    depth = 0
    for token in tokens:
        if not isinstance(node, click.Group) or token not in node.commands:
            return depth
        node = node.commands[token]
        depth += 1
    return depth


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_see_also_entries_name_real_commands(path: str, cmd: click.Command) -> None:
    """Every ``see_also`` target must resolve in the live tree.

    ``see_also`` is a navigation surface: an agent follows it to find the
    next command, so an entry left behind by a rename sends it to a
    ``command_not_found``. Resolution is exact here rather than
    best-effort -- accepting the longest prefix that happens to resolve
    would pass a typo like 'session craete' by stopping at 'session'.
    """
    for entry in _see_also(cmd):
        tokens = _command_tokens(entry)
        if not tokens:
            continue  # names the root itself, e.g. 'dhcli --agents'
        assert _resolved_depth(tokens) == len(tokens), (
            f"{path}: see_also entry {entry!r} does not resolve "
            f"(stopped after {_resolved_depth(tokens)} of {len(tokens)} tokens)"
        )


@pytest.mark.parametrize("path,cmd", _ALL, ids=_ALL_IDS)
def test_help_prose_names_real_commands(path: str, cmd: click.Command) -> None:
    """A ``'dhcli ...'`` pointer in prose must name a real command.

    Descriptions and option help route the reader to a discovery or
    remedy command ("run 'dhcli session list'"), and that pointer is the
    reader's next action -- a stale one strands an agent mid-task. Only
    the leading noun and verb are required to resolve, because prose
    legitimately shows an argument value too ('dhcli session create
    dev'); that is still enough to catch a renamed or misspelled verb.
    """
    spec = getattr(cmd, "help_spec", None)
    if spec is None:
        return
    haystack = [spec.summary or "", spec.description or ""]
    haystack += [entry.help for entry in spec.arguments or ()]
    haystack += [
        str(param.help) for param in cmd.params if getattr(param, "help", None)
    ]
    for text in haystack:
        for quoted in re.findall(r"'dhcli ([a-z][a-z0-9 _-]*)'", text):
            tokens = _command_tokens(f"dhcli {quoted}")
            if not tokens:
                continue
            required = min(2, len(tokens))
            assert _resolved_depth(tokens) >= required, (
                f"{path}: help text names 'dhcli {quoted}', which does not "
                f"resolve past {_resolved_depth(tokens)} token(s)"
            )


_LEAF_BY_PATH = dict(_LEAVES)


def _options_by_name(cmd: click.Command) -> dict[str, click.Option]:
    """Return the command's click options keyed by parameter name."""
    return {
        param.name: param
        for param in cmd.params
        if isinstance(param, click.Option) and param.name is not None
    }


def _option_machinery(option: click.Option) -> tuple[object, ...]:
    """Return how an option parses a value, excluding per-verb semantics.

    Included: everything that decides how a supplied value is parsed and
    forwarded to the tool. Two verbs disagreeing on any of it means one
    of them sends the wrong shape.

    Excluded, as legitimately per-verb: ``help`` (the whole reason these
    options are declared twice) and ``required``. ``--heap-size-gb`` is
    required to create a PQ -- the controller needs a heap to size the
    worker -- and optional to modify one, where omitting it keeps the
    stored value. That is the same "omission means different things"
    axis the help text expresses, not drift.
    """
    return (
        option.type.name,
        option.multiple,
        option.metavar,
        option.is_flag,
        option.nargs,
    )


_PQ_SHARED_OPTIONS = sorted(
    set(_options_by_name(_LEAF_BY_PATH["pq create"]))
    & set(_options_by_name(_LEAF_BY_PATH["pq modify"]))
)


@pytest.mark.parametrize("name", _PQ_SHARED_OPTIONS)
def test_pq_create_and_modify_agree_on_option_machinery(name: str) -> None:
    """An option on both ``pq`` verbs parses identically on each.

    Nine options are declared separately on ``pq create`` and ``pq
    modify`` because omitting one means different things to each verb
    (the controller picks a default versus the stored value is kept), so
    one shared help string cannot be accurate for both. That duplication
    buys accurate help at the cost of two declarations that can drift in
    *type* as well as wording -- and a drop of ``type=int`` on one verb
    would silently start forwarding a string.

    ``tests/cli/test_tool_wrapper_drift.py`` cannot catch it: that guard
    joins wrapper flags to tool parameters by name only and checks
    neither type nor cardinality (see its module docstring). The set is
    derived from the live tree rather than listed, so an option moved
    into or out of ``_create_modify_options`` is covered automatically.
    """
    create = _options_by_name(_LEAF_BY_PATH["pq create"])[name]
    modify = _options_by_name(_LEAF_BY_PATH["pq modify"])[name]
    assert _option_machinery(create) == _option_machinery(modify), (
        f"--{name.replace('_', '-')} parses differently on 'pq create' than on "
        f"'pq modify' (create={_option_machinery(create)}, "
        f"modify={_option_machinery(modify)}); only the help text may differ"
    )
