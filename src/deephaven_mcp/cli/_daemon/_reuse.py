"""Daemon-reuse policy engine for the ``dh-mcp`` CLI.

Decides whether a live daemon can be reused when its recorded build identity
differs from the CLI's own. The comparison is field by field — package
version, virtualenv (``sys.prefix``), and source fingerprint — and each
differing field maps to a configured action via
:class:`~deephaven_mcp.config.schema.DaemonReusePolicy`. When several fields
differ the *most severe* action wins, ordered
``ignore < warn < restart < refuse``.

This module is pure and side-effect free: :func:`decide_reuse` returns a
:class:`ReuseDecision` and :func:`describe_difference` formats a
human-readable summary. Applying the resolved action — emitting the warning,
restarting the daemon, or raising — and all other I/O is the caller's
responsibility (:mod:`deephaven_mcp.cli._daemon._lifecycle`).
"""

from __future__ import annotations

__all__ = [
    "ReuseDecision",
    "decide_reuse",
    "describe_difference",
]

from dataclasses import dataclass

from deephaven_mcp.config.schema import DaemonReuseAction, DaemonReusePolicy
from deephaven_mcp.daemon_registry import DaemonBuildIdentity

_FINGERPRINT_ABBREV_LEN = 12
"""Characters of a SHA-256 source fingerprint shown in difference messages —
enough to disambiguate two builds while keeping the message readable."""


@dataclass(frozen=True, slots=True)
class ReuseDecision:
    """The resolved decision for one daemon-reuse attempt."""

    action: DaemonReuseAction
    """The action to apply. :attr:`DaemonReuseAction.IGNORE` when nothing
    differs (or every differing field is configured ``ignore``)."""

    differing: tuple[str, ...]
    """The identity fields that differed (``version``/``venv``/``fingerprint``);
    empty when the identities match."""


def decide_reuse(
    expected: DaemonBuildIdentity,
    recorded: DaemonBuildIdentity,
    policy: DaemonReusePolicy,
    *,
    can_spawn: bool,
) -> ReuseDecision:
    """Resolve the action to take for a live daemon whose build may differ.

    Compares ``recorded`` against ``expected`` field by field, collecting the
    configured action for each field that differs, then returns the most
    severe of those actions.

    Args:
        expected (DaemonBuildIdentity): The CLI's own identity.
        recorded (DaemonBuildIdentity): The daemon's recorded identity.
        policy (DaemonReusePolicy): The per-field action policy.
        can_spawn (bool): Whether the caller may spawn a replacement daemon.
            When ``False``, a resolved :attr:`DaemonReuseAction.RESTART`
            degrades to :attr:`DaemonReuseAction.REFUSE`, since a restart
            implies a spawn.

    Returns:
        ReuseDecision: The most-severe action across the differing fields, plus
            the differing fields themselves. :attr:`DaemonReuseAction.IGNORE`
            with an empty ``differing`` when the identities match.
    """
    differing: list[str] = []
    actions: list[DaemonReuseAction] = []
    if expected.version != recorded.version:
        differing.append("version")
        actions.append(policy.version)
    if expected.venv != recorded.venv:
        differing.append("venv")
        actions.append(policy.venv)
    if expected.fingerprint != recorded.fingerprint:
        differing.append("fingerprint")
        actions.append(policy.fingerprint)
    if not differing:
        return ReuseDecision(action=DaemonReuseAction.IGNORE, differing=())
    action = max(actions, key=lambda a: a.severity)
    if action is DaemonReuseAction.RESTART and not can_spawn:
        action = DaemonReuseAction.REFUSE
    return ReuseDecision(action=action, differing=tuple(differing))


def describe_difference(
    expected: DaemonBuildIdentity, recorded: DaemonBuildIdentity
) -> str:
    """Return a human-readable summary of how the daemon differs from the CLI.

    Recomputes the per-field comparison so callers need not thread the
    differing-field set through; source fingerprints (SHA-256 hex digests) are
    truncated to :data:`_FINGERPRINT_ABBREV_LEN` characters so the message
    stays readable.

    Args:
        expected (DaemonBuildIdentity): The CLI's own identity.
        recorded (DaemonBuildIdentity): The daemon's recorded identity.

    Returns:
        str: A ``"; "``-joined description naming each differing field with its
            daemon and CLI values.
    """
    parts: list[str] = []
    if expected.version != recorded.version:
        parts.append(f"version (daemon={recorded.version}, cli={expected.version})")
    if expected.venv != recorded.venv:
        parts.append(f"venv (daemon={recorded.venv}, cli={expected.venv})")
    if expected.fingerprint != recorded.fingerprint:
        parts.append(
            f"source fingerprint "
            f"(daemon={recorded.fingerprint[:_FINGERPRINT_ABBREV_LEN]}, "
            f"cli={expected.fingerprint[:_FINGERPRINT_ABBREV_LEN]})"
        )
    return "; ".join(parts)
