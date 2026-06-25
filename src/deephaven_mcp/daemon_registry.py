"""Shared wire contract between the ``dh-mcp`` CLI and a local daemon.

The ``dh-mcp-systems-server`` binary can be launched in *daemon
mode* (``--daemon``) to back the ``dh-mcp`` CLI's per-user local
daemon. The daemon and the CLI live in different top-level
packages (``mcp_systems_server`` and ``cli``) but cooperate via a
small set of artefacts on disk:

- ``daemon.json`` — a JSON file the daemon writes at startup,
  describing the bound loopback host/port, the auto-generated
  pre-shared key (PSK), its own PID + create-time-ns + configured
  server name, and the directory it was configured from. The CLI
  reads this file to discover and connect to the daemon.
- ``daemon.lock`` — an advisory file lock that serializes every
  read-then-mutate sequence on ``daemon.json``. The daemon
  acquires it around publish / unpublish; the CLI acquires it
  around spawn-then-poll, stale-cleanup, stop, and reset. Pure
  reads remain lock-free (``os.replace`` is atomic at the
  directory-entry level) so liveness probes and ``daemon
  status`` happy paths do not contend.
- ``daemon.log`` — the captured stdout/stderr of the daemon
  process. Surfaced by ``dh-mcp daemon logs``.

This module owns the contract *both* sides agree on:

- :class:`DaemonRegistryEntry` — Pydantic model for the
  ``daemon.json`` schema. Inherits from
  :class:`~deephaven_mcp._pydantic.RedactableSchema` so the PSK
  (typed :class:`pydantic.SecretStr`) is masked by default in
  ``repr`` and ``model_dump``, and emitted as
  :data:`~deephaven_mcp._redaction.REDACTED` under
  ``context={"redact": True}``.
- :class:`DaemonDirectory` — typed handle to the daemon's working
  directory exposing the registry / lock / log artifact paths,
  the lock-free :meth:`~DaemonDirectory.read_entry`, and
  :meth:`~DaemonDirectory.locked` (the only door to mutation).
- :class:`LockedRegistry` — the lock-holding session returned by
  :meth:`DaemonDirectory.locked`; owns atomic write / delete /
  quarantine of ``daemon.json`` plus the spawn-in-progress marker.
  Writes the file with mode ``0o600`` on POSIX; directory
  hardening (``0o700``) is performed by
  :func:`deephaven_mcp.config.harden_private_dir` at startup.
- :class:`RegistryCorruptError` — raised by
  :meth:`DaemonDirectory.read_entry` when ``daemon.json`` exists
  but cannot be parsed; defined in
  :mod:`deephaven_mcp._exceptions` and re-exported here.

Liveness probing (deciding whether the registered PID is still
*the* daemon) is decided by
:meth:`DaemonRegistryEntry.is_live`; the underlying PID-reuse-safe
primitive lives in :mod:`deephaven_mcp._processes`
(``ProcessIdentity``). This module owns the wire format only.

The entry also carries the daemon's *build identity*
(:class:`DaemonBuildIdentity` — package version, venv, and a
source fingerprint) under a required ``build_identity``
sub-object, so the CLI can verify it is about to reuse a daemon
running the *same build* it ships from, not merely a live
process. The compatibility policy (warn / refuse / restart /
ignore, per identity field) lives in the CLI
(:mod:`deephaven_mcp.cli._daemon._reuse`); this module owns only the
recorded identity. A registry missing the ``build_identity`` key,
like any other schema mismatch, is caught by Pydantic validation in
:meth:`DaemonDirectory.read_entry`, which raises
:class:`RegistryCorruptError`.

This module intentionally does **not** contain:

- *How to be a daemon.* Idle-shutdown machinery and the HTTP
  transport assembly live in :mod:`deephaven_mcp.mcp_systems_server`
  (specifically :mod:`._idle` and :mod:`.server`).
- *How to spawn / stop a daemon.* The CLI's subprocess management
  lives in :mod:`deephaven_mcp.cli._daemon`.
- *Liveness probing.* See :mod:`deephaven_mcp._processes` and
  :mod:`deephaven_mcp.cli._daemon`.

Async Safety:

- :class:`DaemonDirectory` is synchronous; all I/O is small and
  bounded (single JSON file). Callers that need to run it from
  async code can do so without offloading.

Error Handling:

- :meth:`DaemonDirectory.read_entry` returns ``None`` *only* when
  ``daemon.json`` is genuinely absent. Any other failure (invalid
  JSON, schema validation error, OS read error) raises
  :class:`RegistryCorruptError` with the underlying exception
  chained via ``__cause__``, so callers cannot silently conflate
  "no daemon" with "corrupt registry".
- :meth:`DaemonDirectory.write_entry` performs a fully atomic
  publish-rename; partial files are cleaned up on failure and the
  previously-published registry is preserved.

Dependencies:

- :mod:`pydantic` — for the model.
- Standard library only otherwise.
"""

from __future__ import annotations

__all__ = [
    "DaemonBuildIdentity",
    "DaemonDirectory",
    "DaemonRegistryEntry",
    "LockedRegistry",
    "RegistryCorruptError",
]

import hashlib
import json
import logging
import sys
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, SecretStr

from deephaven_mcp._exceptions import RegistryCorruptError
from deephaven_mcp._platform.fsutil import (
    AdvisoryFileLock,
    atomic_write_private,
    replace_with_retry,
    unlink_with_retry,
)
from deephaven_mcp._processes import ProcessIdentity
from deephaven_mcp._pydantic import RedactableSchema, StrictSchema
from deephaven_mcp._version import version as _PACKAGE_VERSION
from deephaven_mcp.config import daemon_dir, harden_private_dir

_LOGGER = logging.getLogger(__name__)


_DAEMON_REGISTRY_FILENAME = "daemon.json"
"""Filename of the registry written under ``<runtime_dir>/daemon/``."""

_DAEMON_LOCK_FILENAME = "daemon.lock"
"""Filename of the advisory registry lock that serializes every
read-then-mutate sequence on ``daemon.json`` across processes."""

_DAEMON_LOG_FILENAME = "daemon.log"
"""Filename of the captured daemon stdout/stderr log."""

_DAEMON_STARTING_FILENAME = "daemon.starting"
"""Filename of the spawn-in-progress marker. Written under the registry
lock by a CLI about to spawn a daemon and cleared once that spawn
publishes its entry (or times out); lets a peer CLI detect an in-flight
spawn and avoid starting a competing daemon."""


_PACKAGE_ROOT = Path(__file__).resolve().parent
"""Filesystem root of the installed ``deephaven_mcp`` package, used as the
fingerprint walk root."""


def _compute_source_fingerprint(package_root: Path = _PACKAGE_ROOT) -> str:
    """Hash the package's ``*.py`` file stats into a stable fingerprint.

    Walks every ``*.py`` file under ``package_root`` in sorted order and
    folds each file's POSIX relative path, byte size, and modification
    time (integer nanoseconds) into a SHA-256 digest. Stat-only: no file
    contents are read, so the cost is one ``stat`` per file. The digest
    changes whenever a source file is edited in place, added, removed, or
    rewritten (e.g. a reinstall that updates mtimes), and is stable across
    repeated calls against an unchanged install.

    Args:
        package_root (Path): Directory to walk. Defaults to the installed
            ``deephaven_mcp`` package root.

    Returns:
        str: The hex SHA-256 digest of the folded file stats.

    Note:
        Measured at ~6 ms over ~118 ``*.py`` files, run once per CLI
        invocation and once per daemon publish — negligible next to Python
        interpreter startup, so the result is not cached.
    """
    hasher = hashlib.sha256()
    for path in sorted(package_root.rglob("*.py")):
        try:
            stat = path.stat()
        except OSError:
            # A file racing removal during the walk is skipped rather than
            # aborting the whole fingerprint.
            continue
        rel = path.relative_to(package_root).as_posix()
        hasher.update(f"{rel}\0{stat.st_size}\0{stat.st_mtime_ns}\0".encode())
    return hasher.hexdigest()


class DaemonBuildIdentity(StrictSchema):
    """The build a daemon process is running, as a comparable triple.

    The CLI compares the identity it computes for itself
    (:meth:`current`) against the one a running daemon recorded in
    ``daemon.json`` (:attr:`DaemonRegistryEntry.build_identity`) to
    decide whether reusing that daemon is safe. Each field is one
    independent difference dimension; the CLI's per-field reuse policy
    (:mod:`deephaven_mcp.cli._daemon._reuse`) maps each to an action.
    """

    version: str
    """The ``deephaven-mcp`` package version (``deephaven_mcp.__version__``)."""

    venv: str
    """The interpreter's ``sys.prefix`` — the virtualenv root. Distinguishes
    daemons launched from different venvs (the only signal for the
    *surrounding* environment, e.g. which ``deephaven-server`` is installed,
    that the source fingerprint cannot see)."""

    fingerprint: str
    """Hash over the installed ``deephaven_mcp`` package's ``*.py`` file
    stats (see :func:`_compute_source_fingerprint`). Catches in-place code
    edits at an unchanged version + venv."""

    @classmethod
    def current(cls) -> DaemonBuildIdentity:
        """Return the identity of the currently running interpreter/install."""
        return cls(
            version=_PACKAGE_VERSION,
            venv=sys.prefix,
            fingerprint=_compute_source_fingerprint(),
        )


class DaemonRegistryEntry(RedactableSchema):
    """Validated wire format of the on-disk ``daemon.json``.

    Inherits :class:`RedactableSchema` so :attr:`psk` is masked in
    :func:`repr` and :meth:`model_dump` by default; plaintext is
    recoverable only via :meth:`pydantic.SecretStr.get_secret_value`.
    """

    pid: Annotated[int, Field(gt=0)]
    """OS process ID of the daemon. Used by the CLI to check
    liveness and to send termination signals. Constrained to
    ``> 0`` because PID 0 is the kernel and negative values are
    not real PIDs; a registry file claiming either is corrupt.

    Paired with :attr:`create_time_ns` to form a
    :class:`~deephaven_mcp._processes.ProcessIdentity` that survives
    PID reuse (the kernel can recycle ``pid`` to an unrelated
    process between registry write and registry read)."""

    create_time_ns: Annotated[int, Field(gt=0)]
    """Kernel-reported process creation time in integer nanoseconds, sourced
    from :meth:`psutil.Process.create_time`. Paired with :attr:`pid` to form a
    :class:`~deephaven_mcp._processes.ProcessIdentity` (which uses the same
    ``create_time_ns`` name). Distinct from :attr:`started_at`, which is the
    wall-clock instant the daemon *published* its registry entry; this is the
    kernel's *process-creation* instant, used for PID-reuse-safe identity."""

    process_name: Annotated[str, Field(min_length=1)]
    """Expected process-name token. The CLI's liveness check looks
    for it as a substring of ``psutil.Process(pid).name()`` or the
    joined cmdline. Empty values are rejected."""

    host: Literal["127.0.0.1"]
    """Loopback bind address the daemon is listening on."""

    port: Annotated[int, Field(gt=0, lt=65536)]
    """TCP port the daemon's streamable-HTTP transport is bound to.
    Constrained to a valid TCP port (``1..65535``)."""

    psk: SecretStr
    """Auto-generated pre-shared key the CLI must send in the
    ``X-Deephaven-PSK`` header. Stored as :class:`pydantic.SecretStr`
    so default ``repr`` and ``model_dump`` cannot leak the plaintext.
    Length floors are enforced by
    :class:`~deephaven_mcp.auth.middleware.PSKMiddleware`, not here."""

    started_at: AwareDatetime
    """UTC timestamp recorded when the daemon wrote its registry
    entry. Typed as :class:`pydantic.AwareDatetime` so naive
    datetimes are rejected as corrupt. Round-trips to ISO-8601 in
    ``model_dump(mode="json")``."""

    config_dir: Path
    """Absolute path to the configuration directory the daemon was
    started against. Surfaced by ``dh-mcp daemon status`` so the
    operator can confirm which tree is in use. Round-trips to a
    string in ``model_dump(mode="json")``."""

    server_name: Annotated[str, Field(min_length=1)]
    """Human-readable server identifier sourced from
    ``ServerConfig.server_name``. Surfaced by ``dh-mcp daemon
    status`` so the operator can confirm which configured server
    is running. Empty values are rejected because they would
    leave ``daemon status`` output ambiguous."""

    build_identity: DaemonBuildIdentity
    """The daemon's :class:`DaemonBuildIdentity` (package version, venv,
    source fingerprint). Required; a registry missing this key fails
    validation and is surfaced as :class:`RegistryCorruptError`."""

    @property
    def identity(self) -> ProcessIdentity:
        """Return ``ProcessIdentity(pid, create_time_ns)`` for this entry."""
        return ProcessIdentity(pid=self.pid, create_time_ns=self.create_time_ns)

    def is_live(self) -> bool:
        """Return ``True`` iff the recorded process is still running.

        Delegates to :meth:`ProcessIdentity.is_alive`: the PID must
        currently exist *and* its kernel create-time must match the
        recorded :attr:`create_time_ns`, so a recycled PID (a
        different process reusing the number) reads as not live. This
        is the single liveness definition shared by the CLI lifecycle
        and the server's registry-publish refusal.
        """
        return self.identity.is_alive()


class LockedRegistry(AbstractContextManager["LockedRegistry"]):
    """Active, lock-holding session over a daemon registry directory.

    Obtainable only by entering :meth:`DaemonDirectory.locked`::

        with directory.locked() as reg:
            entry = reg.read()
            ...            # decide
            reg.write(new_entry)

    Holding an instance means this process holds the cross-process
    advisory lock (``daemon.lock``) for the duration of the ``with``
    block. Every registry *mutation* lives on this class — not on
    :class:`DaemonDirectory` — so the "lock must be held" rule is
    enforced *structurally*: a caller simply cannot reach
    :meth:`write`, :meth:`delete`, :meth:`quarantine`, or the
    start-marker methods without first entering the lock. There is
    no runtime flag to check and no way to forget the ``with``.

    Lock-free reads stay on :meth:`DaemonDirectory.read_entry` and
    are mirrored here as :meth:`read` for use inside the block; the
    read itself never needs the lock, but re-reading *inside* the
    lock is the correct pattern immediately before a mutation.

    Enter exactly once; instances are neither reentrant nor
    reusable. The underlying
    :class:`~deephaven_mcp._platform.fsutil.AdvisoryFileLock` times out
    (rather than deadlocking) if the same process attempts a nested
    acquire on a second instance.
    """

    def __init__(self, directory: DaemonDirectory) -> None:
        """Capture the directory and build (but do not acquire) the lock.

        Args:
            directory (DaemonDirectory): The directory whose
                ``daemon.lock`` is acquired on ``__enter__`` and
                whose registry artifacts these methods mutate.
        """
        self._dir = directory
        self._lock = AdvisoryFileLock(directory.lock_path)

    def __enter__(self) -> LockedRegistry:
        """Acquire the cross-process advisory lock.

        Returns:
            LockedRegistry: ``self``, for ``with ... as reg:`` form.

        Raises:
            FileLockTimeoutError: If the lock cannot be acquired
                within the lock's timeout (a wedged or crashed peer
                holder, or a same-process nested-acquire bug).
        """
        self._lock.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Release the advisory lock, even if the protected block raised."""
        self._lock.__exit__(exc_type, exc, tb)

    def read(self) -> DaemonRegistryEntry | None:
        """Re-read and validate ``daemon.json`` from inside the lock.

        Delegates to :meth:`DaemonDirectory.read_entry`. The read
        does not itself require the lock, but re-reading inside the
        held lock is the correct first step of a read-then-mutate
        sequence (the lock-free read that selected the slow path may
        be stale by the time the lock is granted).

        Returns:
            DaemonRegistryEntry | None: The validated entry, or
                ``None`` when ``daemon.json`` is genuinely absent.

        Raises:
            RegistryCorruptError: If the file exists but cannot be
                parsed.
        """
        return self._dir.read_entry()

    def write(self, entry: DaemonRegistryEntry) -> None:
        """Atomically write ``entry`` to ``daemon.json``.

        Delegates the write-temp-then-rename mechanics (including
        owner-only ``0o600`` mode on POSIX and file/directory
        ``fsync``) to :func:`deephaven_mcp._platform.fsutil.atomic_write_private`.
        The atomic rename guarantees readers never see a torn file;
        the held lock guarantees no read-then-mutate from a peer
        races the publish.

        Args:
            entry (DaemonRegistryEntry): The validated entry to
                serialize. The PSK is written as plaintext into the
                on-disk JSON.
        """
        registry_path = self._dir.registry_path
        # ``atomic_write_private`` requires the parent directory to
        # exist. Harden (create + lock to user-private mode) rather
        # than a bare ``mkdir`` so a write that lazily creates the
        # daemon directory never leaves the PSK-bearing registry in a
        # group/world-readable directory; idempotent when the caller
        # already hardened it before spawning.
        harden_private_dir(self._dir.path)
        payload = entry.model_dump(mode="json", context={"reveal": True})
        data = json.dumps(payload, indent=2).encode("utf-8")
        atomic_write_private(registry_path, data)
        _LOGGER.info(
            f"[daemon_registry:LockedRegistry.write] Wrote registry "
            f"pid={entry.pid} host={entry.host} port={entry.port} "
            f"-> {registry_path}"
        )

    def delete(self) -> None:
        """Remove ``daemon.json`` if present; otherwise no-op.

        The held lock guarantees the delete is not racing a fresh
        publish from a peer process; ``unlink`` itself is atomic but
        unaware of whose entry it is removing.
        """
        registry_path = self._dir.registry_path
        existed = registry_path.exists()
        unlink_with_retry(registry_path)
        if existed:
            _LOGGER.info(
                f"[daemon_registry:LockedRegistry.delete] Removed "
                f"registry {registry_path}"
            )

    def quarantine(self) -> Path | None:
        """Rename a corrupt/stale ``daemon.json`` to a timestamped sibling.

        The CLI calls this when it discovers the registry cannot be
        parsed (or is stale): the well-known ``daemon.json`` path is
        unblocked (so a fresh spawn can publish a new entry) while
        the malformed bytes are preserved on disk for operator
        postmortem.

        The quarantine filename has the form
        ``daemon.json.corrupt-{YYYYMMDDTHHMMSSZ}`` with UTC, second
        precision; sequential corruptions sort chronologically.

        Returns:
            Path | None: The quarantine path on success, or ``None``
                when ``daemon.json`` did not exist (no-op).
        """
        registry_path = self._dir.registry_path
        if not registry_path.exists():
            return None
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        target = registry_path.with_name(f"{registry_path.name}.corrupt-{timestamp}")
        # On the rare collision (two corruptions in the same second),
        # append a numeric suffix to keep both diagnostics.
        suffix = 1
        while target.exists():
            target = registry_path.with_name(
                f"{registry_path.name}.corrupt-{timestamp}.{suffix}"
            )
            suffix += 1
        try:
            replace_with_retry(registry_path, target)
        except FileNotFoundError:
            # Race tolerated even with the lock: an external process
            # (outside the lock protocol) could have removed the
            # registry between ``exists()`` and the rename. The
            # caller's post-condition (file gone after the call) is
            # satisfied; report a no-op quarantine.
            return None
        _LOGGER.info(
            f"[daemon_registry:LockedRegistry.quarantine] Quarantined "
            f"corrupt registry to {target}"
        )
        return target

    def read_start_marker(self) -> datetime | None:
        """Read the spawn-in-progress marker's recorded start time.

        The marker (``daemon.starting``) is written by a CLI that is
        about to spawn a daemon and is cleared once the spawn either
        publishes its registry entry or times out. A peer CLI that
        finds a *fresh* marker defers to the in-progress spawn rather
        than starting a competing daemon (the double-spawn guard).

        Returns:
            datetime | None: The UTC start time recorded in the
                marker, or ``None`` when no marker exists or its
                contents cannot be parsed (a corrupt marker is
                treated as absent so the caller may overwrite it).
        """
        starting_path = self._dir.starting_path
        try:
            raw = starting_path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return None
        except OSError as exc:
            _LOGGER.warning(
                f"[daemon_registry:LockedRegistry.read_start_marker] Cannot read "
                f"start marker {starting_path}; treating as absent: {exc}"
            )
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError as exc:
            _LOGGER.warning(
                f"[daemon_registry:LockedRegistry.read_start_marker] Malformed "
                f"start marker {starting_path} ({raw!r}); treating as absent: {exc}"
            )
            return None

    def write_start_marker(self, started_at: datetime) -> None:
        """Write the spawn-in-progress marker stamped with ``started_at``.

        Args:
            started_at (datetime): The UTC instant the spawn began.
                Serialized as ISO-8601; read back by
                :meth:`read_start_marker` for staleness comparison.
        """
        path = self._dir.path
        starting_path = self._dir.starting_path
        path.mkdir(parents=True, exist_ok=True)
        starting_path.write_text(started_at.isoformat(), encoding="utf-8")
        _LOGGER.debug(
            f"[daemon_registry:LockedRegistry.write_start_marker] Wrote start "
            f"marker {starting_path} ({started_at.isoformat()})"
        )

    def clear_start_marker(self) -> None:
        """Remove the spawn-in-progress marker if present; otherwise no-op."""
        unlink_with_retry(self._dir.starting_path)


class DaemonDirectory:
    """Typed handle to ``<runtime_dir>/daemon/`` and its protocol artifacts.

    The daemon directory hosts three artifacts the CLI and the daemon
    agree on by filename: the registry (``daemon.json``), the
    advisory registry lock (``daemon.lock``), and the captured
    stdout/stderr log (``daemon.log``). Reach them through
    :attr:`registry_path`, :attr:`lock_path`, :attr:`log_path` rather
    than the filename constants.

    Exposes the artifact paths, the lock-free :meth:`read_entry`,
    and :meth:`locked` — the only door to registry *mutation*.

    Locking contract:

    - All mutations live on :class:`LockedRegistry`, obtained via
      ``with directory.locked() as reg: ...``. Entering the
      context acquires the cross-process advisory lock; the
      mutators (``reg.write``, ``reg.delete``, ``reg.quarantine``,
      and the start-marker methods) are unreachable without it. The
      lock-holding requirement is therefore structural, not a
      runtime assertion.
    - :meth:`read_entry` is lock-free. ``os.replace`` makes the
      published file atomic at the directory-entry level, so a
      concurrent reader observes either the old or the new entry,
      never a torn intermediate.
    """

    def __init__(self, path: Path) -> None:
        """Capture the daemon subdirectory.

        Most callers should construct via
        :meth:`for_runtime_dir` instead, which encapsulates the
        ``<runtime_dir>/daemon/`` convention. The raw constructor
        is exposed primarily for tests that want to point a
        :class:`DaemonDirectory` at an arbitrary ``tmp_path``.

        Args:
            path (Path): The daemon subdirectory itself (not the
                runtime root). Need not exist yet.
        """
        self._path = path
        self._registry_path = path / _DAEMON_REGISTRY_FILENAME
        self._lock_path = path / _DAEMON_LOCK_FILENAME
        self._log_path = path / _DAEMON_LOG_FILENAME
        self._starting_path = path / _DAEMON_STARTING_FILENAME

    @classmethod
    def for_runtime_dir(cls, runtime_dir: Path) -> DaemonDirectory:
        """Construct a :class:`DaemonDirectory` rooted at ``runtime_dir/daemon``.

        Encapsulates the ``<runtime_dir>/daemon/`` convention so
        callers do not need to import
        :func:`deephaven_mcp.config.daemon_dir` separately and so
        the convention has a single source of truth. Prefer this
        over passing ``daemon_dir(runtime_dir)`` to ``__init__``.

        Args:
            runtime_dir (Path): The runtime root (e.g.
                :func:`deephaven_mcp.config.resolve_runtime_dir`'s
                result). Need not exist yet.

        Returns:
            DaemonDirectory: A handle rooted at
                ``runtime_dir / "daemon"``. The directory itself is
                not created by this call; the first
                :meth:`LockedRegistry.write` lazily creates it, and
                operators are expected to harden it via
                :func:`deephaven_mcp.config.harden_private_dir`
                before any sensitive material is written.
        """
        return cls(daemon_dir(runtime_dir))

    @property
    def path(self) -> Path:
        """The daemon subdirectory itself."""
        return self._path

    @property
    def registry_path(self) -> Path:
        """Full path to the ``daemon.json`` registry file."""
        return self._registry_path

    @property
    def lock_path(self) -> Path:
        """Full path to the ``daemon.lock`` advisory registry lock file."""
        return self._lock_path

    @property
    def log_path(self) -> Path:
        """Full path to the ``daemon.log`` captured stdout/stderr log."""
        return self._log_path

    @property
    def starting_path(self) -> Path:
        """Full path to the ``daemon.starting`` spawn-in-progress marker."""
        return self._starting_path

    def locked(self) -> LockedRegistry:
        """Return a :class:`LockedRegistry` session over this directory.

        Entering the returned context manager acquires the
        cross-process advisory lock (``daemon.lock``); the session
        exposes every registry *mutation*. This is the only way to
        mutate the registry — the lock-holding requirement is
        enforced structurally rather than by a runtime check.

        At most one process holds the lock at a time. Every
        read-then-mutate sequence (CLI spawn, stop, reset, stale
        cleanup; daemon publish, unpublish) wraps its decision and
        mutation in a single ``with directory.locked() as reg:``
        block. The lock file at :attr:`lock_path` is created on
        entry if missing.

        **Pure reads do not need the lock.** :meth:`read_entry` is
        intentionally lock-free — the atomic ``os.replace`` publish
        guarantees readers always see either the old or new complete
        file, never a torn write. Holding the lock during reads
        would serialize every CLI invocation against every other one
        for no safety benefit.

        Usage::

            with runtime.daemon_dir.locked() as reg:
                entry = reg.read()
                ...  # decide, then mutate via reg.write/delete/...

        Returns:
            LockedRegistry: An unentered, single-use session.
        """
        return LockedRegistry(self)

    def read_entry(self) -> DaemonRegistryEntry | None:
        """Read and validate ``daemon.json``.

        Lock-free. The atomic-rename publish in
        :meth:`LockedRegistry.write` guarantees a concurrent reader
        sees either the old or the new entry, never a torn
        intermediate. Callers that intend to *mutate* based on the
        result must re-read inside :meth:`locked` before mutating.

        Returns:
            DaemonRegistryEntry | None: The validated entry, or
                ``None`` *only* when the file is genuinely absent
                (``FileNotFoundError`` on open). Callers can
                therefore treat ``None`` as "no daemon" without
                conflating it with corruption.

        Raises:
            RegistryCorruptError: If the file exists but cannot be
                parsed as the expected schema (invalid JSON,
                missing / extra / wrong-type fields, or an OS read
                error such as a permission denial). The original
                exception is chained via ``__cause__``.
        """
        # File-mode validation is skipped here — the 0o700 directory
        # perimeter established at startup by ``harden_private_dir``
        # is the trust boundary, mirroring the config-tree audit
        # model (one-shot perimeter, not per-read).
        try:
            raw = self._registry_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RegistryCorruptError(
                f"Cannot read daemon registry at {self._registry_path}: {exc}"
            ) from exc
        try:
            return DaemonRegistryEntry.model_validate(json.loads(raw))
        except ValueError as exc:
            # ``ValueError`` catches both ``json.JSONDecodeError`` and
            # Pydantic ``ValidationError`` — every "bytes on disk are
            # not a valid registry entry" condition.
            raise RegistryCorruptError(
                f"Malformed daemon registry at {self._registry_path}: {exc}"
            ) from exc
