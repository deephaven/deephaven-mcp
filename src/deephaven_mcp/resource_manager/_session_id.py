"""Validated session-identifier types: :class:`SessionId` and :class:`QualifiedSessionId`.

:class:`SessionId` is the trailing segment of a session identifier — a
non-empty, colon-free, resource-safe string and a :class:`str` subclass.
Construct one from a string (``SessionId("my_worker")``) or, for the
enterprise registry where the controller assigns each persistent query a
numeric serial, from an int via :meth:`SessionId.from_int`.

:class:`QualifiedSessionId` is the **fully qualified** session identifier
used as the public address of a session across MCP tools, CLI commands,
and registry keys. It is a frozen, slotted dataclass with three
validated fields:

- :attr:`~QualifiedSessionId.system_type`
- :attr:`~QualifiedSessionId.system_name`
- :attr:`~QualifiedSessionId.session_id`

Its on-the-wire string form
``"<system_type>:<system_name>:<session_id>"`` is produced via
:meth:`__str__` (``str(qsid)`` or any f-string interpolation).
Construct from components by calling the class directly
(``QualifiedSessionId(t, n, s)``); construct from a wire string via
:meth:`~QualifiedSessionId.from_str`.

:class:`QualifiedSessionId` deliberately does **not** inherit from
:class:`str`. It is a structured value; the wire form is an *output*.
Producing the wire string at boundaries (MCP tool response payloads,
JSON serialization) is an explicit ``str(qsid)`` call, not a free
interop trick.

Validation uses the resource-name character class from
:func:`~deephaven_mcp._names.validate_resource_name`: ASCII
alphanumerics plus ``_``, ``.``, ``-``, starting with an alphanumeric,
non-empty.
"""

from __future__ import annotations

__all__ = ["QualifiedSessionId", "SessionId"]

from dataclasses import dataclass

from deephaven_mcp._exceptions import InvalidSessionNameError
from deephaven_mcp._taxonomy import SystemType

from .._names import validate_resource_name


class SessionId(str):
    """Identifier of a session: a validated non-empty resource-name string.

    Subclass of :class:`str`, so f-string formatting, dict/set keying,
    equality with plain ``str``, and JSON serialization all behave
    exactly as for ``str``. The constructor validates that the value
    matches the resource-name character class (ASCII alphanumerics plus
    ``_``, ``.``, ``-``; must start with an alphanumeric; non-empty).

    The constructor is idempotent on :class:`SessionId` input.

    Examples:
        Community session keyed by name::

            sid = SessionId("my_worker")

        Enterprise session keyed by controller serial::

            sid = SessionId.from_int(42)   # -> SessionId("42")

    Raises:
        InvalidSessionNameError: When the value is empty, contains
            disallowed characters, or starts with a non-alphanumeric.
    """

    # Empty __slots__ keeps each instance the same size as a bare ``str``
    # (no per-instance ``__dict__``); :class:`SessionId` carries no state
    # beyond the string value stored by :class:`str`.
    __slots__ = ()

    def __new__(cls, value: str) -> SessionId:
        """Construct a validated :class:`SessionId`.

        Args:
            value: The identifier string. Already-:class:`SessionId`
                inputs are returned unchanged.

        Returns:
            SessionId: A validated identifier.

        Raises:
            InvalidSessionNameError: If ``value`` fails
                :func:`validate_resource_name`.
        """
        if type(value) is cls:
            return value
        validate_resource_name(value, field="session_id")
        return super().__new__(cls, value)

    @classmethod
    def from_int(cls, value: int) -> SessionId:
        """Build a :class:`SessionId` from a non-negative integer.

        Used by the enterprise registry where the controller assigns
        each persistent query a non-negative serial; the canonical
        decimal-string form of that serial is the resource-manager
        identifier.

        Args:
            value: A non-negative integer (e.g. a
                :class:`~deephaven_mcp.client.CorePlusQuerySerial`).

        Returns:
            SessionId: ``SessionId(str(value))``.

        Raises:
            InvalidSessionNameError: If ``value`` is negative.
        """
        iv = int(value)
        if iv < 0:
            raise InvalidSessionNameError(
                f"SessionId.from_int requires a non-negative integer; got {iv}"
            )
        return cls(str(iv))


@dataclass(frozen=True, slots=True)
class QualifiedSessionId:
    """Fully qualified, validated session identifier.

    A frozen, slotted dataclass with three validated fields. Equality
    and hashing are derived from the field tuple; instances are
    immutable.

    Construct from components by calling the class directly; the
    constructor validates :attr:`system_name` (the other two fields are
    pre-validated by their own types). Parse a wire-format identifier
    via :meth:`from_str`. Produce the wire form via ``str(qsid)``.

    Examples:
        From components::

            qsid = QualifiedSessionId(
                SystemType.COMMUNITY, "community", SessionId("my_worker")
            )

        From a wire string::

            qsid = QualifiedSessionId.from_str("community:community:my_worker")

        Component access::

            qsid.system_type   # SystemType.COMMUNITY
            qsid.system_name   # "community"
            qsid.session_id    # SessionId("my_worker")

        Wire form::

            str(qsid)          # "community:community:my_worker"
    """

    system_type: SystemType
    """The Deephaven deployment type — :data:`SystemType.COMMUNITY` or :data:`SystemType.ENTERPRISE`."""

    system_name: str
    """Middle segment of the identifier.

    For community sessions this is always the literal ``"community"``
    (the umbrella system name; the static-vs-dynamic distinction lives
    on the manager's :attr:`origin` field, not in the id). For
    enterprise sessions this is the ``system_name`` of the configured
    enterprise system.
    """

    session_id: SessionId
    """Trailing segment of the identifier.

    For enterprise sessions this equals the controller's
    ``CorePlusQuerySerial`` rendered as a decimal string. For community
    sessions this is the session name itself.
    """

    def __post_init__(self) -> None:
        """Validate :attr:`system_name` against the resource-name character class.

        :attr:`system_type` is validated by its enum type;
        :attr:`session_id` is validated by :class:`SessionId`'s own
        constructor; only :attr:`system_name` is plain :class:`str` and
        therefore needs explicit validation here.

        Raises:
            InvalidSessionNameError: If :attr:`system_name` is not a
                valid resource name.
        """
        validate_resource_name(self.system_name, field="system")

    @classmethod
    def from_str(cls, value: str) -> QualifiedSessionId:
        """Parse and validate a wire-format identifier.

        Args:
            value: The wire-format string
                ``"<system_type>:<system_name>:<session_id>"``.

        Returns:
            QualifiedSessionId: A validated identifier.

        Raises:
            InvalidSessionNameError: When the value is not exactly three
                non-empty colon-separated segments, when the
                ``system_type`` segment is not a known
                :class:`SystemType` member, or when the ``system_name``
                or ``session_id`` segment fails resource-name validation.
        """
        parts = value.split(":", 2)
        if len(parts) != 3 or not all(parts):
            raise InvalidSessionNameError(
                f"Invalid session id: {value!r}. "
                f"Expected 'system_type:system:session_id' "
                f"with three non-empty segments."
            )
        type_str, system_str, sid_str = parts
        valid_types = {st.value for st in SystemType}
        if type_str not in valid_types:
            raise InvalidSessionNameError(
                f"Invalid session id: {value!r}. "
                f"system_type segment must be one of "
                f"{sorted(valid_types)}; got {type_str!r}."
            )
        try:
            session_id = SessionId(sid_str)
            return cls(SystemType(type_str), system_str, session_id)
        except InvalidSessionNameError as exc:
            raise InvalidSessionNameError(
                f"Invalid session id: {value!r}. {exc}"
            ) from exc

    def __str__(self) -> str:
        """Return the wire-format string ``"<system_type>:<system_name>:<session_id>"``.

        Symmetric with :meth:`from_str`. The wire form is produced by
        ``str(qsid)`` (or any f-string interpolation) — there is no
        separate property; this is the only rendering API. Use it at
        boundaries that require a string: MCP tool response payloads,
        log messages, JSON serialization, and registry-key lookup when
        the caller has only a string handle.
        """
        return f"{self.system_type.value}:{self.system_name}:{self.session_id}"
