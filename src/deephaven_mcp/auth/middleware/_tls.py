"""ASGI middleware enforcing transport-layer security on auth-bearing requests.

The MCP servers consume credentials via HTTP headers
(``X-Deephaven-Password``, ``X-Deephaven-Private-Key``, ``X-Deephaven-PSK``).
Those headers carry secrets in cleartext on the wire and must therefore
travel over TLS. :class:`TlsEnforcementMiddleware` is mounted ahead of
:class:`~deephaven_mcp.auth.middleware.AuthenticationMiddleware` and
rejects requests that cannot be shown to be transport-encrypted.

Decision algorithm (per ``http`` request, evaluated top-to-bottom):

1. If the path is in ``bypass_paths`` — pass. Callers typically include
   their liveness/readiness probe path here so probes succeed
   regardless of peer; this middleware is policy-neutral and does
   *not* hardcode any application routes.
2. If ``scope["scheme"] == "https"`` (uvicorn terminated TLS itself) — pass.
3. If the immediate peer is loopback (``127.0.0.0/8`` / ``::1``) — pass.
   Loopback traffic never leaves the kernel.
4. If ``trust_forwarded_proto`` is set AND the immediate peer is in the
   ``forwarded_allow_ips`` allowlist AND the request carries
   ``X-Forwarded-Proto: https`` (per the chain rule below) — pass.
   The allowlist defends against a malicious unaffiliated client
   spoofing the header. When the request carries multiple
   ``X-Forwarded-Proto`` header lines, only the **last** line is
   honored — the convention every well-behaved proxy follows when
   appending. Within a single header, the value may be a
   comma-separated chain (``"https,http"``); the **last** comma token
   is the entry written by the trusted proxy and the only one consulted.
5. If ``allow_cleartext`` is set — pass, with a throttled WARNING.
6. Otherwise reject with ``426 Upgrade Required``.

The reject path uses RFC 7231 §6.5.15 / RFC 2817 status ``426`` and
emits ``Upgrade: TLS/1.2, HTTP/1.1`` + ``Connection: Upgrade`` headers,
the canonical "client must use TLS" response. No ``WWW-Authenticate``
header is emitted; this is a transport error, not an auth failure.

Non-``http`` scopes (``lifespan``, ``websocket``) pass through unchanged.

This middleware never reads, inspects, or validates the ``X-Deephaven-*``
auth headers; that is the responsibility of
:class:`~deephaven_mcp.auth.middleware.AuthenticationMiddleware`. Its sole
concern is whether the *transport* is acceptable to carry secrets.
"""

from __future__ import annotations

import dataclasses
import ipaddress
import json
import logging
import time
from collections.abc import Sequence

from starlette.types import ASGIApp, Receive, Scope, Send

__all__ = [
    "TlsEnforcementMiddleware",
    "TransportSecurityPolicy",
    "parse_forwarded_allow_ips",
]

_LOGGER = logging.getLogger(__name__)

_CLEARTEXT_WARNING_INTERVAL_SECONDS = 60.0
"""Minimum seconds between successive cleartext-allowed WARNING log lines.

Prevents log flooding when ``allow_cleartext`` is enabled and the server
sees sustained traffic. The first request always logs; subsequent
requests within the interval are silently passed through.
"""


@dataclasses.dataclass(frozen=True)
class TransportSecurityPolicy:
    """Immutable transport-security policy decided once at server startup.

    Attributes:
        trust_forwarded_proto (bool): Whether to honor the
            ``X-Forwarded-Proto`` header from a fronting reverse proxy.
            When ``False``, only ``scope["scheme"] == "https"`` is
            treated as TLS.
        forwarded_allow_ips (tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]):
            Peer-IP allowlist (CIDR-aware) for trusting
            ``X-Forwarded-Proto``. Ignored when
            ``trust_forwarded_proto`` is ``False``. An empty tuple means
            "no peers trusted" (and is therefore equivalent to
            ``trust_forwarded_proto=False``).
        allow_any_forwarded_ip (bool): When ``True``, any peer is treated
            as in the allowlist (set when the operator passed ``"*"``).
        allow_cleartext (bool): When ``True``, accept cleartext non-loopback
            traffic with a throttled WARNING. The "I-know-what-I'm-doing"
            escape hatch.
        bypass_paths (frozenset[str]): Exact-match request paths that
            skip TLS enforcement entirely. Defaults to an empty
            frozenset; callers (typically MCP server entry points)
            are responsible for adding their liveness/readiness probe
            path so probes succeed regardless of peer or scheme.
    """

    trust_forwarded_proto: bool = False
    forwarded_allow_ips: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()
    allow_any_forwarded_ip: bool = False
    allow_cleartext: bool = False
    bypass_paths: frozenset[str] = frozenset()


def parse_forwarded_allow_ips(
    raw: str,
) -> tuple[
    tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...],
    bool,
]:
    """Parse a comma-separated forwarded-IP allowlist.

    Mirrors uvicorn's ``--forwarded-allow-ips`` flag. Accepts:

    - A single IP, broadened to a host network: ``/32`` for IPv4
      (``"10.0.0.5"``) or ``/128`` for IPv6 (``"2001:db8::1"``).
    - A comma-separated list (``"10.0.0.5,192.168.1.0/24"``).
    - CIDR notation (``"10.0.0.0/8"``, ``"2001:db8::/32"``).
    - The wildcard ``"*"`` (any peer trusted; returned via the
      ``allow_any_forwarded_ip`` boolean).

    Whitespace around list entries is stripped. Empty entries are
    rejected to fail loud on misconfiguration.

    Args:
        raw (str): The raw flag value (e.g. from ``--forwarded-allow-ips``
            or ``MCP_FORWARDED_ALLOW_IPS``).

    Returns:
        tuple: ``(networks, allow_any)``. ``networks`` is the parsed
            list of CIDR-aware ``IPv*Network`` objects (empty when
            ``allow_any`` is ``True``). ``allow_any`` is ``True`` iff
            the input contained a ``*`` token.

    Raises:
        ValueError: If ``raw`` is empty/whitespace, contains an empty
            entry (e.g. ``"10.0.0.1,,"``), or contains an unparseable
            address/network.
    """
    if not raw or not raw.strip():
        raise ValueError(
            "--forwarded-allow-ips must not be empty (use '*' to allow any peer)."
        )
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    allow_any = False
    for entry in raw.split(","):
        token = entry.strip()
        if not token:
            raise ValueError(f"--forwarded-allow-ips contains an empty entry: {raw!r}")
        if token == "*":  # noqa: S105 — literal CIDR-list wildcard, not a secret
            allow_any = True
            continue
        try:
            networks.append(ipaddress.ip_network(token, strict=False))
        except ValueError as exc:
            raise ValueError(
                f"--forwarded-allow-ips entry {token!r} is not a valid IP "
                f"address or CIDR network: {exc}"
            ) from exc
    if allow_any:
        # Any-peer trust subsumes any specific networks; return empty
        # tuple so callers don't iterate uselessly.
        return ((), True)
    return (tuple(networks), False)


class TlsEnforcementMiddleware:
    """ASGI middleware that rejects cleartext non-loopback HTTP traffic.

    See module docstring for the full decision algorithm.

    Attributes:
        app (ASGIApp): The inner ASGI application to wrap.
        policy (TransportSecurityPolicy): The transport-security policy
            decided at server startup.
    """

    def __init__(self, app: ASGIApp, policy: TransportSecurityPolicy) -> None:
        """Initialize the middleware.

        Args:
            app (ASGIApp): The inner ASGI app.
            policy (TransportSecurityPolicy): Frozen policy. In production
                this is the value returned by
                ``deephaven_mcp.mcp_systems_server.server._validate_transport_security_or_exit``
                at server startup; tests construct it directly.
        """
        self.app = app
        self.policy = policy
        self._last_cleartext_warning_monotonic: float = 0.0
        # Counts cleartext requests accepted *during* a throttle window,
        # so the next emitted warning can advertise how many similar
        # requests were silently passed since the last log line.
        self._suppressed_cleartext_warnings: int = 0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI entry point.

        Non-``http`` scopes are forwarded unchanged. ``http`` requests
        are evaluated against :attr:`policy`; on pass the request is
        forwarded; on reject a ``426 Upgrade Required`` response is
        sent.
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.policy.bypass_paths:
            await self.app(scope, receive, send)
            return

        if scope.get("scheme") == "https":
            await self.app(scope, receive, send)
            return

        peer_ip = _extract_peer_ip(scope)
        if peer_ip is not None and peer_ip.is_loopback:
            await self.app(scope, receive, send)
            return

        if self.policy.trust_forwarded_proto and peer_ip is not None:
            if _peer_in_allowlist(peer_ip, self.policy):
                if _last_forwarded_proto(scope.get("headers", ())) == "https":
                    await self.app(scope, receive, send)
                    return

        if self.policy.allow_cleartext:
            self._maybe_warn_cleartext(path, peer_ip)
            await self.app(scope, receive, send)
            return

        _LOGGER.warning(
            f"[TlsEnforcementMiddleware:__call__] Rejecting cleartext request "
            f"to {path!r} from peer={peer_ip!s} (HTTP 426 Upgrade Required)."
        )
        await _send_426(send)

    def _maybe_warn_cleartext(
        self,
        path: str,
        peer_ip: ipaddress.IPv4Address | ipaddress.IPv6Address | None,
    ) -> None:
        """Emit a throttled WARNING when cleartext is explicitly allowed.

        The first request always logs; subsequent requests within
        :data:`_CLEARTEXT_WARNING_INTERVAL_SECONDS` are silently passed
        through but counted via :attr:`_suppressed_cleartext_warnings`.
        The next emitted warning advertises that count so log analysis
        can recover the suppressed request rate during the window.
        Uses ``time.monotonic()`` so the throttle is wall-clock
        independent.
        """
        now = time.monotonic()
        if (
            now - self._last_cleartext_warning_monotonic
            < _CLEARTEXT_WARNING_INTERVAL_SECONDS
        ):
            self._suppressed_cleartext_warnings += 1
            return
        suppressed = self._suppressed_cleartext_warnings
        self._suppressed_cleartext_warnings = 0
        self._last_cleartext_warning_monotonic = now
        suppressed_suffix = (
            f" (suppressed {suppressed} similar warning"
            f"{'' if suppressed == 1 else 's'} in the last "
            f"{int(_CLEARTEXT_WARNING_INTERVAL_SECONDS)}s)"
            if suppressed > 0
            else ""
        )
        _LOGGER.warning(
            f"[TlsEnforcementMiddleware:_maybe_warn_cleartext] Accepting "
            f"cleartext request to {path!r} from peer={peer_ip!s} because "
            "allow_cleartext=True. Auth headers are traveling unencrypted "
            f"on the wire.{suppressed_suffix}"
        )


def _extract_peer_ip(
    scope: Scope,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Extract the immediate peer's IP from an ASGI scope.

    Per the ASGI spec, ``scope["client"]`` is ``(host, port)`` or
    ``None``. Returns ``None`` if absent or unparseable so callers can
    treat "unknown peer" as "not loopback / not in allowlist" — the
    fail-secure default.
    """
    client = scope.get("client")
    if not client:
        return None
    host = client[0] if isinstance(client, (tuple, list)) and client else None
    if not isinstance(host, str) or not host:
        return None
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


def _peer_in_allowlist(
    peer_ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    policy: TransportSecurityPolicy,
) -> bool:
    """Return ``True`` iff ``peer_ip`` is trusted to set ``X-Forwarded-Proto``.

    Honors the ``allow_any_forwarded_ip`` short-circuit (``"*"`` was
    passed), then walks the CIDR network list.
    """
    if policy.allow_any_forwarded_ip:
        return True
    return any(peer_ip in net for net in policy.forwarded_allow_ips)


def _last_forwarded_proto(
    raw_headers: Sequence[tuple[bytes, bytes]],
) -> str | None:
    """Return the effective ``X-Forwarded-Proto`` value, lowercased.

    Walks the raw ASGI header tuples (rather than a header-merged
    ``dict``) to defend against header-stuffing on misbehaving proxies.
    The convention every well-behaved proxy follows when adding the
    header is to **append**, so the LAST occurrence is the one written
    by the most-recent (most-trusted) hop. Within that occurrence the
    value may be a comma-separated chain (``"https,http"``); the LAST
    comma-token is the entry written by the trusted proxy. A client
    that sends its own ``X-Forwarded-Proto: http`` header followed by a
    proxy-added ``X-Forwarded-Proto: https`` therefore sees the proxy's
    value win — and a client that prepends a comma-separated chain
    cannot trick the server into accepting cleartext.
    Returns ``None`` if no header is present.
    """
    last_value: bytes | None = None
    for name, value in raw_headers:
        if name.decode("latin-1").lower() == "x-forwarded-proto":
            last_value = value
    if last_value is None:
        return None
    decoded = last_value.decode("latin-1")
    # Take the LAST comma token: the entry the trusted proxy wrote, not
    # any chain prefix that may have come from an upstream client.
    last_token = decoded.rsplit(",", maxsplit=1)[-1]
    return last_token.strip().lower()


async def _send_426(send: Send) -> None:
    """Emit a compact JSON ``426 Upgrade Required`` response.

    Headers per RFC 7231 §6.5.15 / RFC 2817:

    - ``Upgrade: TLS/1.2, HTTP/1.1`` advertises the required protocol.
    - ``Connection: Upgrade`` is mandatory whenever ``Upgrade`` is set.

    The body is a small JSON object compatible with the
    ``AuthenticationMiddleware`` 401 body shape so AI agents and clients
    can parse both responses uniformly.
    """
    body = json.dumps(
        {
            "error": "tls_required",
            "detail": (
                "This endpoint accepts authenticated requests only over TLS. "
                "Connect via https://, or — if a TLS-terminating proxy is in "
                "front of this server — start the server with "
                "--trust-forwarded-proto and ensure the proxy sets "
                "X-Forwarded-Proto: https."
            ),
        }
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 426,
            "headers": [
                (b"content-type", b"application/json"),
                (b"upgrade", b"TLS/1.2, HTTP/1.1"),
                (b"connection", b"Upgrade"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
