"""Tests for ``deephaven_mcp.auth.middleware._tls``.

Exercises every cell of :class:`TlsEnforcementMiddleware`'s decision
matrix plus the :func:`parse_forwarded_allow_ips` helper. Uses raw ASGI
scope dicts and ``send`` callables (no Starlette TestClient) so the
tests stay narrowly scoped to the middleware contract.
"""

from __future__ import annotations

import ipaddress
import json
import logging

import pytest

from deephaven_mcp._health import HEALTH_PATH
from deephaven_mcp.auth.middleware import (
    TlsEnforcementMiddleware,
    TransportSecurityPolicy,
    parse_forwarded_allow_ips,
)
from deephaven_mcp.auth.middleware._tls import (
    _CLEARTEXT_WARNING_INTERVAL_SECONDS,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RecordingApp:
    """ASGI app that records the scopes it was called with and returns 200."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def __call__(self, scope, receive, send):
        self.calls.append(scope)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})


async def _receive_empty():
    return {"type": "http.request", "body": b"", "more_body": False}


def _make_send(sink: list[dict]):
    async def _send(msg: dict) -> None:
        sink.append(msg)

    return _send


def _http_scope(
    *,
    scheme: str = "http",
    path: str = "/mcp",
    client_host: str | None = "203.0.113.5",
    client_port: int = 51234,
    headers: dict[str, str] | None = None,
) -> dict:
    encoded = [
        (name.encode("latin-1"), value.encode("latin-1"))
        for name, value in (headers or {}).items()
    ]
    return {
        "type": "http",
        "scheme": scheme,
        "path": path,
        "method": "POST",
        "client": (client_host, client_port) if client_host is not None else None,
        "headers": encoded,
    }


def _decode_response(events: list[dict]) -> tuple[int, dict[bytes, bytes], dict]:
    start = next(e for e in events if e["type"] == "http.response.start")
    body = b"".join(e["body"] for e in events if e["type"] == "http.response.body")
    headers = {name: value for name, value in start["headers"]}
    payload: dict = {}
    if body and headers.get(b"content-type", b"").startswith(b"application/json"):
        payload = json.loads(body)
    return start["status"], headers, payload


def _strict_policy(**overrides) -> TransportSecurityPolicy:
    """Return the strict default policy (loopback only) with overrides."""
    base = {
        "trust_forwarded_proto": False,
        "forwarded_allow_ips": (),
        "allow_any_forwarded_ip": False,
        "allow_cleartext": False,
        "bypass_paths": frozenset({HEALTH_PATH}),
    }
    base.update(overrides)
    return TransportSecurityPolicy(**base)


# ---------------------------------------------------------------------------
# parse_forwarded_allow_ips
# ---------------------------------------------------------------------------


class TestParseForwardedAllowIps:
    def test_single_ipv4(self):
        nets, allow_any = parse_forwarded_allow_ips("10.0.0.5")
        assert allow_any is False
        assert len(nets) == 1
        assert ipaddress.ip_address("10.0.0.5") in nets[0]
        assert ipaddress.ip_address("10.0.0.6") not in nets[0]

    def test_single_ipv6(self):
        nets, allow_any = parse_forwarded_allow_ips("::1")
        assert allow_any is False
        assert ipaddress.ip_address("::1") in nets[0]

    def test_cidr(self):
        nets, allow_any = parse_forwarded_allow_ips("10.0.0.0/8")
        assert allow_any is False
        assert ipaddress.ip_address("10.255.255.1") in nets[0]
        assert ipaddress.ip_address("11.0.0.1") not in nets[0]

    def test_comma_list(self):
        nets, allow_any = parse_forwarded_allow_ips(
            "10.0.0.5,192.168.1.0/24, 172.16.0.1"
        )
        assert allow_any is False
        assert len(nets) == 3
        assert ipaddress.ip_address("10.0.0.5") in nets[0]
        assert ipaddress.ip_address("192.168.1.42") in nets[1]
        assert ipaddress.ip_address("172.16.0.1") in nets[2]

    def test_wildcard(self):
        nets, allow_any = parse_forwarded_allow_ips("*")
        assert allow_any is True
        assert nets == ()

    def test_wildcard_with_other_entries_subsumes_them(self):
        # When * is present anywhere, allow_any wins and the specific
        # networks list is returned empty (no point in iterating it).
        nets, allow_any = parse_forwarded_allow_ips("10.0.0.0/8,*")
        assert allow_any is True
        assert nets == ()

    def test_strips_whitespace(self):
        nets, allow_any = parse_forwarded_allow_ips("  10.0.0.5  ,  192.168.0.0/16  ")
        assert allow_any is False
        assert len(nets) == 2

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_forwarded_allow_ips("")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_forwarded_allow_ips("   ")

    def test_empty_entry_rejected(self):
        with pytest.raises(ValueError, match="empty entry"):
            parse_forwarded_allow_ips("10.0.0.1,,192.168.0.1")

    def test_trailing_comma_rejected(self):
        with pytest.raises(ValueError, match="empty entry"):
            parse_forwarded_allow_ips("10.0.0.1,")

    def test_invalid_ip_rejected(self):
        with pytest.raises(ValueError, match="not a valid IP address"):
            parse_forwarded_allow_ips("not-an-ip")

    def test_invalid_cidr_rejected(self):
        with pytest.raises(ValueError, match="not a valid IP address"):
            parse_forwarded_allow_ips("10.0.0.1/99")


# ---------------------------------------------------------------------------
# TlsEnforcementMiddleware: pass paths
# ---------------------------------------------------------------------------


class TestPassPaths:
    @pytest.mark.asyncio
    async def test_https_scope_passes(self):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        await mw(_http_scope(scheme="https"), _receive_empty, _make_send(events))
        assert len(app.calls) == 1
        status, _, _ = _decode_response(events)
        assert status == 200

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "loopback_host", ["127.0.0.1", "127.0.0.5", "127.255.255.254", "::1"]
    )
    async def test_loopback_peer_passes_over_http(self, loopback_host):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        await mw(
            _http_scope(scheme="http", client_host=loopback_host),
            _receive_empty,
            _make_send(events),
        )
        assert len(app.calls) == 1

    @pytest.mark.asyncio
    async def test_bypass_path_passes_regardless_of_scheme_or_peer(self):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        await mw(
            _http_scope(scheme="http", client_host="203.0.113.5", path="/health"),
            _receive_empty,
            _make_send(events),
        )
        assert len(app.calls) == 1

    @pytest.mark.asyncio
    async def test_non_http_scope_passes_unchanged(self):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())

        async def _ws_send(_msg):
            pass

        # Provide a websocket scope; the middleware should not inspect or
        # respond to it.
        scope = {"type": "websocket", "path": "/ws"}
        await mw(scope, _receive_empty, _ws_send)
        assert app.calls == [scope]

    @pytest.mark.asyncio
    async def test_lifespan_scope_passes_unchanged(self):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())

        async def _send(_msg):
            pass

        scope = {"type": "lifespan"}
        await mw(scope, _receive_empty, _send)
        assert app.calls == [scope]


# ---------------------------------------------------------------------------
# TlsEnforcementMiddleware: rejection paths
# ---------------------------------------------------------------------------


class TestRejection:
    @pytest.mark.asyncio
    async def test_non_loopback_http_no_opts_rejected_with_426(self, caplog):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        with caplog.at_level(logging.WARNING):
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
        # App is never called.
        assert app.calls == []
        # 426 with proper Upgrade/Connection headers and JSON body.
        status, headers, payload = _decode_response(events)
        assert status == 426
        assert headers[b"upgrade"] == b"TLS/1.2, HTTP/1.1"
        assert headers[b"connection"] == b"Upgrade"
        assert headers[b"content-type"] == b"application/json"
        # Per spec: TLS error must NOT include a WWW-Authenticate header.
        assert b"www-authenticate" not in headers
        assert payload["error"] == "tls_required"
        assert "https://" in payload["detail"]
        # WARNING log at the rejection site.
        assert any(
            "Rejecting cleartext request" in rec.message for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_unparseable_client_host_treated_as_unknown(self):
        # ASGI scope with client=None — middleware must not crash and
        # must reject (loopback check fails on unknown peer).
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        await mw(
            _http_scope(scheme="http", client_host=None),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426

    @pytest.mark.asyncio
    async def test_malformed_client_host_rejected(self):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        # Construct a scope with an unparseable host string by bypassing
        # the helper.
        scope = _http_scope(scheme="http")
        scope["client"] = ("not-an-ip", 1234)
        await mw(scope, _receive_empty, _make_send(events))
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "client_value",
        [
            (None, 1234),  # tuple with None host
            ("", 1234),  # tuple with empty-string host
        ],
    )
    async def test_non_string_or_empty_client_host_rejected(self, client_value):
        """Defends against malformed ASGI scopes (e.g. middleware bugs upstream)."""
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        scope = _http_scope(scheme="http")
        scope["client"] = client_value
        await mw(scope, _receive_empty, _make_send(events))
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426


# ---------------------------------------------------------------------------
# X-Forwarded-Proto trust path
# ---------------------------------------------------------------------------


class TestForwardedProtoTrust:
    @pytest.mark.asyncio
    async def test_trusted_peer_with_https_header_passes(self):
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(
                scheme="http",
                client_host="10.5.5.5",
                headers={"X-Forwarded-Proto": "https"},
            ),
            _receive_empty,
            _make_send(events),
        )
        assert len(app.calls) == 1

    @pytest.mark.asyncio
    async def test_trusted_peer_with_http_header_rejected(self):
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(
                scheme="http",
                client_host="10.5.5.5",
                headers={"X-Forwarded-Proto": "http"},
            ),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426

    @pytest.mark.asyncio
    async def test_trusted_peer_without_header_rejected(self):
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(scheme="http", client_host="10.5.5.5"),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426

    @pytest.mark.asyncio
    async def test_untrusted_peer_with_https_header_rejected(self):
        """Spoofing defense: peer outside allowlist cannot bypass with header."""
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(
                scheme="http",
                client_host="203.0.113.5",
                headers={"X-Forwarded-Proto": "https"},
            ),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426

    @pytest.mark.asyncio
    async def test_trust_disabled_with_header_rejected(self):
        """Even allowlisted peer + header is ignored when trust flag is off."""
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=False, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(
                scheme="http",
                client_host="10.5.5.5",
                headers={"X-Forwarded-Proto": "https"},
            ),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426

    @pytest.mark.asyncio
    async def test_wildcard_allowlist_passes_any_peer(self):
        app = _RecordingApp()
        policy = _strict_policy(
            trust_forwarded_proto=True,
            forwarded_allow_ips=(),
            allow_any_forwarded_ip=True,
        )
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(
                scheme="http",
                client_host="198.51.100.42",
                headers={"X-Forwarded-Proto": "https"},
            ),
            _receive_empty,
            _make_send(events),
        )
        assert len(app.calls) == 1

    @pytest.mark.asyncio
    async def test_https_header_case_insensitive(self):
        """``X-Forwarded-Proto: HTTPS`` is the same as ``https``."""
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(
                scheme="http",
                client_host="10.5.5.5",
                headers={"X-Forwarded-Proto": "  HTTPS  "},
            ),
            _receive_empty,
            _make_send(events),
        )
        assert len(app.calls) == 1

    @pytest.mark.asyncio
    async def test_trust_proto_with_unparseable_peer_rejected(self):
        """Cannot honor X-Forwarded-Proto when peer IP cannot be parsed."""
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _http_scope(
                scheme="http",
                client_host=None,
                headers={"X-Forwarded-Proto": "https"},
            ),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []


# ---------------------------------------------------------------------------
# X-Forwarded-Proto: header-stuffing defenses
# ---------------------------------------------------------------------------


def _scope_with_raw_headers(
    raw_headers: list[tuple[bytes, bytes]],
    *,
    client_host: str = "10.5.5.5",
    path: str = "/mcp",
) -> dict:
    """Build an ASGI scope with arbitrary (possibly duplicate) raw headers."""
    return {
        "type": "http",
        "scheme": "http",
        "path": path,
        "method": "POST",
        "client": (client_host, 51234),
        "headers": raw_headers,
    }


class TestForwardedProtoChainAndDuplicates:
    """Verify the LAST-occurrence / LAST-comma-token rule end-to-end.

    A misbehaving proxy that *appends* (rather than overwrites) means
    the request can carry multiple ``X-Forwarded-Proto`` header lines,
    or a single line whose value is a comma-separated chain. The
    middleware must honor only the last value (the one closest to the
    trusted proxy) so a client cannot trick the server into accepting
    cleartext by injecting an ``http`` token earlier in the chain.
    """

    @pytest.mark.asyncio
    async def test_last_header_wins_when_duplicates_present(self):
        """Two ``X-Forwarded-Proto`` lines: only the last one is consulted."""
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        # Client sent http; proxy appended https. Last wins -> pass.
        events: list[dict] = []
        await mw(
            _scope_with_raw_headers(
                [
                    (b"x-forwarded-proto", b"http"),
                    (b"x-forwarded-proto", b"https"),
                ]
            ),
            _receive_empty,
            _make_send(events),
        )
        assert len(app.calls) == 1

    @pytest.mark.asyncio
    async def test_last_header_http_rejected_even_after_earlier_https(self):
        """Reverse case: proxy https first, then a later http -> reject.

        This is a malformed proxy chain (the server-facing proxy should
        be the last writer), but the middleware refuses to accept
        cleartext rather than guessing the operator's intent.
        """
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _scope_with_raw_headers(
                [
                    (b"x-forwarded-proto", b"https"),
                    (b"x-forwarded-proto", b"http"),
                ]
            ),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426

    @pytest.mark.asyncio
    async def test_comma_chain_last_token_https_passes(self):
        """``http,https`` -> last token https -> pass.

        Models a multi-hop deployment where the trusted proxy appends
        its scheme to a comma-separated chain.
        """
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _scope_with_raw_headers([(b"x-forwarded-proto", b"http, https")]),
            _receive_empty,
            _make_send(events),
        )
        assert len(app.calls) == 1

    @pytest.mark.asyncio
    async def test_comma_chain_last_token_http_rejected(self):
        """``https,http`` -> last token http -> reject.

        Defends against a client that prepends ``https`` hoping the
        merged-dict implementation will pick the first token.
        """
        app = _RecordingApp()
        nets, _ = parse_forwarded_allow_ips("10.0.0.0/8")
        policy = _strict_policy(trust_forwarded_proto=True, forwarded_allow_ips=nets)
        mw = TlsEnforcementMiddleware(app, policy)
        events: list[dict] = []
        await mw(
            _scope_with_raw_headers([(b"x-forwarded-proto", b"https, http")]),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426


# ---------------------------------------------------------------------------
# allow_cleartext path
# ---------------------------------------------------------------------------


class TestAllowCleartext:
    @pytest.mark.asyncio
    async def test_allow_cleartext_passes_with_warning(self, caplog):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy(allow_cleartext=True))
        events: list[dict] = []
        with caplog.at_level(logging.WARNING):
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
        assert len(app.calls) == 1
        # First request always produces the warning banner.
        assert any(
            "allow_cleartext=True" in rec.message and "unencrypted" in rec.message
            for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_allow_cleartext_throttles_warnings(self, caplog):
        """Warnings throttled by ``_CLEARTEXT_WARNING_INTERVAL_SECONDS``."""
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy(allow_cleartext=True))
        events: list[dict] = []
        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                events.clear()
                await mw(
                    _http_scope(scheme="http", client_host="203.0.113.5"),
                    _receive_empty,
                    _make_send(events),
                )
        # Should fire exactly once for three back-to-back requests.
        warning_lines = [
            r for r in caplog.records if "allow_cleartext=True" in r.message
        ]
        assert len(warning_lines) == 1
        # The two suppressed requests are recorded on the middleware
        # instance so the next emitted warning can advertise them.
        assert mw._suppressed_cleartext_warnings == 2

    @pytest.mark.asyncio
    async def test_allow_cleartext_warning_reports_suppressed_count(
        self, monkeypatch, caplog
    ):
        """When the throttle window elapses, the next warning lists the
        count of suppressed requests since the previous emission."""
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy(allow_cleartext=True))
        time_mod = "deephaven_mcp.auth.middleware._tls.time"
        clock = {"now": 1000.0}

        class _FakeTime:
            @staticmethod
            def monotonic() -> float:
                return clock["now"]

        monkeypatch.setattr(time_mod, _FakeTime())
        events: list[dict] = []
        # First request: emits warning with no suffix (nothing suppressed yet).
        with caplog.at_level(logging.WARNING):
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
            # Two more requests inside the throttle window — silently counted.
            for _ in range(2):
                events.clear()
                await mw(
                    _http_scope(scheme="http", client_host="203.0.113.5"),
                    _receive_empty,
                    _make_send(events),
                )
            # Advance past the interval; next request emits a warning that
            # advertises both suppressed entries.
            clock["now"] += _CLEARTEXT_WARNING_INTERVAL_SECONDS + 1.0
            events.clear()
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
        warning_lines = [
            r for r in caplog.records if "allow_cleartext=True" in r.message
        ]
        assert len(warning_lines) == 2
        # The second warning includes the suppressed count + interval.
        assert "suppressed 2 similar warnings" in warning_lines[1].message
        assert (
            f"in the last {int(_CLEARTEXT_WARNING_INTERVAL_SECONDS)}s"
            in warning_lines[1].message
        )
        # And the counter resets after emission.
        assert mw._suppressed_cleartext_warnings == 0

    @pytest.mark.asyncio
    async def test_allow_cleartext_singular_when_one_suppressed(
        self, monkeypatch, caplog
    ):
        """``suppressed 1 similar warning`` (no plural ``s``) when count is 1."""
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy(allow_cleartext=True))
        time_mod = "deephaven_mcp.auth.middleware._tls.time"
        clock = {"now": 1000.0}

        class _FakeTime:
            @staticmethod
            def monotonic() -> float:
                return clock["now"]

        monkeypatch.setattr(time_mod, _FakeTime())
        events: list[dict] = []
        with caplog.at_level(logging.WARNING):
            # First emission.
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
            # Exactly one suppressed.
            events.clear()
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
            # Advance and emit again.
            clock["now"] += _CLEARTEXT_WARNING_INTERVAL_SECONDS + 1.0
            events.clear()
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
        warning_lines = [
            r for r in caplog.records if "allow_cleartext=True" in r.message
        ]
        assert "suppressed 1 similar warning " in warning_lines[1].message
        assert "warnings" not in warning_lines[1].message.split("similar ", 1)[1]

    @pytest.mark.asyncio
    async def test_allow_cleartext_warning_resumes_after_interval(
        self, monkeypatch, caplog
    ):
        """After the interval elapses, warnings resume."""
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy(allow_cleartext=True))
        # Patch monotonic so we can advance time deterministically.
        time_mod = "deephaven_mcp.auth.middleware._tls.time"
        clock = {"now": 1000.0}

        class _FakeTime:
            @staticmethod
            def monotonic():
                return clock["now"]

        monkeypatch.setattr(time_mod, _FakeTime())
        events: list[dict] = []
        with caplog.at_level(logging.WARNING):
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
            # Advance just under the interval — second log should be suppressed.
            clock["now"] += _CLEARTEXT_WARNING_INTERVAL_SECONDS - 0.01
            events.clear()
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
            # Advance past the interval — third log should fire.
            clock["now"] += 1.0
            events.clear()
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5"),
                _receive_empty,
                _make_send(events),
            )
        warning_lines = [
            r for r in caplog.records if "allow_cleartext=True" in r.message
        ]
        assert len(warning_lines) == 2


# ---------------------------------------------------------------------------
# Bypass paths configurability
# ---------------------------------------------------------------------------


class TestBypassPathsConfig:
    @pytest.mark.asyncio
    async def test_custom_bypass_paths(self):
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(
            app,
            _strict_policy(bypass_paths=frozenset({"/livez", "/readyz"})),
        )
        events: list[dict] = []
        for path in ("/livez", "/readyz"):
            events.clear()
            await mw(
                _http_scope(scheme="http", client_host="203.0.113.5", path=path),
                _receive_empty,
                _make_send(events),
            )
        assert len(app.calls) == 2

    @pytest.mark.asyncio
    async def test_bypass_path_only_exact_match(self):
        """``/healthz`` should NOT match ``/health`` exact bypass."""
        app = _RecordingApp()
        mw = TlsEnforcementMiddleware(app, _strict_policy())
        events: list[dict] = []
        await mw(
            _http_scope(scheme="http", client_host="203.0.113.5", path="/healthz"),
            _receive_empty,
            _make_send(events),
        )
        assert app.calls == []
        status, _, _ = _decode_response(events)
        assert status == 426
