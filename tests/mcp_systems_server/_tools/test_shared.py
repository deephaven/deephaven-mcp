"""Tests for ``deephaven_mcp.mcp_systems_server._tools.shared``.

Covers the helpers used by every tool:

- Lifespan-context accessors (``get_lifespan_context``, ``get_registry``,
  ``get_multi_config``, ``get_community_registry``, ``get_enterprise_registry``).
- Identifier parsers (``parse_session_id``, ``parse_pq_id``).
- Session retrieval (``get_session_from_context``, ``get_enterprise_session``).
- Response shapers / size guards (``error_response``, ``check_response_size``,
  ``format_meta_table_result``, ``build_table_data_response``).
- ``format_initialization_status``, ``redact_json_sensitive_fields``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest
from conftest import MockContext

from deephaven_mcp._exceptions import (
    InternalError,
    InvalidSessionNameError,
)
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.mcp_systems_server._tools import shared
from deephaven_mcp.resource_manager import (
    InitializationPhase,
    SystemType,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(*, registry: object = None, multi_config: object = None) -> MockContext:
    """Build a ``MockContext`` whose lifespan context has the given fields.

    Mirrors the production ``LifespanContext`` shape (a frozen dataclass
    with ``registry``, ``multi_config``, ``evictors``, ``instance_tracker``).
    Tests pass in mocks for the fields they assert on; the conftest's
    ``_adapt_lifespan_context`` fills in defaults for the remaining
    fields so attribute access does not raise.
    """
    lifespan: dict[str, object] = {}
    if registry is not None:
        lifespan["registry"] = registry
    if multi_config is not None:
        lifespan["multi_config"] = multi_config
    return MockContext(lifespan)


# ---------------------------------------------------------------------------
# error_response / format_initialization_status
# ---------------------------------------------------------------------------


def test_error_response_shape():
    assert shared.error_response("boom") == {
        "success": False,
        "error": "boom",
        "isError": True,
    }


def test_format_initialization_status_completed_no_errors():
    assert (
        shared.format_initialization_status(InitializationPhase.COMPLETED, {}) is None
    )


def test_format_initialization_status_loading_phase_emits_status():
    info = shared.format_initialization_status(InitializationPhase.LOADING, {})
    assert info is not None
    assert "actively running" in info["status"].lower()


def test_format_initialization_status_partial_phase_emits_status():
    info = shared.format_initialization_status(InitializationPhase.PARTIAL, {})
    assert info is not None
    assert "not yet" in info["status"].lower()


def test_format_initialization_status_failed_phase_emits_status():
    info = shared.format_initialization_status(InitializationPhase.FAILED, {})
    assert info is not None
    assert "failed" in info["status"].lower()


def test_format_initialization_status_includes_errors():
    info = shared.format_initialization_status(
        InitializationPhase.COMPLETED, {"sys-a": "boom"}
    )
    assert info is not None
    assert info["errors"] == {"sys-a": "boom"}


# ---------------------------------------------------------------------------
# Lifespan-context accessors
# ---------------------------------------------------------------------------


def test_get_lifespan_context_returns_dataclass():
    ctx = _ctx(registry="r", multi_config="c")
    out = shared.get_lifespan_context(ctx)
    assert out.registry == "r"
    assert out.multi_config == "c"


def test_get_registry_returns_lifespan_registry():
    sentinel = MagicMock()
    ctx = _ctx(registry=sentinel)
    assert shared.get_registry(ctx) is sentinel


def test_get_multi_config_returns_lifespan_config():
    sentinel = MagicMock()
    ctx = _ctx(multi_config=sentinel)
    assert shared.get_multi_config(ctx) is sentinel


def test_get_enterprise_settings_returns_settings():
    settings = MagicMock(name="enterprise_settings")
    multi_config = MagicMock()
    multi_config.enterprise = MagicMock()
    multi_config.enterprise.settings = settings
    ctx = _ctx(multi_config=multi_config)
    assert shared.get_enterprise_settings(ctx) is settings


def test_get_enterprise_settings_raises_when_enterprise_none():
    """Defensive guard required by _python-coding-practices rule 11.

    The registration-time gate in ``server._register_tools`` makes this
    branch unreachable in normal operation, but the guard must still be
    exercised by a unit test that constructs the bypass.
    """
    multi_config = MagicMock()
    multi_config.enterprise = None
    ctx = _ctx(multi_config=multi_config)
    with pytest.raises(
        InternalError, match="get_enterprise_settings called without enterprise"
    ):
        shared.get_enterprise_settings(ctx)


def test_get_community_settings_returns_settings():
    settings = MagicMock(name="community_settings")
    multi_config = MagicMock()
    multi_config.community = MagicMock()
    multi_config.community.settings = settings
    ctx = _ctx(multi_config=multi_config)
    assert shared.get_community_settings(ctx) is settings


def test_get_community_settings_raises_when_community_none():
    """Defensive guard required by _python-coding-practices rule 11."""
    multi_config = MagicMock()
    multi_config.community = None
    ctx = _ctx(multi_config=multi_config)
    with pytest.raises(
        InternalError, match="get_community_settings called without community"
    ):
        shared.get_community_settings(ctx)


def test_get_community_registry_returns_child():
    community = MagicMock()
    registry = MagicMock(community=community)
    ctx = _ctx(registry=registry)
    assert shared.get_community_registry(ctx) is community


def test_get_community_registry_raises_when_absent():
    registry = MagicMock(community=None)
    ctx = _ctx(registry=registry)
    with pytest.raises(InternalError, match="No community sessions are configured"):
        shared.get_community_registry(ctx)


def test_get_enterprise_registry_returns_child():
    prod = MagicMock()
    registry = MagicMock(enterprise_systems={"prod": prod})
    ctx = _ctx(registry=registry)
    assert shared.get_enterprise_registry(ctx, "prod") is prod


def test_get_enterprise_registry_unknown_system_raises():
    registry = MagicMock(enterprise_systems={"prod": MagicMock()})
    ctx = _ctx(registry=registry)
    with pytest.raises(InvalidSessionNameError, match="not configured"):
        shared.get_enterprise_registry(ctx, "stage")


# ---------------------------------------------------------------------------
# Identifier parsers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "session_id,expected",
    [
        ("community:community:local", (SystemType.COMMUNITY, "community", "local")),
        (
            "enterprise:prod:my-pq",
            (SystemType.ENTERPRISE, "prod", "my-pq"),
        ),
    ],
)
def test_parse_session_id_valid(session_id, expected):
    assert shared.parse_session_id(session_id) == expected


def test_parse_session_id_unknown_type_raises():
    with pytest.raises(InvalidSessionNameError, match="unsupported type"):
        shared.parse_session_id("warp:source:name")


def test_parse_session_id_malformed_raises():
    with pytest.raises(InvalidSessionNameError):
        shared.parse_session_id("not-a-real-id")


@pytest.mark.parametrize(
    "pq_id,expected",
    [
        ("prod:0", ("prod", 0)),
        ("prod:42", ("prod", 42)),
        ("dev:9999", ("dev", 9999)),
    ],
)
def test_parse_pq_id_valid(pq_id, expected):
    assert shared.parse_pq_id(pq_id) == expected


@pytest.mark.parametrize(
    "pq_id",
    [
        "prod",
        "prod:",
        ":42",
        "prod:42:extra",
    ],
)
def test_parse_pq_id_bad_shape(pq_id):
    with pytest.raises(InvalidSessionNameError, match="must be of the form"):
        shared.parse_pq_id(pq_id)


def test_parse_pq_id_non_integer_serial_raises():
    with pytest.raises(InvalidSessionNameError, match="non-integer serial"):
        shared.parse_pq_id("prod:abc")


def test_parse_pq_id_negative_serial_raises():
    with pytest.raises(InvalidSessionNameError, match="negative serial"):
        shared.parse_pq_id("prod:-1")


def test_parse_session_id_returns_named_tuple():
    """Result also exposes attribute access and is a ParsedSessionId."""
    parsed = shared.parse_session_id("community:community:foo")
    assert isinstance(parsed, shared.ParsedSessionId)
    assert parsed.system_name == "community"
    assert parsed.name == "foo"
    # Tuple unpacking still works.
    a, b, c = parsed
    assert (b, c) == ("community", "foo")


def test_parse_pq_id_returns_named_tuple():
    parsed = shared.parse_pq_id("prod:7")
    assert isinstance(parsed, shared.ParsedPqId)
    assert parsed.system_name == "prod"
    assert parsed.serial == 7
    # Tuple unpacking still works.
    s, n = parsed
    assert (s, n) == ("prod", 7)


def test_resolve_pq_ids_to_single_system_happy_path():
    sys_name, serials = shared.resolve_pq_ids_to_single_system(["prod:1", "prod:2"])
    assert sys_name == "prod"
    assert serials == [1, 2]


def test_resolve_pq_ids_to_single_system_rejects_empty():
    with pytest.raises(InvalidSessionNameError):
        shared.resolve_pq_ids_to_single_system([])


def test_resolve_pq_ids_to_single_system_rejects_mixed_systems():
    with pytest.raises(InvalidSessionNameError, match="same"):
        shared.resolve_pq_ids_to_single_system(["prod:1", "staging:2"])


def test_resolve_pq_ids_to_single_system_propagates_parse_errors():
    with pytest.raises(InvalidSessionNameError):
        shared.resolve_pq_ids_to_single_system(["prod:abc"])


# ---------------------------------------------------------------------------
# Session retrieval
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_session_from_context_returns_session():
    expected_session = MagicMock(spec=BaseSession)
    manager = MagicMock()
    manager.get = AsyncMock(return_value=expected_session)

    registry = MagicMock()
    registry.get = AsyncMock(return_value=manager)

    ctx = _ctx(registry=registry)
    out = await shared.get_session_from_context(
        "toolname", ctx, "community:community:s"
    )

    assert out is expected_session
    registry.get.assert_awaited_once_with("community:community:s")


@pytest.mark.asyncio
async def test_get_enterprise_session_success():
    enterprise_session = MagicMock(spec=CorePlusSession)
    manager = MagicMock(get=AsyncMock(return_value=enterprise_session))
    registry = MagicMock(get=AsyncMock(return_value=manager))

    ctx = _ctx(registry=registry)
    sess, err = await shared.get_enterprise_session("tool", ctx, "enterprise:prod:s")
    assert sess is enterprise_session
    assert err is None


@pytest.mark.asyncio
async def test_get_enterprise_session_rejects_non_enterprise():
    community_session = MagicMock(spec=BaseSession)  # not a CorePlusSession
    manager = MagicMock(get=AsyncMock(return_value=community_session))
    registry = MagicMock(get=AsyncMock(return_value=manager))

    ctx = _ctx(registry=registry)
    sess, err = await shared.get_enterprise_session(
        "tool", ctx, "community:community:s"
    )
    assert sess is None
    assert err is not None
    assert err["success"] is False
    assert "enterprise" in err["error"].lower()


@pytest.mark.asyncio
async def test_get_enterprise_session_propagates_lookup_error():
    registry = MagicMock(get=AsyncMock(side_effect=RuntimeError("nope")))
    ctx = _ctx(registry=registry)

    sess, err = await shared.get_enterprise_session(
        "tool", ctx, "enterprise:prod:missing"
    )
    assert sess is None
    assert err is not None
    assert "Failed to get session" in err["error"]


# ---------------------------------------------------------------------------
# Response size guards
# ---------------------------------------------------------------------------


def _default_limits():
    from deephaven_mcp.mcp_systems_server._tools._response_limits import ResponseLimits

    return ResponseLimits()


def test_check_response_size_under_warning_returns_none():
    assert shared.check_response_size("t", 1_000, _default_limits()) is None


def test_check_response_size_in_warning_band_returns_none(caplog):
    caplog.set_level("WARNING")
    limits = _default_limits()
    assert (
        shared.check_response_size("t", limits.warning_response_bytes + 1, limits)
        is None
    )
    assert any("Large response" in rec.message for rec in caplog.records)


def test_check_response_size_above_max_returns_error():
    limits = _default_limits()
    err = shared.check_response_size("t", limits.max_response_bytes + 1, limits)
    assert err is not None
    assert err["success"] is False


def test_get_response_limits_routes_to_enterprise():
    """An enterprise session id routes to enterprise.settings.response_limits."""
    enterprise_limits = MagicMock(name="enterprise_response_limits")
    community_limits = MagicMock(name="community_response_limits")
    multi_config = MagicMock()
    multi_config.enterprise = MagicMock()
    multi_config.enterprise.settings.response_limits = enterprise_limits
    multi_config.community = MagicMock()
    multi_config.community.settings.response_limits = community_limits
    ctx = _ctx(multi_config=multi_config)
    assert (
        shared.get_response_limits(ctx, "enterprise:prod:session1") is enterprise_limits
    )


def test_get_response_limits_routes_to_community():
    """A community session id routes to community.settings.response_limits."""
    enterprise_limits = MagicMock(name="enterprise_response_limits")
    community_limits = MagicMock(name="community_response_limits")
    multi_config = MagicMock()
    multi_config.enterprise = MagicMock()
    multi_config.enterprise.settings.response_limits = enterprise_limits
    multi_config.community = MagicMock()
    multi_config.community.settings.response_limits = community_limits
    ctx = _ctx(multi_config=multi_config)
    assert (
        shared.get_response_limits(ctx, "community:community:session1")
        is community_limits
    )


def test_get_response_limits_raises_on_unhandled_system_type(monkeypatch):
    """If parsed.system_type is not a known SystemType member, the router raises InternalError.

    Required by ``feedback_no_asserts_in_production``: every defensive
    raise in production code must have a unit test that triggers it.
    Future-proofs the router against ``SystemType`` gaining a new
    member that the routing code hasn't been taught about.
    """
    from deephaven_mcp.mcp_systems_server._tools.shared import ParsedSessionId

    sentinel_system_type = "unhandled-future-type"
    fake_parsed = ParsedSessionId(
        system_type=sentinel_system_type,  # type: ignore[arg-type]
        system_name="x",
        name="y",
    )
    monkeypatch.setattr(shared, "parse_session_id", lambda _sid: fake_parsed)

    ctx = _ctx(multi_config=MagicMock())
    with pytest.raises(InternalError, match="Unhandled SystemType"):
        shared.get_response_limits(ctx, "ignored:by:fake-parser")


# ---------------------------------------------------------------------------
# Table formatters
# ---------------------------------------------------------------------------


def _arrow_table() -> pa.Table:
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def test_format_meta_table_result_no_namespace():
    out = shared.format_meta_table_result(_arrow_table(), "T")
    assert out["success"] is True
    assert out["table"] == "T"
    assert out["row_count"] == 3
    assert "namespace" not in out


def test_format_meta_table_result_with_namespace():
    out = shared.format_meta_table_result(_arrow_table(), "T", namespace="ns")
    assert out["namespace"] == "ns"


def test_build_table_data_response_minimal():
    out = shared.build_table_data_response(
        _arrow_table(), is_complete=True, format="json-row"
    )
    assert out["success"] is True
    assert out["row_count"] == 3
    assert out["is_complete"] is True
    assert "namespace" not in out and "table_name" not in out


def test_build_table_data_response_with_namespace_and_name():
    out = shared.build_table_data_response(
        _arrow_table(),
        is_complete=False,
        format="json-row",
        table_name="T",
        namespace="ns",
    )
    assert out["table_name"] == "T"
    assert out["namespace"] == "ns"
    assert out["is_complete"] is False


# ---------------------------------------------------------------------------
# JSON redaction
# ---------------------------------------------------------------------------


def test_redact_json_sensitive_fields_none_returns_none():
    assert shared.redact_json_sensitive_fields(None) is None
    assert shared.redact_json_sensitive_fields("") is None


def test_redact_json_sensitive_fields_unparseable():
    assert shared.redact_json_sensitive_fields("not json") == "[UNPARSEABLE]"


def test_redact_json_sensitive_fields_redacts_known_keys():
    import json

    raw = json.dumps(
        {
            "password": "secret-pw",
            "token": "abc",
            "nested": {"api_key": "k", "ok": "keep-me"},
            "items": [{"secret": "s", "x": 1}],
        }
    )
    out = shared.redact_json_sensitive_fields(raw)
    assert out is not None
    parsed = json.loads(out)
    assert parsed["password"] == "[REDACTED]"
    assert parsed["token"] == "[REDACTED]"
    assert parsed["nested"]["api_key"] == "[REDACTED]"
    assert parsed["nested"]["ok"] == "keep-me"
    assert parsed["items"][0]["secret"] == "[REDACTED]"
    assert parsed["items"][0]["x"] == 1


# ---------------------------------------------------------------------------
# check_session_limit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_session_limit_returns_none_when_cap_disabled():
    registry = MagicMock()
    registry.count_added_sessions = AsyncMock(return_value=99)
    result = await shared.check_session_limit(
        registry,
        SystemType.COMMUNITY,
        "community",
        None,
        "session_community_create",
        "Session limit reached: {current}/{max} sessions active",
    )
    assert result is None
    registry.count_added_sessions.assert_not_called()


@pytest.mark.asyncio
async def test_check_session_limit_returns_none_when_under_cap():
    registry = MagicMock()
    registry.count_added_sessions = AsyncMock(return_value=1)
    result = await shared.check_session_limit(
        registry,
        SystemType.ENTERPRISE,
        "prod",
        5,
        "_check_session_limit",
        "Max concurrent sessions ({max}) reached for system 'prod'",
    )
    assert result is None
    registry.count_added_sessions.assert_awaited_once_with(
        SystemType.ENTERPRISE, "prod"
    )


@pytest.mark.asyncio
async def test_check_session_limit_returns_error_when_at_cap():
    registry = MagicMock()
    registry.count_added_sessions = AsyncMock(return_value=5)
    result = await shared.check_session_limit(
        registry,
        SystemType.ENTERPRISE,
        "prod",
        5,
        "_check_session_limit",
        "Max concurrent sessions ({max}) reached for system 'prod'",
    )
    assert result is not None
    assert result["success"] is False
    assert result["isError"] is True
    assert "Max concurrent sessions (5) reached for system 'prod'" in result["error"]


@pytest.mark.asyncio
async def test_check_session_limit_formats_current_and_max():
    registry = MagicMock()
    registry.count_added_sessions = AsyncMock(return_value=7)
    result = await shared.check_session_limit(
        registry,
        SystemType.COMMUNITY,
        "community",
        5,
        "session_community_create",
        "Session limit reached: {current}/{max} sessions active",
    )
    assert result is not None
    assert "Session limit reached: 7/5 sessions active" in result["error"]
