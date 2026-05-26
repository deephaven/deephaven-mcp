"""Shared test fixtures and helpers for mcp_systems_server tool tests.

The multiplexed systems server reads its configuration once at startup
and yields a single ``LifespanContext`` to every tool invocation. The
mocks below mirror that shape so individual test modules can drop in
their own ``MultiSystemRegistry`` and ``MultiSystemConfig`` fakes
without re-implementing the plumbing.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock


def _adapt_lifespan_context(ctx: Any) -> Any:
    """Translate a legacy single-registry lifespan dict into the new shape.

    Many test modules predate the multiplexed server and pass a
    community-only ``CommunitySessionRegistry`` mock as the ``registry``
    entry. The new tool helpers expect a ``MultiSystemRegistry`` exposing
    a ``community`` attribute (and an ``enterprise_systems`` mapping).

    When a test supplies a plain dict, this helper:

    - Wraps the supplied ``registry`` value in a stand-in
      ``MultiSystemRegistry`` mock whose ``community`` attribute is the
      original registry and whose ``enterprise_systems`` defaults to an
      empty mapping (tests that need enterprise systems should pass a
      ready-made ``MultiSystemRegistry`` mock instead).
    - Ensures a ``multi_config`` entry exists so helpers that read
      community settings find something sensible (an empty
      ``CommunityConfig.raw`` dict by default).

    Tests that already supply a ``MultiSystemRegistry``-shaped object
    (i.e. one with a ``community`` attribute) are passed through
    unchanged.
    """
    if not isinstance(ctx, dict):
        return ctx

    adapted = dict(ctx)
    registry = adapted.get("registry")
    # Only adapt when the registry is a Mock-shaped child registry.
    # Plain sentinel values (strings, raw MagicMocks supplied by tests
    # that exercise the lifespan helpers directly) must pass through
    # unchanged; adaptation here is purely a back-compat shim for tool
    # tests that predate the multiplexed server.
    spec_name = (
        registry._spec_class.__name__
        if isinstance(registry, MagicMock) and registry._spec_class is not None
        else ""
    )
    is_community = "Community" in spec_name and not hasattr(type(registry), "community")
    is_enterprise = "Enterprise" in spec_name and not hasattr(
        type(registry), "enterprise_systems"
    )
    if is_community or is_enterprise:
        multi = MagicMock()
        if is_community:
            multi.community = registry
            multi.enterprise_systems = {}
        else:
            multi.community = None
            # Look up the system name from the registry's own attribute
            # (every enterprise registry mock sets ``system_name`` to a
            # known value); fall back to ``"system"`` for older fixtures.
            system_name = getattr(registry, "system_name", None)
            if not isinstance(system_name, str):
                system_name = "system"
            multi.enterprise_systems = {system_name: registry}
        # The multi-registry's read API (``get`` / ``get_all`` / ``remove``)
        # forwards directly to the wrapped child for tests that configure
        # those methods on the original registry. Sharing the attributes
        # keeps the legacy single-registry assertions working against the
        # new shape without test rewrites.
        for _attr in ("get", "get_all", "remove"):
            inner_attr = getattr(registry, _attr, None)
            if inner_attr is not None:
                setattr(multi, _attr, inner_attr)
        adapted["registry"] = multi

    # ``community_settings`` is a test-only shortcut: tests that want the
    # production code to observe a populated ``community/settings.json``
    # block can pass the dict here without constructing a full
    # ``MultiSystemConfig``. We pop it from the lifespan dict (it is not
    # part of the real shape) and route it into ``multi_config``.
    community_settings = adapted.pop("community_settings", None)
    # Convenience for tests that stash the settings dict on their
    # registry mock as ``registry._community_settings``: pick it up
    # automatically so each create-test does not have to thread it
    # through the lifespan dict by hand.
    if community_settings is None and registry is not None:
        stashed = getattr(registry, "_community_settings", None)
        if isinstance(stashed, dict):
            community_settings = stashed

    if "multi_config" not in adapted:
        from deephaven_mcp.mcp_systems_server._tools._pq_config import PqToolsConfig
        from deephaven_mcp.mcp_systems_server._tools._response_limits import (
            ResponseLimits,
        )
        from deephaven_mcp.mcp_systems_server.config import CommunitySettings

        multi_config = MagicMock()
        community = MagicMock()
        # The community/settings.json content; the new tool code reads
        # ``multi_config.community.settings`` as a typed
        # :class:`CommunitySettings` model. Validate the supplied dict
        # into the real model so attribute access matches production.
        settings_dict = community_settings if community_settings is not None else {}
        community.settings = CommunitySettings.model_validate(settings_dict)
        community.sessions = {}
        multi_config.community = community
        multi_config.server = MagicMock()
        # ``pq_tools`` lives on ``enterprise.settings`` (the PQ tools
        # are enterprise-only); we install a real :class:`PqToolsConfig`
        # below so tools that read ``default_max_concurrent`` and use it
        # in arithmetic/comparisons see schema defaults rather than a
        # MagicMock attribute.
        pq_tools_default = PqToolsConfig()
        # ``response_limits`` is the operator-tunable response-size
        # guard block. Tests that exercise ``check_response_size`` read
        # it through ``get_response_limits``; install a real
        # default-valued :class:`ResponseLimits` so attribute access
        # returns numeric thresholds rather than ``MagicMock`` objects.
        response_limits_default = ResponseLimits()

        # Build the enterprise tree from the wrapped enterprise registry,
        # if any. Legacy enterprise tool tests set
        # ``mock_config_manager.get_config = AsyncMock(return_value=<flat>)``
        # to expose the system's wire-format config dict; pull that value
        # out and arrange for ``system_cfg.model_dump(...)`` to return
        # it so the new tools see the same shape.
        if is_enterprise:
            from deephaven_mcp.sessions import EnterpriseSystemConfig

            cfg_mgr = adapted.get("config_manager")
            raw_dict: dict | None = None
            get_config = (
                getattr(cfg_mgr, "get_config", None) if cfg_mgr is not None else None
            )
            if get_config is not None:
                candidate = getattr(get_config, "return_value", None)
                if isinstance(candidate, dict):
                    raw_dict = candidate
            # Validate the supplied wire-format dict into a real
            # :class:`EnterpriseSystemConfig`. The ``name`` field is
            # the filename stem; carry the test-fixture's system name
            # through so model_validate has the required key.
            payload = dict(raw_dict) if raw_dict is not None else {}
            payload.setdefault("name", system_name)
            # Legacy fixtures predate the discriminated ``auth``
            # block and pass flat ``auth_type`` / ``username`` /
            # ``password`` keys. Migrate them to the current
            # ``auth.credentials`` wire format on the fly so the
            # model validates without rewriting every test.
            if "auth" not in payload and "auth_type" in payload:
                auth_type = payload.pop("auth_type")
                creds: dict[str, Any] = {"type": auth_type}
                for k in ("username", "password", "token", "effective_user"):
                    if k in payload:
                        creds[k] = payload.pop(k)
                payload["auth"] = {"credentials": creds}
            # Fill in connection_json_url and auth when missing so the
            # model validates. Legacy bare-bones fixtures omit these
            # because the older code consumed the same dict via
            # ``.get()`` and tolerated missing keys.
            payload.setdefault(
                "connection_json_url",
                "https://test.example.com/iris/connection.json",
            )
            payload.setdefault(
                "auth",
                {
                    "credentials": {
                        "type": "password",
                        "username": "tester",
                        "password": "secret",
                    }
                },
            )
            try:
                system_cfg = EnterpriseSystemConfig.model_validate(payload)
            except Exception:
                # Fall back to a Mock for tests that intentionally
                # exercise pre-validation paths (e.g. they trigger
                # downstream errors without supplying a full schema).
                system_cfg = MagicMock(spec=EnterpriseSystemConfig)
                system_cfg.name = system_name
                # Surface ``session_creation`` from the raw payload so
                # tools that read it via attribute access still see the
                # legacy shape during fallback.
                session_creation_payload = payload.get("session_creation")
                if isinstance(session_creation_payload, dict):
                    sc = MagicMock()
                    sc.max_concurrent_sessions = session_creation_payload.get(
                        "max_concurrent_sessions", 5
                    )
                    defaults_payload = session_creation_payload.get("defaults") or {}
                    sc.defaults = MagicMock(**defaults_payload)
                    system_cfg.session_creation = sc
                else:
                    system_cfg.session_creation = None
                system_cfg.credentials = MagicMock()
            enterprise_group = MagicMock()
            enterprise_group.systems = {system_name: system_cfg}
            enterprise_group.settings = MagicMock()
            enterprise_group.settings.pq_tools = pq_tools_default
            enterprise_group.settings.response_limits = response_limits_default
            multi_config.enterprise = enterprise_group
        else:
            # No specced enterprise registry; install a minimal
            # enterprise stub so tools that route to
            # ``multi_config.enterprise.settings.response_limits`` (the
            # new response-size guard accessor) work for tests that
            # pass an ``enterprise:...`` session id without explicitly
            # wiring an enterprise tree. Tests that intentionally
            # exercise the "no enterprise configured" path can set
            # ``multi_config.enterprise = None`` after construction.
            _ = pq_tools_default
            enterprise_stub = MagicMock()
            enterprise_stub.systems = {}
            enterprise_stub.settings = MagicMock()
            enterprise_stub.settings.pq_tools = pq_tools_default
            enterprise_stub.settings.response_limits = response_limits_default
            multi_config.enterprise = enterprise_stub
        # Also install ``response_limits`` on the community side; the
        # validated :class:`CommunitySettings` above already carries a
        # real :class:`ResponseLimits` instance, so no further work is
        # required there.
        adapted["multi_config"] = multi_config

    return adapted


class MockRequestContext:
    """Mock MCP request context for testing."""

    def __init__(self, lifespan_context: object) -> None:
        self.lifespan_context = _adapt_lifespan_context(lifespan_context)
        # The MCP request handle is unused by the new tool helpers (no
        # per-request auth), but FastMCP exposes it on the context so we
        # keep a lightweight stub for tests that read it.
        self.request = MagicMock()


class MockContext:
    """Mock MCP context for testing."""

    def __init__(self, lifespan_context: object) -> None:
        self.request_context = MockRequestContext(lifespan_context)


def create_mock_instance_tracker() -> MagicMock:
    """Create a mock InstanceTracker for tests."""
    mock_tracker = MagicMock()
    mock_tracker.instance_id = "test-instance-id"
    mock_tracker.track_python_process = AsyncMock()
    mock_tracker.untrack_python_process = AsyncMock()
    return mock_tracker


def stub_session_config(name: str = "test-session", **overrides):
    """Return a valid anonymous-auth ``CommunitySessionConfig`` stub.

    The typed community-session APIs (managers, registries, and the
    dynamic-session tool) accept a fully-validated declaration. Tests
    that previously passed arbitrary dicts use this helper when the
    dict contents are not inspected by the assertions.
    """
    from deephaven_mcp.sessions import CommunitySessionConfig

    payload: dict = {
        "name": name,
        "auth": {"credentials": {"type": "anonymous"}},
    }
    payload.update(overrides)
    return CommunitySessionConfig.model_validate(payload)
