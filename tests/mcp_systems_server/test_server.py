"""Tests for ``deephaven_mcp.mcp_systems_server.server``.

Covers the CLI surface that boots the multiplexed systems server:

- ``_parse_args``: argparse defaults (all CLI overrides default to
  ``None`` so the JSON-loaded :class:`ServerConfig` provides the
  effective value per field) and explicit values.
- ``_parse_config_dir_arg``: explicit Path vs deferred (``None``).
- ``_is_loopback_host``: classification of literal IPs, hostname
  resolution, and unresolvable inputs.
- ``_load_multi_config_or_exit``: returns the full ``MultiSystemConfig``
  (server, community, enterprise) loaded once and exits on
  ConfigurationError.
- ``_resolve_psk_or_exit``: success, missing PSK.
- ``main``: stdio path, HTTP path with PSK, and the non-loopback / no-PSK
  refusal paths.
"""

from __future__ import annotations

import socket
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.mcp_systems_server import server as server_module
from deephaven_mcp.mcp_systems_server.config import ServerConfig
from deephaven_mcp.mcp_systems_server.server import (
    _is_loopback_host,
    _load_multi_config_or_exit,
    _parse_args,
    _parse_config_dir_arg,
    _resolve_psk_or_exit,
    main,
)

# ---------------------------------------------------------------------------
# _parse_args
# ---------------------------------------------------------------------------


def test_parse_args_defaults():
    """All CLI overrides default to ``None`` so the JSON config provides them."""
    ns = _parse_args([])
    assert ns.transport is None
    assert ns.host is None
    assert ns.port is None
    assert ns.config_dir is None
    assert ns.psk is None


def test_parse_args_psk_flag():
    ns = _parse_args(["--psk", "abc"])
    assert ns.psk == "abc"


def test_parse_args_explicit_values():
    ns = _parse_args(
        [
            "--transport",
            "http",
            "--host",
            "::1",
            "--port",
            "9001",
            "--config-dir",
            "/tmp/cfg",
        ]
    )
    assert ns.transport == "http"
    assert ns.host == "::1"
    assert ns.port == 9001
    assert ns.config_dir == "/tmp/cfg"


def test_parse_args_rejects_unknown_transport():
    with pytest.raises(SystemExit):
        _parse_args(["--transport", "sse"])


# ---------------------------------------------------------------------------
# _parse_config_dir_arg
# ---------------------------------------------------------------------------


def test_parse_config_dir_arg_none_passthrough():
    assert _parse_config_dir_arg(None) is None


def test_parse_config_dir_arg_returns_absolute_path(tmp_path):
    rel = tmp_path / "cfg"
    rel.mkdir()
    out = _parse_config_dir_arg(str(rel))
    assert isinstance(out, Path)
    assert out.is_absolute()
    assert out == rel.resolve()


# ---------------------------------------------------------------------------
# _is_loopback_host
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "host", ["localhost", "LOCALHOST", "127.0.0.1", "127.5.6.7", "::1"]
)
def test_is_loopback_host_accepts_loopback(host):
    assert _is_loopback_host(host) is True


@pytest.mark.parametrize("host", ["10.0.0.1", "8.8.8.8", "2001:db8::1"])
def test_is_loopback_host_rejects_public_ips(host):
    assert _is_loopback_host(host) is False


def test_is_loopback_host_unresolvable_hostname_is_false():
    with patch.object(socket, "getaddrinfo", side_effect=socket.gaierror):
        assert _is_loopback_host("definitely-not-a-real-host") is False


def test_is_loopback_host_resolves_hostname_to_loopback():
    fake_resolution = [(socket.AF_INET, 0, 0, "", ("127.0.0.1", 0))]
    with patch.object(socket, "getaddrinfo", return_value=fake_resolution):
        assert _is_loopback_host("my-loopback-alias") is True


def test_is_loopback_host_mixed_resolution_is_false():
    fake_resolution = [
        (socket.AF_INET, 0, 0, "", ("127.0.0.1", 0)),
        (socket.AF_INET, 0, 0, "", ("8.8.8.8", 0)),
    ]
    with patch.object(socket, "getaddrinfo", return_value=fake_resolution):
        assert _is_loopback_host("dual-host") is False


# ---------------------------------------------------------------------------
# _load_multi_config_or_exit
# ---------------------------------------------------------------------------


def _multi_config_with(
    server_cfg: ServerConfig | None, config_dir: Path | None = None
) -> MagicMock:
    """Build a MultiSystemConfig mock with ``cfg.server`` and ``cfg.config_dir`` set."""
    multi = MagicMock()
    multi.server = server_cfg
    multi.config_dir = config_dir if config_dir is not None else Path("/tmp/cfg")
    return multi


@pytest.mark.asyncio
async def test_load_multi_config_returns_loaded_config(tmp_path):
    loaded = ServerConfig.model_validate({"psk": "hunter2", "port": 9100})
    multi = _multi_config_with(loaded, tmp_path)
    with patch.object(
        server_module,
        "MultiSystemConfigManager",
        MagicMock(
            return_value=MagicMock(
                initialize=AsyncMock(return_value=multi),
            )
        ),
    ):
        result = await _load_multi_config_or_exit(tmp_path)
    assert result is multi
    assert result.server is loaded
    assert result.config_dir == tmp_path


@pytest.mark.asyncio
async def test_load_multi_config_returns_config_with_no_server(tmp_path):
    """When ``server.json`` is absent, ``multi.server`` is ``None``; main() supplies defaults."""
    multi = _multi_config_with(None, tmp_path)
    with patch.object(
        server_module,
        "MultiSystemConfigManager",
        MagicMock(
            return_value=MagicMock(
                initialize=AsyncMock(return_value=multi),
            )
        ),
    ):
        result = await _load_multi_config_or_exit(tmp_path)
    assert result.server is None
    assert result.config_dir == tmp_path


@pytest.mark.asyncio
async def test_load_multi_config_exits_on_configuration_error():
    with (
        patch.object(
            server_module,
            "MultiSystemConfigManager",
            MagicMock(
                return_value=MagicMock(
                    initialize=AsyncMock(side_effect=ConfigurationError("bad config"))
                )
            ),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        await _load_multi_config_or_exit(None)
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# _resolve_psk_or_exit
# ---------------------------------------------------------------------------


def test_resolve_psk_or_exit_cli_flag_wins(tmp_path):
    """CLI ``--psk`` takes precedence over ``server.json``'s ``psk`` field."""
    server_cfg = ServerConfig.model_validate({"psk": "from-server-json"})
    assert _resolve_psk_or_exit("from-cli", server_cfg, tmp_path) == "from-cli"


def test_resolve_psk_or_exit_uses_server_json_psk(tmp_path):
    server_cfg = ServerConfig.model_validate({"psk": "hunter2"})
    assert _resolve_psk_or_exit(None, server_cfg, tmp_path) == "hunter2"


def test_resolve_psk_or_exit_missing_psk_exits(tmp_path):
    """A :class:`ServerConfig` without ``psk`` causes the entry point to exit."""
    server_cfg = ServerConfig()
    with pytest.raises(SystemExit) as exc_info:
        _resolve_psk_or_exit(None, server_cfg, tmp_path)
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# main — transport selection
# ---------------------------------------------------------------------------


@pytest.fixture
def _mute_logging_setup():
    """Suppress the boot-time logging/signal/uvicorn helpers under main()."""
    patches = [
        patch.object(server_module, "setup_logging"),
        patch.object(server_module, "setup_global_exception_logging"),
        patch.object(server_module, "setup_signal_handler_logging"),
        patch.object(server_module, "monkeypatch_uvicorn_exception_handling"),
    ]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


def _patch_load_server_config(server_cfg: ServerConfig, config_dir: Path | None = None):
    """Patch ``_load_multi_config_or_exit`` to return a MultiSystemConfig mock."""
    multi = _multi_config_with(server_cfg, config_dir)
    return patch.object(
        server_module,
        "_load_multi_config_or_exit",
        AsyncMock(return_value=multi),
    )


def test_main_stdio_path_from_cli_flag(_mute_logging_setup):
    fake_server = MagicMock()
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(
            server_module, "_build_fastmcp", return_value=fake_server
        ) as mock_build,
        patch.object(server_module, "_run_stdio") as mock_stdio,
        patch.object(server_module, "_run_http") as mock_http,
    ):
        main(["--transport", "stdio"])
    mock_build.assert_called_once()
    mock_stdio.assert_called_once_with(fake_server)
    mock_http.assert_not_called()


def test_main_stdio_path_from_server_json(_mute_logging_setup):
    """`transport="stdio"` from ``server.json`` is honored when CLI omits ``--transport``."""
    fake_server = MagicMock()
    with (
        _patch_load_server_config(ServerConfig.model_validate({"transport": "stdio"})),
        patch.object(server_module, "_build_fastmcp", return_value=fake_server),
        patch.object(server_module, "_run_stdio") as mock_stdio,
        patch.object(server_module, "_run_http") as mock_http,
    ):
        main([])
    mock_stdio.assert_called_once_with(fake_server)
    mock_http.assert_not_called()


def test_main_http_path_calls_run_http_with_cli_overrides(_mute_logging_setup):
    fake_server = MagicMock()
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(server_module, "_is_loopback_host", return_value=True),
        patch.object(
            server_module, "_resolve_psk_or_exit", MagicMock(return_value="pw")
        ),
        patch.object(server_module, "_build_fastmcp", return_value=fake_server),
        patch.object(server_module, "_run_http") as mock_http,
        patch.object(server_module, "_run_stdio") as mock_stdio,
    ):
        main(["--transport", "http", "--host", "127.0.0.1", "--port", "8765"])
    mock_http.assert_called_once_with(
        fake_server, host="127.0.0.1", port=8765, psk="pw"
    )
    mock_stdio.assert_not_called()


def test_main_http_path_uses_server_json_values(_mute_logging_setup):
    """With no CLI overrides, host/port/psk come from ``server.json``."""
    fake_server = MagicMock()
    server_cfg = ServerConfig.model_validate(
        {"transport": "http", "host": "::1", "port": 9000, "psk": "json-psk"}
    )
    with (
        _patch_load_server_config(server_cfg),
        patch.object(server_module, "_is_loopback_host", return_value=True),
        patch.object(server_module, "_build_fastmcp", return_value=fake_server),
        patch.object(server_module, "_run_http") as mock_http,
    ):
        main([])
    mock_http.assert_called_once_with(
        fake_server, host="::1", port=9000, psk="json-psk"
    )


def test_main_http_refuses_non_loopback_host(_mute_logging_setup):
    with (
        _patch_load_server_config(ServerConfig()),
        patch.object(server_module, "_is_loopback_host", return_value=False),
        pytest.raises(SystemExit) as exc_info,
    ):
        main(["--transport", "http", "--host", "0.0.0.0"])
    assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# _register_health_endpoint / _register_tools / _build_fastmcp
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_health_endpoint_returns_ok():
    """The /health route handler returns ``{"status": "ok"}`` with HTTP 200."""
    captured: dict[str, object] = {}

    def _custom_route(path, methods):
        captured["path"] = path
        captured["methods"] = methods

        def decorator(fn):
            captured["fn"] = fn
            return fn

        return decorator

    fake_server = MagicMock()
    fake_server.custom_route = _custom_route
    server_module._register_health_endpoint(fake_server)

    # Route registered with the documented path + GET method.
    from deephaven_mcp._health import HEALTH_PATH

    assert captured["path"] == HEALTH_PATH
    assert captured["methods"] == ["GET"]

    # Invoke the handler with a dummy request; it must return a JSON 200.
    response = await captured["fn"](MagicMock())
    assert response.status_code == 200
    # The body is a JSONResponse; decoding the content payload is enough.
    import json as _json

    assert _json.loads(response.body) == {"status": "ok"}


def _multi_config_with_sections(
    *, community: object | None, enterprise: object | None
) -> MagicMock:
    """Build a MultiSystemConfig-shaped mock with explicit ``community``/``enterprise``."""
    multi = MagicMock()
    multi.community = community
    multi.enterprise = enterprise
    return multi


def test_register_tools_registers_all_modules_when_both_loaded():
    """When both community and enterprise sections are loaded, every tool module registers."""
    fake_server = MagicMock()
    multi = _multi_config_with_sections(
        community=MagicMock(name="community"),
        enterprise=MagicMock(name="enterprise"),
    )
    with (
        patch.object(server_module, "session") as m_session,
        patch.object(server_module, "table") as m_table,
        patch.object(server_module, "script") as m_script,
        patch.object(server_module, "session_community") as m_sc,
        patch.object(server_module, "session_enterprise") as m_se,
        patch.object(server_module, "catalog") as m_catalog,
        patch.object(server_module, "pq") as m_pq,
    ):
        server_module._register_tools(fake_server, multi)
    for m in (m_session, m_table, m_script, m_sc, m_se, m_catalog, m_pq):
        m.register_tools.assert_called_once_with(fake_server)


def test_register_tools_skips_enterprise_modules_on_community_only():
    """A community-only deployment must not expose enterprise tools."""
    fake_server = MagicMock()
    multi = _multi_config_with_sections(
        community=MagicMock(name="community"), enterprise=None
    )
    with (
        patch.object(server_module, "session") as m_session,
        patch.object(server_module, "table") as m_table,
        patch.object(server_module, "script") as m_script,
        patch.object(server_module, "session_community") as m_sc,
        patch.object(server_module, "session_enterprise") as m_se,
        patch.object(server_module, "catalog") as m_catalog,
        patch.object(server_module, "pq") as m_pq,
    ):
        server_module._register_tools(fake_server, multi)
    # Always-on tools register.
    for m in (m_session, m_table, m_script, m_sc):
        m.register_tools.assert_called_once_with(fake_server)
    # Enterprise tools are gated off.
    for m in (m_se, m_catalog, m_pq):
        m.register_tools.assert_not_called()


def test_register_tools_skips_community_module_on_enterprise_only():
    """An enterprise-only deployment must not expose community session tools."""
    fake_server = MagicMock()
    multi = _multi_config_with_sections(
        community=None, enterprise=MagicMock(name="enterprise")
    )
    with (
        patch.object(server_module, "session") as m_session,
        patch.object(server_module, "table") as m_table,
        patch.object(server_module, "script") as m_script,
        patch.object(server_module, "session_community") as m_sc,
        patch.object(server_module, "session_enterprise") as m_se,
        patch.object(server_module, "catalog") as m_catalog,
        patch.object(server_module, "pq") as m_pq,
    ):
        server_module._register_tools(fake_server, multi)
    for m in (m_session, m_table, m_script, m_se, m_catalog, m_pq):
        m.register_tools.assert_called_once_with(fake_server)
    m_sc.register_tools.assert_not_called()


def test_register_tools_skips_section_specific_modules_when_neither_loaded():
    """When neither section is loaded, only cross-cutting tools register."""
    fake_server = MagicMock()
    multi = _multi_config_with_sections(community=None, enterprise=None)
    with (
        patch.object(server_module, "session") as m_session,
        patch.object(server_module, "table") as m_table,
        patch.object(server_module, "script") as m_script,
        patch.object(server_module, "session_community") as m_sc,
        patch.object(server_module, "session_enterprise") as m_se,
        patch.object(server_module, "catalog") as m_catalog,
        patch.object(server_module, "pq") as m_pq,
    ):
        server_module._register_tools(fake_server, multi)
    for m in (m_session, m_table, m_script):
        m.register_tools.assert_called_once_with(fake_server)
    for m in (m_sc, m_se, m_catalog, m_pq):
        m.register_tools.assert_not_called()


def test_build_fastmcp_wires_lifespan_tools_and_health():
    """``_build_fastmcp`` returns a FastMCP wired with the lifespan, tools, and health."""
    fake_server = MagicMock()
    fake_lifespan = object()
    with (
        patch.object(
            server_module, "FastMCP", return_value=fake_server
        ) as mock_fastmcp,
        patch.object(
            server_module, "make_lifespan", return_value=fake_lifespan
        ) as mock_lifespan,
        patch.object(server_module, "_register_tools") as mock_tools,
        patch.object(server_module, "_register_health_endpoint") as mock_health,
    ):
        fake_multi = _multi_config_with(ServerConfig(), Path("/tmp/x"))
        result = server_module._build_fastmcp(fake_multi, "custom-name")
    assert result is fake_server
    mock_lifespan.assert_called_once_with(fake_multi)
    mock_fastmcp.assert_called_once()
    # FastMCP receives the lifespan and the configured server name.
    assert mock_fastmcp.call_args.kwargs["lifespan"] is fake_lifespan
    assert mock_fastmcp.call_args.kwargs["name"] == "custom-name"
    mock_tools.assert_called_once_with(fake_server, fake_multi)
    mock_health.assert_called_once_with(fake_server)


# ---------------------------------------------------------------------------
# _run_stdio / _run_http
# ---------------------------------------------------------------------------


def test_run_stdio_calls_run_stdio_async():
    """``_run_stdio`` drives ``server.run_stdio_async()`` via ``asyncio.run``."""
    fake_server = MagicMock()
    sentinel = object()
    fake_server.run_stdio_async = MagicMock(return_value=sentinel)
    with patch.object(server_module.asyncio, "run") as mock_run:
        server_module._run_stdio(fake_server)
    fake_server.run_stdio_async.assert_called_once_with()
    mock_run.assert_called_once_with(sentinel)


def test_run_http_appends_psk_middleware_and_starts_uvicorn():
    """``_run_http`` mounts PSKMiddleware on the streamable-HTTP app then runs uvicorn."""
    fake_server = MagicMock()
    fake_app = MagicMock()
    fake_app.user_middleware = []
    fake_server.streamable_http_app = MagicMock(return_value=fake_app)

    fake_config = object()
    fake_uvicorn_server = MagicMock()

    with (
        patch.object(
            server_module.uvicorn, "Config", return_value=fake_config
        ) as mock_config_cls,
        patch.object(
            server_module.uvicorn, "Server", return_value=fake_uvicorn_server
        ) as mock_server_cls,
    ):
        server_module._run_http(fake_server, host="127.0.0.1", port=9999, psk="secret")

    # One Middleware appended; carrying the expected PSK and bypass path.
    from deephaven_mcp._health import HEALTH_PATH
    from deephaven_mcp.auth.middleware import PSKMiddleware

    assert len(fake_app.user_middleware) == 1
    mw = fake_app.user_middleware[0]
    assert mw.cls is PSKMiddleware
    assert mw.kwargs["expected_psk"] == "secret"
    assert mw.kwargs["bypass_paths"] == (HEALTH_PATH,)

    # uvicorn.Config invoked with the host/port; uvicorn.Server(config).run() called.
    mock_config_cls.assert_called_once()
    assert mock_config_cls.call_args.kwargs["host"] == "127.0.0.1"
    assert mock_config_cls.call_args.kwargs["port"] == 9999
    assert mock_config_cls.call_args.kwargs["app"] is fake_app
    mock_server_cls.assert_called_once_with(fake_config)
    fake_uvicorn_server.run.assert_called_once_with()


def test_run_http_inserts_psk_middleware_at_index_zero():
    """``_run_http`` inserts PSKMiddleware before any existing middleware.

    Authentication must run first so other middleware (e.g. request
    logging) never sees un-authed request bodies.
    """
    fake_server = MagicMock()
    fake_app = MagicMock()
    sentinel_pre = MagicMock(name="pre_existing_middleware")
    fake_app.user_middleware = [sentinel_pre]
    fake_server.streamable_http_app = MagicMock(return_value=fake_app)

    fake_config = object()
    fake_uvicorn_server = MagicMock()

    with (
        patch.object(server_module.uvicorn, "Config", return_value=fake_config),
        patch.object(server_module.uvicorn, "Server", return_value=fake_uvicorn_server),
    ):
        server_module._run_http(fake_server, host="127.0.0.1", port=9999, psk="secret")

    from deephaven_mcp.auth.middleware import PSKMiddleware

    assert len(fake_app.user_middleware) == 2
    # PSKMiddleware must be at index 0; the pre-existing entry is pushed
    # back to index 1 by ``list.insert(0, ...)``.
    assert fake_app.user_middleware[0].cls is PSKMiddleware
    assert fake_app.user_middleware[1] is sentinel_pre
