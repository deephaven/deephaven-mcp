"""Tests for ``deephaven_mcp.config._server``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._redaction import REDACTED
from deephaven_mcp.mcp_systems_server.config._server import ServerConfig, load_server

# ---------------------------------------------------------------------------
# ServerConfig — construction / validation
# ---------------------------------------------------------------------------


def test_server_config_accepts_empty_dict() -> None:
    cfg = ServerConfig.model_validate({})
    assert cfg.psk is None


def test_server_config_accepts_literal_psk() -> None:
    cfg = ServerConfig.model_validate({"psk": "literal"})
    assert isinstance(cfg.psk, SecretStr)
    assert cfg.psk.get_secret_value() == "literal"


def test_server_config_rejects_legacy_psk_env_var_field() -> None:
    # Env-var indirection is now expressed in the JSON as
    # ``"psk": "${env:NAME}"`` and is resolved by the templating
    # engine before the model sees the value. The legacy shadow
    # field is no longer accepted.
    with pytest.raises(ValidationError, match="Extra inputs"):
        ServerConfig.model_validate({"psk_env_var": "MY_PSK"})


def test_server_config_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        ServerConfig.model_validate({"bogus_field": "x"})


def test_server_config_transport_host_port_defaults() -> None:
    """The transport/host/port/server_name fields carry schema defaults."""
    cfg = ServerConfig.model_validate({})
    assert cfg.transport == "stdio"
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 8000
    assert cfg.server_name == "deephaven-mcp-systems"


def test_server_config_accepts_full_block() -> None:
    cfg = ServerConfig.model_validate(
        {
            "transport": "http",
            "host": "::1",
            "port": 9001,
            "server_name": "custom-name",
            "psk": "shh",
        }
    )
    assert cfg.transport == "http"
    assert cfg.host == "::1"
    assert cfg.port == 9001
    assert cfg.server_name == "custom-name"
    assert cfg.psk is not None
    assert cfg.psk.get_secret_value() == "shh"


def test_server_config_rejects_invalid_transport() -> None:
    with pytest.raises(ValidationError, match="transport"):
        ServerConfig.model_validate({"transport": "sse"})


def test_server_config_rejects_invalid_port() -> None:
    with pytest.raises(ValidationError, match="port"):
        ServerConfig.model_validate({"port": 0})
    with pytest.raises(ValidationError, match="port"):
        ServerConfig.model_validate({"port": 70000})


def test_server_config_is_frozen() -> None:
    cfg = ServerConfig.model_validate({"psk": "x"})
    with pytest.raises(ValidationError):
        cfg.psk = SecretStr("y")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_server_config_redacted_dump() -> None:
    cfg = ServerConfig.model_validate({"psk": "topsecret"})
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["psk"] == REDACTED
    # Non-secret fields dump as-is.
    assert out["transport"] == "stdio"
    assert out["host"] == "127.0.0.1"
    assert out["port"] == 8000
    assert out["server_name"] == "deephaven-mcp-systems"


def test_server_config_redacted_dump_none_psk_stays_none() -> None:
    cfg = ServerConfig.model_validate({})
    out = cfg.model_dump(mode="json", context={"redact": True})
    assert out["psk"] is None


def test_server_config_reveal_dump_resolves_secret() -> None:
    cfg = ServerConfig.model_validate({"psk": "from-env"})
    out = cfg.model_dump(mode="json", context={"reveal": True})
    assert out["psk"] == "from-env"


def test_server_config_repr_masks_psk() -> None:
    cfg = ServerConfig.model_validate({"psk": "topsecret"})
    assert "topsecret" not in repr(cfg)


# ---------------------------------------------------------------------------
# load_server
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_server_returns_none_when_absent(tmp_path: Path) -> None:
    assert await load_server(tmp_path) is None


@pytest.mark.asyncio
async def test_load_server_returns_config_with_literal_psk(tmp_path: Path) -> None:
    (tmp_path / "server.json").write_text(json.dumps({"psk": "literal"}))
    result = await load_server(tmp_path)
    assert isinstance(result, ServerConfig)
    assert result.psk is not None
    assert result.psk.get_secret_value() == "literal"


@pytest.mark.asyncio
async def test_load_server_empty_file_yields_none_psk(tmp_path: Path) -> None:
    (tmp_path / "server.json").write_text(json.dumps({}))
    result = await load_server(tmp_path)
    assert isinstance(result, ServerConfig)
    assert result.psk is None


@pytest.mark.asyncio
async def test_load_server_validates(tmp_path: Path) -> None:
    (tmp_path / "server.json").write_text(json.dumps({"unknown": 1}))
    with pytest.raises(ConfigurationError, match="Extra inputs"):
        await load_server(tmp_path)


@pytest.mark.asyncio
async def test_load_server_resolves_env_template(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`${env:NAME}` is expanded by the templating engine at load time."""
    monkeypatch.setenv("ENV_PSK", "fromenv")
    (tmp_path / "server.json").write_text(json.dumps({"psk": "${env:ENV_PSK}"}))
    result = await load_server(tmp_path)
    assert result is not None
    assert result.psk is not None
    assert result.psk.get_secret_value() == "fromenv"
