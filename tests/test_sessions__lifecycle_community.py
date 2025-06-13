import pytest
from unittest.mock import MagicMock
from deephaven_mcp.sessions import _lifecycle_community

@pytest.mark.asyncio
async def test_create_session_for_worker(monkeypatch):
    async def fake_get_named_config(cfg_mgr, section, name):
        assert section == "community_sessions"
        assert name == "workerZ"
        return {"host": "localhost"}

    async def fake_get_session_parameters(cfg):
        assert cfg["host"] == "localhost"
        return {"host": "localhost"}

    async def fake_create_session(**kwargs):
        assert kwargs["host"] == "localhost"
        return "SESSION"

    monkeypatch.setattr(_lifecycle_community.config, "get_named_config", fake_get_named_config)
    monkeypatch.setattr(_lifecycle_community, "get_session_parameters", fake_get_session_parameters)
    monkeypatch.setattr(_lifecycle_community, "create_session", fake_create_session)

    cfg_mgr = MagicMock()
    session = await _lifecycle_community.create_session_for_worker(cfg_mgr, "workerZ")
    assert session == "SESSION"

@pytest.mark.asyncio
async def test_create_session_for_worker_config_fail(monkeypatch):
    async def fake_get_named_config(cfg_mgr, section, name):
        raise RuntimeError("fail-config")
    monkeypatch.setattr(_lifecycle_community.config, "get_named_config", fake_get_named_config)
    cfg_mgr = MagicMock()
    with pytest.raises(RuntimeError):
        await _lifecycle_community.create_session_for_worker(cfg_mgr, "workerZ")

@pytest.mark.asyncio
async def test_create_session_for_worker_session_fail(monkeypatch):
    async def fake_get_named_config(cfg_mgr, section, name):
        return {"host": "localhost"}
    async def fake_get_session_parameters(cfg):
        return {"host": "localhost"}
    async def fake_create_session(**kwargs):
        raise RuntimeError("fail-create")
    monkeypatch.setattr(_lifecycle_community.config, "get_named_config", fake_get_named_config)
    monkeypatch.setattr(_lifecycle_community, "get_session_parameters", fake_get_session_parameters)
    monkeypatch.setattr(_lifecycle_community, "create_session", fake_create_session)
    cfg_mgr = MagicMock()
    with pytest.raises(RuntimeError):
        await _lifecycle_community.create_session_for_worker(cfg_mgr, "workerZ")
