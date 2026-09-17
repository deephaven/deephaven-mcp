"""
Tests for EnterpriseSessionRegistry in _registry_enterprise.py.

Covers:
- _fetch_factory_pqs (all branches)
- EnterpriseSessionRegistry.__init__
- factory_manager property
- _load_items
- initialize (idempotency)
- close (various scenarios)
- get / get_all
- _sync_enterprise_sessions
- _snapshot_factory_state
- _apply_result (success path delegates to _apply_factory_success;
  error path is inlined — record error, replace client, no _items changes)
- _remove_sessions_by_keys
- _apply_factory_success
- _discover_enterprise_sessions
- _build_not_found_message
- _make_enterprise_session_manager (static)

Note: add_session / remove / count_added_sessions are inherited from
MutableSessionRegistry and tested via test__registry.py.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deephaven_mcp._exceptions import (
    InternalError,
    RegistryItemNotFoundError,
)
from deephaven_mcp._taxonomy import SessionOrigin
from deephaven_mcp.auth.credentials import PasswordCredentials
from deephaven_mcp.client import EnterpriseClientTimeouts
from deephaven_mcp.resource_manager import (
    EnterpriseSessionRegistry,
    InitializationPhase,
    QualifiedSessionId,
    RegistrySnapshot,
)
from deephaven_mcp.resource_manager._manager import (
    BaseItemManager,
    CorePlusSessionFactoryManager,
    EnterpriseSessionManager,
)
from deephaven_mcp.resource_manager._registry_enterprise import (
    _FactoryQueryError,
    _FactoryQueryResult,
    _FactorySnapshot,
    _fetch_factory_pqs,
)
from deephaven_mcp.sessions import EnterpriseSystemConfig

_TEST_SYSTEM_NAME = "system"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _password_creds(
    username: str = "alice", password: str = "pw", effective: str | None = None
) -> PasswordCredentials:
    return PasswordCredentials(
        username=username, password=password, effective_user=effective
    )


def _make_config(
    system_name: str = _TEST_SYSTEM_NAME,
    raw: dict | None = None,
    creds: PasswordCredentials | None = None,
) -> EnterpriseSystemConfig:
    """Build a validated :class:`EnterpriseSystemConfig` for tests.

    The legacy ``raw`` parameter is retained for backward compatibility
    of test signatures, but the new pydantic model has typed fields
    rather than a free-form dict; we accept ``raw`` as an optional
    extra-fields dict that is merged into the canonical payload.
    """
    resolved_creds = creds if creds is not None else _password_creds()
    payload: dict = {
        "name": system_name,
        "connection_json_url": "https://example.com/iris/connection.json",
        "auth": {
            "credentials": resolved_creds.model_dump(
                mode="json", context={"reveal": True}
            )
        },
    }
    if raw:
        # Filter out keys that aren't part of the EnterpriseSystemConfig
        # schema (legacy callers passed ``{"host": ..., "port": ...}``
        # which never matched the on-disk schema either).
        for key in ("session_creation", "connection_timeout_seconds"):
            if key in raw:
                payload[key] = raw[key]
    return EnterpriseSystemConfig.model_validate(payload)


def _make_registry() -> EnterpriseSessionRegistry:
    """Return a fresh, **un-initialized** registry bound to a stock config."""
    return EnterpriseSessionRegistry(
        _make_config(), timeouts=EnterpriseClientTimeouts()
    )


def _make_initialized_registry() -> EnterpriseSessionRegistry:
    """Return a registry in the post-``_load_items`` state.

    Bypasses :meth:`initialize` so tests can swap in a mock factory
    manager and exercise downstream behavior without launching the
    background discovery task.
    """
    registry = _make_registry()
    registry._initialized = True
    registry._phase = InitializationPhase.COMPLETED
    registry._factory_manager = MagicMock(spec=CorePlusSessionFactoryManager)
    return registry


def _make_mock_manager(qualified_session_id: str) -> MagicMock:
    """Return a BaseItemManager mock with the given qualified_session_id."""
    mgr = MagicMock(spec=BaseItemManager)
    mgr.qualified_session_id = QualifiedSessionId.from_str(qualified_session_id)
    mgr.close = AsyncMock()
    return mgr


def _qsid(s: str) -> QualifiedSessionId:
    """Test helper — wire-form to typed key."""
    return QualifiedSessionId.from_str(s)


def _make_factory_snapshot(client=None) -> _FactorySnapshot:
    """Return a _FactorySnapshot with a mock factory_manager."""
    factory_manager = MagicMock(spec=CorePlusSessionFactoryManager)
    return _FactorySnapshot(factory_manager=factory_manager, client=client)


# ---------------------------------------------------------------------------
# 1. _fetch_factory_pqs — pure I/O function
# ---------------------------------------------------------------------------


def _make_pq_info(pq_name: str) -> MagicMock:
    """Return a mock PQ info object whose .config.pb.name == pq_name."""
    pb = MagicMock()
    pb.name = pq_name
    config = MagicMock()
    config.pb = pb
    info = MagicMock()
    info.config = config
    return info


@pytest.mark.asyncio
async def test_fetch_factory_pqs_no_cached_client_success():
    """No cached client → creates client via get_controller_client(), calls map()."""
    snapshot = _make_factory_snapshot(client=None)

    mock_client = AsyncMock()
    mock_client.map = AsyncMock(
        return_value={
            "q1": _make_pq_info("pq1"),
            "q2": _make_pq_info("pq2"),
        }
    )
    snapshot.factory_manager.get_controller_client = AsyncMock(return_value=mock_client)

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryResult)
    assert result.new_client is mock_client
    assert result.query_map == {"q1": "pq1", "q2": "pq2"}


@pytest.mark.asyncio
async def test_fetch_factory_pqs_poisoned_cached_client_recreates():
    """Poisoned cached client → discarded and recreated via get_controller_client()."""
    mock_old_client = AsyncMock()
    mock_old_client.is_poisoned = True

    mock_new_client = AsyncMock()
    mock_new_client.is_poisoned = False
    mock_new_client.map = AsyncMock(return_value={"q1": _make_pq_info("healed-pq")})

    snapshot = _make_factory_snapshot(client=mock_old_client)
    snapshot.factory_manager.get_controller_client = AsyncMock(
        return_value=mock_new_client
    )

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryResult)
    assert result.new_client is mock_new_client
    assert result.query_map == {"q1": "healed-pq"}
    # Poisoned client is discarded without pinging.
    mock_old_client.ping.assert_not_called()
    snapshot.factory_manager.get_controller_client.assert_awaited_once()
    # The healer owns teardown of a poisoned factory; discovery must not
    # evict it out from under an in-progress recreate.
    snapshot.factory_manager.close.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_factory_pqs_cached_client_ping_ok():
    """Cached client with good ping → reuses client, new_client is NOT set (stays None in result's new_client)."""
    mock_client = AsyncMock()
    mock_client.is_poisoned = False
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.map = AsyncMock(
        return_value={
            "q1": _make_pq_info("mypq"),
        }
    )

    snapshot = _make_factory_snapshot(client=mock_client)

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryResult)
    # When ping ok, we reuse the cached client and return it as new_client
    assert result.new_client is mock_client
    assert result.query_map == {"q1": "mypq"}
    # get_controller_client should NOT have been called
    snapshot.factory_manager.get_controller_client.assert_not_called()
    snapshot.factory_manager.close.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_factory_pqs_ping_failure_evicts_the_dead_client():
    """A client the manager still holds after failing its ping is evicted.

    Regression guard: ``get_controller_client()`` returns the cached factory's
    own controller whenever it is not poisoned, and a ping failure does not
    poison it. Without the eviction the "recreate" hands back the very client
    that just failed, and ``map()`` runs on it.
    """
    mock_old_client = AsyncMock()
    mock_old_client.is_poisoned = False
    mock_old_client.ping = AsyncMock(side_effect=ConnectionError("dead"))

    mock_new_client = AsyncMock()
    mock_new_client.map = AsyncMock(return_value={})

    snapshot = _make_factory_snapshot(client=mock_old_client)
    calls: list[str] = []
    # The manager still holds the dead client until it is evicted.
    returns = [mock_old_client, mock_new_client]

    async def _record_close():
        calls.append("close")

    async def _record_get():
        calls.append("get_controller_client")
        return returns.pop(0)

    snapshot.factory_manager.close = _record_close
    snapshot.factory_manager.get_controller_client = _record_get

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryResult)
    assert calls == ["get_controller_client", "close", "get_controller_client"]
    assert result.new_client is mock_new_client
    mock_old_client.map.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_factory_pqs_ping_failure_keeps_an_already_replaced_factory():
    """A snapshot client the manager has already replaced is dropped, not evicted.

    Regression guard: the healer can swap the factory after the snapshot was
    taken, so closing on every ping failure would tear down a fresh healthy
    replacement that concurrent callers are using.
    """
    mock_stale_client = AsyncMock()
    mock_stale_client.is_poisoned = False
    mock_stale_client.ping = AsyncMock(side_effect=ConnectionError("stale"))

    mock_current_client = AsyncMock()
    mock_current_client.map = AsyncMock(return_value={"q1": _make_pq_info("live-pq")})

    snapshot = _make_factory_snapshot(client=mock_stale_client)
    snapshot.factory_manager.get_controller_client = AsyncMock(
        return_value=mock_current_client
    )

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryResult)
    assert result.new_client is mock_current_client
    assert result.query_map == {"q1": "live-pq"}
    snapshot.factory_manager.close.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_factory_pqs_cached_client_ping_returns_false():
    """Ping returns False → recreates client via get_controller_client()."""
    mock_old_client = AsyncMock()
    mock_old_client.is_poisoned = False
    mock_old_client.ping = AsyncMock(return_value=False)

    mock_new_client = AsyncMock()
    mock_new_client.map = AsyncMock(
        return_value={
            "q1": _make_pq_info("fresh-pq"),
        }
    )

    snapshot = _make_factory_snapshot(client=mock_old_client)
    snapshot.factory_manager.get_controller_client = AsyncMock(
        side_effect=[mock_old_client, mock_new_client]
    )

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryResult)
    assert result.new_client is mock_new_client
    assert result.query_map == {"q1": "fresh-pq"}
    snapshot.factory_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_factory_pqs_cached_client_ping_raises():
    """Ping raises Exception → recreates client."""
    mock_old_client = AsyncMock()
    mock_old_client.is_poisoned = False
    mock_old_client.ping = AsyncMock(side_effect=ConnectionError("timeout"))

    mock_new_client = AsyncMock()
    mock_new_client.map = AsyncMock(
        return_value={
            "q1": _make_pq_info("recovered-pq"),
        }
    )

    snapshot = _make_factory_snapshot(client=mock_old_client)
    snapshot.factory_manager.get_controller_client = AsyncMock(
        side_effect=[mock_old_client, mock_new_client]
    )

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryResult)
    assert result.new_client is mock_new_client
    assert result.query_map == {"q1": "recovered-pq"}
    snapshot.factory_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_factory_pqs_no_cached_client_factory_get_raises():
    """No cached client, get_controller_client() raises → _FactoryQueryError(new_client=None)."""
    snapshot = _make_factory_snapshot(client=None)
    snapshot.factory_manager.get_controller_client = AsyncMock(
        side_effect=RuntimeError("factory down")
    )

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryError)
    assert result.new_client is None
    assert "RuntimeError" in result.error
    assert "factory down" in result.error


@pytest.mark.asyncio
async def test_fetch_factory_pqs_cached_client_ping_fails_recreate_fails():
    """Ping raises AND get_controller_client() also raises → _FactoryQueryError(new_client=None)."""
    mock_old_client = AsyncMock()
    mock_old_client.is_poisoned = False
    mock_old_client.ping = AsyncMock(side_effect=ConnectionError("dead"))

    snapshot = _make_factory_snapshot(client=mock_old_client)
    snapshot.factory_manager.get_controller_client = AsyncMock(
        side_effect=RuntimeError("factory unreachable")
    )

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryError)
    # new_client is None because get_controller_client() failed before assignment
    assert result.new_client is None
    assert "RuntimeError" in result.error


@pytest.mark.asyncio
async def test_fetch_factory_pqs_map_raises_new_client_created():
    """No cached client, get_controller_client() succeeds, but client.map() raises → _FactoryQueryError(new_client=<new>, ...)."""
    mock_new_client = AsyncMock()
    mock_new_client.map = AsyncMock(side_effect=IOError("map failed"))

    snapshot = _make_factory_snapshot(client=None)
    snapshot.factory_manager.get_controller_client = AsyncMock(
        return_value=mock_new_client
    )

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryError)
    # new_client IS set because we created it before map() failed
    assert result.new_client is mock_new_client
    assert "IOError" in result.error or "OSError" in result.error


@pytest.mark.asyncio
async def test_fetch_factory_pqs_map_raises_using_cached_client():
    """Cached client, ping ok, map() raises → _FactoryQueryError(new_client=None, ...).

    new_client stays None because we reused the cached client (no new creation).
    """
    mock_client = AsyncMock()
    mock_client.is_poisoned = False
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.map = AsyncMock(side_effect=ValueError("bad map"))

    snapshot = _make_factory_snapshot(client=mock_client)

    result = await _fetch_factory_pqs(snapshot)

    assert isinstance(result, _FactoryQueryError)
    # new_client is None — reused cached, not a new creation
    assert result.new_client is None
    assert "ValueError" in result.error
    assert "bad map" in result.error


# ---------------------------------------------------------------------------
# 2. _make_enterprise_session_manager static method
# ---------------------------------------------------------------------------


def test_make_enterprise_session_manager_returns_enterprise_session_manager():
    """Creates an EnterpriseSessionManager with system=system_name, name=display_name, session_id=serial."""
    mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)

    manager = EnterpriseSessionRegistry._make_enterprise_session_manager(
        mock_factory, 42, "my-pq", _TEST_SYSTEM_NAME
    )

    assert isinstance(manager, EnterpriseSessionManager)
    assert manager.system == _TEST_SYSTEM_NAME
    assert manager.name == "my-pq"
    assert manager.session_id == "42"
    assert str(manager.qualified_session_id) == f"enterprise:{_TEST_SYSTEM_NAME}:42"
    assert manager.origin is SessionOrigin.DISCOVERED


@pytest.mark.asyncio
async def test_make_enterprise_session_manager_creation_function_calls_connect():
    """The creation_function calls factory.get() then connect_to_persistent_query(serial=serial)."""
    mock_session = MagicMock()
    mock_factory_instance = AsyncMock()
    mock_factory_instance.connect_to_persistent_query = AsyncMock(
        return_value=mock_session
    )

    mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
    mock_factory.get = AsyncMock(return_value=mock_factory_instance)

    manager = EnterpriseSessionRegistry._make_enterprise_session_manager(
        mock_factory, 7, "pq-test", _TEST_SYSTEM_NAME
    )

    # The creation_function is stored in manager._creation_function
    result = await manager._creation_function(_TEST_SYSTEM_NAME, "pq-test")

    mock_factory.get.assert_awaited_once()
    mock_factory_instance.connect_to_persistent_query.assert_awaited_once_with(serial=7)
    assert result is mock_session


# ---------------------------------------------------------------------------
# 3. __init__
# ---------------------------------------------------------------------------


def test_init_default_state():
    """__init__ captures config and initializes per-run state to defaults."""
    cfg = _make_config()
    registry = EnterpriseSessionRegistry(cfg, timeouts=EnterpriseClientTimeouts())

    # Config-derived fields are populated immediately.
    assert registry._system_name == cfg.name
    # The registry now stores the typed declaration directly rather
    # than a model-dumped dict; downstream consumers (the factory
    # manager and ``CorePlusSessionFactory.from_credentials``) accept
    # the typed value, eliminating the previous reveal round-trip.
    assert registry._system_config is cfg
    assert registry._creds is cfg.auth.credentials
    # Per-run state stays at defaults until initialize() runs.
    assert registry._factory_manager is None
    assert registry._controller_client is None
    assert registry._added_session_ids == set()
    assert registry._phase == InitializationPhase.NOT_STARTED
    assert registry._error is None
    assert registry._discovery_task is None
    assert not registry._initialized


# ---------------------------------------------------------------------------
# system_name property
# ---------------------------------------------------------------------------


def test_system_name_not_initialized_raises():
    """system_name raises InternalError when registry not initialized."""
    registry = _make_registry()

    with pytest.raises(InternalError):
        _ = registry.system_name


def test_system_name_initialized_returns_name():
    """system_name returns _system_name when initialized."""
    registry = _make_initialized_registry()
    assert registry.system_name == _TEST_SYSTEM_NAME


# ---------------------------------------------------------------------------
# 4. factory_manager property
# ---------------------------------------------------------------------------


def test_factory_manager_not_initialized_raises():
    """factory_manager raises InternalError when registry not initialized."""
    registry = _make_registry()

    with pytest.raises(InternalError):
        _ = registry.factory_manager


def test_factory_manager_returns_factory():
    """factory_manager returns _factory_manager when initialized and set."""
    registry = _make_initialized_registry()
    fm = registry.factory_manager

    assert fm is registry._factory_manager


def test_factory_manager_initialized_but_unset_raises():
    """factory_manager raises InternalError when the registry is marked
    initialized but ``_factory_manager`` was never assigned."""
    registry = _make_initialized_registry()
    registry._factory_manager = None

    with pytest.raises(InternalError, match="did not run to completion"):
        _ = registry.factory_manager


# ---------------------------------------------------------------------------
# 5. _load_items / initialize
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_items_builds_factory_and_launches_discovery():
    """:meth:`_load_items` builds the factory manager and starts discovery."""
    registry = _make_registry()
    mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
    mock_discover = AsyncMock()

    with (
        patch(
            "deephaven_mcp.resource_manager._registry_enterprise.CorePlusSessionFactoryManager",
            return_value=mock_factory,
        ) as mock_cls,
        patch.object(registry, "_discover_enterprise_sessions", mock_discover),
    ):
        await registry._load_items()
        await asyncio.sleep(0)

    mock_cls.assert_called_once_with(
        registry._system_name,
        registry._system_config,
        registry._creds,
        timeouts=registry._timeouts,
    )
    assert registry._factory_manager is mock_factory
    assert registry._phase == InitializationPhase.PARTIAL
    assert registry._discovery_task is not None


@pytest.mark.asyncio
async def test_initialize_starts_discovery_once():
    """A full ``initialize()`` call wires the factory and discovery task."""
    registry = _make_registry()
    mock_factory = MagicMock(spec=CorePlusSessionFactoryManager)
    mock_discover = AsyncMock()

    with (
        patch(
            "deephaven_mcp.resource_manager._registry_enterprise.CorePlusSessionFactoryManager",
            return_value=mock_factory,
        ),
        patch.object(registry, "_discover_enterprise_sessions", mock_discover),
    ):
        await registry.initialize()
        # Idempotent: a second call must not re-create the factory.
        await registry.initialize()
        await asyncio.sleep(0)

    assert registry._initialized
    assert registry._factory_manager is mock_factory
    assert registry._discovery_task is not None


# ---------------------------------------------------------------------------
# 7. close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_not_initialized_raises():
    """close() raises InternalError if registry is not initialized."""
    registry = _make_registry()

    with pytest.raises(InternalError):
        await registry.close()


@pytest.mark.asyncio
async def test_close_cancels_discovery_task():
    """close() cancels a running discovery task and clears all state."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.LOADING

    async def long_task():
        await asyncio.sleep(100)

    registry._discovery_task = asyncio.create_task(long_task())

    await registry.close()

    assert not registry._initialized
    assert registry._factory_manager is None
    assert registry._phase == InitializationPhase.NOT_STARTED
    assert registry._discovery_task is None


@pytest.mark.asyncio
async def test_close_no_discovery_task():
    """close() succeeds even if _discovery_task is None."""
    registry = _make_initialized_registry()
    registry._discovery_task = None

    await registry.close()

    assert not registry._initialized
    assert registry._factory_manager is None


@pytest.mark.asyncio
async def test_close_factory_close_raises_is_caught():
    """close() catches exceptions from factory.close() and continues."""
    registry = _make_initialized_registry()
    registry._factory_manager.close = AsyncMock(
        side_effect=RuntimeError("factory close error")
    )
    registry._discovery_task = None

    # Should not raise
    await registry.close()

    assert not registry._initialized


@pytest.mark.asyncio
async def test_close_closes_the_factory_manager():
    """close() closes the factory manager, which stops its healer."""
    registry = _make_initialized_registry()
    factory = registry._factory_manager
    registry._discovery_task = None

    await registry.close()

    factory.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_item_close_raises_is_caught():
    """close() catches exceptions from item.close() and continues."""
    registry = _make_initialized_registry()
    registry._discovery_task = None

    mgr = _make_mock_manager("enterprise:system:pq1")
    mgr.close = AsyncMock(side_effect=RuntimeError("item close error"))
    registry._items["enterprise:system:pq1"] = mgr

    # Should not raise
    await registry.close()

    assert not registry._initialized
    assert registry._items == {}


@pytest.mark.asyncio
async def test_close_clears_controller_client_and_errors():
    """close() clears _controller_client, _error, and _added_session_ids."""
    registry = _make_initialized_registry()
    registry._controller_client = MagicMock()
    registry._error = "some error"
    registry._added_session_ids = {"enterprise:system:pq1"}
    registry._discovery_task = None

    await registry.close()

    assert registry._controller_client is None
    assert registry._error is None
    assert registry._added_session_ids == set()


# ---------------------------------------------------------------------------
# 8. get
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_not_initialized_raises():
    """get() raises InternalError when not initialized."""
    registry = _make_registry()

    with pytest.raises(InternalError):
        await registry.get("enterprise:system:pq1")


@pytest.mark.asyncio
async def test_get_in_completed_phase_triggers_sync():
    """get() in COMPLETED phase calls _sync_enterprise_sessions before lookup."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED

    mgr = _make_mock_manager("enterprise:system:my-pq")
    registry._items["enterprise:system:my-pq"] = mgr

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()) as mock_sync:
        result = await registry.get("enterprise:system:my-pq")

    mock_sync.assert_awaited_once()
    assert result is mgr


@pytest.mark.asyncio
async def test_get_in_loading_phase_no_sync():
    """get() in LOADING phase does NOT call _sync_enterprise_sessions."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.LOADING

    mgr = _make_mock_manager("enterprise:system:my-pq")
    registry._items["enterprise:system:my-pq"] = mgr

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()) as mock_sync:
        result = await registry.get("enterprise:system:my-pq")

    mock_sync.assert_not_awaited()
    assert result is mgr


@pytest.mark.asyncio
async def test_get_not_found_raises():
    """get() raises RegistryItemNotFoundError when item not present."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()):
        with pytest.raises(RegistryItemNotFoundError):
            await registry.get("enterprise:system:nonexistent")


# ---------------------------------------------------------------------------
# 9. get_all
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_not_initialized_raises():
    """get_all() raises InternalError when not initialized."""
    registry = _make_registry()

    with pytest.raises(InternalError):
        await registry.get_all()


@pytest.mark.asyncio
async def test_get_all_in_completed_phase_triggers_sync():
    """get_all() in COMPLETED phase calls sync and returns snapshot."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED

    mgr = _make_mock_manager("enterprise:system:pq1")
    registry._items["enterprise:system:pq1"] = mgr

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()) as mock_sync:
        snapshot = await registry.get_all()

    mock_sync.assert_awaited_once()
    assert isinstance(snapshot, RegistrySnapshot)
    assert "enterprise:system:pq1" in snapshot.items
    assert snapshot.initialization_phase == InitializationPhase.COMPLETED


@pytest.mark.asyncio
async def test_get_all_non_completed_phase_no_sync():
    """get_all() in LOADING phase does not call sync."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.LOADING

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()) as mock_sync:
        snapshot = await registry.get_all()

    mock_sync.assert_not_awaited()
    assert snapshot.initialization_phase == InitializationPhase.LOADING


@pytest.mark.asyncio
async def test_get_all_includes_errors():
    """get_all() snapshot includes initialization errors."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED
    registry._error = "connection refused"

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()):
        snapshot = await registry.get_all()

    assert snapshot.initialization_errors == {"error": "connection refused"}


# ---------------------------------------------------------------------------
# 10. _check_and_sync
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_and_sync_not_initialized_raises():
    """_check_and_sync raises InternalError when registry not initialized."""
    registry = _make_registry()

    with pytest.raises(InternalError):
        await registry._check_and_sync()


@pytest.mark.asyncio
async def test_check_and_sync_completed_phase_triggers_sync():
    """_check_and_sync triggers _sync_enterprise_sessions in COMPLETED phase."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()) as mock_sync:
        await registry._check_and_sync()

    mock_sync.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_and_sync_loading_phase_no_sync():
    """_check_and_sync does NOT trigger sync in LOADING phase."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.LOADING

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()) as mock_sync:
        await registry._check_and_sync()

    mock_sync.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_and_sync_partial_phase_no_sync():
    """_check_and_sync does NOT trigger sync in PARTIAL phase."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.PARTIAL

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()) as mock_sync:
        await registry._check_and_sync()

    mock_sync.assert_not_awaited()


# ---------------------------------------------------------------------------
# 11. isinstance — mutation API is inherited from MutableSessionRegistry
# ---------------------------------------------------------------------------


def test_enterprise_registry_is_mutable_session_registry():
    """EnterpriseSessionRegistry is a MutableSessionRegistry (mutation API inherited)."""
    from deephaven_mcp.resource_manager._registry import MutableSessionRegistry

    assert isinstance(_make_registry(), MutableSessionRegistry)


# ---------------------------------------------------------------------------
# 11. _sync_enterprise_sessions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_enterprise_sessions_normal():
    """Normal flow: snapshot → fetch → apply → close stale managers."""
    registry = _make_initialized_registry()

    mock_snapshot = MagicMock(spec=_FactorySnapshot)
    mock_snapshot.factory_manager = registry._factory_manager
    mock_result = MagicMock(spec=_FactoryQueryResult)
    mock_stale_mgr = _make_mock_manager("enterprise:system:stale")

    with (
        patch.object(
            registry, "_snapshot_factory_state", AsyncMock(return_value=mock_snapshot)
        ),
        patch(
            "deephaven_mcp.resource_manager._registry_enterprise._fetch_factory_pqs",
            AsyncMock(return_value=mock_result),
        ),
        patch.object(registry, "_apply_result", return_value=[mock_stale_mgr]),
    ):
        await registry._sync_enterprise_sessions()

    mock_stale_mgr.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_enterprise_sessions_returns_early_when_snapshot_none():
    """_sync_enterprise_sessions returns early when snapshot is None (factory unavailable)."""
    registry = _make_initialized_registry()

    with (
        patch.object(registry, "_snapshot_factory_state", AsyncMock(return_value=None)),
        patch(
            "deephaven_mcp.resource_manager._registry_enterprise._fetch_factory_pqs",
            AsyncMock(),
        ) as mock_fetch,
    ):
        await registry._sync_enterprise_sessions()

    mock_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_enterprise_sessions_stale_close_raises_is_caught():
    """_sync_enterprise_sessions catches exceptions from stale manager close."""
    registry = _make_initialized_registry()

    mock_snapshot = MagicMock(spec=_FactorySnapshot)
    mock_snapshot.factory_manager = registry._factory_manager
    mock_result = MagicMock(spec=_FactoryQueryResult)
    mock_stale_mgr = _make_mock_manager("enterprise:system:stale")
    mock_stale_mgr.close = AsyncMock(side_effect=RuntimeError("close error"))

    with (
        patch.object(
            registry, "_snapshot_factory_state", AsyncMock(return_value=mock_snapshot)
        ),
        patch(
            "deephaven_mcp.resource_manager._registry_enterprise._fetch_factory_pqs",
            AsyncMock(return_value=mock_result),
        ),
        patch.object(registry, "_apply_result", return_value=[mock_stale_mgr]),
    ):
        # Should not raise
        await registry._sync_enterprise_sessions()


# ---------------------------------------------------------------------------
# 14. _snapshot_factory_state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_factory_state_returns_snapshot():
    """_snapshot_factory_state returns _FactorySnapshot when factory is available."""
    registry = _make_initialized_registry()
    registry._controller_client = MagicMock()

    snapshot = await registry._snapshot_factory_state()

    assert snapshot is not None
    assert isinstance(snapshot, _FactorySnapshot)
    assert snapshot.factory_manager is registry._factory_manager
    assert snapshot.client is registry._controller_client


@pytest.mark.asyncio
async def test_snapshot_factory_state_returns_none_when_factory_none():
    """_snapshot_factory_state returns None when _factory_manager is None."""
    registry = _make_initialized_registry()
    registry._factory_manager = None

    snapshot = await registry._snapshot_factory_state()

    assert snapshot is None


# ---------------------------------------------------------------------------
# 15. _apply_result
# ---------------------------------------------------------------------------


def test_apply_result_success_calls_apply_factory_success():
    """_apply_result delegates to _apply_factory_success for _FactoryQueryResult."""
    registry = _make_initialized_registry()
    result = _FactoryQueryResult(new_client=MagicMock(), query_map={1: "pq1"})
    factory_manager = registry._factory_manager

    with patch.object(
        registry, "_apply_factory_success", return_value=[]
    ) as mock_success:
        managers = registry._apply_result(result, factory_manager)

    mock_success.assert_called_once_with(result, factory_manager)
    assert managers == []


def test_apply_result_error_records_error_and_returns_empty():
    """_apply_result handles _FactoryQueryError inline: record error,
    replace controller client, do not touch _items, return empty."""
    registry = _make_initialized_registry()
    key = f"enterprise:{_TEST_SYSTEM_NAME}:pq1"
    mgr = _make_mock_manager(key)
    registry._items[key] = mgr

    new_client = MagicMock()
    result = _FactoryQueryError(new_client=new_client, error="conn refused")
    factory_manager = registry._factory_manager

    managers = registry._apply_result(result, factory_manager)

    assert managers == []
    assert registry._items[key] is mgr
    assert registry._controller_client is new_client
    assert registry._error == "conn refused"


def test_apply_result_unexpected_type_raises():
    """_apply_result raises InternalError for unexpected result type."""
    registry = _make_initialized_registry()

    with pytest.raises(InternalError, match="Unexpected result type"):
        registry._apply_result("not-a-result", registry._factory_manager)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 16. _items key set (used directly in _apply_factory_success / _apply_factory_error)
# ---------------------------------------------------------------------------


def test_items_keys_used_directly_for_reconciliation():
    """_apply_factory_success uses set(_items.keys()) — all items are reconciled."""
    registry = _make_initialized_registry()
    key1 = _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:1")
    key2 = _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:2")
    registry._items[key1] = _make_mock_manager(str(key1))
    registry._items[key2] = _make_mock_manager(str(key2))

    # Controller reports only pq1 — pq2 is stale and should be removed.
    result = _FactoryQueryResult(new_client=MagicMock(), query_map={1: "pq1"})
    managers_to_close = registry._apply_factory_success(
        result, registry._factory_manager
    )

    assert key1 in registry._items
    assert key2 not in registry._items
    assert len(managers_to_close) == 1


# ---------------------------------------------------------------------------
# 17. _remove_sessions_by_keys
# ---------------------------------------------------------------------------


def test_remove_sessions_by_keys_removes_and_returns():
    """_remove_sessions_by_keys removes keys from _items and discards from _added_session_ids."""
    registry = _make_initialized_registry()
    mgr1 = _make_mock_manager(f"enterprise:{_TEST_SYSTEM_NAME}:pq1")
    mgr2 = _make_mock_manager(f"enterprise:{_TEST_SYSTEM_NAME}:pq2")

    registry._items[f"enterprise:{_TEST_SYSTEM_NAME}:pq1"] = mgr1
    registry._items[f"enterprise:{_TEST_SYSTEM_NAME}:pq2"] = mgr2
    registry._added_session_ids = {
        f"enterprise:{_TEST_SYSTEM_NAME}:pq1",
        f"enterprise:{_TEST_SYSTEM_NAME}:pq2",
    }

    removed = registry._remove_sessions_by_keys({f"enterprise:{_TEST_SYSTEM_NAME}:pq1"})

    assert removed == [mgr1]
    assert f"enterprise:{_TEST_SYSTEM_NAME}:pq1" not in registry._items
    assert f"enterprise:{_TEST_SYSTEM_NAME}:pq2" in registry._items
    assert f"enterprise:{_TEST_SYSTEM_NAME}:pq1" not in registry._added_session_ids
    assert f"enterprise:{_TEST_SYSTEM_NAME}:pq2" in registry._added_session_ids


def test_remove_sessions_by_keys_missing_key_is_ignored():
    """_remove_sessions_by_keys ignores keys not in _items."""
    registry = _make_initialized_registry()

    removed = registry._remove_sessions_by_keys({"enterprise:system:nonexistent"})

    assert removed == []


def test_remove_sessions_by_keys_empty_set():
    """_remove_sessions_by_keys with empty set returns empty list."""
    registry = _make_initialized_registry()

    removed = registry._remove_sessions_by_keys(set())

    assert removed == []


# ---------------------------------------------------------------------------
# 18. _apply_factory_success
# ---------------------------------------------------------------------------


def test_apply_factory_success_adds_new_pqs():
    """_apply_factory_success adds PQs reported by controller not in _items."""
    registry = _make_initialized_registry()
    registry._items.clear()

    mock_factory = registry._factory_manager
    new_client = MagicMock()
    result = _FactoryQueryResult(new_client=new_client, query_map={1: "pq1", 2: "pq2"})

    with patch.object(
        EnterpriseSessionRegistry,
        "_make_enterprise_session_manager",
        side_effect=lambda factory, serial, display_name, system_name: _make_mock_manager(
            f"enterprise:{system_name}:{serial}"
        ),
    ):
        managers_to_close = registry._apply_factory_success(result, mock_factory)

    assert registry._controller_client is new_client
    assert _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:1") in registry._items
    assert _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:2") in registry._items
    assert managers_to_close == []


def test_apply_factory_success_removes_stale_pqs():
    """_apply_factory_success removes sessions not reported by controller."""
    registry = _make_initialized_registry()
    stale_mgr = _make_mock_manager(f"enterprise:{_TEST_SYSTEM_NAME}:stale-pq")
    registry._items[f"enterprise:{_TEST_SYSTEM_NAME}:stale-pq"] = stale_mgr

    mock_factory = registry._factory_manager
    new_client = MagicMock()
    # Controller reports empty map — stale-pq should be removed
    result = _FactoryQueryResult(new_client=new_client, query_map={})

    managers_to_close = registry._apply_factory_success(result, mock_factory)

    assert f"enterprise:{_TEST_SYSTEM_NAME}:stale-pq" not in registry._items
    assert stale_mgr in managers_to_close


def test_apply_factory_success_clears_error():
    """_apply_factory_success clears previous _error."""
    registry = _make_initialized_registry()
    registry._error = "previous error"
    registry._items.clear()

    mock_factory = registry._factory_manager
    result = _FactoryQueryResult(new_client=MagicMock(), query_map={})

    registry._apply_factory_success(result, mock_factory)

    assert registry._error is None


def test_apply_factory_success_noop_when_unchanged():
    """_apply_factory_success is a no-op when controller matches current state."""
    registry = _make_initialized_registry()
    key = _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:7")
    existing_mgr = _make_mock_manager(str(key))
    registry._items[key] = existing_mgr

    mock_factory = registry._factory_manager
    new_client = MagicMock()
    result = _FactoryQueryResult(new_client=new_client, query_map={7: "existing"})

    managers_to_close = registry._apply_factory_success(result, mock_factory)

    assert key in registry._items
    assert managers_to_close == []
    assert registry._controller_client is new_client


# ---------------------------------------------------------------------------
# 19. _apply_result error-path behavior (inlined; no separate _apply_factory_error)
# ---------------------------------------------------------------------------


def test_apply_result_error_records_error_and_preserves_items():
    """Error path: record the error, clear the controller client, leave
    ``_items`` untouched.

    A failed controller query carries no information about which
    sessions are still alive; wiping would be too disruptive. The next
    successful refresh reconciles.
    """
    registry = _make_initialized_registry()
    key = f"enterprise:{_TEST_SYSTEM_NAME}:pq1"
    mgr = _make_mock_manager(key)
    registry._items[key] = mgr

    result = _FactoryQueryError(new_client=None, error="ValueError: connection refused")
    managers_to_close = registry._apply_result(result, registry._factory_manager)

    assert registry._error is not None
    assert "connection refused" in registry._error
    assert registry._items[key] is mgr
    assert managers_to_close == []
    assert registry._controller_client is None


def test_apply_result_error_with_new_client_updates_controller_client():
    """Error path updates ``_controller_client`` when ``new_client`` is provided."""
    registry = _make_initialized_registry()
    new_client = MagicMock()
    result = _FactoryQueryError(new_client=new_client, error="IOError: map failed")

    registry._apply_result(result, registry._factory_manager)

    assert registry._controller_client is new_client


def test_apply_result_error_no_new_client_sets_controller_client_none():
    """Error path sets ``_controller_client=None`` when ``new_client`` is ``None``."""
    registry = _make_initialized_registry()
    registry._controller_client = MagicMock()
    result = _FactoryQueryError(new_client=None, error="ConnError: timeout")

    registry._apply_result(result, registry._factory_manager)

    assert registry._controller_client is None


# ---------------------------------------------------------------------------
# 20. _discover_enterprise_sessions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_enterprise_sessions_success():
    """_discover_enterprise_sessions sets phase to LOADING then COMPLETED on success."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.PARTIAL

    with patch.object(registry, "_sync_enterprise_sessions", AsyncMock()):
        await registry._discover_enterprise_sessions()

    assert registry._phase == InitializationPhase.COMPLETED


@pytest.mark.asyncio
async def test_discover_enterprise_sessions_cancelled():
    """_discover_enterprise_sessions sets phase to FAILED and re-raises CancelledError."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.PARTIAL

    with patch.object(
        registry,
        "_sync_enterprise_sessions",
        AsyncMock(side_effect=asyncio.CancelledError()),
    ):
        with pytest.raises(asyncio.CancelledError):
            await registry._discover_enterprise_sessions()

    assert registry._phase == InitializationPhase.FAILED


@pytest.mark.asyncio
async def test_discover_enterprise_sessions_generic_exception():
    """_discover_enterprise_sessions records error and sets phase to COMPLETED on generic exception."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.PARTIAL

    with patch.object(
        registry,
        "_sync_enterprise_sessions",
        AsyncMock(side_effect=RuntimeError("oops")),
    ):
        await registry._discover_enterprise_sessions()

    assert registry._phase == InitializationPhase.COMPLETED
    assert registry._error is not None
    assert "RuntimeError" in registry._error


@pytest.mark.asyncio
async def test_discover_enterprise_sessions_sets_loading_phase_first():
    """_discover_enterprise_sessions transitions through LOADING before COMPLETED."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.PARTIAL

    phases_seen = []

    async def track_sync():
        phases_seen.append(registry._phase)

    with patch.object(
        registry, "_sync_enterprise_sessions", AsyncMock(side_effect=track_sync)
    ):
        await registry._discover_enterprise_sessions()

    assert InitializationPhase.LOADING in phases_seen
    assert registry._phase == InitializationPhase.COMPLETED


@pytest.mark.asyncio
async def test_discover_enterprise_sessions_logs_warning_when_errors_present():
    """_discover_enterprise_sessions logs a warning when completed with errors."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.PARTIAL

    async def sync_adds_error():
        async with registry._lock:
            registry._error = "some factory error"

    with patch.object(
        registry, "_sync_enterprise_sessions", AsyncMock(side_effect=sync_adds_error)
    ):
        await registry._discover_enterprise_sessions()

    # Phase should still be COMPLETED
    assert registry._phase == InitializationPhase.COMPLETED
    # Error should still be present
    assert registry._error is not None


# ---------------------------------------------------------------------------
# 21. _build_not_found_message
# ---------------------------------------------------------------------------


def test_build_not_found_message_simple():
    """No phase note, no error: simple message."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED

    msg = registry._build_not_found_message(f"enterprise:{_TEST_SYSTEM_NAME}:my-pq")

    assert f"enterprise:{_TEST_SYSTEM_NAME}:my-pq" in msg
    assert "No item with name" in msg


def test_build_not_found_message_loading_phase():
    """LOADING phase: message includes 'still in progress'."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.LOADING

    msg = registry._build_not_found_message(f"enterprise:{_TEST_SYSTEM_NAME}:my-pq")

    assert "still in progress" in msg


def test_build_not_found_message_not_started_phase():
    """NOT_STARTED phase: message includes 'not completed'."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.NOT_STARTED

    msg = registry._build_not_found_message(f"enterprise:{_TEST_SYSTEM_NAME}:my-pq")

    assert "not completed" in msg


def test_build_not_found_message_partial_phase():
    """PARTIAL phase: message includes 'not completed'."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.PARTIAL

    msg = registry._build_not_found_message(f"enterprise:{_TEST_SYSTEM_NAME}:my-pq")

    assert "not completed" in msg


def test_build_not_found_message_failed_phase():
    """FAILED phase: message includes 'not completed'."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.FAILED

    msg = registry._build_not_found_message(f"enterprise:{_TEST_SYSTEM_NAME}:my-pq")

    assert "not completed" in msg


def test_build_not_found_message_with_error():
    """_error set: message includes 'factory error' and the error text."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED
    registry._error = "connection refused"

    msg = registry._build_not_found_message(f"enterprise:{_TEST_SYSTEM_NAME}:my-pq")

    assert "connection refused" in msg
    assert "factory error" in msg


def test_build_not_found_message_malformed_name_no_raise():
    """Malformed name does NOT raise — _build_not_found_message never parses the id."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED

    # Should not raise regardless of error state
    msg = registry._build_not_found_message("badname")
    assert "badname" in msg


def test_build_not_found_message_malformed_name_with_error_no_raise():
    """Malformed name does NOT raise even when _error is set."""
    registry = _make_initialized_registry()
    registry._phase = InitializationPhase.COMPLETED
    registry._error = "some error"

    msg = registry._build_not_found_message("badname")
    assert "badname" in msg
    assert "some error" in msg


# ---------------------------------------------------------------------------
# Reconciliation behavior: registry mirrors the controller
# ---------------------------------------------------------------------------


def test_apply_factory_success_removes_keys_not_reported_by_controller():
    """A controller refresh removes any session the controller no longer reports.

    The registry is a cache of the controller's view. Anything not in
    the controller's report is removed — this includes sessions that
    were originally added via ``add_session`` (cache-warming). If the
    controller has stopped reporting it, the worker is gone and the
    manager should not linger.
    """
    registry = _make_initialized_registry()
    stale_controller_key = f"enterprise:{_TEST_SYSTEM_NAME}:stale-pq"
    stale_dynamic_key = f"enterprise:{_TEST_SYSTEM_NAME}:dead-dynamic"
    stale_controller_mgr = _make_mock_manager(stale_controller_key)
    stale_dynamic_mgr = _make_mock_manager(stale_dynamic_key)
    registry._items[stale_controller_key] = stale_controller_mgr
    registry._items[stale_dynamic_key] = stale_dynamic_mgr
    # The dead dynamic session was added via the create tool and the
    # worker has since died (controller no longer reports it).
    registry._added_session_ids.add(stale_dynamic_key)

    mock_factory = registry._factory_manager
    result = _FactoryQueryResult(new_client=MagicMock(), query_map={})

    managers_to_close = registry._apply_factory_success(result, mock_factory)

    # Both removed; both managers returned for the caller to close.
    assert stale_controller_key not in registry._items
    assert stale_dynamic_key not in registry._items
    # ``_added_session_ids`` is synchronously kept in lockstep with
    # ``_items`` removals (handled by ``_remove_sessions_by_keys``), so
    # the cap drops correctly when the dead dynamic session goes away.
    assert stale_dynamic_key not in registry._added_session_ids
    assert set(managers_to_close) == {stale_controller_mgr, stale_dynamic_mgr}


def test_apply_factory_success_adds_keys_reported_by_controller():
    """A controller refresh adds any new session the controller reports."""
    registry = _make_initialized_registry()
    new_pq_serial = 42
    new_pq_name = "new-pq"
    new_key = _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:{new_pq_serial}")

    mock_factory = registry._factory_manager
    result = _FactoryQueryResult(
        new_client=MagicMock(), query_map={new_pq_serial: new_pq_name}
    )

    managers_to_close = registry._apply_factory_success(result, mock_factory)

    assert new_key in registry._items
    assert managers_to_close == []


def test_apply_factory_error_does_not_modify_items():
    """A failed controller query preserves the last known state.

    Wiping the catalog on every transient controller blip would be too
    disruptive; with no new information from the controller, the
    safest move is to keep what we had. The next successful refresh
    will reconcile.
    """
    registry = _make_initialized_registry()
    controller_key = f"enterprise:{_TEST_SYSTEM_NAME}:controller-pq"
    dynamic_key = f"enterprise:{_TEST_SYSTEM_NAME}:dynamic-pq"
    controller_mgr = _make_mock_manager(controller_key)
    dynamic_mgr = _make_mock_manager(dynamic_key)
    registry._items[controller_key] = controller_mgr
    registry._items[dynamic_key] = dynamic_mgr
    registry._added_session_ids.add(dynamic_key)

    result = _FactoryQueryError(new_client=None, error="RuntimeError: outage")
    managers_to_close = registry._apply_result(result, registry._factory_manager)

    # Both entries untouched.
    assert registry._items[controller_key] is controller_mgr
    assert registry._items[dynamic_key] is dynamic_mgr
    assert dynamic_key in registry._added_session_ids
    assert managers_to_close == []
    assert registry._error is not None
    assert "outage" in registry._error


@pytest.mark.asyncio
async def test_sync_enterprise_sessions_dropping_known_session_removes_it():
    """Full-stack: when the controller stops reporting a session it had reported,
    the next refresh removes it from ``_items``.

    Covers the worker-died case: session was alive (in both ``_items``
    and the controller's report), then dies (controller drops it).
    Next refresh sweeps it.
    """
    registry = _make_initialized_registry()
    dead_key = f"enterprise:{_TEST_SYSTEM_NAME}:dead-pq"
    dead_mgr = _make_mock_manager(dead_key)
    registry._items[dead_key] = dead_mgr

    with patch(
        "deephaven_mcp.resource_manager._registry_enterprise._fetch_factory_pqs",
        new=AsyncMock(
            return_value=_FactoryQueryResult(new_client=MagicMock(), query_map={})
        ),
    ):
        await registry._sync_enterprise_sessions()

    assert dead_key not in registry._items


@pytest.mark.asyncio
async def test_sync_enterprise_sessions_controller_error_preserves_all_state():
    """Full-stack: a refresh tick where the factory query errors leaves
    every session in place. The error is recorded for diagnostics.
    """
    registry = _make_initialized_registry()
    controller_key = _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:controller-pq")
    dynamic_key = _qsid(f"enterprise:{_TEST_SYSTEM_NAME}:dynamic-pq")
    controller_mgr = _make_mock_manager(str(controller_key))
    dynamic_mgr = _make_mock_manager(str(dynamic_key))
    registry._items[controller_key] = controller_mgr
    await registry.add_session(dynamic_mgr)

    with patch(
        "deephaven_mcp.resource_manager._registry_enterprise._fetch_factory_pqs",
        new=AsyncMock(
            return_value=_FactoryQueryError(
                new_client=None, error="RuntimeError: outage"
            )
        ),
    ):
        await registry._sync_enterprise_sessions()

    assert registry._items[controller_key] is controller_mgr
    assert registry._items[dynamic_key] is dynamic_mgr
    assert registry._error is not None
