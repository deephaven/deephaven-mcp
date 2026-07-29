"""End-to-end tests for :class:`deephaven_mcp.config.tree.ConfigTreeLoader`.

These tests drive the manager against real on-disk fixture trees and
exercise the cooperation between :mod:`_tree`, :mod:`_config_dir`,
the per-file schema/loader modules (``_server``, ``_community``,
``_enterprise``), and :mod:`_dir_permissions`.

The directory-resolution helpers themselves are exercised in
``test__config_dir.py``.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from deephaven_mcp._exceptions import (
    ConfigurationError,
    InternalError,
)
from deephaven_mcp._taxonomy import SystemRef, SystemType
from deephaven_mcp.auth.credentials import (
    PasswordCredentials,
    PrivateKeyCredentials,
)
from deephaven_mcp.config._data_root import DATA_DIR_ENV_VAR, _default_data_root
from deephaven_mcp.config.tree import (
    ConfigTreeLoader,
    no_systems_configured_message,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload))
    return path


def _lock_down(config_dir: Path) -> None:
    """Apply user-only POSIX modes to ``config_dir`` and everything below it."""
    if sys.platform == "win32":
        return
    config_dir.chmod(0o700)
    for child in config_dir.rglob("*"):
        child.chmod(0o700 if child.is_dir() else 0o600)


def _make_manager(config_dir: Path) -> ConfigTreeLoader:
    """Lock down ``config_dir`` and return an *uninitialized* manager."""
    _lock_down(config_dir)
    return ConfigTreeLoader(config_dir)


async def _load(config_dir: Path) -> ConfigTreeLoader:
    """Lock down ``config_dir``, initialize a manager, and return it."""
    mgr = _make_manager(config_dir)
    await mgr.initialize()
    return mgr


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    """Return a permission-clean empty config directory."""
    return _make_dir(tmp_path / "config")


# ---------------------------------------------------------------------------
# config_dir resolution via the manager
# ---------------------------------------------------------------------------


def test_manager_config_dir_explicit_wins(tmp_path: Path) -> None:
    explicit = tmp_path / "explicit"
    with patch.dict(os.environ, {DATA_DIR_ENV_VAR: str(tmp_path / "env")}):
        assert ConfigTreeLoader(explicit).config_dir == explicit


def test_manager_config_dir_uses_data_root_env_var(tmp_path: Path) -> None:
    """With no explicit arg, the manager uses ``$DH_AI_DATA_DIR/config``."""
    root = str(tmp_path / "data_root")
    with patch.dict(os.environ, {DATA_DIR_ENV_VAR: root}):
        assert ConfigTreeLoader().config_dir == Path(root) / "config"


def test_manager_config_dir_default_when_env_unset() -> None:
    env = {k: v for k, v in os.environ.items() if k != DATA_DIR_ENV_VAR}
    with patch.dict(os.environ, env, clear=True):
        assert ConfigTreeLoader().config_dir == _default_data_root() / "config"


# ---------------------------------------------------------------------------
# ConfigTreeLoader: empty / required-system handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_empty_config_dir_loads_zero_system_tree(config_dir: Path) -> None:
    """A zero-system tree loads cleanly; the invariant is a consumer concern."""
    manager = _make_manager(config_dir)
    cfg = await manager.initialize()
    assert cfg.community is None
    assert cfg.enterprise is None
    assert cfg.has_usable_system() is False
    assert cfg.list_systems() == []


@pytest.mark.asyncio
async def test_missing_dir_fails(tmp_path: Path) -> None:
    manager = ConfigTreeLoader(tmp_path / "nope")
    with pytest.raises(ConfigurationError, match="does not exist"):
        await manager.initialize()


@pytest.mark.asyncio
async def test_config_before_initialize_raises(config_dir: Path) -> None:
    manager = _make_manager(config_dir)
    with pytest.raises(InternalError, match="before initialize"):
        _ = manager.config


@pytest.mark.asyncio
async def test_config_after_failed_initialize_still_raises(
    config_dir: Path,
) -> None:
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    (sessions_dir / "bad.json").write_text("{ not valid json")
    manager = _make_manager(config_dir)
    with pytest.raises(ConfigurationError):
        await manager.initialize()
    with pytest.raises(InternalError, match="before initialize"):
        _ = manager.config


@pytest.mark.asyncio
async def test_initialize_twice_raises(config_dir: Path) -> None:
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {
            "session_name": "local",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    manager = await _load(config_dir)
    with pytest.raises(InternalError, match="more than once"):
        await manager.initialize()


# ---------------------------------------------------------------------------
# server.json
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_json_with_literal_psk(config_dir: Path) -> None:
    _write_json(config_dir / "server.json", {"psk": "topsecret"})
    # Need at least one system to satisfy the no-systems check.
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {
            "session_name": "local",
            "host": "localhost",
            "port": 10000,
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.server is not None
    assert cfg.server.psk is not None
    assert cfg.server.psk.get_secret_value() == "topsecret"


@pytest.mark.asyncio
async def test_server_json_with_env_var_template(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PSK pulled from an env var via templating syntax in the JSON."""
    monkeypatch.setenv("MY_PSK", "from-env")
    _write_json(config_dir / "server.json", {"psk": "${env:MY_PSK}"})
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {
            "session_name": "local",
            "host": "localhost",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.server is not None
    assert cfg.server.psk is not None
    assert cfg.server.psk.get_secret_value() == "from-env"


@pytest.mark.asyncio
async def test_server_json_missing_env_var_template(config_dir: Path) -> None:
    """An unset env var in a ``${env:...}`` placeholder is a config error."""
    _write_json(config_dir / "server.json", {"psk": "${env:DEFINITELY_UNSET}"})
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {
            "session_name": "local",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    with pytest.raises(ConfigurationError, match="DEFINITELY_UNSET"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_server_json_rejects_legacy_psk_env_var_field(config_dir: Path) -> None:
    _write_json(config_dir / "server.json", {"psk_env_var": "Y"})
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {"auth": {"credentials": {"type": "anonymous"}}},
    )
    with pytest.raises(ConfigurationError, match="Extra inputs"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_server_json_unknown_field(config_dir: Path) -> None:
    _write_json(config_dir / "server.json", {"bogus_field": "x"})
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {"auth": {"credentials": {"type": "anonymous"}}},
    )
    with pytest.raises(ConfigurationError, match="Extra inputs"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_server_json_must_be_dict(config_dir: Path) -> None:
    (config_dir / "server.json").write_text("[]")
    if sys.platform != "win32":
        (config_dir / "server.json").chmod(0o600)
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {"auth": {"credentials": {"type": "anonymous"}}},
    )
    with pytest.raises(ConfigurationError, match="must contain a JSON object"):
        await _load(config_dir)


# ---------------------------------------------------------------------------
# Community section
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_community_settings_only_no_sessions_is_not_a_system(
    config_dir: Path,
) -> None:
    """A settings-only file (no sessions, no session_creation) is not a system."""
    community_dir = _make_dir(config_dir / "community")
    _write_json(
        community_dir / "settings.json",
        {"timeouts": {"eviction": {"session_idle_timeout_seconds": 30}}},
    )
    cfg = (await _load(config_dir)).config
    assert cfg.community is not None
    assert cfg.has_usable_system() is False
    assert cfg.list_systems() == []


@pytest.mark.asyncio
async def test_community_settings_with_session_creation_counts_as_system(
    config_dir: Path,
) -> None:
    community_dir = _make_dir(config_dir / "community")
    _write_json(
        community_dir / "settings.json",
        {
            "session_creation": {"max_concurrent_sessions": 2},
            "timeouts": {"eviction": {"session_idle_timeout_seconds": 30}},
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.community is not None
    assert cfg.community.sessions == {}
    assert cfg.community.settings.timeouts.eviction.session_idle_timeout_seconds == 30.0
    assert cfg.has_usable_system() is True
    assert cfg.list_systems() == [
        SystemRef(name="community", type=SystemType.COMMUNITY)
    ]


@pytest.mark.asyncio
async def test_community_sessions_only_no_settings(config_dir: Path) -> None:
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "alpha.json",
        {
            "session_name": "alpha",
            "host": "h",
            "port": 1,
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.community is not None
    # An empty CommunitySettings still surfaces effective timer
    # defaults via Pydantic field defaults.
    assert (
        cfg.community.settings.timeouts.eviction.session_idle_timeout_seconds == 3600.0
    )
    assert cfg.community.settings.timeouts.eviction.sweep_interval_seconds == 60.0
    assert "alpha" in cfg.community.sessions
    assert cfg.has_usable_system() is True
    assert cfg.list_systems() == [
        SystemRef(name="community", type=SystemType.COMMUNITY)
    ]


@pytest.mark.asyncio
async def test_community_session_filename_must_match_session_name(
    config_dir: Path,
) -> None:
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "alpha.json",
        {
            "session_name": "beta",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    with pytest.raises(ConfigurationError, match="session_name"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_community_session_must_be_object(config_dir: Path) -> None:
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    bad = sessions_dir / "alpha.json"
    bad.write_text("[]")
    if sys.platform != "win32":
        bad.chmod(0o600)
    with pytest.raises(ConfigurationError, match="must contain a JSON object"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_community_settings_invalid_field(config_dir: Path) -> None:
    community_dir = _make_dir(config_dir / "community")
    _write_json(community_dir / "settings.json", {"unknown": 1})
    with pytest.raises(ConfigurationError, match="Extra inputs"):
        await _load(config_dir)


# ---------------------------------------------------------------------------
# Enterprise section
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enterprise_settings_only_no_systems_is_not_a_system(
    config_dir: Path,
) -> None:
    """A settings-only enterprise section (no systems) is not a system."""
    enterprise_dir = _make_dir(config_dir / "enterprise")
    _write_json(enterprise_dir / "settings.json", {})
    cfg = (await _load(config_dir)).config
    assert cfg.enterprise is not None
    assert cfg.has_usable_system() is False
    assert cfg.list_systems() == []


@pytest.mark.asyncio
async def test_enterprise_password_with_literal(config_dir: Path) -> None:
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "alice",
                    "password": "hunter2",
                }
            },
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.enterprise is not None
    sys_cfg = cfg.enterprise.systems["prod"]
    assert isinstance(sys_cfg.auth.credentials, PasswordCredentials)
    assert sys_cfg.auth.credentials.username == "alice"
    assert sys_cfg.auth.credentials.password.get_secret_value() == "hunter2"


@pytest.mark.asyncio
async def test_enterprise_password_via_env_template(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Password pulled from env via ``${env:NAME}`` templating."""
    monkeypatch.setenv("PROD_PW", "secretpw")
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "alice",
                    "password": "${env:PROD_PW}",
                }
            },
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.enterprise is not None
    creds = cfg.enterprise.systems["prod"].auth.credentials
    assert isinstance(creds, PasswordCredentials)
    assert creds.password.get_secret_value() == "secretpw"


@pytest.mark.asyncio
async def test_enterprise_private_key_via_file_template(config_dir: Path) -> None:
    """Private key pulled from disk via ``${file:PATH}`` templating."""
    key_path = config_dir / "id.pem"
    key_text = "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n"
    key_path.write_text(key_text)
    if sys.platform != "win32":
        key_path.chmod(0o600)
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "private_key",
                    "key_text": "${file:" + str(key_path) + "}",
                }
            },
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.enterprise is not None
    creds = cfg.enterprise.systems["prod"].auth.credentials
    assert isinstance(creds, PrivateKeyCredentials)
    assert creds.key_text.get_secret_value() == key_text


@pytest.mark.asyncio
async def test_enterprise_private_key_unreadable(config_dir: Path) -> None:
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "private_key",
                    "key_text": "${file:" + str(config_dir / "no-such-file") + "}",
                }
            },
        },
    )
    with pytest.raises(ConfigurationError, match="does not exist"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_enterprise_private_key_not_utf8(config_dir: Path) -> None:
    key_path = config_dir / "binary.bin"
    key_path.write_bytes(b"\xff\xfe\x00")
    if sys.platform != "win32":
        key_path.chmod(0o600)
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "private_key",
                    "key_text": "${file:" + str(key_path) + "}",
                }
            },
        },
    )
    with pytest.raises(ConfigurationError, match="not valid UTF-8"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_enterprise_dotted_filename_stem_rejected(config_dir: Path) -> None:
    """A dotted stem (e.g. prod.east.json) is not a valid system name."""
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.east.json",
        {
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    with pytest.raises(ConfigurationError, match="name"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_enterprise_filename_must_match_system_name(config_dir: Path) -> None:
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "different",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    with pytest.raises(ConfigurationError, match="system_name"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_enterprise_settings_must_be_empty(config_dir: Path) -> None:
    enterprise_dir = _make_dir(config_dir / "enterprise")
    _write_json(enterprise_dir / "settings.json", {"foo": 1})
    systems_dir = _make_dir(enterprise_dir / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    with pytest.raises(ConfigurationError, match="Extra inputs"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_enterprise_invalid_auth_type(config_dir: Path) -> None:
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {"credentials": {"type": "saml"}},
        },
    )
    with pytest.raises(ConfigurationError):
        await _load(config_dir)


# ---------------------------------------------------------------------------
# Aggregated error reporting + caching + list_systems
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_errors_aggregated_across_sections(config_dir: Path) -> None:
    _write_json(config_dir / "server.json", {"unknown": True})
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "alpha.json",
        {
            "session_name": "wrong",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    with pytest.raises(ConfigurationError) as exc:
        await _load(config_dir)
    text = str(exc.value)
    assert "server.json" in text
    assert "community" in text


@pytest.mark.asyncio
async def test_caching(config_dir: Path) -> None:
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "x.json",
        {
            "session_name": "x",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    manager = await _load(config_dir)
    # `config` is a property; repeated reads return the same object.
    assert manager.config is manager.config


@pytest.mark.asyncio
async def test_list_systems_lists_all(
    config_dir: Path,
) -> None:
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {
            "session_name": "local",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    _write_json(
        systems_dir / "stage.json",
        {
            "system_name": "stage",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    cfg = (await _load(config_dir)).config
    systems = cfg.list_systems()
    assert systems == [
        SystemRef(name="community", type=SystemType.COMMUNITY),
        SystemRef(name="prod", type=SystemType.ENTERPRISE),
        SystemRef(name="stage", type=SystemType.ENTERPRISE),
    ]
    # Backward-compat: NamedTuple instances equal plain (name, type_str) tuples.
    assert systems[0] == ("community", "community")
    assert systems[1] == ("prod", "enterprise")
    # Field access also works.
    assert systems[0].name == "community"
    assert systems[0].type is SystemType.COMMUNITY


# ---------------------------------------------------------------------------
# no_systems_configured_message
# ---------------------------------------------------------------------------


def test_no_systems_configured_message_names_dir_and_every_remedy(
    tmp_path: Path,
) -> None:
    """The shared zero-system guidance names the directory and all three fixes.

    Both enforcement points (systems-server startup, CLI daemon
    acquisition) emit this one string, so it must stay actionable: the
    offending directory plus each way to declare a system.
    """
    message = no_systems_configured_message(tmp_path)
    assert str(tmp_path) in message
    assert "community/sessions/" in message
    assert "session_creation" in message
    assert "enterprise/systems/" in message
    assert "dhcli config init" in message


# ---------------------------------------------------------------------------
# community_sessions / enterprise_systems
# ---------------------------------------------------------------------------


def _write_community_session(config_dir: Path, name: str) -> None:
    """Declare one anonymous static community session named ``name``."""
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / f"{name}.json",
        {
            "session_name": name,
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )


def _write_enterprise_system(config_dir: Path, name: str) -> None:
    """Declare one password-auth enterprise system named ``name``."""
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / f"{name}.json",
        {
            "system_name": name,
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )


@pytest.mark.asyncio
async def test_accessors_empty_when_no_sections(config_dir: Path) -> None:
    """Both accessors read an absent section as no entries, not ``None``."""
    cfg = (await _load(config_dir)).config
    assert cfg.community is None
    assert cfg.enterprise is None
    assert cfg.community_sessions == {}
    assert cfg.enterprise_systems == {}


@pytest.mark.asyncio
async def test_community_sessions_returns_declared_sessions(
    config_dir: Path,
) -> None:
    """The community accessor keys declarations by session name."""
    _write_community_session(config_dir, "local")
    cfg = (await _load(config_dir)).config
    sessions = cfg.community_sessions
    assert set(sessions) == {"local"}
    assert sessions["local"].name == "local"
    # The enterprise counterpart stays empty for a community-only tree.
    assert cfg.enterprise_systems == {}


@pytest.mark.asyncio
async def test_enterprise_systems_returns_declared_systems(
    config_dir: Path,
) -> None:
    """The enterprise accessor keys declarations by system name."""
    _write_enterprise_system(config_dir, "prod")
    _write_enterprise_system(config_dir, "stage")
    cfg = (await _load(config_dir)).config
    systems = cfg.enterprise_systems
    assert set(systems) == {"prod", "stage"}
    assert systems["prod"].connection_json_url == "https://x/connection.json"
    # The community counterpart stays empty for an enterprise-only tree.
    assert cfg.community_sessions == {}


@pytest.mark.asyncio
async def test_accessors_are_the_underlying_section_maps(config_dir: Path) -> None:
    """Each accessor exposes its section's own map, not a copy of it."""
    _write_community_session(config_dir, "local")
    _write_enterprise_system(config_dir, "prod")
    cfg = (await _load(config_dir)).config
    assert cfg.community is not None
    assert cfg.enterprise is not None
    assert cfg.community_sessions is cfg.community.sessions
    assert cfg.enterprise_systems is cfg.enterprise.systems


@pytest.mark.asyncio
async def test_settings_only_sections_yield_empty_accessors(
    config_dir: Path,
) -> None:
    """A settings-only section is present yet declares nothing.

    The empty result therefore does not imply an absent section, which is
    why consumers needing that distinction (the registry builder pairing
    each section with its client timeouts) read the section attribute.
    """
    _write_json(_make_dir(config_dir / "community") / "settings.json", {})
    _write_json(_make_dir(config_dir / "enterprise") / "settings.json", {})
    cfg = (await _load(config_dir)).config
    assert cfg.community is not None
    assert cfg.enterprise is not None
    assert cfg.community_sessions == {}
    assert cfg.enterprise_systems == {}


@pytest.mark.asyncio
async def test_idle_timeout_settings_round_trip(config_dir: Path) -> None:
    community_dir = _make_dir(config_dir / "community")
    _write_json(
        community_dir / "settings.json",
        {
            "timeouts": {
                "eviction": {
                    "session_idle_timeout_seconds": 5,
                    "sweep_interval_seconds": 1,
                },
            },
        },
    )
    sessions_dir = _make_dir(community_dir / "sessions")
    _write_json(
        sessions_dir / "x.json",
        {
            "session_name": "x",
            "auth": {"credentials": {"type": "anonymous"}},
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.community is not None
    assert cfg.community.settings.timeouts.eviction.session_idle_timeout_seconds == 5
    assert cfg.community.settings.timeouts.eviction.sweep_interval_seconds == 1


@pytest.mark.asyncio
async def test_enterprise_idle_timeout_fields_round_trip(config_dir: Path) -> None:
    """Enterprise idle/sweep timers live on settings (system-wide)."""
    enterprise_dir = _make_dir(config_dir / "enterprise")
    _write_json(
        enterprise_dir / "settings.json",
        {
            "timeouts": {
                "eviction": {
                    "session_idle_timeout_seconds": 11,
                    "sweep_interval_seconds": 2,
                },
            },
        },
    )
    systems_dir = _make_dir(enterprise_dir / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.enterprise is not None
    assert cfg.enterprise.settings.timeouts.eviction.session_idle_timeout_seconds == 11
    assert cfg.enterprise.settings.timeouts.eviction.sweep_interval_seconds == 2


@pytest.mark.asyncio
async def test_enterprise_defaults_when_optionals_absent(config_dir: Path) -> None:
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    cfg = (await _load(config_dir)).config
    assert cfg.enterprise is not None
    # Idle/sweep defaults live on EnterpriseSettings now.
    assert (
        cfg.enterprise.settings.timeouts.eviction.session_idle_timeout_seconds == 3600.0
    )
    assert cfg.enterprise.settings.timeouts.eviction.sweep_interval_seconds == 60.0
    # Connection timeout for factory construction is the global
    # ``timeouts.client.session_connect_timeout_seconds`` (no per-system
    # override); the per-system field was retired.
    assert (
        cfg.enterprise.settings.timeouts.client.session_connect_timeout_seconds == 60.0
    )


@pytest.mark.asyncio
async def test_server_json_empty_dict_yields_no_psk(config_dir: Path) -> None:
    """An empty server.json is legal; neither psk nor psk_env_var set."""
    _write_json(config_dir / "server.json", {})
    sessions_dir = _make_dir(config_dir / "community" / "sessions")
    _write_json(
        sessions_dir / "local.json",
        {"auth": {"credentials": {"type": "anonymous"}}},
    )
    cfg = (await _load(config_dir)).config
    assert cfg.server is not None
    assert cfg.server.psk is None


@pytest.mark.asyncio
async def test_enterprise_system_name_community_rejected(config_dir: Path) -> None:
    """A file named ``community.json`` under enterprise/systems/ is rejected."""
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    _write_json(
        systems_dir / "community.json",
        {
            "system_name": "community",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "u",
                    "password": "p",
                }
            },
        },
    )
    with pytest.raises(ConfigurationError, match="'community' is reserved"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_enterprise_system_file_must_be_object(config_dir: Path) -> None:
    """A non-object JSON value under enterprise/systems/ is rejected."""
    systems_dir = _make_dir(config_dir / "enterprise" / "systems")
    bad = systems_dir / "prod.json"
    bad.write_text("[]")
    if sys.platform != "win32":
        bad.chmod(0o600)
    with pytest.raises(ConfigurationError, match="must contain a JSON object"):
        await _load(config_dir)


@pytest.mark.asyncio
async def test_enterprise_settings_json_loads_and_logs(
    config_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An ``enterprise/settings.json`` file is loaded and logged at INFO."""
    enterprise_dir = _make_dir(config_dir / "enterprise")
    _write_json(enterprise_dir / "settings.json", {})  # empty schema today
    # A settings-only tree fails the no-systems check; declare one system.
    systems_dir = _make_dir(enterprise_dir / "systems")
    _write_json(
        systems_dir / "prod.json",
        {
            "system_name": "prod",
            "connection_json_url": "https://x/connection.json",
            "auth": {
                "credentials": {
                    "type": "password",
                    "username": "alice",
                    "password": "hunter2",
                }
            },
        },
    )
    with caplog.at_level("INFO", logger="deephaven_mcp.config.schema._enterprise"):
        cfg = (await _load(config_dir)).config
    assert cfg.enterprise is not None
    msgs = " ".join(rec.message for rec in caplog.records)
    assert "enterprise/settings.json" in msgs


# ---------------------------------------------------------------------------
# Bundled example config tree
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_examples_ai_config_loads_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bundled ``config-samples/ai/config/`` tree validates against the schemas.

    Guards ``docs/CONFIGURATION.md``'s claim that the example tree is
    "copy-paste-ready": copies it to a tmp dir, supplies the env vars
    referenced by ``${env:...}`` placeholders, retargets the
    ``${file:...}`` placeholder at a tmp keyfile, and runs the same
    :class:`ConfigTreeLoader` the production lifespan uses.
    """
    import shutil

    repo_root = Path(__file__).resolve().parents[2]
    src = repo_root / "config-samples" / "ai" / "config"
    assert src.is_dir(), f"missing example tree at {src}"

    config_dir = tmp_path / "config"
    shutil.copytree(src, config_dir)

    # Retarget the ${file:...} placeholder at a real, readable file
    # *inside* the configuration directory. The templating engine
    # refuses ``${file:...}`` references that resolve outside the
    # audited configuration root.
    keyfile = config_dir / "staging-key.pem"
    keyfile.write_text("-----BEGIN FAKE KEY-----\nx\n-----END FAKE KEY-----\n")
    keyfile.chmod(0o600)
    staging_path = config_dir / "enterprise" / "systems" / "staging.json"
    staging_path.write_text(
        staging_path.read_text().replace("/etc/deephaven/staging-key.pem", str(keyfile))
    )

    # Provide the env vars the example references via ${env:...}.
    monkeypatch.setenv("DH_MCP_PSK", "test-server-psk")
    monkeypatch.setenv("DH_LOCAL_DEV_PSK", "test-local-psk")
    monkeypatch.setenv("DH_DYNAMIC_SESSION_TOKEN", "test-dynamic-psk")
    monkeypatch.setenv("DH_PROD_PASSWORD", "test-prod-pw")

    cfg = (await _load(config_dir)).config

    # server.json
    assert cfg.server is not None
    assert cfg.server.psk is not None
    assert cfg.server.psk.get_secret_value() == "test-server-psk"

    # community section
    assert cfg.community is not None
    assert "local_dev" in cfg.community.sessions
    assert cfg.community.settings.session_creation is not None

    # enterprise systems
    assert cfg.enterprise is not None
    assert set(cfg.enterprise.systems) == {"prod", "staging"}
    prod = cfg.enterprise.systems["prod"]
    assert isinstance(prod.auth.credentials, PasswordCredentials)
    assert prod.auth.credentials.password.get_secret_value() == "test-prod-pw"
    staging = cfg.enterprise.systems["staging"]
    assert isinstance(staging.auth.credentials, PrivateKeyCredentials)
    assert staging.auth.credentials.key_text.get_secret_value().startswith(
        "-----BEGIN FAKE KEY-----"
    )
