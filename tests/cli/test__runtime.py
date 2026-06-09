"""Tests for ``deephaven_mcp.cli._runtime``."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._runtime import Runtime, load_runtime
from deephaven_mcp.cli.config import CliConfig
from deephaven_mcp.mcp_systems_server.config import ConfigTree


def _seed_minimal_config_dir(d: Path) -> None:
    """Write the minimum tree the loader needs to load successfully."""
    d.mkdir(parents=True, exist_ok=True)
    (d / "community").mkdir()
    (d / "community" / "sessions").mkdir()
    session_path = d / "community" / "sessions" / "demo.json"
    session_path.write_text(
        json.dumps(
            {
                "host": "localhost",
                "port": 10000,
                "auth": {"credentials": {"type": "anonymous"}},
            }
        )
    )
    # The directory-permission audit refuses anything looser than 0o700/0o600.
    for sub in (d, d / "community", d / "community" / "sessions"):
        os.chmod(sub, 0o700)
    os.chmod(session_path, 0o600)


# ---------------------------------------------------------------------------
# load_runtime: happy paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_runtime_returns_populated_runtime(tmp_path: Path) -> None:
    """``load_runtime`` resolves paths and validates the entire tree eagerly."""
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "runtime"
    _seed_minimal_config_dir(cfg_dir)
    runtime = await load_runtime(
        config_dir_override=cfg_dir, runtime_dir_override=runtime_dir
    )
    assert isinstance(runtime, Runtime)
    assert runtime.config_dir == cfg_dir
    assert runtime.runtime_dir == runtime_dir
    assert isinstance(runtime.config, ConfigTree)
    assert isinstance(runtime.config.cli, CliConfig)
    assert runtime.daemon_dir.path == runtime_dir / "daemon"


@pytest.mark.asyncio
async def test_load_runtime_picks_up_cli_json(tmp_path: Path) -> None:
    """The ``cli.json`` value lands on ``runtime.config.cli``."""
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    cli_json = cfg_dir / "cli.json"
    cli_json.write_text(json.dumps({"output": {"format": "json"}}))
    os.chmod(cli_json, 0o600)
    runtime = await load_runtime(
        config_dir_override=cfg_dir, runtime_dir_override=tmp_path / "rt"
    )
    assert runtime.config.cli.output.format == "json"


@pytest.mark.asyncio
async def test_load_runtime_substitutes_defaults_when_cli_absent(
    tmp_path: Path,
) -> None:
    """An absent ``cli.json`` produces an all-defaults :class:`CliConfig`."""
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    runtime = await load_runtime(
        config_dir_override=cfg_dir, runtime_dir_override=tmp_path / "rt"
    )
    assert runtime.config.cli == CliConfig()


@pytest.mark.asyncio
async def test_load_runtime_applies_cli_overrides(tmp_path: Path) -> None:
    """``cli_overrides`` replace the corresponding ``CliConfig`` sub-models."""
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    base = CliConfig()
    runtime = await load_runtime(
        config_dir_override=cfg_dir,
        runtime_dir_override=tmp_path / "rt",
        cli_overrides={"output": base.output.model_copy(update={"format": "json"})},
    )
    assert runtime.config.cli.output.format == "json"
    # Other sections stay at defaults.
    assert runtime.config.cli.daemon.auto_start is True


@pytest.mark.asyncio
async def test_load_runtime_overrides_win_over_disk_value(
    tmp_path: Path,
) -> None:
    """When ``cli.json`` and ``cli_overrides`` disagree, the override wins."""
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    cli_json = cfg_dir / "cli.json"
    cli_json.write_text(json.dumps({"output": {"format": "yaml"}}))
    os.chmod(cli_json, 0o600)

    base = CliConfig()
    runtime = await load_runtime(
        config_dir_override=cfg_dir,
        runtime_dir_override=tmp_path / "rt",
        cli_overrides={"output": base.output.model_copy(update={"format": "json"})},
    )
    assert runtime.config.cli.output.format == "json"


# ---------------------------------------------------------------------------
# load_runtime: directory handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX-only permission check")
async def test_load_runtime_locks_runtime_dir_to_0700(tmp_path: Path) -> None:
    """``load_runtime`` chmod's ``runtime_dir`` to ``0o700`` on POSIX."""
    import stat

    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    rt = tmp_path / "rt"
    rt.mkdir()
    os.chmod(rt, 0o755)  # Looser than 0o700; should be tightened.

    await load_runtime(config_dir_override=cfg_dir, runtime_dir_override=rt)

    mode = stat.S_IMODE(rt.stat().st_mode)
    assert mode == 0o700, f"runtime_dir mode {oct(mode)} != 0o700"


@pytest.mark.asyncio
async def test_load_runtime_creates_runtime_dir_when_missing(
    tmp_path: Path,
) -> None:
    """``load_runtime`` creates ``runtime_dir`` (and parents) when absent."""
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    rt = tmp_path / "nested" / "rt"
    assert not rt.exists()

    runtime = await load_runtime(config_dir_override=cfg_dir, runtime_dir_override=rt)
    assert rt.is_dir()
    assert runtime.runtime_dir == rt


# ---------------------------------------------------------------------------
# load_runtime: failure modes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_runtime_rejects_missing_config_dir(tmp_path: Path) -> None:
    """A non-existent config dir fails the load with ``CONFIG_INVALID``.

    Eager validation: the configuration directory is part of the
    audited surface, so ``dh-mcp`` cannot operate without it.
    """
    cfg_dir = tmp_path / "does-not-exist"
    rt = tmp_path / "rt"
    with pytest.raises(CliError) as excinfo:
        await load_runtime(config_dir_override=cfg_dir, runtime_dir_override=rt)
    assert excinfo.value.code == ErrorCode.CONFIG_INVALID


@pytest.mark.asyncio
async def test_load_runtime_rejects_malformed_cli_json(tmp_path: Path) -> None:
    """A broken ``cli.json`` is a hard ``CONFIG_INVALID`` error.

    Eager validation: ``cli.json`` is part of the configuration tree
    and is not given the best-effort treatment the previous design
    used. Recovery is to fix the file (the error message names it).
    """
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    cli_json = cfg_dir / "cli.json"
    cli_json.write_text("{ not valid json")
    os.chmod(cli_json, 0o600)

    with pytest.raises(CliError) as excinfo:
        await load_runtime(
            config_dir_override=cfg_dir, runtime_dir_override=tmp_path / "rt"
        )
    assert excinfo.value.code == ErrorCode.CONFIG_INVALID
    assert isinstance(excinfo.value.__cause__, ConfigurationError)


@pytest.mark.asyncio
async def test_load_runtime_rejects_malformed_systems_section(
    tmp_path: Path,
) -> None:
    """A malformed ``community/`` session file surfaces as ``CONFIG_INVALID``."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    os.chmod(cfg_dir, 0o700)
    (cfg_dir / "community").mkdir()
    os.chmod(cfg_dir / "community", 0o700)
    (cfg_dir / "community" / "sessions").mkdir()
    os.chmod(cfg_dir / "community" / "sessions", 0o700)
    bad = cfg_dir / "community" / "sessions" / "bad.json"
    bad.write_text("{ not valid json")
    os.chmod(bad, 0o600)

    with pytest.raises(CliError) as excinfo:
        await load_runtime(
            config_dir_override=cfg_dir, runtime_dir_override=tmp_path / "rt"
        )
    assert excinfo.value.code == ErrorCode.CONFIG_INVALID
    assert isinstance(excinfo.value.__cause__, ConfigurationError)
