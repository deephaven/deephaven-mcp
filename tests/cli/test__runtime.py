"""Tests for ``deephaven_mcp.cli._runtime``."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.cli._runtime import (
    Runtime,
    RuntimeSpec,
    apply_cli_overrides,
    load_runtime,
)
from deephaven_mcp.config.schema import CliConfig
from deephaven_mcp.config.tree import ConfigTree


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
# RuntimeSpec.resolve
# ---------------------------------------------------------------------------


def test_runtime_spec_resolve_builds_runtime(tmp_path: Path) -> None:
    """``resolve`` runs ``load_runtime`` with the spec's fields (sync caller)."""
    cfg_dir = tmp_path / "cfg"
    runtime_dir = tmp_path / "runtime"
    _seed_minimal_config_dir(cfg_dir)
    spec = RuntimeSpec(
        config_dir_override=cfg_dir,
        runtime_dir_override=runtime_dir,
        cli_overrides={"output": {"format": "yaml"}},
    )
    runtime = spec.resolve()
    assert isinstance(runtime, Runtime)
    assert runtime.config_dir == cfg_dir
    assert runtime.runtime_dir == runtime_dir
    assert runtime.config.cli.output.format == "yaml"


def test_runtime_spec_resolve_propagates_config_error(tmp_path: Path) -> None:
    """A malformed config surfaces from ``resolve`` as ``CONFIG_INVALID``."""
    spec = RuntimeSpec(
        config_dir_override=tmp_path / "missing",
        runtime_dir_override=tmp_path / "runtime",
    )
    with pytest.raises(CliError) as exc_info:
        spec.resolve()
    assert exc_info.value.code is ErrorCode.CONFIG_INVALID


def test_runtime_spec_defaults_are_none() -> None:
    """An all-defaults spec matches ``load_runtime``'s keyword defaults."""
    spec = RuntimeSpec()
    assert spec.config_dir_override is None
    assert spec.runtime_dir_override is None
    assert spec.cli_overrides is None


# ---------------------------------------------------------------------------
# load_runtime: happy paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_runtime_returns_populated_runtime(tmp_path: Path) -> None:
    """``load_runtime`` resolves paths and validates the entire tree in one pass."""
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


def test_apply_cli_overrides_merges_and_preserves_siblings() -> None:
    """Nested overrides win at the leaves; untouched siblings survive."""
    base = CliConfig.model_validate({"docs": {"url": "https://docs.example.test/mcp"}})
    out = apply_cli_overrides(base, {"docs": {"timeouts": {"request_seconds": 7}}})
    assert out.docs.timeouts.request_seconds == 7
    assert out.docs.url == "https://docs.example.test/mcp"
    # The input model is not mutated.
    assert base.docs.timeouts.request_seconds == 120


def test_apply_cli_overrides_revalidates() -> None:
    """An out-of-range override fails CliConfig validation."""
    with pytest.raises(ValidationError):
        apply_cli_overrides(
            CliConfig(), {"request": {"timeouts": {"default_seconds": 0}}}
        )


@pytest.mark.asyncio
async def test_load_runtime_applies_cli_overrides(tmp_path: Path) -> None:
    """``cli_overrides`` deep-merge into the loaded ``CliConfig``."""
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    runtime = await load_runtime(
        config_dir_override=cfg_dir,
        runtime_dir_override=tmp_path / "rt",
        cli_overrides={"output": {"format": "json"}},
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

    runtime = await load_runtime(
        config_dir_override=cfg_dir,
        runtime_dir_override=tmp_path / "rt",
        cli_overrides={"output": {"format": "json"}},
    )
    assert runtime.config.cli.output.format == "json"


@pytest.mark.asyncio
async def test_load_runtime_overrides_preserve_sibling_fields(
    tmp_path: Path,
) -> None:
    """A nested override keeps on-disk siblings it does not touch."""
    cfg_dir = tmp_path / "cfg"
    _seed_minimal_config_dir(cfg_dir)
    cli_json = cfg_dir / "cli.json"
    cli_json.write_text(json.dumps({"docs": {"url": "https://docs.example.test/mcp"}}))
    os.chmod(cli_json, 0o600)

    runtime = await load_runtime(
        config_dir_override=cfg_dir,
        runtime_dir_override=tmp_path / "rt",
        cli_overrides={"docs": {"timeouts": {"request_seconds": 7}}},
    )
    # The --timeout-style override lands...
    assert runtime.config.cli.docs.timeouts.request_seconds == 7
    # ...without clobbering the configured docs URL.
    assert runtime.config.cli.docs.url == "https://docs.example.test/mcp"


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

    Total validation: the configuration directory is part of the
    audited surface, so ``dhcli`` cannot operate without it.
    """
    cfg_dir = tmp_path / "does-not-exist"
    rt = tmp_path / "rt"
    with pytest.raises(CliError) as excinfo:
        await load_runtime(config_dir_override=cfg_dir, runtime_dir_override=rt)
    assert excinfo.value.code == ErrorCode.CONFIG_INVALID


@pytest.mark.asyncio
async def test_load_runtime_rejects_malformed_cli_json(tmp_path: Path) -> None:
    """A broken ``cli.json`` is a hard ``CONFIG_INVALID`` error.

    Total validation: ``cli.json`` is part of the configuration tree
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
