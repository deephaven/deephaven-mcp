"""Tests for ``deephaven_mcp.cli._commands.config``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner

from deephaven_mcp.cli import _main
from deephaven_mcp.cli._main import cli
from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.cli.config import CliConfig
from deephaven_mcp.mcp_systems_server.config import ConfigTree, ServerConfig

from .._helpers import fake_load_runtime, make_runtime


def _invoke(args: list[str], runtime: Runtime):
    runner = CliRunner()
    with patch.object(_main, "load_runtime", fake_load_runtime(runtime)):
        return runner.invoke(cli, args)


# ---------------------------------------------------------------------------
# config show
# ---------------------------------------------------------------------------


def test_config_show_outputs_paths_and_models(tmp_path: Path) -> None:
    """Default tree (cli + server populated, others absent) dumps cleanly."""
    rt = make_runtime(tmp_path)
    result = _invoke(["-o", "json", "config", "show"], rt)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["config_dir"] == str(rt.config_dir)
    assert "cli" in payload
    assert "server" in payload
    assert "community" not in payload
    assert "enterprise" not in payload


def test_config_show_includes_community_and_enterprise_when_present(
    tmp_path: Path,
) -> None:
    """When the tree has all four sections, all four appear in the dump."""
    from deephaven_mcp.mcp_systems_server.config import (
        CommunityConfig,
        CommunitySettings,
        EnterpriseConfig,
        EnterpriseSettings,
    )

    config = ConfigTree(
        config_dir=tmp_path / "cfg",
        cli=CliConfig(),
        server=ServerConfig(),
        community=CommunityConfig(settings=CommunitySettings(), sessions={}),
        enterprise=EnterpriseConfig(settings=EnterpriseSettings(), systems={}),
    )
    rt = make_runtime(tmp_path, config=config)
    result = _invoke(["-o", "json", "config", "show"], rt)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert "community" in payload
    assert "enterprise" in payload


def test_config_show_redacts_secrets(tmp_path: Path) -> None:
    """Secret-bearing fields under any section are redacted in the dump."""
    from pydantic import SecretStr

    from deephaven_mcp.mcp_systems_server.config import (
        DaemonConfig,
        ServerConfig,
    )

    server = ServerConfig(
        psk=SecretStr("supersecret-token"),
        daemon=DaemonConfig(),
    )
    config = ConfigTree(config_dir=tmp_path / "cfg", cli=CliConfig(), server=server)
    rt = make_runtime(tmp_path, config=config)
    result = _invoke(["-o", "json", "config", "show"], rt)
    assert result.exit_code == 0, result.output
    assert "supersecret-token" not in result.output


# ---------------------------------------------------------------------------
# config validate
# ---------------------------------------------------------------------------


def test_config_validate_success(tmp_path: Path) -> None:
    """``config validate`` returns the success payload when load_runtime succeeds.

    Validation happens during runtime construction (``load_runtime``);
    ``config validate``'s body just renders the success record. We
    inject a pre-built ``Runtime`` to mimic that pre-validated state.
    """
    rt = make_runtime(tmp_path)
    result = _invoke(["-o", "json", "config", "validate"], rt)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["valid"] is True
    assert payload["config_dir"] == str(rt.config_dir)


def test_config_validate_failure_propagates_load_error(tmp_path: Path) -> None:
    """A ``CliError`` raised from ``load_runtime`` produces exit code 2.

    ``config validate``'s only job is to confirm the load succeeded.
    Any failure is detected upstream in ``_main``'s root callback,
    which raises before dispatching here.
    """
    from deephaven_mcp.cli._errors import CliError, ErrorCode

    runner = CliRunner()
    with patch.object(
        _main,
        "load_runtime",
        AsyncMock(side_effect=CliError("oops", code=ErrorCode.CONFIG_INVALID)),
    ):
        result = runner.invoke(cli, ["-o", "json", "config", "validate"])
    assert result.exit_code == 2


def test_config_validate_help_describes_eager_validation() -> None:
    """The help reflects eager validation, not a per-verb re-load.

    Regression test: the help previously claimed the verb "re-loads the
    configuration tree from disk," contradicting the handler (which only
    reports the already-validated state).
    """
    from deephaven_mcp.cli._commands.config import config as config_group

    help_text = config_group.commands["validate"].help or ""
    assert "eagerly on every" in help_text
    assert "re-load" not in help_text.lower()
