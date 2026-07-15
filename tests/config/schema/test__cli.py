"""Tests for ``deephaven_mcp.config.schema._cli``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config.schema._cli import (
    CliConfig,
    DaemonControlConfig,
    DaemonReuseAction,
    DaemonReusePolicy,
    DaemonTimeouts,
    DocsConfig,
    DocsTimeouts,
    OutputConfig,
    RequestConfig,
    RequestTimeouts,
    load_cli,
)

# ---------------------------------------------------------------------------
# CliConfig — defaults
# ---------------------------------------------------------------------------


def test_defaults_empty_object() -> None:
    """A missing or empty cli.json yields an all-defaults model."""
    cfg = CliConfig.model_validate({})
    assert cfg.docs == DocsConfig()
    assert cfg.docs.timeouts.request_seconds == 120
    assert cfg.output.format == "json"
    assert cfg.daemon.auto_start is True
    assert cfg.daemon.timeouts.startup_deadline_seconds == 30
    assert cfg.request.timeouts.default_seconds == 60


def test_defaults_subsections_accept_empty_objects() -> None:
    """Each sub-section must accept ``{}`` as an all-defaults override."""
    cfg = CliConfig.model_validate({"output": {}, "daemon": {}, "request": {}})
    assert cfg.output.format == "json"
    assert cfg.daemon.auto_start is True
    assert cfg.daemon.timeouts.startup_deadline_seconds == 30
    assert cfg.request.timeouts.default_seconds == 60


def test_daemon_timeouts_subsection_accepts_empty_object() -> None:
    """``daemon.timeouts: {}`` yields all-defaults for that nested level."""
    cfg = CliConfig.model_validate({"daemon": {"timeouts": {}}})
    assert cfg.daemon.timeouts.startup_deadline_seconds == 30


def test_request_timeouts_subsection_accepts_empty_object() -> None:
    """``request.timeouts: {}`` yields all-defaults for that nested level."""
    cfg = CliConfig.model_validate({"request": {"timeouts": {}}})
    assert cfg.request.timeouts.default_seconds == 60


# ---------------------------------------------------------------------------
# CliConfig — full overrides
# ---------------------------------------------------------------------------


def test_accepts_full_block() -> None:
    cfg = CliConfig.model_validate(
        {
            "output": {"format": "json"},
            "daemon": {
                "auto_start": False,
                "timeouts": {"startup_deadline_seconds": 5},
            },
            "request": {"timeouts": {"default_seconds": 15}},
        }
    )
    assert cfg.output.format == "json"
    assert cfg.daemon.auto_start is False
    assert cfg.daemon.timeouts.startup_deadline_seconds == 5
    assert cfg.request.timeouts.default_seconds == 15


@pytest.mark.parametrize("value", ["human", "json", "yaml"])
def test_accepts_all_output_formats(value: str) -> None:
    cfg = CliConfig.model_validate({"output": {"format": value}})
    assert cfg.output.format == value


# ---------------------------------------------------------------------------
# CliConfig — rejection of malformed inputs
# ---------------------------------------------------------------------------


def test_rejects_unknown_top_level_field() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"bogus": 1})


def test_rejects_unknown_field_in_output_section() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"output": {"color": True}})


def test_rejects_unknown_field_in_daemon_section() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"daemon": {"log_level": "DEBUG"}})


def test_rejects_unknown_field_in_daemon_timeouts() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate(
            {"daemon": {"timeouts": {"idle_shutdown_seconds": 60}}}
        )


def test_rejects_unknown_field_in_request_timeouts() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"request": {"timeouts": {"connect_seconds": 5}}})


def test_rejects_unknown_field_in_request_section() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"request": {"retry_count": 3}})


def test_rejects_invalid_output_format() -> None:
    with pytest.raises(ValidationError, match="format"):
        CliConfig.model_validate({"output": {"format": "xml"}})


def test_rejects_zero_request_default_timeout() -> None:
    with pytest.raises(ValidationError, match="default_seconds"):
        CliConfig.model_validate({"request": {"timeouts": {"default_seconds": 0}}})


def test_rejects_zero_daemon_startup_deadline() -> None:
    with pytest.raises(ValidationError, match="startup_deadline_seconds"):
        CliConfig.model_validate(
            {"daemon": {"timeouts": {"startup_deadline_seconds": 0}}}
        )


def test_daemon_timeouts_kill_after_seconds_default() -> None:
    """``kill_after_seconds`` defaults to 10 (mirrors ``stop_daemon``'s prior literal)."""
    cfg = CliConfig.model_validate({})
    assert cfg.daemon.timeouts.kill_after_seconds == 10


def test_daemon_timeouts_kill_after_seconds_override() -> None:
    """``daemon.timeouts.kill_after_seconds`` accepts an override."""
    cfg = CliConfig.model_validate({"daemon": {"timeouts": {"kill_after_seconds": 5}}})
    assert cfg.daemon.timeouts.kill_after_seconds == 5


def test_rejects_zero_kill_after_seconds() -> None:
    with pytest.raises(ValidationError, match="kill_after_seconds"):
        CliConfig.model_validate({"daemon": {"timeouts": {"kill_after_seconds": 0}}})


def test_top_level_is_frozen() -> None:
    cfg = CliConfig.model_validate({})
    with pytest.raises(ValidationError):
        cfg.output = OutputConfig(format="json")  # type: ignore[misc]


def test_subsections_are_frozen() -> None:
    cfg = CliConfig.model_validate({})
    with pytest.raises(ValidationError):
        cfg.output.format = "json"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        cfg.daemon.timeouts.startup_deadline_seconds = 5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Sub-model construction
# ---------------------------------------------------------------------------


def test_output_config_defaults() -> None:
    assert OutputConfig().format == "json"


def test_daemon_config_defaults() -> None:
    cfg = DaemonControlConfig()
    assert cfg.auto_start is True
    assert isinstance(cfg.timeouts, DaemonTimeouts)
    assert cfg.timeouts.startup_deadline_seconds == 30
    assert isinstance(cfg.reuse, DaemonReusePolicy)


# ---------------------------------------------------------------------------
# DaemonReusePolicy
# ---------------------------------------------------------------------------


def test_reuse_policy_defaults() -> None:
    """version/venv default to refuse; fingerprint defaults to warn."""
    policy = DaemonReusePolicy()
    assert policy.version == "refuse"
    assert policy.venv == "refuse"
    assert policy.fingerprint == "warn"


def test_reuse_policy_defaults_via_cliconfig() -> None:
    """The policy is reachable as ``cli.daemon.reuse`` with defaults."""
    cfg = CliConfig.model_validate({})
    assert cfg.daemon.reuse.version == "refuse"
    assert cfg.daemon.reuse.fingerprint == "warn"


def test_reuse_policy_accepts_per_field_override() -> None:
    cfg = CliConfig.model_validate(
        {
            "daemon": {
                "reuse": {
                    "version": "restart",
                    "venv": "warn",
                    "fingerprint": "ignore",
                }
            }
        }
    )
    assert cfg.daemon.reuse.version == "restart"
    assert cfg.daemon.reuse.venv == "warn"
    assert cfg.daemon.reuse.fingerprint == "ignore"


def test_reuse_policy_accepts_empty_object() -> None:
    """``daemon.reuse: {}`` yields all-defaults for that nested level."""
    cfg = CliConfig.model_validate({"daemon": {"reuse": {}}})
    assert cfg.daemon.reuse.version == "refuse"


def test_reuse_policy_rejects_invalid_action() -> None:
    with pytest.raises(ValidationError, match="version"):
        CliConfig.model_validate({"daemon": {"reuse": {"version": "explode"}}})


def test_reuse_policy_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"daemon": {"reuse": {"python": "refuse"}}})


# ---------------------------------------------------------------------------
# DaemonReuseAction
# ---------------------------------------------------------------------------


def test_reuse_action_string_values() -> None:
    """Each member's string value is the ``cli.json`` token, not the tuple."""
    assert DaemonReuseAction.IGNORE == "ignore"
    assert DaemonReuseAction.WARN == "warn"
    assert DaemonReuseAction.RESTART == "restart"
    assert DaemonReuseAction.REFUSE == "refuse"


def test_reuse_action_severity_is_strictly_increasing() -> None:
    """``severity`` ranks the members least to most severe for the reuse engine."""
    ranks = [
        DaemonReuseAction.IGNORE.severity,
        DaemonReuseAction.WARN.severity,
        DaemonReuseAction.RESTART.severity,
        DaemonReuseAction.REFUSE.severity,
    ]
    assert ranks == sorted(ranks)
    assert len(set(ranks)) == len(ranks)
    assert max(DaemonReuseAction, key=lambda a: a.severity) is DaemonReuseAction.REFUSE


def test_request_config_defaults() -> None:
    cfg = RequestConfig()
    assert isinstance(cfg.timeouts, RequestTimeouts)
    assert cfg.timeouts.default_seconds == 60


# ---------------------------------------------------------------------------
# DocsConfig / DocsTimeouts
# ---------------------------------------------------------------------------


def test_docs_config_defaults() -> None:
    """The default docs endpoint is the Deephaven-hosted production server."""
    cfg = DocsConfig()
    assert cfg.url == "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp"
    assert isinstance(cfg.timeouts, DocsTimeouts)
    assert cfg.timeouts.request_seconds == 120


def test_docs_section_accepts_empty_object() -> None:
    """``docs: {}`` yields all-defaults for that nested level."""
    cfg = CliConfig.model_validate({"docs": {}})
    assert cfg.docs == DocsConfig()


def test_docs_url_override() -> None:
    cfg = CliConfig.model_validate({"docs": {"url": "http://localhost:8001/mcp"}})
    assert cfg.docs.url == "http://localhost:8001/mcp"
    # The untouched sibling timeout keeps its default.
    assert cfg.docs.timeouts.request_seconds == 120


def test_docs_timeout_override() -> None:
    cfg = CliConfig.model_validate({"docs": {"timeouts": {"request_seconds": 300}}})
    assert cfg.docs.timeouts.request_seconds == 300


def test_rejects_zero_docs_request_timeout() -> None:
    with pytest.raises(ValidationError, match="request_seconds"):
        CliConfig.model_validate({"docs": {"timeouts": {"request_seconds": 0}}})


def test_rejects_unknown_field_in_docs_section() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"docs": {"psk": "secret"}})


def test_rejects_unknown_field_in_docs_timeouts() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        CliConfig.model_validate({"docs": {"timeouts": {"connect_seconds": 5}}})


@pytest.mark.parametrize(
    "value",
    [
        "",
        "not a url",
        "localhost:8001/mcp",
        "ftp://docs.example.test/mcp",
        "ws://docs.example.test/mcp",
        "http://",
        "https:///mcp",
        # Hostless authorities: netloc is non-empty but no host exists.
        "http://@/mcp",
        "http://:8000/mcp",
        "http://user:pass@/mcp",
        # urlsplit itself raises ValueError (bracket mismatch).
        "http://[::1/mcp",
        # Malformed ports: .port raises for non-numeric / out-of-range.
        "https://docs.example.test:not-a-port/mcp",
        "https://docs.example.test:99999/mcp",
    ],
)
def test_rejects_non_http_docs_url(value: str) -> None:
    """docs.url must fail eager validation, not a later mcp_request_failed."""
    with pytest.raises(ValidationError, match="http:// or https:// URL"):
        CliConfig.model_validate({"docs": {"url": value}})


@pytest.mark.parametrize(
    "value",
    [
        "http://localhost:8001/mcp",
        "https://docs.example.test/mcp",
    ],
)
def test_accepts_http_and_https_docs_url(value: str) -> None:
    cfg = CliConfig.model_validate({"docs": {"url": value}})
    assert cfg.docs.url == value


# ---------------------------------------------------------------------------
# load_cli
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_cli_returns_defaults_when_absent(tmp_path: Path) -> None:
    """A missing ``cli.json`` resolves to ``CliConfig()`` (all defaults).

    Centralizing the substitution in ``load_cli`` keeps
    :class:`ConfigTree.cli` non-Optional so consumers do not need
    to choose between a parsed value and a default.
    """
    assert await load_cli(tmp_path) == CliConfig()


@pytest.mark.asyncio
async def test_load_cli_returns_config_when_present(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text(json.dumps({"output": {"format": "json"}}))
    cfg = await load_cli(tmp_path)
    assert isinstance(cfg, CliConfig)
    assert cfg.output.format == "json"


@pytest.mark.asyncio
async def test_load_cli_empty_file_yields_defaults(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text("{}")
    cfg = await load_cli(tmp_path)
    assert isinstance(cfg, CliConfig)
    assert cfg.output.format == "json"
    assert cfg.daemon.auto_start is True
    assert cfg.daemon.timeouts.startup_deadline_seconds == 30
    assert cfg.request.timeouts.default_seconds == 60


@pytest.mark.asyncio
async def test_load_cli_full_nested_block(tmp_path: Path) -> None:
    payload = {
        "output": {"format": "yaml"},
        "daemon": {
            "auto_start": False,
            "timeouts": {"startup_deadline_seconds": 7},
        },
        "request": {"timeouts": {"default_seconds": 11}},
    }
    (tmp_path / "cli.json").write_text(json.dumps(payload))
    cfg = await load_cli(tmp_path)
    assert isinstance(cfg, CliConfig)
    assert cfg.output.format == "yaml"
    assert cfg.daemon.auto_start is False
    assert cfg.daemon.timeouts.startup_deadline_seconds == 7
    assert cfg.request.timeouts.default_seconds == 11


@pytest.mark.asyncio
async def test_load_cli_validates_format(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text(json.dumps({"output": {"format": "xml"}}))
    with pytest.raises(ConfigurationError, match="format"):
        await load_cli(tmp_path)


@pytest.mark.asyncio
async def test_load_cli_rejects_unknown_subsection_key(tmp_path: Path) -> None:
    (tmp_path / "cli.json").write_text(json.dumps({"daemon": {"log_level": "DEBUG"}}))
    with pytest.raises(ConfigurationError, match="Extra inputs"):
        await load_cli(tmp_path)
