"""Tests for ``deephaven_mcp.cli._commands._acquire``."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from deephaven_mcp.cli._commands import _acquire as acquire_mod
from deephaven_mcp.cli._commands._acquire import (
    acquire_daemon,
    registry_corrupt_message,
)
from deephaven_mcp.cli._daemon import DaemonClientError, DaemonStartupTimeoutError
from deephaven_mcp.cli._errors import CliError, ErrorCode
from deephaven_mcp.daemon_registry import RegistryCorruptError

from .._helpers import make_entry, make_runtime


def _corrupt_to_error(exc: RegistryCorruptError) -> CliError:
    return CliError(str(exc), code=ErrorCode.DAEMON_REGISTRY_CORRUPT)


# ---------------------------------------------------------------------------
# registry_corrupt_message
# ---------------------------------------------------------------------------


def test_registry_corrupt_message_contents() -> None:
    msg = registry_corrupt_message(
        RegistryCorruptError("malformed"), retry_command="dh-mcp daemon start"
    )
    assert "malformed" in msg
    assert "dh-mcp daemon repair" in msg
    assert "Re-run `dh-mcp daemon start`." in msg


# ---------------------------------------------------------------------------
# acquire_daemon
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acquire_daemon_returns_entry(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    entry = make_entry()
    with patch.object(
        acquire_mod, "get_or_start_daemon", AsyncMock(return_value=entry)
    ):
        result = await acquire_daemon(
            rt,
            auto_start=True,
            client_error_code=ErrorCode.DAEMON_NOT_RUNNING,
            on_registry_corrupt=_corrupt_to_error,
        )
    assert result is entry


@pytest.mark.asyncio
async def test_acquire_daemon_maps_startup_timeout(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        acquire_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonStartupTimeoutError("slow")),
    ):
        with pytest.raises(CliError) as excinfo:
            await acquire_daemon(
                rt,
                auto_start=True,
                client_error_code=ErrorCode.DAEMON_NOT_RUNNING,
                on_registry_corrupt=_corrupt_to_error,
            )
    assert excinfo.value.code is ErrorCode.DAEMON_STARTUP_TIMEOUT


@pytest.mark.asyncio
async def test_acquire_daemon_client_error_uses_provided_code(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    with patch.object(
        acquire_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=DaemonClientError("nope")),
    ):
        with pytest.raises(CliError) as excinfo:
            await acquire_daemon(
                rt,
                auto_start=False,
                client_error_code=ErrorCode.DAEMON_NOT_RUNNING,
                on_registry_corrupt=_corrupt_to_error,
            )
    assert excinfo.value.code is ErrorCode.DAEMON_NOT_RUNNING


@pytest.mark.asyncio
async def test_acquire_daemon_registry_corrupt_delegates(tmp_path: Path) -> None:
    rt = make_runtime(tmp_path)
    seen: dict[str, RegistryCorruptError] = {}

    def on_corrupt(exc: RegistryCorruptError) -> CliError:
        seen["exc"] = exc
        return CliError("recovery", code=ErrorCode.DAEMON_REGISTRY_CORRUPT)

    with patch.object(
        acquire_mod,
        "get_or_start_daemon",
        AsyncMock(side_effect=RegistryCorruptError("malformed")),
    ):
        with pytest.raises(CliError) as excinfo:
            await acquire_daemon(
                rt,
                auto_start=True,
                client_error_code=ErrorCode.DAEMON_NOT_RUNNING,
                on_registry_corrupt=on_corrupt,
            )
    assert excinfo.value.code is ErrorCode.DAEMON_REGISTRY_CORRUPT
    assert excinfo.value.format_message() == "recovery"
    assert isinstance(seen["exc"], RegistryCorruptError)
