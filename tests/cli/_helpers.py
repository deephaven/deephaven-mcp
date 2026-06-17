"""Shared CLI test helpers."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from pydantic import SecretStr

from deephaven_mcp.cli._runtime import Runtime
from deephaven_mcp.config.schema import CliConfig, ServerConfig
from deephaven_mcp.config.tree import ConfigTree
from deephaven_mcp.daemon_registry import DaemonRegistryEntry


def make_runtime(
    tmp_path: Path | None = None,
    *,
    output_format: str | None = None,
    **overrides: object,
) -> Runtime:
    """Construct a :class:`Runtime` populated with safe defaults.

    Bypasses the on-disk loader path entirely: the resulting
    :class:`ConfigTree` is hand-built so tests do not need a real
    config directory. Override ``config`` to supply a different
    tree, or ``daemon_dir`` to swap in a custom mock.

    ``output_format`` is a convenience for the common case of pinning the
    rendered output mode (e.g. ``"human"``): the CLI now defaults to
    ``json``, so tests asserting human-formatted output must request it
    explicitly. Ignored when an explicit ``cli_config`` override is given.
    """
    base = tmp_path or Path("/tmp")
    cli_config = overrides.get("cli_config")
    if cli_config is None:
        cli_config = (
            CliConfig(output={"format": output_format})
            if output_format is not None
            else CliConfig()
        )
    config = overrides.get("config") or ConfigTree(
        config_dir=base / "cfg",
        cli=cli_config,  # type: ignore[arg-type]
        server=ServerConfig(),
    )
    return Runtime(
        config_dir=base / "cfg",
        runtime_dir=base / "rt",
        config=config,  # type: ignore[arg-type]
        daemon_dir=overrides.get("daemon_dir") or MagicMock(),  # type: ignore[arg-type]
    )


def fake_load_runtime(
    runtime: Runtime,
) -> Callable[..., Coroutine[Any, Any, Runtime]]:
    """Build a stand-in for :func:`load_runtime` that honors ``cli_overrides``.

    The CLI's root callback turns top-level flags (``-o``,
    ``--timeout``, ``--no-auto-start``) into a ``cli_overrides``
    dict and passes it to :func:`load_runtime`. Tests that patch
    ``load_runtime`` to return a fixed :class:`Runtime` lose that
    plumbing — the runtime they supply has all-defaults output
    mode no matter what flags they pass to ``CliRunner.invoke``.

    This helper closes the gap: the returned coroutine accepts the
    real signature, applies ``cli_overrides`` to the supplied
    runtime's ``config.cli`` via :meth:`pydantic.BaseModel.model_copy`,
    and returns a fresh :class:`Runtime`. Tests can therefore pass
    ``-o json`` and assert on JSON output without constructing a
    JSON-mode runtime upfront.
    """

    async def _load(
        *,
        config_dir_override: Path | None = None,
        runtime_dir_override: Path | None = None,
        cli_overrides: dict[str, object] | None = None,
    ) -> Runtime:
        if not cli_overrides:
            return runtime
        new_cli = runtime.config.cli.model_copy(update=cli_overrides)
        new_config = runtime.config.model_copy(update={"cli": new_cli})
        return Runtime(
            config_dir=runtime.config_dir,
            runtime_dir=runtime.runtime_dir,
            config=new_config,
            daemon_dir=runtime.daemon_dir,
        )

    return _load


def locked_session(runtime: Runtime) -> MagicMock:
    """Return the mock ``LockedRegistry`` yielded by ``daemon_dir.locked()``.

    For runtimes whose ``daemon_dir`` is a :class:`MagicMock`, the
    ``with runtime.daemon_dir.locked() as reg:`` block binds ``reg``
    to ``daemon_dir.locked.return_value.__enter__.return_value``.
    Tests configure that mock's ``read`` / ``delete`` / ``quarantine``
    here and assert against them.
    """
    return runtime.daemon_dir.locked.return_value.__enter__.return_value  # type: ignore[union-attr]


def make_entry() -> DaemonRegistryEntry:
    """Return a deterministic :class:`DaemonRegistryEntry` for tests."""
    return DaemonRegistryEntry.model_validate(
        {
            "pid": 1,
            "create_time_ns": 1_700_000_000_000_000_000,
            "process_name": "python",
            "host": "127.0.0.1",
            "port": 9999,
            "psk": SecretStr("x" * 16),
            "started_at": datetime(2026, 5, 27, 0, 0, 0, tzinfo=UTC),
            "config_dir": Path("/tmp"),
            "server_name": "dh-test",
        }
    )
