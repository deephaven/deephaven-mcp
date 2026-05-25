"""Async JSON/JSON5 file loader with templating.

Provides :func:`load_config_from_file`, the package-wide low-level
reader used to materialize a JSON5 file on disk into a
fully-resolved Python ``dict`` ready for Pydantic validation. The
function reads the file with :mod:`aiofiles`, parses it with
:mod:`json5`, and then runs the result through
:func:`deephaven_mcp.config._templating.expand_tree` so the caller
never sees raw ``${env:...}`` / ``${file:...}`` placeholders. Any
I/O, parse, or templating failure is surfaced as
:class:`~deephaven_mcp._exceptions.ConfigurationError`.

Lives next to :mod:`deephaven_mcp.config._templating` inside the
:mod:`deephaven_mcp.config` package because together they form a
small, self-contained config-loading primitive layer. Any subpackage
that loads a JSON configuration file — the per-section loaders in
:mod:`deephaven_mcp.mcp_systems_server.config` today, the docs
server or other tooling tomorrow — can import it directly.
"""

__all__ = [
    "load_config_from_file",
]

import logging
from pathlib import Path
from typing import Any, cast

import aiofiles
import json5

from deephaven_mcp._exceptions import ConfigurationError

from ._templating import expand_tree

_LOGGER = logging.getLogger(__name__)


async def load_config_from_file(
    config_path: str, *, config_dir: Path | None = None
) -> dict[str, Any]:
    """Load, parse, and template-expand a JSON/JSON5 configuration file.

    After parsing the file via JSON5 the in-memory tree is passed
    through :func:`deephaven_mcp.config._templating.expand_tree`, which
    resolves every ``${env:VAR}`` / ``${env:VAR:-default}`` /
    ``${file:PATH}`` placeholder before the caller hands the dict to a
    Pydantic model. Callers therefore see only fully-resolved values
    and never have to look at parallel ``<field>_env_var`` /
    ``<field>_path`` shadow fields.

    Args:
        config_path (str): Path to the configuration file.
        config_dir (Path | None): Optional audited configuration
            directory used to constrain ``${file:PATH}`` placeholder
            resolution. When supplied, ``${file:...}`` placeholders
            must reference files inside the directory; symlinks are
            refused and a size cap is enforced. When ``None``, the
            placeholder accepts any absolute path (backward-compatible
            default); the caller falls back to the parent directory of
            ``config_path`` so a freestanding loader call still has a
            meaningful containment root.

    Returns:
        dict[str, Any]: The parsed and template-expanded file contents
            as a dictionary. The parsed root is checked at runtime to
            be a JSON object; a non-mapping top-level value raises
            :class:`ConfigurationError`.

    Raises:
        ConfigurationError: When the file cannot be read or parsed,
            when the parsed top-level value is not a JSON object, or
            when any placeholder cannot be resolved (missing env var,
            missing file, malformed syntax, etc.). Wraps
            :class:`FileNotFoundError`, :class:`PermissionError`,
            :class:`ValueError` (raised by ``json5.loads``,
            including :class:`json.JSONDecodeError`), and any other
            unexpected error.
    """
    effective_config_dir = (
        config_dir if config_dir is not None else Path(config_path).parent
    )
    try:
        async with aiofiles.open(config_path) as f:
            content = await f.read()
        parsed = json5.loads(content)
        if not isinstance(parsed, dict):
            raise ConfigurationError(
                f"Configuration file {config_path} must contain a JSON "
                f"object at the top level, got {type(parsed).__name__}."
            )
        return cast(
            dict[str, Any],
            expand_tree(parsed, source=config_path, config_dir=effective_config_dir),
        )
    except ConfigurationError:
        raise
    except FileNotFoundError as e:
        _LOGGER.error(
            f"[_file_loader:load_config_from_file] Configuration file not "
            f"found: {config_path}: {e!r}",
            exc_info=True,
        )
        raise ConfigurationError(f"Configuration file not found: {config_path}") from e
    except PermissionError as e:
        _LOGGER.error(
            f"[_file_loader:load_config_from_file] Permission denied when "
            f"reading configuration file: {config_path}: {e!r}",
            exc_info=True,
        )
        raise ConfigurationError(
            f"Permission denied when reading configuration file: {config_path}"
        ) from e
    except ValueError as e:
        _LOGGER.error(
            f"[_file_loader:load_config_from_file] Invalid JSON/JSON5 in "
            f"configuration file {config_path}: {e!r}",
            exc_info=True,
        )
        raise ConfigurationError(
            f"Invalid JSON/JSON5 in configuration file {config_path}: {e}"
        ) from e
    except Exception as e:
        _LOGGER.error(
            f"[_file_loader:load_config_from_file] Unexpected error reading "
            f"configuration file {config_path}: {e!r}",
            exc_info=True,
        )
        raise ConfigurationError(
            f"Unexpected error loading or parsing config file {config_path}: {e}"
        ) from e
