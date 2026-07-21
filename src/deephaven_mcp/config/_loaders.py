"""Shared JSON-to-Pydantic loader helpers.

Extracts the recurring "load JSON file -> validate against Pydantic
model -> redact-log" pipeline. Each public helper takes the model
class, the source file path, and the audited configuration root; it
returns a validated model instance or raises
:class:`~deephaven_mcp._exceptions.ConfigurationError` on any failure.

Callers: the `dhcli` CLI's ``cli.json`` loader
(:mod:`deephaven_mcp.config.schema._cli`) and every per-section loader
under :mod:`deephaven_mcp.config.schema` (``server.json``,
``community/settings.json``, ``community/sessions/<name>.json``,
``enterprise/settings.json``, ``enterprise/systems/<name>.json``).

Two variants:

- :func:`load_named_json` — loads a single configuration file whose
  contents map directly onto the model fields (e.g. ``cli.json``,
  ``server.json``, ``community/settings.json``).
- :func:`load_named_json_with_stem` — loads a per-name file inside a
  section directory (e.g. ``community/sessions/<name>.json``,
  ``enterprise/systems/<name>.json``); the filename stem is injected
  as the ``name`` field before validation.
"""

from __future__ import annotations

__all__ = [
    "load_named_json",
    "load_named_json_with_stem",
]

import logging
from pathlib import Path

from pydantic import BaseModel, ValidationError

from deephaven_mcp._pydantic import as_configuration_error, log_redacted
from deephaven_mcp.config._file_loader import load_config_from_file


async def load_named_json[M: BaseModel](
    model_cls: type[M],
    *,
    path: Path,
    config_dir: Path,
    error_label: str,
    log_label: str,
    logger: logging.Logger,
) -> M:
    """Load a JSON file and validate it against ``model_cls``.

    Reads ``path`` via
    :func:`deephaven_mcp.config._file_loader.load_config_from_file`
    (which already enforces a JSON-object top-level shape, returns
    a templated dict, and wraps any I/O or parse failure as
    :class:`~deephaven_mcp._exceptions.ConfigurationError`), then
    validates the result against ``model_cls`` and emits a redacted
    audit-trail log line.

    Args:
        model_cls (type[M]): Pydantic v2 model class to validate
            against.
        path (Path): Full path to the configuration file. Must exist
            and be readable; absence is the caller's responsibility
            to detect before invoking this helper.
        config_dir (Path): The audited configuration root, forwarded
            to the file loader so ``${file:PATH}`` placeholders
            inside the file are resolved against it.
        error_label (str): Human-readable file label (e.g.
            ``"community/settings.json"``) used in the
            :class:`~deephaven_mcp._exceptions.ConfigurationError`
            message when validation fails.
        log_label (str): Label prefix for the redacted audit-trail
            log line emitted on successful validation.
        logger (logging.Logger): Logger used for the redacted
            audit-trail line.

    Returns:
        M: The validated model instance.

    Raises:
        ConfigurationError: When the file cannot be loaded, parsed,
            template-expanded, or validated against ``model_cls``.
    """
    raw = await load_config_from_file(str(path), config_dir=config_dir)
    try:
        model = model_cls.model_validate(raw)
    except ValidationError as exc:
        raise as_configuration_error(error_label, exc) from exc
    log_redacted(model, label=log_label, logger=logger)
    return model


async def load_named_json_with_stem[M: BaseModel](
    model_cls: type[M],
    *,
    path: Path,
    config_dir: Path,
    error_label: str,
    log_label: str,
    logger: logging.Logger,
) -> M:
    """Load a per-name JSON file, injecting the filename stem as ``name``.

    Used by the community-session and enterprise-system loaders
    where every file's stem is the canonical session/system name and
    the on-disk JSON omits it. The stem is merged into the raw dict
    under the ``"name"`` key before validation; an existing
    ``"name"`` field in the file overrides it (the per-section
    schema's filename-vs-name validator catches the mismatch).

    Args:
        model_cls (type[M]): Pydantic v2 model class to validate
            against.
        path (Path): Full path to the per-name JSON file. The
            filename stem becomes the model's ``name``.
        config_dir (Path): The audited configuration root, forwarded
            to the file loader for ``${file:PATH}`` resolution.
        error_label (str): Human-readable file label used in the
            :class:`~deephaven_mcp._exceptions.ConfigurationError`
            message when validation fails.
        log_label (str): Label prefix for the redacted audit-trail
            log line emitted on successful validation.
        logger (logging.Logger): Logger used for the redacted
            audit-trail line.

    Returns:
        M: The validated model instance with ``name`` set to the
            file's stem (unless the file's JSON overrode it).

    Raises:
        ConfigurationError: When the file cannot be loaded, parsed,
            template-expanded, or validated against ``model_cls``.
    """
    name = path.stem
    raw = await load_config_from_file(str(path), config_dir=config_dir)
    try:
        model = model_cls.model_validate({"name": name, **raw})
    except ValidationError as exc:
        raise as_configuration_error(error_label, exc) from exc
    log_redacted(model, label=log_label, logger=logger)
    return model
