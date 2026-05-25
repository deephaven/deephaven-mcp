"""Project-wide Pydantic base classes and helpers.

This module owns the cross-cutting Pydantic plumbing every schema in
the project sits on top of. It contains zero domain-specific logic
(no concrete config, session, credential, or TLS schema lives here)
and only depends on :mod:`pydantic` and the project's exception /
redaction primitives.

Contents:

*Base classes*

- :class:`StrictSchema` - :class:`pydantic.BaseModel` with
  ``extra="forbid"`` and ``frozen=True``. Every schema in the
  project inherits from this, directly or via :class:`RedactableSchema`.
- :class:`RedactableSchema` - :class:`StrictSchema` plus a
  ``model_serializer`` that handles :class:`pydantic.SecretStr`
  fields under two opt-in ``model_dump`` contexts:
  ``{"redact": True}`` (replace with :data:`REDACTED`) and
  ``{"reveal": True}`` (emit the plain-text value). Schemas that
  hold secrets inherit from this class.

*Error adapters*

- :func:`format_validation_error` - translate a Pydantic
  :class:`~pydantic.ValidationError` into the project's
  :class:`~deephaven_mcp._exceptions.ConfigurationError` message
  style.
- :func:`as_configuration_error` - build a
  :class:`~deephaven_mcp._exceptions.ConfigurationError` from a
  caught :class:`~pydantic.ValidationError`.

*Model validators* (helpers for ``@model_validator(mode="before")``)

- :func:`unwrap_auth_credentials` - unwrap the wire-format
  ``auth.credentials`` block into a top-level ``credentials`` field.
- :func:`reconcile_filename_stem` - cross-check an optional
  declared name field against the filename stem carried via
  ``name``.

*Logging*

- :func:`log_redacted` - log a Pydantic model at INFO with secrets
  redacted (uses ``model_dump(context={"redact": True})`` under the
  hood).
"""

from __future__ import annotations

__all__ = [
    "as_configuration_error",
    "format_validation_error",
    "log_redacted",
    "reconcile_filename_stem",
    "unwrap_auth_credentials",
    "RedactableSchema",
    "StrictSchema",
]

import logging
import types
from typing import Any, Union, get_args, get_origin

import json5
from pydantic import (
    BaseModel,
    ConfigDict,
    SecretStr,
    ValidationError,
    model_serializer,
)
from pydantic.fields import FieldInfo

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._redaction import REDACTED

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base classes
# ---------------------------------------------------------------------------


class StrictSchema(BaseModel):
    """Common base for every Pydantic schema in the project.

    Subclasses inherit strict validation (no unknown fields), frozen
    instances after construction, and runtime-introspectable field
    descriptions harvested from PEP 257 trailing docstrings. The
    project's default pydantic coercion behavior is kept
    (``strict=False``) so that JSON inputs decoded from disk parse
    the same way as ``json5`` returns them.

    See :attr:`model_config` for the specific settings.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, use_attribute_docstrings=True
    )
    """Project-wide Pydantic configuration shared by every schema:

    - ``extra="forbid"`` — unknown JSON keys raise a validation
      error rather than being silently ignored.
    - ``frozen=True`` — instances are immutable after construction.
    - ``use_attribute_docstrings=True`` — Pydantic harvests the
      trailing PEP 257 string literal beneath each field declaration
      into :attr:`pydantic.fields.FieldInfo.description`. That string
      reaches runtime consumers (``model.model_fields[name].description``,
      ``model.model_json_schema()``, MCP tool schemas,
      FastAPI/OpenAPI generators) which a Sphinx-style ``Attributes:``
      block does not.
    """


class RedactableSchema(StrictSchema):
    """:class:`StrictSchema` plus context-aware SecretStr handling.

    Subclasses gain two opt-in ``model_dump`` modes, both selected by
    the ``context`` argument to :meth:`pydantic.BaseModel.model_dump`:

    - ``context={"redact": True}`` — every field typed as
      :class:`pydantic.SecretStr` (including ``SecretStr | None``
      and ``Annotated[SecretStr, ...]``) is emitted as :data:`REDACTED`
      instead of the default ``"**********"`` mask. This is the
      project's canonical log-output mode.
    - ``context={"reveal": True}`` — every :class:`SecretStr` field
      is emitted as its plain-text value (the result of
      :meth:`SecretStr.get_secret_value`). Internal-only: used at the
      registry-to-manager handoff where downstream code needs the
      actual secret to pass to the underlying client SDK.

    Without either context flag the model serializes per pydantic's
    defaults — masked secrets, unwrapped non-secret fields.
    """

    @model_serializer(mode="wrap")
    def _redact_serializer(self, handler: Any, info: Any) -> dict[str, Any]:
        """Apply context-driven secret handling on top of the default dump.

        Args:
            handler (Any): Pydantic-provided callable that runs the
                default serialization.
            info (Any): Pydantic-provided
                :class:`pydantic.SerializationInfo` carrying the
                caller's ``context`` mapping.

        Returns:
            dict[str, Any]: The default serialization with each
                :class:`SecretStr` field replaced according to the
                active mode (``redact`` → :data:`REDACTED`,
                ``reveal`` → plain text), or unchanged when no
                mode flag is set.
        """
        data: dict[str, Any] = handler(self)
        if not info.context:
            return data
        redact = info.context.get("redact")
        reveal = info.context.get("reveal")
        if not (redact or reveal):
            return data
        for name, field in type(self).model_fields.items():
            if not _is_secret_field(field):
                continue
            if redact:
                if data.get(name) is not None:
                    data[name] = REDACTED
            else:  # reveal
                value = getattr(self, name)
                if isinstance(value, SecretStr):
                    data[name] = value.get_secret_value()
        return data


def _is_secret_field(field: FieldInfo) -> bool:
    """Return ``True`` if ``field``'s annotation contains :class:`SecretStr`.

    Recognizes bare ``SecretStr``, ``SecretStr | None`` (and the
    legacy ``Optional[SecretStr]`` / ``Union[SecretStr, None]``
    forms), and ``Annotated[SecretStr, ...]``.

    Args:
        field (FieldInfo): The Pydantic-provided per-field metadata
            object.

    Returns:
        bool: ``True`` when the annotation includes
            :class:`pydantic.SecretStr`.
    """
    return _annotation_contains_secret(field.annotation)


def _annotation_contains_secret(ann: Any) -> bool:
    """Recursively check whether ``ann`` references :class:`SecretStr`."""
    if ann is SecretStr:
        return True
    # Match both ``typing.Union[A, B]`` and PEP 604 ``A | B`` syntax;
    # the latter has ``types.UnionType`` as its origin. Recurse into
    # every arg so ``str | SecretStr``, ``None | SecretStr``, etc. are
    # detected regardless of position.
    if get_origin(ann) in (Union, types.UnionType):
        return any(_annotation_contains_secret(a) for a in get_args(ann))
    # Annotated[...] surfaces as a typing form whose first arg is the
    # underlying type; pydantic also flattens this for us, but be
    # defensive.
    args = get_args(ann)
    if args:
        return _annotation_contains_secret(args[0])
    return False


# ---------------------------------------------------------------------------
# Error formatting
# ---------------------------------------------------------------------------


def format_validation_error(context: str, exc: ValidationError) -> str:
    """Translate a Pydantic :class:`ValidationError` into the project's style.

    Produces a one-line summary of every reported error, each prefixed
    by the JSON-pointer-style ``loc`` path (e.g.
    ``"session_creation.defaults.heap_size_gb"``). When multiple
    errors are present they are joined with ``"; "`` so the result
    fits on a single log line.

    Args:
        context (str): Identifier of the surrounding config used as
            the message prefix (e.g. ``"enterprise system 'prod'"``
            or ``"server.json"``).
        exc (ValidationError): The exception raised by
            :meth:`pydantic.BaseModel.model_validate`.

    Returns:
        str: A human-readable error message ready for embedding in a
            :class:`ConfigurationError`.
    """
    parts: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        parts.append(f"{loc}: {err['msg']}")
    return f"{context}: " + "; ".join(parts)


def log_redacted(model: BaseModel, *, label: str, logger: logging.Logger) -> None:
    """Log a Pydantic model at INFO with secrets redacted.

    Dumps the model with ``mode="json"`` and ``context={"redact": True}``
    so any field typed as :class:`pydantic.SecretStr` is replaced with
    the project's :data:`~deephaven_mcp._redaction.REDACTED` sentinel
    (subclasses of :class:`RedactableSchema`); non-secret fields are
    emitted verbatim. Falls back to ``repr(model)`` and a WARNING when
    :func:`json5.dumps` raises :class:`TypeError` or :class:`ValueError`
    on the dumped dict.

    Args:
        model (BaseModel): The validated Pydantic model to log. Should
            inherit from :class:`RedactableSchema` so the ``redact``
            context flag is honored; plain :class:`StrictSchema`
            subclasses log their non-secret fields verbatim.
        label (str): Log prefix label identifying the caller (e.g.
            ``"_community:load_community/settings.json"``).
        logger (logging.Logger): The caller's logger; the resulting
            log records carry the caller's module name as their
            ``name`` attribute.
    """
    logger.info(f"[{label}] Configuration summary:")
    try:
        redacted = model.model_dump(mode="json", context={"redact": True})
        formatted = json5.dumps(redacted, indent=2, sort_keys=True)
        logger.info(f"[{label}] Loaded configuration:\n{formatted}")
    except (TypeError, ValueError) as e:
        logger.warning(f"[{label}] Failed to format config as JSON: {e}")
        logger.info(f"[{label}] Loaded configuration: {model!r}")


def as_configuration_error(context: str, exc: ValidationError) -> ConfigurationError:
    """Build a :class:`ConfigurationError` from a Pydantic error.

    Centralizes the ``ValidationError`` → :class:`ConfigurationError`
    translation so loaders raise a consistent message style. The
    returned exception is intended for the caller to re-raise (e.g.
    ``raise as_configuration_error(ctx, exc) from exc``).

    Args:
        context (str): Identifier of the surrounding config used as
            the message prefix.
        exc (ValidationError): The exception raised by
            :meth:`pydantic.BaseModel.model_validate`.

    Returns:
        ConfigurationError: The translated exception, ready to
            ``raise ... from exc``.
    """
    msg = format_validation_error(context, exc)
    _LOGGER.error(f"[deephaven_mcp._pydantic:as_configuration_error] {msg}")
    return ConfigurationError(msg)


def unwrap_auth_credentials(data: Any, *, allow_top_level: bool = True) -> Any:
    """Unwrap a ``{"auth": {"credentials": ...}}`` wrapper into top-level ``credentials``.

    The community session, enterprise system, and community session-
    creation defaults JSON schemas all nest credentials under
    ``auth.credentials``. This helper performs the unwrap in a
    ``model_validator(mode="before")`` so the resulting Pydantic field
    can be a single
    :class:`~deephaven_mcp.auth.credentials.CredentialsUnion`.

    Two wire-format shapes are recognized:

    - ``{"auth": {"credentials": ...}}`` — the on-disk shape.
    - ``{"credentials": ...}`` — the post-``model_dump`` round-trip
      shape; only accepted when ``allow_top_level`` is ``True``.

    The two shapes are mutually exclusive.

    Args:
        data (Any): The raw mapping passed to
            :meth:`pydantic.BaseModel.model_validate`. Non-dict inputs
            pass through unchanged.
        allow_top_level (bool): When ``True`` (the default), a
            pre-unwrapped ``{"credentials": ...}`` mapping is accepted
            (the shape produced by ``model_dump`` round-trips); when
            absent, an explicit ``ValueError`` requests credentials.
            When ``False``, ``auth`` is the only accepted source and
            absence of both ``auth`` and ``credentials`` is allowed
            (used by settings sub-blocks where credentials are
            optional).

    Returns:
        Any: A new mapping with ``auth`` removed and ``credentials``
            populated when ``auth`` was present. Non-dict inputs are
            returned unchanged. When ``allow_top_level`` is ``False``
            and neither key is present, the (copied) input is
            returned unchanged.

    Raises:
        ValueError: When ``auth`` is malformed (not a dict, contains
            keys other than ``credentials``, or lacks ``credentials``),
            when both forms are present simultaneously, or when
            ``allow_top_level`` is ``True`` and neither form is
            present.
    """
    if not isinstance(data, dict):
        return data
    out = dict(data)
    auth = out.pop("auth", None)
    has_unwrapped = out.get("credentials") is not None
    if auth is None and not has_unwrapped:
        if allow_top_level:
            raise ValueError(
                "'auth' is required. Use "
                "{'credentials': {'type': 'anonymous'}} for anonymous "
                "callers."
            )
        # Sub-block variant: missing auth is allowed.
        return out
    if auth is not None and has_unwrapped:
        raise ValueError(
            "'auth' and 'credentials' are mutually exclusive at the "
            "top level; supply only one."
        )
    if auth is not None:
        if not isinstance(auth, dict) or set(auth.keys()) - {"credentials"}:
            raise ValueError("'auth' must contain only a 'credentials' sub-block.")
        if "credentials" not in auth:
            raise ValueError("'auth.credentials' is required when 'auth' is set.")
        out["credentials"] = auth["credentials"]
    return out


def reconcile_filename_stem(
    data: Any,
    *,
    declared_field: str,
    model_label: str,
) -> Any:
    """Validate ``name`` and reconcile it with an optional declared-field shadow.

    Both :class:`~deephaven_mcp.sessions.CommunitySessionConfig` and
    :class:`~deephaven_mcp.sessions.EnterpriseSystemConfig` use the
    loader-supplied ``name`` (typically the filename stem) as the
    canonical identifier, but the on-disk file may *also* declare
    ``session_name`` / ``system_name`` for human-friendly readability.
    This helper enforces that, when both are present, they agree --
    and rejects an empty or non-string ``name`` outright.

    Args:
        data (Any): The raw mapping passed to
            :meth:`pydantic.BaseModel.model_validate`. Non-dict inputs
            pass through unchanged.
        declared_field (str): The JSON field name whose value (when
            present) must equal ``name`` (e.g. ``"session_name"`` or
            ``"system_name"``). Removed from the returned mapping when
            present.
        model_label (str): The model class name used in the error
            message (e.g. ``"CommunitySessionConfig"``).

    Returns:
        Any: A copied mapping with ``declared_field`` removed, or the
            original input unchanged when it is not a dict.

    Raises:
        ValueError: When ``name`` is absent, empty, or not a string,
            or when ``declared_field`` is present and disagrees with
            ``name``.
    """
    if not isinstance(data, dict):
        return data
    out = dict(data)
    name = out.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"'name' is required when constructing a {model_label} "
            "(typically the filename stem when loaded from disk)."
        )
    declared = out.pop(declared_field, None)
    if declared is not None and declared != name:
        raise ValueError(
            f"{declared_field!r} field {declared!r} does not match the "
            f"filename stem {name!r}."
        )
    return out
