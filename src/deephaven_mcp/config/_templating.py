r"""JSON-value templating engine for Deephaven MCP configuration.

This module owns the single mechanism by which a configuration JSON file
pulls in values from the process environment or from files on disk. It is
invoked by :func:`deephaven_mcp.config._file_loader.load_config_from_file` after
the JSON5 parser has produced an in-memory tree and *before* that tree is
handed to a Pydantic model for validation. The Pydantic schemas therefore
see only fully-resolved string values and never carry parallel
``<field>`` / ``<field>_env_var`` or ``<field>`` / ``<field>_path`` shadow
fields.

Syntax
------

Placeholders use a ``${kind:argument}`` form and may appear anywhere inside
a JSON string value, including inside an otherwise-literal string
(substring expansion). The three recognized kinds are:

- ``${env:VAR}`` --- resolves to the value of environment variable
  ``VAR``. Raises :class:`~deephaven_mcp._exceptions.ConfigurationError`
  if ``VAR`` is unset or empty.
- ``${env:VAR:-default}`` --- resolves to the value of ``VAR`` when set
  and non-empty, otherwise the literal text after ``:-`` (which may
  itself be empty, e.g. ``${env:FOO:-}``).
- ``${file:PATH}`` --- resolves to the UTF-8 text contents of the file
  at ``PATH``, returned verbatim (trailing newlines preserved). A
  leading ``~`` in ``PATH`` is expanded via :meth:`Path.expanduser`.
  Raises :class:`~deephaven_mcp._exceptions.ConfigurationError` if the
  file is missing, unreadable, or not valid UTF-8. **No fallback form**:
  ``${file:PATH:-default}`` is intentionally not supported; file paths
  are required when written.

Nesting is intentionally **not** supported. The placeholder grammar is a
single-pass match of ``\\$\\{[^}]+\\}``; a ``}`` inside the argument
closes the placeholder. Documented limitation.

Substring expansion
-------------------

A string value may contain zero or more placeholders interleaved with
literal text. The result is the string-concatenation of the resolved
parts. Numeric coercion happens later, at Pydantic validation time --- an
``"${env:DH_MCP_PORT}"`` that resolves to ``"10000"`` is accepted as an
``int`` field because Pydantic parses it. Authors of multi-placeholder
strings must therefore ensure the final string is a valid representation
of the field's declared type.

Recursive walk
--------------

:func:`expand_tree` recursively visits every value in the parsed JSON
tree (dicts, lists, scalars). Only string values are scanned for
placeholders; non-string scalars pass through unchanged. Dict keys are
**not** expanded.

Error reporting
---------------

Every :class:`~deephaven_mcp._exceptions.ConfigurationError` raised by
this module carries the source filename and the JSON-path of the
offending value, e.g.::

    In community/sessions/local.json at credentials.token:
    env var DH_MCP_PSK is not set

This is the format consumed by
:func:`deephaven_mcp._pydantic.format_validation_error` so configuration
errors surface with the same shape regardless of whether they originate
in templating or in Pydantic validation.
"""

from __future__ import annotations

__all__ = [
    "expand_string",
    "expand_tree",
]

import os
import re
from pathlib import Path
from typing import Any, Final

from deephaven_mcp._exceptions import ConfigurationError

# Match ``${kind:argument}`` where argument is anything not containing ``}``.
# Non-greedy ``}``-termination intentionally rejects nesting: ``${a:${b:c}}``
# matches ``${a:${b:c}`` (then trailing ``}`` is literal), which fails parsing.
_PLACEHOLDER_RE = re.compile(r"\$\{([^}]+)\}")

_MAX_FILE_TEMPLATE_BYTES: Final[int] = 1024 * 1024
"""Maximum number of bytes a ``${file:PATH}`` placeholder may pull in.

Anything larger raises :class:`ConfigurationError`. The cap is sized
for real PEM material: the largest legitimate input is a system CA
trust bundle (e.g. ``/etc/ssl/cert.pem`` on macOS is ~333 KiB; the
Mozilla curated bundle is ~190 KiB). 1 MiB is roughly 3x the largest
observed real-world bundle, with headroom for future growth, while
still refusing accidental gigabyte reads (log files, ``/dev/zero``,
attacker-pinned files).
"""


def expand_string(
    template: str,
    *,
    source: str,
    path: str,
    config_dir: Path | None = None,
) -> str:
    """Resolve every ``${...}`` placeholder inside ``template``.

    Args:
        template: The JSON string value to expand. May contain zero or
            more placeholders interleaved with literal text.
        source: Human-readable label for the originating JSON file
            (e.g. ``"community/sessions/local.json"``). Included in
            every error message.
        path: Dotted JSON-path to the value within the source file
            (e.g. ``"credentials.token"``). Included in every error
            message.
        config_dir: Optional base directory for resolving *relative*
            ``${file:PATH}`` arguments. A relative path is resolved
            against this directory; an absolute path (including one
            produced by expanding a leading ``~``) is used as-is.
            When ``None``, a relative path is resolved against the
            process working directory.

    Returns:
        The fully-resolved string with every placeholder replaced by
        its resolved value.

    Raises:
        ConfigurationError: If any placeholder kind is unknown, any
            required environment variable is unset/empty, any
            referenced file cannot be read as UTF-8 text, or exceeds
            :data:`_MAX_FILE_TEMPLATE_BYTES`.
    """

    def _replace(match: re.Match[str]) -> str:
        body = match.group(1)
        kind, _, argument = body.partition(":")
        if not _:
            # No colon at all => bare ``${something}`` with no kind separator.
            raise ConfigurationError(
                f"In {source} at {path}: malformed placeholder "
                f"{match.group(0)!r}: expected '${{env:...}}' or "
                f"'${{file:...}}'"
            )
        if kind == "env":
            return _resolve_env(argument, source=source, path=path)
        if kind == "file":
            return _resolve_file(
                argument, source=source, path=path, config_dir=config_dir
            )
        raise ConfigurationError(
            f"In {source} at {path}: unknown placeholder kind {kind!r} in "
            f"{match.group(0)!r}; expected 'env' or 'file'"
        )

    return _PLACEHOLDER_RE.sub(_replace, template)


def expand_tree(
    node: Any,
    *,
    source: str,
    path: str = "",
    config_dir: Path | None = None,
) -> Any:
    """Recursively expand every string value in a parsed JSON tree.

    Walks ``node`` depth-first. Strings are passed to
    :func:`expand_string`; dicts and lists are recursed into; all other
    scalars (``int``, ``float``, ``bool``, ``None``) pass through.

    Args:
        node: The parsed JSON value. Typically a ``dict`` at the top
            level but the function is total over the JSON value space.
        source: Human-readable label for the originating JSON file.
        path: Dotted JSON-path accumulated during recursion. Callers
            typically pass the default (``""``); recursive calls
            extend it with the field name or list index.
        config_dir: Optional base directory for resolving relative
            ``${file:PATH}`` arguments. Forwarded to
            :func:`expand_string`; see that function for the exact
            semantics.

    Returns:
        A new value of the same shape as ``node`` with every string
        leaf fully resolved. Non-string scalars are returned as-is.
        Dicts and lists are returned as freshly-constructed objects;
        the input is not mutated.

    Raises:
        ConfigurationError: Propagated from :func:`expand_string` when
            a string leaf contains a placeholder that cannot be
            resolved. The error message includes ``source`` and the
            accumulated ``path``.
    """
    if isinstance(node, str):
        return expand_string(
            node, source=source, path=path or "<root>", config_dir=config_dir
        )
    if isinstance(node, dict):
        return {
            key: expand_tree(
                value,
                source=source,
                path=f"{path}.{key}" if path else key,
                config_dir=config_dir,
            )
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [
            expand_tree(
                item,
                source=source,
                path=f"{path}[{idx}]",
                config_dir=config_dir,
            )
            for idx, item in enumerate(node)
        ]
    return node


def _resolve_env(argument: str, *, source: str, path: str) -> str:
    """Resolve a single ``${env:...}`` placeholder argument."""
    name, sep, default = argument.partition(":-")
    if not name:
        raise ConfigurationError(
            f"In {source} at {path}: empty env-var name in " f"'${{env:{argument}}}'"
        )
    value = os.environ.get(name, "")
    if value:
        return value
    if sep:
        # ``${env:NAME:-default}`` --- ``default`` may be empty.
        return default
    raise ConfigurationError(f"In {source} at {path}: env var {name!r} is not set")


def _resolve_file(
    argument: str,
    *,
    source: str,
    path: str,
    config_dir: Path | None,
) -> str:
    """Resolve a single ``${file:...}`` placeholder argument.

    Reads up to :data:`_MAX_FILE_TEMPLATE_BYTES` from the file at
    ``argument`` and returns its UTF-8 decoded contents. A leading
    ``~`` is expanded via :meth:`Path.expanduser`. A relative
    ``argument`` is resolved against ``config_dir`` when supplied
    (otherwise against the process working directory); an absolute
    ``argument`` is used as-is. Symlinks are followed (common for
    system CA bundles such as ``/etc/ssl/cert.pem``).
    """
    if not argument:
        raise ConfigurationError(
            f"In {source} at {path}: empty file path in '${{file:}}'"
        )
    if ":-" in argument:
        # File form has no fallback. Reject explicitly to avoid the
        # silent-acceptance footgun of treating ``:-default`` as part of
        # the path.
        raise ConfigurationError(
            f"In {source} at {path}: '${{file:...}}' does not support "
            f"':-default' fallback syntax; file paths are always required"
        )

    file_path = Path(argument).expanduser()
    if config_dir is not None and not file_path.is_absolute():
        file_path = config_dir / file_path

    try:
        with open(file_path, "rb") as handle:
            data = handle.read(_MAX_FILE_TEMPLATE_BYTES + 1)
    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"In {source} at {path}: file {argument!r} does not exist"
        ) from exc
    except PermissionError as exc:
        raise ConfigurationError(
            f"In {source} at {path}: file {argument!r} cannot be read "
            f"(permission denied)"
        ) from exc
    except OSError as exc:
        raise ConfigurationError(
            f"In {source} at {path}: cannot read file {argument!r}: {exc}"
        ) from exc

    if len(data) > _MAX_FILE_TEMPLATE_BYTES:
        raise ConfigurationError(
            f"In {source} at {path}: file {argument!r} exceeds the "
            f"{_MAX_FILE_TEMPLATE_BYTES}-byte limit for "
            f"'${{file:...}}' placeholders"
        )

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigurationError(
            f"In {source} at {path}: file {argument!r} is not valid UTF-8"
        ) from exc
