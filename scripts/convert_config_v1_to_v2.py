#!/usr/bin/env python3
"""Convert a Deephaven MCP v1 config file into the v2 config directory tree.

Deephaven MCP v1 read a single JSON/JSON5 file (located via the
``DH_MCP_CONFIG_FILE`` environment variable). v2 reads a *directory* of small
JSON files under ``~/.deephaven/ai/config`` (``community/sessions/<name>.json``,
``community/settings.json``, ``enterprise/systems/<name>.json``, ...). This
script reads a v1 file and writes the equivalent v2 tree.

Design notes:

- **Standard library only.** This script imports nothing outside the Python
  standard library — no ``json5``, no ``pydantic``, no ``deephaven_mcp``. It can
  be copied out of the repository and run on its own with any ``python3`` on
  macOS, Linux, or Windows. Correctness is proven by a CI integration test that
  converts a fixture and validates the result with ``dhcli config validate``.
  Because it cannot import ``json5``, it parses only the JSON5 subset v1 configs
  use in practice: ``//`` / ``/* */`` comments and trailing commas. v1 itself
  read configs with the full ``json5`` library, so a file using other JSON5
  features (single-quoted strings, unquoted keys, hex numbers, ...) must be
  normalized to standard JSON before conversion.
- **Faithful secrets.** Inline literal secrets are copied verbatim; v1
  ``*_env_var`` fields become ``${env:VAR}`` templating and file-path fields
  become ``${file:PATH}`` templating. When a non-anonymous auth type carries no
  secret at all (no inline value and no ``*_env_var`` reference), conversion
  stops with an error and nothing is written — v2 requires the secret and the
  converter will not invent one.
- **Value-aware conversion.** Cases v1 expressed differently from v2 are resolved
  from the values rather than deferred to the user: ``Basic`` auth with an inline
  ``user:password`` token becomes a v2 ``password`` credential (and a ``custom``
  ``Basic`` credential when the token is an env var); a single ``docker_image``
  becomes the per-language image for the configured ``programming_language``
  (the other language keeps its v2 schema default); ``max_concurrent_sessions: 0``
  drops the ``session_creation`` block (v2 disables creation by omission); and a
  per-system ``connection_timeout`` becomes the shared
  ``enterprise/settings.json`` ``timeouts.client.session_connect_timeout_seconds``.
  A warning is emitted only when a human must decide.
- **Minimum-viable output.** A file is written only when it carries content the
  v1 file actually specified. All-default files (``server.json``, ``cli.json``)
  are never produced; ``community/settings.json`` is produced only for a v1
  ``security`` and/or ``session_creation`` block, and ``enterprise/settings.json``
  only when a ``connection_timeout`` is carried over.
- **Secure permissions.** Created directories are mode ``0o700`` and files mode
  ``0o600`` (POSIX; best-effort on Windows), satisfying the v2 startup
  permission audit.

Run ``python3 scripts/convert_config_v1_to_v2.py --help`` for options.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger("convert_config_v1_to_v2")


class ConversionError(Exception):
    """Raised when the v1 input cannot be read or is structurally invalid."""


# --- Known v1 fields (anything else triggers an "unknown field" warning) -----------------

_COMMUNITY_SESSION_FIELDS = {
    "host",
    "port",
    "auth_type",
    "auth_token",
    "auth_token_env_var",
    "never_timeout",
    "session_type",
    "use_tls",
    "tls_root_certs",
    "client_cert_chain",
    "client_private_key",
}
_COMMUNITY_DEFAULTS_FIELDS = {
    "launch_method",
    "auth_type",
    "auth_token",
    "auth_token_env_var",
    "programming_language",
    "docker_image",
    "docker_memory_limit_gb",
    "docker_cpu_limit",
    "docker_volumes",
    "python_venv_path",
    "heap_size_gb",
    "extra_jvm_args",
    "environment_vars",
    "startup_timeout_seconds",
    "startup_check_interval_seconds",
    "startup_retries",
}
_ENTERPRISE_SYSTEM_FIELDS = {
    "connection_json_url",
    "auth_type",
    "username",
    "password",
    "password_env_var",
    "private_key_path",
    "connection_timeout",
    "session_creation",
}
_ENTERPRISE_DEFAULTS_FIELDS = {
    "heap_size_gb",
    "programming_language",
    "auto_delete_timeout",
    "server",
    "engine",
    "extra_jvm_args",
    "extra_environment_vars",
    "admin_groups",
    "viewer_groups",
    "timeout_seconds",
    "session_arguments",
}

_PSK_HANDLER_CLASS = "io.deephaven.authentication.psk.PskAuthenticationHandler"

_NAME_PATTERN = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_-]*\Z")
"""v2 resource-name rule (mirrors ``deephaven_mcp._names``): ASCII
alphanumerics plus ``_`` and ``-``, starting with an alphanumeric. Dots are
not allowed — a name must work as one segment of a dot-separated
configuration path (``community.sessions.<name>.port``)."""


# --- JSON5-lite parsing (stdlib only) ----------------------------------------------------


def strip_json5(text: str) -> str:
    """Strip ``//`` / ``/* */`` comments and trailing commas, returning plain JSON.

    Handles only two JSON5 conveniences over plain JSON: ``//`` line and
    ``/* */`` block comments, and trailing commas before ``}``/``]``. The scan is
    string- and escape-aware, so comment markers and commas inside string
    literals are preserved. This is **not** a full JSON5 parser: single-quoted
    strings, unquoted object keys, hex numbers, and other JSON5 features pass
    through unchanged and will fail the subsequent :func:`json.loads`.

    Args:
        text (str): Raw file contents, possibly containing JSON5 extensions.

    Returns:
        str: Equivalent text safe to hand to :func:`json.loads`.
    """
    out: list[str] = []
    i = 0
    n = len(text)
    in_string = False
    quote = ""
    while i < n:
        c = text[i]
        if in_string:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if c == quote:
                in_string = False
            i += 1
            continue
        if c in ('"', "'"):
            in_string = True
            quote = c
            out.append(c)
            i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            i += 2
            while i < n and text[i] not in ("\n", "\r"):
                i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            i += 2
            while i < n and not (text[i] == "*" and i + 1 < n and text[i + 1] == "/"):
                i += 1
            i += 2
            continue
        if c in ("}", "]"):
            j = len(out) - 1
            while j >= 0 and out[j] in " \t\r\n":
                j -= 1
            if j >= 0 and out[j] == ",":
                del out[j]
            out.append(c)
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def load_v1(path: Path) -> dict[str, Any]:
    """Read and parse a v1 config file (JSON plus comments and trailing commas).

    Args:
        path (Path): Path to the v1 configuration file.

    Returns:
        dict[str, Any]: The parsed top-level object.

    Raises:
        ConversionError: When the file is missing, cannot be parsed by the
            limited JSON5 handling of :func:`strip_json5` (see its note on
            unsupported JSON5 features), or its top level is not a JSON object.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConversionError(f"Cannot read v1 config {path}: {exc}") from exc
    try:
        data = json.loads(strip_json5(raw))
    except json.JSONDecodeError as exc:
        raise ConversionError(
            f"Could not parse v1 config {path} after stripping comments and "
            f"trailing commas: {exc}. This converter does not support the full "
            "JSON5 grammar; if the file uses single-quoted strings, unquoted "
            "keys, or other JSON5 features, normalize it to standard JSON and "
            "retry."
        ) from exc
    if not isinstance(data, dict):
        raise ConversionError(f"v1 config {path} must contain a top-level object.")
    return data


# --- Conversion result -------------------------------------------------------------------


@dataclass
class ConversionResult:
    """Outcome of converting a v1 config: the files to write and any warnings."""

    files: dict[str, dict[str, Any]] = field(default_factory=dict)
    """Map of config-dir-relative path (e.g. ``community/sessions/foo.json``) to
    its JSON-serializable contents."""

    warnings: list[str] = field(default_factory=list)
    """Human-readable notes about fields that need manual review or could not be
    converted cleanly."""


# --- Shared helpers ----------------------------------------------------------------------


def _check_name(name: str, where: str) -> None:
    """Reject a session/system name that is not a valid v2 resource name.

    v1 accepted any JSON key as a name; v2 names become filename stems and
    segments of dot-separated configuration paths, so they must match
    :data:`_NAME_PATTERN`. The converter will not invent a new name, so
    conversion stops and nothing is written.

    Args:
        name (str): The v1 session or system name.
        where (str): Context label for the error message.

    Raises:
        ConversionError: When ``name`` does not satisfy the v2 name rule.
    """
    if not _NAME_PATTERN.match(name):
        raise ConversionError(
            f"{where}: name {name!r} is not a valid v2 name. v2 names must "
            "start with a letter or digit and contain only letters, digits, "
            "'_', and '-' (dots are not allowed). Rename it in the v1 file "
            "and retry."
        )


def _secret(literal: Any, env_var: Any) -> Any:
    """Resolve a secret value to a literal or an ``${env:VAR}`` reference.

    Args:
        literal (Any): The inline literal secret from v1, or ``None``.
        env_var (Any): The v1 ``*_env_var`` field naming an environment
            variable, or ``None``.

    Returns:
        Any: ``"${env:VAR}"`` when ``env_var`` is set, else the literal value
            (possibly ``None``).
    """
    if env_var:
        return "${env:" + str(env_var) + "}"
    return literal


def _file_ref(path: Any, warnings: list[str], where: str) -> Any:
    """Wrap a v1 file path as a ``${file:PATH}`` template reference.

    Args:
        path (Any): The v1 file-path value, or ``None``.
        warnings (list[str]): Accumulator for review notes.
        where (str): Context label used in any warning message.

    Returns:
        Any: ``"${file:PATH}"`` when ``path`` is set, else ``None``.
    """
    if not path:
        return None
    if not os.path.isabs(str(path)):
        warnings.append(
            f"{where}: path {path!r} is not absolute; ${{file:}} references "
            "should use absolute paths. Review the result."
        )
    return "${file:" + str(path) + "}"


def _title_language(value: Any) -> Any:
    """Normalize a v1 language string (``python``/``groovy``) to title case."""
    if isinstance(value, str):
        normalized = value.strip().title()
        if normalized in ("Python", "Groovy"):
            return normalized
    return value


def _warn_unknown(cfg: Any, known: set[str], warnings: list[str], where: str) -> None:
    """Append a warning for every key in ``cfg`` not present in ``known``."""
    if not isinstance(cfg, dict):
        return
    for key in cfg:
        if key not in known:
            warnings.append(f"{where}: unknown v1 field {key!r} was not converted.")


def _community_credentials(
    cfg: dict[str, Any], warnings: list[str], where: str
) -> dict[str, Any]:
    """Build a v2 community ``credentials`` block from v1 auth fields.

    Args:
        cfg (dict[str, Any]): The v1 mapping carrying ``auth_type`` /
            ``auth_token`` / ``auth_token_env_var``.
        warnings (list[str]): Accumulator for review notes.
        where (str): Context label for warnings.

    Returns:
        dict[str, Any]: A discriminated v2 credentials object.

    Raises:
        ConversionError: When a non-anonymous auth type carries no token (no
            inline ``auth_token`` and no ``auth_token_env_var``). v2 requires
            the secret and the converter will not invent one, so conversion
            stops and nothing is written.
    """
    auth_type = str(cfg.get("auth_type") or "").strip()
    literal = cfg.get("auth_token")
    env_var = cfg.get("auth_token_env_var")
    token = _secret(literal, env_var)
    if auth_type == "" or auth_type.lower() == "anonymous":
        return {"type": "anonymous"}
    if auth_type.upper() == "PSK" or auth_type == _PSK_HANDLER_CLASS:
        if token is None:
            raise ConversionError(
                f"{where}: PSK auth has no token. v2 requires one; add it to "
                "the v1 file as 'auth_token' or 'auth_token_env_var' and retry."
            )
        return {"type": "psk", "token": token}
    if auth_type.lower() == "basic":
        if not env_var and isinstance(literal, str) and ":" in literal:
            username, password = literal.split(":", 1)
            return {"type": "password", "username": username, "password": password}
        if env_var:
            return {"type": "custom", "auth_type": "Basic", "auth_token": token}
        if token is None:
            raise ConversionError(
                f"{where}: Basic auth has no token. v2 requires one; add it to "
                "the v1 file as 'auth_token' or 'auth_token_env_var' and retry."
            )
        warnings.append(
            f"{where}: Basic auth_token must be in 'username:password' form; "
            "could not split it into a username and password. Set them manually."
        )
        return {"type": "custom", "auth_type": "Basic", "auth_token": token}
    if token is None:
        raise ConversionError(
            f"{where}: custom auth ({auth_type!r}) has no token. v2 requires "
            "one; add it to the v1 file as 'auth_token' or 'auth_token_env_var' "
            "and retry."
        )
    warnings.append(
        f"{where}: auth_type {auth_type!r} is a custom authentication handler; "
        "converted to a 'custom' credential that passes it through unchanged. "
        "It works as long as that handler is configured on the server."
    )
    return {"type": "custom", "auth_type": auth_type, "auth_token": token}


def _enterprise_credentials(
    cfg: dict[str, Any], warnings: list[str], where: str
) -> dict[str, Any]:
    """Build a v2 enterprise ``credentials`` block from v1 auth fields.

    Args:
        cfg (dict[str, Any]): The v1 system mapping carrying ``auth_type`` and
            the auth-specific fields.
        warnings (list[str]): Accumulator for review notes.
        where (str): Context label for warnings.

    Returns:
        dict[str, Any]: A discriminated v2 credentials object.

    Raises:
        ConversionError: When ``auth_type`` is neither ``"password"`` nor
            ``"private_key"`` (the only two values a v1 enterprise system could
            carry; v1 rejected any other), or when the chosen type carries no
            secret (``password`` auth with no password / ``private_key`` auth
            with no ``private_key_path``). v2 requires the secret and the
            converter will not invent one, so conversion stops and nothing is
            written.
    """
    auth_type = str(cfg.get("auth_type") or "").strip()
    if auth_type == "password":
        password = _secret(cfg.get("password"), cfg.get("password_env_var"))
        if password is None:
            raise ConversionError(
                f"{where}: password auth has no password. v2 requires one; add "
                "it to the v1 file as 'password' or 'password_env_var' and retry."
            )
        return {
            "type": "password",
            "username": cfg.get("username"),
            "password": password,
        }
    if auth_type == "private_key":
        key_path = cfg.get("private_key_path")
        if not key_path:
            raise ConversionError(
                f"{where}: private_key auth has no private_key_path. v2 requires "
                "the key; add 'private_key_path' to the v1 file and retry."
            )
        return {
            "type": "private_key",
            "key_text": _file_ref(key_path, warnings, where),
        }
    raise ConversionError(
        f"{where}: unsupported enterprise auth_type {auth_type!r}; v1 enterprise "
        "systems support only 'password' or 'private_key'."
    )


def _tls_block(
    cfg: dict[str, Any], warnings: list[str], where: str
) -> dict[str, Any] | None:
    """Build a v2 ``tls`` block from v1 TLS fields, or ``None`` if unused.

    Args:
        cfg (dict[str, Any]): The v1 session mapping.
        warnings (list[str]): Accumulator for review notes.
        where (str): Context label for warnings.

    Returns:
        dict[str, Any] | None: The ``tls`` block, or ``None`` when v1 enabled
            no TLS.
    """
    use_tls = cfg.get("use_tls")
    root = cfg.get("tls_root_certs")
    chain = cfg.get("client_cert_chain")
    key = cfg.get("client_private_key")
    if not (use_tls or root or chain or key):
        return None
    tls: dict[str, Any] = {}
    if root:
        tls["root_certs"] = _file_ref(root, warnings, where)
    if chain or key:
        if not (chain and key):
            warnings.append(
                f"{where}: a client certificate needs both client_cert_chain "
                "and client_private_key; only one was present. Review the result."
            )
        cert: dict[str, Any] = {}
        if chain:
            cert["cert_chain"] = _file_ref(chain, warnings, where)
        if key:
            cert["private_key"] = _file_ref(key, warnings, where)
        tls["client_certificate"] = cert
    return tls


# --- Community conversion ----------------------------------------------------------------


def _convert_community_session(
    name: str, cfg: dict[str, Any], warnings: list[str]
) -> dict[str, Any]:
    """Convert one v1 community session into a v2 session file body."""
    where = f"community session '{name}'"
    _check_name(name, where)
    if not isinstance(cfg, dict):
        raise ConversionError(f"{where}: expected an object.")
    out: dict[str, Any] = {"session_name": name}
    if "host" in cfg:
        out["host"] = cfg["host"]
    if "port" in cfg:
        out["port"] = cfg["port"]
    if "session_type" in cfg:
        out["programming_language"] = _title_language(cfg["session_type"])
    if "never_timeout" in cfg:
        out["never_timeout"] = cfg["never_timeout"]
    tls = _tls_block(cfg, warnings, where)
    if tls is not None:
        out["tls"] = tls
    out["auth"] = {"credentials": _community_credentials(cfg, warnings, where)}
    _warn_unknown(cfg, _COMMUNITY_SESSION_FIELDS, warnings, where)
    return out


def _convert_community_creation_defaults(
    defaults: dict[str, Any], warnings: list[str]
) -> dict[str, Any]:
    """Convert v1 ``community.session_creation.defaults`` to the v2 shape."""
    where = "community session_creation.defaults"
    out: dict[str, Any] = {}
    if "launch_method" in defaults:
        out["launch_method"] = defaults["launch_method"]
    docker: dict[str, Any] = {}
    if "docker_image" in defaults:
        language = _title_language(defaults.get("programming_language"))
        image_key = "groovy" if language == "Groovy" else "python"
        docker["images"] = {image_key: defaults["docker_image"]}
    if "docker_memory_limit_gb" in defaults:
        docker["memory_limit_gb"] = defaults["docker_memory_limit_gb"]
    if "docker_cpu_limit" in defaults:
        docker["cpu_limit"] = defaults["docker_cpu_limit"]
    if "docker_volumes" in defaults:
        docker["volumes"] = defaults["docker_volumes"]
    if docker:
        out["docker"] = docker
    if "python_venv_path" in defaults:
        out["python"] = {"venv_path": defaults["python_venv_path"]}
    if any(k in defaults for k in ("auth_type", "auth_token", "auth_token_env_var")):
        out["auth"] = {"credentials": _community_credentials(defaults, warnings, where)}
    if "programming_language" in defaults:
        out["programming_language"] = _title_language(defaults["programming_language"])
    for key in (
        "heap_size_gb",
        "extra_jvm_args",
        "environment_vars",
        "startup_timeout_seconds",
        "startup_check_interval_seconds",
        "startup_retries",
    ):
        if key in defaults:
            out[key] = defaults[key]
    _warn_unknown(defaults, _COMMUNITY_DEFAULTS_FIELDS, warnings, where)
    return out


def _convert_community_session_creation(
    sc: dict[str, Any], warnings: list[str]
) -> dict[str, Any] | None:
    """Convert v1 ``community.session_creation`` to the v2 shape, or drop it."""
    where = "community session_creation"
    if not isinstance(sc, dict):
        raise ConversionError(f"{where}: expected an object.")
    max_concurrent = sc.get("max_concurrent_sessions")
    if max_concurrent == 0:
        return None
    out: dict[str, Any] = {}
    if max_concurrent is not None:
        out["max_concurrent_sessions"] = max_concurrent
    defaults = sc.get("defaults")
    if isinstance(defaults, dict):
        out["defaults"] = _convert_community_creation_defaults(defaults, warnings)
    _warn_unknown(sc, {"max_concurrent_sessions", "defaults"}, warnings, where)
    return out or None


def _convert_community_settings(
    v1: dict[str, Any], warnings: list[str]
) -> dict[str, Any] | None:
    """Build ``community/settings.json`` from v1 ``security`` + ``session_creation``."""
    settings: dict[str, Any] = {}
    security = v1.get("security")
    if isinstance(security, dict):
        community_security = security.get("community")
        if isinstance(community_security, dict):
            mode = community_security.get("credential_retrieval_mode")
            if mode and mode != "none":
                settings["security"] = {"credential_retrieval_mode": mode}
            _warn_unknown(
                community_security,
                {"credential_retrieval_mode"},
                warnings,
                "security.community",
            )
        _warn_unknown(security, {"community"}, warnings, "security")
    community = v1.get("community")
    if isinstance(community, dict):
        sc = community.get("session_creation")
        if sc is not None:
            converted = _convert_community_session_creation(sc, warnings)
            if converted is not None:
                settings["session_creation"] = converted
    return settings or None


# --- Enterprise conversion ---------------------------------------------------------------


def _convert_enterprise_defaults(
    defaults: dict[str, Any], warnings: list[str], where: str
) -> dict[str, Any]:
    """Convert v1 enterprise ``session_creation.defaults`` to the v2 shape."""
    out: dict[str, Any] = {}
    for key in (
        "heap_size_gb",
        "auto_delete_timeout",
        "server",
        "engine",
        "extra_jvm_args",
        "admin_groups",
        "viewer_groups",
        "session_arguments",
    ):
        if key in defaults:
            out[key] = defaults[key]
    if "programming_language" in defaults:
        out["programming_language"] = _title_language(defaults["programming_language"])
    if "extra_environment_vars" in defaults:
        env: dict[str, str] = {}
        for item in defaults["extra_environment_vars"] or []:
            if isinstance(item, str) and "=" in item:
                env_name, env_value = item.split("=", 1)
                env[env_name] = env_value
            else:
                warnings.append(
                    f"{where}: extra_environment_vars entry {item!r} is not in "
                    "'NAME=value' form; skipped."
                )
        if env:
            out["environment_vars"] = env
    if "timeout_seconds" in defaults:
        warnings.append(
            f"{where}: session_creation.defaults.timeout_seconds has no v2 "
            "equivalent; dropped."
        )
    _warn_unknown(defaults, _ENTERPRISE_DEFAULTS_FIELDS, warnings, where)
    return out


def _convert_enterprise_session_creation(
    sc: dict[str, Any], warnings: list[str], where: str
) -> dict[str, Any] | None:
    """Convert v1 enterprise ``session_creation`` to the v2 shape, or drop it."""
    if not isinstance(sc, dict):
        raise ConversionError(f"{where}: session_creation must be an object.")
    max_concurrent = sc.get("max_concurrent_sessions")
    if max_concurrent == 0:
        return None
    out: dict[str, Any] = {}
    if max_concurrent is not None:
        out["max_concurrent_sessions"] = max_concurrent
    defaults = sc.get("defaults")
    if isinstance(defaults, dict):
        out["defaults"] = _convert_enterprise_defaults(
            defaults, warnings, f"{where} session_creation"
        )
    _warn_unknown(
        sc,
        {"max_concurrent_sessions", "defaults"},
        warnings,
        f"{where} session_creation",
    )
    return out or None


def _convert_enterprise_settings(
    systems: dict[str, Any], warnings: list[str]
) -> dict[str, Any] | None:
    """Build ``enterprise/settings.json`` from per-system v1 ``connection_timeout``.

    Args:
        systems (dict[str, Any]): The v1 ``enterprise.systems`` mapping.
        warnings (list[str]): Accumulator for review notes.

    Returns:
        dict[str, Any] | None: The settings body when one connect timeout can be
            chosen, else ``None``.
    """
    timeouts = {
        cfg["connection_timeout"]
        for cfg in systems.values()
        if isinstance(cfg, dict) and "connection_timeout" in cfg
    }
    if not timeouts:
        return None
    if len(timeouts) > 1:
        warnings.append(
            "enterprise: systems set different connection_timeout values "
            f"({sorted(timeouts)}); v2's connect timeout is shared across all "
            "systems (enterprise/settings.json "
            "timeouts.client.session_connect_timeout_seconds), so none was "
            "written. Set one value manually."
        )
        return None
    return {"timeouts": {"client": {"session_connect_timeout_seconds": timeouts.pop()}}}


def _convert_enterprise_system(
    name: str, cfg: dict[str, Any], warnings: list[str]
) -> dict[str, Any]:
    """Convert one v1 enterprise system into a v2 system file body."""
    where = f"enterprise system '{name}'"
    _check_name(name, where)
    if not isinstance(cfg, dict):
        raise ConversionError(f"{where}: expected an object.")
    out: dict[str, Any] = {"system_name": name}
    if "connection_json_url" in cfg:
        out["connection_json_url"] = cfg["connection_json_url"]
    else:
        warnings.append(f"{where}: missing required connection_json_url.")
    out["auth"] = {"credentials": _enterprise_credentials(cfg, warnings, where)}
    # connection_timeout is handled across all systems by
    # _convert_enterprise_settings (v2's connect timeout is a shared setting).
    sc = cfg.get("session_creation")
    if sc is not None:
        converted = _convert_enterprise_session_creation(sc, warnings, where)
        if converted is not None:
            out["session_creation"] = converted
    _warn_unknown(cfg, _ENTERPRISE_SYSTEM_FIELDS, warnings, where)
    return out


# --- Top-level conversion ----------------------------------------------------------------


def convert(v1: dict[str, Any]) -> ConversionResult:
    """Convert a parsed v1 config object into a v2 file tree plan.

    Args:
        v1 (dict[str, Any]): The parsed v1 configuration object.

    Returns:
        ConversionResult: The files to write (config-dir-relative paths) and any
            review warnings.
    """
    result = ConversionResult()

    community = v1.get("community") or {}
    if not isinstance(community, dict):
        raise ConversionError("'community' must be an object.")
    sessions = community.get("sessions") or {}
    if not isinstance(sessions, dict):
        raise ConversionError("'community.sessions' must be an object.")
    for name in sorted(sessions):
        result.files[f"community/sessions/{name}.json"] = _convert_community_session(
            name, sessions[name], result.warnings
        )

    settings = _convert_community_settings(v1, result.warnings)
    if settings is not None:
        result.files["community/settings.json"] = settings

    enterprise = v1.get("enterprise") or {}
    if not isinstance(enterprise, dict):
        raise ConversionError("'enterprise' must be an object.")
    systems = enterprise.get("systems") or {}
    if not isinstance(systems, dict):
        raise ConversionError("'enterprise.systems' must be an object.")
    for name in sorted(systems):
        result.files[f"enterprise/systems/{name}.json"] = _convert_enterprise_system(
            name, systems[name], result.warnings
        )

    enterprise_settings = _convert_enterprise_settings(systems, result.warnings)
    if enterprise_settings is not None:
        result.files["enterprise/settings.json"] = enterprise_settings

    _warn_unknown(
        v1, {"security", "community", "enterprise"}, result.warnings, "top-level"
    )
    _warn_unknown(
        community, {"sessions", "session_creation"}, result.warnings, "community"
    )
    _warn_unknown(enterprise, {"systems"}, result.warnings, "enterprise")
    return result


# --- Output location + filesystem --------------------------------------------------------


def _default_data_root() -> Path:
    """Return the platform-default Deephaven MCP user-data root.

    Mirrors :func:`deephaven_mcp.config._data_root._default_data_root` without
    importing the package, keeping this script dependency-free.

    Returns:
        Path: ``~/.deephaven/ai`` on macOS/Linux; ``%APPDATA%/Deephaven/ai`` on
            Windows (falling back to the home-directory form).
    """
    if sys.platform == "win32":
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "Deephaven" / "ai"
    return Path.home() / ".deephaven" / "ai"


def resolve_output_dir(explicit: str | None) -> Path:
    """Resolve the target config directory using the v2 precedence.

    Args:
        explicit (str | None): The ``--output`` value, or ``None``.

    Returns:
        Path: The config directory. ``--output`` wins; otherwise
            ``$DH_AI_DATA_DIR/config`` when that env var is set; otherwise the
            platform default ``.../ai/config``.
    """
    if explicit:
        return Path(explicit).expanduser()
    env_value = os.environ.get("DH_AI_DATA_DIR")
    if env_value:
        return Path(env_value).expanduser() / "config"
    return _default_data_root() / "config"


def _try_chmod(path: Path, mode: int) -> None:
    """chmod ``path`` to ``mode``, ignoring failures (e.g. on Windows)."""
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def write_tree(root: Path, files: dict[str, dict[str, Any]]) -> None:
    """Write the converted files under ``root`` with restrictive permissions.

    Directories are created mode ``0o700`` and files written mode ``0o600`` so
    they are never momentarily world-readable, satisfying the v2 startup
    permission audit.

    Args:
        root (Path): The target config directory.
        files (dict[str, dict[str, Any]]): Relative path to file body.
    """
    dirs: set[Path] = {root}
    for rel in files:
        current = (root / rel).parent
        while True:
            dirs.add(current)
            if current == root:
                break
            current = current.parent
    for directory in sorted(dirs, key=lambda p: len(p.parts)):
        directory.mkdir(parents=True, exist_ok=True)
        _try_chmod(directory, 0o700)
    for rel, body in files.items():
        path = root / rel
        text = json.dumps(body, indent=2) + "\n"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
        _try_chmod(path, 0o600)


# --- Interactive prompts -----------------------------------------------------------------


def _confirm(prompt: str) -> bool:
    """Ask a yes/no question (default no) on the console."""
    return input(f"{prompt} [y/N]: ").strip().lower() in ("y", "yes")


def _existing_dir_action() -> str:
    """Ask what to do about a non-empty existing output directory.

    Returns:
        str: One of ``"stop"``, ``"delete"``, or ``"write"``.
    """
    while True:
        choice = (
            input(
                "Output directory exists and is not empty. "
                "[s]top, [d]elete and recreate, [w]rite into it (default s): "
            )
            .strip()
            .lower()
        )
        if choice in ("", "s", "stop"):
            return "stop"
        if choice in ("d", "delete"):
            return "delete"
        if choice in ("w", "write"):
            return "write"
        print("Please enter 's', 'd', or 'w'.")


def _dir_is_nonempty(path: Path) -> bool:
    """Return whether ``path`` exists and contains any entries."""
    return path.is_dir() and any(path.iterdir())


# --- CLI ---------------------------------------------------------------------------------


def _print_summary(root: Path, result: ConversionResult, *, written: bool) -> None:
    """Print the list of files written (or that would be written) to the console."""
    verb = "Wrote" if written else "Would write"
    print(f"\n{verb} {len(result.files)} file(s) under {root}:")
    for rel in sorted(result.files):
        print(f"  {rel}")


def _print_review_items(result: ConversionResult) -> None:
    """Print the review items as a loud trailing banner (the last thing shown).

    When there are no review items, prints a single clean-bill line instead.

    Args:
        result (ConversionResult): The conversion outcome carrying any warnings.
    """
    if not result.warnings:
        print("\nNo items need review — every setting was converted automatically.")
        return
    bar = "=" * 72
    count = len(result.warnings)
    print(f"\n{bar}")
    print(f"ACTION REQUIRED: {count} item(s) need your review.")
    print("The converter made a best effort but flagged these for you to check:")
    print(bar)
    for index, warning in enumerate(result.warnings, start=1):
        print(f"  {index}. {warning}")
    print(bar)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="convert_config_v1_to_v2.py",
        description=(
            "Convert a Deephaven MCP v1 config file into the v2 config "
            "directory tree."
        ),
    )
    parser.add_argument(
        "v1_config",
        type=Path,
        help="Path to the v1 JSON/JSON5 config file to convert.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help=(
            "Target config directory. Defaults to $DH_AI_DATA_DIR/config when "
            "set, otherwise ~/.deephaven/ai/config "
            "(%%APPDATA%%/Deephaven/ai/config on Windows)."
        ),
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help=(
            "Do not prompt. Confirm the target and write into an existing "
            "directory (never deletes)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the files that would be written and exit without writing.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point: convert a v1 config file to the v2 directory tree.

    Args:
        argv (list[str] | None): Argument vector (defaults to ``sys.argv``).

    Returns:
        int: ``0`` on success or clean abort; ``2`` on a conversion error.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)

    try:
        v1 = load_v1(args.v1_config)
        result = convert(v1)
    except ConversionError as exc:
        _LOGGER.error("Conversion failed: %s", exc)
        return 2

    if not result.files:
        _LOGGER.warning(
            "No convertible community sessions, enterprise systems, or settings "
            "were found in %s. Nothing to write.",
            args.v1_config,
        )
        return 0

    root = resolve_output_dir(args.output)

    if args.dry_run:
        _print_summary(root, result, written=False)
        _print_review_items(result)
        return 0

    if not args.yes:
        if not _confirm(f"Write the converted v2 config to {root}?"):
            print("Aborted; nothing written.")
            return 0

    if _dir_is_nonempty(root):
        action = "write" if args.yes else _existing_dir_action()
        if action == "stop":
            print("Aborted; nothing written.")
            return 0
        if action == "delete":
            if not args.yes and not _confirm(
                f"Permanently delete {root} and everything under it?"
            ):
                print("Aborted; nothing written.")
                return 0
            shutil.rmtree(root)

    write_tree(root, result.files)
    _print_summary(root, result, written=True)
    print(f"\nValidate the result with:\n  dhcli --config-dir {root} config validate")
    _print_review_items(result)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
