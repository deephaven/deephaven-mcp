"""Unit tests for the standalone v1->v2 config converter script.

The script lives under ``scripts/`` (not an importable package), so it is
loaded by file path via :mod:`importlib`. The tests exercise the JSON5-lite
preprocessor, the field-level mapping for every auth/TLS/session-creation case,
the minimum-viable file omission, the filesystem writer's permission bits, and
the interactive ``main`` entry point.
"""

from __future__ import annotations

import importlib.util
import json
import os
import stat
import sys
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "convert_config_v1_to_v2.py"
)


def _load_module() -> ModuleType:
    """Load the converter script as a module by file path."""
    spec = importlib.util.spec_from_file_location(
        "convert_config_v1_to_v2", _SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


conv = _load_module()


# ---------------------------------------------------------------------------
# strip_json5
# ---------------------------------------------------------------------------


def test_strip_json5_line_and_block_comments():
    text = '{\n  // line\n  "a": 1, /* block */ "b": 2\n}'
    assert json.loads(conv.strip_json5(text)) == {"a": 1, "b": 2}


def test_strip_json5_trailing_commas():
    text = '{"a": [1, 2,], "b": {"c": 3,},}'
    assert json.loads(conv.strip_json5(text)) == {"a": [1, 2], "b": {"c": 3}}


def test_strip_json5_preserves_comment_markers_inside_strings():
    text = '{"url": "http://x//y", "note": "a, } b", "blk": "/* not */"}'
    assert json.loads(conv.strip_json5(text)) == {
        "url": "http://x//y",
        "note": "a, } b",
        "blk": "/* not */",
    }


def test_strip_json5_preserves_escaped_quote_in_string():
    text = r'{"a": "he said \"// hi\""}'
    assert json.loads(conv.strip_json5(text)) == {"a": 'he said "// hi"'}


# ---------------------------------------------------------------------------
# load_v1
# ---------------------------------------------------------------------------


def test_load_v1_reads_json5(tmp_path):
    path = tmp_path / "v1.json"
    path.write_text('{\n  // hi\n  "community": {},\n}', encoding="utf-8")
    assert conv.load_v1(path) == {"community": {}}


def test_load_v1_missing_file_raises(tmp_path):
    with pytest.raises(conv.ConversionError, match="Cannot read"):
        conv.load_v1(tmp_path / "nope.json")


def test_load_v1_invalid_json_raises(tmp_path):
    path = tmp_path / "v1.json"
    path.write_text("{not valid", encoding="utf-8")
    with pytest.raises(conv.ConversionError, match="Could not parse"):
        conv.load_v1(path)


def test_load_v1_unsupported_json5_feature_raises(tmp_path):
    # Single-quoted strings are legal JSON5 (v1 used the full json5 library) but
    # are outside this converter's comments + trailing-commas subset; the error
    # must point the user at normalizing, not claim the file is invalid.
    path = tmp_path / "v1.json"
    path.write_text("{'community': {}}", encoding="utf-8")
    with pytest.raises(conv.ConversionError, match="normalize it to standard JSON"):
        conv.load_v1(path)


def test_load_v1_non_object_raises(tmp_path):
    path = tmp_path / "v1.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(conv.ConversionError, match="top-level object"):
        conv.load_v1(path)


# ---------------------------------------------------------------------------
# community credentials
# ---------------------------------------------------------------------------


def test_community_credentials_anonymous_when_absent():
    warnings: list[str] = []
    assert conv._community_credentials({}, warnings, "x") == {"type": "anonymous"}
    assert warnings == []


def test_community_credentials_explicit_anonymous():
    warnings: list[str] = []
    cred = conv._community_credentials({"auth_type": "Anonymous"}, warnings, "x")
    assert cred == {"type": "anonymous"}


def test_community_credentials_psk_literal_and_handler_class():
    warnings: list[str] = []
    assert conv._community_credentials(
        {"auth_type": "PSK", "auth_token": "tok"}, warnings, "x"
    ) == {"type": "psk", "token": "tok"}
    assert conv._community_credentials(
        {"auth_type": conv._PSK_HANDLER_CLASS, "auth_token": "tok"}, warnings, "x"
    ) == {"type": "psk", "token": "tok"}


def test_community_credentials_psk_env_var():
    warnings: list[str] = []
    cred = conv._community_credentials(
        {"auth_type": "PSK", "auth_token_env_var": "DH_PSK"}, warnings, "x"
    )
    assert cred == {"type": "psk", "token": "${env:DH_PSK}"}


def test_community_credentials_psk_without_token_raises():
    with pytest.raises(conv.ConversionError, match="PSK auth has no token"):
        conv._community_credentials({"auth_type": "PSK"}, [], "x")


def test_community_credentials_basic_inline_splits_to_password():
    warnings: list[str] = []
    cred = conv._community_credentials(
        {"auth_type": "Basic", "auth_token": "alice:secret"}, warnings, "x"
    )
    assert cred == {"type": "password", "username": "alice", "password": "secret"}
    assert warnings == []


def test_community_credentials_basic_splits_on_first_colon_only():
    warnings: list[str] = []
    cred = conv._community_credentials(
        {"auth_type": "basic", "auth_token": "alice:a:b"}, warnings, "x"
    )
    assert cred == {"type": "password", "username": "alice", "password": "a:b"}


def test_community_credentials_basic_env_var_becomes_custom():
    warnings: list[str] = []
    cred = conv._community_credentials(
        {"auth_type": "Basic", "auth_token_env_var": "DH_BASIC"}, warnings, "x"
    )
    assert cred == {
        "type": "custom",
        "auth_type": "Basic",
        "auth_token": "${env:DH_BASIC}",
    }
    assert warnings == []


def test_community_credentials_basic_without_colon_warns():
    warnings: list[str] = []
    cred = conv._community_credentials(
        {"auth_type": "Basic", "auth_token": "nocolon"}, warnings, "x"
    )
    assert cred == {"type": "custom", "auth_type": "Basic", "auth_token": "nocolon"}
    assert any("username:password" in w for w in warnings)


def test_community_credentials_basic_without_token_raises():
    with pytest.raises(conv.ConversionError, match="Basic auth has no token"):
        conv._community_credentials({"auth_type": "Basic"}, [], "x")


def test_community_credentials_custom_handler_passthrough():
    warnings: list[str] = []
    cred = conv._community_credentials(
        {"auth_type": "com.example.MyHandler", "auth_token": "tok"}, warnings, "x"
    )
    assert cred == {
        "type": "custom",
        "auth_type": "com.example.MyHandler",
        "auth_token": "tok",
    }
    assert any("custom authentication handler" in w for w in warnings)


def test_community_credentials_custom_handler_without_token_raises():
    with pytest.raises(conv.ConversionError, match="custom auth .* has no token"):
        conv._community_credentials({"auth_type": "com.example.MyHandler"}, [], "x")


# ---------------------------------------------------------------------------
# enterprise credentials
# ---------------------------------------------------------------------------


def test_enterprise_credentials_password_literal_and_env():
    warnings: list[str] = []
    assert conv._enterprise_credentials(
        {"auth_type": "password", "username": "iris", "password": "pw"}, warnings, "x"
    ) == {"type": "password", "username": "iris", "password": "pw"}
    assert conv._enterprise_credentials(
        {"auth_type": "password", "username": "iris", "password_env_var": "PW"},
        warnings,
        "x",
    ) == {"type": "password", "username": "iris", "password": "${env:PW}"}


def test_enterprise_credentials_private_key_file_ref():
    warnings: list[str] = []
    cred = conv._enterprise_credentials(
        {"auth_type": "private_key", "private_key_path": "/etc/dh/key.pem"},
        warnings,
        "x",
    )
    assert cred == {"type": "private_key", "key_text": "${file:/etc/dh/key.pem}"}


def test_enterprise_credentials_unknown_raises():
    # v1 enterprise validation hard-rejected any auth_type other than 'password'
    # or 'private_key', so an unknown value cannot come from a valid v1 file; the
    # converter mirrors that rejection rather than emitting an invalid credential.
    warnings: list[str] = []
    with pytest.raises(conv.ConversionError, match="unsupported enterprise auth_type"):
        conv._enterprise_credentials({"auth_type": "saml"}, warnings, "x")


def test_enterprise_credentials_password_missing_secret_raises():
    with pytest.raises(conv.ConversionError, match="password auth has no password"):
        conv._enterprise_credentials(
            {"auth_type": "password", "username": "iris"}, [], "x"
        )


def test_enterprise_credentials_private_key_missing_path_raises():
    with pytest.raises(
        conv.ConversionError, match="private_key auth has no private_key_path"
    ):
        conv._enterprise_credentials({"auth_type": "private_key"}, [], "x")


# ---------------------------------------------------------------------------
# file ref / tls
# ---------------------------------------------------------------------------


def test_file_ref_relative_path_warns():
    warnings: list[str] = []
    assert conv._file_ref("rel/ca.pem", warnings, "x") == "${file:rel/ca.pem}"
    assert any("not absolute" in w for w in warnings)


def test_tls_block_none_when_no_tls():
    assert conv._tls_block({"host": "h"}, [], "x") is None


def test_tls_block_root_and_client_cert():
    warnings: list[str] = []
    tls = conv._tls_block(
        {
            "use_tls": True,
            "tls_root_certs": "/c/ca.pem",
            "client_cert_chain": "/c/cert.pem",
            "client_private_key": "/c/key.pem",
        },
        warnings,
        "x",
    )
    assert tls == {
        "root_certs": "${file:/c/ca.pem}",
        "client_certificate": {
            "cert_chain": "${file:/c/cert.pem}",
            "private_key": "${file:/c/key.pem}",
        },
    }
    assert warnings == []


def test_tls_block_partial_client_cert_warns():
    warnings: list[str] = []
    tls = conv._tls_block(
        {"use_tls": True, "client_cert_chain": "/c/cert.pem"}, warnings, "x"
    )
    assert tls == {"client_certificate": {"cert_chain": "${file:/c/cert.pem}"}}
    assert any("needs both" in w for w in warnings)


# ---------------------------------------------------------------------------
# convert: top-level structure + minimum-viable omission
# ---------------------------------------------------------------------------


def test_convert_empty_produces_no_files():
    result = conv.convert({})
    assert result.files == {}


def test_convert_community_session_and_settings():
    v1 = {
        "security": {"community": {"credential_retrieval_mode": "all"}},
        "community": {
            "sessions": {
                "local": {
                    "host": "localhost",
                    "port": 10000,
                    "session_type": "python",
                    "auth_type": "PSK",
                    "auth_token": "tok",
                }
            },
            "session_creation": {
                "max_concurrent_sessions": 1,
                "defaults": {"launch_method": "python", "heap_size_gb": 4},
            },
        },
    }
    result = conv.convert(v1)
    assert result.files["community/sessions/local.json"] == {
        "session_name": "local",
        "host": "localhost",
        "port": 10000,
        "programming_language": "Python",
        "auth": {"credentials": {"type": "psk", "token": "tok"}},
    }
    assert result.files["community/settings.json"] == {
        "security": {"credential_retrieval_mode": "all"},
        "session_creation": {
            "max_concurrent_sessions": 1,
            "defaults": {"launch_method": "python", "heap_size_gb": 4},
        },
    }


def test_convert_credential_mode_none_omits_security_block():
    v1 = {"security": {"community": {"credential_retrieval_mode": "none"}}}
    assert conv.convert(v1).files == {}


def test_convert_docker_image_maps_to_configured_language():
    # A single v1 docker_image becomes the image for the configured language
    # (Python by default); the other language keeps its v2 schema default.
    v1 = {
        "community": {
            "session_creation": {
                "max_concurrent_sessions": 2,
                "defaults": {"launch_method": "docker", "docker_image": "img:1"},
            }
        }
    }
    result = conv.convert(v1)
    docker = result.files["community/settings.json"]["session_creation"]["defaults"][
        "docker"
    ]
    assert docker == {"images": {"python": "img:1"}}
    assert result.warnings == []


def test_convert_docker_image_groovy_language():
    v1 = {
        "community": {
            "session_creation": {
                "defaults": {
                    "docker_image": "img:1",
                    "programming_language": "groovy",
                },
            }
        }
    }
    defaults = conv.convert(v1).files["community/settings.json"]["session_creation"][
        "defaults"
    ]
    assert defaults["docker"] == {"images": {"groovy": "img:1"}}
    assert defaults["programming_language"] == "Groovy"


def test_convert_community_session_creation_zero_dropped_silently():
    v1 = {"community": {"session_creation": {"max_concurrent_sessions": 0}}}
    result = conv.convert(v1)
    assert "community/settings.json" not in result.files
    assert result.warnings == []


def test_convert_enterprise_system_full():
    v1 = {
        "enterprise": {
            "systems": {
                "prod": {
                    "connection_json_url": "https://x/iris/connection.json",
                    "auth_type": "password",
                    "username": "iris",
                    "password_env_var": "PW",
                    "connection_timeout": 30,
                    "session_creation": {
                        "max_concurrent_sessions": 5,
                        "defaults": {
                            "heap_size_gb": 8,
                            "programming_language": "groovy",
                            "extra_environment_vars": ["FOO=bar", "BAZ=qux"],
                            "timeout_seconds": 99,
                        },
                    },
                }
            }
        }
    }
    result = conv.convert(v1)
    system = result.files["enterprise/systems/prod.json"]
    assert system == {
        "system_name": "prod",
        "connection_json_url": "https://x/iris/connection.json",
        "auth": {
            "credentials": {
                "type": "password",
                "username": "iris",
                "password": "${env:PW}",
            }
        },
        "session_creation": {
            "max_concurrent_sessions": 5,
            "defaults": {
                "heap_size_gb": 8,
                "programming_language": "Groovy",
                "environment_vars": {"FOO": "bar", "BAZ": "qux"},
            },
        },
    }
    assert result.files["enterprise/settings.json"] == {
        "timeouts": {"client": {"session_connect_timeout_seconds": 30}}
    }
    assert not any("connection_timeout" in w for w in result.warnings)
    assert any("timeout_seconds has no v2" in w for w in result.warnings)


def test_convert_enterprise_connection_timeout_single_value():
    base = {
        "connection_json_url": "u",
        "auth_type": "password",
        "username": "u",
        "password": "p",
        "connection_timeout": 45,
    }
    v1 = {"enterprise": {"systems": {"a": dict(base), "b": dict(base)}}}
    result = conv.convert(v1)
    assert result.files["enterprise/settings.json"] == {
        "timeouts": {"client": {"session_connect_timeout_seconds": 45}}
    }
    assert result.warnings == []


def test_convert_enterprise_connection_timeout_conflict_warns():
    def system(timeout):
        return {
            "connection_json_url": "u",
            "auth_type": "password",
            "username": "u",
            "password": "p",
            "connection_timeout": timeout,
        }

    v1 = {"enterprise": {"systems": {"a": system(30), "b": system(60)}}}
    result = conv.convert(v1)
    assert "enterprise/settings.json" not in result.files
    assert any("different connection_timeout" in w for w in result.warnings)


def test_convert_extra_environment_vars_malformed_warns():
    v1 = {
        "enterprise": {
            "systems": {
                "s": {
                    "connection_json_url": "u",
                    "auth_type": "password",
                    "username": "u",
                    "password": "p",
                    "session_creation": {
                        "defaults": {"extra_environment_vars": ["NOEQ"]}
                    },
                }
            }
        }
    }
    result = conv.convert(v1)
    defaults = result.files["enterprise/systems/s.json"]["session_creation"]["defaults"]
    assert "environment_vars" not in defaults
    assert any("not in 'NAME=value'" in w for w in result.warnings)


def test_convert_unknown_fields_warn():
    v1 = {
        "community": {
            "sessions": {"s": {"host": "h", "bogus": 1, "auth_type": "Anonymous"}}
        }
    }
    result = conv.convert(v1)
    assert any("unknown v1 field 'bogus'" in w for w in result.warnings)


def test_convert_missing_connection_url_warns():
    v1 = {
        "enterprise": {
            "systems": {
                "s": {"auth_type": "password", "username": "u", "password": "p"}
            }
        }
    }
    result = conv.convert(v1)
    assert any("missing required connection_json_url" in w for w in result.warnings)


@pytest.mark.parametrize("bad_key", ["community", "enterprise"])
def test_convert_non_object_section_raises(bad_key):
    with pytest.raises(conv.ConversionError):
        conv.convert({bad_key: [1]})


# ---------------------------------------------------------------------------
# resolve_output_dir
# ---------------------------------------------------------------------------


def test_resolve_output_dir_explicit(tmp_path):
    assert conv.resolve_output_dir(str(tmp_path / "c")) == tmp_path / "c"


def test_resolve_output_dir_data_dir_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DH_MCP_DATA_DIR", str(tmp_path))
    assert conv.resolve_output_dir(None) == tmp_path / "config"


def test_resolve_output_dir_default(monkeypatch):
    monkeypatch.delenv("DH_MCP_DATA_DIR", raising=False)
    assert conv.resolve_output_dir(None) == conv._default_data_root() / "config"


# ---------------------------------------------------------------------------
# write_tree
# ---------------------------------------------------------------------------


def test_write_tree_contents_and_permissions(tmp_path):
    root = tmp_path / "cfg"
    conv.write_tree(root, {"community/sessions/a.json": {"x": 1}})
    written = root / "community" / "sessions" / "a.json"
    assert json.loads(written.read_text()) == {"x": 1}
    if os.name == "posix":
        assert stat.S_IMODE(written.stat().st_mode) == 0o600
        assert stat.S_IMODE((root / "community").stat().st_mode) == 0o700
        assert stat.S_IMODE(root.stat().st_mode) == 0o700


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _write_v1(tmp_path: Path) -> Path:
    path = tmp_path / "v1.json"
    path.write_text(
        json.dumps(
            {"community": {"sessions": {"s": {"host": "h", "auth_type": "Anonymous"}}}}
        ),
        encoding="utf-8",
    )
    return path


def test_main_dry_run_writes_nothing(tmp_path, capsys):
    v1 = _write_v1(tmp_path)
    out = tmp_path / "out"
    rc = conv.main([str(v1), "--output", str(out), "--dry-run"])
    assert rc == 0
    assert not out.exists()
    assert "Would write" in capsys.readouterr().out


def test_main_yes_writes_tree(tmp_path):
    v1 = _write_v1(tmp_path)
    out = tmp_path / "out"
    rc = conv.main([str(v1), "--output", str(out), "--yes"])
    assert rc == 0
    assert (out / "community" / "sessions" / "s.json").is_file()


def test_main_conversion_error_returns_2(tmp_path):
    bad = tmp_path / "v1.json"
    bad.write_text('{"community": "oops"}', encoding="utf-8")
    assert conv.main([str(bad), "--output", str(tmp_path / "o"), "--yes"]) == 2


def test_main_nothing_to_write_returns_0(tmp_path):
    empty = tmp_path / "v1.json"
    empty.write_text("{}", encoding="utf-8")
    assert conv.main([str(empty), "--output", str(tmp_path / "o"), "--yes"]) == 0


def test_main_confirm_decline_aborts(tmp_path, monkeypatch):
    v1 = _write_v1(tmp_path)
    out = tmp_path / "out"
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")
    rc = conv.main([str(v1), "--output", str(out)])
    assert rc == 0
    assert not out.exists()


def test_main_existing_dir_stop(tmp_path, monkeypatch):
    v1 = _write_v1(tmp_path)
    out = tmp_path / "out"
    (out / "sub").mkdir(parents=True)
    (out / "sub" / "keep.txt").write_text("keep", encoding="utf-8")
    # confirm target (yes), then choose stop for existing dir
    replies = iter(["y", "s"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(replies))
    rc = conv.main([str(v1), "--output", str(out)])
    assert rc == 0
    assert (out / "sub" / "keep.txt").exists()
    assert not (out / "community").exists()


def test_main_existing_dir_delete(tmp_path, monkeypatch):
    v1 = _write_v1(tmp_path)
    out = tmp_path / "out"
    (out / "sub").mkdir(parents=True)
    (out / "sub" / "old.txt").write_text("old", encoding="utf-8")
    # confirm target (yes), choose delete, confirm delete (yes)
    replies = iter(["y", "d", "y"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(replies))
    rc = conv.main([str(v1), "--output", str(out)])
    assert rc == 0
    assert not (out / "sub").exists()
    assert (out / "community" / "sessions" / "s.json").is_file()


def test_main_existing_dir_write_into(tmp_path, monkeypatch):
    v1 = _write_v1(tmp_path)
    out = tmp_path / "out"
    (out / "sub").mkdir(parents=True)
    (out / "sub" / "keep.txt").write_text("keep", encoding="utf-8")
    replies = iter(["y", "w"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(replies))
    rc = conv.main([str(v1), "--output", str(out)])
    assert rc == 0
    assert (out / "sub" / "keep.txt").exists()
    assert (out / "community" / "sessions" / "s.json").is_file()


def test_existing_dir_action_reprompts_on_bad_input(monkeypatch):
    replies = iter(["?", "d"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(replies))
    assert conv._existing_dir_action() == "delete"


# ---------------------------------------------------------------------------
# branch coverage: optional fields, defensive guards, helpers
# ---------------------------------------------------------------------------


def test_file_ref_none_returns_none():
    assert conv._file_ref(None, [], "x") is None


def test_warn_unknown_ignores_non_dict():
    warnings: list[str] = []
    conv._warn_unknown([1, 2], set(), warnings, "x")
    assert warnings == []


def test_convert_community_session_optional_fields():
    v1 = {
        "community": {
            "sessions": {
                "s": {
                    "host": "h",
                    "never_timeout": True,
                    "use_tls": True,
                    "tls_root_certs": "/c/ca.pem",
                    "auth_type": "Anonymous",
                }
            }
        }
    }
    session = conv.convert(v1).files["community/sessions/s.json"]
    assert session["never_timeout"] is True
    assert session["tls"] == {"root_certs": "${file:/c/ca.pem}"}


def test_convert_community_defaults_docker_options_venv_and_auth():
    v1 = {
        "community": {
            "session_creation": {
                "defaults": {
                    "docker_memory_limit_gb": 8,
                    "docker_cpu_limit": 4,
                    "docker_volumes": ["/h:/c"],
                    "python_venv_path": "/opt/venv",
                    "auth_type": "PSK",
                    "auth_token": "tok",
                }
            }
        }
    }
    defaults = conv.convert(v1).files["community/settings.json"]["session_creation"][
        "defaults"
    ]
    assert defaults["docker"] == {
        "memory_limit_gb": 8,
        "cpu_limit": 4,
        "volumes": ["/h:/c"],
    }
    assert defaults["python"] == {"venv_path": "/opt/venv"}
    assert defaults["auth"] == {"credentials": {"type": "psk", "token": "tok"}}


def test_convert_community_session_not_object_raises():
    with pytest.raises(conv.ConversionError, match="expected an object"):
        conv.convert({"community": {"sessions": {"s": [1]}}})


def test_convert_community_session_creation_not_object_raises():
    with pytest.raises(conv.ConversionError, match="expected an object"):
        conv.convert({"community": {"session_creation": [1]}})


def test_convert_sessions_not_object_raises():
    with pytest.raises(conv.ConversionError, match="community.sessions"):
        conv.convert({"community": {"sessions": [1]}})


def _enterprise_one(extra: dict) -> dict:
    system = {
        "connection_json_url": "u",
        "auth_type": "password",
        "username": "u",
        "password": "p",
    }
    system.update(extra)
    return {"enterprise": {"systems": {"s": system}}}


def test_convert_enterprise_session_creation_not_object_raises():
    with pytest.raises(
        conv.ConversionError, match="session_creation must be an object"
    ):
        conv.convert(_enterprise_one({"session_creation": [1]}))


def test_convert_community_session_creation_unknown_field_warns():
    v1 = {"community": {"session_creation": {"bogus_sc_field": 1}}}
    result = conv.convert(v1)
    assert any(
        "session_creation" in w and "bogus_sc_field" in w for w in result.warnings
    )


def test_convert_enterprise_session_creation_unknown_field_warns():
    result = conv.convert(_enterprise_one({"session_creation": {"bogus_sc_field": 1}}))
    assert any(
        "session_creation" in w and "bogus_sc_field" in w for w in result.warnings
    )


def test_convert_enterprise_session_creation_zero_dropped():
    system = conv.convert(
        _enterprise_one({"session_creation": {"max_concurrent_sessions": 0}})
    ).files["enterprise/systems/s.json"]
    assert "session_creation" not in system


def test_convert_enterprise_system_not_object_raises():
    with pytest.raises(conv.ConversionError, match="expected an object"):
        conv.convert({"enterprise": {"systems": {"s": [1]}}})


def test_convert_systems_not_object_raises():
    with pytest.raises(conv.ConversionError, match="enterprise.systems"):
        conv.convert({"enterprise": {"systems": [1]}})


def test_default_data_root_windows(monkeypatch):
    monkeypatch.setattr(conv.sys, "platform", "win32")
    monkeypatch.setenv("APPDATA", "/appdata")
    assert conv._default_data_root() == conv.Path("/appdata") / "Deephaven" / "ai"


def test_default_data_root_matches_package(monkeypatch):
    # The script re-implements deephaven_mcp's data-root default to stay
    # import-free; pin its copy to the package's public contract so the two
    # cannot silently diverge.
    from deephaven_mcp.config import resolve_data_root

    monkeypatch.delenv("DH_MCP_DATA_DIR", raising=False)
    assert conv._default_data_root() == resolve_data_root()


def test_try_chmod_ignores_oserror(tmp_path, monkeypatch):
    def boom(*_args, **_kwargs):
        raise OSError("nope")

    monkeypatch.setattr(conv.os, "chmod", boom)
    conv._try_chmod(tmp_path, 0o700)


def test_main_prints_warnings(tmp_path, capsys):
    v1 = tmp_path / "v1.json"
    v1.write_text(
        json.dumps(
            {
                "community": {
                    "sessions": {
                        "s": {
                            "host": "h",
                            "auth_type": "Basic",
                            "auth_token": "nocolon",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    rc = conv.main([str(v1), "--output", str(tmp_path / "o"), "--yes"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "ACTION REQUIRED" in out
    assert "need your review" in out


def test_main_existing_dir_delete_decline_aborts(tmp_path, monkeypatch):
    v1 = _write_v1(tmp_path)
    out = tmp_path / "out"
    (out / "sub").mkdir(parents=True)
    (out / "sub" / "keep.txt").write_text("keep", encoding="utf-8")
    # confirm target (yes), choose delete, decline the delete confirmation (no)
    replies = iter(["y", "d", "n"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(replies))
    rc = conv.main([str(v1), "--output", str(out)])
    assert rc == 0
    assert (out / "sub" / "keep.txt").exists()
