"""Tests for the JSON-value templating engine."""

from __future__ import annotations

import os

import pytest

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp.config._templating import (
    _MAX_FILE_TEMPLATE_BYTES,
    JsonLoc,
    expand_string,
    expand_tree,
    expand_tree_lenient,
    is_single_placeholder,
)

# ---------------------------------------------------------------------------
# is_single_placeholder
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("${env:DH_PSK}", True),
        ("${file:/etc/ssl/key.pem}", True),
        # A literal prefix may itself be sensitive, so the string is a
        # value, not a bare reference.
        ("tok-${env:DH_PSK}", False),
        ("${env:DH_PSK}-suffix", False),
        # Two adjacent placeholders are a concatenation, not one
        # reference: ``[^}]+`` cannot span the intervening ``}``.
        ("${env:A}${env:B}", False),
        # Syntactically malformed forms are values here; they fail later,
        # at load time, with a proper error.
        ("${}", False),
        ("${env:A", False),
        ("", False),
        ("plain-secret", False),
    ],
)
def test_is_single_placeholder(value: str, expected: bool) -> None:
    """Distinguishes a value that *points* at a secret from one that *is*
    a secret. Redaction leans on this to keep a bare reference legible."""
    assert is_single_placeholder(value) is expected


# ---------------------------------------------------------------------------
# JsonLoc
# ---------------------------------------------------------------------------


def test_jsonloc_root_is_empty() -> None:
    assert JsonLoc.ROOT == ()
    assert str(JsonLoc.ROOT) == "<root>"


def test_jsonloc_render_mixed_segments() -> None:
    loc = JsonLoc(("sessions", 0, "token"))
    assert str(loc) == "sessions[0].token"


def test_jsonloc_render_leading_index() -> None:
    assert str(JsonLoc((0, "a"))) == "[0].a"


def test_jsonloc_child_extends() -> None:
    loc = JsonLoc.ROOT.child("a").child(1)
    assert loc == ("a", 1)
    assert isinstance(loc, JsonLoc)


def test_jsonloc_equals_plain_tuple() -> None:
    # Pydantic's err["loc"] tuples must compare and hash identically.
    assert JsonLoc(("a", 0)) == ("a", 0)
    assert JsonLoc(("a", 0)) in frozenset({("a", 0)})
    assert ("a", 0) in frozenset({JsonLoc(("a", 0))})


def test_jsonloc_rejects_bare_str() -> None:
    with pytest.raises(TypeError, match="bare str"):
        JsonLoc("abc")


def test_jsonloc_repr() -> None:
    assert repr(JsonLoc(("a", 0))) == "JsonLoc(('a', 0))"


# ---------------------------------------------------------------------------
# expand_string --- env placeholder
# ---------------------------------------------------------------------------


def test_env_required_present(monkeypatch):
    monkeypatch.setenv("DH_TEST_VAR", "hello")
    assert expand_string("${env:DH_TEST_VAR}", source="t.json", path="x") == "hello"


def test_env_required_missing_raises(monkeypatch):
    monkeypatch.delenv("DH_TEST_MISSING", raising=False)
    with pytest.raises(ConfigurationError, match="DH_TEST_MISSING"):
        expand_string("${env:DH_TEST_MISSING}", source="t.json", path="x")


def test_env_required_empty_raises(monkeypatch):
    monkeypatch.setenv("DH_TEST_EMPTY", "")
    with pytest.raises(ConfigurationError, match="DH_TEST_EMPTY"):
        expand_string("${env:DH_TEST_EMPTY}", source="t.json", path="x")


def test_env_default_used_when_missing(monkeypatch):
    monkeypatch.delenv("DH_TEST_MISSING", raising=False)
    assert (
        expand_string("${env:DH_TEST_MISSING:-fallback}", source="t.json", path="x")
        == "fallback"
    )


def test_env_default_used_when_empty(monkeypatch):
    monkeypatch.setenv("DH_TEST_EMPTY", "")
    assert (
        expand_string("${env:DH_TEST_EMPTY:-fallback}", source="t.json", path="x")
        == "fallback"
    )


def test_env_default_empty_string(monkeypatch):
    monkeypatch.delenv("DH_TEST_MISSING", raising=False)
    assert expand_string("${env:DH_TEST_MISSING:-}", source="t.json", path="x") == ""


def test_env_default_with_colons(monkeypatch):
    # The default literal may contain colons (only ``:-`` splits).
    monkeypatch.delenv("DH_TEST_MISSING", raising=False)
    assert (
        expand_string("${env:DH_TEST_MISSING:-a:b:c}", source="t.json", path="x")
        == "a:b:c"
    )


def test_env_value_overrides_default(monkeypatch):
    monkeypatch.setenv("DH_TEST_VAR", "actual")
    assert (
        expand_string("${env:DH_TEST_VAR:-fallback}", source="t.json", path="x")
        == "actual"
    )


def test_env_empty_name_raises():
    with pytest.raises(ConfigurationError, match="empty env-var name"):
        expand_string("${env:}", source="t.json", path="x")


# ---------------------------------------------------------------------------
# expand_string --- file placeholder
# ---------------------------------------------------------------------------


def test_file_present(tmp_path):
    path = tmp_path / "key.pem"
    path.write_text("-----BEGIN-----\nabc\n", encoding="utf-8")
    out = expand_string(f"${{file:{path}}}", source="t.json", path="x")
    assert out == "-----BEGIN-----\nabc\n"


def test_file_preserves_trailing_newline(tmp_path):
    path = tmp_path / "k"
    path.write_text("line1\nline2\n", encoding="utf-8")
    out = expand_string(f"${{file:{path}}}", source="t.json", path="x")
    assert out.endswith("\n")


def test_file_expands_tilde(tmp_path, monkeypatch):
    """A leading ``~`` in ``${file:PATH}`` resolves against the user's home."""
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / "token.txt").write_text("SECRET", encoding="utf-8")
    out = expand_string("${file:~/token.txt}", source="t.json", path="x")
    assert out == "SECRET"


def test_file_missing_raises(tmp_path):
    bogus = tmp_path / "nope"
    with pytest.raises(ConfigurationError, match="does not exist"):
        expand_string(f"${{file:{bogus}}}", source="t.json", path="x")


def test_file_non_utf8_raises(tmp_path):
    path = tmp_path / "binary"
    path.write_bytes(b"\xff\xfe\xfa")
    with pytest.raises(ConfigurationError, match="not valid UTF-8"):
        expand_string(f"${{file:{path}}}", source="t.json", path="x")


def test_file_empty_path_raises():
    with pytest.raises(ConfigurationError, match="empty file path"):
        expand_string("${file:}", source="t.json", path="x")


def test_file_rejects_fallback_syntax(tmp_path):
    with pytest.raises(ConfigurationError, match="does not support ':-default'"):
        expand_string(f"${{file:{tmp_path / 'k'}:-default}}", source="t.json", path="x")


def test_file_permission_denied_raises(tmp_path, monkeypatch):
    path = tmp_path / "k"
    path.write_text("data", encoding="utf-8")

    def _raise_permission(*_args, **_kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr("builtins.open", _raise_permission)
    with pytest.raises(ConfigurationError, match="permission denied"):
        expand_string(f"${{file:{path}}}", source="t.json", path="x")


def test_file_other_oserror_raises(tmp_path, monkeypatch):
    path = tmp_path / "k"
    path.write_text("data", encoding="utf-8")

    def _raise_oserror(*_args, **_kwargs):
        raise OSError("disk gone")

    monkeypatch.setattr("builtins.open", _raise_oserror)
    with pytest.raises(ConfigurationError, match="cannot read file"):
        expand_string(f"${{file:{path}}}", source="t.json", path="x")


# ---------------------------------------------------------------------------
# expand_string --- file placeholder: safety constraints
# ---------------------------------------------------------------------------


def test_file_template_follows_symlink(tmp_path):
    """A symlink is followed (common for system CA bundles)."""
    real = tmp_path / "real.pem"
    real.write_text("KEY", encoding="utf-8")
    link = tmp_path / "link.pem"
    os.symlink(real, link)
    out = expand_string(
        f"${{file:{link}}}",
        source="t.json",
        path="x",
        config_dir=tmp_path,
    )
    assert out == "KEY"


def test_file_template_allows_absolute_path_outside_config_dir(tmp_path):
    """An absolute path outside ``config_dir`` reads (e.g. a system CA bundle)."""
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    outside_file = outside_dir / "ca.pem"
    outside_file.write_text("DATA", encoding="utf-8")

    config_dir = tmp_path / "ai"
    config_dir.mkdir()

    out = expand_string(
        f"${{file:{outside_file}}}",
        source="t.json",
        path="x",
        config_dir=config_dir,
    )
    assert out == "DATA"


def test_file_template_resolves_relative_path_against_config_dir(tmp_path):
    """A relative ``${file:}`` path is resolved against ``config_dir``."""
    config_dir = tmp_path / "ai"
    (config_dir / "certs").mkdir(parents=True)
    (config_dir / "certs" / "ca.pem").write_text("REL", encoding="utf-8")

    out = expand_string(
        "${file:certs/ca.pem}",
        source="t.json",
        path="x",
        config_dir=config_dir,
    )
    assert out == "REL"


def test_file_template_rejects_too_large_file(tmp_path):
    """A file exceeding the size cap raises ``ConfigurationError``."""
    path = tmp_path / "big.bin"
    path.write_bytes(b"x" * (_MAX_FILE_TEMPLATE_BYTES + 1))
    with pytest.raises(ConfigurationError, match="exceeds the"):
        expand_string(
            f"${{file:{path}}}",
            source="t.json",
            path="x",
            config_dir=tmp_path,
        )


def test_file_template_reads_file_under_size_cap(tmp_path):
    """A file at-or-below the size cap reads cleanly."""
    payload = "y" * _MAX_FILE_TEMPLATE_BYTES
    path = tmp_path / "ok.pem"
    path.write_text(payload, encoding="utf-8")
    out = expand_string(
        f"${{file:{path}}}",
        source="t.json",
        path="x",
        config_dir=tmp_path,
    )
    assert out == payload


def test_file_template_allows_any_path_when_config_dir_none(tmp_path):
    """``config_dir=None``: an absolute path resolves against the filesystem."""
    payload = "anywhere"
    path = tmp_path / "anywhere.pem"
    path.write_text(payload, encoding="utf-8")
    out = expand_string(
        f"${{file:{path}}}",
        source="t.json",
        path="x",
    )
    assert out == payload


# ---------------------------------------------------------------------------
# expand_string --- structural
# ---------------------------------------------------------------------------


def test_no_placeholder_passes_through():
    assert expand_string("literal", source="t.json", path="x") == "literal"


def test_empty_string_passes_through():
    assert expand_string("", source="t.json", path="x") == ""


def test_substring_expansion(monkeypatch):
    monkeypatch.setenv("DH_HOST", "127.0.0.1")
    monkeypatch.setenv("DH_PORT", "10000")
    out = expand_string(
        "https://${env:DH_HOST}:${env:DH_PORT}/api",
        source="t.json",
        path="x",
    )
    assert out == "https://127.0.0.1:10000/api"


def test_multiple_placeholders_same_kind(monkeypatch):
    monkeypatch.setenv("A", "1")
    monkeypatch.setenv("B", "2")
    assert expand_string("${env:A}-${env:B}", source="t.json", path="x") == "1-2"


def test_unknown_kind_raises():
    with pytest.raises(ConfigurationError, match="unknown placeholder kind"):
        expand_string("${vault:secret}", source="t.json", path="x")


def test_malformed_no_colon_raises():
    with pytest.raises(ConfigurationError, match="malformed placeholder"):
        expand_string("${bare}", source="t.json", path="x")


def test_error_includes_source_and_path(monkeypatch):
    monkeypatch.delenv("DH_X", raising=False)
    with pytest.raises(
        ConfigurationError,
        match="In src.json at deep.field: env var 'DH_X' is not set",
    ):
        expand_string("${env:DH_X}", source="src.json", path="deep.field")


# ---------------------------------------------------------------------------
# expand_tree
# ---------------------------------------------------------------------------


def test_tree_dict_recursion(monkeypatch):
    monkeypatch.setenv("DH_TOKEN", "secret")
    tree = {"credentials": {"type": "psk", "token": "${env:DH_TOKEN}"}}
    out = expand_tree(tree, source="t.json")
    assert out == {"credentials": {"type": "psk", "token": "secret"}}


def test_tree_list_recursion(monkeypatch):
    monkeypatch.setenv("A", "x")
    monkeypatch.setenv("B", "y")
    out = expand_tree(["${env:A}", "${env:B}", "literal"], source="t.json")
    assert out == ["x", "y", "literal"]


def test_tree_non_string_scalars_pass_through():
    tree = {"port": 10000, "enabled": True, "nothing": None, "ratio": 1.5}
    assert expand_tree(tree, source="t.json") == tree


def test_tree_dict_keys_not_expanded(monkeypatch):
    monkeypatch.setenv("KEYNAME", "x")
    tree = {"${env:KEYNAME}": "literal"}
    out = expand_tree(tree, source="t.json")
    # Key is unchanged; only values expand.
    assert out == {"${env:KEYNAME}": "literal"}


def test_tree_deep_nesting(monkeypatch):
    monkeypatch.setenv("SECRET", "shh")
    tree = {
        "a": {
            "b": [
                {"c": "${env:SECRET}"},
                {"c": ["nested", "${env:SECRET}"]},
            ]
        }
    }
    out = expand_tree(tree, source="t.json")
    assert out == {
        "a": {
            "b": [
                {"c": "shh"},
                {"c": ["nested", "shh"]},
            ]
        }
    }


def test_tree_error_path_includes_dotted_path(monkeypatch):
    monkeypatch.delenv("MISSING", raising=False)
    tree = {"outer": {"inner": "${env:MISSING}"}}
    with pytest.raises(
        ConfigurationError, match="In t.json at outer.inner: env var 'MISSING'"
    ):
        expand_tree(tree, source="t.json")


def test_tree_error_path_includes_list_index(monkeypatch):
    monkeypatch.delenv("MISSING", raising=False)
    tree = {"items": ["ok", "${env:MISSING}"]}
    with pytest.raises(
        ConfigurationError, match=r"In t.json at items\[1\]: env var 'MISSING'"
    ):
        expand_tree(tree, source="t.json")


def test_tree_does_not_mutate_input(monkeypatch):
    monkeypatch.setenv("X", "x")
    original = {"a": "${env:X}", "b": [1, "${env:X}"]}
    snapshot = {"a": "${env:X}", "b": [1, "${env:X}"]}
    expand_tree(original, source="t.json")
    assert original == snapshot


def test_tree_root_path_label(monkeypatch):
    monkeypatch.delenv("MISSING", raising=False)
    with pytest.raises(
        ConfigurationError, match="In t.json at <root>: env var 'MISSING'"
    ):
        expand_tree("${env:MISSING}", source="t.json")


def test_tree_mixed_with_file(monkeypatch, tmp_path):
    monkeypatch.setenv("USER", "alice")
    keyfile = tmp_path / "k.pem"
    keyfile.write_text("KEY", encoding="utf-8")
    tree = {
        "username": "${env:USER}",
        "private_key": f"${{file:{keyfile}}}",
        "literal": "no-placeholders",
    }
    out = expand_tree(tree, source="t.json")
    assert out == {
        "username": "alice",
        "private_key": "KEY",
        "literal": "no-placeholders",
    }


# ---------------------------------------------------------------------------
# expand_tree_lenient
# ---------------------------------------------------------------------------


def test_lenient_resolvable_expands_with_no_warnings(monkeypatch):
    monkeypatch.setenv("DH_TOKEN", "secret")
    tree = {"credentials": {"token": "${env:DH_TOKEN}"}}
    result = expand_tree_lenient(tree, source="t.json")
    assert result.value == {"credentials": {"token": "secret"}}
    assert result.warnings == []
    assert result.unresolved_locations == frozenset()


def test_lenient_unresolved_env_kept_verbatim_with_warning(monkeypatch):
    monkeypatch.delenv("DH_MISSING", raising=False)
    tree = {"token": "${env:DH_MISSING}"}
    result = expand_tree_lenient(tree, source="t.json")
    assert result.value == {"token": "${env:DH_MISSING}"}
    assert len(result.warnings) == 1
    assert "DH_MISSING" in result.warnings[0]
    assert result.unresolved_locations == frozenset({("token",)})


def test_lenient_unresolved_file_kept_verbatim_with_warning(tmp_path):
    bogus = tmp_path / "nope.pem"
    tree = {"key": f"${{file:{bogus}}}"}
    result = expand_tree_lenient(tree, source="t.json")
    assert result.value == tree
    assert len(result.warnings) == 1
    assert "does not exist" in result.warnings[0]
    assert result.unresolved_locations == frozenset({("key",)})


def test_lenient_collects_multiple_warnings_across_nesting(monkeypatch):
    monkeypatch.delenv("DH_A", raising=False)
    monkeypatch.delenv("DH_B", raising=False)
    tree = {"outer": {"a": "${env:DH_A}"}, "items": ["${env:DH_B}"]}
    result = expand_tree_lenient(tree, source="t.json")
    assert result.value == tree
    assert len(result.warnings) == 2
    assert any("DH_A" in w for w in result.warnings)
    assert any("DH_B" in w for w in result.warnings)
    assert result.unresolved_locations == frozenset({("outer", "a"), ("items", 0)})


def test_lenient_syntax_error_still_raises():
    with pytest.raises(ConfigurationError, match="unknown placeholder kind"):
        expand_tree_lenient({"x": "${vault:secret}"}, source="t.json")


def test_syntax_error_after_unresolvable_still_raises(monkeypatch):
    """A malformed placeholder is fatal even when an earlier one is unresolvable.

    Regression: resolution stops at the first failure, so without an
    up-front syntax pass the trailing ``${vault:...}`` would never be
    validated and a malformed template could be persisted.
    """
    monkeypatch.delenv("DH_MISSING", raising=False)
    with pytest.raises(ConfigurationError, match="unknown placeholder kind"):
        expand_string("${env:DH_MISSING}${vault:secret}", source="t.json", path="x")


def test_lenient_syntax_error_after_unresolvable_still_raises(monkeypatch):
    monkeypatch.delenv("DH_MISSING", raising=False)
    with pytest.raises(ConfigurationError, match="unknown placeholder kind"):
        expand_tree_lenient({"x": "${env:DH_MISSING}${vault:secret}"}, source="t.json")


def test_lenient_does_not_mutate_input(monkeypatch):
    monkeypatch.delenv("DH_MISSING", raising=False)
    original = {"a": "${env:DH_MISSING}", "b": [1, "${env:DH_MISSING}"]}
    snapshot = {"a": "${env:DH_MISSING}", "b": [1, "${env:DH_MISSING}"]}
    expand_tree_lenient(original, source="t.json")
    assert original == snapshot


def test_lenient_warnings_independent_per_call(monkeypatch):
    # Regression: warnings must not accumulate across calls (the
    # function owns a fresh list per invocation, never a shared or
    # caller-supplied one).
    monkeypatch.delenv("DH_MISSING", raising=False)
    first = expand_tree_lenient({"x": "${env:DH_MISSING}"}, source="t.json")
    second = expand_tree_lenient({"x": "${env:DH_MISSING}"}, source="t.json")
    assert len(first.warnings) == 1
    assert len(second.warnings) == 1
    assert first.warnings is not second.warnings


# ---------------------------------------------------------------------------
# Nesting rejection
# ---------------------------------------------------------------------------


def test_nested_placeholder_not_supported(monkeypatch, tmp_path):
    # ``${file:${env:PATH_VAR}}`` --- the inner ``}`` closes the file form
    # with argument ``${env:PATH_VAR``, which is not a valid file path.
    monkeypatch.setenv("PATH_VAR", str(tmp_path / "k"))
    (tmp_path / "k").write_text("data", encoding="utf-8")
    # The outer ``${file:${env:PATH_VAR}`` matches; trailing ``}`` is literal.
    # The file path ``${env:PATH_VAR`` does not exist on disk.
    with pytest.raises(ConfigurationError):
        expand_string("${file:${env:PATH_VAR}}", source="t.json", path="x")
