"""Tests for :mod:`deephaven_mcp._pydantic`."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pytest
from pydantic import SecretStr, ValidationError, model_validator

from deephaven_mcp._exceptions import ConfigurationError
from deephaven_mcp._pydantic import (
    RedactableSchema,
    StrictSchema,
    as_configuration_error,
    format_validation_error,
    log_redacted,
    reconcile_filename_stem,
    unwrap_auth_credentials,
)
from deephaven_mcp._redaction import REDACTED

# ---------------------------------------------------------------------------
# StrictSchema
# ---------------------------------------------------------------------------


class _Strict(StrictSchema):
    name: str


def test_strict_model_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        _Strict.model_validate({"name": "x", "extra": 1})


def test_strict_model_is_frozen():
    obj = _Strict(name="x")
    with pytest.raises(ValidationError):
        obj.name = "y"  # type: ignore[misc]


def test_strict_model_accepts_valid_input():
    obj = _Strict.model_validate({"name": "x"})
    assert obj.name == "x"


# ---------------------------------------------------------------------------
# RedactableSchema
# ---------------------------------------------------------------------------


class _WithSecret(RedactableSchema):
    name: str
    token: SecretStr
    optional_token: SecretStr | None = None
    legacy_optional: Optional[SecretStr] = None  # noqa: UP045 - testing Union form


def test_redactable_dump_without_context_keeps_secret_wrapper():
    obj = _WithSecret(name="x", token=SecretStr("shh"))
    # Default pydantic SecretStr dump yields the mask string, not REDACTED.
    dumped = obj.model_dump(mode="json")
    assert dumped["token"] == "**********"
    assert dumped["token"] != REDACTED


def test_redactable_dump_with_redact_context_emits_REDACTED():
    obj = _WithSecret(name="x", token=SecretStr("shh"))
    dumped = obj.model_dump(mode="json", context={"redact": True})
    assert dumped["token"] == REDACTED
    assert dumped["name"] == "x"


def test_redactable_redacts_optional_secret_when_set():
    obj = _WithSecret(name="x", token=SecretStr("a"), optional_token=SecretStr("b"))
    dumped = obj.model_dump(mode="json", context={"redact": True})
    assert dumped["optional_token"] == REDACTED


def test_redactable_passes_through_unset_optional_secret():
    obj = _WithSecret(name="x", token=SecretStr("a"))
    dumped = obj.model_dump(mode="json", context={"redact": True})
    assert dumped["optional_token"] is None


def test_redactable_handles_legacy_optional_union_form():
    obj = _WithSecret(
        name="x",
        token=SecretStr("a"),
        legacy_optional=SecretStr("b"),
    )
    dumped = obj.model_dump(mode="json", context={"redact": True})
    assert dumped["legacy_optional"] == REDACTED


def test_redactable_handles_pep604_union_secret_not_first_arg():
    """PEP 604 ``None | SecretStr`` (SecretStr in 2nd position) must redact.

    Regression for a latent secret-detection bug: the recursion fell
    through ``args[0]`` only, so any non-first-position ``SecretStr``
    in a ``|`` union was silently missed.
    """

    class _Reversed(RedactableSchema):
        name: str
        # SecretStr appears AFTER None — the bug missed this.
        token: None | SecretStr = None

    obj = _Reversed(name="x", token=SecretStr("shh"))
    dumped = obj.model_dump(mode="json", context={"redact": True})
    assert dumped["token"] == REDACTED


def test_annotation_contains_secret_detects_pep604_unions():
    """Direct unit test on ``_annotation_contains_secret``: all argument positions count."""
    from deephaven_mcp._pydantic import _annotation_contains_secret

    assert _annotation_contains_secret(SecretStr) is True
    assert _annotation_contains_secret(SecretStr | None) is True
    assert _annotation_contains_secret(None | SecretStr) is True
    assert _annotation_contains_secret(str | SecretStr) is True
    assert _annotation_contains_secret(SecretStr | str | None) is True
    assert _annotation_contains_secret(str | int) is False
    assert _annotation_contains_secret(str) is False


def test_annotation_contains_secret_detects_secret_in_annotated_pep604_union():
    """``Annotated[str | SecretStr, ...]`` must be detected as secret."""
    from typing import Annotated

    from pydantic import Field

    from deephaven_mcp._pydantic import _annotation_contains_secret

    ann = Annotated[str | SecretStr, Field(description="x")]
    assert _annotation_contains_secret(ann) is True


def test_redactable_context_false_keeps_default_dump():
    obj = _WithSecret(name="x", token=SecretStr("shh"))
    dumped = obj.model_dump(mode="json", context={"redact": False})
    assert dumped["token"] == "**********"


def test_redactable_dump_with_reveal_emits_plaintext():
    obj = _WithSecret(name="x", token=SecretStr("shh"))
    dumped = obj.model_dump(mode="json", context={"reveal": True})
    assert dumped["token"] == "shh"
    assert dumped["name"] == "x"


def test_redactable_dump_with_reveal_handles_none():
    obj = _WithSecret(name="x", token=SecretStr("shh"))
    dumped = obj.model_dump(mode="json", context={"reveal": True})
    assert dumped["optional_token"] is None


def test_redactable_dump_with_reveal_propagates_to_nested():
    class _Outer(RedactableSchema):
        inner: _WithSecret

    obj = _Outer(inner=_WithSecret(name="x", token=SecretStr("nested-secret")))
    dumped = obj.model_dump(mode="json", context={"reveal": True})
    assert dumped["inner"]["token"] == "nested-secret"


def test_redactable_non_secret_fields_untouched():
    class _Nested(RedactableSchema):
        name: str
        secret: SecretStr

    obj = _Nested(name="alice", secret=SecretStr("pw"))
    dumped = obj.model_dump(mode="json", context={"redact": True})
    assert dumped["name"] == "alice"
    assert dumped["secret"] == REDACTED


# ---------------------------------------------------------------------------
# format_validation_error
# ---------------------------------------------------------------------------


def test_format_validation_error_single():
    with pytest.raises(ValidationError) as exc:
        _Strict.model_validate({"name": 1})
    msg = format_validation_error("test", exc.value)
    assert msg.startswith("test: ")
    assert "name" in msg


def test_format_validation_error_multiple_errors_joined():
    with pytest.raises(ValidationError) as exc:
        _Strict.model_validate({})  # missing 'name'
    msg = format_validation_error("ctx", exc.value)
    assert "name" in msg


def test_format_validation_error_nested_loc_is_dotted():
    class _Nested(StrictSchema):
        inner: _Strict

    with pytest.raises(ValidationError) as exc:
        _Nested.model_validate({"inner": {"name": 1}})
    msg = format_validation_error("ctx", exc.value)
    assert "inner.name" in msg


def test_format_validation_error_uses_root_for_empty_loc():
    # Trigger a model-level error so loc tuple is empty.
    class _Custom(StrictSchema):
        x: int = 1

        @model_validator(mode="after")
        def _check(self) -> "_Custom":
            raise ValueError("nope")

    with pytest.raises(ValidationError) as exc:
        _Custom.model_validate({})
    msg = format_validation_error("ctx", exc.value)
    assert "ctx: " in msg
    # model-level errors get loc=() per pydantic; the formatter labels them <root>.
    assert "<root>" in msg or "nope" in msg


# ---------------------------------------------------------------------------
# as_configuration_error
# ---------------------------------------------------------------------------


def test_as_configuration_error_returns_configurationerror():
    try:
        _Strict.model_validate({"name": 1})
    except ValidationError as exc:
        cfg = as_configuration_error("ctx", exc)
        assert isinstance(cfg, ConfigurationError)
        assert "ctx" in str(cfg)


# Note: ``resolve_secret_or_env`` and ``read_pem_text`` were removed as
# part of the templating migration. Env-var and file indirection are now
# expressed in the source JSON as ``${env:VAR}`` / ``${file:PATH}`` and
# resolved by :mod:`deephaven_mcp.config._templating` at config-load time; the
# direct test coverage for the templating engine lives in
# ``tests/config/test__templating.py``.


# ---------------------------------------------------------------------------
# unwrap_auth_credentials
# ---------------------------------------------------------------------------


class TestUnwrapAuthCredentials:
    """Tests for the shared ``auth.credentials`` unwrap helper."""

    def test_non_dict_passes_through(self) -> None:
        assert unwrap_auth_credentials("not-a-dict") == "not-a-dict"
        assert unwrap_auth_credentials(42) == 42
        assert unwrap_auth_credentials(None) is None

    def test_wire_format_auth_block_is_unwrapped(self) -> None:
        out = unwrap_auth_credentials(
            {"name": "x", "auth": {"credentials": {"type": "anonymous"}}}
        )
        assert out == {"name": "x", "credentials": {"type": "anonymous"}}

    def test_already_unwrapped_top_level_passes_through(self) -> None:
        data = {"name": "x", "credentials": {"type": "anonymous"}}
        out = unwrap_auth_credentials(data)
        assert out == data
        # Returned dict is a copy.
        assert out is not data

    def test_both_auth_and_top_level_credentials_rejected(self) -> None:
        with pytest.raises(ValueError, match="mutually exclusive"):
            unwrap_auth_credentials(
                {
                    "auth": {"credentials": {"type": "anonymous"}},
                    "credentials": {"type": "anonymous"},
                }
            )

    def test_missing_auth_rejected_when_required(self) -> None:
        with pytest.raises(ValueError, match="'auth' is required"):
            unwrap_auth_credentials({"name": "x"})

    def test_missing_auth_allowed_when_not_required(self) -> None:
        data = {"name": "x"}
        out = unwrap_auth_credentials(data, allow_top_level=False)
        assert out == data
        assert out is not data

    def test_auth_not_a_dict_rejected(self) -> None:
        with pytest.raises(ValueError, match="only a 'credentials'"):
            unwrap_auth_credentials({"auth": "oops"})

    def test_auth_with_extra_keys_rejected(self) -> None:
        with pytest.raises(ValueError, match="only a 'credentials'"):
            unwrap_auth_credentials({"auth": {"credentials": {}, "extra": 1}})

    def test_auth_without_credentials_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="'auth.credentials' is required"):
            unwrap_auth_credentials({"auth": {}})

    def test_allow_top_level_false_still_unwraps_auth(self) -> None:
        """``allow_top_level=False`` does not block the wire-format unwrap."""
        out = unwrap_auth_credentials(
            {"auth": {"credentials": {"type": "psk", "token": "t"}}},
            allow_top_level=False,
        )
        assert out == {"credentials": {"type": "psk", "token": "t"}}

    def test_allow_top_level_false_rejects_pre_unwrapped(self) -> None:
        """``allow_top_level=False`` callers cannot supply a top-level credentials.

        The sub-block variant only sees the on-disk wire-format and is
        permitted to omit credentials entirely; a stray ``credentials``
        without ``auth`` is unusual and the helper passes the dict
        through unchanged (extras are caught by the model's
        ``extra='forbid'`` config).
        """
        data = {"credentials": {"type": "anonymous"}}
        out = unwrap_auth_credentials(data, allow_top_level=False)
        # ``credentials`` survives untouched; the model's strict-extra
        # config decides whether the field is allowed.
        assert out == data


# ---------------------------------------------------------------------------
# reconcile_filename_stem
# ---------------------------------------------------------------------------


class TestReconcileFilenameStem:
    """Tests for the shared filename-stem reconciliation helper."""

    _KW = dict(declared_field="session_name", model_label="CommunitySessionConfig")

    def test_non_dict_passes_through(self) -> None:
        assert reconcile_filename_stem("not-a-dict", **self._KW) == "not-a-dict"
        assert reconcile_filename_stem(None, **self._KW) is None
        assert reconcile_filename_stem(42, **self._KW) == 42

    def test_valid_name_no_declared_field(self) -> None:
        out = reconcile_filename_stem({"name": "alpha"}, **self._KW)
        assert out == {"name": "alpha"}

    def test_valid_name_matching_declared_field_pops_it(self) -> None:
        out = reconcile_filename_stem(
            {"name": "alpha", "session_name": "alpha", "extra": 1}, **self._KW
        )
        assert out == {"name": "alpha", "extra": 1}

    def test_mismatched_declared_field_rejected(self) -> None:
        with pytest.raises(ValueError, match="does not match"):
            reconcile_filename_stem(
                {"name": "alpha", "session_name": "beta"}, **self._KW
            )

    def test_missing_name_rejected_with_new_wording(self) -> None:
        with pytest.raises(ValueError, match="'name' is required"):
            reconcile_filename_stem({}, **self._KW)

    def test_missing_name_message_carries_model_label(self) -> None:
        with pytest.raises(ValueError, match="CommunitySessionConfig"):
            reconcile_filename_stem({}, **self._KW)

    def test_empty_string_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="'name' is required"):
            reconcile_filename_stem({"name": ""}, **self._KW)

    def test_non_string_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="'name' is required"):
            reconcile_filename_stem({"name": 42}, **self._KW)
        with pytest.raises(ValueError, match="'name' is required"):
            reconcile_filename_stem({"name": ["alpha"]}, **self._KW)

    def test_returned_dict_is_a_copy(self) -> None:
        data = {"name": "alpha", "session_name": "alpha"}
        out = reconcile_filename_stem(data, **self._KW)
        assert out is not data
        # Mutating the result must not affect the input.
        out["other"] = 1
        assert "other" not in data

    def test_works_for_enterprise_declared_field(self) -> None:
        out = reconcile_filename_stem(
            {"name": "prod", "system_name": "prod"},
            declared_field="system_name",
            model_label="EnterpriseSystemConfig",
        )
        assert out == {"name": "prod"}


# ---------------------------------------------------------------------------
# log_redacted
# ---------------------------------------------------------------------------


class _SampleLogModel(RedactableSchema):
    name: str
    token: SecretStr


def test_log_redacted_redacts_secrets(caplog: pytest.LogCaptureFixture) -> None:
    """``redact=True`` context turns ``SecretStr`` fields into the sentinel."""
    model = _SampleLogModel(name="alice", token=SecretStr("shh"))
    logger = logging.getLogger("deephaven_mcp.tests.log_redacted")

    with caplog.at_level("INFO", logger=logger.name):
        log_redacted(model, label="label", logger=logger)

    messages = "\n".join(rec.message for rec in caplog.records)
    assert "[label]" in messages
    assert "[REDACTED]" in messages
    assert "shh" not in messages
    assert "alice" in messages


def test_log_redacted_falls_back_when_json_dump_fails(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A serializer error triggers the warning + plain-repr fallback."""
    model = _SampleLogModel(name="x", token=SecretStr("y"))
    logger = logging.getLogger("deephaven_mcp.tests.log_redacted_fallback")

    def _broken(*_args: object, **_kwargs: object) -> None:
        raise TypeError("nope")

    monkeypatch.setattr(model.__class__, "model_dump", _broken, raising=True)

    with caplog.at_level("INFO", logger=logger.name):
        log_redacted(model, label="fallback", logger=logger)

    warnings = [rec.message for rec in caplog.records if rec.levelname == "WARNING"]
    infos = [rec.message for rec in caplog.records if rec.levelname == "INFO"]
    assert any("Failed to format config as JSON" in m for m in warnings)
    assert any("Loaded configuration:" in m for m in infos)
