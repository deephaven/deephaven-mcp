"""Tests for :mod:`deephaven_mcp.config.schema._response_limits`."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from deephaven_mcp.config.schema._response_limits import ResponseLimits


def test_default_values():
    cfg = ResponseLimits()
    assert cfg.max_response_bytes == 50 * 1024 * 1024
    assert cfg.warning_response_bytes == 5 * 1024 * 1024
    assert cfg.estimated_bytes_per_cell == 50


def test_validate_full_block():
    cfg = ResponseLimits.model_validate(
        {
            "max_response_bytes": 1000,
            "warning_response_bytes": 500,
            "estimated_bytes_per_cell": 10,
        }
    )
    assert cfg.max_response_bytes == 1000
    assert cfg.warning_response_bytes == 500
    assert cfg.estimated_bytes_per_cell == 10


def test_rejects_zero_for_positive_fields():
    for field in (
        "max_response_bytes",
        "warning_response_bytes",
        "estimated_bytes_per_cell",
    ):
        with pytest.raises(ValidationError):
            ResponseLimits.model_validate({field: 0})


def test_rejects_negative_for_positive_fields():
    with pytest.raises(ValidationError):
        ResponseLimits.model_validate({"max_response_bytes": -1})


def test_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        ResponseLimits.model_validate({"bogus": 1})


def test_validator_rejects_warning_above_max():
    """``warning_response_bytes`` may not exceed ``max_response_bytes``."""
    with pytest.raises(ValidationError, match="warning_response_bytes"):
        ResponseLimits.model_validate(
            {
                "max_response_bytes": 100,
                "warning_response_bytes": 200,
            }
        )


def test_validator_accepts_warning_equal_to_max():
    """Equal values are permitted; only strict-greater is refused."""
    cfg = ResponseLimits.model_validate(
        {"max_response_bytes": 100, "warning_response_bytes": 100}
    )
    assert cfg.max_response_bytes == cfg.warning_response_bytes
