"""Tests for :mod:`deephaven_mcp.config.schema._pq_config`."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from deephaven_mcp.config.schema._pq_config import PqToolsConfig


def test_default_values():
    cfg = PqToolsConfig()
    assert cfg.default_max_concurrent == 20


def test_validate_full_block():
    cfg = PqToolsConfig.model_validate({"default_max_concurrent": 5})
    assert cfg.default_max_concurrent == 5


def test_rejects_zero_for_positive_fields():
    with pytest.raises(ValidationError):
        PqToolsConfig.model_validate({"default_max_concurrent": 0})


def test_rejects_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs"):
        PqToolsConfig.model_validate({"bogus": 1})
