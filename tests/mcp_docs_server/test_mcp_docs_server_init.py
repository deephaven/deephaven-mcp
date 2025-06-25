import importlib
import logging
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest


def test_module_exports(monkeypatch):
    monkeypatch.setenv("INKEEP_API_KEY", "dummy-key")
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    mod = importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
    assert hasattr(mod, "mcp_server")
    assert hasattr(mod, "__all__")
    assert "mcp_server" in mod.__all__


def test_import_docs_init():
    sys.modules.pop("deephaven_mcp.mcp_docs_server.__init__", None)
    importlib.import_module("deephaven_mcp.mcp_docs_server.__init__")
