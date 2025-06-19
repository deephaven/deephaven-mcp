"""
Tests for deephaven_mcp.__init__ (package docstring, version, logging).
"""

import logging

def test_imports_and_version():
    import deephaven_mcp as mod
    # __version__ should be present and a string
    assert hasattr(mod, "__version__")
    assert isinstance(mod.__version__, str)
    # __all__ should contain __version__
    assert "__version__" in mod.__all__


def test_logger_null_handler():
    import deephaven_mcp as mod
    logger = getattr(mod, "_LOGGER", None)
    assert logger is not None
    # Should have at least one NullHandler
    assert any(isinstance(h, type(logging.NullHandler())) for h in logger.handlers)
