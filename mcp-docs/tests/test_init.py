import importlib
import logging


def test_init_module_attributes():
    mod = importlib.import_module("deephaven_mcp")
    # __version__ should exist and be a string
    assert hasattr(mod, "__version__")
    assert isinstance(mod.__version__, str)
    # __all__ should exist and include __version__
    assert hasattr(mod, "__all__")
    assert "__version__" in mod.__all__


def test_logger_is_null_handler():
    mod = importlib.import_module("deephaven_mcp")
    logger = logging.getLogger(mod.__name__)
    # There should be at least one NullHandler attached
    assert any(isinstance(h, logging.NullHandler) for h in logger.handlers)
