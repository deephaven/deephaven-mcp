import importlib
import types

def test_version_module_attributes():
    version_mod = importlib.import_module("deephaven_mcp._version")
    assert hasattr(version_mod, "__version__")
    assert hasattr(version_mod, "__version_tuple__")
    assert hasattr(version_mod, "version")
    assert hasattr(version_mod, "version_tuple")
    assert isinstance(version_mod.__version__, str)
    assert isinstance(version_mod.version, str)
    assert isinstance(version_mod.__version_tuple__, tuple)
    assert isinstance(version_mod.version_tuple, tuple)
    # Check the default values
    assert version_mod.__version__ == "0.0.0"
    assert version_mod.version == "0.0.0"
    assert version_mod.__version_tuple__ == (0, 0, 0)
    assert version_mod.version_tuple == (0, 0, 0)
