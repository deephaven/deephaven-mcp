"""Smoke tests for deephaven_mcp.config public re-export surface.

Verifies that all symbols declared in __all__ are importable from the package.
Logic is tested in test__base.py, test__community.py, and test__enterprise.py.
"""

import pytest


def test_config_env_var_importable():
    from deephaven_mcp.config import CONFIG_ENV_VAR
    assert isinstance(CONFIG_ENV_VAR, str)


def test_config_manager_importable():
    from deephaven_mcp.config import ConfigManager
    import abc
    assert issubclass(ConfigManager, abc.ABC)


def test_community_server_config_manager_importable():
    from deephaven_mcp.config import CommunityServerConfigManager, ConfigManager
    assert issubclass(CommunityServerConfigManager, ConfigManager)


def test_enterprise_server_config_manager_importable():
    from deephaven_mcp.config import EnterpriseServerConfigManager, ConfigManager
    assert issubclass(EnterpriseServerConfigManager, ConfigManager)


def test_validate_enterprise_config_importable():
    from deephaven_mcp.config import validate_enterprise_config
    assert callable(validate_enterprise_config)


def test_validate_single_community_session_config_importable():
    from deephaven_mcp.config import validate_single_community_session_config
    assert callable(validate_single_community_session_config)


def test_redact_community_session_config_importable():
    from deephaven_mcp.config import redact_community_session_config
    assert callable(redact_community_session_config)


def test_redact_enterprise_system_config_importable():
    from deephaven_mcp.config import redact_enterprise_system_config
    assert callable(redact_enterprise_system_config)


def test_default_connection_timeout_importable():
    from deephaven_mcp.config import DEFAULT_CONNECTION_TIMEOUT_SECONDS
    assert isinstance(DEFAULT_CONNECTION_TIMEOUT_SECONDS, float)


def test_exception_types_importable():
    from deephaven_mcp.config import (
        CommunitySessionConfigurationError,
        ConfigurationError,
        EnterpriseSystemConfigurationError,
    )
    assert issubclass(CommunitySessionConfigurationError, Exception)
    assert issubclass(ConfigurationError, Exception)
    assert issubclass(EnterpriseSystemConfigurationError, Exception)


def test_all_surface_importable():
    """All symbols in __all__ can be imported."""
    import deephaven_mcp.config as cfg_module
    for name in cfg_module.__all__:
        assert hasattr(cfg_module, name), f"{name!r} declared in __all__ but not found"
