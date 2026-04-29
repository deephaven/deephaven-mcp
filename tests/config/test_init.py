"""Smoke tests for deephaven_mcp.config public re-export surface.

Verifies that all symbols declared in __all__ are importable from the package
and have the expected values. Logic is tested in test__base.py,
test__community.py, and test__enterprise.py.
"""


def test_config_env_var_importable():
    from deephaven_mcp.config import CONFIG_ENV_VAR

    assert CONFIG_ENV_VAR == "DH_MCP_CONFIG_FILE"


def test_config_manager_importable():
    import abc

    from deephaven_mcp.config import ConfigManager

    assert issubclass(ConfigManager, abc.ABC)


def test_community_server_config_manager_importable():
    from deephaven_mcp.config import CommunityServerConfigManager, ConfigManager

    assert issubclass(CommunityServerConfigManager, ConfigManager)


def test_enterprise_server_config_manager_importable():
    from deephaven_mcp.config import ConfigManager, EnterpriseServerConfigManager

    assert issubclass(EnterpriseServerConfigManager, ConfigManager)


def test_validate_enterprise_config_importable():
    from deephaven_mcp.config import validate_enterprise_config

    assert callable(validate_enterprise_config)


def test_validate_community_session_config_importable():
    from deephaven_mcp.config import validate_community_session_config

    assert callable(validate_community_session_config)


def test_redact_community_session_config_importable():
    from deephaven_mcp.config import redact_community_session_config

    assert callable(redact_community_session_config)


def test_redact_enterprise_config_importable():
    from deephaven_mcp.config import redact_enterprise_config

    assert callable(redact_enterprise_config)


def test_default_connection_timeout_importable():
    from deephaven_mcp.config import DEFAULT_CONNECTION_TIMEOUT_SECONDS

    assert DEFAULT_CONNECTION_TIMEOUT_SECONDS == 10.0


def test_default_mcp_session_idle_timeout_importable():
    from deephaven_mcp.config import DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS

    assert DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS == 3600.0


def test_exception_types_importable():
    from deephaven_mcp._exceptions import ConfigurationError as _CanonicalConfigError
    from deephaven_mcp._exceptions import McpError
    from deephaven_mcp.config import ConfigurationError

    # The public re-export must be the same class object as the canonical one
    # in deephaven_mcp._exceptions, not a shadowed alias.
    assert ConfigurationError is _CanonicalConfigError
    assert issubclass(ConfigurationError, McpError)
    assert issubclass(ConfigurationError, Exception)


def test_all_surface_importable():
    """All symbols in __all__ can be imported."""
    import deephaven_mcp.config as cfg_module

    for name in cfg_module.__all__:
        assert hasattr(cfg_module, name), f"{name!r} declared in __all__ but not found"
