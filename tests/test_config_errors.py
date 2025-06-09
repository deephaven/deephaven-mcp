import pytest

from deephaven_mcp.config import errors


def test_mcp_configuration_error_is_exception():
    with pytest.raises(errors.McpConfigurationError) as exc_info:
        raise errors.McpConfigurationError("base error")
    assert str(exc_info.value) == "base error"


def test_community_session_configuration_error_is_subclass():
    assert issubclass(
        errors.CommunitySessionConfigurationError, errors.McpConfigurationError
    )
    with pytest.raises(errors.CommunitySessionConfigurationError) as exc_info:
        raise errors.CommunitySessionConfigurationError("community error")
    assert str(exc_info.value) == "community error"


def test_enterprise_system_configuration_error_is_subclass():
    assert issubclass(
        errors.EnterpriseSystemConfigurationError, errors.McpConfigurationError
    )
    with pytest.raises(errors.EnterpriseSystemConfigurationError) as exc_info:
        raise errors.EnterpriseSystemConfigurationError("enterprise error")
    assert str(exc_info.value) == "enterprise error"


def test_errors_all_exports():
    exported = set(errors.__all__)
    expected = {
        "McpConfigurationError",
        "CommunitySessionConfigurationError",
        "EnterpriseSystemConfigurationError",
    }
    assert exported == expected
