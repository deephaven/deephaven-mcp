"""Custom types for Deephaven MCP configuration errors."""


class McpConfigurationError(Exception):
    """Base class for all Deephaven MCP configuration errors."""

    pass


class CommunitySessionConfigurationError(McpConfigurationError):
    """Raised when a community session's configuration cannot be retrieved or is invalid."""

    pass


class EnterpriseSystemConfigurationError(McpConfigurationError):
    """Custom exception for errors in enterprise system configuration."""

    pass


__all__ = [
    "McpConfigurationError",
    "CommunitySessionConfigurationError",
    "EnterpriseSystemConfigurationError",
]
