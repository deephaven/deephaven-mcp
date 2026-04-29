"""Shared test fixtures and helpers for mcp_systems_server tests."""

from unittest.mock import AsyncMock, MagicMock


class MockRequest:
    """Mock Starlette request with MCP session headers."""

    def __init__(self, mcp_session_id: str = "test-mcp-session-id"):
        self.headers = {"mcp-session-id": mcp_session_id}


class MockRequestContext:
    """Mock MCP request context for testing."""

    def __init__(self, lifespan_context, mcp_session_id: str = "test-mcp-session-id"):
        self.lifespan_context = lifespan_context
        self.request = MockRequest(mcp_session_id)


class MockContext:
    """Mock MCP context for testing."""

    def __init__(self, lifespan_context, mcp_session_id: str = "test-mcp-session-id"):
        self.request_context = MockRequestContext(lifespan_context, mcp_session_id)


def create_mock_session_registry_manager(registry=None):
    """Create a mock SessionRegistryManager that returns the given registry from get_or_create_registry."""
    mock = AsyncMock()
    mock.get_or_create_registry = AsyncMock(return_value=registry)
    mock.close_session = AsyncMock()
    mock.start = AsyncMock()
    mock.stop = AsyncMock()
    return mock


def create_mock_instance_tracker():
    """Create a mock InstanceTracker for tests."""
    mock_tracker = MagicMock()
    mock_tracker.instance_id = "test-instance-id"
    mock_tracker.track_python_process = AsyncMock()
    mock_tracker.untrack_python_process = AsyncMock()
    return mock_tracker
