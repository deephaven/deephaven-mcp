"""Shared test fixtures and helpers for mcp_systems_server tests."""

from unittest.mock import AsyncMock, MagicMock


class MockRequestContext:
    """Mock MCP request context for testing."""

    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context


class MockContext:
    """Mock MCP context for testing."""

    def __init__(self, lifespan_context):
        self.request_context = MockRequestContext(lifespan_context)


def create_mock_instance_tracker():
    """Create a mock InstanceTracker for tests."""
    mock_tracker = MagicMock()
    mock_tracker.instance_id = "test-instance-id"
    mock_tracker.track_python_process = AsyncMock()
    mock_tracker.untrack_python_process = AsyncMock()
    return mock_tracker
