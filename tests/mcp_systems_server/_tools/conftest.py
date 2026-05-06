"""Shared test fixtures and helpers for mcp_systems_server tests."""

from unittest.mock import AsyncMock, MagicMock

from deephaven_mcp.auth.credentials import PasswordCredentials
from deephaven_mcp.auth.middleware import SCOPE_KEY_CREDENTIALS

# Default per-request credentials injected into every MockRequest.scope so
# that helpers like get_enterprise_registry, which read
# scope[SCOPE_KEY_CREDENTIALS], succeed in tests without each test having to
# stage its own creds. Tests that need a different credential type (e.g.
# PrivateKeyCredentials) can pass `creds=...` to MockContext.
_DEFAULT_TEST_CREDENTIALS = PasswordCredentials(
    username="test-user", password="test-pw"
)


class MockRequest:
    """Mock Starlette request with MCP session headers and ASGI scope."""

    def __init__(
        self,
        mcp_session_id: str = "test-mcp-session-id",
        creds: object = _DEFAULT_TEST_CREDENTIALS,
    ):
        self.headers = {"mcp-session-id": mcp_session_id}
        # Mirror the keys the real AuthenticationMiddleware writes into the
        # ASGI scope. ``creds`` is intentionally typed as ``object`` so tests
        # can also exercise the "wrong creds type" rejection path.
        self.scope: dict[str, object] = {SCOPE_KEY_CREDENTIALS: creds}


class MockRequestContext:
    """Mock MCP request context for testing."""

    def __init__(
        self,
        lifespan_context,
        mcp_session_id: str = "test-mcp-session-id",
        creds: object = _DEFAULT_TEST_CREDENTIALS,
    ):
        self.lifespan_context = lifespan_context
        self.request = MockRequest(mcp_session_id, creds)


class MockContext:
    """Mock MCP context for testing."""

    def __init__(
        self,
        lifespan_context,
        mcp_session_id: str = "test-mcp-session-id",
        creds: object = _DEFAULT_TEST_CREDENTIALS,
    ):
        self.request_context = MockRequestContext(
            lifespan_context, mcp_session_id, creds
        )


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
