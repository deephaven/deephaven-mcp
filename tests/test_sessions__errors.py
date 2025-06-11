import pytest

from deephaven_mcp.sessions._errors import SessionCreationError


def test_session_creation_error_is_exception():
    with pytest.raises(SessionCreationError) as exc_info:
        raise SessionCreationError("session error")
    assert str(exc_info.value) == "session error"
