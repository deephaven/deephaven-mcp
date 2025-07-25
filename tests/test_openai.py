import asyncio
import types
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import openai
import pytest

from deephaven_mcp.openai import OpenAIClient, OpenAIClientError


class DummyOpenAIError(openai.OpenAIError):
    pass


class DummyCompletions:
    def __init__(self, parent):
        self.parent = parent

    async def create(self, **kwargs):
        if kwargs.get("stream"):
            # Return async generator for streaming
            async def stream():
                if self.parent.should_fail:
                    raise DummyOpenAIError("Simulated OpenAI error")
                for chunk in self.parent.stream_content:

                    class DummyChoice:
                        def __init__(self, content):
                            self.delta = types.SimpleNamespace(content=content)

                    yield types.SimpleNamespace(choices=[DummyChoice(chunk)])

            return stream()
        # Non-streaming
        if self.parent.should_fail:
            raise DummyOpenAIError("Simulated OpenAI error")

        class DummyResponse:
            choices = [
                types.SimpleNamespace(
                    message=types.SimpleNamespace(content=self.parent.response_content)
                )
            ]

        return DummyResponse()


class DummyChat:
    def __init__(self, parent):
        self.completions = DummyCompletions(parent)


class DummyAsyncOpenAI:
    def __init__(self):
        self.should_fail = False
        self.response_content = "Hello, world!"
        self.stream_content = ["Hello,", " world!"]
        self.chat = DummyChat(self)


@pytest.mark.asyncio
async def test_chat_success():
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com/v1",
        model="gpt-test",
        client=DummyAsyncOpenAI(),
    )
    result = await client.chat("hello", history=[{"role": "user", "content": "hi"}])
    assert result == "Hello, world!"


@pytest.mark.asyncio
async def test_chat_failure():
    dummy = DummyAsyncOpenAI()
    dummy.should_fail = True
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com/v1",
        model="gpt-test",
        client=dummy,
    )
    with pytest.raises(OpenAIClientError):
        await client.chat("fail")


@pytest.mark.asyncio
async def test_stream_chat_success():
    dummy = DummyAsyncOpenAI()
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com/v1",
        model="gpt-test",
        client=dummy,
    )
    result = []
    async for token in client.stream_chat("hello"):  # type: ignore
        result.append(token)
    assert result == ["Hello,", " world!"]


@pytest.mark.asyncio
async def test_stream_chat_failure():
    dummy = DummyAsyncOpenAI()
    dummy.should_fail = True
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com/v1",
        model="gpt-test",
        client=dummy,
    )
    with pytest.raises(OpenAIClientError):
        async for _ in client.stream_chat("fail"):  # type: ignore
            pass


def test_build_messages_and_validate_history():
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com/v1",
        model="gpt-test",
        client=DummyAsyncOpenAI(),
    )
    prompt = "What's up?"
    history = [{"role": "user", "content": "Hi"}]
    # No system prompt
    messages = client._build_messages(prompt, history)
    assert messages[-1]["content"] == prompt
    assert messages[0]["role"] == "user"
    # Should insert system prompt if provided
    sys_prompts = ["You are a bot.", "Be concise."]
    messages2 = client._build_messages(prompt, history, sys_prompts)
    assert messages2[0]["role"] == "system"
    assert messages2[0]["content"] == "You are a bot."
    assert messages2[1]["role"] == "system"
    assert messages2[1]["content"] == "Be concise."
    assert messages2[2]["role"] == "user"
    assert messages2[2]["content"] == "Hi"
    assert messages2[-1]["role"] == "user"
    assert messages2[-1]["content"] == prompt
    # Invalid history
    with pytest.raises(OpenAIClientError):
        client._validate_history([{"role": "user"}])
    with pytest.raises(OpenAIClientError):
        client._validate_history([123])
    # Accepts None and empty history
    client._validate_history(None)
    client._validate_history([])
    # Non-sequence
    with pytest.raises(OpenAIClientError):
        client._validate_history("notalist")
    # Non-dict in sequence
    with pytest.raises(OpenAIClientError):
        client._validate_history(["notadict"])
    # Non-string values
    with pytest.raises(OpenAIClientError):
        client._validate_history([{"role": "user", "content": 123}])


def test_validate_system_prompts():
    client = OpenAIClient(api_key="x", base_url="y", model="z")
    # Accepts None
    client._validate_system_prompts(None)
    # Accepts empty list
    client._validate_system_prompts([])
    # Accepts list of strings
    client._validate_system_prompts(["a", "b"])
    # Raises on non-sequence
    with pytest.raises(OpenAIClientError):
        client._validate_system_prompts("notalist")
    with pytest.raises(OpenAIClientError):
        client._validate_system_prompts(123)
    # Raises on non-string in sequence
    with pytest.raises(OpenAIClientError):
        client._validate_system_prompts([123])
    with pytest.raises(OpenAIClientError):
        client._validate_system_prompts(["ok", None])


@pytest.mark.asyncio
async def test_chat_invalid_response_structure(monkeypatch):
    class BadResponse:
        pass

    dummy = DummyAsyncOpenAI()

    async def bad_create(**kwargs):
        return BadResponse()

    dummy.chat.completions.create = bad_create
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=dummy)
    with pytest.raises(
        OpenAIClientError, match="Unexpected response structure from OpenAI API"
    ):
        await client.chat("prompt")


@pytest.mark.asyncio
async def test_chat_null_content(monkeypatch):
    class NullContent:
        choices = [types.SimpleNamespace(message=types.SimpleNamespace(content=None))]

    dummy = DummyAsyncOpenAI()

    async def null_content_create(**kwargs):
        return NullContent()

    dummy.chat.completions.create = null_content_create
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=dummy)
    with pytest.raises(
        OpenAIClientError, match="OpenAI API returned a null content message"
    ):
        await client.chat("prompt")


@pytest.mark.asyncio
async def test_chat_openaierror(monkeypatch):
    dummy = DummyAsyncOpenAI()

    async def fail(**kwargs):
        raise openai.OpenAIError("fail")

    dummy.chat.completions.create = fail
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=dummy)
    with pytest.raises(OpenAIClientError, match="OpenAI API call failed"):
        await client.chat("prompt")


@pytest.mark.asyncio
async def test_chat_generic_exception(monkeypatch):
    dummy = DummyAsyncOpenAI()

    async def fail(**kwargs):
        raise RuntimeError("fail")

    dummy.chat.completions.create = fail
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=dummy)
    with pytest.raises(OpenAIClientError, match="Unexpected error"):
        await client.chat("prompt")


@pytest.mark.asyncio
async def test_stream_chat_openaierror(monkeypatch):
    dummy = DummyAsyncOpenAI()

    async def fail(**kwargs):
        raise openai.OpenAIError("fail")

    dummy.chat.completions.create = fail
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=dummy)
    with pytest.raises(OpenAIClientError, match="OpenAI API streaming call failed"):
        async for _ in client.stream_chat("prompt"):
            pass


@pytest.mark.asyncio
async def test_stream_chat_generic_exception(monkeypatch):
    dummy = DummyAsyncOpenAI()

    async def fail(**kwargs):
        raise RuntimeError("fail")

    dummy.chat.completions.create = fail
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=dummy)
    with pytest.raises(OpenAIClientError, match="Unexpected error"):
        async for _ in client.stream_chat("prompt"):
            pass


@pytest.mark.asyncio
async def test_stream_chat_not_async_iterable():
    class NotAsyncIterable:
        pass

    dummy = DummyAsyncOpenAI()

    async def not_async_iterable_create(**kwargs):
        return NotAsyncIterable()

    dummy.chat.completions.create = not_async_iterable_create
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=dummy)
    with pytest.raises(
        OpenAIClientError, match="did not return an async iterable for streaming chat"
    ):
        async for _ in client.stream_chat("prompt"):
            pass


def test_openai_client_constructor_validation():
    with pytest.raises(OpenAIClientError):
        OpenAIClient(api_key=None, base_url="x", model="y")
    with pytest.raises(OpenAIClientError):
        OpenAIClient(api_key="x", base_url=None, model="y")
    with pytest.raises(OpenAIClientError):
        OpenAIClient(api_key="x", base_url="y", model=None)
    with pytest.raises(OpenAIClientError):
        OpenAIClient(api_key=123, base_url="y", model="z")
    with pytest.raises(OpenAIClientError):
        OpenAIClient(api_key="x", base_url=123, model="z")
    with pytest.raises(OpenAIClientError):
        OpenAIClient(api_key="x", base_url="y", model=123)


@patch("deephaven_mcp.openai.httpx.AsyncClient")
@patch("deephaven_mcp.openai.openai.AsyncOpenAI")
def test_openai_client_constructor_with_advanced_config(mock_openai, mock_httpx):
    """Test that constructor properly configures HTTP client with advanced settings."""
    mock_http_client = MagicMock()
    mock_httpx.return_value = mock_http_client
    mock_openai_client = MagicMock()
    mock_openai.return_value = mock_openai_client

    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com",
        model="test-model",
        timeout=30.0,
        max_retries=5,
        max_connections=20,
        max_keepalive_connections=10,
        connect_timeout=15.0,
        write_timeout=20.0,
        pool_timeout=8.0,
    )

    # Verify httpx.AsyncClient was called with correct limits and timeout
    mock_httpx.assert_called_once_with(
        limits=httpx.Limits(
            max_connections=20,
            max_keepalive_connections=10,
        ),
        timeout=httpx.Timeout(connect=15.0, read=30.0, write=20.0, pool=8.0),
    )

    # Verify OpenAI client was configured correctly
    mock_openai.assert_called_once_with(
        api_key="test-key",
        base_url="https://api.test.com",
        timeout=30.0,
        max_retries=5,
        http_client=mock_http_client,
    )

    # Verify client ownership tracking
    assert client._client_owned is True
    assert client.client == mock_openai_client


def test_openai_client_constructor_with_injected_client():
    """Test that constructor properly handles injected client for testing."""
    injected_client = MagicMock()

    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com",
        model="test-model",
        client=injected_client,
    )

    # Verify injected client is used and not owned
    assert client.client == injected_client
    assert client._client_owned is False


@pytest.mark.asyncio
async def test_close_method_owned_client():
    """Test close method properly closes owned HTTP client."""
    mock_client = AsyncMock()

    with (
        patch("deephaven_mcp.openai.httpx.AsyncClient"),
        patch("deephaven_mcp.openai.openai.AsyncOpenAI", return_value=mock_client),
    ):

        client = OpenAIClient(
            api_key="test-key", base_url="https://api.test.com", model="test-model"
        )

        # Verify client is owned
        assert client._client_owned is True

        # Call close method
        await client.close()

        # Verify close was called on the OpenAI client
        mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_close_method_injected_client():
    """Test close method does not close injected client."""
    mock_client = AsyncMock()

    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com",
        model="test-model",
        client=mock_client,
    )

    # Verify client is not owned
    assert client._client_owned is False

    # Call close method
    await client.close()

    # Verify close was NOT called on the injected client
    mock_client.close.assert_not_called()


@pytest.mark.asyncio
async def test_close_method_handles_exceptions():
    """Test close method handles exceptions gracefully."""
    mock_client = AsyncMock()
    mock_client.close.side_effect = Exception("Close failed")

    with (
        patch("deephaven_mcp.openai.httpx.AsyncClient"),
        patch("deephaven_mcp.openai.openai.AsyncOpenAI", return_value=mock_client),
        patch("deephaven_mcp.openai._LOGGER") as mock_logger,
    ):

        client = OpenAIClient(
            api_key="test-key", base_url="https://api.test.com", model="test-model"
        )

        # Call close method - should not raise exception
        await client.close()

        # Verify warning was logged
        mock_logger.warning.assert_called_once_with(
            "[OpenAIClient.close] Error closing HTTP client: Close failed"
        )


@pytest.mark.asyncio
async def test_chat_malformed_response():
    class MalformedDummy:
        async def chat_create(self, **kwargs):
            # Missing choices
            class DummyResponse:
                pass

            return DummyResponse()

        @property
        def chat(self):
            parent = self

            class Chat:
                @property
                def completions(self_inner):
                    class Completions:
                        def __init__(self, parent):
                            self._parent = parent

                        async def create(self, **kwargs):
                            return await self._parent.chat_create(**kwargs)

                    return Completions(parent)

            return Chat()

    client = OpenAIClient(api_key="x", base_url="y", model="z", client=MalformedDummy())
    with pytest.raises(OpenAIClientError):
        await client.chat("test")


@pytest.mark.asyncio
async def test_stream_chat_no_content():
    class NoContentDummy:
        async def chat_create(self, **kwargs):
            async def stream():
                class DummyChoice:
                    def __init__(self):
                        self.delta = type("Delta", (), {"content": None})

                for _ in range(2):
                    yield type("Chunk", (), {"choices": [DummyChoice()]})

            return stream()

        @property
        def chat(self):
            parent = self

            class Chat:
                @property
                def completions(self_inner):
                    class Completions:
                        def __init__(self, parent):
                            self._parent = parent

                        async def create(self, **kwargs):
                            return await self._parent.chat_create(**kwargs)

                    return Completions(parent)

            return Chat()

    client = OpenAIClient(api_key="x", base_url="y", model="z", client=NoContentDummy())
    # Should not yield any content, but should not raise
    tokens = [token async for token in client.stream_chat("test")]
    assert tokens == []


@pytest.mark.asyncio
async def test_chat_and_stream_chat_wraps_non_openai_error():
    class NonOpenAIErrorDummy:
        async def chat_create(self, **kwargs):
            raise DummyOpenAIError("unexpected")

        @property
        def chat(self):
            parent = self

            class Chat:
                @property
                def completions(self_inner):
                    class Completions:
                        def __init__(self, parent):
                            self._parent = parent

                        async def create(self, **kwargs):
                            return await self._parent.chat_create(**kwargs)

                    return Completions(parent)

            return Chat()

    client = OpenAIClient(
        api_key="x", base_url="y", model="z", client=NonOpenAIErrorDummy()
    )
    # chat: should raise OpenAIClientError due to caught Exception
    with pytest.raises(OpenAIClientError):
        await client.chat("test")
    # stream_chat: should raise OpenAIClientError due to caught Exception
    with pytest.raises(OpenAIClientError):
        async for _ in client.stream_chat("test"):
            pass


@pytest.mark.asyncio
async def test_async_context_manager_success():
    """Test OpenAIClient works as async context manager with successful operation."""
    # Use a dummy client to avoid real HTTP calls
    dummy_client = DummyAsyncOpenAI()
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.openai.com/v1",
        model="gpt-3.5-turbo",
        client=dummy_client
    )
    
    # Test that async context manager works correctly
    async with client as context_client:
        # Should return the same client instance (covers line 429: return self)
        assert context_client is client
        
        # Should be able to use the client normally
        response = await client.chat("Test prompt")
        assert response == "Hello, world!"
    
    # After exiting context, __aexit__ was called (covers line 444: await self.close())


@pytest.mark.asyncio
async def test_async_context_manager_with_exception():
    """Test OpenAIClient async context manager properly handles exceptions and still closes."""
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.openai.com/v1",
        model="gpt-3.5-turbo"
    )
    
    # Test that context manager still calls close() even when exception occurs
    with patch.object(client, 'close', new_callable=AsyncMock) as mock_close:
        try:
            async with client as context_client:
                assert context_client is client  # Covers line 429
                # Raise an exception inside the context
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected exception
        
        # Verify close was called even though exception occurred (covers line 444)
        mock_close.assert_called_once()


@pytest.mark.asyncio
async def test_async_context_manager_close_exception():
    """Test OpenAIClient async context manager handles close() exceptions gracefully."""
    # Use dummy client to avoid real HTTP calls
    dummy_client = DummyAsyncOpenAI()
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.openai.com/v1",
        model="gpt-3.5-turbo",
        client=dummy_client
    )
    
    # Test that exceptions in close() are handled gracefully in __aexit__
    # We need to patch the actual close method to raise an exception
    original_close = client.close
    
    async def failing_close():
        raise Exception("Close failed")
    
    client.close = failing_close
    
    # Should not raise exception even if close() fails
    try:
        async with client as context_client:
            assert context_client is client  # Covers line 429
        # If we get here, __aexit__ handled the exception properly (covers line 444)
    except Exception as e:
        # The __aexit__ method should handle close() exceptions gracefully
        # If we get an exception here, the test should fail
        pytest.fail(f"__aexit__ should handle close() exceptions gracefully, but got: {e}")
    
    # Restore original close method
    client.close = original_close
