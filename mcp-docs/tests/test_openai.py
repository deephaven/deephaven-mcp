import types

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
            choices = [types.SimpleNamespace(message=types.SimpleNamespace(content=self.parent.response_content))]
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
        client=DummyAsyncOpenAI()
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
        client=dummy
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
        client=dummy
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
        client=dummy
    )
    with pytest.raises(OpenAIClientError):
        async for _ in client.stream_chat("fail"):  # type: ignore
            pass

def test_build_messages_and_validate_history():
    client = OpenAIClient(
        api_key="test-key",
        base_url="https://api.test.com/v1",
        model="gpt-test",
        client=DummyAsyncOpenAI()
    )
    prompt = "What's up?"
    history = [{"role": "user", "content": "Hi"}]
    # No system prompt
    messages = client._build_messages(prompt, history)
    assert messages[-1]["content"] == prompt
    assert messages[0]["role"] == "user"
    # Should insert system prompt if set
    client.system_prompt = "You are a bot."
    messages2 = client._build_messages(prompt, history)
    assert messages2[0]["role"] == "system"
    # Validation raises on bad history
    with pytest.raises(OpenAIClientError):
        client._validate_history([{"role": 123, "content": "hi"}])
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
        client._validate_history([{ "role": "user", "content": 123 }])


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

import asyncio


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
    client = OpenAIClient(api_key="x", base_url="y", model="z", client=NonOpenAIErrorDummy())
    # chat: should raise OpenAIClientError due to caught Exception
    with pytest.raises(OpenAIClientError):
        await client.chat("test")
    # stream_chat: should raise OpenAIClientError due to caught Exception
    with pytest.raises(OpenAIClientError):
        async for _ in client.stream_chat("test"):
            pass
