"""
openai.py - Generic OpenAI/LLM client utilities for deephaven_mcp

This module provides a generic OpenAIClient class for interacting with OpenAI-compatible LLM APIs.
"""

import time
import logging
import openai
from typing import Any
from collections.abc import Sequence, AsyncGenerator

_LOGGER = logging.getLogger(__name__)

class OpenAIClientError(Exception):
    """
    Custom exception for OpenAIClient errors.

    Raised when an OpenAI API call fails or when invalid parameters are provided.
    """
    pass

class OpenAIClient:
    """
    Asynchronous client for OpenAI-compatible chat APIs, supporting chat completion and streaming.

    This class wraps the OpenAI Python SDK (or compatible APIs) and provides methods to send chat
    completion requests with configurable parameters such as base URL, API key, model, and default
    system prompt. It supports dependency injection of the OpenAI async client for testability, and
    provides robust validation of chat message history.

    Attributes:
        api_key (str): The API key for authentication.
        base_url (str): The base URL of the OpenAI-compatible API endpoint.
        model (str): The model name to use for chat completions.
        system_prompt (str | None): Default system prompt to prepend to the conversation (optional).
        client (openai.AsyncOpenAI): The underlying OpenAI async client instance. Can be injected for testing.

    Example:
        >>> import openai
        >>> client = OpenAIClient(api_key="sk-...", base_url="https://api.openai.com/v1", model="gpt-3.5-turbo")
        >>> response = await client.chat("Hello, who are you?", max_tokens=100, temperature=0.7)
        >>> print(response)
        I am an AI language model developed by OpenAI...
    """
    def __init__(self, api_key: str, base_url: str, model: str, system_prompt: str | None = None, client: openai.AsyncOpenAI | None = None):
        """
        Initialize an OpenAIClient instance.

        Args:
            api_key (str): API key for authentication with the OpenAI-compatible API.
            base_url (str): Base URL for the OpenAI-compatible API endpoint.
            model (str): Model name to use for chat completions (e.g., 'gpt-3.5-turbo').
            system_prompt (str | None, optional): Default system prompt to prepend to the conversation. If None, no system prompt is used.
            client (openai.AsyncOpenAI | None, optional): Optionally inject a custom OpenAI async client (for testing or advanced usage).

        Raises:
            OpenAIClientError: If any required parameter is missing or invalid.

        Example:
            >>> client = OpenAIClient(api_key="sk-...", base_url="https://api.openai.com/v1", model="gpt-3.5-turbo")
        """
        if not api_key or not isinstance(api_key, str):
            raise OpenAIClientError("api_key must be a non-empty string.")
        if not base_url or not isinstance(base_url, str):
            raise OpenAIClientError("base_url must be a non-empty string.")
        if not model or not isinstance(model, str):
            raise OpenAIClientError("model must be a non-empty string.")
        self.api_key: str = api_key
        self.base_url: str = base_url
        self.model: str = model
        self.system_prompt: str | None = system_prompt
        self.client: openai.AsyncOpenAI = client or openai.AsyncOpenAI(api_key=api_key, base_url=base_url)

    def _validate_history(self, history: Sequence[dict[str, str]] | None) -> None:
        """
        Validate that the chat history is a sequence of dicts with 'role' and 'content' string keys.

        Args:
            history (Sequence[dict[str, str]] | None): The chat history to validate. Each entry must be a dict with string 'role' and 'content'.

        Raises:
            OpenAIClientError: If history is not a sequence of dicts, or if any entry is missing required keys or has non-string values.

        Example:
            >>> client._validate_history([
            ...     {"role": "user", "content": "Hi"},
            ...     {"role": "assistant", "content": "Hello!"}
            ... ])
        """
        if history is not None:
            if not isinstance(history, (list, tuple)):
                raise OpenAIClientError("history must be a sequence (list or tuple) of dicts")
            for msg in history:
                if not isinstance(msg, dict):
                    raise OpenAIClientError("Each message in history must be a dict")
                if 'role' not in msg or 'content' not in msg:
                    raise OpenAIClientError("Each message in history must have 'role' and 'content' keys")
                if not isinstance(msg['role'], str) or not isinstance(msg['content'], str):
                    raise OpenAIClientError("'role' and 'content' in each message must be strings")

    def _build_messages(self, prompt: str, history: Sequence[dict[str, str]] | None) -> list[dict[str, str]]:
        """
        Construct the messages list for OpenAI chat completion requests, including system prompt, history, and user prompt.

        Args:
            prompt (str): The latest user message to append to the conversation.
            history (Sequence[dict[str, str]] | None): Previous chat messages for context. Each must be a dict with 'role' and 'content'.

        Returns:
            list[dict[str, str]]: The formatted list of messages for the OpenAI API.

        Example:
            >>> client._build_messages("What's the weather?", [{"role": "user", "content": "Hi"}])
            [
                {"role": "system", "content": "..."},
                {"role": "user", "content": "Hi"},
                {"role": "user", "content": "What's the weather?"}
            ]
        """
        messages = []
        if history:
            messages.extend(history)
        if self.system_prompt:
            messages.insert(0, {"role": "system", "content": self.system_prompt})
        messages.append({"role": "user", "content": prompt})
        return messages

    async def chat(
        self,
        prompt: str,
        history: Sequence[dict[str, str]] | None = None,
        **kwargs: Any
    ) -> str:
        """
        Asynchronously send a chat completion request to the OpenAI API and return the assistant's response.

        This method validates the chat history, constructs the message list (including system prompt and user prompt), and sends a chat completion request using the injected or default OpenAI async client. It logs request and response details, and raises a custom error on failure.

        Args:
            prompt (str): The user's question or message to send to the assistant.
            history (Sequence[dict[str, str]] | None, optional): Previous chat messages for context. Each must be a dict with 'role' and 'content'.
            **kwargs: Additional keyword arguments to pass to the OpenAI API (e.g., max_tokens, temperature, stop, presence_penalty, frequency_penalty, etc.).

        Returns:
            str: The assistant's response message content (stripped of leading/trailing whitespace).

        Raises:
            OpenAIClientError: If the API call fails, returns an error, or if parameters are invalid.

        Example:
            >>> await client.chat("Hello, who are you?", max_tokens=100, temperature=0.7, stop=["\n"])
            'I am an AI language model developed by OpenAI...'
        """
        self._validate_history(history)
        messages = self._build_messages(prompt, history)
        try:
            _LOGGER.info(f"[OpenAIClient.chat] Sending chat completion request | model={self.model}, base_url={self.base_url}, prompt_len={len(prompt)}, history_len={len(history) if history else 0}")
            start_time = time.monotonic()
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                **kwargs
            )
            elapsed = time.monotonic() - start_time
            request_id = getattr(response, 'id', None)
            # Validate response structure
            if not hasattr(response, 'choices') or not response.choices or not hasattr(response.choices[0], 'message') or not hasattr(response.choices[0].message, 'content'):
                _LOGGER.error(f"[OpenAIClient.chat] Unexpected response structure: {response}")
                raise OpenAIClientError("Unexpected response structure from OpenAI API")
            _LOGGER.info(f"[OpenAIClient.chat] Chat completion succeeded | request_id={request_id} | elapsed={elapsed:.3f}s")
            return response.choices[0].message.content.strip()
        except openai.OpenAIError as e:
            _LOGGER.error(f"[OpenAIClient.chat] OpenAI API call failed: {e}", exc_info=True)
            raise OpenAIClientError(f"OpenAI API call failed: {e}") from e
        except Exception as e:
            _LOGGER.error(f"[OpenAIClient.chat] Unexpected error: {e}", exc_info=True)
            raise OpenAIClientError(f"Unexpected error: {e}") from e

    async def stream_chat(
        self,
        prompt: str,
        history: Sequence[dict[str, str]] | None = None,
        **kwargs: Any
    ) -> AsyncGenerator[str, None]:
        """
        Asynchronously send a streaming chat completion request to the OpenAI API, yielding tokens as they arrive.

        This method validates the chat history, constructs the message list (including system prompt and user prompt), and sends a streaming chat completion request using the injected or default OpenAI async client. It logs request and response details, and raises a custom error on failure. Each yielded string is a new token or chunk from the assistant's response.

        Args:
            prompt (str): The user's question or message to send to the assistant.
            history (Sequence[dict[str, str]] | None, optional): Previous chat messages for context. Each must be a dict with 'role' and 'content'.
            **kwargs: Additional keyword arguments to pass to the OpenAI API (e.g., max_tokens, temperature, stop, presence_penalty, frequency_penalty, etc.).

        Yields:
            str: The next chunk or token from the assistant's response.

        Raises:
            OpenAIClientError: If the API call fails, streaming is not supported, or if parameters are invalid.

        Example:
            >>> async for chunk in client.stream_chat("Tell me a joke.", max_tokens=20, temperature=0.5, stop=["\n"]):
            ...     print(chunk, end="")
        """
        self._validate_history(history)
        messages = self._build_messages(prompt, history)
        try:
            _LOGGER.info(f"[OpenAIClient.stream_chat] Sending streaming chat request | model={self.model}, base_url={self.base_url}, prompt_len={len(prompt)}, history_len={len(history) if history else 0}")
            start_time = time.monotonic()
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                stream=True,
                **kwargs
            )
            request_id = getattr(response, 'id', None)
            yielded = False
            async for chunk in response:
                # OpenAI's SDK returns chunks with choices[0].delta.content
                content = getattr(chunk.choices[0].delta, "content", None)
                if content:
                    yielded = True
                    yield content
            elapsed = time.monotonic() - start_time
            if not yielded:
                _LOGGER.warning(f"[OpenAIClient.stream_chat] No content yielded in stream | request_id={request_id}")
            _LOGGER.info(f"[OpenAIClient.stream_chat] Streaming chat completion finished | request_id={request_id} | elapsed={elapsed:.3f}s")
        except openai.OpenAIError as e:
            _LOGGER.error(f"[OpenAIClient.stream_chat] OpenAI API streaming call failed: {e}", exc_info=True)
            raise OpenAIClientError(f"OpenAI API streaming call failed: {e}") from e
        except Exception as e:
            _LOGGER.error(f"[OpenAIClient.stream_chat] Unexpected error: {e}", exc_info=True)
            raise OpenAIClientError(f"Unexpected error: {e}") from e

