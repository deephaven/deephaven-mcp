"""
openai.py - Generic OpenAI/LLM client utilities for deephaven_mcp.

This module provides a robust, production-ready OpenAIClient class for interacting with
OpenAI-compatible LLM APIs. The client is designed for high-volume usage scenarios and
includes comprehensive error handling, connection pooling, and resource management.

Key Features:
    - Async context manager support for automatic resource cleanup
    - Configurable HTTP connection pooling to prevent resource exhaustion
    - Comprehensive timeout and retry configuration
    - Support for both streaming and non-streaming chat completions
    - Chat history validation and message formatting
    - System prompt support for conversation context
    - Detailed logging for debugging and monitoring

Classes:
    OpenAIClientError: Custom exception for OpenAI client errors.
    OpenAIClient: Asynchronous client for OpenAI-compatible chat APIs.

Typical Usage:
    >>> async with OpenAIClient(
    ...     api_key="sk-...",
    ...     base_url="https://api.openai.com/v1",
    ...     model="gpt-3.5-turbo"
    ... ) as client:
    ...     response = await client.chat("Hello, world!")
    ...     print(response)
"""

import logging
import time
from collections.abc import AsyncGenerator, Sequence
from typing import Any

import httpx
import openai

_LOGGER = logging.getLogger(__name__)


class OpenAIClientError(Exception):
    """
    Custom exception for OpenAIClient errors.

    This exception is raised when the OpenAIClient encounters errors during initialization,
    API communication, or response processing. It serves as a unified error interface for
    all OpenAI client-related failures, wrapping underlying exceptions with additional context.

    Common Error Scenarios:
        - **API Communication Failures**: Network timeouts, connection errors, HTTP errors
        - **Authentication Issues**: Invalid API keys, expired tokens, permission denied
        - **Parameter Validation**: Invalid or missing required parameters (api_key, base_url, model)
        - **Response Processing**: Malformed API responses, null content, unexpected structure
        - **Resource Management**: Connection pool exhaustion, client cleanup failures
        - **Streaming Errors**: Non-iterable streaming responses, interrupted streams

    Error Message Format:
        The exception message typically includes:
        - A descriptive error summary
        - The underlying error details (when available)
        - Context about the operation that failed
        - Relevant request parameters for debugging

    Attributes:
        Inherits all attributes from the base Exception class.

    Example:
        >>> try:
        ...     client = OpenAIClient(api_key="", base_url="invalid", model="")
        ... except OpenAIClientError as e:
        ...     print(f"Client initialization failed: {e}")

        >>> try:
        ...     response = await client.chat("Hello")
        ... except OpenAIClientError as e:
        ...     print(f"Chat request failed: {e}")
    """

    pass


class OpenAIClient:
    """
    Asynchronous client for OpenAI-compatible chat APIs, supporting chat completion and streaming.

    This class provides a production-ready wrapper around the OpenAI Python SDK with enhanced
    reliability, resource management, and error handling. It's designed for high-volume usage
    scenarios and includes sophisticated HTTP connection pooling to prevent resource exhaustion.

    The client supports both OpenAI's official API and compatible endpoints (e.g., Azure OpenAI,
    local LLM servers, or other OpenAI-compatible services). It provides comprehensive validation
    of chat message history and system prompts, ensuring API compatibility and data integrity.

    Architecture:
        - **Connection Management**: Custom HTTP client with configurable connection pools
        - **Resource Cleanup**: Async context manager support for automatic resource management
        - **Error Handling**: Comprehensive exception wrapping with detailed error context
        - **Logging**: Detailed request/response logging for debugging and monitoring
        - **Validation**: Input validation for chat history, system prompts, and parameters

    Key Features:
        - **Async Context Manager**: Use with `async with` for automatic resource cleanup
        - **Connection Pooling**: Configurable HTTP connection limits to prevent resource exhaustion
        - **Timeout Configuration**: Granular timeout settings (connect, read, write, pool)
        - **Retry Logic**: Configurable retry behavior for transient failures
        - **Streaming Support**: Both regular and streaming chat completions
        - **History Validation**: Ensures chat history follows OpenAI API format requirements
        - **System Prompts**: Support for conversation context and behavior modification
        - **Testing Support**: Dependency injection for mock clients in unit tests

    Performance Considerations:
        - Uses connection pooling to reuse HTTP connections across requests
        - Configurable connection limits prevent resource exhaustion during stress testing
        - Proper timeout settings prevent hanging requests and improve reliability
        - Async design allows for concurrent request processing

    Attributes:
        api_key (str): The API key for authentication with the LLM service.
        base_url (str): The base URL of the OpenAI-compatible API endpoint.
        model (str): The model name to use for chat completions (e.g., "gpt-3.5-turbo").
        client (openai.AsyncOpenAI): The underlying OpenAI async client instance.

    Private Attributes:
        _client_owned (bool): Whether this instance owns the client and is responsible for cleanup.
            True for production clients, False for injected test clients.

    Usage Patterns:
        **Recommended (Async Context Manager):**
        >>> async with OpenAIClient(
        ...     api_key="sk-...",
        ...     base_url="https://api.openai.com/v1",
        ...     model="gpt-3.5-turbo",
        ...     timeout=30.0,
        ...     max_retries=2
        ... ) as client:
        ...     # Single request
        ...     response = await client.chat("Hello, world!")
        ...
        ...     # With conversation history
        ...     history = [{"role": "user", "content": "Hi"}, {"role": "assistant", "content": "Hello!"}]
        ...     response = await client.chat("How are you?", history=history)
        ...
        ...     # With system prompts
        ...     response = await client.chat(
        ...         "Tell me a joke",
        ...         system_prompts=["You are a helpful assistant", "Be concise"]
        ...     )
        ...
        ...     # Streaming response
        ...     async for chunk in client.stream_chat("Tell me a story"):
        ...         print(chunk, end="")
        # Client automatically cleaned up

        **Manual Resource Management:**
        >>> client = OpenAIClient(api_key="sk-...", base_url="https://api.openai.com/v1", model="gpt-3.5-turbo")
        >>> try:
        ...     response = await client.chat("Hello!")
        ... finally:
        ...     await client.close()  # Important: Manual cleanup required

        **High-Volume Configuration:**
        >>> async with OpenAIClient(
        ...     api_key="sk-...",
        ...     base_url="https://api.openai.com/v1",
        ...     model="gpt-3.5-turbo",
        ...     max_connections=20,  # Increase for high volume
        ...     max_keepalive_connections=10,
        ...     timeout=60.0,  # Longer timeout for complex requests
        ...     max_retries=3  # More retries for reliability
        ... ) as client:
        ...     # Handle multiple concurrent requests
        ...     tasks = [client.chat(f"Request {i}") for i in range(100)]
        ...     responses = await asyncio.gather(*tasks)
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        client: openai.AsyncOpenAI | None = None,
        timeout: float = 60.0,
        max_retries: int = 2,
        max_connections: int = 10,
        max_keepalive_connections: int = 5,
        connect_timeout: float = 10.0,
        write_timeout: float = 10.0,
        pool_timeout: float = 5.0,
    ) -> None:
        """
        Initialize an OpenAIClient instance with comprehensive configuration options.

        This constructor creates a production-ready OpenAI client with optimized connection
        pooling and timeout settings. The client can be configured for different usage patterns,
        from single requests to high-volume concurrent operations.

        Args:
            api_key (str): The API key for authentication with the LLM service.
                Must be a non-empty string. For OpenAI, this starts with 'sk-'.
                For other services, consult their documentation for the expected format.
            base_url (str): The base URL of the OpenAI-compatible API endpoint.
                Must be a non-empty string with proper URL format.
                Examples:
                - OpenAI: "https://api.openai.com/v1"
                - Azure OpenAI: "https://your-resource.openai.azure.com/"
                - Local server: "http://localhost:8000/v1"
            model (str): The model name to use for chat completions.
                Must be a non-empty string. The model must be available at the specified endpoint.
                Examples:
                - OpenAI: "gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"
                - Other services: Consult service documentation for available models
            client (openai.AsyncOpenAI | None, optional): Optionally inject a custom OpenAI async client.
                This parameter is primarily intended for testing scenarios where you need to
                inject a mock client. In production, leave this as None to use the optimized
                client configuration. Defaults to None.
            timeout (float, optional): Primary request timeout in seconds for API calls.
                This is the maximum time to wait for a complete response from the API.
                Should be set based on expected response times for your use case.
                Defaults to 60.0 seconds.
            max_retries (int, optional): Maximum number of automatic retries for failed requests.
                The client will retry requests that fail due to transient errors (network issues,
                temporary server errors). Set to 0 to disable retries. Defaults to 2.
            max_connections (int, optional): Maximum total HTTP connections in the connection pool.
                This limits the total number of concurrent connections to prevent resource exhaustion.
                Increase for high-volume concurrent usage. Defaults to 10.
            max_keepalive_connections (int, optional): Maximum persistent connections to keep alive.
                These connections are reused for subsequent requests to improve performance.
                Should be <= max_connections. Defaults to 5.
            connect_timeout (float, optional): Maximum time to establish a connection in seconds.
                This is the timeout for the initial TCP handshake and TLS negotiation.
                Defaults to 10.0 seconds.
            write_timeout (float, optional): Maximum time to send request data in seconds.
                This is the timeout for sending the request body to the server.
                Defaults to 10.0 seconds.
            pool_timeout (float, optional): Maximum time to acquire a connection from the pool.
                This is the timeout for waiting when all connections are busy.
                Defaults to 5.0 seconds.

        Raises:
            OpenAIClientError: If any required parameter (api_key, base_url, model) is missing,
                empty, or not a string. The error message will specify which parameter is invalid.

        Configuration Guidelines:
            **Low-Volume Usage (< 10 requests/minute):**
            >>> client = OpenAIClient(
            ...     api_key="sk-...",
            ...     base_url="https://api.openai.com/v1",
            ...     model="gpt-3.5-turbo"
            ... )  # Use defaults

            **Medium-Volume Usage (10-100 requests/minute):**
            >>> client = OpenAIClient(
            ...     api_key="sk-...",
            ...     base_url="https://api.openai.com/v1",
            ...     model="gpt-3.5-turbo",
            ...     max_connections=15,
            ...     max_keepalive_connections=8,
            ...     timeout=45.0
            ... )

            **High-Volume Usage (> 100 requests/minute):**
            >>> client = OpenAIClient(
            ...     api_key="sk-...",
            ...     base_url="https://api.openai.com/v1",
            ...     model="gpt-3.5-turbo",
            ...     max_connections=25,
            ...     max_keepalive_connections=15,
            ...     timeout=90.0,
            ...     max_retries=3
            ... )
        """
        if not api_key or not isinstance(api_key, str):
            raise OpenAIClientError("api_key must be a non-empty string.")
        if not base_url or not isinstance(base_url, str):
            raise OpenAIClientError("base_url must be a non-empty string.")
        if not model or not isinstance(model, str):
            raise OpenAIClientError("model must be a non-empty string.")

        _LOGGER.debug(
            f"[OpenAIClient.__init__] Initializing client | model={model}, base_url={base_url}, timeout={timeout}"
        )

        self.api_key: str = api_key
        self.base_url: str = base_url
        self.model: str = model
        # Client Creation Strategy:
        # We create our own HTTP client configuration to prevent "Truncated response body" errors
        # that occur during high-volume usage (e.g., stress testing with 100+ sequential requests).
        # The default OpenAI client configuration can lead to connection pool exhaustion and timeouts.

        if client is None:
            # PRODUCTION PATH: Create a properly configured client with resource limits
            # This prevents connection pool exhaustion that causes truncated responses

            # Step 1: Configure the underlying HTTP client with connection pool limits
            # This is the key to preventing resource exhaustion during stress testing
            http_client: httpx.AsyncClient = httpx.AsyncClient(
                # Connection Pool Limits (prevents resource exhaustion)
                limits=httpx.Limits(
                    max_connections=max_connections,  # Total connections across all hosts
                    max_keepalive_connections=max_keepalive_connections,  # Reusable connections
                ),
                # Timeout Configuration (prevents hanging requests)
                timeout=httpx.Timeout(
                    connect=connect_timeout,  # Time to establish connection (handshake)
                    read=timeout,  # Time to read response (main request timeout)
                    write=write_timeout,  # Time to send request data
                    pool=pool_timeout,  # Time to get connection from pool
                ),
            )

            # Step 2: Create OpenAI client with our configured HTTP client
            # This ensures all requests use our connection limits and timeout settings
            self.client: openai.AsyncOpenAI = openai.AsyncOpenAI(
                api_key=api_key,
                base_url=base_url,
                timeout=timeout,  # OpenAI-level timeout (should match read timeout)
                max_retries=max_retries,  # Retry failed requests (handles transient errors)
                http_client=http_client,  # Use our configured HTTP client
            )
            self._client_owned = True  # We created it, so we're responsible for cleanup
            _LOGGER.debug(
                f"[OpenAIClient.__init__] Created production client with connection limits | max_connections={max_connections}, max_keepalive={max_keepalive_connections}"
            )
        else:
            # TESTING PATH: Use injected client (for unit tests)
            # This allows tests to inject mock clients without our production configuration
            self.client = client
            self._client_owned = False  # We don't own it, so don't close it
            _LOGGER.debug("[OpenAIClient.__init__] Using injected client for testing")

    def _validate_history(self, history: Sequence[dict[str, str]] | None) -> None:
        """
        Validate that the chat history is a sequence of dicts with 'role' and 'content' string keys.

        This method ensures that the chat history follows the OpenAI API format requirements.
        Each message must be a dictionary with exactly 'role' and 'content' keys, both containing strings.
        Valid roles include 'user', 'assistant', and 'system'.

        Args:
            history (Sequence[dict[str, str]] | None): The chat history to validate.
                Each entry must be a dict with string 'role' and 'content' keys.
                If None, validation is skipped.

        Raises:
            OpenAIClientError: If history is not a sequence of dicts, or if any entry
                is missing required keys or has non-string values.

        Example:
            >>> client._validate_history([
            ...     {"role": "user", "content": "Hi"},
            ...     {"role": "assistant", "content": "Hello!"}
            ... ])
        """
        if history is not None:
            if not isinstance(history, list | tuple):
                raise OpenAIClientError(
                    "history must be a sequence (list or tuple) of dicts"
                )
            for msg in history:
                if not isinstance(msg, dict):
                    raise OpenAIClientError("Each message in history must be a dict")
                if "role" not in msg or "content" not in msg:
                    raise OpenAIClientError(
                        "Each message in history must have 'role' and 'content' keys"
                    )
                if not isinstance(msg["role"], str) or not isinstance(
                    msg["content"], str
                ):
                    raise OpenAIClientError(
                        "'role' and 'content' in each message must be strings"
                    )

    def _validate_system_prompts(self, system_prompts: Sequence[str] | None) -> None:
        """
        Validate that the system prompts are a sequence of strings.

        System prompts are used to set the behavior and context for the AI assistant.
        Each prompt must be a string that will be sent as a system message.

        Args:
            system_prompts (Sequence[str] | None): The system prompts to validate.
                Each entry must be a string. If None, validation is skipped.

        Raises:
            OpenAIClientError: If system_prompts is not a sequence of strings.

        Example:
            >>> client._validate_system_prompts(["You are a helpful assistant.", "Be concise."])
        """
        if system_prompts is not None:
            if not isinstance(system_prompts, list | tuple):
                raise OpenAIClientError(
                    "system_prompts must be a sequence (list or tuple) of strings"
                )
            for prompt in system_prompts:
                if not isinstance(prompt, str):
                    raise OpenAIClientError("Each system prompt must be a string")

    def _build_messages(
        self,
        prompt: str,
        history: Sequence[dict[str, str]] | None,
        system_prompts: Sequence[str] | None = None,
    ) -> list[dict[str, str]]:
        """
        Construct the messages list for OpenAI chat completion requests.

        This method builds a properly formatted message list by combining system prompts,
        conversation history, and the current user prompt in the correct order required
        by the OpenAI API. The order is: system messages first, then history, then the new prompt.

        Args:
            prompt (str): The latest user message to append to the conversation.
            history (Sequence[dict[str, str]] | None): Previous chat messages for context.
                Each must be a dict with 'role' and 'content' keys. If None, no history is added.
            system_prompts (Sequence[str] | None): Optional sequence of system prompt strings
                to prepend as system messages. If None, no system messages are added.

        Returns:
            list[dict[str, str]]: The formatted list of messages for the OpenAI API,
                with each message containing 'role' and 'content' keys.

        Raises:
            OpenAIClientError: If history or system_prompts validation fails.

        Example:
            >>> client._build_messages("What's the weather?", [{"role": "user", "content": "Hi"}], ["You are a helpful assistant."])
            [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hi"},
                {"role": "user", "content": "What's the weather?"}
            ]
        """
        self._validate_history(history)
        self._validate_system_prompts(system_prompts)
        messages: list[dict[str, str]] = []
        # Insert system prompts first (in order)
        if system_prompts:
            for sys_msg in system_prompts:
                messages.append({"role": "system", "content": sys_msg})
        # Then add history
        if history:
            messages.extend(history)
        # Finally, add the new user prompt
        messages.append({"role": "user", "content": prompt})
        return messages

    async def chat(
        self,
        prompt: str,
        history: Sequence[dict[str, str]] | None = None,
        system_prompts: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> str:
        r"""
        Asynchronously send a chat completion request to the OpenAI API and return the assistant's response.

        This method validates the chat history and system prompts, constructs the message list,
        and sends a non-streaming chat completion request using the configured OpenAI async client.
        It logs request and response details, and raises a custom error on failure.

        Args:
            prompt (str): The prompt to send to the model.
            history (Sequence[dict[str, str]] | None, optional): Previous chat messages for context.
                Each must be a dict with 'role' and 'content' keys. Defaults to None.
            system_prompts (Sequence[str] | None, optional): Optional sequence of system prompt strings
                to prepend as system messages. Defaults to None.
            **kwargs (Any): Additional keyword arguments to pass to the OpenAI API
                (e.g., max_tokens, temperature, stop, presence_penalty, frequency_penalty, etc.).

        Returns:
            str: The assistant's response message content (stripped of leading/trailing whitespace).

        Raises:
            OpenAIClientError: If the API call fails, returns an error, or if parameters are invalid.

        Example:
            >>> await client.chat("Hello, who are you?", max_tokens=100, temperature=0.7, stop=["\n"], system_prompts=["You are a bot."])
            'I am an AI language model developed by OpenAI...'
        """
        messages = self._build_messages(prompt, history, system_prompts)
        try:
            _LOGGER.info(
                f"[OpenAIClient.chat] Sending chat completion request | model={self.model}, base_url={self.base_url}, prompt_len={len(prompt)}, history_len={len(history) if history else 0}, system_prompts_len={len(system_prompts) if system_prompts else 0}"
            )
            start_time = time.monotonic()
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,  # type: ignore[arg-type]  # Acceptable: OpenAI expects a list of dicts with 'role' and 'content'
                **kwargs,
            )
            elapsed = time.monotonic() - start_time
            request_id = getattr(response, "id", None)
            # Validate response structure
            if (
                not hasattr(response, "choices")
                or not response.choices
                or not hasattr(response.choices[0], "message")
                or not hasattr(response.choices[0].message, "content")
            ):
                _LOGGER.error(
                    f"[OpenAIClient.chat] Unexpected response structure: {response}"
                )
                raise OpenAIClientError("Unexpected response structure from OpenAI API")
            _LOGGER.info(
                f"[OpenAIClient.chat] Chat completion succeeded | request_id={request_id} | elapsed={elapsed:.3f}s"
            )
            content = response.choices[0].message.content
            if content is None:
                raise OpenAIClientError("OpenAI API returned a null content message")
            return content.strip()
        except openai.OpenAIError as e:
            _LOGGER.error(
                f"[OpenAIClient.chat] OpenAI API call failed: {e}", exc_info=True
            )
            raise OpenAIClientError(f"OpenAI API call failed: {e}") from e
        except Exception as e:
            _LOGGER.error(f"[OpenAIClient.chat] Unexpected error: {e}", exc_info=True)
            raise OpenAIClientError(f"Unexpected error: {e}") from e

    async def stream_chat(
        self,
        prompt: str,
        history: Sequence[dict[str, str]] | None = None,
        system_prompts: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> AsyncGenerator[str, None]:
        r"""Asynchronously send a streaming chat completion request to the OpenAI API, yielding tokens as they arrive.

        This method validates the chat history, constructs the message list (including system prompt(s)
        and user prompt), and sends a streaming chat completion request using the configured OpenAI
        async client. It logs request and response details, and raises a custom error on failure.
        Each yielded string is a new token or chunk from the assistant's response.

        Args:
            prompt (str): The user's question or message to send to the assistant.
            history (Sequence[dict[str, str]] | None, optional): Previous chat messages for context.
                Each must be a dict with 'role' and 'content' keys. Defaults to None.
            system_prompts (Sequence[str] | None, optional): Optional sequence of system prompt strings
                to prepend as system messages. Defaults to None.
            **kwargs (Any): Additional keyword arguments to pass to the OpenAI API
                (e.g., max_tokens, temperature, stop, presence_penalty, frequency_penalty, etc.).

        Yields:
            str: The next chunk or token from the assistant's response.

        Raises:
            OpenAIClientError: If the API call fails, streaming is not supported, or if parameters are invalid.

        Example:
            >>> async for chunk in client.stream_chat("Tell me a joke.", max_tokens=20, temperature=0.5, stop=["\n"], system_prompts=["You are a bot."]):
            ...     print(chunk, end="")
        """
        messages = self._build_messages(prompt, history, system_prompts)
        try:
            _LOGGER.info(
                f"[OpenAIClient.stream_chat] Sending streaming chat request | model={self.model}, base_url={self.base_url}, prompt_len={len(prompt)}, history_len={len(history) if history else 0}, system_prompts_len={len(system_prompts) if system_prompts else 0}"
            )
            start_time = time.monotonic()
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,  # type: ignore[arg-type]  # Acceptable: OpenAI expects a list of dicts with 'role' and 'content'
                stream=True,
                **kwargs,
            )
            request_id = getattr(response, "id", None)
            yielded = False
            # Only async iterate if response is an async iterable
            if hasattr(response, "__aiter__"):
                async for chunk in response:
                    # OpenAI's SDK returns chunks with choices[0].delta.content
                    content = getattr(chunk.choices[0].delta, "content", None)
                    if content:
                        yielded = True
                        yield content
            else:
                _LOGGER.error(
                    f"[OpenAIClient.stream_chat] Response is not async iterable: {type(response)} | request_id={request_id}"
                )
                raise OpenAIClientError(
                    "OpenAI API did not return an async iterable for streaming chat."
                )
            elapsed = time.monotonic() - start_time
            if not yielded:
                _LOGGER.warning(
                    f"[OpenAIClient.stream_chat] No content yielded in stream | request_id={request_id}"
                )
            _LOGGER.info(
                f"[OpenAIClient.stream_chat] Streaming chat completion finished | request_id={request_id} | elapsed={elapsed:.3f}s"
            )
        except openai.OpenAIError as e:
            _LOGGER.error(
                f"[OpenAIClient.stream_chat] OpenAI API streaming call failed: {e}",
                exc_info=True,
            )
            raise OpenAIClientError(f"OpenAI API streaming call failed: {e}") from e
        except Exception as e:
            _LOGGER.error(
                f"[OpenAIClient.stream_chat] Unexpected error: {e}", exc_info=True
            )
            raise OpenAIClientError(f"Unexpected error: {e}") from e

    async def close(self) -> None:
        """
        Close the underlying OpenAI client and release HTTP connection resources.

        This method performs cleanup of the HTTP connection pool and any other resources
        associated with the OpenAI client. It's essential for preventing resource leaks,
        especially in long-running applications or high-volume usage scenarios.

        The method is designed to be safe and robust:
        - Only closes resources for clients created by this instance (not injected test clients)
        - Idempotent: safe to call multiple times without side effects
        - Graceful error handling: logs cleanup errors without raising exceptions
        - Automatic: called automatically when using async context manager

        Resource Cleanup Details:
            - Closes HTTP connection pool to free network resources
            - Releases any pending connections and associated memory
            - Cancels any background tasks related to connection management
            - Ensures proper cleanup even if exceptions occurred during usage

        When to Call:
            - **Manual Resource Management**: Always call when done with the client
            - **Long-Running Applications**: Call periodically to prevent resource accumulation
            - **High-Volume Usage**: Essential after processing batches of requests
            - **Error Recovery**: Call during cleanup after exceptions

        Note:
            After calling close(), this client instance should not be used for further
            API requests. Create a new instance if you need to make additional requests.

        Examples:
            **Manual Cleanup:**
            >>> client = OpenAIClient(api_key="sk-...", base_url="https://api.openai.com/v1", model="gpt-3.5-turbo")
            >>> try:
            ...     response = await client.chat("Hello!")
            ... finally:
            ...     await client.close()  # Always clean up

            **Batch Processing:**
            >>> for batch in request_batches:
            ...     client = OpenAIClient(...)
            ...     try:
            ...         results = await process_batch(client, batch)
            ...     finally:
            ...         await client.close()  # Clean up after each batch

            **Error Recovery:**
            >>> client = OpenAIClient(...)
            >>> try:
            ...     response = await client.chat("Hello!")
            ... except Exception as e:
            ...     logger.error(f"Request failed: {e}")
            ... finally:
            ...     await client.close()  # Clean up even on error
        """
        if self._client_owned:
            try:
                await self.client.close()
                _LOGGER.debug("[OpenAIClient.close] HTTP client connections closed")
            except Exception as e:
                _LOGGER.warning(f"[OpenAIClient.close] Error closing HTTP client: {e}")
        else:
            _LOGGER.debug(
                "[OpenAIClient.close] Not closing HTTP client since it was not owned by this instance"
            )

    async def __aenter__(self) -> "OpenAIClient":
        """
        Async context manager entry point.

        Returns:
            OpenAIClient: This client instance for use within the async with block.

        Example:
            >>> async with OpenAIClient(api_key="sk-...", base_url="https://api.openai.com/v1", model="gpt-3.5-turbo") as client:
            ...     response = await client.chat("Hello!")
            ...     print(response)
            # Client is automatically closed when exiting the with block
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        """
        Async context manager exit point with automatic resource cleanup.

        Args:
            exc_type: Exception type (if any exception occurred in the with block).
            exc_val: Exception value (if any exception occurred in the with block).
            exc_tb: Exception traceback (if any exception occurred in the with block).

        Note:
            This method automatically calls close() to clean up HTTP connection resources,
            regardless of whether an exception occurred in the with block. Any exceptions
            from close() are logged but not propagated to avoid masking the original exception.
        """
        try:
            await self.close()
        except Exception as e:
            # Log cleanup errors but don't propagate them to avoid masking the original exception
            _LOGGER.warning(f"[OpenAIClient.__aexit__] Error during cleanup: {e}")
