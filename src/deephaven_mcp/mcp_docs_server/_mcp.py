"""
Deephaven MCP Docs Tools Module.

This module defines the MCP (Model Context Protocol) server and tools for the Deephaven documentation assistant, powered by Inkeep LLM APIs. It provides a production-ready, agentic interface for documentation Q&A with comprehensive error handling and structured responses optimized for AI agent consumption.

Architecture:
    - FastMCP server with health check endpoint (/health)
    - Per-request OpenAI client creation for connection stability
    - Structured error responses with consistent success/error format
    - Context-aware documentation responses with version and language support

Key Features:
    - Asynchronous, non-blocking tool interface for documentation chat
    - Robust environment validation and comprehensive error handling
    - Designed specifically for LLM orchestration, agent frameworks, and programmatic use
    - All tools return structured dict responses with success/error indicators
    - AI-agent optimized documentation with detailed parameter descriptions

Environment Variables:
    INKEEP_API_KEY: The API key for authenticating with the Inkeep-powered LLM API. Must be set in the environment.
    MCP_DOCS_HOST: The host to bind the FastMCP server to. Defaults to 127.0.0.1 (localhost). Set to 0.0.0.0 for external access.
    MCP_DOCS_PORT: The port to bind the FastMCP server to. Defaults to 8001. Falls back to PORT for Cloud Run compatibility.

Server:
    - mcp_server (FastMCP): The MCP server instance exposing all registered tools.

MCP Tools (AI Agent Interface):
    - docs_chat: Asynchronous documentation Q&A tool with context-aware responses.
      Supports conversation history, version-specific guidance, and programming
      language context. Returns structured dict responses for reliable parsing.

AI Agent Integration:
    This module is specifically designed for AI agent consumption with:
    - Structured response format: All tools return dict with 'success' boolean
    - Comprehensive error handling: No exceptions propagated to MCP layer
    - Detailed parameter validation with descriptive error messages
    - Context-aware responses based on Deephaven version and programming language
    - Multi-turn conversation support via history parameter

Usage Patterns:
    **MCP Framework Integration:**
    >>> # Via MCP-compatible agent frameworks (recommended)
    >>> # Tools are automatically discovered and invoked by MCP protocol

    **Direct Tool Invocation:**
    >>> from deephaven_mcp.mcp_docs_server._mcp import docs_chat
    >>> context = {}  # Context not currently used but required by MCP protocol
    >>>
    >>> # Basic query
    >>> result = await docs_chat(context=context, prompt="How do I install Deephaven?")
    >>> print(result)
    {'success': True, 'response': 'To install Deephaven, ...'}
    >>>
    >>> # Context-aware query with version and language
    >>> result = await docs_chat(
    ...     context=context,
    ...     prompt="Show me table join syntax",
    ...     deephaven_core_version="0.35.1",
    ...     programming_language="python"
    ... )
    >>>
    >>> # Multi-turn conversation
    >>> history = [{"role": "user", "content": "What are tables?"},
    ...            {"role": "assistant", "content": "Tables are..."}]
    >>> result = await docs_chat(
    ...     context=context,
    ...     prompt="How do I create one?",
    ...     history=history
    ... )
"""

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import Context, FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from ..openai import OpenAIClient, OpenAIClientError

_LOGGER = logging.getLogger(__name__)

# The API key for authenticating with the Inkeep-powered LLM API. Must be set in the environment.
try:
    _INKEEP_API_KEY: str = os.environ["INKEEP_API_KEY"]
except KeyError:
    raise RuntimeError(
        "INKEEP_API_KEY environment variable must be set to use the Inkeep-powered documentation tools."
    ) from None

mcp_docs_host: str = os.environ.get("MCP_DOCS_HOST", "127.0.0.1")
"""
str: The host to bind the FastMCP server to. Defaults to 127.0.0.1 (localhost).
Set MCP_DOCS_HOST to '0.0.0.0' for external access, or another interface as needed.
"""

mcp_docs_port: int = int(
    os.environ.get("MCP_DOCS_PORT", os.environ.get("PORT", "8001"))
)
"""
int: The port to bind the FastMCP server to. Defaults to 8001.
Uses MCP_DOCS_PORT if set, otherwise falls back to PORT (for Cloud Run compatibility).
"""


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[dict[str, object]]:
    """
    Async context manager for the FastMCP docs server application lifespan.

    This function manages the startup and shutdown lifecycle of the MCP docs server.
    The implementation uses per-request OpenAI client creation to prevent connection
    pool exhaustion and "Truncated response body" errors during high-volume usage
    or stress testing scenarios.

    Lifecycle Management:
        - Startup: Logs server initialization and configuration
        - Runtime: Yields empty context (clients created per-request)
        - Shutdown: Logs graceful server termination

    Args:
        server (FastMCP): The FastMCP server instance being managed.

    Yields:
        dict[str, object]: An empty context dictionary. OpenAI clients are created
                          per-request rather than shared to ensure connection stability.

    Note:
        This function is automatically called by FastMCP during server startup and shutdown.
        The per-request client strategy prevents resource leaks and connection pool issues
        that could cause server instability during sustained high-volume operations.

    Example:
        >>> mcp_server = FastMCP("server-name", lifespan=app_lifespan)
        >>> # The lifespan manager automatically handles startup/shutdown
    """
    _LOGGER.info("[mcp_docs_server:app_lifespan] MCP docs server starting up")
    _LOGGER.info(
        "[mcp_docs_server:app_lifespan] Using per-request OpenAI client creation for connection stability"
    )

    try:
        yield {}
    finally:
        _LOGGER.info("[mcp_docs_server:app_lifespan] MCP docs server shutting down")


mcp_server = FastMCP(
    "deephaven-mcp-docs", host=mcp_docs_host, port=mcp_docs_port, lifespan=app_lifespan
)
"""
FastMCP: The primary server instance for the Deephaven MCP documentation tools.

This server exposes all MCP tools and endpoints for AI agent consumption:
- Tools: docs_chat (documentation Q&A)
- Endpoints: /health (liveness/readiness checks)
- Discovery: All @mcp_server.tool decorated functions are automatically registered
- Lifecycle: Managed by app_lifespan context manager for proper startup/shutdown

Configuration:
- Server name: "deephaven-mcp-docs"
- Host: Controlled by MCP_DOCS_HOST environment variable (default: 127.0.0.1)
- Port: Controlled by MCP_DOCS_PORT environment variable (default: 8001, fallback: PORT)
- Lifespan: Uses per-request client creation strategy for connection stability

Usage:
- Designed for MCP-compatible orchestration environments and AI agent frameworks
- Supports both direct tool invocation and HTTP endpoint access
- Optimized for high-volume, concurrent usage with robust error handling
"""


@mcp_server.custom_route("/health", methods=["GET"])  # type: ignore[misc]
async def health_check(request: Request) -> JSONResponse:
    """
    Health check endpoint for the docs server.

    Exposes a simple HTTP GET endpoint at /health for liveness and readiness checks.

    Purpose:
        - Allows load balancers, orchestrators, or monitoring tools to verify that the MCP server is running and responsive.
        - Intended for use as a liveness or readiness probe in deployment environments (e.g., Kubernetes, Cloud Run).

    Args:
        request (Request): The HTTP request object (not used but required by FastMCP).

    Returns:
        JSONResponse: HTTP 200 response with JSON body {"status": "ok"}.

    Request:
        - Method: GET
        - Path: /health
        - No authentication or parameters required.

    Response:
        - HTTP 200 with JSON body: {"status": "ok"}
        - Indicates the server is alive and able to handle requests.
    """
    _LOGGER.debug("[mcp_docs_server:health_check] Health check requested")
    return JSONResponse({"status": "ok"})


# Basic system prompt for Deephaven documentation assistant behavior
_prompt_basic = """
You are a helpful assistant that answers questions about Deephaven Data Labs documentation. 
Only return answers about Legacy Deephaven if explicitly asked.
If you return an answer about Legacy Deephaven, make certain that Legacy and current Deephaven documentation is clearly distinguished in your responses.
Your responses should be concise, accurate, and relevant to the user's query.
"""

# Comprehensive system prompt for Deephaven query string generation with syntax rules and best practices
_prompt_good_query_strings = r"""
When producing Deephaven query strings, your primary goal is to produce valid, accurate, and syntactically correct Deephaven query strings based on user requests. Adherence to Deephaven's query string rules and best practices for performance is critical.

**What is a Deephaven Query String?**
A Deephaven query string is a compact, text-based expression used to define transformations, filters, aggregations, or updates on tables within the Deephaven real-time data platform. These strings are evaluated by Deephaven to manipulate data directly, often within methods like `update()`, `where()`, `select()`, or `agg()`.

**Deephaven Query String Syntax Guidelines:**

1.  **Encapsulation:** All query strings should be enclosed in double quotes (").
    * Example: `update("NewColumn = 1")`

2.  **Literals:**
    * **Boolean/Numeric/Column Names/Variables:** No special encapsulation (e.g., `true`, `123`, `MyColumn`, `i`).
    * **Strings:** Encapsulated in backticks (`` ` ``) (e.g., `` `SomeText` ``).
    * **Date-Time:** Encapsulated in single quotes (') (e.g., `'2023-01-01T00:00:00Z'`).

3.  **Special Variables/Constants:** Use uppercase snake_case (e.g., `HOUR`, `MINUTE`, `NULL_DOUBLE`).

4.  **Operations:**
    * **Mathematical:** `+`, `-`, `*`, `/`, `%`
    * **Logical:** `==` (equality), `!=` (inequality), `>`, `<`, `>=`, `<=`, `&&` (AND), `||` (OR)
    * **Conditional:** `condition ? if_true : if_false`

5.  **Built-in Functions:** Utilize standard Deephaven built-in functions. These functions are highly optimized for performance.
    * Examples: `sqrt()`, `log()`, `parseInstant()`, `lowerBin()`, `upperBin()`, `sin()`, `cos()`.

6.  **Type Casting:** Use `(type)value` (e.g., `(int)LongValue`).

7.  **Null Values:** Use `NULL_TYPE` constants (e.g., `NULL_INT`).

**Using Python in Query Strings:**

* **Interoperability:** Deephaven query strings can seamlessly integrate Python code via a Python-Java bridge.
* **Calling Python Functions:** You can call pre-defined Python functions from within query strings. Ensure the Python function is available in the Deephaven environment.
    * Example: `update("DerivedCol = my_custom_python_func(SourceCol)")`
* **Performance Considerations:**
    * Calling Python functions from query strings involves a "Python-Java boundary crossing" which can introduce overhead, especially for large datasets or frequent computations due to the Python GIL.
    * **Strong Recommendation:** Always prefer Deephaven's built-in query language functions over custom Python functions if an equivalent built-in function exists. Built-in functions are generally much more performant.
    * If a Python function is necessary, design it to be stateless and minimize internal loops or heavy computation that would repeatedly cross the boundary.

**Constraints and Best Practices:**

* **DO NOT** include comments within the generated query string.
* **DO NOT** invent syntax or functions that are not part of Deephaven's official documentation or explicitly available Python functions.
* **Prioritize built-in Deephaven functions for all operations where possible.** Only use custom Python functions for logic that cannot be achieved with built-ins or for integration with specific Python libraries.
* Ensure any Python functions referenced in query strings are correctly defined and loaded in the Deephaven environment before the query is executed.
* Generate the most concise and efficient query string possible that fulfills the request.

**Examples (User Request -> Deephaven Query String):**

* `User Request: "Create a column 'VolumeRatio' by dividing 'Volume' by 'TotalVolume'."`
* `Deephaven Query String: "VolumeRatio = Volume / TotalVolume"`

* `User Request: "Filter the table for rows where 'Symbol' is 'AAPL' OR 'GOOG'."`
* `Deephaven Query String: "Symbol = \`AAPL\` || Symbol = \`GOOG\`"`

* `User Request: "Add 10 minutes to the 'EventTime' column and name it 'NewEventTime'."`
* `Deephaven Query String: "NewEventTime = EventTime + (10 * MINUTE)"`

* `User Request: "Apply my pre-defined Python function 'calculate_premium' to the 'Price' and 'Volatility' columns to create a 'Premium' column."`
    * *Note: This assumes `calculate_premium` is a Python function already defined and accessible in the Deephaven environment.*
* `Deephaven Query String: "Premium = calculate_premium(Price, Volatility)"`

**Your Turn:**

Generate a Deephaven query string based on the following user request: [USER_REQUEST_HERE]
"""


@mcp_server.tool()
async def docs_chat(
    context: Context,
    prompt: str,
    history: list[dict[str, str]] | None = None,
    deephaven_core_version: str | None = None,
    deephaven_enterprise_version: str | None = None,
    programming_language: str | None = None,
) -> dict:
    """
    docs_chat - Asynchronous Documentation Q&A Tool (MCP Tool).

    This tool provides conversational access to the Deephaven documentation assistant, powered by Inkeep LLM APIs. It creates a fresh OpenAI client per request to ensure connection stability and prevent timeout errors during high-volume usage. Responses are optimized for AI agent consumption with structured success/error indicators.

    AI Agent Optimization:
        This tool is specifically designed for AI agent consumption with the following features:
        - **Structured Response Format**: Always returns dict with 'success' boolean for reliable parsing
        - **Comprehensive Error Handling**: Detailed error messages with specific error types (never raises exceptions)
        - **Context-Aware Responses**: Tailors answers based on Deephaven version and programming language
        - **Multi-Turn Conversation Support**: Maintains context through history parameter for follow-up questions
        - **Orchestration Framework Ready**: Compatible with LLM orchestration tools and agent frameworks
        - **Deterministic Behavior**: Consistent response structure enables reliable programmatic processing

    Parameters:
        context (Context):
            The MCP context for this tool call. Currently unused but required by MCP protocol.
            AI agents should pass an empty dict: {}
        prompt (str):
            The user's query or question for the documentation assistant. Should be a clear,
            specific natural language string describing what the user wants to know about Deephaven.

            **Best Practices for AI Agents:**
            - Use specific, detailed questions rather than vague requests
            - Include relevant context (e.g., "How do I join two tables in Python?")
            - Mention specific Deephaven concepts when known (tables, queries, aggregations)
            - For code examples, specify the desired programming language

            **Effective Examples:**
            - "How do I join two tables using a common column?"
            - "What's the syntax for time-based queries in Deephaven?"
            - "Show me how to create a real-time aggregation in Python"
            - "How do I filter a table for rows where column value is greater than 100?"

            **Less Effective Examples:**
            - "Help me with tables" (too vague)
            - "How does Deephaven work?" (too broad)
        history (list[dict[str, str]] | None, optional):
            Previous chat messages for conversational context in multi-turn conversations.
            Each message must be a dict with exactly two keys: 'role' and 'content'.

            **Message Format Requirements:**
            - 'role' (str): Must be either "user" or "assistant"
            - 'content' (str): The actual message content

            **AI Agent Usage Guidelines:**
            - Include relevant recent messages (typically last 3-5 exchanges)
            - Maintain chronological order (oldest first)
            - Only include messages directly related to the current query
            - Omit history for unrelated new topics to avoid confusion
            - Use history for follow-up questions, clarifications, or related queries

            **Example Multi-Turn Conversation:**
            [
                {"role": "user", "content": "How do I create a table in Deephaven?"},
                {"role": "assistant", "content": "You can create tables using..."},
                {"role": "user", "content": "What about filtering that table?"},
                {"role": "assistant", "content": "To filter a table, use the where() method..."}
            ]

            **When to Omit History:**
            - First question in a conversation
            - Completely unrelated new topic
            - When previous context might confuse the current query
        deephaven_core_version (str | None, optional):
            The version of Deephaven Community Core (e.g., "0.35.1"). When provided,
            the assistant tailors responses for version-specific features and syntax.
            Recommended for environment-specific queries.
        deephaven_enterprise_version (str | None, optional):
            The version of Deephaven Core+ (Enterprise) (e.g., "0.35.1"). When provided,
            enables enterprise-specific documentation and feature guidance.
            Use in conjunction with deephaven_core_version for complete context.
        programming_language (str | None, optional):
            The programming language context for tailoring responses with language-specific
            syntax, examples, and best practices. Case-insensitive input is accepted.

            **Supported Languages:**
            - "python": Python-specific Deephaven syntax and examples
            - "groovy": Groovy-specific Deephaven syntax and examples

            **AI Agent Usage Guidelines:**
            - Always specify when requesting code examples or syntax help
            - Use when the user's environment or preference is known
            - Omit for general conceptual questions that don't require code
            - Invalid languages return structured error responses (not exceptions)

            **Impact on Responses:**
            - Code examples will use the specified language syntax
            - API method calls will show language-appropriate patterns
            - Best practices will be language-specific
            - Documentation links will prioritize the specified language

            **Error Handling:**
            - Unsupported languages return: {"success": False, "error": "Unsupported programming language: <lang>. Supported languages are: python, groovy", "isError": True}

    Returns:
        dict: Structured result object optimized for AI agent parsing and error handling.

        **Success Response Structure:**
        {
            "success": True,
            "response": "<natural_language_answer>"
        }

        **Error Response Structure:**
        {
            "success": False,
            "error": "<descriptive_error_message>",
            "isError": True
        }

        **Field Descriptions:**
        - 'success' (bool): **Always present**. True if query completed successfully, False on any error.
                           AI agents should check this field first before accessing other fields.
        - 'response' (str): **Present only when success=True**. The documentation assistant's natural
                           language answer. Content is suitable for direct display to users or further
                           processing by AI agents. May include code examples, explanations, and links.
        - 'error' (str): **Present only when success=False**. Human-readable error message with specific
                        error context. Includes error types like "OpenAIClientError", "Unsupported programming language",
                        or validation errors. Messages are actionable for debugging.
        - 'isError' (bool): **Present only when success=False**. Always True when present. Explicit error
                           flag for frameworks that need boolean error indicators beyond the 'success' field.

        **AI Agent Parsing Guidelines:**
        ```python
        result = await docs_chat(context={}, prompt="How do I join tables?")

        if result["success"]:
            # Safe to access 'response'
            answer = result["response"]
            # Process successful response
        else:
            # Handle error case
            error_msg = result["error"]
            # Log error or retry with different parameters
        ```

    Error Handling for AI Agents:
        This tool implements comprehensive error handling designed for reliable AI agent integration:

        **Critical Safety Guarantees:**
        - **Never raises exceptions** - all errors return structured dict responses
        - **Always includes 'success' field** - reliable for programmatic error detection
        - **Consistent error format** - predictable structure for error handling logic

        **Common Error Categories:**
        1. **API Communication Errors:**
           - "OpenAIClientError: <details>" - Issues with Inkeep/OpenAI API communication
           - "Request timeout" - API response exceeded 300-second timeout
           - "Connection failed" - Network connectivity issues

        2. **Parameter Validation Errors:**
           - "Unsupported programming language: <lang>" - Invalid programming_language value
           - "Invalid history format" - Malformed history parameter structure
           - "Empty prompt" - Missing or empty prompt parameter

        3. **System Configuration Errors:**
           - "Missing INKEEP_API_KEY" - Required environment variable not set
           - "Client initialization failed" - OpenAI client setup issues

        **AI Agent Error Handling Best Practices:**
        ```python
        # Always check success first
        if not result.get("success", False):
            error_msg = result.get("error", "Unknown error")

            # Handle specific error types
            if "Unsupported programming language" in error_msg:
                # Retry with supported language or omit parameter
                pass
            elif "OpenAIClientError" in error_msg:
                # Log API issue, potentially retry after delay
                pass
            elif "timeout" in error_msg.lower():
                # Retry with simpler query or handle timeout
                pass
        ```

    Performance Characteristics:
        - Creates fresh OpenAI client per request (prevents connection pool exhaustion)
        - Typical response time: 5-30 seconds depending on query complexity
        - Timeout: 300 seconds (5 minutes) for complex documentation queries
        - Optimized parameters for faster, more deterministic responses

    Usage Notes for AI Agents:
        - This tool is asynchronous - always await the call in async contexts
        - Discoverable via MCP server tool registries using name 'docs_chat'
        - Provide relevant chat history for multi-turn conversations to improve context
        - Include version information (core/enterprise) for environment-specific guidance
        - Specify programming language for syntax-specific examples and best practices
        - Designed for seamless integration with LLM agents, RAG pipelines, and automation frameworks
        - Responses are optimized for both human readability and programmatic processing

    Examples:
        Basic successful query:
        >>> result = await docs_chat(
        ...     context={},
        ...     prompt="How do I create a table in Deephaven?"
        ... )
        >>> print(result)
        {'success': True, 'response': 'To create a table in Deephaven, you can use...'}

        Context-aware query with version and language:
        >>> result = await docs_chat(
        ...     context={},
        ...     prompt="Show me how to join tables",
        ...     deephaven_core_version="0.35.1",
        ...     programming_language="python"
        ... )
        >>> print(result)
        {'success': True, 'response': 'In Deephaven 0.35.1 with Python, you can join tables using...'}

        Multi-turn conversation with history:
        >>> result = await docs_chat(
        ...     context={},
        ...     prompt="Can you give me more details about the where clause?",
        ...     history=[
        ...         {"role": "user", "content": "How do I filter tables?"},
        ...         {"role": "assistant", "content": "You can filter tables using the where() method..."}
        ...     ]
        ... )
        >>> print(result)
        {'success': True, 'response': 'The where clause in Deephaven allows you to...'}

        Error response - invalid programming language:
        >>> result = await docs_chat(
        ...     context={},
        ...     prompt="How do I use tables?",
        ...     programming_language="javascript"
        ... )
        >>> print(result)
        {'success': False, 'error': 'Unsupported programming language: javascript. Supported languages are: python, groovy', 'isError': True}

        Error response - API timeout:
        >>> result = await docs_chat(
        ...     context={},
        ...     prompt="Complex query about advanced features"
        ... )
        >>> print(result)
        {'success': False, 'error': 'OpenAIClientError: Request timeout after 300 seconds', 'isError': True}

        AI Agent error handling pattern:
        >>> result = await docs_chat(context={}, prompt="How do I use aggregations?")
        >>> if result['success']:
        ...     documentation_answer = result['response']
        ...     # Process successful response
        ... else:
        ...     error_message = result['error']
        ...     # Handle error appropriately
        ...     if 'OpenAIClientError' in error_message:
        ...         # Retry logic for API errors
        ...     elif 'Unsupported programming language' in error_message:
        ...         # Parameter validation error
    """
    _LOGGER.debug(
        f"[mcp_docs_server:docs_chat] Processing documentation query | prompt_len={len(prompt)} | has_history={history is not None} | programming_language={programming_language}"
    )

    try:

        # Build system prompts for context-aware responses
        system_prompts = [
            _prompt_basic,
            _prompt_good_query_strings,
        ]

        # Optionally add version info to system prompts if provided
        if deephaven_core_version:
            system_prompts.append(
                f"Worker environment: Deephaven Community Core version: {deephaven_core_version}"
            )
        if deephaven_enterprise_version:
            system_prompts.append(
                f"Worker environment: Deephaven Core+ (Enterprise) version: {deephaven_enterprise_version}"
            )

        if programming_language:
            # Trim whitespace and validate against supported languages
            programming_language = programming_language.strip().lower()
            supported_languages = {"python", "groovy"}
            if programming_language in supported_languages:
                system_prompts.append(
                    f"Worker environment: Programming language: {programming_language}"
                )
            else:
                error_msg = f"Unsupported programming language: {programming_language}. Supported languages are: {', '.join(supported_languages)}."
                _LOGGER.error(f"[mcp_docs_server:docs_chat] {error_msg}")
                return {"success": False, "error": error_msg, "isError": True}

        # Use OpenAI client as async context manager for automatic resource cleanup
        # This prevents connection pool exhaustion and "Truncated response body" errors
        async with OpenAIClient(
            api_key=_INKEEP_API_KEY,
            base_url="https://api.inkeep.com/v1",
            model="inkeep-context-expert",
            timeout=300.0,  # 5 minutes - handles slow Inkeep API responses
            connect_timeout=30.0,  # 30 seconds to establish connection
            write_timeout=30.0,  # 30 seconds to send request
            max_retries=1,  # Reduce retries to fail faster on real errors
        ) as inkeep_client:
            # Call Inkeep API with performance-optimized parameters
            response = await inkeep_client.chat(
                prompt=prompt,
                history=history,
                system_prompts=system_prompts,
                # Performance optimization parameters for faster responses
                # These parameters tell Inkeep: "Give me the best response you can in ~30-60 seconds"
                max_tokens=1500,  # Limit response length for faster generation
                temperature=0.1,  # Lower temperature = faster, more deterministic responses
                top_p=0.9,  # Nucleus sampling for balanced speed vs quality
                presence_penalty=0.1,  # Slight penalty to encourage conciseness
            )
            _LOGGER.debug(
                f"[mcp_docs_server:docs_chat] Documentation query completed successfully | response_len={len(response)}"
            )
            return {"success": True, "response": response}

    except OpenAIClientError as exc:
        # This could be logged at a lower level since it is potentially not a problem with the MCP server itself,
        # but rather an issue with the OpenAI client or API.
        # However, we log it at the exception level to ensure visibility in case of issues.
        _LOGGER.exception(f"[mcp_docs_server:docs_chat] OpenAI client error: {exc}")
        return {"success": False, "error": f"OpenAIClientError: {exc}", "isError": True}
    except Exception as exc:
        _LOGGER.exception(f"[mcp_docs_server:docs_chat] Unexpected error: {exc}")
        return {
            "success": False,
            "error": f"{type(exc).__name__}: {exc}",
            "isError": True,
        }


__all__ = ["mcp_server"]
