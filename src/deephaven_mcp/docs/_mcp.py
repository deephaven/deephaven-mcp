"""
Deephaven MCP Docs Server - Internal Tool and API Definitions

This module defines the MCP (Multi-Cluster Platform) server and tools for the Deephaven documentation assistant, powered by Inkeep LLM APIs. It exposes agentic, LLM-friendly tool endpoints for documentation Q&A and future extensibility.

Key Features:
    - Asynchronous, agentic tool interface for documentation chat.
    - Robust environment validation and error handling.
    - Designed for LLM orchestration, agent frameworks, and programmatic use.
    - All tools return structured, type-annotated results and have detailed pydocs for agentic consumption.

Environment Variables:
    INKEEP_API_KEY: The API key for authenticating with the Inkeep-powered LLM API. Must be set in the environment.

Server:
    - mcp_server (FastMCP): The MCP server instance exposing all registered tools.

Tools:
    - docs_chat: Asynchronous chat tool for Deephaven documentation Q&A.

Usage:
    Import this module and use the registered tools via MCP-compatible agent frameworks, or invoke directly for backend automation.

Example (agentic usage):
    >>> from deephaven_mcp.docs._mcp import mcp_server
    >>> response = await mcp_server.tools['docs_chat'](prompt="How do I install Deephaven?")
    >>> print(response)
    To install Deephaven, ...
"""

import os

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from ..openai import OpenAIClient

#: The API key for authenticating with the Inkeep-powered LLM API. Must be set in the environment. Private to this module.
_INKEEP_API_KEY = os.environ.get("INKEEP_API_KEY")
"""str: The API key for authenticating with the Inkeep-powered LLM API. Must be set in the environment. Private to this module."""
if not _INKEEP_API_KEY:
    raise RuntimeError(
        "INKEEP_API_KEY environment variable must be set to use the Inkeep-powered documentation tools."
    )

inkeep_client = OpenAIClient(
    api_key=_INKEEP_API_KEY,
    base_url="https://api.inkeep.com/v1",
    model="inkeep-context-expert",
)
"""
OpenAIClient: Configured for Inkeep-powered Deephaven documentation Q&A.
- api_key: Pulled from _INKEEP_API_KEY env var.
- base_url: https://api.inkeep.com/v1
- model: inkeep-context-expert

This client is injected into tools for agentic and programmatic use. It should not be instantiated directly by users.
"""

mcp_server = FastMCP("deephaven-mcp-docs")
"""
FastMCP: The server instance for the Deephaven documentation tools.
- All tools decorated with @mcp_server.tool are registered here and discoverable by agentic frameworks.
- The server is intended for use in MCP-compatible orchestration environments.
"""


@mcp_server.custom_route("/health", methods=["GET"])  # type: ignore[misc]
async def health_check(request: Request) -> JSONResponse:
    """
    Health Check Endpoint
    ---------------------
    Exposes a simple HTTP GET endpoint at /health for liveness and readiness checks.

    Purpose:
        - Allows load balancers, orchestrators, or monitoring tools to verify that the MCP server is running and responsive.
        - Intended for use as a liveness or readiness probe in deployment environments (e.g., Kubernetes, Cloud Run).

    Request:
        - Method: GET
        - Path: /health
        - No authentication or parameters required.

    Response:
        - HTTP 200 with JSON body: {"status": "ok"}
        - Indicates the server is alive and able to handle requests.
    """
    return JSONResponse({"status": "ok"})


_prompt_basic = """
You are a helpful assistant that answers questions about Deephaven Data Labs documentation. 
Never return answers about Legacy Deephaven.
"""

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
    prompt: str,
    history: list[dict[str, str]] | None = None,
    deephaven_core_version: str | None = None,
    deephaven_enterprise_version: str | None = None,
) -> str:
    """
    docs_chat - Asynchronous Documentation Q&A Tool (MCP Tool)

    This tool provides conversational access to the Deephaven documentation assistant, powered by LLM APIs. It is designed for LLM agents, orchestration frameworks, and backend automation to answer Deephaven documentation questions in natural language.

    Parameters:
        prompt (str):
            The user's query or question for the documentation assistant. Should be a clear, natural language string describing the information sought.
        history (list[dict[str, str]] | None, optional):
            Previous chat messages for context. Each message must be a dict with 'role' ("user" or "assistant") and 'content' (str). Use this to maintain conversational context for follow-up questions.
            Example:
                [
                    {"role": "user", "content": "How do I install Deephaven?"},
                    {"role": "assistant", "content": "To install Deephaven, ..."}
                ]
        deephaven_core_version (str | None, optional):
            The version of Deephaven Community Core installed for the relevant worker. Providing this enables the documentation assistant to tailor its answers for greater accuracy.
        deephaven_enterprise_version (str | None, optional):
            The version of Deephaven Core+ (Enterprise) installed for the relevant worker. Providing this enables the documentation assistant to tailor its answers for greater accuracy.

    Returns:
        str: The assistant's response message answering the user's documentation question. The response is a natural language string, suitable for direct display or further agentic processing.

    Raises:
        OpenAIClientError: If the underlying LLM API call fails or parameters are invalid. The error message will describe the failure reason for agentic error handling.

    Usage Notes:
        - This tool is asynchronous and should be awaited in agentic or orchestration frameworks.
        - The tool is discoverable via MCP server tool registries and can be invoked by name ('docs_chat').
        - For best results, provide relevant chat history for multi-turn conversations.
        - For environment-specific questions, provide Deephaven version information for more accurate answers.
        - Including Deephaven Core and Core+ versions leads to more precise, context-aware responses.
        - Designed for integration with LLM agents, RAG pipelines, chatbots, and automation scripts.

    Example (agentic call):
        >>> response = await docs_chat(
        ...     prompt="How do I install Deephaven?",
        ...     history=[{"role": "user", "content": "Hi"}],
        ...     deephaven_core_version="1.2.3",
        ...     deephaven_enterprise_version="4.5.6"
        ... )
        >>> print(response)
        To install Deephaven, ...
    """

    system_prompts = [
        _prompt_basic,
        _prompt_good_query_strings,
    ]

    # Optionally add version info to prompt context if provided
    if deephaven_core_version:
        system_prompts.append(
            f"Worker environment: Deephaven Community Core version: {deephaven_core_version}"
        )
    if deephaven_enterprise_version:
        system_prompts.append(
            f"Worker environment: Deephaven Core+ (Enterprise) version: {deephaven_enterprise_version}"
        )

    return await inkeep_client.chat(
        prompt=prompt, history=history, system_prompts=system_prompts
    )


__all__ = ["mcp_server"]
