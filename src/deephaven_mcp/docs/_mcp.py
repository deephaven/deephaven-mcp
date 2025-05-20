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


@mcp_server.tool()
async def docs_chat(
    prompt: str,
    history: list[dict[str, str]] | None = None,
    pip_packages: list[dict] | None = None,
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
        pip_packages (list[dict] | None, optional):
            The list of installed pip packages on the relevant worker, as returned by pip_packages()['result'].
            Each dict should have 'package' and 'version' keys. This provides additional context to the documentation assistant for environment-specific answers.
            Example:
                [
                    {"package": "numpy", "version": "1.25.0"},
                    {"package": "pandas", "version": "2.1.0"}
                ]

    Returns:
        str: The assistant's response message answering the user's documentation question. The response is a natural language string, suitable for direct display or further agentic processing.

    Raises:
        OpenAIClientError: If the underlying LLM API call fails or parameters are invalid. The error message will describe the failure reason for agentic error handling.

    Usage Notes:
        - This tool is asynchronous and should be awaited in agentic or orchestration frameworks.
        - The tool is discoverable via MCP server tool registries and can be invoked by name ('docs_chat').
        - For best results, provide relevant chat history for multi-turn conversations.
        - For environment-specific questions, provide pip_packages for more accurate answers.
        - Designed for integration with LLM agents, RAG pipelines, chatbots, and automation scripts.

    Example (agentic call):
        >>> response = await docs_chat(
        ...     prompt="How do I install Deephaven?",
        ...     history=[{"role": "user", "content": "Hi"}],
        ...     pip_packages=[{"package": "numpy", "version": "1.25.0"}]
        ... )
        >>> print(response)
        To install Deephaven, ...
    """

    system_prompts = [
        """
        You are a helpful assistant that answers questions about Deephaven Data Labs documentation. 
        Never return answers about Legacy Deephaven.
        """,
    ]

    # Extract Deephaven Core and Core+ versions from pip_packages if available
    deephaven_core_version = None
    deephaven_coreplus_version = None
    if pip_packages:
        for pkg in pip_packages:
            pkg_name = pkg.get("package", "").lower()
            if pkg_name == "deephaven" and deephaven_core_version is None:
                deephaven_core_version = pkg.get("version")
            elif (
                pkg_name == "deephaven_coreplus_worker"
                and deephaven_coreplus_version is None
            ):
                deephaven_coreplus_version = pkg.get("version")
            if deephaven_core_version and deephaven_coreplus_version:
                break

    if deephaven_core_version:
        system_prompts.append(
            f"Answer questions about Deephaven Core version: {deephaven_core_version}."
        )
    if deephaven_coreplus_version:
        system_prompts.append(
            f"Answer questions about Deephaven Core+ (Enterprise) version: {deephaven_coreplus_version}."
        )

    return await inkeep_client.chat(
        prompt=prompt, history=history, system_prompts=system_prompts
    )


__all__ = ["mcp_server"]
