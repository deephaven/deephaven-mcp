"""
Deephaven Model Context Protocol (MCP)

This package provides unified Python implementations of the Deephaven Model Context Protocol (MCP) servers, supporting both:

- **Community Core MCP Server:** Orchestrates and manages connections to Deephaven Community Core workers, enabling AI agents and tools to interact with live data tables and computations.
- **Docs MCP Server:** Provides a natural language Q&A assistant for Deephaven documentation, leveraging LLMs to answer user and agent queries.

Key Modules:
    - config: Configuration management for server and worker setup
    - community: Community Core MCP server implementation and CLI entrypoint
    - docs: Documentation Q&A MCP server implementation and CLI entrypoint
    - openai: Utilities for interacting with OpenAI-compatible LLM APIs

Usage:
    - To run a Community Core MCP server, use the `run_server` function from the `community` module or the `dh-mcp-systems` CLI entrypoint.
    - To run a Docs Q&A MCP server, use the `run_server` function from the `docs` module or the `dh-mcp-docs` CLI entrypoint (if enabled in your installation).

See the project README for setup, configuration, and usage details for each server.
"""

# Import version from _version.py
# Initialize logging
import logging

from ._version import version as __version__

__all__ = ["__version__"]

_LOGGER = logging.getLogger(__name__)
_LOGGER.addHandler(logging.NullHandler())
