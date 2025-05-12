"""
Deephaven Model Context Protocol (MCP) Docs Edition

This package provides a Python implementation of the Deephaven MCP protocol for
learning about Deephaven Data Labs documentation using LLM-powered tools.

Modules:
    - docs: Main MCP implementation for documentation Q&A and chat

To run a Deephaven MCP Docs server, use the `run_server` function from the `docs` module.
"""

# Import version from _version.py
# Initialize logging
import logging

from ._version import version as __version__

__all__ = ["__version__"]

_LOGGER = logging.getLogger(__name__)
_LOGGER.addHandler(logging.NullHandler())

