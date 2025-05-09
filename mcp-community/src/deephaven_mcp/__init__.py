"""
Deephaven Model Context Protocol (MCP) Community Edition

This package provides a Python implementation of the Deephaven MCP protocol for
interacting with Deephaven Community Core workers. It includes tools for managing
worker configurations, establishing connections, and executing operations on
Deephaven workers.

Modules:
    - config: Configuration management for Deephaven MCP servers
    - community: Main MCP implementation for Deephaven Community Core

To run a Deephaven MCP server, use the `run_server` function from the `community` module.
"""

# Import version from _version.py
# Initialize logging
import logging

from ._version import version as __version__

__all__ = ["__version__"]

_LOGGER = logging.getLogger(__name__)
_LOGGER.addHandler(logging.NullHandler())
