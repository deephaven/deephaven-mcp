# Deephaven MCP Docs Server

This package provides an MCP-compatible server for interacting with documentation about Deephaven Data Labs. It exposes a single tool that allows users to chat with an OpenAI-powered assistant to learn about Deephaven documentation.

## Features
- MCP server modeled after the `mcp-community` implementation
- Single tool: `chat_docs` — chat with an OpenAI AI about Deephaven Data Labs documentation

## Setup
1. Install dependencies: `pip install -e .[dev]`
2. Set your OpenAI API key in the environment: `export OPENAI_API_KEY=...`
3. Run the server: `python -m deephaven_mcp`

## Configuration
- The OpenAI API key must be set in the environment as `OPENAI_API_KEY`.
- The documentation context can be extended by modifying the tool implementation.

## License
Apache-2.0

#TODO: update docs