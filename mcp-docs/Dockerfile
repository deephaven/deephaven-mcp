# ------------------------------------------------------------------------------
# Deephaven MCP Docs Server Dockerfile
#
# This Dockerfile builds a container image for the Deephaven Model Context Protocol
# (MCP) Docs Server. The server provides an API for interacting with documentation
# about Deephaven Data Labs, powered by Inkeep LLM APIs.
#
# IMPORTANT:
#   - The container REQUIRES the INKEEP_API_KEY environment variable to be set at
#     runtime. Without this variable, the server will fail to start.
#     Example:
#         docker run --rm -e INKEEP_API_KEY=your-actual-api-key deephaven-mcp-docs
#   - The server listens on port 8000 by default.
#
# Build:   docker build -t deephaven-mcp-docs .
# Run:     docker run --rm -e INKEEP_API_KEY=your-actual-api-key -p 8000:8000 deephaven-mcp-docs
# ------------------------------------------------------------------------------

# Use the official Python 3.13 slim image for compatibility and minimal size
FROM python:3.13-slim

# Set the working directory for the application code
WORKDIR /app

# Install required build tools, curl, and uv (modern Python package manager and process runner)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && pip install --no-cache-dir uv \
    && uv --version \
    && rm -rf /var/lib/apt/lists/*

# Copy all project files into the container image
COPY . .

# Install Python dependencies using uv for fast and reliable installs
RUN uv venv && uv pip install ".[dev]"

# Expose the default port used by the docs MCP server (8000)
EXPOSE 8000

# Run the docs MCP server using the script entrypoint (dh-mcp-docs) defined in pyproject.toml
CMD ["uv", "run", "dh-mcp-docs"]
