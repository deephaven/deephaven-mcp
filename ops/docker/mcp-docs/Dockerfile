# ------------------------------------------------------------------------------
# Deephaven MCP Docs Server Dockerfile
#
# Builds a container image for the Deephaven Model Context Protocol (MCP) Docs Server.
# The server provides an API for interacting with documentation about Deephaven Data Labs,
# powered by Inkeep LLM APIs.
#
# REQUIREMENTS:
#   - The INKEEP_API_KEY environment variable must be set at runtime.
#     - Locally: via .env file or docker-compose.yml (see docker/mcp-docs/README.md)
#     - In CI/CD: via GitHub secret
#   - The server listens on port 8000 by default.
#
# BUILD CONTEXT:
#   - Build context must be the repository root (.) so all code/assets are available.
#
# IMAGE NAME:
#   - Standard image/tag: mcp-docs:latest (see Compose and workflow)
#
# USAGE:
#   - Build:   docker build -f docker/mcp-docs/Dockerfile -t mcp-docs:latest .
#   - Run:     docker run --rm -e INKEEP_API_KEY=your-actual-api-key -p 8000:8000 mcp-docs:latest
#   - Orchestration: see docker/mcp-docs/docker-compose.yml for multi-service/local dev
#   - In CI/CD, see .github/workflows/docker-mcp-docs.yml
#
# ENTRYPOINT:
#   - The default container entrypoint runs the docs server using the 'dh-mcp-docs' script
#     (see pyproject.toml) with SSE transport.
# ------------------------------------------------------------------------------

# Use the official Python slim image for compatibility and minimal size
FROM python:3.12-slim

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
# Only install production dependencies
RUN uv venv && uv pip install "."

# Always bind the docs server to all interfaces by default
ENV MCP_DOCS_HOST=0.0.0.0

# Run the docs MCP server using the script entrypoint (dh-mcp-docs) defined in pyproject.toml
CMD ["uv", "run", "dh-mcp-docs-server", "--transport", "streamable-http"]
