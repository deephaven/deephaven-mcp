
#TODO: write readme

DH_MCP_CONFIG_FILE=/Users/chip/dev/test-dh-mcp/deephaven_workers.json uv run dh-mcp-community
uv run dh-mcp-community

https://www.firecrawl.dev/blog/fastmcp-tutorial-building-mcp-servers-python

#TODO: composite service
https://github.com/jlowin/fastmcp

# for debugging
mcp dev <file>

# install dev dependencies
uv pip install ".[dev]"


You can now use all the development tools. Here are some quick commands:

Run tests: uv pytest
Run type checking: uv mypy .
Run linting: uv ruff .
Format code: uv black .
Sort imports: uv isort .