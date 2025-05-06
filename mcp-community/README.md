
#TODO: write readme

DH_MCP_CONFIG_FILE=/Users/chip/dev/test-dh-mcp/deephaven_workers.json uv run dh-mcp-community
uv run dh-mcp-community

https://www.firecrawl.dev/blog/fastmcp-tutorial-building-mcp-servers-python

#TODO: composite service
https://github.com/jlowin/fastmcp

# for debugging
mcp dev <file>
uv run mcp run<file>

PYTHONPATH=src uv run mcp dev src/deephaven_mcp/community/_mcp.py:mcp_server

npx @modelcontextprotocol/inspector@latest \
        uv run dh-mcp-community --config ../mcp-config.json --server mcp-community

npx @modelcontextprotocol/inspector@latest \
        --config ../mcp-config.json --server mcp-community 


uv run ./scripts/run_deephaven_test_server.py --table-group all
DH_MCP_CONFIG_FILE=/Users/chip/dev/test-dh-mcp/deephaven_workers.json uv run dh-mcp-community --transport sse
npx @modelcontextprotocol/inspector@latest

PYTHONPATH=src DH_MCP_CONFIG_FILE=/Users/chip/dev/test-dh-mcp/deephaven_workers.json uv run mcp run src/deephaven_mcp/community/_mcp.py:mcp_server --transport sse


uv run scripts/mcp_test_client.py --transport stdio --env DH_MCP_CONFIG_FILE=/Users/chip/dev/test-dh-mcp/deephaven_workers.json

# install dev dependencies
uv pip install ".[dev]"


You can now use all the development tools. Here are some quick commands:

Run tests: uv pytest
Run type checking: uv mypy .
Run linting: uv ruff .
Format code: uv black .
Sort imports: uv isort .