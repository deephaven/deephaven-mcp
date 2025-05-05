"""
mcp_client.py

Async Python client for discovering and calling all tools on an MCP (Model Context Protocol) server using SSE.

- Connects to a running MCP server via SSE endpoint and lists available tools.
- Demonstrates how to call each tool registered on the server, using appropriate or sample arguments for each tool.
- Requires `autogen-ext[mcp]` to be installed.

Edit the `url` and `headers` parameters as needed for your server configuration.

Example:
    $ PYTHONPATH=./src python mcp-community/scripts/mcp_test_client.py

This script will:
    - Connect to the MCP server
    - List all available tools
    - Call each tool with the correct or sample arguments (as defined in _mcp.py)
    - Print the results or errors for each tool invocation

See the project README for further details.
"""

#TODO: *** is this needed with the "mcp dev" command?

import asyncio
from autogen_core import CancellationToken
from autogen_ext.tools.mcp import SseServerParams, mcp_server_tools

async def main():
    """
    Connects to the MCP server, lists available tools, and demonstrates invocation of all tools.

    - Establishes a connection to the MCP server using SSE.
    - Lists all registered tools on the server.
    - Calls each tool with correct or sample arguments based on its definition in _mcp.py.
    - Prints the results or errors for each tool invocation.
    - Modify the tool arguments as needed for your server setup or data.
    """
    # Set up server params for your MCP SSE server
    server_params = SseServerParams(
        url="http://localhost:8000/sse",  # Adjust endpoint as needed
        headers={"Authorization": "Bearer YOUR_TOKEN"}  # Optional
    )

    # Get all available tools (this also establishes the connection)
    tools = await mcp_server_tools(server_params)

    # List all tools
    print("Available tools:", [t.name for t in tools])

    # Build a map for tool lookup by name
    tool_map = {t.name: t for t in tools}

    # 1. refresh
    print("\nCalling tool: refresh")
    if 'refresh' in tool_map:
        try:
            result = await tool_map['refresh'].run_json({}, cancellation_token=CancellationToken())
            print(f"Result for refresh: {result}")
        except Exception as e:
            print(f"Error calling refresh: {e}")

    # 2. default_worker
    print("\nCalling tool: default_worker")
    if 'default_worker' in tool_map:
        try:
            result = await tool_map['default_worker'].run_json({}, cancellation_token=CancellationToken())
            print(f"Result for default_worker: {result}")
        except Exception as e:
            print(f"Error calling default_worker: {e}")

    # 3. worker_names
    print("\nCalling tool: worker_names")
    if 'worker_names' in tool_map:
        try:
            result = await tool_map['worker_names'].run_json({}, cancellation_token=CancellationToken())
            print(f"Result for worker_names: {result}")
        except Exception as e:
            print(f"Error calling worker_names: {e}")

    # 4. table_schemas (call with no args, then with sample args)
    print("\nCalling tool: table_schemas (no args)")
    if 'table_schemas' in tool_map:
        try:
            result = await tool_map['table_schemas'].run_json({}, cancellation_token=CancellationToken())
            print(f"Result for table_schemas (no args): {result}")
        except Exception as e:
            print(f"Error calling table_schemas (no args): {e}")
        # Try with sample args
        print("\nCalling tool: table_schemas (sample args)")
        try:
            result = await tool_map['table_schemas'].run_json({"worker_name": "local", "table_names": ["MyTable"]}, cancellation_token=CancellationToken())
            print(f"Result for table_schemas (sample args): {result}")
        except Exception as e:
            print(f"Error calling table_schemas (sample args): {e}")

    # 5. run_script (must provide script or script_path)
    print("\nCalling tool: run_script (with script)")
    if 'run_script' in tool_map:
        try:
            result = await tool_map['run_script'].run_json({"script": "print('hello world')"}, cancellation_token=CancellationToken())
            print(f"Result for run_script: {result}")
        except Exception as e:
            print(f"Error calling run_script: {e}")

if __name__ == "__main__":
    asyncio.run(main())
