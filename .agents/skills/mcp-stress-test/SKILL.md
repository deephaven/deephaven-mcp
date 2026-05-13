---
name: mcp-stress-test
description: Stress test a deephaven-docs MCP server (dev or prod) by calling docs_chat 100 times sequentially
---

# MCP Stress Test Workflow

This workflow stress tests a deephaven-docs MCP server by making 100 sequential calls to its `docs_chat` tool.

## Steps

1. **Ask which server to target**
   - Ask the user: dev (`deephaven-docs-dev-remote`) or prod (`deephaven-docs-prod-remote`)?
   - Do not proceed until the user has confirmed the target server.

2. **Verify MCP Server Connection**
   - Ensure the target server is connected and responding
   - Test with a single `docs_chat` call from the target server first

3. **Execute Stress Test**
   - Call the `docs_chat` tool from the target server with the query: "Write a query to join quotes onto trades."
   - Repeat this call exactly 100 times **sequentially** (one at a time, not in parallel)
   - Display the JSON result from each call
   - Number each call (e.g., "Call 1/100:", "Call 2/100:", etc.)
   - Provide pass/fail status updates as the tests are run

4. **Monitor for Issues**
   - Watch for connection failures, timeouts, or rate limiting
   - Note any variations in response content or format
   - Track response times if possible

5. **Report Results**
   - Summarize the total number of successful vs failed calls
   - Note any patterns in response variations
   - Report any server errors or connectivity issues

## Expected Behavior

- Each `docs_chat` call should return a JSON object with a "content" field
- Content should contain Deephaven query examples for joining quotes to trades
- Responses may vary slightly but should cover similar concepts (as-of joins, aj method, etc.)
- All calls should complete successfully unless there are server issues

## Server Identification

- **Dev server**: `deephaven-docs-dev-remote` (development/testing environment)
- **Prod server**: `deephaven-docs-prod-remote` (production environment)
- Both servers have a `docs_chat` tool — always confirm you are using the tool from the correct server
- The actual tool names may vary (e.g., `mcp0_docs_chat`, `mcp1_docs_chat`) but the server names are stable

## Troubleshooting

- If the target MCP server becomes unavailable, the user may need to refresh the connection
- Rate limiting may cause failures — wait and retry if needed
- Connection timeouts indicate server overload or network issues
- If the `docs_chat` tool is not found, verify the server is properly connected
- Check available MCP servers and their tools if there is confusion about which tool to use
