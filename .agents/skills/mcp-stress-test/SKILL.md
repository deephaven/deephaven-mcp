---
name: mcp-stress-test
description: Stress test a deephaven-docs MCP server (dev or prod) by calling docs_chat N times sequentially (default N=100)
---

# MCP Stress Test Workflow

This workflow stress tests a deephaven-docs MCP server by making N sequential calls to its `docs_chat` tool.

## Steps

1. **Ask which server to target**
   - Ask the user: dev (`deephaven-docs-dev-remote`) or prod (`deephaven-docs-prod-remote`)?
   - Do not proceed until the user has confirmed the target server.

2. **Confirm prompt and call count**
   - Default prompt: `"Write a query to join quotes onto trades."`
   - Default call count: `100`
   - If the user supplied either as a parameter, use their value; otherwise confirm the defaults.

3. **Verify MCP Server Connection**
   - Ensure the target server is connected and responding
   - Test with a single `docs_chat` call from the target server first

4. **Execute Stress Test**
   - Call the `docs_chat` tool from the target server with the confirmed prompt
   - Repeat the call N times **sequentially** (one at a time, not in parallel)
   - Display the JSON result from each call
   - Number each call (e.g., `Call 1/N:`, `Call 2/N:`)
   - Provide pass/fail status updates as the tests are run

5. **Monitor for Issues**
   - Watch for connection failures, timeouts, or rate limiting
   - Note any variations in response content or format
   - Track response times if possible

6. **Report Results**
   - Summarize the total number of successful vs failed calls
   - Note any patterns in response variations
   - Report any server errors or connectivity issues

## Expected Behavior

- Each `docs_chat` call should return a non-error JSON object with a `content` field.
- When using the **default prompt** (`"Write a query to join quotes onto trades."`), the content should contain Deephaven query examples covering as-of joins / the `aj` method.
- When using a **custom prompt**, success means a non-error JSON response with a `content` field — the actual content depends on the prompt, and content variation across calls is normal.
- All calls should complete successfully unless there are server issues.

## Server Identification

- **Dev server**: `deephaven-docs-dev-remote` (development/testing environment)
- **Prod server**: `deephaven-docs-prod-remote` (production environment)
- Both servers expose a `docs_chat` tool — always confirm you are calling it on the intended server.

## Troubleshooting

- If the target MCP server becomes unavailable, the user may need to refresh the connection
- Rate limiting may cause failures — wait and retry if needed
- Connection timeouts indicate server overload or network issues
- If the `docs_chat` tool is not found, verify the server is properly connected
- Check available MCP servers and their tools if there is confusion about which tool to use
