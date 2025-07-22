---
description: Stress test MCP server by calling docs_chat 100 times
---

# MCP Stress Test Workflow

This workflow performs a stress test on the **deephaven-docs-prod-remote** MCP server specifically by making 100 sequential calls to its `docs_chat` tool and displaying all JSON results.

## Steps

1. **Verify deephaven-docs-prod-remote MCP Server Connection**
   - Ensure the deephaven-docs-prod-remote MCP server is connected and responding
   - Test with a single `docs_chat` call from the deephaven-docs-prod-remote server first
   - **IMPORTANT**: Use the docs_chat tool from deephaven-docs-prod-remote, NOT from deephaven-docs-dev-remote

2. **Execute Stress Test on deephaven-docs-prod-remote**
   - Call the `docs_chat` tool from the **deephaven-docs-prod-remote** server with the query: "Write a query to join quotes onto trades."
   - **CRITICAL**: Ensure you're using the docs_chat tool from deephaven-docs-prod-remote server, not deephaven-docs-dev-remote
   - Repeat this call exactly 100 times
   - Display the JSON result from each call
   - Number each call (e.g., "Call 1/100:", "Call 2/100:", etc.)

3. **Monitor for Issues**
   - Watch for connection failures, timeouts, or rate limiting
   - Note any variations in response content or format
   - Track response times if possible

4. **Report Results**
   - Summarize the total number of successful vs failed calls
   - Note any patterns in response variations
   - Report any server errors or connectivity issues

## Expected Behavior

- Each `docs_chat` call from deephaven-docs-prod-remote should return a JSON object with a "content" field
- Content should contain Deephaven query examples for joining quotes to trades
- Responses may vary slightly but should cover similar concepts (as-of joins, aj method, etc.)
- All calls should complete successfully unless there are server issues
- Responses should come from the deephaven-docs-prod-remote server specifically

## Server Identification

- **Target Server**: deephaven-docs-prod-remote (production environment)
- **Alternative Server**: deephaven-docs-dev-remote (development/testing environment) - DO NOT USE for this test
- Both servers have a `docs_chat` tool, but this test specifically targets the dev server
- The actual tool names may vary (e.g., mcp0_docs_chat, mcp1_docs_chat) but the server names are stable

## Troubleshooting

- If deephaven-docs-prod-remote MCP server becomes unavailable, user may need to refresh the connection
- Rate limiting may cause failures - wait and retry if needed
- Connection timeouts indicate server overload or network issues
- If the docs_chat tool from deephaven-docs-prod-remote is not found, verify the server is properly connected
- Do NOT fall back to the docs_chat tool from deephaven-docs-dev-remote - this test is specifically for the dev server
- Check available MCP servers and their tools if there's confusion about which tool to use