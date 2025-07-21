---
description: Improve pydocs
---

Review the pydocs in this file for correctness, completeness, and clarity.  Be sure to also review the pydocs for the file.  Just change the pydocs, and do not change the source code.  

To make code review easy, make sure there is a reason for any change.  If there isn't a significant improvement from a change, do not make the change to make code reviews easier.

Pydocs should include typehints.

Functions marked as MCP tools (@mcp_server.tool()) will be used by AI agents.  Their documentation should be very detailed and specific to be maximally useful to an AI agent.