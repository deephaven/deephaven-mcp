# Docker Assets Directory

This directory contains all Docker-related assets for the Deephaven MCP project. Each service with Docker support has its own subdirectory under `ops/docker/` for clarity and maintainability.

## Structure & Conventions

- Each service (e.g., `mcp-docs`, `mcp-systems`) should have its own subdirectory containing:
  - `Dockerfile` — Build instructions for the service's container image
  - `docker-compose.yml` — (Optional) Compose file for local orchestration
  - `README.md` — (Optional) Service-specific Docker usage notes

Example:

```text
/ops/docker/
  mcp-docs/
    Dockerfile
    docker-compose.yml
    README.md
  mcp-systems/
    Dockerfile
    docker-compose.yml
    README.md
```

## MCP Docs Server Example

See [`mcp-docs/README.md`](mcp-docs/README.md) for details on building and running the MCP Docs server container.

## Adding a New Service

1. Create a new subdirectory under `/ops/docker/` (e.g., `/ops/docker/my-service/`).
2. Add your `Dockerfile`, optional `docker-compose.yml`, and documentation.
3. Update CI/CD workflows as needed to reference your new Docker assets.
