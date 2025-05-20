# Docker Assets for MCP Docs Server

This directory contains the Dockerfile and docker-compose.yml for building and running the MCP Docs server container.

- [`Dockerfile`](Dockerfile): Build instructions for the MCP Docs server image.
- [`docker-compose.yml`](docker-compose.yml): Compose file for local orchestration of the MCP Docs server (and optional dependencies).

## Usage

### Build the Docker Image
```
docker build -f docker/mcp-docs/Dockerfile -t mcp-docs:latest .
```

### Run with Docker Compose
```
docker compose -f docker/mcp-docs/docker-compose.yml up
```

## Notes
- The build context is the repo root, so all code/assets are accessible to the Dockerfile.
- Update the GitHub Actions workflow to use [`docker/mcp-docs/Dockerfile`](Dockerfile).
