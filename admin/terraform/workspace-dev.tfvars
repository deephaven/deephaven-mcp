# DO NOT PUT SENSITIVE DATA IN THIS FILE
# This file is committed to the repository.
workspace = "dev"
project_id = "deephaven-oss"
region     = "us-central1"
app = "deephaven-mcp-docs"
dns_managed_zone = "dhc-demo"
image = "us-central1-docker.pkg.dev/deephaven-oss/deephaven-mcp/deephaven-mcp-docs:dev"
min_instance_count = 0
max_instance_count = 10
container_memory = "4Gi"
