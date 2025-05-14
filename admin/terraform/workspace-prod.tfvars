# DO NOT PUT SENSITIVE DATA IN THIS FILE
# This file is committed to the repository.
workspace = "prod"
project_id = "deephaven-oss"
region     = "us-central1"
app = "deephaven-mcp-docs"
dns_managed_zone = "dhc-demo"
image = "us-central1-docker.pkg.dev/deephaven-oss/deephaven-mcp/deephaven-mcp-docs:prod"
min_instance_count = 1
max_instance_count = 100
container_memory = "4Gi"