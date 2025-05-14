terraform {
  backend "gcs" {
    bucket = "deephaven-mcp-tfstate"
    prefix = "terraform/state/deephaven-mcp-docs"
  }
}