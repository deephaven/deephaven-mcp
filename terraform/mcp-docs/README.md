# MCP Docs Terraform

This Terraform directory creates all of the necessary resources for the MCP Docs.

The Terraform state is stored in a Google Cloud Storage bucket.

To use this Terraform directory, use [run_terraform.sh](../../run_terraform.sh) to run the Terraform commands.  The script manages configuring and using workspaces, which allow multiple deployments to be easily supported.

