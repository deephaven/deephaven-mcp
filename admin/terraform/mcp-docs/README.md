# MCP Docs Terraform

This directory contains the Terraform configuration to deploy all resources required for the MCP Docs service on Google Cloud Platform (GCP).

## Features
- Deploys and manages infrastructure for the MCP Docs service
- Supports multiple environments (workspaces) such as `dev` and `prod`
- Stores Terraform state securely in a Google Cloud Storage bucket

## Prerequisites
- [Terraform](https://www.terraform.io/) installed (v1.0+ recommended)
- Google Cloud SDK installed and authenticated (`gcloud auth application-default login`)
- Sufficient permissions in GCP for resource creation (see project documentation)
- `.env` file at the project root with required secrets (see below)

## State Storage
Terraform state is stored remotely in a GCS bucket for safety and collaboration. The backend configuration is managed in this directory.

## Usage
All Terraform commands should be run via the helper script for consistency and workspace management:

```sh
# From the project root
tools:
  ./admin/run_terraform.sh <workspace> <command>
```

Examples:
- Initialize Terraform for the `dev` workspace:
  ```sh
  ./admin/run_terraform.sh dev init
  ```
- Apply infrastructure changes for the `prod` workspace:
  ```sh
  ./admin/run_terraform.sh prod apply
  ```

The script manages workspace selection and variable files automatically.

## Environment Variables
The following secrets are required for deployment. Locally, create a `.env` file at the project root with:

```
INKEEP_API_KEY=your-inkeep-api-key-here
```

In CI/CD (GitHub Actions), the key is injected automatically from repository secrets.

## Directory Structure
- `admin/terraform/mcp-docs/` - This directory (Terraform configs for MCP Docs)
- `admin/run_terraform.sh` - Helper script for all Terraform operations
- `.env` - Secrets file (project root, **never commit to git**)

## References
- [Terraform Documentation](https://www.terraform.io/docs/)
- [Google Cloud Terraform Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

---
For more details or troubleshooting, see the main project README or contact the maintainers.

