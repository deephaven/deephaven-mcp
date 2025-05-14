# Admin Directory: Infrastructure & Deployment Management

This directory contains all infrastructure-as-code (IaC) and deployment scripts for the Deephaven MCP project. It is the central hub for provisioning, updating, and maintaining cloud resources using Terraform and related tooling.

---

## Table of Contents
- [Overview](#overview)
- [Directory Structure](#directory-structure)
- [Environment Configuration](#environment-configuration)
- [Terraform Workflow](#terraform-workflow)
- [Scripts](#scripts)
- [CI/CD Integration](#cicd-integration)
- [Onboarding: Step-by-Step](#onboarding-step-by-step)
- [Security Best Practices](#security-best-practices)
- [References & Links](#references--links)

---

## Overview
This directory enables reproducible infrastructure management for all cloud resources supporting MCP. It includes:
- Terraform modules for GCP resources ([backend-bucket](terraform/backend-bucket/), [Docker Artifact Registry](terraform/docker-repo/), [MCP Docs infra](terraform/mcp-docs/))
- Scripts for safe and repeatable Terraform operations ([run_terraform.sh](run_terraform.sh))
- Documentation for onboarding and best practices ([backend-bucket/README.md](terraform/backend-bucket/README.md), [docker-repo/README.md](terraform/docker-repo/README.md), [mcp-docs/README.md](terraform/mcp-docs/README.md))

---

## Directory Structure

```
admin/
├── README.md                 # This file
├── run_terraform.sh          # Main script for running Terraform operations
└── terraform/
    ├── backend-bucket/       # GCS bucket for remote Terraform state
    │   └── README.md
    ├── docker-repo/          # GCP Artifact Registry for Docker images
    │   └── README.md
    └── mcp-docs/             # Main MCP Docs infrastructure
        └── README.md
```

- **[backend-bucket/](terraform/backend-bucket/)**: Provision the bucket for remote state. Run once per project. See [backend-bucket/README.md](terraform/backend-bucket/README.md)
- **[docker-repo/](terraform/docker-repo/)**: Create the Docker Artifact Registry. Run once per project. See [docker-repo/README.md](terraform/docker-repo/README.md)
- **[mcp-docs/](terraform/mcp-docs/)**: Main infrastructure for the MCP Docs service. Supports multiple workspaces (dev, prod, etc). See [mcp-docs/README.md](terraform/mcp-docs/README.md)
- **[run_terraform.sh](run_terraform.sh)**: Wrapper script for workspace-aware Terraform operations ([see below](#scripts)).

---

## Environment Configuration

Sensitive configuration is managed via a `.env` file in the project root (outside [`admin/`](./)). Example:

```
INKEEP_API_KEY=your-inkeep-api-key-here
```

- **Never commit `.env` to version control!**
- In CI/CD, secrets are injected automatically from GitHub repository secrets.

---

## Terraform Workflow

### 1. Backend Bucket
> **⚠️ WARNING:** This step should only be performed by experts who fully understand remote state management in Terraform. Creating or modifying the backend bucket can have major consequences for all infrastructure management. If in doubt, consult a project maintainer.
- Used for remote state storage.
- Run directly with Terraform:
  ```sh
  cd admin/terraform/backend-bucket
  terraform init
  terraform plan
  terraform apply
  ```
- See [backend-bucket/README.md](terraform/backend-bucket/README.md)

### 2. Docker Artifact Registry
> **⚠️ WARNING:** This step should only be performed by experts. The Artifact Registry is critical for storing Docker images. Deleting or recreating it can break deployments and cause data loss. Only proceed if you are certain of the implications.
- Used to store Docker images for MCP Docs and related services.
- Run directly with Terraform:
  ```sh
  cd admin/terraform/docker-repo
  terraform init
  terraform plan
  terraform apply
  ```
- See [docker-repo/README.md](terraform/docker-repo/README.md)

### 3. MCP Docs Infrastructure
- Supports multiple workspaces (e.g., dev, prod).
- Use the helper script for all operations:
  ```sh
  ./admin/run_terraform.sh <workspace> <command>
  # Example:
  ./admin/run_terraform.sh dev apply
  ```
- See [mcp-docs/README.md](terraform/mcp-docs/README.md)

---

## Scripts

### run_terraform.sh
- Handles environment loading, workspace selection, and command validation.
- Loads `.env` from the project root.
- Usage:
  ```sh
  ./admin/run_terraform.sh <workspace> <command>
  # Commands: init, plan, apply, destroy, etc.
  ```
- Run from the project root for best results.
- See inline help: `./admin/run_terraform.sh help`

---

## CI/CD Integration

- GitHub Actions workflows (see [`.github/workflows/`](../.github/workflows/)) automate builds, tests, and deployments.
- The workflow creates a `.env` file from repository secrets before running deployment scripts.
- The deploy step uses [`admin/run_terraform.sh`](run_terraform.sh) for workspace-aware deployments.

---

## Onboarding: Step-by-Step

1. **Clone the repository**
2. **Install prerequisites:**
   - [Terraform](https://www.terraform.io/downloads.html)
   - [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
3. **Authenticate with GCP:**
   ```sh
   gcloud auth application-default login
   ```
4. **Set up your `.env` file** at the project root with required secrets.
5. **Run infrastructure operations** for MCP Docs using [`run_terraform.sh`](run_terraform.sh).
6. **Commit and push any configuration changes** (never commit `.env` or state files).

---

## Security Best Practices
- Do **not** commit secrets (e.g., `.env`) or Terraform state files to git.
- Restrict permissions for Google Cloud service accounts and Artifact Registry.
- Rotate API keys and service account credentials regularly.
- Use remote state with locking (GCS backend) to avoid concurrent modification issues.
- Use GitHub Secrets for CI/CD.

---

## References & Links
- [Terraform Documentation](https://www.terraform.io/docs/)
- [Google Cloud Terraform Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Storing Terraform State in Google Cloud](https://cloud.google.com/docs/terraform/resource-management/store-state)
- [Terraform GCS Backend](https://developer.hashicorp.com/terraform/language/settings/backends/gcs)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

---

For more details, see the subdirectory READMEs or contact the project maintainers.
