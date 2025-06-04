# Ops Directory: Infrastructure, Deployment, and Operations

This directory contains all operational assets for the Deephaven MCP project, including infrastructure-as-code (Terraform), Dockerfiles, deployment scripts, and automation tools. It is the central hub for provisioning, updating, and maintaining cloud resources and containerized applications.

---

## Table of Contents
- [Overview](#overview)
- [Directory Structure](#directory-structure)
- [Prerequisites](#prerequisites)
- [Environment Configuration](#environment-configuration)
- [Security Best Practices](#security-best-practices)
- [Terraform Workflow](#terraform-workflow)
  - [Using `run_terraform.sh`](#using-run_terraformsh)
  - [Code Quality and Validation](#code-quality-and-validation)
  - [Remote State and Locking](#remote-state-and-locking)
  - [Workspaces](#workspaces)
  - [Module Details](#module-details)
- [Docker Workflow](#docker-workflow)
  - [Building Images](#building-images)
  - [Pushing Images to a Registry](#pushing-images-to-a-registry)
  - [Running Containers Locally](#running-containers-locally)
- [CI/CD Integration](#cicd-integration)
- [Troubleshooting](#troubleshooting)
- [References & Links](#references--links)

---

## Overview
The `ops/` directory is the single source of truth for all infrastructure, deployment, and operational automation for the Deephaven MCP project. It centralizes Docker, Terraform, and supporting scripts to ensure consistent and reproducible management of cloud and local environments.

## Directory Structure
```
ops/
  docker/           # Dockerfiles & Compose files per service
    mcp-docs/       # Example: MCP Docs service
      Dockerfile
      docker-compose.yml
      README.md     # Service-specific Docker instructions
    ...             # Other services
  terraform/        # Terraform modules for GCP (or other cloud) resources
    backend-bucket/ # Manages Terraform remote state GCS bucket
    docker-repo/    # Manages Docker Artifact Registry
    mcp-docs/       # Manages MCP Docs cloud infrastructure
    ...             # Other infrastructure modules
  run_terraform.sh  # Unified helper script for workspace-aware Terraform operations
  README.md         # This file: guidance for the ops directory
```

## Prerequisites
Before using the assets in this directory, ensure you have:
1.  **Cloned the Repository:** You need the entire `deephaven-mcp` project.
2.  **Installed Core Dependencies:** Follow the setup instructions in the main project [`README.md`](../README.md) (e.g., `uv`).
3.  **Terraform CLI:** Install the [Terraform CLI](https://learn.hashicorp.com/tutorials/terraform/install-cli) (version specified in `ops/terraform/**/versions.tf` or project docs).
4.  **Docker:** Install [Docker Desktop](https://www.docker.com/products/docker-desktop) or Docker Engine.
5.  **Cloud SDK (Optional but Recommended):** For direct cloud provider interactions (e.g., [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)). Authenticate as needed.

## Environment Configuration
Sensitive information and environment-specific variables are managed via a `.env` file and CI/CD secrets.

-   **Local Development (`.env` file):**
    1.  Create a `.env` file in the **project root** (i.e., `deephaven-mcp/.env`).
    2.  You can copy [`../.env.example`](../.env.example) (if it exists) as a template.
    3.  Populate it with necessary secrets. A common required variable for deployments is:
        ```
        INKEEP_API_KEY=your-inkeep-api-key-here
        ```
    4.  **Never commit your `.env` file to version control.** It is listed in [`../.gitignore`](../.gitignore).
-   **CI/CD (GitHub Actions):**
    -   Secrets (like `INKEEP_API_KEY`, `GH_ACTION_GCLOUD_JSON`) are stored as [GitHub Actions Secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets).
    -   Workflows in [`.github/workflows/`](../.github/workflows/) consume these secrets and may generate temporary `.env` files for scripts that require them.

## Security Best Practices
-   **Never Commit Sensitive Data:** Ensure `.env` files, service account keys, and any local Terraform state files (e.g., `terraform.tfstate`, `terraform.tfstate.backup`) are **never** committed to version control. While this project is configured to use remote state for Terraform, local state files might be generated during certain operations (e.g., if the backend is misconfigured or during initial local testing before remote state is established). These files **must not** be committed. Ensure your project's root [`../.gitignore`](../.gitignore) correctly lists `*.tfstate*` (which covers files like `terraform.tfstate` and `terraform.tfstate.backup`) to prevent accidental commits; this should be standard, but verification is crucial.
-   **Use GitHub Secrets for CI/CD:** Store all sensitive data required for CI/CD pipelines as encrypted secrets in GitHub.
-   **Least Privilege Principle:** When creating service accounts (e.g., for GCP), grant only the minimum necessary permissions required for their tasks. Review permissions regularly.
-   **Regularly Rotate Credentials:** If manual credential management is used (e.g., for service accounts not using Workload Identity Federation, or API keys), establish a process for regular rotation.
-   **Secure Remote State:** Ensure your Terraform remote state backend (e.g., GCS bucket) has appropriate access controls (e.g., restricted IAM permissions, bucket policies) and utilizes locking mechanisms to prevent concurrent modifications and state corruption.

## Terraform Workflow
All Terraform operations for managing cloud infrastructure should be performed using the provided helper script.

### Using `run_terraform.sh`
The [`run_terraform.sh`](run_terraform.sh) script is the unified entry point for executing Terraform commands. These operations are generally idempotent, meaning they can be run multiple times with the same outcome if the desired state is already achieved. It handles:
-   Loading environment variables from the project's `.env` file.
-   Terraform workspace selection and management.
-   Interfacing with the configured remote state backend.
-   Targeting operations to the correct module directory within `ops/terraform/`.
-   Operating from within the selected module's directory for Terraform execution.
-   Passing necessary variables to Terraform.

**Usage:**
The script should be run from the **project root directory**.
```sh
./ops/run_terraform.sh <workspace> <terraform_command> <module_name> [additional_terraform_options]
```
-   `<workspace>`: The Terraform workspace to operate on (e.g., `dev`, `prod`).
-   `<terraform_command>`: The Terraform CLI command (e.g., `init`, `plan`, `apply`, `validate`, `fmt`).
-   `<module_name>`: The name of the Terraform module directory within `ops/terraform/` (e.g., `mcp-docs`, `backend-bucket`).
-   `[additional_terraform_options]`: Any other options to pass directly to the Terraform CLI command.

**Examples:**
```sh
# Initialize Terraform for the 'mcp-docs' module in the 'dev' workspace
./ops/run_terraform.sh dev init mcp-docs

# Plan changes for the 'mcp-docs' module in the 'dev' workspace
./ops/run_terraform.sh dev plan mcp-docs

# Apply changes for the 'docker-repo' module in the 'prod' workspace, with an auto-approve flag
./ops/run_terraform.sh prod apply docker-repo -auto-approve

# Validate the 'backend-bucket' module configuration in the 'dev' workspace
./ops/run_terraform.sh dev validate backend-bucket
```
-   For detailed script behavior and more options, consult the script's help output (e.g., by running `./ops/run_terraform.sh help` or examining the script itself).

### Code Quality and Validation
It's good practice to regularly format and validate your Terraform code:
-   **`terraform fmt`**: Use this command (e.g., via `run_terraform.sh <ws> fmt <module>`) to automatically format your `.tf` files for readability and consistency.
-   **`terraform validate`**: Use this command (e.g., via `run_terraform.sh <ws> validate <module>`) to check your configuration for syntax errors and internal consistency before planning or applying changes.
CI/CD pipelines may also enforce these checks.

### Remote State and Locking
This project utilizes Terraform's remote state capabilities, typically configured with a Google Cloud Storage (GCS) backend. This approach offers:
-   **Shared State:** Allows team members to collaborate by accessing a single, consistent state of the infrastructure.
-   **State Locking:** Prevents concurrent `apply` operations, reducing the risk of state corruption.
The [`run_terraform.sh`](run_terraform.sh) script and individual Terraform module configurations (e.g., in `backend.tf` files) are set up to use this remote backend. Ensure the backend (e.g., GCS bucket) is provisioned and accessible as per the module instructions (e.g., [`ops/terraform/backend-bucket/README.md`](terraform/backend-bucket/README.md)).

### Workspaces
Terraform workspaces are used to manage multiple, distinct environments (e.g., `dev`, `staging`, `prod`) with the same infrastructure configuration, all while using the same remote state backend (each workspace gets its own state file within the backend). The `run_terraform.sh` script typically expects the workspace name as its first argument.

### Module Details
Detailed information about specific Terraform configurations (variables, outputs, specific setup, backend details) can be found in the `README.md` file within each module's subdirectory under [`ops/terraform/`](terraform/).
-   Example: [`ops/terraform/mcp-docs/README.md`](terraform/mcp-docs/README.md)

## Docker Workflow
This directory houses Dockerfiles and related assets for containerizing services.

### Building Images
-   Dockerfiles are located in `ops/docker/<service_name>/Dockerfile`.
-   Build context is typically the project root (`.`) to allow access to all project files.
-   **Example (MCP Docs):**
    ```sh
    # From the project root
    docker build -f ops/docker/mcp-docs/Dockerfile -t mcp-docs:latest .
    ```
-   Refer to the specific `README.md` in each `ops/docker/<service_name>/` directory for detailed build instructions and available build arguments.

### Pushing Images to a Registry
After building and tagging an image, you'll typically push it to a container registry (e.g., Google Artifact Registry, Docker Hub) so it can be pulled for deployment.
-   **Example (Conceptual):**
    ```sh
    docker push your-registry-host/your-project/mcp-docs:latest
    ```
-   The exact commands, registry host, and image naming conventions will depend on your project's setup (e.g., the registry provisioned by `ops/terraform/docker-repo/`).
-   Authentication with the registry (`docker login`) is usually required.
-   See service-specific READMEs or CI/CD workflow configurations for precise instructions.

### Running Containers Locally
-   `docker-compose.yml` files may be provided in `ops/docker/<service_name>/` for easy local orchestration.
-   **Example (MCP Docs, if Compose file exists):**
    ```sh
    # From the project root
    docker compose -f ops/docker/mcp-docs/docker-compose.yml up
    ```
-   Consult the service-specific `README.md` (e.g., [`ops/docker/mcp-docs/README.md`](docker/mcp-docs/README.md)) for instructions on running containers, required environment variables, and port mappings.

## CI/CD Integration
[GitHub Actions workflows](../.github/workflows/) heavily utilize the scripts and configurations within this `ops/` directory to:
-   Build Docker images and push them to an artifact registry (e.g., Google Artifact Registry).
-   Run Terraform to provision or update cloud infrastructure.
-   Deploy services to hosting platforms (e.g., Google Cloud Run).

Key workflows to review include:
-   [`docker-mcp-docs.yml`](../.github/workflows/docker-mcp-docs.yml) (example for building and deploying a service)

## Troubleshooting
*(This section can be populated with common issues and their resolutions as they are identified.)*
-   **Check Detailed Logs:** Always start by examining the full output logs from the script or tool you are using (e.g., `run_terraform.sh`, `docker build`, CI/CD job logs). Verbose flags can often provide more insight.
-   **Verify Cloud Provider Authentication and Context:** Ensure your local CLI or CI/CD environment is authenticated with the correct cloud provider account/project and region. For GCP, check `gcloud config list`. For AWS, check `aws sts get-caller-identity`.
-   **Permission Denied (Terraform/Docker/GCP):** Ensure your local user or CI/CD service account has the necessary IAM permissions for the Google Cloud project, GCS buckets, Docker daemon, etc.
-   **`.env` file not found:** Make sure your `.env` file is in the project root and correctly named.
-   **Terraform state lock issues:** Usually resolved by waiting if another operation is in progress. If a lock persists due to an incomplete operation, manual intervention might be needed (see Terraform documentation on state locking and the `force-unlock` command, use with extreme caution).
-   **Terraform Init Fails (Backend Issues):** Verify the GCS bucket for remote state exists and your credentials provide access. Check the [`ops/terraform/backend-bucket/README.md`](terraform/backend-bucket/README.md) for setup details.

## References & Links
-   **Project Documentation:**
    -   Main Project [`README.md`](../README.md)
    -   Project [`DEVELOPER_GUIDE.md`](../docs/DEVELOPER_GUIDE.md)
-   **External Documentation:**
    -   [Terraform Documentation](https://www.terraform.io/docs/)
    -   [Terraform GCS Backend](https://developer.hashicorp.com/terraform/language/settings/backends/gcs)
    -   [Docker Documentation](https://docs.docker.com/)
    -   [Google Cloud Documentation](https://cloud.google.com/docs)
    -   [GitHub Actions Documentation](https://docs.github.com/en/actions)
-   **Service/Module Specifics:**
    -   See `README.md` files within subdirectories of [`ops/terraform/`](terraform/) and [`ops/docker/`](docker/) for detailed instructions.
