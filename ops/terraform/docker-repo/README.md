# Terraform: Docker Artifact Repository

This directory contains Terraform configuration to create and manage a Docker Artifact Registry repository in Google Cloud Platform (GCP). This repository is used to store Docker images for the MCP Docs and related services.

---

## ⚠️ Important Notes
- **This setup is typically a one-time operation.**
- **Do NOT delete the repository unless you are certain!**
  - The repository contains Docker images required for cloud deployments. Deleting it may break production or development environments.

---

## Prerequisites
- [Terraform](https://www.terraform.io/) installed (v1.0+ recommended)
- Google Cloud SDK installed
- Sufficient permissions in GCP to create and manage Artifact Registry repositories
- Authenticated with GCP:
  ```sh
  gcloud auth application-default login
  ```

---

## Usage
This module is intended to be run directly with Terraform, NOT with the helper script. These resources are typically provisioned once per project.

Initialize the working directory:
```sh
terraform init
```

See planned actions:
```sh
terraform plan
```

Apply changes (create the repository):
```sh
terraform apply
```

---

## Viewing the Repository
To view the Docker Artifact Registry in the Google Cloud Console, visit:
- [Google Cloud Console Artifact Repository](https://console.cloud.google.com/artifacts)

---

For more information, see the main project README or contact the maintainers.
