# Terraform: Backend Bucket (GCS)

This directory contains Terraform configuration to provision a Google Cloud Storage (GCS) bucket for storing Terraform state remotely. This bucket is critical for tracking infrastructure deployed via Terraform and supporting collaboration across your team.

---

## ⚠️ Important Notes

- **This setup is typically a one-time operation per project.**
- **Do NOT delete the backend bucket unless you are absolutely certain!**
  - The bucket contains the Terraform state, which is required to manage and update cloud resources. Deleting it could result in losing track of all deployed infrastructure.
- **After running Terraform, always check in any changed files (e.g., backend config or lock files).**

---

## Why Use a Remote Backend?

- Storing state remotely in GCS enables safe collaboration and disaster recovery.
- See: [Storing Terraform State in Google Cloud](https://cloud.google.com/docs/terraform/resource-management/store-state)

---

## Prerequisites

- [Terraform](https://www.terraform.io/) installed (v1.0+ recommended)
- Google Cloud SDK installed
- Sufficient permissions in GCP to create and manage GCS buckets
- Authenticated with GCP:

  ```sh
  gcloud auth application-default login
  ```

---

## Usage

Run these commands directly in this directory:

Initialize the working directory:

```sh
terraform init
```

See planned actions:

```sh
terraform plan
```

Apply changes (create the backend bucket):

```sh
terraform apply
```

---

## After Setup

Once the backend bucket is created, reference it in your Terraform backend configuration to enable remote state storage:

- [Terraform GCS Backend Documentation](https://developer.hashicorp.com/terraform/language/settings/backends/gcs)

---

## Example: Using the Backend Bucket

After applying actions, you will see an output that looks like:

```terraform
tfstate_bucket = "deephaven-mcp-tfstate"
```

To use the backend, create `backend.tf` in your project:

```terraform
terraform {
  backend "gcs" {
    bucket  = "deephaven-mcp-tfstate"
    prefix  = "terraform/state"
  }
}
```

See also [Terraform Backend Configuration](https://developer.hashicorp.com/terraform/language/settings/backends/configuration)
for details on configuring using files or the command line.

---

For more details, see the main project README or contact the maintainers.
