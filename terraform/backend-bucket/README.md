# Terraform: backend-bucket

This Terraform directory creates a backend bucket to persist Terraform state.

**This should only need to be run once**
**Do not delete the bucket unless you know what you are doing!  It contains Terraform state on what has been spun up on the cloud.**

See: [https://cloud.google.com/docs/terraform/resource-management/store-state](https://cloud.google.com/docs/terraform/resource-management/store-state)

**After executing commands, all file changes must be checked in!!!**

To authenticate:
```bash
gcloud auth application-default login
```

To init:
```bash
terraform init
```

To plan actions:
```bash
terraform plan
```

To apply actions:
```bash
terraform apply
```

After the buckets are created, they are used in Terraform backends to store terraform state.
See: [https://developer.hashicorp.com/terraform/language/settings/backends/gcs](https://developer.hashicorp.com/terraform/language/settings/backends/gcs)

After applying actions, you will see an output that looks like:
```
tfstate_bucket = "deephaven-mcp-tfstate"
```
To use the backend, create `backend.tf` in your project:
```
terraform {
  backend "gcs" {
    bucket  = "deephaven-mcp-tfstate"
    prefix  = "terraform/state"
  }
}
```

See also [Terraform Backend Configuration](https://developer.hashicorp.com/terraform/language/settings/backends/configuration)
for details on configuring using files or the command line.