data "assert_test" "workspace" {
    test = terraform.workspace == "default"
    throw = "Must use the default workspace"
}

variable "project_id" {
  description = "project id"
}

variable "region" {
  description = "region"
}

variable "bucket" {
  description = "bucket"
}

variable "app" {
  description = "Application name"
}

output "tfstate_bucket" {
  description = "terraform state bucket"
  value = var.bucket
}

// See: https://cloud.google.com/docs/terraform/resource-management/store-state
resource "google_storage_bucket" "default" {
  name = var.bucket
  force_destroy = true
  location = "US"
  storage_class = "STANDARD"
  versioning {
    enabled = true
  }

  // label the resources for accounting
  labels = {
    app = var.app
  }
}