variable "project_id" {
  description = "The ID of the project in which to create the Docker repository."
  type        = string
}

variable "region" {
  description = "The region in which to create the Docker repository."
  type        = string
}

variable "app" {
  description = "The name of the application."
  type        = string
}

resource "google_artifact_registry_repository" "docker_repo" {
  project       = var.project_id
  location      = var.region
  repository_id = var.app
  format        = "DOCKER"
  description   = "Deephaven MCP Docker repository."

  labels = {
    app = var.app,
  }

  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"

    condition {
      tag_state = "UNTAGGED"
    }
  }
  cleanup_policies {
    id     = "keep-new-untagged"
    action = "KEEP"

    condition {
      tag_state  = "UNTAGGED"
      newer_than = "7d"
    }
  }
}

output "docker_uri" {
  description = "URI of the Docker repository."
  value       = google_artifact_registry_repository.docker_repo.id
}
