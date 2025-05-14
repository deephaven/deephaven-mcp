# Configure a Google Cloud Run service for the MCP Docs

resource "google_cloud_run_v2_service" "mcp_docs_service" {
  name     = "deephaven-mcp-docs-${var.workspace}"
  location = var.region
  description = "Deephaven MCP Docs service"
  deletion_protection = false

  labels = {
    app = var.app
  }

  template {
    containers {
      image = var.image

      resources {
        startup_cpu_boost = true
        limits = {
          memory = var.container_memory
        }
      }

      ports {
        container_port = 8000
      }

      # env {
      #   name  = "PYTHONLOGLEVEL"
      #   value = "DEBUG"
      # }

      env {
        name = "INKEEP_API_KEY"
        value = var.inkeep_api_key
      }

      startup_probe {
        http_get {
          path = "/health"
          port = 8000
        }
        initial_delay_seconds = 20
        period_seconds        = 5
        failure_threshold     = 3
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8000
        }
        initial_delay_seconds = 20
        period_seconds        = 5
        failure_threshold     = 3
      }
    }

    scaling {
      min_instance_count = var.min_instance_count
      max_instance_count = var.max_instance_count
    }
  }

}

# Allow public access to the Cloud Run service
resource "google_cloud_run_service_iam_member" "mcp_docs_service" {
  service    = google_cloud_run_v2_service.mcp_docs_service.name
  location   = google_cloud_run_v2_service.mcp_docs_service.location
  role       = "roles/run.invoker"
  member     = "allUsers"
}

# Create a DNS record in your existing Cloud DNS zone
# The DNS record needs to point at the address for all Google Hosted Services
# https://cloud.google.com/run/docs/mapping-custom-domains#dns-records
# https://www.perplexity.ai/search/can-a-google-cloud-run-be-crea-oyLpWwtpRRK8TTI7h1GJlA
resource "google_dns_record_set" "mcp_docs_service" {
  name         = "deephaven-mcp-docs-${var.workspace}.${data.google_dns_managed_zone.default.dns_name}"
  managed_zone = data.google_dns_managed_zone.default.name
  type         = "CNAME"
  ttl          = 300
  rrdatas = ["ghs.googlehosted.com."] 
}

# Map the custom domain to your Cloud Run service
resource "google_cloud_run_domain_mapping" "mcp_docs_service" {
  location = google_cloud_run_v2_service.mcp_docs_service.location
  name = replace(google_dns_record_set.mcp_docs_service.name, "/\\.$/", "")

  metadata {
    namespace = google_cloud_run_v2_service.mcp_docs_service.project
  }

  spec {
    route_name = google_cloud_run_v2_service.mcp_docs_service.name
  }
}

################################################################

output "mcp_docs_service_uri" {
  description = "URI of the Cloud Run service"
  value       = google_cloud_run_v2_service.mcp_docs_service.uri
}

output "mcp_docs_service_urls" {
  description = "URLs of the Cloud Run service"
  value       = google_cloud_run_v2_service.mcp_docs_service.urls
}

output "mcp_docs_service_dns" {
  description = "DNS address of the Cloud Run service"
  value       = google_cloud_run_domain_mapping.mcp_docs_service.name
}

