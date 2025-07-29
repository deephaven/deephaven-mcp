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
    # TODO: revisit -- Cloud Run service request timeout.
    # Request timeout for the Cloud Run service. Default is 300s (5 minutes).
    # Maximum is 3600s (60 minutes). For long-running requests, such as those
    # used by MCP persistent connections, a longer timeout is required to prevent
    # premature disconnection.
    # timeout = "3600s"
    
    # TODO: revisit -- Cloud Run service request concurrency.
    # The number of concurrent requests a single container instance will handle.
    # While this I/O-bound service can handle many concurrent tasks internally, this
    # setting controls Cloud Run's scaling behavior. A lower value (e.g., 20)
    # encourages Cloud Run to scale out (add more instances) sooner. This is a
    # safer, more stable strategy for I/O-bound tasks that are not trivially
    # lightweight, as it prevents any single instance from becoming a memory or
    # resource bottleneck. The default is 80. This value should be tuned based
    # on performance testing.
    # max_instance_request_concurrency = 20
    
    containers {
      image = var.image

      resources {
        startup_cpu_boost = true
        limits = {
          memory = var.container_memory
          # TODO: revisit -- Cloud Run service container CPU limit.
          # The amount of CPU allocated to the container instance. Default is "1000m" (1 vCPU).
          # Can be configured up to "8000m" (8 vCPUs). Increasing this to "2000m" (2 vCPUs)
          # or higher is recommended for production environments or during stress tests
          # to handle higher loads.
          cpu = "2000m"
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

