data "google_dns_managed_zone" "default" {
  name    = var.dns_managed_zone
  project = var.project_id
}
