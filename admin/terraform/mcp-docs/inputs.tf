variable "project_id" {
  description = "The ID of the project in which to create the Cloud Run service."
  type        = string
}

variable "region" {
  description = "The region in which to create the Cloud Run service."
  type        = string
}

variable "app" {
  description = "The name of the application."
  type        = string
}

variable "image" {
  description = "The image to deploy."
  type        = string
}

variable "dns_managed_zone" {
  description = "The name of the DNS managed zone."
  type        = string
}

variable "inkeep_api_key" {
  description = "The Inkeep API key."
  type        = string
}

variable "min_instance_count" {
  description = "The minimum number of instances to run."
  type        = number
}

variable "max_instance_count" {
  description = "The maximum number of instances to run."
  type        = number
}

variable "container_memory" {
  description = "The amount of memory to allocate to the container (e.g., 512Mi, 1Gi)."
  type        = string
}