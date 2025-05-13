terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.53"
    }

    assert = {
      source  = "bwoznicki/assert"
      version = "0.0.1"
    }

  }

  required_version = ">= 1.3"
}