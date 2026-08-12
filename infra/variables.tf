variable "project" {
  description = "Project slug used for naming and tagging."
  type        = string
  default     = "transitpulse"
}

variable "env" {
  description = "Environment name (dev or prod)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "Primary AWS region for all resources."
  type        = string
  default     = "ca-central-1"
}

variable "owner" {
  description = "Owner tag value."
  type        = string
}

variable "alert_email" {
  description = "Email address subscribed to the SNS alerts topic."
  type        = string
}

variable "poller_package_type" {
  description = "Zip (no Docker required) or Image (container build). Zip is the default."
  type        = string
  default     = "Zip"
}

variable "poller_image_tag" {
  description = "ECR image tag for the GTFS poller Lambda."
  type        = string
  default     = "v1"
}

variable "translink_secret_name" {
  description = "Secrets Manager secret holding the TransLink API key."
  type        = string
  default     = "transitpulse/translink-api-key"
}

variable "gtfs_static_url" {
  description = "URL of the TransLink GTFS static ZIP archive."
  type        = string
  default     = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
}

variable "daily_cost_threshold_usd" {
  description = "Daily estimated charge that trips the ingestion kill switch."
  type        = number
  default     = 3
}

variable "force_destroy_buckets" {
  description = "Allow terraform destroy to empty non-empty buckets. Dev only."
  type        = bool
  default     = true
}

variable "github_repo" {
  description = "GitHub repository allowed to deploy via OIDC, as owner/repo. Leave empty to skip CI/CD setup."
  type        = string
  default     = ""
}
