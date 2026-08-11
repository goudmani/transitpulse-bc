variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "bronze_bucket" {
  type = string
}

variable "bronze_arn" {
  type = string
}

variable "glue_db" {
  type = string
}

variable "alerts_topic_arn" {
  type = string
}

variable "poller_package_type" {
  description = "Zip (no Docker needed, ~2 MB) or Image (container, ~600 MB local build)."
  type        = string
  default     = "Zip"

  validation {
    condition     = contains(["Zip", "Image"], var.poller_package_type)
    error_message = "poller_package_type must be Zip or Image."
  }
}

variable "poller_image" {
  description = "ECR image URI. Only used when poller_package_type is Image."
  type        = string
  default     = ""
}

variable "poller_zip_path" {
  description = "Path to the built zip, relative to the infra directory."
  type        = string
  default     = "../build/poller.zip"
}

variable "secret_name" {
  type = string
}

variable "gtfs_static_url" {
  type = string
}

variable "online_table_arn" {
  type = string
}

variable "online_table" {
  type = string
}

variable "log_retention_days" {
  type    = number
  default = 14
}
