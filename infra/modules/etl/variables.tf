variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "bronze_arn" {
  type = string
}

variable "silver_arn" {
  type = string
}

variable "gold_arn" {
  type = string
}

variable "artifacts_bucket" {
  type = string
}

variable "artifacts_arn" {
  type = string
}

variable "glue_db" {
  type = string
}

variable "alerts_topic_arn" {
  type = string
}

variable "glue_version" {
  type    = string
  default = "4.0"
}

variable "log_retention_days" {
  type    = number
  default = 14
}
