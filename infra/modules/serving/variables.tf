variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "endpoint_name" {
  type    = string
  default = ""
}

variable "log_retention_days" {
  type    = number
  default = 14
}
