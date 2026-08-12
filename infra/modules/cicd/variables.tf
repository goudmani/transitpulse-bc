variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "github_repo" {
  description = "GitHub repo allowed to assume the deploy role, as owner/repo. Empty disables CI/CD entirely."
  type        = string
  default     = ""
}

variable "state_bucket" {
  type = string
}
