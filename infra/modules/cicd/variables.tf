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

variable "github_repo_immutable" {
  description = <<-EOT
    Same repo in GitHub's immutable OIDC form: owner@<owner-id>/name@<repo-id>.
    Required for repositories created after 2026-07-15, which emit this in the
    `sub` claim instead of the plain name. Leave empty for older repositories.

    Get it with:
      curl -s https://api.github.com/repos/<owner>/<name> \
        | python3 -c "import sys,json;d=json.load(sys.stdin);\
print(f\"{d['owner']['login']}@{d['owner']['id']}/{d['name']}@{d['id']}\")"
  EOT
  type        = string
  default     = ""
}

variable "state_bucket" {
  type = string
}
