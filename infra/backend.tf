terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # Values are supplied at init time via -backend-config so the account id
  # is never hardcoded in the repo. See scripts/bootstrap.sh.
  backend "s3" {
    key            = "transitpulse/terraform.tfstate"
    dynamodb_table = "tfstate-lock"
    encrypt        = true
  }
}
