provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      Env       = var.env
      Owner     = var.owner
      ManagedBy = "terraform"
    }
  }
}

# Billing metrics only exist in us-east-1, so the cost alarm needs a second
# provider configuration pointed at that region.
provider "aws" {
  alias  = "useast1"
  region = "us-east-1"

  default_tags {
    tags = {
      Project   = var.project
      Env       = var.env
      Owner     = var.owner
      ManagedBy = "terraform"
    }
  }
}
