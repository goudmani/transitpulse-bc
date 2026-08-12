locals {
  enabled = var.github_repo != "" ? 1 : 0
}

# GitHub's OIDC issuer. Creating this lets GitHub Actions exchange a workflow
# token for temporary AWS credentials, so no long-lived access keys are ever
# stored in the repository.
resource "aws_iam_openid_connect_provider" "github" {
  count = local.enabled

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]

  # AWS validates GitHub's certificate chain itself now, but the provider
  # still requires this argument.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "assume" {
  count = local.enabled

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Scoped to this repository only. Without this condition ANY GitHub
    # repository on the internet could assume the role.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "deploy" {
  count = local.enabled

  name                 = "github-actions-${var.name}"
  description          = "Assumed by GitHub Actions via OIDC to plan and apply this project"
  assume_role_policy   = data.aws_iam_policy_document.assume[0].json
  max_session_duration = 3600
}

# Broad for a solo project, but bounded: it can only be assumed by one repo,
# sessions last an hour, and every action is in CloudTrail. A team setup would
# split plan (read-only) from apply and scope the apply policy per service.
resource "aws_iam_role_policy_attachment" "deploy" {
  count = local.enabled

  role       = aws_iam_role.deploy[0].name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

# PowerUserAccess excludes IAM, which this project needs in order to manage
# its own service roles.
data "aws_iam_policy_document" "iam_management" {
  count = local.enabled

  statement {
    sid    = "ManageProjectRoles"
    effect = "Allow"

    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:PassRole",
      "iam:TagRole",
      "iam:UpdateRole",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:GetRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy"
    ]

    resources = ["arn:aws:iam::${var.acct}:role/${var.name}-*"]
  }

  statement {
    sid       = "TerraformState"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.state_bucket}", "arn:aws:s3:::${var.state_bucket}/*"]
  }

  statement {
    sid       = "TerraformLock"
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
    resources = ["arn:aws:dynamodb:${var.region}:${var.acct}:table/tfstate-lock"]
  }
}

resource "aws_iam_role_policy" "iam_management" {
  count = local.enabled

  name   = "${var.name}-iam-management"
  role   = aws_iam_role.deploy[0].id
  policy = data.aws_iam_policy_document.iam_management[0].json
}
