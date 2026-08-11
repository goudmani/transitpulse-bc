output "deploy_role_arn" {
  value       = local.enabled == 1 ? aws_iam_role.deploy[0].arn : ""
  description = "Role ARN for the role-to-assume field in the GitHub Actions workflow"
}

output "oidc_provider_arn" {
  value = local.enabled == 1 ? aws_iam_openid_connect_provider.github[0].arn : ""
}
