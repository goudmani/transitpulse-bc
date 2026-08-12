output "sagemaker_role_arn" {
  value = aws_iam_role.sagemaker.arn
}

output "model_package_group_name" {
  value = aws_sagemaker_model_package_group.models.model_package_group_name
}

output "retrain_rule_name" {
  value = aws_cloudwatch_event_rule.retrain_weekly.name
}
