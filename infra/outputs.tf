output "account_id" {
  value = local.acct
}

output "bucket_names" {
  value = module.lake.bucket_names
}

output "glue_database" {
  value = module.lake.glue_db
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "kinesis_stream_name" {
  value = module.ingest.kinesis_stream_name
}

output "poller_function_name" {
  value = module.ingest.poller_function_name
}

output "state_machine_arn" {
  value = module.etl.state_machine_arn
}

output "sagemaker_role_arn" {
  value = module.ml.sagemaker_role_arn
}

output "model_package_group" {
  value = module.ml.model_package_group_name
}

output "online_feature_table" {
  value = module.serving.online_table_name
}

output "api_base_url" {
  value = module.serving.api_base_url
}

output "github_deploy_role_arn" {
  description = "Paste into role-to-assume in .github/workflows/ci.yml"
  value       = module.cicd.deploy_role_arn
}

output "agent_role_arn" {
  description = "Read-only role for the daily ops agent. Store as the AGENT_ROLE_ARN repo secret."
  value       = module.cicd.agent_role_arn
}
