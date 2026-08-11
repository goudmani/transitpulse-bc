output "state_machine_arn" {
  value = aws_sfn_state_machine.etl.arn
}

output "state_machine_name" {
  value = aws_sfn_state_machine.etl.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "glue_job_names" {
  value = { for k, j in aws_glue_job.job : k => j.name }
}
