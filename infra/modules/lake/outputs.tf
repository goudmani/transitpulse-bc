output "bucket_names" {
  value = { for k, v in aws_s3_bucket.b : k => v.id }
}

output "bucket_arns" {
  value = { for k, v in aws_s3_bucket.b : k => v.arn }
}

output "glue_db" {
  value = aws_glue_catalog_database.lake.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.wg.name
}
