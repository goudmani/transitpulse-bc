output "online_table_name" {
  value = aws_dynamodb_table.online.name
}

output "online_table_arn" {
  value = aws_dynamodb_table.online.arn
}

output "predict_function_name" {
  value = aws_lambda_function.predict.function_name
}

output "api_base_url" {
  value = aws_apigatewayv2_stage.v1.invoke_url
}

output "endpoint_name" {
  value = local.endpoint
}
