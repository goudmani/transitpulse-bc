output "kinesis_stream_name" {
  value = aws_kinesis_stream.gtfs.name
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.gtfs.arn
}

output "poller_function_name" {
  value = aws_lambda_function.poller.function_name
}

output "poller_log_group" {
  value = aws_cloudwatch_log_group.poller.name
}

output "poller_dlq_name" {
  value = aws_sqs_queue.poller_dlq.name
}

output "poller_rule_name" {
  value = aws_cloudwatch_event_rule.poll.name
}

output "static_loader_function_name" {
  value = aws_lambda_function.static_loader.function_name
}

output "killswitch_function_arn" {
  value = aws_lambda_function.killswitch.arn
}

output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.bronze.name
}
