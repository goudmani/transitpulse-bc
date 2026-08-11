output "dashboard_name" {
  value = aws_cloudwatch_dashboard.main.dashboard_name
}

output "alarm_names" {
  value = [
    aws_cloudwatch_metric_alarm.ingest_stalled.alarm_name,
    aws_cloudwatch_metric_alarm.poller_errors.alarm_name,
    aws_cloudwatch_metric_alarm.poller_dlq.alarm_name,
    aws_cloudwatch_metric_alarm.iterator_age.alarm_name,
    aws_cloudwatch_metric_alarm.model_mae_degraded.alarm_name
  ]
}
