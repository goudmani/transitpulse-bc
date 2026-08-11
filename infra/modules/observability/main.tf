# --------------------------------------------------------------------------
# Turn the poller's structured log line into a metric, then alarm on silence.
# Without this, an expired API key goes unnoticed for weeks.
# --------------------------------------------------------------------------
resource "aws_cloudwatch_log_metric_filter" "ingested" {
  name           = "${var.name}-records-ingested"
  log_group_name = var.poller_log_group
  pattern        = "{ $.metric = \"records_ingested\" }"

  metric_transformation {
    name          = "RecordsIngested"
    namespace     = "TransitPulse"
    value         = "$.value"
    unit          = "Count"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingest_stalled" {
  alarm_name          = "${var.name}-ingest-stalled"
  alarm_description   = "No GTFS records ingested in the last 15 minutes"
  namespace           = "TransitPulse"
  metric_name         = "RecordsIngested"
  statistic           = "Sum"
  period              = 900
  evaluation_periods  = 1
  threshold           = 1000
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [var.alerts_topic_arn]
  ok_actions          = [var.alerts_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "poller_errors" {
  alarm_name          = "${var.name}-poller-errors"
  alarm_description   = "Poller Lambda is throwing errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 3
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]

  dimensions = {
    FunctionName = var.poller_function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "poller_dlq" {
  alarm_name          = "${var.name}-poller-dlq-not-empty"
  alarm_description   = "Messages landed in the poller dead letter queue"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]

  dimensions = {
    QueueName = var.poller_dlq_name
  }
}

resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${var.name}-kinesis-iterator-age"
  alarm_description   = "Stream consumers are falling behind"
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 600000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]

  dimensions = {
    StreamName = var.kinesis_stream_name
  }
}

resource "aws_cloudwatch_metric_alarm" "model_mae_degraded" {
  alarm_name          = "${var.name}-model-mae-degraded"
  alarm_description   = "Rolling model MAE has degraded past the baseline ratio"
  namespace           = "TransitPulse"
  metric_name         = "ModelMaeRatioVsPersistence"
  statistic           = "Average"
  period              = 86400
  evaluation_periods  = 2
  threshold           = 1.0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "missing"
  alarm_actions       = [var.alerts_topic_arn]
}

# --------------------------------------------------------------------------
# Cost circuit breaker. Billing metrics live only in us-east-1.
# --------------------------------------------------------------------------
# CloudWatch requires every alarm action to live in the alarm's own region, and
# AWS/Billing metrics are published only in us-east-1. So the billing alarm
# cannot target the primary-region SNS topic or invoke the killswitch Lambda
# directly. It publishes to a us-east-1 topic instead, which fans out to both.
resource "aws_sns_topic" "billing_alerts" {
  provider = aws.useast1
  name     = "${var.name}-billing-alerts"
}

resource "aws_sns_topic_subscription" "billing_email" {
  provider  = aws.useast1
  topic_arn = aws_sns_topic.billing_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# SNS supports cross-region Lambda subscriptions, which is what carries the
# signal back to the killswitch in the primary region.
resource "aws_sns_topic_subscription" "billing_killswitch" {
  provider  = aws.useast1
  topic_arn = aws_sns_topic.billing_alerts.arn
  protocol  = "lambda"
  endpoint  = var.killswitch_function_arn
}

resource "aws_lambda_permission" "cost_alarm" {
  statement_id  = "AllowBillingTopicInvokeKillswitch"
  action        = "lambda:InvokeFunction"
  function_name = var.killswitch_function_arn
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.billing_alerts.arn
}

resource "aws_cloudwatch_metric_alarm" "estimated_charges" {
  provider = aws.useast1

  alarm_name          = "${var.name}-estimated-charges"
  alarm_description   = "Estimated charges exceeded the daily threshold; disabling ingestion"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  statistic           = "Maximum"
  period              = 21600
  evaluation_periods  = 1
  threshold           = var.daily_cost_threshold_usd
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Currency = "USD"
  }

  alarm_actions = [aws_sns_topic.billing_alerts.arn]
}

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = var.name

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Records ingested"
          region = var.region
          stat   = "Sum"
          period = 300
          metrics = [
            ["TransitPulse", "RecordsIngested"]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Poller health"
          region = var.region
          stat   = "Sum"
          period = 300
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", var.poller_function_name],
            ["AWS/Lambda", "Errors", "FunctionName", var.poller_function_name],
            ["AWS/Lambda", "Duration", "FunctionName", var.poller_function_name, { stat = "Average" }]
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Model MAE vs baselines (seconds)"
          region = var.region
          stat   = "Average"
          period = 86400
          metrics = [
            ["TransitPulse", "ModelMae"],
            ["TransitPulse", "PersistenceMae"],
            ["TransitPulse", "ScheduleMae"]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Stream backlog"
          region = var.region
          stat   = "Maximum"
          period = 300
          metrics = [
            ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", var.kinesis_stream_name]
          ]
        }
      }
    ]
  })
}
