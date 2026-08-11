locals {
  iceberg_conf = join(" ", [
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.glue_catalog.warehouse=${replace(var.silver_arn, "arn:aws:s3:::", "s3://")}/iceberg/",
    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  ])

  scripts = {
    silver   = "silver_stop_events.py"
    gold     = "gold_features.py"
    dq       = "dq_checks.py"
    backtest = "backtest.py"
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data" {
  statement {
    sid       = "ReadBronze"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.bronze_arn, "${var.bronze_arn}/*"]
  }

  statement {
    sid    = "WriteSilverGold"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      var.silver_arn,
      "${var.silver_arn}/*",
      var.gold_arn,
      "${var.gold_arn}/*"
    ]
  }

  statement {
    sid       = "ReadScripts"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.artifacts_arn, "${var.artifacts_arn}/*"]
  }

  statement {
    sid    = "Catalog"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:UpdatePartition"
    ]

    resources = [
      "arn:aws:glue:${var.region}:${var.acct}:catalog",
      "arn:aws:glue:${var.region}:${var.acct}:database/${var.glue_db}",
      "arn:aws:glue:${var.region}:${var.acct}:table/${var.glue_db}/*"
    ]
  }

  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["TransitPulse"]
    }
  }
}

resource "aws_iam_role_policy" "glue_data" {
  name   = "${var.name}-glue-data"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}

resource "aws_s3_object" "scripts" {
  for_each = local.scripts

  bucket = var.artifacts_bucket
  key    = "glue/${each.value}"
  source = "${path.module}/../../../src/glue/${each.value}"
  etag   = filemd5("${path.module}/../../../src/glue/${each.value}")
}

resource "aws_glue_job" "job" {
  for_each = local.scripts

  name              = "${var.name}-${each.key}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2

  # A runaway Spark job is a runaway bill. Always set this.
  timeout = 30

  execution_property {
    max_concurrent_runs = 3
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.artifacts_bucket}/glue/${each.value}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.artifacts_bucket}/spark-logs/"
    "--TempDir"                          = "s3://${var.artifacts_bucket}/glue-temp/"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = local.iceberg_conf
    "--bronze_db"                        = var.glue_db
    "--glue_db"                          = var.glue_db
    "--silver_table"                     = "glue_catalog.${var.glue_db}.stop_events"
    "--gold_bucket"                      = replace(var.gold_arn, "arn:aws:s3:::", "")
    "--run_date"                         = "AUTO"
  }

  depends_on = [aws_s3_object.scripts]
}

# --------------------------------------------------------------------------
# Step Functions: orchestrates silver -> DQ -> gold with a quality gate.
# --------------------------------------------------------------------------
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn" {
  statement {
    effect    = "Allow"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [for j in aws_glue_job.job : j.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [var.alerts_topic_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["arn:aws:events:${var.region}:${var.acct}:rule/StepFunctionsGetEventsForGlueJobRule"]
  }

  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "${var.name}-sfn"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.name}-etl"
  retention_in_days = var.log_retention_days
}

resource "aws_sfn_state_machine" "etl" {
  name     = "${var.name}-etl"
  role_arn = aws_iam_role.sfn.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  definition = jsonencode({
    Comment = "TransitPulse hourly ETL: silver, data quality gate, gold"
    StartAt = "SilverStopEvents"
    States = {
      SilverStopEvents = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["silver"].name
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed", "Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 60
            MaxAttempts     = 2
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
            ResultPath  = "$.error"
          }
        ]
        ResultPath = "$.silver"
        Next       = "DataQualityChecks"
      }

      DataQualityChecks = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["dq"].name
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "QuarantinePartition"
            ResultPath  = "$.error"
          }
        ]
        ResultPath = "$.dq"
        Next       = "GoldFeatures"
      }

      GoldFeatures = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["gold"].name
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 2
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
            ResultPath  = "$.error"
          }
        ]
        ResultPath = "$.gold"
        Next       = "Succeeded"
      }

      QuarantinePartition = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.alerts_topic_arn
          Subject  = "TransitPulse: data quality gate FAILED"
          "Message.$" = "States.Format('Data quality checks failed for run_date {}. Gold features were NOT rebuilt. Inspect s3 dq/ results before promoting.', $.run_date)"
        }
        Next = "FailDueToDataQuality"
      }

      FailDueToDataQuality = {
        Type  = "Fail"
        Error = "DataQualityGateFailed"
        Cause = "Data quality checks did not pass; gold layer intentionally not updated."
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.alerts_topic_arn
          Subject  = "TransitPulse: ETL pipeline failed"
          "Message.$" = "States.Format('ETL failed for run_date {}. Check the Step Functions execution history.', $.run_date)"
        }
        Next = "FailPipeline"
      }

      FailPipeline = {
        Type  = "Fail"
        Error = "EtlPipelineFailed"
      }

      Succeeded = {
        Type = "Succeed"
      }
    }
  })
}

# Fires 20 minutes past the hour so Firehose has flushed its buffer.
resource "aws_cloudwatch_event_rule" "etl_hourly" {
  name                = "${var.name}-etl-hourly"
  description         = "Run the ETL state machine every hour"
  # Daily, not hourly. Hourly meant 24 executions x 3 Glue jobs = 72 runs/day,
  # roughly $150/month, for training data that is only consumed by a weekly
  # retrain. Serving freshness comes from the online path (Kinesis -> Lambda ->
  # DynamoDB) and is unaffected by this schedule.
  #
  # 02:20 UTC: late enough that the previous UTC day is complete and Firehose
  # has flushed its 300s buffer. The jobs resolve run_date to the previous day
  # -- see resolve_run_date() in src/glue/*.py.
  schedule_expression = "cron(20 2 * * ? *)"
  state               = "ENABLED"
}

data "aws_iam_policy_document" "events_sfn_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events_sfn" {
  name               = "${var.name}-events-sfn"
  assume_role_policy = data.aws_iam_policy_document.events_sfn_assume.json
}

resource "aws_iam_role_policy" "events_sfn" {
  name = "${var.name}-events-sfn"
  role = aws_iam_role.events_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.etl.arn
      }
    ]
  })
}

resource "aws_cloudwatch_event_target" "etl_hourly" {
  rule      = aws_cloudwatch_event_rule.etl_hourly.name
  target_id = "etl-state-machine"
  arn       = aws_sfn_state_machine.etl.arn
  role_arn  = aws_iam_role.events_sfn.arn

  input_transformer {
    input_paths = {
      time = "$.time"
    }

    # Glue jobs receive run_date as YYYY-MM-DD, sliced from the event time.
    input_template = "{\"run_date\": <time>}"
  }
}
