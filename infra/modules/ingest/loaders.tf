# --------------------------------------------------------------------------
# Static GTFS loader: weekly, idempotent via a SHA-256 stored in SSM.
# --------------------------------------------------------------------------
data "archive_file" "static_loader" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/ingest/static_loader"
  output_path = "${path.module}/build/static_loader.zip"
}

resource "aws_iam_role" "static_loader" {
  name               = "${var.name}-static-loader"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "static_loader" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    resources = [var.bronze_arn, "${var.bronze_arn}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["ssm:GetParameter", "ssm:PutParameter"]
    resources = ["arn:aws:ssm:${var.region}:${var.acct}:parameter/${var.name}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "static_loader" {
  name   = "${var.name}-static-loader"
  role   = aws_iam_role.static_loader.id
  policy = data.aws_iam_policy_document.static_loader.json
}

resource "aws_cloudwatch_log_group" "static_loader" {
  name              = "/aws/lambda/${var.name}-static-loader"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "static_loader" {
  function_name    = "${var.name}-static-loader"
  role             = aws_iam_role.static_loader.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.static_loader.output_path
  source_code_hash = data.archive_file.static_loader.output_base64sha256

  # stop_times.txt is large; give it room and time.
  timeout     = 600
  memory_size = 2048

  environment {
    variables = {
      BRONZE_BUCKET = var.bronze_bucket
      GTFS_URL      = var.gtfs_static_url
      SSM_PARAM     = "/${var.name}/gtfs-static/sha256"
    }
  }

  depends_on = [
    aws_iam_role_policy.static_loader,
    aws_cloudwatch_log_group.static_loader
  ]
}

resource "aws_cloudwatch_event_rule" "static_weekly" {
  name                = "${var.name}-gtfs-static-weekly"
  description         = "Refresh the GTFS static schedule every Saturday"
  schedule_expression = "cron(0 9 ? * SAT *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "static_weekly" {
  rule      = aws_cloudwatch_event_rule.static_weekly.name
  target_id = "static-loader"
  arn       = aws_lambda_function.static_loader.arn
}

resource "aws_lambda_permission" "static_weekly" {
  statement_id  = "AllowEventBridgeInvokeStaticLoader"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.static_loader.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.static_weekly.arn
}

# --------------------------------------------------------------------------
# Online feature writer: second consumer of the same Kinesis stream.
# This is why the design uses Data Streams rather than Firehose alone.
# --------------------------------------------------------------------------
data "archive_file" "online_features" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/ingest/online_features"
  output_path = "${path.module}/build/online_features.zip"
}

resource "aws_iam_role" "online_features" {
  name               = "${var.name}-online-features"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "online_features" {
  statement {
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:ListStreams",
      "kinesis:SubscribeToShard"
    ]

    resources = [aws_kinesis_stream.gtfs.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:BatchWriteItem", "dynamodb:GetItem"]
    resources = [var.online_table_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "online_features" {
  name   = "${var.name}-online-features"
  role   = aws_iam_role.online_features.id
  policy = data.aws_iam_policy_document.online_features.json
}

resource "aws_cloudwatch_log_group" "online_features" {
  name              = "/aws/lambda/${var.name}-online-features"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "online_features" {
  function_name    = "${var.name}-online-features"
  role             = aws_iam_role.online_features.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.online_features.output_path
  source_code_hash = data.archive_file.online_features.output_base64sha256
  timeout          = 60
  memory_size      = 512

  environment {
    variables = {
      ONLINE_TABLE = var.online_table
      TTL_SECONDS  = "7200"
    }
  }

  depends_on = [
    aws_iam_role_policy.online_features,
    aws_cloudwatch_log_group.online_features
  ]
}

resource "aws_lambda_event_source_mapping" "online_features" {
  event_source_arn                   = aws_kinesis_stream.gtfs.arn
  function_name                      = aws_lambda_function.online_features.arn
  starting_position                  = "LATEST"
  batch_size                         = 500
  maximum_batching_window_in_seconds = 30
  maximum_retry_attempts             = 2
  bisect_batch_on_function_error     = true
}

# --------------------------------------------------------------------------
# Cost kill switch: disables the poll schedule when spend spikes.
# --------------------------------------------------------------------------
data "archive_file" "killswitch" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/ops/killswitch"
  output_path = "${path.module}/build/killswitch.zip"
}

resource "aws_iam_role" "killswitch" {
  name               = "${var.name}-killswitch"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "killswitch" {
  statement {
    effect    = "Allow"
    actions   = ["events:DisableRule", "events:DescribeRule"]
    resources = [aws_cloudwatch_event_rule.poll.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [var.alerts_topic_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "killswitch" {
  name   = "${var.name}-killswitch"
  role   = aws_iam_role.killswitch.id
  policy = data.aws_iam_policy_document.killswitch.json
}

resource "aws_cloudwatch_log_group" "killswitch" {
  name              = "/aws/lambda/${var.name}-killswitch"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "killswitch" {
  function_name    = "${var.name}-killswitch"
  role             = aws_iam_role.killswitch.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.killswitch.output_path
  source_code_hash = data.archive_file.killswitch.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      RULE_NAME = aws_cloudwatch_event_rule.poll.name
      TOPIC_ARN = var.alerts_topic_arn
    }
  }

  depends_on = [
    aws_iam_role_policy.killswitch,
    aws_cloudwatch_log_group.killswitch
  ]
}
