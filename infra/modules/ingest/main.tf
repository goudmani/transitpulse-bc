locals {
  secret_arn = "arn:aws:secretsmanager:${var.region}:${var.acct}:secret:${var.secret_name}-*"
  log_arn    = "arn:aws:logs:${var.region}:${var.acct}:*"
}

# --------------------------------------------------------------------------
# Kinesis stream: one shard is ~1,000 records/sec and ~1 MiB/sec of writes.
# --------------------------------------------------------------------------
resource "aws_kinesis_stream" "gtfs" {
  name             = "${var.name}-gtfs"
  retention_period = 24
  shard_count      = 1

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

resource "aws_sqs_queue" "poller_dlq" {
  name                      = "${var.name}-poller-dlq"
  message_retention_seconds = 1209600
}

# --------------------------------------------------------------------------
# Poller Lambda (container image). Deliberately NOT in the VPC: it needs
# outbound internet, and a VPC-attached Lambda would require a NAT Gateway.
# --------------------------------------------------------------------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "poller" {
  name               = "${var.name}-poller"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "poller" {
  statement {
    sid       = "WriteStream"
    effect    = "Allow"
    actions   = ["kinesis:PutRecord", "kinesis:PutRecords"]
    resources = [aws_kinesis_stream.gtfs.arn]
  }

  statement {
    sid       = "ReadApiKey"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [local.secret_arn]
  }

  statement {
    sid       = "DeadLetter"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.poller_dlq.arn]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "poller" {
  name   = "${var.name}-poller"
  role   = aws_iam_role.poller.id
  policy = data.aws_iam_policy_document.poller.json
}

resource "aws_cloudwatch_log_group" "poller" {
  name              = "/aws/lambda/${var.name}-poller"
  retention_in_days = var.log_retention_days
}

locals {
  poller_is_zip = var.poller_package_type == "Zip"
  poller_zip_ok = local.poller_is_zip && fileexists(var.poller_zip_path)
}

# Ships either as a ~2 MB zip (default, no Docker) or a container image.
# The zip path exists so a laptop short on disk and RAM never has to run
# Docker Desktop just to deploy a 200-line function.
resource "aws_lambda_function" "poller" {
  function_name = "${var.name}-poller"
  role          = aws_iam_role.poller.arn
  timeout       = 120
  memory_size   = 1024

  package_type     = var.poller_package_type
  image_uri        = local.poller_is_zip ? null : var.poller_image
  filename         = local.poller_is_zip ? var.poller_zip_path : null
  runtime          = local.poller_is_zip ? "python3.12" : null
  handler          = local.poller_is_zip ? "handler.lambda_handler" : null
  source_code_hash = local.poller_zip_ok ? filebase64sha256(var.poller_zip_path) : null

  # No reserved concurrency. A new AWS account starts with an account-wide
  # Lambda concurrency quota of 10 rather than the usual 1000, and reserving
  # any of it drops UnreservedConcurrentExecutions below the required minimum
  # of 10 -- PutFunctionConcurrency rejects it outright.
  #
  # The guardrail this replaced ("a scheduler misfire must not fan out into
  # hundreds of concurrent polls") is already provided by that account quota:
  # nothing here can exceed 10 concurrent executions in total. Set this back to
  # 2 after requesting a Service Quotas increase for concurrent executions.

  environment {
    variables = {
      STREAM_NAME = aws_kinesis_stream.gtfs.name
      SECRET_ID   = var.secret_name
      LOG_LEVEL   = "INFO"
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.poller_dlq.arn
  }

  depends_on = [
    aws_iam_role_policy.poller,
    aws_cloudwatch_log_group.poller
  ]
}

resource "aws_cloudwatch_event_rule" "poll" {
  name                = "${var.name}-poll-1min"
  description         = "Poll the GTFS-Realtime feed every minute"
  schedule_expression = "rate(1 minute)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "poll" {
  rule      = aws_cloudwatch_event_rule.poll.name
  target_id = "poller"
  arn       = aws_lambda_function.poller.arn
}

resource "aws_lambda_permission" "poll" {
  statement_id  = "AllowEventBridgeInvokePoller"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.poller.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.poll.arn
}

# --------------------------------------------------------------------------
# Firehose: buffers the stream and writes Parquet into bronze.
# --------------------------------------------------------------------------
data "aws_iam_policy_document" "firehose_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.name}-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid    = "WriteBronze"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject"
    ]

    resources = [var.bronze_arn, "${var.bronze_arn}/*"]
  }

  statement {
    sid    = "ReadStream"
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards"
    ]

    resources = [aws_kinesis_stream.gtfs.arn]
  }

  statement {
    sid    = "ReadCatalog"
    effect = "Allow"

    actions = [
      "glue:GetTable",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:GetDatabase"
    ]

    resources = [
      "arn:aws:glue:${var.region}:${var.acct}:catalog",
      "arn:aws:glue:${var.region}:${var.acct}:database/${var.glue_db}",
      "arn:aws:glue:${var.region}:${var.acct}:table/${var.glue_db}/*"
    ]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.name}-firehose"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.name}-to-bronze"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# The Glue table below only exists to give Firehose a schema for Parquet
# conversion. Analysts query the partition-projected tables in sql/.
resource "aws_glue_catalog_table" "firehose_schema" {
  name          = "firehose_schema"
  database_name = var.glue_db
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bronze_bucket}/raw/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "record_type"
      type = "string"
    }

    columns {
      name = "feed_timestamp"
      type = "bigint"
    }

    columns {
      name = "ingest_ts"
      type = "bigint"
    }

    columns {
      name = "trip_id"
      type = "string"
    }

    columns {
      name = "route_id"
      type = "string"
    }

    columns {
      name = "direction_id"
      type = "int"
    }

    columns {
      name = "start_date"
      type = "string"
    }

    columns {
      name = "schedule_relationship"
      type = "int"
    }

    columns {
      name = "vehicle_id"
      type = "string"
    }

    columns {
      name = "stop_id"
      type = "string"
    }

    columns {
      name = "stop_sequence"
      type = "int"
    }

    columns {
      name = "arrival_time"
      type = "bigint"
    }

    columns {
      name = "arrival_delay"
      type = "int"
    }

    columns {
      name = "departure_time"
      type = "bigint"
    }

    columns {
      name = "departure_delay"
      type = "int"
    }

    columns {
      name = "latitude"
      type = "double"
    }

    columns {
      name = "longitude"
      type = "double"
    }

    columns {
      name = "bearing"
      type = "double"
    }

    columns {
      name = "speed"
      type = "double"
    }

    columns {
      name = "current_stop_sequence"
      type = "int"
    }

    columns {
      name = "occupancy_status"
      type = "int"
    }

    columns {
      name = "vehicle_timestamp"
      type = "bigint"
    }
  }
}

resource "aws_kinesis_firehose_delivery_stream" "bronze" {
  name        = "${var.name}-to-bronze"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.gtfs.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.bronze_arn

    # Bigger buffers mean fewer, larger files, which makes Athena cheaper.
    # Chasing lower latency here produces thousands of tiny files instead.
    buffering_size     = 64
    buffering_interval = 300
    compression_format = "UNCOMPRESSED"

    prefix              = "raw/!{partitionKeyFromQuery:record_type}/dt=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"

    dynamic_partitioning_configuration {
      enabled = true
    }

    processing_configuration {
      enabled = true

      processors {
        type = "MetadataExtraction"

        parameters {
          parameter_name  = "MetadataExtractionQuery"
          parameter_value = "{record_type:.record_type}"
        }

        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }
      }
    }

    data_format_conversion_configuration {
      enabled = true

      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }

      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }

      schema_configuration {
        database_name = var.glue_db
        table_name    = aws_glue_catalog_table.firehose_schema.name
        role_arn      = aws_iam_role.firehose.arn
        region        = var.region
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
}
