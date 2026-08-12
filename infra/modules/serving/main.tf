locals {
  endpoint = var.endpoint_name != "" ? var.endpoint_name : "${var.name}-delay-predictor"
  log_arn  = "arn:aws:logs:${var.region}:${var.acct}:*"
}

resource "aws_dynamodb_table" "online" {
  name         = "${var.name}-online-features"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  # Live state expires on its own; no cleanup job, no storage creep.
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = false
  }
}

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

data "archive_file" "predict" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/serving/predict"
  output_path = "${path.module}/build/predict.zip"
}

resource "aws_iam_role" "predict" {
  name               = "${var.name}-predict"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "predict" {
  statement {
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.online.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sagemaker:InvokeEndpoint"]
    resources = ["arn:aws:sagemaker:${var.region}:${var.acct}:endpoint/${local.endpoint}"]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }

  statement {
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

resource "aws_iam_role_policy" "predict" {
  name   = "${var.name}-predict"
  role   = aws_iam_role.predict.id
  policy = data.aws_iam_policy_document.predict.json
}

resource "aws_cloudwatch_log_group" "predict" {
  name              = "/aws/lambda/${var.name}-predict"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "predict" {
  function_name    = "${var.name}-predict"
  role             = aws_iam_role.predict.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.predict.output_path
  source_code_hash = data.archive_file.predict.output_base64sha256
  timeout          = 15
  memory_size      = 512

  environment {
    variables = {
      ONLINE_TABLE  = aws_dynamodb_table.online.name
      ENDPOINT_NAME = local.endpoint
      METRIC_NS     = "TransitPulse"
    }
  }

  depends_on = [
    aws_iam_role_policy.predict,
    aws_cloudwatch_log_group.predict
  ]
}

resource "aws_apigatewayv2_api" "api" {
  name          = "${var.name}-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "OPTIONS"]
    allow_headers = ["content-type"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "predict" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.predict.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "predict" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /v1/predict"
  target    = "integrations/${aws_apigatewayv2_integration.predict.id}"
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/apigateway/${var.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_apigatewayv2_stage" "v1" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api.arn
    format          = "$context.requestId $context.httpMethod $context.path $context.status $context.responseLatency"
  }

  # Throttling matters: an unthrottled public endpoint in front of SageMaker
  # is an open invitation to run up your bill.
  default_route_settings {
    throttling_burst_limit = 20
    throttling_rate_limit  = 10
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowApiGatewayInvokePredict"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.predict.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
