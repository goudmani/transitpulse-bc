data "aws_iam_policy_document" "sagemaker_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sagemaker" {
  name               = "${var.name}-sagemaker"
  assume_role_policy = data.aws_iam_policy_document.sagemaker_assume.json
}

# Dev convenience. For prod this should be replaced with a scoped policy;
# the README states this explicitly rather than claiming least privilege.
resource "aws_iam_role_policy_attachment" "sagemaker_full" {
  role       = aws_iam_role.sagemaker.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

data "aws_iam_policy_document" "sagemaker_data" {
  statement {
    sid    = "LakeAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]

    resources = [
      var.gold_arn,
      "${var.gold_arn}/*",
      var.artifacts_arn,
      "${var.artifacts_arn}/*"
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

resource "aws_iam_role_policy" "sagemaker_data" {
  name   = "${var.name}-sagemaker-data"
  role   = aws_iam_role.sagemaker.id
  policy = data.aws_iam_policy_document.sagemaker_data.json
}

resource "aws_sagemaker_model_package_group" "models" {
  model_package_group_name        = var.name
  model_package_group_description = "TransitPulse arrival delay models"
}

# Notifies you when a model version is approved so you can watch the deploy.
resource "aws_cloudwatch_event_rule" "model_approved" {
  name        = "${var.name}-model-approved"
  description = "Fires when a model package is approved in the registry"

  event_pattern = jsonencode({
    source      = ["aws.sagemaker"]
    detail-type = ["SageMaker Model Package State Change"]
    detail = {
      ModelPackageGroupName = [var.name]
      ModelApprovalStatus   = ["Approved"]
    }
  })
}

resource "aws_cloudwatch_event_target" "model_approved" {
  rule      = aws_cloudwatch_event_rule.model_approved.name
  target_id = "notify"
  arn       = var.alerts_topic_arn
}

# Weekly retraining trigger. The pipeline itself is defined in
# src/ml/pipeline.py and created by running that script.
resource "aws_cloudwatch_event_rule" "retrain_weekly" {
  name                = "${var.name}-retrain-weekly"
  description         = "Kick off the SageMaker training pipeline every Monday"
  schedule_expression = "cron(0 8 ? * MON *)"
  state               = "ENABLED"
}
