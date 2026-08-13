# Role for the daily ops agent (agent/, run by .github/workflows/agent-daily.yml).
#
# Deliberately NOT the deploy role above. That one carries PowerUserAccess so it
# can apply Terraform; this one runs unattended every night with an LLM choosing
# which calls to make, and the only safe answer to "what could the model do if it
# were talked into something" is "read things".
#
# The one write in here is s3:PutObject on the Athena results prefix, which
# Athena requires in order to return any result at all. It is scoped to that one
# prefix in that one bucket.

resource "aws_iam_role" "agent" {
  count = local.enabled

  name        = "github-actions-${var.name}-agent"
  description = "Read-only role assumed by the daily ops agent via GitHub OIDC"

  # Same OIDC provider and the same repo condition as the deploy role. A second
  # provider for the same issuer URL would fail to create -- AWS allows one per
  # URL per account.
  assume_role_policy   = data.aws_iam_policy_document.assume[0].json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "agent_read" {
  count = local.enabled

  # Metrics, alarms and logs. CloudWatch has no resource-level permissions for
  # GetMetricData, so these are necessarily account-wide reads.
  statement {
    sid    = "ObservabilityRead"
    effect = "Allow"
    actions = [
      "cloudwatch:DescribeAlarms",
      "cloudwatch:DescribeAlarmHistory",
      "cloudwatch:GetMetricData",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:ListMetrics",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:FilterLogEvents",
      "logs:GetLogEvents",
    ]
    resources = ["*"]
  }

  # Service inventory and run history. List* calls do not support resource
  # scoping; the Get*/Describe* calls that do are scoped by the second statement.
  statement {
    sid    = "PipelineListing"
    effect = "Allow"
    actions = [
      "lambda:ListFunctions",
      "kinesis:ListStreams",
      "firehose:ListDeliveryStreams",
      "states:ListStateMachines",
      "glue:ListJobs",
      "sqs:ListQueues",
      "events:ListRules",
      "athena:ListWorkGroups",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "PipelineInspect"
    effect = "Allow"
    actions = [
      "lambda:GetFunction",
      "lambda:GetFunctionConfiguration",
      "lambda:ListEventSourceMappings",
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "firehose:DescribeDeliveryStream",
      "states:DescribeStateMachine",
      "states:ListExecutions",
      "states:DescribeExecution",
      "glue:GetJob",
      "glue:GetJobs",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "sqs:GetQueueUrl",
      "sqs:GetQueueAttributes",
      "events:DescribeRule",
    ]
    resources = ["*"]
  }

  # Athena and the Glue Data Catalog behind it, for the data quality agent.
  # Stop* is included so a runaway query can be cancelled rather than left to
  # scan the lake to completion.
  statement {
    sid    = "AthenaRead"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "athena:GetDataCatalog",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "LakeRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::${var.name}-*-${var.acct}",
      "arn:aws:s3:::${var.name}-*-${var.acct}/*",
    ]
  }

  # Athena refuses to run without somewhere to write results. Scoped to the
  # results prefix of the Athena bucket only -- it cannot write to bronze,
  # silver, gold or artifacts.
  statement {
    sid       = "AthenaResults"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["arn:aws:s3:::${var.name}-athena-${var.acct}/results/*"]
  }

  # Cost Explorer is a global service with no resource-level permissions.
  statement {
    sid    = "CostRead"
    effect = "Allow"
    actions = [
      "ce:GetCostAndUsage",
      "ce:GetCostForecast",
      "ce:GetDimensionValues",
      "ce:GetTags",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "agent_read" {
  count = local.enabled

  name   = "${var.name}-agent-read"
  role   = aws_iam_role.agent[0].id
  policy = data.aws_iam_policy_document.agent_read[0].json
}

# An explicit deny is belt and braces on top of a policy that grants no writes,
# but it is the difference between "we did not grant it" and "it cannot happen".
# If someone later attaches a broader policy to this role by mistake, this still
# holds.
data "aws_iam_policy_document" "agent_deny_writes" {
  count = local.enabled

  statement {
    sid    = "NeverMutate"
    effect = "Deny"
    actions = [
      "s3:DeleteObject",
      "s3:DeleteBucket",
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      "lambda:InvokeFunction",
      "lambda:UpdateFunctionCode",
      "lambda:UpdateFunctionConfiguration",
      "states:StartExecution",
      "states:StopExecution",
      "glue:StartJobRun",
      "glue:UpdateJob",
      "glue:DeleteJob",
      "events:EnableRule",
      "events:DisableRule",
      "events:PutRule",
      "iam:*",
      "sts:AssumeRole",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "agent_deny_writes" {
  count = local.enabled

  name   = "${var.name}-agent-deny-writes"
  role   = aws_iam_role.agent[0].id
  policy = data.aws_iam_policy_document.agent_deny_writes[0].json
}
