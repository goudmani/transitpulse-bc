locals {
  buckets = {
    bronze    = "${var.name}-bronze-${var.acct}"
    silver    = "${var.name}-silver-${var.acct}"
    gold      = "${var.name}-gold-${var.acct}"
    artifacts = "${var.name}-artifacts-${var.acct}"
    athena    = "${var.name}-athena-${var.acct}"
  }
}

resource "aws_s3_bucket" "b" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "b" {
  for_each                = aws_s3_bucket.b
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "b" {
  for_each = aws_s3_bucket.b
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }

    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.b["artifacts"].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.b
  bucket   = each.value.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          each.value.arn,
          "${each.value.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.b]
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.b["bronze"].id

  rule {
    id     = "expire-raw"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    # No Intelligent-Tiering transition. IT only moves an object to a cheaper
    # class after 30 days without access, and these objects are deleted at 30
    # days -- so it would never reach the cheaper tier and would only add the
    # per-object monitoring fee, which is real money against many small
    # Parquet files.
    #
    # 30 days, not 90: raw bronze is fully reproducible from silver, and the
    # silver Iceberg table is the thing worth keeping.
    expiration {
      days = 30
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena" {
  bucket = aws_s3_bucket.b["athena"].id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = 7
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name        = var.name
  description = "TransitPulse BC lakehouse catalog"
}

resource "aws_athena_workgroup" "wg" {
  name          = var.name
  force_destroy = var.force_destroy

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.b["athena"].id}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
