data "aws_caller_identity" "current" {}

locals {
  acct = data.aws_caller_identity.current.account_id
  name = var.project
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

module "network" {
  source = "./modules/network"

  name   = local.name
  region = var.region
}

module "lake" {
  source = "./modules/lake"

  name          = local.name
  acct          = local.acct
  force_destroy = var.force_destroy_buckets
}

module "ingest" {
  source = "./modules/ingest"

  name                = local.name
  acct                = local.acct
  region              = var.region
  bronze_bucket       = module.lake.bucket_names["bronze"]
  bronze_arn          = module.lake.bucket_arns["bronze"]
  glue_db             = module.lake.glue_db
  alerts_topic_arn    = aws_sns_topic.alerts.arn
  poller_package_type = var.poller_package_type
  poller_image        = "${local.acct}.dkr.ecr.${var.region}.amazonaws.com/${local.name}/poller:${var.poller_image_tag}"
  poller_zip_path     = "../build/poller.zip"
  secret_name         = var.translink_secret_name
  gtfs_static_url     = var.gtfs_static_url
  online_table_arn    = module.serving.online_table_arn
  online_table        = module.serving.online_table_name
}

module "etl" {
  source = "./modules/etl"

  name             = local.name
  acct             = local.acct
  region           = var.region
  bronze_arn       = module.lake.bucket_arns["bronze"]
  silver_arn       = module.lake.bucket_arns["silver"]
  gold_arn         = module.lake.bucket_arns["gold"]
  artifacts_bucket = module.lake.bucket_names["artifacts"]
  artifacts_arn    = module.lake.bucket_arns["artifacts"]
  glue_db          = module.lake.glue_db
  alerts_topic_arn = aws_sns_topic.alerts.arn
}

module "ml" {
  source = "./modules/ml"

  name             = local.name
  gold_arn         = module.lake.bucket_arns["gold"]
  artifacts_arn    = module.lake.bucket_arns["artifacts"]
  alerts_topic_arn = aws_sns_topic.alerts.arn
}

module "serving" {
  source = "./modules/serving"

  name   = local.name
  acct   = local.acct
  region = var.region
}

module "cicd" {
  source = "./modules/cicd"

  name         = local.name
  acct         = local.acct
  region       = var.region
  github_repo  = var.github_repo
  state_bucket = "tfstate-transitpulse-${local.acct}"
}

module "observability" {
  source = "./modules/observability"

  providers = {
    aws         = aws
    aws.useast1 = aws.useast1
  }

  name                     = local.name
  region                   = var.region
  alerts_topic_arn         = aws_sns_topic.alerts.arn
  alert_email              = var.alert_email
  poller_function_name     = module.ingest.poller_function_name
  poller_log_group         = module.ingest.poller_log_group
  poller_dlq_name          = module.ingest.poller_dlq_name
  kinesis_stream_name      = module.ingest.kinesis_stream_name
  killswitch_function_arn  = module.ingest.killswitch_function_arn
  daily_cost_threshold_usd = var.daily_cost_threshold_usd
}
