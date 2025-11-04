# Main Terraform configuration for F1 Streaming Graph Infrastructure
# Orchestrates conditional resource creation based on discovery

# Data sources for AWS account and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Data source for existing MSK cluster (if provided)
data "aws_msk_cluster" "existing" {
  count        = var.existing_msk_cluster_arn != "" ? 1 : 0
  cluster_name = split("/", var.existing_msk_cluster_arn)[1]
}

resource "time_static" "bucket_suffix" {}

# Locals for standardized naming and tagging
locals {
  name_prefix = "${var.project}-${var.environment}"

  common_tags = merge(
    var.tags,
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  )

  vpc_id             = var.vpc_id
  private_subnet_ids = var.private_subnet_ids

  # Ensure bucket-friendly prefix (lowercase, hyphen separated)
  bucket_prefix = replace(lower("${var.project}-${var.environment}"), "_", "-")

  # Generate deterministic suffix for bucket uniqueness (unless user overrides)
  generated_bucket_suffix = substr(md5(time_static.bucket_suffix.rfc3339), 0, 6)
  bucket_suffix           = var.bucket_name_suffix != "" ? replace(lower(var.bucket_name_suffix), "_", "-") : local.generated_bucket_suffix
  bucket_suffix_segment   = local.bucket_suffix != "" ? "-${local.bucket_suffix}" : ""

  # Use existing KMS keys if provided, otherwise use newly created ones
  kms_data_key_arn = var.existing_kms_keys.data != "" ? var.existing_kms_keys.data : try(module.kms.kms_key_arns["data"], "")
  kms_msk_key_arn  = var.existing_kms_keys.msk != "" ? var.existing_kms_keys.msk : try(module.kms.kms_key_arns["msk"], "")

  checkpoints_bucket = var.existing_bucket_names.checkpoints != "" ? var.existing_bucket_names.checkpoints : "${local.bucket_prefix}${local.bucket_suffix_segment}-checkpoints"
  artifacts_bucket   = var.existing_bucket_names.artifacts != "" ? var.existing_bucket_names.artifacts : "${local.bucket_prefix}${local.bucket_suffix_segment}-artifacts"
  raw_bucket         = var.existing_bucket_names.raw != "" ? var.existing_bucket_names.raw : "${local.bucket_prefix}${local.bucket_suffix_segment}-raw"

  checkpoints_bucket_arn = local.checkpoints_bucket != "" ? "arn:aws:s3:::${local.checkpoints_bucket}" : ""
  artifacts_bucket_arn   = local.artifacts_bucket != "" ? "arn:aws:s3:::${local.artifacts_bucket}" : ""
  raw_bucket_arn         = local.raw_bucket != "" ? "arn:aws:s3:::${local.raw_bucket}" : ""

  bronze_base_uri = local.raw_bucket != "" ? "s3://${local.raw_bucket}/bronze" : ""
  silver_base_uri = local.artifacts_bucket != "" ? "s3://${local.artifacts_bucket}/silver" : ""
  gold_base_uri   = local.artifacts_bucket != "" ? "s3://${local.artifacts_bucket}/gold" : ""
  checkpoints_uri = local.checkpoints_bucket != "" ? "s3://${local.checkpoints_bucket}/checkpoints" : ""

  security_groups = {
    msk = try(var.security_group_ids["msk"], "")
    emr = try(var.security_group_ids["emr"], "")
  }

  s3_bucket_arns = flatten([
    for name in [local.checkpoints_bucket, local.artifacts_bucket, local.raw_bucket] : [
      "arn:aws:s3:::${name}",
      "arn:aws:s3:::${name}/*"
    ] if name != ""
  ])

}

data "aws_vpc" "existing" {
  count = var.vpc_id != "" ? 1 : 0
  id    = var.vpc_id
}

################################################################################
# KMS Module - Encryption Keys
################################################################################

module "kms" {
  source = "./modules/kms"

  project     = var.project
  environment = var.environment
  region      = data.aws_region.current.id
  account_id  = data.aws_caller_identity.current.account_id

  # Only create keys if not using existing ones
  create_data_key = var.existing_kms_keys.data == ""
  create_msk_key  = var.existing_kms_keys.msk == ""

  tags = local.common_tags
}

################################################################################
# S3 Module - Buckets for Spark pipelines
################################################################################

module "s3" {
  source = "./modules/s3"

  project                 = var.project
  environment             = var.environment
  region                  = var.region
  checkpoints_bucket_name = local.checkpoints_bucket
  artifacts_bucket_name   = local.artifacts_bucket
  raw_bucket_name         = local.raw_bucket

  kms_key_arn = local.kms_data_key_arn

  create_checkpoints_bucket = var.existing_bucket_names.checkpoints == ""
  create_artifacts_bucket   = var.existing_bucket_names.artifacts == ""
  create_raw_bucket         = var.existing_bucket_names.raw == ""

  tags = local.common_tags

  depends_on = [module.kms]
}

################################################################################
# MSK Module - Kafka Cluster
################################################################################

module "msk" {
  source = "./modules/msk"
  count  = var.create_msk && var.existing_msk_cluster_arn == "" ? 1 : 0

  project            = var.project
  environment        = var.environment
  vpc_id             = local.vpc_id
  subnet_ids         = local.private_subnet_ids
  security_group_ids = [for sg in [local.security_groups.msk] : sg if sg != ""]

  instance_type   = var.msk_instance_type
  broker_count    = var.msk_broker_count
  ebs_volume_size = var.msk_ebs_volume_size
  kafka_version   = var.msk_kafka_version
  kms_key_arn     = local.kms_msk_key_arn

  tags = local.common_tags
}

################################################################################
# EMR Cluster (Optional EC2-based)
################################################################################

module "emr_cluster" {
  source = "./modules/emr-cluster"
  count  = var.create_emr_cluster ? 1 : 0

  project              = var.project
  environment          = var.environment
  release_label        = var.emr_cluster_release_label
  master_instance_type = var.emr_cluster_master_instance_type
  core_instance_type   = var.emr_cluster_core_instance_type
  core_instance_count  = var.emr_cluster_core_instance_count
  subnet_id            = element(local.private_subnet_ids, 0)
  security_group_ids   = [for sg in [local.security_groups.emr] : sg if sg != ""]
  log_uri              = local.artifacts_bucket != "" ? "s3://${local.artifacts_bucket}/emr-logs/" : ""
  s3_bucket_arns       = local.s3_bucket_arns
  applications         = ["Spark"]
  tags                 = local.common_tags

  depends_on = [module.s3]
}

################################################################################
# CloudWatch Alarms
################################################################################

# MSK: Under-Replicated Partitions
resource "aws_cloudwatch_metric_alarm" "msk_under_replicated_partitions" {
  count = var.create_msk || var.existing_msk_cluster_arn != "" ? 1 : 0

  alarm_name          = "${local.name_prefix}-msk-under-replicated-partitions"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "UnderReplicatedPartitions"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "MSK cluster has under-replicated partitions"
  treat_missing_data  = "notBreaching"

  dimensions = {
    "Cluster Name" = var.existing_msk_cluster_arn != "" ? split("/", var.existing_msk_cluster_arn)[1] : module.msk[0].cluster_name
  }

  tags = local.common_tags
}

# MSK: Disk Usage
resource "aws_cloudwatch_metric_alarm" "msk_disk_usage" {
  count = var.create_msk || var.existing_msk_cluster_arn != "" ? 1 : 0

  alarm_name          = "${local.name_prefix}-msk-disk-usage"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "KafkaDataLogsDiskUsedPercent"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "MSK cluster disk usage exceeds 80%"
  treat_missing_data  = "notBreaching"

  dimensions = {
    "Cluster Name" = var.existing_msk_cluster_arn != "" ? split("/", var.existing_msk_cluster_arn)[1] : module.msk[0].cluster_name
  }

  tags = local.common_tags
}

################################################################################
# Outputs Note
################################################################################
