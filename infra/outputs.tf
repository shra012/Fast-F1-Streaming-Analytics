# VPC Outputs
output "vpc_id" {
  description = "VPC ID"
  value       = var.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = var.private_subnet_ids
}

# Region Output
output "aws_region" {
  description = "AWS region for deployed resources"
  value       = data.aws_region.current.id
}

# Security Group Outputs
output "security_group_ids" {
  description = "Security group IDs"
  value       = var.security_group_ids
}

# S3 Outputs
output "s3_checkpoints_bucket" {
  description = "S3 bucket for checkpoints"
  value       = local.checkpoints_bucket
}

output "s3_artifacts_bucket" {
  description = "S3 bucket for artifacts"
  value       = local.artifacts_bucket
}

output "s3_raw_bucket" {
  description = "S3 bucket for raw data"
  value       = local.raw_bucket
}

output "s3_checkpoint_uri" {
  description = "S3 URI for Spark checkpoints"
  value       = local.checkpoints_bucket != "" ? "${local.checkpoints_uri}/" : ""
}

output "s3_artifacts_uri" {
  description = "S3 URI for artifacts"
  value       = local.artifacts_bucket != "" ? "s3://${local.artifacts_bucket}/" : ""
}

output "spark_bronze_base_uri" {
  description = "Default base URI for bronze (raw) Spark tables"
  value       = local.bronze_base_uri
}

output "spark_silver_base_uri" {
  description = "Default base URI for silver tables"
  value       = local.silver_base_uri
}

output "spark_gold_base_uri" {
  description = "Default base URI for gold tables"
  value       = local.gold_base_uri
}

# KMS Outputs
output "kms_key_arns" {
  description = "KMS key ARNs"
  value = {
    data = local.kms_data_key_arn
    msk  = local.kms_msk_key_arn
  }
}

# MSK Outputs
output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = var.existing_msk_cluster_arn != "" ? var.existing_msk_cluster_arn : (var.create_msk ? module.msk[0].cluster_arn : "")
}

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap brokers (plaintext)"
  value       = var.existing_msk_cluster_arn != "" ? data.aws_msk_cluster.existing[0].bootstrap_brokers : (var.create_msk ? module.msk[0].bootstrap_brokers : "")
  sensitive   = true
}

output "msk_zookeeper_connect_string" {
  description = "MSK Zookeeper connection string"
  value       = var.existing_msk_cluster_arn != "" ? data.aws_msk_cluster.existing[0].zookeeper_connect_string : (var.create_msk ? module.msk[0].zookeeper_connect_string : "")
  sensitive   = true
}

# EMR Outputs
output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = var.create_emr_cluster ? module.emr_cluster[0].cluster_id : ""
}

output "emr_cluster_master_public_dns" {
  description = "EMR master public DNS"
  value       = var.create_emr_cluster ? module.emr_cluster[0].master_public_dns : ""
}

output "emr_service_role_arn" {
  description = "EMR service role ARN"
  value       = var.create_emr_cluster ? module.emr_cluster[0].service_role_arn : ""
}

output "emr_instance_profile_arn" {
  description = "EMR EC2 instance profile ARN"
  value       = var.create_emr_cluster ? module.emr_cluster[0].instance_profile_arn : ""
}

output "emr_key_pair_name" {
  description = "EC2 key pair name associated with the EMR cluster"
  value       = local.emr_key_pair_name
}
