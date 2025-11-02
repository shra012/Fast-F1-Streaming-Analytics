# VPC Outputs
output "vpc_id" {
  description = "VPC ID"
  value       = var.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = var.private_subnet_ids
}

# Security Group Outputs
output "security_group_ids" {
  description = "Security group IDs"
  value       = var.security_group_ids
}

# S3 Outputs
output "s3_checkpoints_bucket" {
  description = "S3 bucket for checkpoints"
  value       = var.existing_bucket_names.checkpoints
}

output "s3_artifacts_bucket" {
  description = "S3 bucket for artifacts"
  value       = var.existing_bucket_names.artifacts
}

output "s3_raw_bucket" {
  description = "S3 bucket for raw data"
  value       = var.existing_bucket_names.raw
}

output "s3_checkpoint_uri" {
  description = "S3 URI for Spark checkpoints"
  value       = var.existing_bucket_names.checkpoints != "" ? "s3://${var.existing_bucket_names.checkpoints}/checkpoints/" : ""
}

output "s3_artifacts_uri" {
  description = "S3 URI for artifacts"
  value       = var.existing_bucket_names.artifacts != "" ? "s3://${var.existing_bucket_names.artifacts}/" : ""
}

# KMS Outputs
output "kms_key_arns" {
  description = "KMS key ARNs"
  value = {
    data    = local.kms_data_key_arn
    secrets = local.kms_secrets_key_arn
    msk     = local.kms_msk_key_arn
  }
}

# MSK Outputs
output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = var.existing_msk_cluster_arn != "" ? var.existing_msk_cluster_arn : (var.create_msk ? module.msk[0].cluster_arn : "")
}

output "msk_bootstrap_brokers_sasl_iam" {
  description = "MSK bootstrap brokers (SASL/IAM)"
  value       = var.existing_msk_cluster_arn != "" ? data.aws_msk_cluster.existing[0].bootstrap_brokers_sasl_iam : (var.create_msk ? module.msk[0].bootstrap_brokers_sasl_iam : "")
  sensitive   = true
}

output "msk_zookeeper_connect_string" {
  description = "MSK Zookeeper connection string"
  value       = var.existing_msk_cluster_arn != "" ? data.aws_msk_cluster.existing[0].zookeeper_connect_string : (var.create_msk ? module.msk[0].zookeeper_connect_string : "")
  sensitive   = true
}

# EMR Serverless Outputs
output "emr_serverless_app_id" {
  description = "EMR Serverless application ID"
  value       = var.existing_emr_app_id != "" ? var.existing_emr_app_id : (var.create_emr ? module.emr_serverless[0].application_id : "")
}

output "emr_serverless_app_arn" {
  description = "EMR Serverless application ARN"
  value       = var.create_emr ? module.emr_serverless[0].application_arn : ""
}

output "emr_execution_role_arn" {
  description = "EMR Serverless execution role ARN"
  value       = aws_iam_role.emr_serverless_runtime.arn
}

# Secrets Manager Outputs
output "neo4j_secret_arn" {
  description = "Secrets Manager secret ARN for Neo4j credentials"
  value       = var.existing_secret_arn != "" ? var.existing_secret_arn : aws_secretsmanager_secret.neo4j[0].arn
}

# ECS Producer Outputs
output "ecs_cluster_name" {
  description = "ECS cluster name (if producer is enabled)"
  value       = var.create_ecs_producer ? module.ecs_producer[0].cluster_name : ""
}

output "ecs_service_name" {
  description = "ECS service name (if producer is enabled)"
  value       = var.create_ecs_producer ? module.ecs_producer[0].service_name : ""
}

# CloudWatch Outputs
output "cloudwatch_log_groups" {
  description = "CloudWatch log group names"
  value = {
    emr      = aws_cloudwatch_log_group.emr.name
    producer = var.create_ecs_producer ? aws_cloudwatch_log_group.ecs_producer[0].name : ""
  }
}

# IAM Role Outputs
output "producer_task_role_arn" {
  description = "IAM role ARN for ECS producer task (if enabled)"
  value       = var.create_ecs_producer ? aws_iam_role.ecs_producer_task[0].arn : ""
}
