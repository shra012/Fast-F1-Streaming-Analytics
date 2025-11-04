# VPC Endpoints Module - Creates VPC endpoints for private AWS service access

variable "project" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for interface endpoints"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for endpoints"
  type        = list(string)
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "create_s3_endpoint" {
  description = "Whether to create S3 endpoint"
  type        = bool
  default     = true
}

variable "create_kms_endpoint" {
  description = "Whether to create KMS endpoint"
  type        = bool
  default     = true
}

variable "create_logs_endpoint" {
  description = "Whether to create CloudWatch Logs endpoint"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}

# S3 Gateway Endpoint
resource "aws_vpc_endpoint" "s3" {
  count        = var.create_s3_endpoint ? 1 : 0
  vpc_id       = var.vpc_id
  service_name = "com.amazonaws.${var.region}.s3"

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-s3-endpoint"
  })
}

# KMS Interface Endpoint
resource "aws_vpc_endpoint" "kms" {
  count               = var.create_kms_endpoint ? 1 : 0
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.region}.kms"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = var.security_group_ids
  private_dns_enabled = true

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-kms-endpoint"
  })
}

# CloudWatch Logs Interface Endpoint
resource "aws_vpc_endpoint" "logs" {
  count               = var.create_logs_endpoint ? 1 : 0
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.region}.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = var.security_group_ids
  private_dns_enabled = true

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-logs-endpoint"
  })
}

output "endpoint_ids" {
  description = "VPC endpoint IDs"
  value = {
    s3   = var.create_s3_endpoint ? aws_vpc_endpoint.s3[0].id : ""
    kms  = var.create_kms_endpoint ? aws_vpc_endpoint.kms[0].id : ""
    logs = var.create_logs_endpoint ? aws_vpc_endpoint.logs[0].id : ""
  }
}
