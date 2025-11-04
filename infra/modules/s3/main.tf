# S3 Module - Creates S3 buckets for checkpoints, artifacts, and raw data

variable "project" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "checkpoints_bucket_name" {
  description = "Name for the checkpoints bucket when created"
  type        = string
}

variable "artifacts_bucket_name" {
  description = "Name for the artifacts bucket when created"
  type        = string
}

variable "raw_bucket_name" {
  description = "Name for the raw bucket when created"
  type        = string
}

variable "create_checkpoints_bucket" {
  description = "Whether to create checkpoints bucket"
  type        = bool
  default     = true
}

variable "create_artifacts_bucket" {
  description = "Whether to create artifacts bucket"
  type        = bool
  default     = true
}

variable "create_raw_bucket" {
  description = "Whether to create raw data bucket"
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "KMS key ARN for S3 encryption"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}

resource "aws_s3_bucket" "checkpoints" {
  count  = var.create_checkpoints_bucket ? 1 : 0
  bucket = var.checkpoints_bucket_name

  tags = merge(var.tags, {
    Name = var.checkpoints_bucket_name
    Type = "checkpoints"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "checkpoints" {
  count  = var.create_checkpoints_bucket ? 1 : 0
  bucket = aws_s3_bucket.checkpoints[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "checkpoints" {
  count  = var.create_checkpoints_bucket ? 1 : 0
  bucket = aws_s3_bucket.checkpoints[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "artifacts" {
  count  = var.create_artifacts_bucket ? 1 : 0
  bucket = var.artifacts_bucket_name

  tags = merge(var.tags, {
    Name = var.artifacts_bucket_name
    Type = "artifacts"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  count  = var.create_artifacts_bucket ? 1 : 0
  bucket = aws_s3_bucket.artifacts[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  count  = var.create_artifacts_bucket ? 1 : 0
  bucket = aws_s3_bucket.artifacts[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "raw" {
  count  = var.create_raw_bucket ? 1 : 0
  bucket = var.raw_bucket_name

  tags = merge(var.tags, {
    Name = var.raw_bucket_name
    Type = "raw"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  count  = var.create_raw_bucket ? 1 : 0
  bucket = aws_s3_bucket.raw[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  count  = var.create_raw_bucket ? 1 : 0
  bucket = aws_s3_bucket.raw[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "bucket_names" {
  description = "S3 bucket names"
  value = {
    checkpoints = var.checkpoints_bucket_name
    artifacts   = var.artifacts_bucket_name
    raw         = var.raw_bucket_name
  }
}

output "bucket_arns" {
  description = "S3 bucket ARNs"
  value = {
    checkpoints = var.checkpoints_bucket_name != "" ? "arn:aws:s3:::${var.checkpoints_bucket_name}" : ""
    artifacts   = var.artifacts_bucket_name != "" ? "arn:aws:s3:::${var.artifacts_bucket_name}" : ""
    raw         = var.raw_bucket_name != "" ? "arn:aws:s3:::${var.raw_bucket_name}" : ""
  }
}
