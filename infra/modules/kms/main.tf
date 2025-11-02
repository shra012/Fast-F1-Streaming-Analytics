# KMS Module - Creates KMS keys for encryption

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

variable "account_id" {
  description = "AWS account ID"
  type        = string
}

variable "create_data_key" {
  description = "Whether to create data encryption key"
  type        = bool
  default     = true
}

variable "create_secrets_key" {
  description = "Whether to create secrets encryption key"
  type        = bool
  default     = true
}

variable "create_msk_key" {
  description = "Whether to create MSK encryption key"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}

resource "aws_kms_key" "data" {
  count               = var.create_data_key ? 1 : 0
  description         = "${var.project} data encryption key"
  enable_key_rotation = true

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-data-key"
  })
}

resource "aws_kms_alias" "data" {
  count         = var.create_data_key ? 1 : 0
  name          = "alias/${var.project}-data"
  target_key_id = aws_kms_key.data[0].key_id
}

resource "aws_kms_key" "secrets" {
  count               = var.create_secrets_key ? 1 : 0
  description         = "${var.project} secrets encryption key"
  enable_key_rotation = true

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-secrets-key"
  })
}

resource "aws_kms_alias" "secrets" {
  count         = var.create_secrets_key ? 1 : 0
  name          = "alias/${var.project}-secrets"
  target_key_id = aws_kms_key.secrets[0].key_id
}

resource "aws_kms_key" "msk" {
  count               = var.create_msk_key ? 1 : 0
  description         = "${var.project} MSK encryption key"
  enable_key_rotation = true

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-msk-key"
  })
}

resource "aws_kms_alias" "msk" {
  count         = var.create_msk_key ? 1 : 0
  name          = "alias/${var.project}-msk"
  target_key_id = aws_kms_key.msk[0].key_id
}

output "key_arns" {
  description = "KMS key ARNs"
  value = {
    data    = var.create_data_key ? aws_kms_key.data[0].arn : ""
    secrets = var.create_secrets_key ? aws_kms_key.secrets[0].arn : ""
    msk     = var.create_msk_key ? aws_kms_key.msk[0].arn : ""
  }
}
