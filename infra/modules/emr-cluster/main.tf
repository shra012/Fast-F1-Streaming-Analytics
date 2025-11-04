# EMR Cluster (EC2) module

variable "project" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "release_label" {
  description = "EMR release label"
  type        = string
}

variable "master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
}

variable "core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
}

variable "core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
}

variable "subnet_id" {
  description = "Subnet ID for EMR cluster"
  type        = string
}

variable "applications" {
  description = "List of EMR applications to install"
  type        = list(string)
  default     = ["Spark"]
}

variable "security_group_ids" {
  description = "Additional security groups to attach"
  type        = list(string)
  default     = []
}

variable "log_uri" {
  description = "S3 URI for EMR logs"
  type        = string
  default     = ""
}

variable "s3_bucket_arns" {
  description = "Bucket ARNs the cluster requires access to"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

data "aws_iam_policy_document" "emr_service_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_service" {
  name               = "${var.project}-${var.environment}-emr-service-role"
  assume_role_policy = data.aws_iam_policy_document.emr_service_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

data "aws_iam_policy_document" "emr_ec2_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_ec2" {
  name               = "${var.project}-${var.environment}-emr-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.emr_ec2_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_ec2_managed" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

data "aws_iam_policy_document" "emr_s3_access" {
  count = length(var.s3_bucket_arns) > 0 ? 1 : 0

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = var.s3_bucket_arns
  }
}

resource "aws_iam_role_policy" "emr_s3_access" {
  count  = length(var.s3_bucket_arns) > 0 ? 1 : 0
  name   = "${var.project}-${var.environment}-emr-s3-policy"
  role   = aws_iam_role.emr_ec2.id
  policy = data.aws_iam_policy_document.emr_s3_access[0].json
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project}-${var.environment}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2.name
}

locals {
  log_uri = trimspace(var.log_uri)
}

resource "aws_emr_cluster" "main" {
  name          = "${var.project}-${var.environment}"
  release_label = var.release_label
  applications  = var.applications

  service_role = aws_iam_role.emr_service.arn
  log_uri      = local.log_uri != "" ? local.log_uri : null

  ec2_attributes {
    subnet_id                         = var.subnet_id
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    additional_master_security_groups = length(var.security_group_ids) > 0 ? join(",", var.security_group_ids) : null
    additional_slave_security_groups  = length(var.security_group_ids) > 0 ? join(",", var.security_group_ids) : null
  }

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
    name           = "master"
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    name           = "core"
  }

  configurations_json               = jsonencode([])
  visible_to_all_users              = true
  keep_job_flow_alive_when_no_steps = true

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-emr-cluster"
  })
}

output "cluster_id" {
  description = "EMR cluster ID"
  value       = aws_emr_cluster.main.id
}

output "master_public_dns" {
  description = "EMR master public DNS"
  value       = aws_emr_cluster.main.master_public_dns
}

output "service_role_arn" {
  description = "EMR service role ARN"
  value       = aws_iam_role.emr_service.arn
}

output "instance_profile_arn" {
  description = "EMR EC2 instance profile ARN"
  value       = aws_iam_instance_profile.emr_ec2.arn
}
