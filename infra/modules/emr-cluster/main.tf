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

variable "ec2_key_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
  default     = ""
}

variable "ssh_ingress_cidrs" {
  description = "CIDR blocks allowed to SSH to EMR master node"
  type        = list(string)
  default     = []
}

variable "s3_bucket_arns" {
  description = "Bucket ARNs the cluster requires access to"
  type        = list(string)
  default     = []
}

variable "data_kms_key_arn" {
  description = "KMS key ARN used for S3 data encryption (granted decrypt access)"
  type        = string
  default     = ""
}

variable "msk_cluster_arn" {
  description = "MSK cluster ARN for Kafka access"
  type        = string
  default     = ""
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

resource "aws_iam_role_policy" "emr_kms_access" {
  name = "${var.project}-${var.environment}-emr-kms-policy"
  role = aws_iam_role.emr_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:ReEncrypt*"
        ]
        Resource = var.data_kms_key_arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "emr_msk_access" {
  count = var.msk_cluster_arn != "" ? 1 : 0
  name  = "${var.project}-${var.environment}-emr-msk-policy"
  role  = aws_iam_role.emr_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeCluster"
        ]
        Resource = var.msk_cluster_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:*Topic*",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData"
        ]
        Resource = "${var.msk_cluster_arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ]
        Resource = "${var.msk_cluster_arn}/*"
      }
    ]
  })
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

  lifecycle {
    ignore_changes = [
      log_uri,
      os_release_label,
      master_instance_group[0].ebs_config,
      core_instance_group[0].ebs_config,
      ec2_attributes[0].subnet_ids,
      ec2_attributes[0].emr_managed_master_security_group,
      ec2_attributes[0].emr_managed_slave_security_group
    ]
  }

  service_role = aws_iam_role.emr_service.arn
  log_uri      = local.log_uri != "" ? local.log_uri : null

  ec2_attributes {
    subnet_id                         = var.subnet_id
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    additional_master_security_groups = length(var.security_group_ids) > 0 ? join(",", var.security_group_ids) : null
    additional_slave_security_groups  = length(var.security_group_ids) > 0 ? join(",", var.security_group_ids) : null
    key_name                          = var.ec2_key_name != "" ? var.ec2_key_name : null
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

# Security group rule to allow SSH access to EMR master node
resource "aws_security_group_rule" "emr_master_ssh" {
  count             = length(var.ssh_ingress_cidrs) > 0 ? 1 : 0
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = var.ssh_ingress_cidrs
  security_group_id = aws_emr_cluster.main.ec2_attributes[0].emr_managed_master_security_group
  description       = "Allow SSH access to EMR master node"
}

# Security group rules for EMR Web UIs
resource "aws_security_group_rule" "emr_web_ui" {
  for_each = {
    "yarn_resourcemanager"     = { port = 8088, description = "YARN ResourceManager Web UI" }
    "yarn_nodemanager"         = { port = 8042, description = "YARN NodeManager Web UI" }
    "yarn_nodemanager_logs"    = { port = 8041, description = "YARN NodeManager Log Server" }
    "spark_history_server"     = { port = 18080, description = "Spark History Server" }
    "hadoop_namenode"          = { port = 9870, description = "Hadoop NameNode Web UI" }
    "hadoop_datanode"          = { port = 9864, description = "Hadoop DataNode Web UI" }
    "hdfs_namenode_secondary"  = { port = 9868, description = "HDFS Secondary NameNode" }
    "ganglia"                  = { port = 80, description = "Ganglia Monitoring" }
    "hue"                      = { port = 8888, description = "Hue Web UI" }
    "livy"                     = { port = 8998, description = "Livy REST API" }
    "zeppelin"                 = { port = 8890, description = "Zeppelin Notebook" }
    "jupyter"                  = { port = 9443, description = "JupyterHub" }
  }

  type              = "ingress"
  from_port         = each.value.port
  to_port           = each.value.port
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_emr_cluster.main.ec2_attributes[0].emr_managed_master_security_group
  description       = each.value.description
}

resource "aws_security_group_rule" "emr_worker_web_ui" {
  for_each = {
    "yarn_nodemanager"         = { port = 8042, description = "YARN NodeManager Web UI" }
    "yarn_nodemanager_logs"    = { port = 8041, description = "YARN NodeManager Log Server" }
    "spark_worker"             = { port = 18081, description = "Spark Executor Log UI" }
    "hadoop_datanode"          = { port = 9864, description = "Hadoop DataNode Web UI" }
    "hdfs_namenode_secondary"  = { port = 9868, description = "HDFS Secondary NameNode" }
  }

  type              = "ingress"
  from_port         = each.value.port
  to_port           = each.value.port
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_emr_cluster.main.ec2_attributes[0].emr_managed_slave_security_group
  description       = each.value.description
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

output "master_security_group_id" {
  description = "EMR master security group ID"
  value       = aws_emr_cluster.main.ec2_attributes[0].emr_managed_master_security_group
}

output "core_security_group_id" {
  description = "EMR core/worker security group ID"
  value       = aws_emr_cluster.main.ec2_attributes[0].emr_managed_slave_security_group
}
