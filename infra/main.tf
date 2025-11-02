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

  # Use existing KMS keys if provided, otherwise use newly created ones
  kms_data_key_arn    = var.existing_kms_keys.data != "" ? var.existing_kms_keys.data : try(module.kms.kms_key_arns["data"], "")
  kms_secrets_key_arn = var.existing_kms_keys.secrets != "" ? var.existing_kms_keys.secrets : try(module.kms.kms_key_arns["secrets"], "")
  kms_msk_key_arn     = var.existing_kms_keys.msk != "" ? var.existing_kms_keys.msk : try(module.kms.kms_key_arns["msk"], "")

  checkpoints_bucket = var.existing_bucket_names.checkpoints
  artifacts_bucket   = var.existing_bucket_names.artifacts
  raw_bucket         = var.existing_bucket_names.raw

  security_groups = {
    msk      = try(var.security_group_ids["msk"], "")
    emr      = try(var.security_group_ids["emr"], "")
    producer = try(var.security_group_ids["producer"], "")
  }

  s3_bucket_arns = flatten([
    for name in [local.checkpoints_bucket, local.artifacts_bucket, local.raw_bucket] : [
      "arn:aws:s3:::${name}",
      "arn:aws:s3:::${name}/*"
    ] if name != ""
  ])

  kms_policy_arns = compact([
    local.kms_data_key_arn,
    local.kms_secrets_key_arn
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
  create_data_key    = var.existing_kms_keys.data == ""
  create_secrets_key = var.existing_kms_keys.secrets == ""
  create_msk_key     = var.existing_kms_keys.msk == ""

  tags = local.common_tags
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
# EMR Serverless Module
################################################################################

module "emr_serverless" {
  source = "./modules/emr-serverless"
  count  = var.create_emr && var.existing_emr_app_id == "" ? 1 : 0

  project     = var.project
  environment = var.environment

  release_label      = var.emr_release_label
  subnet_ids         = local.private_subnet_ids
  security_group_ids = [for sg in [local.security_groups.emr] : sg if sg != ""]

  tags = local.common_tags

  depends_on = [aws_iam_role.emr_serverless_runtime]
}

################################################################################
# ECS Producer Module (Optional)
################################################################################

module "ecs_producer" {
  source = "./modules/ecs-producer"
  count  = var.create_ecs_producer ? 1 : 0

  project     = var.project
  environment = var.environment

  vpc_id             = local.vpc_id
  subnet_ids         = local.private_subnet_ids
  security_group_ids = [for sg in [local.security_groups.producer] : sg if sg != ""]

  container_image = var.ecs_producer_image
  cpu             = var.ecs_producer_cpu
  memory          = var.ecs_producer_memory
  desired_count   = var.ecs_producer_desired_count

  tags = local.common_tags

  depends_on = [module.msk, data.aws_msk_cluster.existing]
}

################################################################################
# IAM Roles and Policies
################################################################################

# EMR Serverless Runtime Role
resource "aws_iam_role" "emr_serverless_runtime" {
  name               = "${local.name_prefix}-emr-runtime-role"
  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role_policy" "emr_runtime_policy" {
  name   = "${local.name_prefix}-emr-runtime-policy"
  role   = aws_iam_role.emr_serverless_runtime.id
  policy = data.aws_iam_policy_document.emr_runtime.json
}

data "aws_iam_policy_document" "emr_runtime" {
  dynamic "statement" {
    for_each = length(local.s3_bucket_arns) > 0 ? [1] : []
    content {
      effect = "Allow"
      actions = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      resources = local.s3_bucket_arns
    }
  }

  # Secrets Manager access (Neo4j credentials only)
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    resources = [
      var.existing_secret_arn != "" ? var.existing_secret_arn : aws_secretsmanager_secret.neo4j[0].arn
    ]
  }

  # CloudWatch Logs
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless/*"
    ]
  }

  # MSK access (if using IAM auth)
  dynamic "statement" {
    for_each = var.create_msk || var.existing_msk_cluster_arn != "" ? [1] : []
    content {
      effect = "Allow"
      actions = [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:DescribeGroup"
      ]
      resources = [
        var.existing_msk_cluster_arn != "" ? var.existing_msk_cluster_arn : module.msk[0].cluster_arn,
        "${var.existing_msk_cluster_arn != "" ? var.existing_msk_cluster_arn : module.msk[0].cluster_arn}/*"
      ]
    }
  }

  # KMS decryption
  dynamic "statement" {
    for_each = length(local.kms_policy_arns) > 0 ? [1] : []
    content {
      effect = "Allow"
      actions = [
        "kms:Decrypt",
        "kms:DescribeKey"
      ]
      resources = local.kms_policy_arns
    }
  }
}

# ECS Producer Task Role (only if ECS producer is enabled)
resource "aws_iam_role" "ecs_producer_task" {
  count = var.create_ecs_producer ? 1 : 0

  name               = "${local.name_prefix}-ecs-producer-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role[0].json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "ecs_task_assume_role" {
  count = var.create_ecs_producer ? 1 : 0

  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role_policy" "ecs_producer_task_policy" {
  count = var.create_ecs_producer ? 1 : 0

  name   = "${local.name_prefix}-ecs-producer-task-policy"
  role   = aws_iam_role.ecs_producer_task[0].id
  policy = data.aws_iam_policy_document.ecs_producer_task[0].json
}

data "aws_iam_policy_document" "ecs_producer_task" {
  count = var.create_ecs_producer ? 1 : 0

  # MSK write access
  statement {
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:WriteData",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:CreateTopic"
    ]
    resources = [
      var.existing_msk_cluster_arn != "" ? var.existing_msk_cluster_arn : module.msk[0].cluster_arn,
      "${var.existing_msk_cluster_arn != "" ? var.existing_msk_cluster_arn : module.msk[0].cluster_arn}/*"
    ]
  }

  # CloudWatch Logs
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "${aws_cloudwatch_log_group.ecs_producer[0].arn}:*"
    ]
  }
}

# ECS Producer Execution Role
resource "aws_iam_role" "ecs_producer_execution" {
  count = var.create_ecs_producer ? 1 : 0

  name               = "${local.name_prefix}-ecs-producer-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role[0].json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_producer_execution_policy" {
  count = var.create_ecs_producer ? 1 : 0

  role       = aws_iam_role.ecs_producer_execution[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

################################################################################
# Secrets Manager - Neo4j Credentials
################################################################################

resource "aws_secretsmanager_secret" "neo4j" {
  count = var.existing_secret_arn == "" ? 1 : 0

  name        = "neo4j/aura"
  description = "Neo4j Aura database credentials for F1 streaming pipeline"
  kms_key_id  = local.kms_secrets_key_arn != "" ? local.kms_secrets_key_arn : null
  tags        = local.common_tags
}

resource "aws_secretsmanager_secret_version" "neo4j" {
  count = var.existing_secret_arn == "" ? 1 : 0

  secret_id = aws_secretsmanager_secret.neo4j[0].id
  secret_string = jsonencode({
    bolt_url = "neo4j+s://REPLACE_WITH_YOUR_AURA_URL.databases.neo4j.io"
    username = "neo4j"
    password = "REPLACE_WITH_YOUR_PASSWORD"
  })

  lifecycle {
    ignore_changes = [secret_string] # Allow manual updates without Terraform overwriting
  }
}

################################################################################
# CloudWatch Log Groups
################################################################################

resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${var.project}-${var.environment}"
  retention_in_days = 7
  # kms_key_id        = local.kms_data_key_arn  # Disabled: requires additional KMS permissions for CloudWatch
  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "ecs_producer" {
  count = var.create_ecs_producer ? 1 : 0

  name              = "/aws/ecs/${var.project}-${var.environment}-producer"
  retention_in_days = 7
  # kms_key_id        = local.kms_data_key_arn  # Disabled: requires additional KMS permissions for CloudWatch
  tags = local.common_tags
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

# EMR Serverless: Failed Job Runs (requires metric filter - simplified alarm)
# Note: This is a placeholder - actual implementation would need custom metrics
# from EMR Serverless job runs logged to CloudWatch

################################################################################
# Outputs Note
################################################################################

# NOTE: Neo4j Aura is an external service. This infrastructure only:
# 1. Stores credentials securely in Secrets Manager
# 2. Provides NAT Gateway for outbound HTTPS connectivity (bolt+s)
# 3. Does NOT create or manage Neo4j Aura instances
#
# You must:
# - Create a Neo4j Aura instance separately at https://console.neo4j.io/
# - Update the secret with actual credentials: aws secretsmanager update-secret --secret-id neo4j/aura
# - Add NAT Gateway public IP(s) to Aura IP allowlist if required
