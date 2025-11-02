# MSK Module - Creates Amazon MSK (Kafka) cluster

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

variable "subnet_ids" {
  description = "Subnet IDs for MSK brokers"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for MSK"
  type        = list(string)
}

variable "instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m7g.large"
}

variable "broker_count" {
  description = "Number of MSK brokers"
  type        = number
  default     = 3
}

variable "ebs_volume_size" {
  description = "EBS volume size per broker in GB"
  type        = number
  default     = 100
}

variable "kafka_version" {
  description = "Apache Kafka version"
  type        = string
  default     = "3.6.0"
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project}-${var.environment}"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.instance_type
    client_subnets  = var.subnet_ids
    security_groups = var.security_group_ids

    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }

    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-msk"
  })
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project}-${var.environment}"
  retention_in_days = 7

  tags = var.tags
}

output "cluster_arn" {
  description = "MSK cluster ARN"
  value       = aws_msk_cluster.main.arn
}

output "cluster_name" {
  description = "MSK cluster name"
  value       = aws_msk_cluster.main.cluster_name
}

output "bootstrap_brokers_sasl_iam" {
  description = "MSK bootstrap brokers (SASL/IAM)"
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
  sensitive   = true
}

output "zookeeper_connect_string" {
  description = "MSK Zookeeper connection string"
  value       = aws_msk_cluster.main.zookeeper_connect_string
  sensitive   = true
}
