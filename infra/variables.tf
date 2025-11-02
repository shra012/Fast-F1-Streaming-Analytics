# Project Configuration
variable "project" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "f1-streaming-graph"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region for resource deployment"
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "vpc_id" {
  description = "Existing VPC ID to use for all resources"
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "List of existing private subnet IDs"
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "Map of existing security group IDs keyed by component (msk, emr, producer)"
  type        = map(string)
  default     = {}
}

variable "existing_bucket_names" {
  description = "Map of existing S3 bucket names (checkpoints, artifacts, raw)"
  type = object({
    checkpoints = string
    artifacts   = string
    raw         = string
  })
  default = {
    checkpoints = ""
    artifacts   = ""
    raw         = ""
  }
}

variable "existing_kms_keys" {
  description = "Map of existing KMS key ARNs (data, secrets, msk)"
  type = object({
    data    = string
    secrets = string
    msk     = string
  })
  default = {
    data    = ""
    secrets = ""
    msk     = ""
  }
}

variable "existing_msk_cluster_arn" {
  description = "ARN of existing MSK cluster to use"
  type        = string
  default     = ""
}

variable "existing_emr_app_id" {
  description = "ID of existing EMR Serverless application to use"
  type        = string
  default     = ""
}

variable "existing_secret_arn" {
  description = "ARN of existing Secrets Manager secret for Neo4j credentials"
  type        = string
  default     = ""
}

# Feature Flags
variable "create_ecs_producer" {
  description = "Whether to create the optional ECS producer service"
  type        = bool
  default     = false
}

variable "create_msk" {
  description = "Whether to create MSK cluster (false to use existing)"
  type        = bool
  default     = true
}

variable "create_emr" {
  description = "Whether to create EMR Serverless application (false to use existing)"
  type        = bool
  default     = true
}

# MSK Configuration
variable "msk_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.t3.small" # Bare minimum for testing/dev
}

variable "msk_broker_count" {
  description = "Number of MSK brokers (must match AZ count for HA)"
  type        = number
  default     = 3
}

variable "msk_ebs_volume_size" {
  description = "EBS volume size per broker in GB"
  type        = number
  default     = 10 # Minimum for bare bones setup
}

variable "msk_kafka_version" {
  description = "Apache Kafka version"
  type        = string
  default     = "3.6.0"
}

# EMR Serverless Configuration
variable "emr_release_label" {
  description = "EMR release label (Spark version)"
  type        = string
  default     = "emr-7.10.0"
}

variable "emr_instance_type" {
  description = "EMR instance type for non-serverless clusters or for reference (EMR Serverless does not accept EC2 instance types). Set to m5.large as default per requirement."
  type        = string
  default     = "m5.large"
}

variable "emr_initial_capacity_workers" {
  description = "Initial number of EMR Serverless workers"
  type        = number
  default     = 2
}

variable "emr_max_capacity_workers" {
  description = "Maximum number of EMR Serverless workers"
  type        = number
  default     = 10
}

# ECS Producer Configuration
variable "ecs_producer_cpu" {
  description = "CPU units for ECS producer task (1024 = 1 vCPU)"
  type        = number
  default     = 512
}

variable "ecs_producer_memory" {
  description = "Memory for ECS producer task in MB"
  type        = number
  default     = 1024
}

variable "ecs_producer_desired_count" {
  description = "Desired number of ECS producer tasks"
  type        = number
  default     = 1
}

variable "ecs_producer_image" {
  description = "Docker image for ECS producer (format: registry/image:tag)"
  type        = string
  default     = "public.ecr.aws/docker/library/alpine:latest" # Placeholder
}

# Neo4j Configuration
variable "existing_neo4j_secret_arn" {
  description = "ARN for existing Neo4j Aura credentials in Secrets Manager"
  type        = string
  default     = ""
}

# EMR Auto Scaling and Lifecycle
variable "emr_idle_timeout" {
  description = "EMR Serverless idle timeout in seconds"
  type        = number
  default     = 900 # 15 minutes
}

variable "emr_initial_capacity" {
  description = "EMR Serverless initial capacity configuration"
  type = object({
    driver = object({
      cpu    = string
      memory = string
    })
    executor = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    driver = {
      cpu    = "2 vCPU"
      memory = "8 GB"
    }
    executor = {
      cpu    = "4 vCPU"
      memory = "16 GB"
    }
  }
}

variable "emr_maximum_capacity" {
  description = "EMR Serverless maximum capacity configuration"
  type = object({
    cpu    = string
    memory = string
  })
  default = {
    cpu    = "100 vCPU"
    memory = "400 GB"
  }
}

variable "emr_auto_start_enabled" {
  description = "Enable EMR Serverless auto-start"
  type        = bool
  default     = true
}

variable "emr_auto_stop_enabled" {
  description = "Enable EMR Serverless auto-stop"
  type        = bool
  default     = true
}

# MSK Instance Count
variable "msk_instance_count" {
  description = "Number of MSK broker nodes (deprecated, use msk_broker_count)"
  type        = number
  default     = 3
}

# Monitoring and Logging
variable "enable_cloudwatch_logs" {
  description = "Enable CloudWatch logs for resources"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "enable_alarms" {
  description = "Enable CloudWatch alarms"
  type        = bool
  default     = true
}

variable "alarm_email" {
  description = "Email address for alarm notifications"
  type        = string
  default     = "alerts@example.com"
}
