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
  description = "Map of existing security group IDs keyed by component (msk, emr)"
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
  description = "Map of existing KMS key ARNs (data, msk)"
  type = object({
    data = string
    msk  = string
  })
  default = {
    data = ""
    msk  = ""
  }
}

variable "existing_msk_cluster_arn" {
  description = "ARN of existing MSK cluster to use"
  type        = string
  default     = ""
}

variable "bucket_name_suffix" {
  description = "Optional suffix appended to auto-created S3 bucket names (leave empty to use generated hash)"
  type        = string
  default     = ""
}

# Feature Flags

variable "create_msk" {
  description = "Whether to create MSK cluster (false to use existing)"
  type        = bool
  default     = true
}


variable "create_emr_cluster" {
  description = "Whether to create EMR cluster (EC2) with master/core nodes"
  type        = bool
  default     = false
}

variable "emr_existing_key_name" {
  description = "Existing EC2 key pair name to attach to the EMR cluster (leave blank to create one)"
  type        = string
  default     = ""
}

variable "emr_key_pair_name" {
  description = "Key pair name to create for EMR when no existing key is provided"
  type        = string
  default     = ""
}

variable "emr_ssh_public_key_path" {
  description = "Path to the SSH public key used to create the EMR key pair (ignored when emr_existing_key_name is set)"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "emr_ssh_ingress_cidrs" {
  description = "CIDR blocks allowed to SSH into EMR master/core nodes (e.g., [\"203.0.113.10/32\"])"
  type        = list(string)
  default     = []
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

variable "msk_client_ingress_cidrs" {
  description = "List of CIDR blocks allowed to connect to MSK (e.g., your laptop IP)"
  type        = list(string)
  default     = []
}

variable "emr_cluster_master_instance_type" {
  description = "Instance type for EMR cluster master node"
  type        = string
  default     = "m5.large"
}

variable "emr_cluster_core_instance_type" {
  description = "Instance type for EMR cluster core nodes"
  type        = string
  default     = "m5.large"
}

variable "emr_cluster_core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
  default     = 2
}

variable "emr_cluster_release_label" {
  description = "EMR release label for the EC2-based cluster"
  type        = string
  default     = "emr-7.10.0"
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
