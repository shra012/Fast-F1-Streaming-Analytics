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
  description = "List of subnet IDs for Kafka nodes (should be at least 3)"
  type        = list(string)
}

variable "instance_type" {
  description = "EC2 instance type for Kafka nodes"
  type        = string
  default     = "t3.medium"
}

variable "node_count" {
  description = "Number of Kafka nodes"
  type        = number
  default     = 3
}

variable "ec2_key_name" {
  description = "EC2 key pair name for SSH access (leave empty to create new key pair)"
  type        = string
  default     = ""
}

variable "ssh_public_key" {
  description = "SSH public key content (required if ec2_key_name is empty)"
  type        = string
  default     = ""
}

variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "3.6.0"
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access Kafka and UIs (default: 0.0.0.0/0 for all)"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = ""
}

