# EMR Serverless Module - Creates EMR Serverless application

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
  default     = "emr-7.10.0"
}

variable "subnet_ids" {
  description = "Subnet IDs for EMR Serverless"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for EMR Serverless"
  type        = list(string)
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}

resource "aws_emrserverless_application" "main" {
  name          = "${var.project}-${var.environment}"
  release_label = var.release_label
  type          = "Spark"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "8 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"

    initial_capacity_config {
      worker_count = 2
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
      }
    }
  }

  maximum_capacity {
    cpu    = "100 vCPU"
    memory = "400 GB"
  }

  network_configuration {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-emr"
  })
}

output "application_id" {
  description = "EMR Serverless application ID"
  value       = aws_emrserverless_application.main.id
}

output "application_arn" {
  description = "EMR Serverless application ARN"
  value       = aws_emrserverless_application.main.arn
}
