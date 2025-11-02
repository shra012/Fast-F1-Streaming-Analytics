# Example Terraform Variables Configuration
# Minimal configuration: MSK + EMR Serverless + ECS Fargate producer, Neo4j Aura as external target.
# Copy this file to terraform.tfvars and customize values for your environment.

################################################################################
# Project Configuration
################################################################################

project     = "f1-streaming-graph"
environment = "dev"
region      = "us-east-1"

# Additional tags for all resources
tags = {
  Team        = "big-data-team"
  CostCenter  = "engineering"
  Application = "f1-analytics"
}

################################################################################
# VPC / Networking (reuse existing VPC)
################################################################################

# Supply existing networking primitives.
vpc_id = "REDACTED"
private_subnet_ids = [
  "subnet-aaa111",
  "subnet-bbb222",
  "subnet-ccc333"
]

security_group_ids = {
  msk      = "REDACTED"
  emr      = "REDACTED"
  producer = "REDACTED"
}

################################################################################
# Feature Flags (only MSK, EMR, and ECS producer enabled)
################################################################################

create_msk          = true
create_emr          = true
create_ecs_producer = true # Producer runs on AWS Fargate (ECS)

################################################################################
# Existing Resources (Discovery Override)
################################################################################

# Provide existing resources; Terraform no longer creates networking, KMS, or S3.
existing_bucket_names = {
  checkpoints = ""
  artifacts   = ""
  raw         = ""
}

existing_kms_keys = {
  data    = ""
  secrets = ""
  msk     = ""
}

existing_msk_cluster_arn  = "" # Provide ARN to reuse an existing MSK cluster
existing_emr_app_id       = "" # Provide EMR Serverless app ID if reusing
existing_neo4j_secret_arn = "" # ARN for Neo4j Aura credentials in Secrets Manager

################################################################################
# MSK Configuration
################################################################################

msk_instance_type   = "kafka.m7g.large"
msk_broker_count    = 3
msk_ebs_volume_size = 100
msk_kafka_version   = "3.6.0"

################################################################################
# EMR Configuration
################################################################################

# EMR Serverless release label
emr_release_label = "emr-7.10.0"

# EMR instance type reference (set to m5.large as requested). Note: EMR Serverless ignores EC2 instance types; this is provided for clarity or if switching to EMR on EC2.
emr_instance_type = "m5.large"

# Serverless capacity (workers) settings
emr_initial_capacity = {
  driver = {
    cpu    = "2 vCPU"
    memory = "8 GB"
  }
  executor = {
    cpu    = "4 vCPU"
    memory = "16 GB"
  }
}

emr_maximum_capacity = {
  cpu    = "100 vCPU"
  memory = "400 GB"
}

emr_auto_start_enabled = true
emr_auto_stop_enabled  = true
emr_idle_timeout       = 900 # seconds (15 minutes)

################################################################################
# ECS Producer Configuration (Fargate)
################################################################################

ecs_producer_cpu    = 512
ecs_producer_memory = 1024
# ecs_producer_image = "123456789012.dkr.ecr.us-east-1.amazonaws.com/f1-producer:latest"

################################################################################
# Monitoring and lifecycle
################################################################################

enable_cloudwatch_logs = true
log_retention_days     = 7
enable_alarms          = true
alarm_email            = "alerts@example.com"
