# Example Terraform Variables Configuration
# Minimal configuration: MSK + EMR cluster
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

vpc_id = "REDACTED"
private_subnet_ids = [
  "subnet-aaa111",
  "subnet-bbb222",
  "subnet-ccc333"
]

security_group_ids = {
  msk = "REDACTED"
  emr = "REDACTED"
}

################################################################################
# Feature Flags
################################################################################

create_msk         = true
create_emr_cluster = true

################################################################################
# Existing Resources (Discovery Override)
################################################################################

existing_bucket_names = {
  checkpoints = ""
  artifacts   = ""
  raw         = ""
}

existing_kms_keys = {
  data = ""
  msk  = ""
}

existing_msk_cluster_arn = "" # Provide ARN to reuse an existing MSK cluster

# Optional: pin the S3 bucket suffix if you need a predictable name
# bucket_name_suffix = "20240223"

################################################################################
# MSK Configuration
################################################################################

msk_instance_type   = "kafka.m7g.large"
msk_broker_count    = 3
msk_ebs_volume_size = 100
msk_kafka_version   = "3.6.0"

################################################################################
# EMR Cluster Configuration
################################################################################

emr_cluster_release_label        = "emr-7.10.0"
emr_cluster_master_instance_type = "m7g.large"
emr_cluster_core_instance_type   = "m7g.large"
emr_cluster_core_instance_count  = 2

################################################################################
# Monitoring and lifecycle
################################################################################

enable_cloudwatch_logs = true
log_retention_days     = 7
enable_alarms          = true
alarm_email            = "alerts@example.com"
