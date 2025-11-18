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

vpc_id = "vpc-05996c242ac205af2"
private_subnet_ids = [
  "subnet-aaa111",
  "subnet-bbb222",
  "subnet-ccc333"
]

security_group_ids = {
  msk = "sg-1234567890abcdef0"
  emr = "sg-abcdef01234567890"
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

existing_msk_cluster_arn = "arn:aws:kafka:us-east-1:061391278833:cluster/f1-streaming-graph-dev/a4bdf626-1722-4df1-b27c-de62ffe2cd90-19" # Provide ARN to reuse an existing MSK cluster
emr_existing_key_name    = "" # Set if you already have a key pair in AWS
emr_key_pair_name        = "" # Optional custom name when creating a new key pair
emr_ssh_public_key_path  = "~/.ssh/id_rsa.pub"
emr_ssh_ingress_cidrs    = ["203.0.113.10/32"] # Replace with your public IP address

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
emr_cluster_master_instance_type = "m4.large"
emr_cluster_core_instance_type   = "m4.large"
emr_cluster_core_instance_count  = 4

################################################################################
# Monitoring and lifecycle
################################################################################

enable_cloudwatch_logs = true
log_retention_days     = 7
enable_alarms          = true
alarm_email            = "alerts@example.com"
