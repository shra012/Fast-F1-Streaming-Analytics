# Infrastructure

Terraform infrastructure for Kafka → Spark streaming pipeline.

## Quick Start

```bash
make preflight
terraform init
terraform plan
terraform apply -auto-approve
terraform output
```

## What This Deploys

- VPC & Networking: 3 AZ VPC with private/public subnets
- Amazon MSK: 3-broker Kafka cluster
- EMR Cluster: Spark/YARN for Structured Streaming
- S3 Buckets: raw, artifacts, checkpoints
- KMS Keys: Encryption keys
- Security Groups: Least-privilege rules
- IAM Roles: EMR service/instance roles

## Prerequisites

- Terraform >= 1.8.0
- AWS CLI >= 2.x
- Python >= 3.8 with boto3

## Makefile Targets

```bash
make preflight   # Run resource discovery
make init        # Initialize Terraform
make plan        # Show execution plan
make apply       # Apply changes
make destroy     # Destroy infrastructure
```
