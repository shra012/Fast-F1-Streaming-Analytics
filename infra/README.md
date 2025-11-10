# F1 Streaming Graph Infrastructure

Production-grade Terraform infrastructure for a Kafka -> Spark streaming pipeline with intelligent AWS resource discovery.

## Architecture

```
+-----------------+      +----------------------------+
|  Amazon MSK     |----->|  EMR Cluster (Spark/YARN)  |
|  (Kafka, IAM)   |      |  - Bronze/Silver/Gold jobs |
+-----------------+      +-------------+--------------+
                                       |
                                +------v------+
                                |   Amazon S3 |  (raw / artifacts / checkpoints)
                                +-------------+

Private subnets host MSK brokers and EMR core nodes.
Public subnets provide outbound access via a NAT Gateway.
Gateway/interface VPC endpoints keep S3, KMS, and CloudWatch traffic inside the VPC.
All data in transit uses TLS and KMS CMKs protect data at rest.
```

## What This Deploys

### Core Infrastructure
- **VPC & Networking**: 3 AZ VPC with private/public subnets, NAT Gateway, Internet Gateway
- **Amazon MSK**: 3-broker Kafka cluster with IAM authentication and TLS encryption
- **EMR Cluster (EC2)**: One master, two core nodes running Spark/YARN for Structured Streaming
- **S3 Buckets**: raw landing zone, artifacts (silver/gold), checkpoints - all SSE-KMS encrypted with deterministic 6-char suffix for global uniqueness (override via `bucket_name_suffix`)
- **SSH Key Pair**: Imports your local public key (or reuses an existing AWS key pair) for EMR SSH access
- **KMS Keys**: Dedicated CMKs for data and MSK encryption
- **Security Groups**: Least-privilege rules for MSK and EMR cluster traffic
- **VPC Endpoints**: S3 gateway + interface endpoints for KMS/Logs
- **CloudWatch**: Log groups and alarms for MSK/EMR observability
- **IAM Roles**: EMR service/instance roles and supporting policies

### External Dependencies

## Cost Considerations

### Significant Costs (Production)
- **NAT Gateway**: ~$32/month + data transfer ($0.045/GB)
- **MSK**: kafka.m7g.large x 3 brokers = ~$500/month + storage
- **EMR Cluster**: EC2 instances (master + core) billed hourly (plus storage/EBS)
- **VPC Interface Endpoints**: ~$7/endpoint/month x 3 = ~$21/month

### Development Cost Optimization
For dev/test environments, use single NAT Gateway and smaller MSK instances:
```hcl
# In terraform.tfvars
nat_gateway_count = 1  # Single NAT for dev (warning: single point of failure)
msk_instance_type = "kafka.t3.small"  # Smaller instance for testing
```

**Warning**: Single NAT Gateway eliminates redundancy. Only use for non-production.

### Estimated Monthly Costs
- **Minimal (dev)**: ~$350/month (1 NAT, t3.small MSK, m5.large EMR cluster, light usage)
- **Production**: ~$900-1100/month (HA NAT, m7g.large MSK, m5.xlarge EMR cluster, moderate usage)

## Prerequisites

### Required Tools
- **Terraform**: >= 1.8.0
- **AWS CLI**: >= 2.x, configured with credentials
- **Python**: >= 3.8 with `boto3` library
- **Make**: Standard GNU Make

### Install Dependencies
```bash
# Install boto3 for discovery script
pip3 install boto3

# Verify Terraform version
terraform version

# Configure AWS credentials
aws configure
```

### AWS Permissions Required
The IAM user/role running this needs:
- VPC: Describe/Create VPCs, Subnets, Security Groups, Endpoints
- MSK: Describe/Create Kafka clusters
- EMR: Describe/Create clusters (EC2), IAM instance/service roles, steps
- S3: List/Create/Configure buckets
- KMS: Describe/Create keys and aliases
- IAM: Create roles and policies
- CloudWatch: Create log groups and alarms

## Quick Start (One Command Flow)

```bash
# 1. Discover existing AWS resources and generate config
make preflight

# 2. Initialize Terraform
terraform init

# 3. Review the plan
terraform plan

# 4. Apply (creates ONLY missing resources)
terraform apply -auto-approve

# 5. View outputs
terraform output
```

## How Discovery Works

The `preflight.py` script intelligently discovers existing AWS resources to avoid duplicates:

### Discovery Logic

1. **VPC Detection**:
   - Checks `VPC_ID` environment variable first
   - Searches for VPCs tagged with project name
   - Falls back to largest non-default VPC with >=2 private subnets
   - Prefers creating NEW VPC over reusing default VPC (safety)

2. **Subnet Classification**:
   - Private: Tagged `Tier=private` OR routes to NAT Gateway
   - Public: Tagged `Tier=public` OR routes to Internet Gateway

3. **Resource Discovery**:
   - **S3 Buckets**: Matches `{project}-checkpoints|artifacts|raw` (case-insensitive)
   - **KMS Keys**: Finds aliases `alias/{project}-data|msk`
   - **MSK Clusters**: Tagged `Project={project}`

4. **Output**: `generated.auto.tfvars.json` with discovered resource IDs

### Discovery Override

To override discovery, set explicit values:

> **SSH access**: by default Terraform imports `~/.ssh/id_rsa.pub` into a key pair named `<project>-<environment>-emr-key`. Provide `emr_existing_key_name` to reuse an existing AWS key pair instead, or change `emr_key_pair_name` / `emr_ssh_public_key_path` to match your environment. The corresponding private key is needed for `ssh`/`scp`.
> Set `emr_ssh_ingress_cidrs = ["<your-public-ip>/32"]` to open port 22 on the EMR master/core nodes for your workstation.

```hcl
# terraform.tfvars
vpc_id = "vpc-abc123"
private_subnet_ids = ["subnet-111", "subnet-222", "subnet-333"]
security_group_ids = {
  msk = "sg-aaa111"
  emr = "sg-bbb222"
}

existing_bucket_names = {
  checkpoints = "my-checkpoints-bucket"
  artifacts   = "my-artifacts-bucket"
  raw         = "my-raw-bucket"
}

existing_kms_keys = {
  data = "arn:aws:kms:us-east-1:123456789012:key/..."
  msk  = "arn:aws:kms:us-east-1:123456789012:key/..."
}

existing_msk_cluster_arn = "arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/..."
emr_existing_key_name   = ""                          # Provide existing key pair name or leave blank
emr_key_pair_name       = "f1-streaming-dev-emr-key"  # Optional custom name when creating a key pair
emr_ssh_public_key_path = "~/.ssh/id_rsa.pub"
emr_ssh_ingress_cidrs  = ["203.0.113.10/32"]          # Replace with your workstation's public IP

# Optional: provide a fixed suffix if you need predictable bucket names
# bucket_name_suffix = "20240223153000"
```

## Running Spark Jobs on the EMR Cluster

### 1. Package and Upload the Application
Use the helper Makefile in the `spark/` directory to bundle shared code and entry points:
```bash
cd spark
make package
make upload S3_PREFIX=s3://<artifact-bucket>/spark
```
This uploads `spark_package.zip` plus the bronze/silver/gold entry point scripts to the specified prefix.

### 2. Connect to the Cluster
Fetch the cluster outputs after apply:
```bash
terraform output emr_cluster_id
terraform output emr_cluster_master_dns
```
SSH to the master node (ensure the EMR security group allows your IP/key pair):
```bash
ssh -i <key.pem> hadoop@$(terraform output -raw emr_cluster_master_dns)
```

### 3. Submit the Streaming Job (YARN)
From the master node run `spark-submit` against YARN:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files s3://<artifact-bucket>/spark/spark_package.zip \
  s3://<artifact-bucket>/spark/bronze_stream.py \
  --bootstrap-servers "$(terraform output -raw msk_bootstrap_brokers_sasl_iam)" \
  --telemetry-topic telemetry.raw \
  --events-topic race.events \
  --output-base $SPARK_BRONZE_BASE \
  --checkpoint-base $SPARK_CHECKPOINT_BASE/bronze
```
Repeat for the silver/gold entry points with their respective output/checkpoint paths or CLI overrides.

### 4. Monitor Jobs
- YARN ResourceManager UI (port-forward via SSH if required)
- `yarn application -list` and `yarn logs -applicationId <app-id>`
- CloudWatch log group `/aws/emr/<cluster-name>` for driver/executor logs

## Validation & Linting

```bash
# Validate Terraform syntax
terraform validate

# Format Terraform files
terraform fmt -recursive

# Run tflint (install first: brew install tflint)
tflint --init
tflint

# Validate AWS credentials
aws sts get-caller-identity
```

## Project Structure

```
.
+-- README.md                          # This file
+-- Makefile                           # Build automation
+-- preflight.py                       # AWS resource discovery script
+-- versions.tf                        # Terraform version constraints
+-- providers.tf                       # AWS provider configuration
+-- variables.tf                       # Input variables
+-- outputs.tf                         # Output values
+-- main.tf                            # Main infrastructure orchestration
+-- examples/
|   +-- terraform.tfvars.example      # Example variable values
+-- modules/
    +-- vpc/                          # VPC, subnets, NAT, IGW
    +-- s3/                           # S3 buckets with encryption
    +-- kms/                          # KMS keys for encryption
    +-- sg/                           # Security groups
    +-- msk/                          # Amazon MSK cluster
    +-- emr-cluster/                 # EMR Spark cluster (EC2)
    +-- vpc-endpoints/                # VPC endpoints
```

## Makefile Targets

```bash
make preflight   # Run resource discovery
make init        # Initialize Terraform
make plan        # Show execution plan
make apply       # Apply changes
make destroy     # Destroy infrastructure
make fmt         # Format Terraform files
make validate    # Validate configuration
make lint        # Run tflint
make clean       # Remove generated files
```

## Terraform Backend

By default, state is stored locally. For team collaboration, configure S3 backend:

```hcl
# backend.tf
terraform {
  backend "s3" {
    bucket         = "your-terraform-state-bucket"
    key            = "f1-streaming-graph/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-locks"
  }
}
```

## Troubleshooting

### Discovery Issues
```bash
# Run discovery with debug output
python3 preflight.py --debug

# Manually set VPC
export VPC_ID=vpc-abc123
make preflight
```

### Resource Already Exists
If `terraform apply` fails with "already exists", the discovery missed it. Add to `terraform.tfvars`:
```hcl
existing_msk_cluster_arn = "arn:aws:kafka:..."
```

### MSK Connection Issues
- Verify security groups allow 9092 from EMR SG
- Check IAM role has `kafka-cluster:Connect` permission
- Ensure EMR in same VPC as MSK

- Verify NAT Gateway is working: `terraform output nat_gateway_public_ips`

## Security Best Practices

- All data encrypted at rest (KMS) and in transit (TLS)
- Least-privilege IAM roles and security groups
- Private subnets for compute (no direct internet access)
- S3 buckets block public access and enforce TLS
- MSK uses IAM authentication (no credentials in code)
- CloudWatch alarms for operational monitoring

## License

MIT
