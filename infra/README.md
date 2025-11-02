# F1 Streaming Graph Infrastructure

Production-grade Terraform infrastructure for a Kafka → Spark → Neo4j streaming pipeline with intelligent AWS resource discovery.

## Architecture

```
┌─────────────────┐
│ Producer (ECS)  │ (optional)
│   Container     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐      ┌──────────────┐
│  Amazon MSK     │─────▶│ EMR Serverless   │─────▶│  Neo4j Aura  │
│  (Kafka)        │      │ (Spark Streaming)│      │  (External)  │
│  IAM Auth       │      │                  │      │              │
└─────────────────┘      └──────────────────┘      └──────────────┘
         │                        │                        ▲
         │                        ▼                        │
         │               ┌─────────────────┐              │
         │               │  S3 Buckets     │              │
         │               │  - checkpoints  │              │
         │               │  - artifacts    │              │
         │               │  - raw data     │              │
         │               └─────────────────┘              │
         │                                                 │
         └─────────────── NAT Gateway ────────────────────┘
                         (Public Egress)

Private Subnets: MSK, EMR, ECS
Public Subnets: NAT Gateway
VPC Endpoints: S3 (gateway), Secrets Manager, KMS, CloudWatch Logs (interface)
Encryption: KMS CMKs for S3, Secrets Manager, MSK
Security: Least-privilege SGs, IAM policies, TLS in-transit
```

## What This Deploys

### Core Infrastructure (Conditionally Created)
- **VPC & Networking**: 3 AZ VPC with private/public subnets, NAT Gateway, Internet Gateway
- **Amazon MSK**: 3-broker Kafka cluster with IAM authentication and TLS encryption
- **EMR Serverless**: Spark application for structured streaming
- **S3 Buckets**: checkpoints, artifacts, raw data (SSE-KMS encrypted)
- **KMS Keys**: Separate CMKs for data, secrets, and MSK encryption
- **Security Groups**: Strict least-privilege rules for MSK, EMR, and Producer
- **VPC Endpoints**: S3 gateway + interface endpoints for Secrets/KMS/Logs
- **Secrets Manager**: Encrypted storage for Neo4j Aura credentials
- **CloudWatch**: Log groups and alarms for monitoring
- **IAM Roles**: Runtime roles for EMR and ECS with minimal permissions

### Optional Components
- **ECS Producer**: Fargate task for Kafka message production (toggle with `create_ecs_producer`)

### External Dependencies
- **Neo4j Aura**: External managed graph database (you provide credentials)

## Cost Considerations

### Significant Costs (Production)
- **NAT Gateway**: ~$32/month + data transfer ($0.045/GB)
- **MSK**: kafka.m7g.large × 3 brokers = ~$500/month + storage
- **EMR Serverless**: Pay per vCPU-hour and memory-GB-hour when running jobs
- **VPC Interface Endpoints**: ~$7/endpoint/month × 3 = ~$21/month

### Development Cost Optimization
For dev/test environments, use single NAT Gateway and smaller MSK instances:
```hcl
# In terraform.tfvars
nat_gateway_count = 1  # Single NAT for dev (warning: single point of failure)
msk_instance_type = "kafka.t3.small"  # Smaller instance for testing
```

**Warning**: Single NAT Gateway eliminates redundancy. Only use for non-production.

### Estimated Monthly Costs
- **Minimal (dev)**: ~$250/month (1 NAT, t3.small MSK, minimal usage)
- **Production**: ~$600-800/month (HA NAT, m7g.large MSK, moderate usage)

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
- EMR: Describe/Create Serverless applications
- S3: List/Create/Configure buckets
- KMS: Describe/Create keys and aliases
- Secrets Manager: Describe/Create secrets
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
   - Falls back to largest non-default VPC with ≥2 private subnets
   - Prefers creating NEW VPC over reusing default VPC (safety)

2. **Subnet Classification**:
   - Private: Tagged `Tier=private` OR routes to NAT Gateway
   - Public: Tagged `Tier=public` OR routes to Internet Gateway

3. **Resource Discovery**:
   - **S3 Buckets**: Matches `{project}-checkpoints|artifacts|raw` (case-insensitive)
   - **KMS Keys**: Finds aliases `alias/{project}-data|secrets|msk`
   - **MSK Clusters**: Tagged `Project={project}`
   - **EMR Apps**: Name contains project string
   - **Secrets**: Checks for existing `neo4j/aura` secret

4. **Output**: `generated.auto.tfvars.json` with discovered resource IDs

### Discovery Override

To override discovery, set explicit values:

```hcl
# terraform.tfvars
vpc_id = "vpc-abc123"
private_subnet_ids = ["subnet-111", "subnet-222", "subnet-333"]
security_group_ids = {
  msk      = "sg-aaa111"
  emr      = "sg-bbb222"
  producer = "sg-ccc333"
}

existing_bucket_names = {
  checkpoints = "my-checkpoints-bucket"
  artifacts   = "my-artifacts-bucket"
  raw         = "my-raw-bucket"
}

existing_kms_keys = {
  data    = "arn:aws:kms:us-east-1:123456789012:key/..."
  secrets = "arn:aws:kms:us-east-1:123456789012:key/..."
  msk     = "arn:aws:kms:us-east-1:123456789012:key/..."
}

existing_msk_cluster_arn = "arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/..."
existing_emr_app_id      = "app-00a1b2c3d4e5f6g7"
```

## Submitting a Spark Job to EMR Serverless

### 1. Package Your Spark Application

```python
# spark_kafka_neo4j.py
from pyspark.sql import SparkSession
import boto3
import json

# Fetch Neo4j credentials from Secrets Manager
def get_neo4j_creds(secret_name, region):
    client = boto3.client('secretsmanager', region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

spark = SparkSession.builder \
    .appName("F1-Kafka-Neo4j-Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.neo4j:neo4j-connector-apache-spark_2.12:5.2.0") \
    .getOrCreate()

# Get Neo4j credentials
creds = get_neo4j_creds("neo4j/aura", "us-east-1")

# Read from Kafka (MSK with IAM auth)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "BOOTSTRAP_BROKERS_HERE") \
    .option("subscribe", "f1-telemetry") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
    .option("startingOffsets", "latest") \
    .load()

# Transform and write to Neo4j
query = df.selectExpr("CAST(value AS STRING) as json") \
    .writeStream \
    .format("org.neo4j.spark.DataSource") \
    .option("url", creds['bolt_url']) \
    .option("authentication.basic.username", creds['username']) \
    .option("authentication.basic.password", creds['password']) \
    .option("labels", ":Telemetry") \
    .option("node.keys", "timestamp,driver") \
    .option("checkpointLocation", "s3://PROJECT-checkpoints/neo4j/") \
    .start()

query.awaitTermination()
```

### 2. Upload to S3

```bash
# Get S3 artifacts bucket from Terraform output
ARTIFACTS_BUCKET=$(terraform output -raw s3_artifacts_bucket)

# Upload your script
aws s3 cp spark_kafka_neo4j.py s3://${ARTIFACTS_BUCKET}/scripts/

# Upload any dependencies (if needed)
aws s3 cp requirements.txt s3://${ARTIFACTS_BUCKET}/dependencies/
```

### 3. Submit Job via AWS CLI

```bash
# Get EMR app ID from Terraform output
EMR_APP_ID=$(terraform output -raw emr_serverless_app_id)
EMR_ROLE_ARN=$(terraform output -raw emr_execution_role_arn)
MSK_BOOTSTRAP=$(terraform output -raw msk_bootstrap_brokers_sasl_iam)

# Submit job
aws emr-serverless start-job-run \
    --application-id ${EMR_APP_ID} \
    --execution-role-arn ${EMR_ROLE_ARN} \
    --name "f1-kafka-neo4j-streaming" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/scripts/spark_kafka_neo4j.py",
            "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=2 --conf spark.driver.memory=8g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.neo4j:neo4j-connector-apache-spark_2.12:5.2.0,software.amazon.msk:aws-msk-iam-auth:1.1.9"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'${ARTIFACTS_BUCKET}'/logs/"
            }
        }
    }'
```

### 4. Monitor Job

```bash
# Get job run ID from previous command output
JOB_RUN_ID="<from-previous-output>"

# Check status
aws emr-serverless get-job-run \
    --application-id ${EMR_APP_ID} \
    --job-run-id ${JOB_RUN_ID}

# View logs in CloudWatch or S3
aws logs tail /aws/emr-serverless/${EMR_APP_ID} --follow
```

## Neo4j Aura Configuration

See [neo4j_spark_config.md](./neo4j_spark_config.md) for detailed Spark configuration.

### Required: Update Secrets Manager

After deployment, update the Neo4j Aura credentials:

```bash
aws secretsmanager update-secret \
    --secret-id neo4j/aura \
    --secret-string '{
        "bolt_url": "neo4j+s://xxxxx.databases.neo4j.io",
        "username": "neo4j",
        "password": "your-actual-password"
    }'
```

### Network Access (IP Allowlisting)

If your Neo4j Aura instance has IP allowlisting enabled, you must allow the NAT Gateway IP(s):

```bash
# Get NAT Gateway public IP(s)
terraform output nat_gateway_public_ips

# Add these IPs to Neo4j Aura allowlist in the Aura console
```

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
├── README.md                          # This file
├── Makefile                           # Build automation
├── preflight.py                       # AWS resource discovery script
├── versions.tf                        # Terraform version constraints
├── providers.tf                       # AWS provider configuration
├── variables.tf                       # Input variables
├── outputs.tf                         # Output values
├── main.tf                            # Main infrastructure orchestration
├── neo4j_spark_config.md             # Neo4j + Spark integration guide
├── examples/
│   └── terraform.tfvars.example      # Example variable values
└── modules/
    ├── vpc/                          # VPC, subnets, NAT, IGW
    ├── s3/                           # S3 buckets with encryption
    ├── kms/                          # KMS keys for encryption
    ├── sg/                           # Security groups
    ├── msk/                          # Amazon MSK cluster
    ├── emr-serverless/               # EMR Serverless application
    ├── vpc-endpoints/                # VPC endpoints
    └── ecs-producer/                 # Optional ECS producer
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

### Neo4j Connection Issues
- Verify NAT Gateway is working: `terraform output nat_gateway_public_ips`
- Check Aura IP allowlist includes NAT IPs
- Validate credentials in Secrets Manager

## Security Best Practices

- All data encrypted at rest (KMS) and in transit (TLS)
- Least-privilege IAM roles and security groups
- Private subnets for compute (no direct internet access)
- S3 buckets block public access and enforce TLS
- Secrets stored in AWS Secrets Manager
- MSK uses IAM authentication (no credentials in code)
- CloudWatch alarms for operational monitoring

## License

MIT
