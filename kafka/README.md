# Kafka Module Overview

This directory houses the client-side components used to interact with the Amazon MSK cluster that powers our streaming pipeline. The producer streams F1 telemetry and race event data from the FastF1 API to Kafka topics, which are then consumed by our Bronze Spark Streaming jobs.

## Architecture

```
FastF1 API → producer.py → MSK Kafka
                              ├─ telemetry.raw (car telemetry)
                              └─ race.events (race control events)
                                      ↓
                              Bronze Spark Job (EMR)
                                      ↓
                                  S3 Bronze Layer (Delta/Parquet)
```

## Repo Layout
```
kafka/
├── README.md                 # This document
├── requirements.txt          # Python dependencies for admin + producer tooling
├── topics.yaml               # Declarative topic definitions
├── scripts/
│   └── create_topics.py      # Helper to create topics in MSK
└── producer/
    ├── producer.py           # Comprehensive F1 data producer
    ├── run_producer.sh       # Helper script with Terraform integration
    └── README.md             # Detailed producer documentation
```

## Prerequisites
- Python 3.10+ with `pip`
- AWS credentials configured (profile or env vars) with permissions for MSK
- Working MSK cluster with **PLAINTEXT authentication** (unauthenticated mode)
- Network access to MSK brokers (VPN or running inside VPC)

Set up the project environment with [uv](https://github.com/astral-sh/uv):
```bash
# Install the dependencies into a local virtualenv
uv sync

# Activate the environment (POSIX shells)
source .venv/bin/activate
```
> `requirements.txt` is auto-generated via `uv export` for compatibility with tools that still rely on pip requirements. Regenerate it after changing dependencies:
> ```bash
> uv export --format requirements.txt --no-dev --no-hashes > requirements.txt
> ```

## 1. Retrieve Connection Details
Use the helper script to discover the latest MSK cluster and write the connection
details into a `.env` file that the tooling will read automatically:
```bash
python scripts/update_bootstrap_env.py --region us-east-1
```
This populates `MSK_BOOTSTRAP_BROKERS`, `KAFKA_BOOTSTRAP_BROKERS`, `MSK_CLUSTER_ARN`,
`AWS_REGION`, and the `SPARK_*` S3 base paths inside `.env` (and sets `KAFKA_AUTH_MODE=iam`).
Subsequent scripts will fall back to these values if command-line arguments are omitted.
You can run the same refresh via `make update-env REGION=us-east-1` from `kafka/`.
Subsequent scripts will fall back to these values if command-line arguments are omitted.
Switch `KAFKA_AUTH_MODE=plain` and set `KAFKA_BOOTSTRAP_BROKERS=localhost:9092` to
target the local Docker broker instead of MSK.

If you prefer to capture the brokers manually, the AWS CLI provides the same data:
```bash
aws kafka get-bootstrap-brokers \
  --cluster-arn $MSK_CLUSTER_ARN \
  --query "BootstrapBrokerString" \
  --output text
```
Record the comma-separated broker string; it is required for both topic creation and producing events.

> **Note:** We use `BootstrapBrokerString` (plaintext on port 9092) instead of `BootstrapBrokerStringSaslIam` since we simplified the cluster to use unauthenticated mode.

## 2. Define Topics
Edit `topics.yaml` to list the topics the platform needs. Each entry specifies partitions, replication factor, and optional configs. The defaults are tuned for development. Update replication factor to match the broker count when running in production.

Current topics:
```yaml
- name: telemetry.raw       # Car telemetry (speed, throttle, GPS, etc.)
  partitions: 3
  replication_factor: 3
  config:
    cleanup.policy: delete

- name: race.events         # Race events (laps, pit stops, flags, etc.)
  partitions: 3
  replication_factor: 3
  config:
    cleanup.policy: delete
```

## 3. Create Topics
Run the helper script, providing the bootstrap brokers and MSK cluster ARN (or rely
on the values stored in `.env`):
```bash
python scripts/create_topics.py \
  --bootstrap $(aws kafka get-bootstrap-brokers --cluster-arn $MSK_CLUSTER_ARN --query "BootstrapBrokerStringSaslIam" --output text) \
  --cluster-arn $MSK_CLUSTER_ARN \
  --region us-east-1
```
With `.env` seeded by `scripts/update_bootstrap_env.py`, the minimal invocation becomes:
```bash
python scripts/create_topics.py
```
For the local Docker Compose broker, switch to PLAINTEXT auth and skip the ARN/region inputs:
```bash
python scripts/create_topics.py --auth-mode plain --bootstrap localhost:9092
```
The script picks the auth mode from `KAFKA_AUTH_MODE` (default `iam`). It will create any
topics that do not already exist and skip ones that are present.

> **Note:** Topic creation uses the Kafka Admin client under the hood. Ensure the machine running the script has network access to the brokers (e.g., via VPN or by running inside the VPC).

## 4. Run the Producer

The producer streams comprehensive F1 data (telemetry and race events) from multiple seasons to Kafka topics. It matches the exact schemas expected by the Bronze Spark jobs.

### Quick Start with Helper Script

The `run_producer.sh` script automatically fetches MSK bootstrap servers from Terraform:

```bash
cd producer

# Stream all 2024 data at 30x speed
./run_producer.sh --start-year 2024 --speedup 30

# Stream specific event
./run_producer.sh --start-year 2024 --event "Monaco" --speedup 20

# Dry run to preview data
./run_producer.sh --start-year 2024 --event "Bahrain" --dry-run
```

### Manual Invocation

Or invoke the producer directly:

```bash
# Get bootstrap servers from Terraform
BOOTSTRAP=$(cd ../infra && terraform output -raw msk_bootstrap_brokers)

# Stream 2024 data
./producer.py \
  --bootstrap "$BOOTSTRAP" \
  --start-year 2024 \
  --speedup 30

# Stream 2020-2024 for specific driver
./producer.py \
  --bootstrap "$BOOTSTRAP" \
  --start-year 2020 \
  --end-year 2024 \
  --driver VER \
  --speedup 50
```

### Producer Features

- **Multi-season support**: Process 2020-2025 data
- **Two topics**: `telemetry.raw` (33 fields) and `race.events` (12 fields)
- **Schema matching**: Exactly matches `spark/schemas.py`
- **Configurable playback**: 1x to 100x+ speed
- **Filtering**: By year, event, session, driver
- **Batch processing**: Efficient Kafka publishing

### Common Options

| Option | Description | Example |
|--------|-------------|---------|
| `--start-year` | Starting season | `2020` |
| `--end-year` | Ending season | `2024` |
| `--event` | Event name or round | `"Monaco"` or `5` |
| `--session` | Session type | `R` (race), `Q` (quali) |
| `--driver` | Driver code filter | `VER`, `HAM`, `LEC` |
| `--speedup` | Playback multiplier | `30` (30x faster) |
| `--dry-run` | Preview without sending | - |

See `producer/README.md` for complete documentation.

### Monitoring Production

While the producer runs, monitor:
- **Producer logs**: Messages published per topic
- **Kafka topics**: Check lag in MSK console
- **Bronze Spark job**: Verify consumption on EMR
- **S3 Bronze layer**: Validate data written to Delta/Parquet

```bash
# Check Bronze job status
ssh -i ~/.ssh/id_rsa hadoop@<EMR_MASTER_DNS>
yarn application -list

# Check S3 bronze data
aws s3 ls s3://<BRONZE_BUCKET>/bronze/ --recursive
```

## 5. Next Steps
- Monitor Bronze Spark job consumption and validate S3 bronze layer data
- Add schema registry validation once data flow is established
- Extend producer with real-time F1 timing API integration (currently uses historical data)
- Add health checks and structured logging for production deployment
- Implement producer checkpointing for resumable sessions

## 6. CI Automation
- **Sync Kafka Topics** (`.github/workflows/msk-topic-sync.yml`): Runs the topic creation script automatically whenever `kafka/topics.yaml` changes, or on demand. Secrets required:
  - `AWS_ROLE_TO_ASSUME`
  - `AWS_REGION`
  - `MSK_CLUSTER_ARN`
- **Deploy Producer** (`.github/workflows/deploy-fargate-producer.yml`): Builds the Kafka producer container with uv, pushes it to ECR, and rolls out to the chosen runtime. Configure these secrets before enabling the workflow:
  - `AWS_ROLE_TO_ASSUME` (or a dedicated deployment role)
  - `AWS_REGION`
  - `AWS_ECR_REPOSITORY`
  - `AWS_ECS_CLUSTER`
  - `AWS_ECS_SERVICE`
  - `AWS_ECS_CONTAINER_NAME`

The deploy workflow assumes a Dockerfile exists at `kafka/Dockerfile` and that the ECS service/task definition already references the specified container name. Use the Actions tab to run either workflow manually when needed.
