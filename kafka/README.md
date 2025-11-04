# Kafka Module Overview

This directory houses the client-side components used to interact with the Amazon MSK cluster that powers our streaming pipeline. The initial focus is keeping things simple: create the Kafka topics we need and publish telemetry messages sourced from the FastF1 API. As the architecture matures, we will package these components for managed runtimes alongside the EMR cluster and add richer operational tooling.

## Repo Layout (initial)
```
kafka/
├── README.md                 # This document
├── requirements.txt          # Python dependencies for admin + producer tooling
├── topics.yaml               # Declarative topic definitions
├── scripts/
│   └── create_topics.py      # Helper to create topics in MSK
└── producer/
    └── simple_producer.py    # Minimal FastF1 replay producer
```

## Prerequisites
- Python 3.10+ with `pip`
- AWS credentials configured (profile or env vars) with permissions for MSK and IAM auth
- Working MSK cluster with IAM authentication enabled
- Kafka client utilities available on the EMR cluster (or local environment)

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
  --query "BootstrapBrokerStringSaslIam" \
  --output text
```
Record the comma-separated broker string; it is required for both topic creation and producing events.

## 2. Define Topics
Edit `topics.yaml` to list the topics the platform needs. Each entry specifies partitions, replication factor, and optional configs. The defaults are tuned for development. Update replication factor to match the broker count when running in production.

Example snippet:
```yaml
- name: telemetry.fastf1.raw
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

## 4. Run the Simple Producer
The initial producer replays telemetry for a single session using the FastF1 API. It publishes lap-level messages to the configured topic with an adjustable playback speed (default is real-time).
```bash
python producer/simple_producer.py \
  --cluster-arn $MSK_CLUSTER_ARN \
  --region us-east-1 \
  --bootstrap "$(aws kafka get-bootstrap-brokers --cluster-arn $MSK_CLUSTER_ARN --query "BootstrapBrokerStringSaslIam" --output text)" \
  --topic telemetry.fastf1.raw \
  --season 2023 \
  --event "Bahrain Grand Prix" \
  --session R \
  --speedup 30
```
When `.env` contains the MSK values (`KAFKA_BOOTSTRAP_BROKERS`, `MSK_CLUSTER_ARN`, `AWS_REGION`),
those arguments can be omitted:
```bash
python producer/simple_producer.py \
  --season 2023 \
  --event "Bahrain Grand Prix" \
  --session R \
  --speedup 30
```
When targeting the local Docker broker, run in PLAINTEXT mode:
```bash
python producer/simple_producer.py \
  --auth-mode plain \
  --bootstrap localhost:9092 \
  --topic telemetry.fastf1.raw \
  --season 2023 \
  --event "Bahrain Grand Prix" \
  --session R \
  --speedup 30
```
Or use the helper script which reads defaults from `.env`:
```bash
./producer/start.sh
# override on the fly, e.g.
PRODUCER_MAX_EVENTS=100 PRODUCER_SPEEDUP=60 ./producer/start.sh
```

> **Tip:** `make consume-local` (or the `console-consumer` service in `docker-compose.yml`)
> lets you watch the messages flowing into the local broker.

To verify events locally, launch the console consumer defined in `docker-compose.yml`.
It defaults to the same telemetry topic but can be overridden with `KAFKA_TOPIC`:
```bash
make consume-local
# or
KAFKA_TOPIC=telemetry.fastf1.raw docker compose run --rm console-consumer
```

The script fetches telemetry via FastF1, normalizes each lap into JSON, and publishes it using IAM-authenticated SASL (or PLAINTEXT for local tests). Use the `--speedup` flag to accelerate playback during testing.

Additional throttling flags help bound local test runs:
- `--max-events` limits the number of lap events sent (0 = unlimited).
- `--max-runtime` stops the producer after the specified number of seconds (0 = unlimited).

## 5. Run the Producer in Docker
Build the image and publish a handful of events to a local broker (the included `docker-compose.yaml` exposes Redpanda on `kafka_default`):
```bash
# Build the container (uses requirements.txt/pyproject.toml/uv.lock)
docker build -t fastf1-producer:local -f Dockerfile .

# Produce five events at high speed against the local broker
docker run --rm \
  --network kafka_default \
  fastf1-producer:local \
  --auth-mode plain \
  --bootstrap redpanda:9092 \
  --topic telemetry.fastf1.raw \
  --season 2023 \
  --event "Bahrain Grand Prix" \
  --session R \
  --speedup 1000 \
  --max-events 5 \
  --max-runtime 60
```

## 6. Next Steps
- Add health checks, structured logging, and checkpointing before broader production deployment.
- Introduce schema registry validation and error handling once we expand beyond the minimal prototype.
- Extend the admin tooling with ACL management and topic configuration drift detection.

## 7. CI Automation
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
