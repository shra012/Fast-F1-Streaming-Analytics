# Spark Streaming Jobs

The `spark/` directory contains three Structured Streaming applications that
implement the Bronze → Silver → Gold processing stages for Formula 1 telemetry:

| Stage  | Script               | Purpose                                                                 |
|--------|----------------------|-------------------------------------------------------------------------|
| Bronze | `bronze_stream.py`   | Ingest raw telemetry & race events from Kafka topics into object storage|
| Silver | `silver_stream.py`   | Clean & join bronze data, generating enriched telemetry tables          |
| Gold   | `gold_stream.py`     | Aggregate lap-level metrics; optional Kafka fan-out for consumers       |

Each job accepts CLI arguments so they can run locally (Spark standalone) or on
an EMR cluster (Spark on YARN). All paths default to S3-style URIs but can be overridden.

## Packaging

Use the provided Makefile to bundle the `spark` package (all supporting modules)
so the EMR cluster can import shared code:

```bash
cd spark
make package                 # produces dist/spark_package.zip
```

The archive contains the entire `spark` package (including utils and schemas).
Attach the zip via `--py-files` when invoking `spark-submit` on EMR.

## Upload Artifacts

Sync the build products to the artifacts bucket that Terraform wires for EMR:

```bash
# Upload package + entrypoints to the artifacts bucket under a spark/ prefix
make upload S3_PREFIX="s3://$(terraform -chdir=../infra output -raw s3_artifacts_bucket)/spark"
```

Note the resulting URIs—they are referenced by the submission commands below.

## Terraform Outputs Needed at Runtime

After `terraform apply` finishes, capture the runtime values:

```bash
cd ../infra
export EMR_MASTER_DNS=$(terraform output -raw emr_cluster_master_public_dns)
export EMR_CLUSTER_ID=$(terraform output -raw emr_cluster_id)
export KAFKA_BOOTSTRAP=$(terraform output -raw msk_bootstrap_brokers_sasl_iam)
export SPARK_BRONZE_BASE=$(terraform output -raw spark_bronze_base_uri)
export SPARK_SILVER_BASE=$(terraform output -raw spark_silver_base_uri)
export SPARK_GOLD_BASE=$(terraform output -raw spark_gold_base_uri)
export SPARK_CHECKPOINT_BASE=$(terraform output -raw s3_checkpoint_uri)
export SPARK_ARTIFACT_BUCKET=$(terraform output -raw s3_artifacts_bucket)
cd ../spark   # return to spark/ working tree for the commands below
```

These exports make it easy to copy-paste the commands below. The MSK bootstrap
value is sensitive—avoid printing it to shared logs.

> **Quick automation**: from the repo root run `./scripts/prepare_emr_job.sh`.
> It packages the Spark code, uploads it to S3, generates `spark/emr_job.env`,
> and prints ready-to-use `scp`, `ssh`, and `spark-submit` commands. The script
> assumes `terraform apply` has completed successfully in `infra/`.

## Running on the EMR Cluster

1. **Upload artifacts** – ensure `spark_package.zip` and the entrypoint scripts exist under `s3://$SPARK_ARTIFACT_BUCKET/spark/`.
2. **SSH to the master node** – use the DNS output and your key pair:
   ```bash
   ssh -i <path-to-key.pem> hadoop@$EMR_MASTER_DNS
   ```
3. **Submit the Bronze job** – from the EMR master:
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --py-files s3://$SPARK_ARTIFACT_BUCKET/spark/spark_package.zip \
     s3://$SPARK_ARTIFACT_BUCKET/spark/bronze_stream.py \
     --bootstrap-servers "$KAFKA_BOOTSTRAP" \
     --telemetry-topic telemetry.raw \
     --events-topic race.events \
     --output-base "$SPARK_BRONZE_BASE" \
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/bronze"
   ```
4. **Submit Silver / Gold jobs** – repeat with `silver_stream.py` and `gold_stream.py`, adjusting arguments:
   ```bash
   # Silver
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --py-files s3://$SPARK_ARTIFACT_BUCKET/spark/spark_package.zip \
     s3://$SPARK_ARTIFACT_BUCKET/spark/silver_stream.py \
     --bronze-base "$SPARK_BRONZE_BASE" \
     --output-base "$SPARK_SILVER_BASE" \
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/silver"

   # Gold
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --py-files s3://$SPARK_ARTIFACT_BUCKET/spark/spark_package.zip \
     s3://$SPARK_ARTIFACT_BUCKET/spark/gold_stream.py \
     --silver-base "$SPARK_SILVER_BASE" \
     --output-base "$SPARK_GOLD_BASE" \
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/gold"
   ```
5. **Monitor execution** – leverage YARN CLI (`yarn application -list`, `yarn logs -applicationId ...`), the ResourceManager UI (tunnel via SSH), and CloudWatch log group `/aws/emr/<cluster-name>`.

### Common Spark Arguments

- `--bootstrap-servers` – pass MSK bootstrap string if running the bronze job directly against Kafka.
- `--starting-offsets` – choose `earliest` for backfills.
- `--write-format` / `--output-format` – defaults to `delta`. For pure Parquet, override and remove the Delta jar.

### Delta Lake Support

Delta is enabled via `spark.sql.extensions` in code. When submitting to EMR,
provide the appropriate package via `spark.jars.packages`. Example:
`--conf spark.jars.packages=io.delta:delta-core_2.12:2.4.0`.

## Local Testing

You can test locally with a standalone Spark installation:

```bash
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --py-files spark/dist/spark_package.zip \
  spark/bronze_stream.py \
  --bootstrap-servers localhost:9092 \
  --output-base ./tmp/bronze \
  --checkpoint-base ./tmp/checkpoints/bronze \
  --write-format delta
```

Update the output paths for the silver and gold jobs accordingly.

## Configuration Summary

| Env Var / Flag               | Description                                        | Default                              |
|------------------------------|----------------------------------------------------|--------------------------------------|
| `SPARK_BRONZE_BASE`        | Base S3 path for bronze sink                       | `s3://<raw-bucket>/bronze`           |
| `SPARK_SILVER_BASE`         | Base S3 path for silver sink                       | `s3://<artifacts-bucket>/silver`     |
| `SPARK_GOLD_BASE`           | Base S3 path for gold sink                         | `s3://<artifacts-bucket>/gold`       |
| `SPARK_CHECKPOINT_BASE`     | Root checkpoint path (stage subdirectories appended)| `s3://<checkpoints-bucket>/checkpoints` |
| `KAFKA_BOOTSTRAP_BROKERS`   | Kafka connection string for streaming ingestion    | `localhost:9092`                     |
| `KAFKA_STARTING_OFFSETS`    | Kafka starting offsets (`latest` / `earliest`)     | `latest`                             |
| `GOLD_KAFKA_TOPIC`          | Optional fan-out topic for gold lap metrics        | *(empty)*                            |

Adjust these via environment variables or CLI arguments as needed for EMR vs. local runs.
