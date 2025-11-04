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
Environment variables such as `SPARK_BRONZE_BASE` can be populated from Terraform
outputs by running `../scripts/sync_msk_env.sh`, which updates `kafka/.env`.

## Upload to S3

Pick a code bucket (for example, `s3://fastf1-artifacts/spark/`) and upload:

```bash
make upload S3_PREFIX=s3://fastf1-artifacts/spark
```

Record the S3 URIs; they are referenced by `spark-submit` on EMR.

## Running on the EMR Cluster

1. **Upload artifacts** – ensure `spark_package.zip` and the entrypoint scripts are in S3 (see “Packaging” above).
2. **SSH to the master node** – use `terraform output emr_cluster_master_dns` and your key pair.
3. **Submit a job** – from the master:
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
   Repeat for silver/gold entry points with their respective output/checkpoint paths or overrides.
4. **Monitor** – use YARN (`yarn application -list`, `yarn logs`), the ResourceManager UI, and CloudWatch log group `/aws/emr/<cluster-name>`.

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
