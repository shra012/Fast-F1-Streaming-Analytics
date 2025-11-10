# Spark Streaming Jobs

The `spark/` directory contains two Structured Streaming applications that
implement the **Bronze → Gold** processing pipeline for Formula 1 telemetry and Neo4j graph analytics:

| Stage  | Script               | Status | Purpose                                                                 |
|--------|----------------------|--------|-------------------------------------------------------------------------|
| Bronze | `bronze_stream.py`   | Running | **Kafka → S3**: Ingest raw telemetry & race events from MSK topics, parse JSON, write to Delta tables|
| Gold   | `gold_stream.py`     | Planned | **S3 → S3/Neo4j**: Read Bronze, cleanse/aggregate, compute graph analytics, write to S3 + Neo4j, expose serving layer |

**Architecture Decision**: Two-stage pipeline consolidating Silver + Gold + Platinum into unified Gold stage. 

Gold stage responsibilities:
- Cleansing and deduplication (formerly Silver)
- Lap aggregation and stint detection (formerly Gold)
- Graph computation with inline PageRank/centrality (formerly Platinum)
- Multi-sink output: S3 Delta tables + Neo4j graph + serving layer prep

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

Note the resulting URIs--they are referenced by the submission commands below.

## Terraform Outputs Needed at Runtime

After `terraform apply` finishes, capture the runtime values:

```bash
cd ../infra
export EMR_MASTER_DNS=$(terraform output -raw emr_cluster_master_public_dns)
export EMR_CLUSTER_ID=$(terraform output -raw emr_cluster_id)
export KAFKA_BOOTSTRAP=$(terraform output -raw msk_bootstrap_brokers_sasl_iam)
export SPARK_BRONZE_BASE=$(terraform output -raw spark_bronze_base_uri)
export SPARK_GOLD_BASE=$(terraform output -raw spark_gold_base_uri)
export SPARK_CHECKPOINT_BASE=$(terraform output -raw s3_checkpoint_uri)
export SPARK_ARTIFACT_BUCKET=$(terraform output -raw s3_artifacts_bucket)
export NEO4J_URI=$(terraform output -raw neo4j_uri)  # or manually set for Aura
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="<your-neo4j-password>"
cd ../spark   # return to spark/ working tree for the commands below
```

These exports make it easy to copy-paste the commands below. MSK bootstrap and Neo4j credentials are sensitive--avoid printing to shared logs.

> **Quick automation**: from the repo root run `./scripts/prepare_emr_job.sh`.
> It packages the Spark code, uploads it to S3, generates `spark/emr_job.env`,
> and prints ready-to-use `scp`, `ssh`, and `spark-submit` commands. The script
> assumes `terraform apply` has completed successfully in `infra/`.

## Running on the EMR Cluster

1. **Upload artifacts** - ensure `spark_package.zip` and the entrypoint scripts exist under `s3://$SPARK_ARTIFACT_BUCKET/spark/`.

2. **SSH to the master node** - use the DNS output and your key pair:
   ```bash
   ssh -i <path-to-key.pem> hadoop@$EMR_MASTER_DNS
   ```

3. **Submit the Bronze job** - from the EMR master (already running):
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
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/bronze" \
     --starting-offsets earliest
   ```
   
   **Status**: Running successfully, 133.58 MB processed

4. **Submit the Gold job** - unified analytics and serving layer (planned):
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0,graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
     --py-files s3://$SPARK_ARTIFACT_BUCKET/spark/spark_package.zip \
     s3://$SPARK_ARTIFACT_BUCKET/spark/gold_stream.py \
     --bronze-base "$SPARK_BRONZE_BASE" \
     --output-base "$SPARK_GOLD_BASE" \
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/gold" \
     --neo4j-uri "$NEO4J_URI" \
     --neo4j-user "$NEO4J_USER" \
     --neo4j-password "$NEO4J_PASSWORD" \
     --enable-graph-analytics \
     --centrality-window-laps 50
   ```
   
   **Prerequisites for Gold**:
   - Neo4j Aura instance provisioned
   - Connection credentials in `~/spark.env` on EMR
   - Spark cluster IP allowlisted in Neo4j
   - Schema constraints created (unique driver_id, session_id)
   - Indexes on frequently queried properties (driver_id, lap_number, session_id)
   - Dimension seed data loaded (drivers, teams, circuits)

5. **Monitor execution** - leverage YARN CLI (`yarn application -list`, `yarn logs -applicationId ...`), the ResourceManager UI (tunnel via SSH), and CloudWatch log group `/aws/emr/<cluster-name>`.

### Common Spark Arguments

- `--bootstrap-servers` - MSK bootstrap string (Bronze job only, reads from Kafka)
- `--starting-offsets` - choose `earliest` for backfills, `latest` for real-time only
- `--write-format` / `--output-format` - defaults to `delta`. For pure Parquet, override and remove Delta jar
- `--neo4j-uri` - Neo4j Aura connection URI (Gold job, e.g., `neo4j+s://xxxxx.databases.neo4j.io`)
- `--neo4j-user` / `--neo4j-password` - Neo4j authentication credentials

### Delta Lake Support

Delta is enabled via `spark.sql.extensions` in code. When submitting to EMR,
provide the appropriate package via `spark.jars.packages`. Example:
`--conf spark.jars.packages=io.delta:delta-core_2.12:2.4.0`.

### Neo4j Spark Connector & GraphFrames

Gold job requires Neo4j Spark Connector and GraphFrames for inline analytics. Add via `--packages`:
```bash
--packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0,graphframes:graphframes:0.8.3-spark3.5-s_2.12
```

Configure connection in code or via Spark conf:
```python
# Neo4j connection
spark.conf.set("spark.neo4j.url", neo4j_uri)
spark.conf.set("spark.neo4j.authentication.type", "basic")
spark.conf.set("spark.neo4j.authentication.basic.username", neo4j_user)
spark.conf.set("spark.neo4j.authentication.basic.password", neo4j_password)

# GraphFrames for PageRank/centrality
from graphframes import GraphFrame
# Create graph from edges/vertices DataFrames
# Run PageRank inline during micro-batch processing
```

**Gold Stage Analytics Features**:
- Incremental centrality computation on windowed data (last N laps per session)
- Inline PageRank using GraphFrames (avoids separate batch job)
- Driver influence scores updated in real-time
- Team battle intensity computed per micro-batch

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
