# Spark Streaming Jobs

Two Structured Streaming jobs for the **Bronze → Gold** pipeline:

| Stage  | Script               | Purpose                                                                 |
|--------|----------------------|-------------------------------------------------------------------------|
| Bronze | `bronze_stream.py`   | Kafka → S3 Delta: Ingest telemetry & race events from MSK topics|
| Gold   | `gold_stream.py`     | S3 → Neo4j: Read Bronze Delta, derive graph nodes/edges, write to Neo4j + Kafka|

## Quick Start

### 1. Setup (one-time)
```bash
# From repo root
./scripts/setup_all.sh
```

This packages Spark code, uploads to S3, and creates `spark/emr_job.env` with all connection details.

### 2. SSH to EMR
```bash
ssh -i ~/.ssh/id_rsa hadoop@<EMR_MASTER_DNS>
```

### 3. Start Bronze Stream (Kafka → S3)
```bash
source spark.env && nohup spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-memory 4G \
  --executor-cores 2 \
  --driver-memory 4G \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  bronze_stream.py \
    --bootstrap-servers "$KAFKA_BOOTSTRAP" \
    --telemetry-topic telemetry.raw \
    --events-topic race.events \
    --output-base "$BRONZE_PATH" \
    --checkpoint-base "$CHECKPOINT_PATH/bronze_stream" \
    --starting-offsets latest \
  > bronze_stream.log 2>&1 &
```

### 4. Start Gold Stream (S3 → Neo4j)
```bash
source spark.env && nohup spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-memory 4G \
  --executor-cores 2 \
  --driver-memory 4G \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  --conf spark.sql.streaming.schemaInference=true \
  --conf spark.sql.adaptive.enabled=true \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3,graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  gold_stream.py \
    --bronze-path "$BRONZE_PATH" \
    --checkpoint-path "$CHECKPOINT_PATH/gold_stream" \
    --kafka-bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
    --neo4j-uri "$NEO4J_URI" \
    --neo4j-username "$NEO4J_USERNAME" \
    --neo4j-password "$NEO4J_PASSWORD" \
  > gold_stream.log 2>&1 &
```

### 5. Monitor
```bash
# Check logs
tail -f bronze_stream.log
tail -f gold_stream.log

# Check YARN applications
yarn application -list

# Kill jobs
yarn application -kill <application_id>
```

## Package & Deploy Updates

```bash
cd spark
make package                                    # Creates dist/spark_package.zip
aws s3 cp dist/spark_package.zip s3://<bucket>/spark/
aws s3 cp bronze_stream.py s3://<bucket>/spark/
aws s3 cp gold_stream.py s3://<bucket>/spark/
```

## Key Environment Variables (in spark.env)

```bash
KAFKA_BOOTSTRAP=b-1.msk.amazonaws.com:9092,...
BRONZE_PATH=s3://bucket/bronze
CHECKPOINT_PATH=s3://bucket/checkpoints
NEO4J_URI=neo4j+s://xxxxx.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<password>
SPARK_WRITE_TO_NEO4J=true
```
