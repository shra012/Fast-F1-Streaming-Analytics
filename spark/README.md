# Spark Streaming Jobs

Bronze → Gold pipeline:

| Stage  | Script               | Purpose                                                                 |
|--------|----------------------|-------------------------------------------------------------------------|
| Bronze | `bronze_stream.py`   | Kafka → S3 Delta: Ingest telemetry & race events from MSK topics|
| Gold   | `gold_stream.py`     | S3 → Neo4j: Read Bronze Delta, derive graph nodes/edges, write to Neo4j|

## Setup

```bash
./scripts/setup_all.sh
```

## Start Bronze Stream

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

## Start Gold Stream

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

## Monitor

```bash
tail -f bronze_stream.log
tail -f gold_stream.log
yarn application -list
```

## Package & Deploy

```bash
cd spark
make package
export S3_PREFIX=s3://$(terraform -chdir=../infra output -raw s3_artifacts_bucket)/spark
make upload
```
