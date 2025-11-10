# Pipeline Status Checker

Comprehensive monitoring script for the F1 Streaming Pipeline running on EMR. The `check_pipeline_status.sh` script provides real-time visibility into Kafka consumption, Spark streaming progress, S3 output, and data processing efficiency.

## Overview

The status checker consolidates multiple AWS and YARN commands into a single executable that monitors:
- YARN application status (Spark jobs)
- Kafka topic message counts across all partitions
- Spark streaming checkpoint progress
- S3 bronze layer output (file counts and sizes)
- Data processing efficiency (messages consumed vs. data written)
- Recent streaming activity and error detection

## Prerequisites

### Local Environment Setup

From your local machine, source the EMR job environment variables:
```bash
# From the repo root
source spark/emr_job.env
```

This sets:
- `EMR_MASTER_DNS`: EMR cluster master node DNS
- `SSH_KEY`: Path to SSH private key
- `KAFKA_BOOTSTRAP`: MSK broker addresses
- `S3_RAW_BUCKET`: Raw data bucket name
- `S3_CHECKPOINTS_BUCKET`: Checkpoint bucket name
- `S3_ARTIFACTS_BUCKET`: Artifacts bucket name

### Deploy Script to EMR

Copy the monitoring script to the EMR master node:
```bash
scp -i ${SSH_KEY} scripts/check_pipeline_status.sh hadoop@${EMR_MASTER_DNS}:~/
```

Make it executable:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'chmod +x check_pipeline_status.sh'
```

### Create Environment File on EMR

Create `~/spark.env` on the EMR master node with required variables:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} "cat > ~/spark.env << 'EOF'
export KAFKA_BOOTSTRAP=\"${KAFKA_BOOTSTRAP}\"
export SPARK_BRONZE_BASE=\"s3://${S3_RAW_BUCKET}/bronze/\"
export SPARK_CHECKPOINT_BASE=\"s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/\"
export S3_RAW_BUCKET=\"${S3_RAW_BUCKET}\"
export S3_CHECKPOINTS_BUCKET=\"${S3_CHECKPOINTS_BUCKET}\"
EOF"
```


---

## Usage

### Basic Status Check

Run the standard health check:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh'
```

**Output includes:**
- YARN application list and bronze_stream status
- Kafka topic message counts (total across partitions)
- Active streaming queries with batch counts
- S3 output directories with file counts and sizes
- Data efficiency summary (Kafka messages vs S3 output)

### Detailed Status Check

Include logs, partition-level metrics, and error detection:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh --detailed'
```

**Additional information:**
- Per-partition Kafka offset details
- Recent streaming activity (last 20 log lines per stream)
- Error detection in Spark application logs
- Processing delay warnings

### Pipeline Reset

Interactive cleanup utility to reset checkpoints and/or output:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh --reset'
```

**Interactive menu options:**
1. **Clean checkpoints only**: Removes checkpoint state, keeps S3 output. Next run will reprocess all Kafka messages from beginning.
2. **Clean checkpoints and output**: Complete reset. Deletes all bronze layer data and checkpoint state.
3. **Cancel**: Exit without making changes

**What gets deleted:**
- Checkpoints: `s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze/`
- Output: `s3://${S3_RAW_BUCKET}/bronze/` (option 2 only)
- YARN applications: Kills running bronze_stream jobs before cleanup

---

## Individual Monitoring Commands

These commands can be run directly from your local machine after sourcing `spark/emr_job.env`. Each performs a specific check without requiring the full script.

### 1. Check YARN Applications

List all running YARN applications:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list'
```

**What it checks:** Verifies if bronze_stream Spark job is running  
**Expected output:** Application ID, name, state (RUNNING), queue, and tracking URL

### 2. Check Specific Application Status

Get detailed status of the bronze_stream application:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list | grep -E "bronze|fastf1" | head -1 | awk "{print \$1}" | xargs -I {} yarn application -status {}'
```

**What it checks:** Application start time, resource usage, and current state  
**Expected output:** Full application details including memory/CPU allocation

### 3. Check Kafka Topics

List all Kafka topics:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && ~/kafka-tools/bin/kafka-topics.sh --bootstrap-server $(echo $KAFKA_BOOTSTRAP | cut -d, -f1) --list'
```

**What it checks:** Verifies telemetry.raw and race.events topics exist  
**Expected output:** List of topic names

### 4. Check Kafka Message Counts

Get total message count for telemetry.raw:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && ~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $(echo $KAFKA_BOOTSTRAP | cut -d, -f1) --topic telemetry.raw | awk -F: "{sum+=\$3} END {print \"telemetry.raw: \" sum \" messages\"}"'
```

**What it checks:** Total messages available across all partitions  
**Expected output:** `telemetry.raw: 1116826 messages`

For race.events:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && ~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $(echo $KAFKA_BOOTSTRAP | cut -d, -f1) --topic race.events | awk -F: "{sum+=\$3} END {print \"race.events: \" sum \" messages\"}"'
```

### 5. Check Kafka Partition Details

Show per-partition message counts:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && ~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $(echo $KAFKA_BOOTSTRAP | cut -d, -f1) --topic telemetry.raw'
```

**What it checks:** Message distribution across partitions  
**Expected output:** `telemetry.raw:0:220896`, `telemetry.raw:1:422166`, `telemetry.raw:2:473764`

### 6. Sample Kafka Messages

Read first 3 messages from telemetry.raw:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} '~/kafka-tools/bin/kafka-console-consumer.sh --bootstrap-server REDACTED --topic telemetry.raw --from-beginning --max-messages 3'
```

**What it checks:** Message format and content validity  
**Expected output:** JSON objects with session_id, driver_id, speed_kph, etc.

### 7. Check Spark Streaming Checkpoints

List checkpoint directories in S3:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && aws s3 ls ${SPARK_CHECKPOINT_BASE}bronze/'
```

**What it checks:** Streaming queries are maintaining state  
**Expected output:** Directories for telemetry_raw-parsed, telemetry_raw-raw, race_events-parsed, race_events-raw

### 8. Check Batch Processing Progress

Count batches processed per stream:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && for stream in telemetry_raw-parsed telemetry_raw-raw race_events-parsed race_events-raw; do echo "$stream: $(aws s3 ls ${SPARK_CHECKPOINT_BASE}bronze/$stream/commits/ 2>/dev/null | grep -v PRE | wc -l) batches"; done'
```

**What it checks:** Number of successfully committed batches  
**Expected output:** Batch count for each stream (increases over time)

### 9. Check S3 Bronze Layer Output

List output directories:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && aws s3 ls ${SPARK_BRONZE_BASE}'
```

**What it checks:** Data is being written to S3  
**Expected output:** PRE telemetry_raw-parsed/, PRE telemetry_raw-raw/, etc.

### 10. Check S3 Output File Counts and Sizes

Show file counts and total sizes:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && for stream in telemetry_raw-parsed telemetry_raw-raw race_events-parsed race_events-raw; do echo "=== $stream ==="; aws s3 ls ${SPARK_BRONZE_BASE}$stream/ --recursive --summarize 2>/dev/null | tail -2; done'
```

**What it checks:** Volume of data written per stream  
**Expected output:** Total Objects and Total Size for each stream

### 11. View Recent Spark Logs

Show last 50 lines of application logs:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list | grep -E "bronze|fastf1" | head -1 | awk "{print \$1}" | xargs -I {} yarn logs -applicationId {} -log_files stderr -size -5000 2>/dev/null | tail -50'
```

**What it checks:** Recent processing activity and warnings  
**Expected output:** Log entries showing batch completions and stream starts

### 12. Check for Processing Delays

Search for "falling behind" warnings:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list | grep -E "bronze|fastf1" | head -1 | awk "{print \$1}" | xargs -I {} yarn logs -applicationId {} -log_files stderr -size -10000 2>/dev/null | grep "falling behind" | tail -10'
```

**What it checks:** If processing is slower than 30-second trigger interval  
**Expected output:** Empty if healthy, warning messages if falling behind

### 13. Check for Errors in Spark Logs

Search for ERROR entries:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list | grep -E "bronze|fastf1" | head -1 | awk "{print \$1}" | xargs -I {} yarn logs -applicationId {} -log_files stderr -size -10000 2>/dev/null | grep -E "ERROR|Exception" | grep -v "WARN\|RuntimeWarning" | tail -10'
```

**What it checks:** Application errors and exceptions  
**Expected output:** Empty if healthy, error stack traces if issues exist

### 14. Check Application Start Time

Get human-readable start time:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'APP_ID=$(yarn application -list | grep -E "bronze|fastf1" | head -1 | awk "{print \$1}"); yarn application -status $APP_ID 2>/dev/null | grep "Start-Time"'
```

**What it checks:** When the Spark job was started  
**Expected output:** Start-Time timestamp in milliseconds

### 15. Check Environment Variables on EMR

Verify environment variables are set:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && echo "KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP"; echo "SPARK_BRONZE_BASE=$SPARK_BRONZE_BASE"; echo "SPARK_CHECKPOINT_BASE=$SPARK_CHECKPOINT_BASE"'
```

**What it checks:** Required variables are properly configured  
**Expected output:** Each variable with its value

### 16. Check Checkpoint Consumed Offsets

Read checkpoint offset file to see messages consumed:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && aws s3 cp ${SPARK_CHECKPOINT_BASE}bronze/telemetry_raw-parsed/offsets/0 - 2>/dev/null | tail -1'
```

**What it checks:** How many Kafka messages have been consumed  
**Expected output:** JSON with partition offsets like `{"telemetry.raw":{"0":220896,"1":422166,"2":473764}}`

### 17. View Sample Parquet Output

Download and inspect a sample output file:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && FILE=$(aws s3 ls ${SPARK_BRONZE_BASE}telemetry_raw-parsed/ --recursive 2>/dev/null | grep ".parquet" | grep -v "_delta_log" | head -1 | awk "{print \$4}"); aws s3 cp s3://$(echo ${SPARK_BRONZE_BASE} | sed "s|s3://||")$FILE /tmp/sample.parquet && python3 -c "import pyarrow.parquet as pq; t=pq.read_table(\"/tmp/sample.parquet\"); print(f\"Rows: {t.num_rows}\"); print(f\"Columns: {t.num_columns}\")"'
```

**What it checks:** Actual data in output files  
**Expected output:** Row count and column count

### 18. Check EMR Cluster Nodes

List cluster nodes and resources:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn node -list -all'
```

**What it checks:** Available compute resources  
**Expected output:** Node hostnames, state, and available resources

### 19. View Application Tracking URL

Get Spark UI URL:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list | grep -E "bronze|fastf1" | head -1 | awk "{print \"Application ID: \" \$1 \"\nTracking URL: \" \$9}"'
```

**What it checks:** Where to access Spark UI  
**Expected output:** Application ID and tracking URL for port forwarding

### 20. Kill Running Spark Jobs

Terminate all bronze_stream applications:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list | grep -E "bronze|fastf1" | awk "{print \$1}" | xargs -I {} yarn application -kill {}'
```

**What it does:** Stops running Spark streaming jobs  
**Use case:** Before redeploying with updated code

---

## Copy and Run the Entire Script

To copy the entire check_pipeline_status.sh script and run it directly on EMR:

### Copy Script to EMR
```bash
# Copy the script
scp -i ${SSH_KEY} scripts/check_pipeline_status.sh hadoop@${EMR_MASTER_DNS}:~/

# Make executable
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'chmod +x check_pipeline_status.sh'
```

### Run on EMR Machine

SSH to EMR and run directly:
```bash
# SSH to EMR
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS}

# Run basic check
./check_pipeline_status.sh

# Run detailed check
./check_pipeline_status.sh --detailed

# Run reset
./check_pipeline_status.sh --reset
```

### Run from Local Machine (One-liner)

Run the script remotely without SSH login:
```bash
# Basic check
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh'

# Detailed check
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh --detailed'

# Automated reset (option 2 - full cleanup)
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh --reset' << 'EOF'
2
yes
EOF
```

---

## Status Check Output

### Section Breakdown

#### 1. YARN Applications
```
--- YARN Applications ---
Application ID: application_1762718943551_0011
State: RUNNING
Started: Mon Nov  9 23:45:12 UTC 2025
```

Shows whether the Spark streaming job is active. If no application is found, indicates the job needs to be submitted.

#### 2. Kafka Topics & Message Counts
```
--- Kafka Topics & Message Counts ---
Topics found:
  race.events: 2343 messages (3 partitions)
  telemetry.raw: 1116826 messages (3 partitions)
```

Total messages available in Kafka. With `--detailed`, also shows per-partition breakdown.

#### 3. Spark Streaming Checkpoints
```
--- Spark Streaming Checkpoints ---
Active streaming queries:
  telemetry_raw-parsed: 6 batches processed
  telemetry_raw-raw: 6 batches processed
  race_events-parsed: 2 batches processed
  race_events-raw: 2 batches processed
```

Number of successfully committed batches. Each batch represents one 30-second trigger interval.

#### 4. S3 Bronze Layer Output
```
--- S3 Bronze Layer Output ---
Output streams:
  telemetry_raw-parsed: 25 files, 45.17 MB
  telemetry_raw-raw: 25 files, 88.02 MB
  race_events-parsed: 5 files, 182.88 KB
  race_events-raw: 5 files, 223.68 KB
```

Actual data written to S3. File count should increase with each batch.

#### 5. Data Processing Efficiency
```
--- Data Processing Efficiency ---
  Kafka messages available: 1119169
  Checkpoint shows consumed: 1116826
  S3 output size: 133.58 MB
```

Compares Kafka message counts against checkpoint offsets and S3 output size. If checkpoint shows messages consumed but S3 output is minimal, a **DATA LOSS DETECTED** warning appears with troubleshooting suggestions.

#### 6. Recent Streaming Activity (--detailed only)
```
--- Recent Streaming Activity ---
Application: application_1762718943551_0011

Recent logs (last 20 lines):
Started stream for telemetry.raw-parsed -> s3://bucket/bronze/telemetry_raw-parsed
Started stream for race.events-parsed -> s3://bucket/bronze/race_events-parsed
Batch: 5 completed at 2025-11-09 23:46:42
```

Shows recent log entries from the Spark application stderr.

#### 7. Error Detection (--detailed only)
```
--- Error Detection ---
Checking for errors in application logs...
  No errors found in recent logs
```

Searches for ERROR and Exception entries. If found, displays the last 10 occurrences.

---

## Data Loss Detection

The script automatically detects scenarios where Spark consumes Kafka messages but fails to write them to S3:

**Triggers:**
- Checkpoint shows > 100K messages consumed
- S3 output size < 10 MB

**Possible causes:**
- Schema mismatch between Kafka JSON and Spark schema
- JSON parsing failures returning all NULL values
- Filtering logic removing valid data
- Memory issues preventing writes

**Recommended action:**
Run `./check_pipeline_status.sh --reset` to clean checkpoints and restart with corrected code.

---

## Environment Variable Reference

The script sources `~/spark.env` on the EMR master node. Required variables:

| Variable | Purpose | Example |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP` | MSK broker connection string | `b-1.cluster.kafka.us-east-1.amazonaws.com:9092,...` |
| `SPARK_BRONZE_BASE` | S3 URI for bronze layer output | `s3://bucket-name/bronze/` |
| `SPARK_CHECKPOINT_BASE` | S3 URI for checkpoint storage | `s3://bucket-name/checkpoints/` |
| `S3_RAW_BUCKET` | Bucket name (no s3:// prefix) | `bucket-name` |
| `S3_CHECKPOINTS_BUCKET` | Bucket name (no s3:// prefix) | `bucket-name` |

### Creating ~/spark.env

From your local machine after Terraform deployment:
```bash
cd infra

cat > /tmp/spark.env << EOF
export KAFKA_BOOTSTRAP="$(terraform output -raw msk_bootstrap_brokers_plaintext)"
export SPARK_BRONZE_BASE="s3://$(terraform output -raw s3_raw_bucket)/bronze/"
export SPARK_CHECKPOINT_BASE="s3://$(terraform output -raw s3_checkpoints_bucket)/checkpoints/"
export S3_RAW_BUCKET="$(terraform output -raw s3_raw_bucket)"
export S3_CHECKPOINTS_BUCKET="$(terraform output -raw s3_checkpoints_bucket)"
EOF

scp -i ${SSH_KEY} /tmp/spark.env hadoop@${EMR_MASTER_DNS}:~/spark.env
```

---

## Troubleshooting

### Script Reports "No bronze_stream application found"

**Cause**: The Spark job isn't running or has a different name.

**Check:**
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'yarn application -list'
```

The script searches for applications containing "bronze" or "fastf1" in the name. If your application has a different name, update the grep pattern in `check_yarn_applications()` function.

### "Failed to connect to Kafka" errors

**Cause**: Kafka bootstrap servers unreachable or environment variable not set.

**Verify:**
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'source ~/spark.env && echo $KAFKA_BOOTSTRAP'
```

Should print comma-separated MSK broker addresses. If empty, recreate `~/spark.env` as shown above.

### S3 commands fail with access denied

**Cause**: EMR instance role lacks S3 permissions.

**Verify:**
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} 'aws s3 ls s3://${S3_RAW_BUCKET}/'
```

If this fails, check the EMR instance profile IAM role has `s3:ListBucket` and `s3:GetObject` permissions on the raw and checkpoint buckets.

### Checkpoint shows consumed messages but S3 output is tiny

**Cause**: Data loss during processing - messages read but not written.

**Common reasons:**
1. **Starting offsets**: If not specifying `--starting-offsets earliest`, job defaults to `latest` and only processes NEW messages
2. **Schema mismatch**: JSON parsing failures cause all fields to be NULL
3. **Overly aggressive filtering**: NULL filters removing valid data

**Solution:**
1. Kill the job: `yarn application -kill application_xxxxx`
2. Reset: `./check_pipeline_status.sh --reset` (option 2 - full cleanup)
3. Resubmit with `--starting-offsets earliest` parameter

---

## Integration with spark-submit

The status checker is designed to monitor jobs submitted with this pattern:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name fastf1-bronze-stream \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --py-files s3://${S3_ARTIFACTS_BUCKET}/spark/spark_package.zip \
  s3://${S3_ARTIFACTS_BUCKET}/spark/bronze_stream.py \
  --bootstrap-servers ${KAFKA_BOOTSTRAP} \
  --starting-offsets earliest \
  --output-base ${SPARK_BRONZE_BASE} \
  --checkpoint-base ${SPARK_CHECKPOINT_BASE}bronze \
  --write-format delta
```

The application name (`--name fastf1-bronze-stream`) should contain "bronze" or "fastf1" for the script to detect it automatically.

---

## Script Dependencies

The checker requires these tools on the EMR master node:

| Tool | Purpose | Location |
|------|---------|----------|
| `yarn` | Query YARN applications | `/usr/bin/yarn` |
| `aws` | S3 operations | `/usr/bin/aws` |
| `kafka-run-class.sh` | Get Kafka topic offsets | `~/kafka-tools/bin/` |
| `python3` | Parse checkpoint JSON | `/usr/bin/python3` |

The Kafka tools directory (`~/kafka-tools/`) should be created during EMR bootstrap or manually copied from a Kafka distribution.

---

## Exit Codes

The script uses exit codes to indicate status:

| Code | Meaning |
|------|---------|
| `0` | Success - pipeline healthy |
| `1` | Warning - issues detected (check output) |
| `2` | Error - critical failure |

Use exit codes in automation:
```bash
if ! ./check_pipeline_status.sh; then
  echo "Pipeline issues detected, sending alert..."
fi
```

---

## Advanced Usage

### Automated Monitoring Loop

Run status checks every 5 minutes:
```bash
while true; do
  ./check_pipeline_status.sh
  sleep 300
done
```

### Capture Output to File

Log status checks with timestamps:
```bash
./check_pipeline_status.sh --detailed >> /tmp/pipeline_status.log 2>&1
```

### Remote Execution from Local Machine

Run checks without SSH'ing to EMR:
```bash
ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh --detailed'
```

### Conditional Reset

Auto-reset if data loss detected:
```bash
if ./check_pipeline_status.sh | grep -q "DATA LOSS DETECTED"; then
  echo "2" | ./check_pipeline_status.sh --reset
fi
```

---

## Related Documentation

- **Spark Jobs**: See `spark/README.md` for packaging and deployment details
- **Infrastructure**: See `infra/README.md` for Terraform setup and outputs
- **Kafka Producer**: See `kafka/README.md` for message production setup

---

## Summary

The `check_pipeline_status.sh` script provides comprehensive monitoring for Spark Structured Streaming jobs on EMR. Deploy it to the master node, ensure environment variables are set, and run regularly to maintain pipeline visibility.

For a complete health check:
```bash
./check_pipeline_status.sh --detailed
```

For pipeline reset:
```bash
./check_pipeline_status.sh --reset
```

