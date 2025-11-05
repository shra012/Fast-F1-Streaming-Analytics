#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INFRA_DIR="${REPO_ROOT}/infra"
SPARK_DIR="${REPO_ROOT}/spark"
ENV_FILE="${SPARK_DIR}/emr_job.env"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

tf_output() {
  local name=$1
  terraform -chdir="${INFRA_DIR}" output -raw "${name}" 2>/dev/null || true
}

require_cmd terraform
require_cmd make

echo "Gathering Terraform outputs..."
EMR_MASTER_DNS=$(tf_output emr_cluster_master_public_dns)
EMR_CLUSTER_ID=$(tf_output emr_cluster_id)
KAFKA_BOOTSTRAP=$(tf_output msk_bootstrap_brokers)
SPARK_BRONZE_BASE=$(tf_output spark_bronze_base_uri)
SPARK_SILVER_BASE=$(tf_output spark_silver_base_uri)
SPARK_GOLD_BASE=$(tf_output spark_gold_base_uri)
SPARK_CHECKPOINT_BASE=$(tf_output s3_checkpoint_uri)
SPARK_ARTIFACT_BUCKET=$(tf_output s3_artifacts_bucket)
EMR_KEY_PAIR_NAME=$(tf_output emr_key_pair_name)

missing_vars=()
[[ -n "${EMR_MASTER_DNS}" ]] || missing_vars+=("emr_cluster_master_public_dns")
[[ -n "${EMR_CLUSTER_ID}" ]] || missing_vars+=("emr_cluster_id")
[[ -n "${KAFKA_BOOTSTRAP}" ]] || missing_vars+=("msk_bootstrap_brokers")
[[ -n "${SPARK_ARTIFACT_BUCKET}" ]] || missing_vars+=("s3_artifacts_bucket")
[[ -n "${SPARK_BRONZE_BASE}" ]] || missing_vars+=("spark_bronze_base_uri")
[[ -n "${SPARK_SILVER_BASE}" ]] || missing_vars+=("spark_silver_base_uri")
[[ -n "${SPARK_GOLD_BASE}" ]] || missing_vars+=("spark_gold_base_uri")
[[ -n "${SPARK_CHECKPOINT_BASE}" ]] || missing_vars+=("s3_checkpoint_uri")

if (( ${#missing_vars[@]} > 0 )); then
  echo "Error: missing Terraform outputs (${missing_vars[*]})." >&2
  echo "Run 'cd ${INFRA_DIR} && terraform apply' before executing this script." >&2
  exit 1
fi

ARTIFACT_PREFIX="s3://${SPARK_ARTIFACT_BUCKET}/spark"

echo "Packaging Spark sources..."
make -C "${SPARK_DIR}" package

echo "Uploading Spark artifacts to ${ARTIFACT_PREFIX}..."
make -C "${SPARK_DIR}" upload S3_PREFIX="${ARTIFACT_PREFIX}"

echo "Writing environment file to ${ENV_FILE}..."
cat > "${ENV_FILE}" <<EOF
EMR_CLUSTER_ID=${EMR_CLUSTER_ID}
EMR_MASTER_DNS=${EMR_MASTER_DNS}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP}
SPARK_ARTIFACT_BUCKET=${SPARK_ARTIFACT_BUCKET}
SPARK_ARTIFACT_PREFIX=${ARTIFACT_PREFIX}
SPARK_BRONZE_BASE=${SPARK_BRONZE_BASE}
SPARK_SILVER_BASE=${SPARK_SILVER_BASE}
SPARK_GOLD_BASE=${SPARK_GOLD_BASE}
SPARK_CHECKPOINT_BASE=${SPARK_CHECKPOINT_BASE}
EMR_KEY_PAIR_NAME=${EMR_KEY_PAIR_NAME}
EOF
chmod 600 "${ENV_FILE}"

SSH_KEY_PATH="${LOCAL_SSH_KEY:-${HOME}/.ssh/id_rsa}"
if [[ ! -f "${SSH_KEY_PATH}" ]]; then
  echo "Warning: SSH private key not found at ${SSH_KEY_PATH}. Update LOCAL_SSH_KEY env var before running scp/ssh commands." >&2
fi

echo
echo "Environment file created: ${ENV_FILE}"
echo
echo "Next steps:"
echo "1. Copy the env file to EMR master (set LOCAL_SSH_KEY to override the private key path):"
echo "   scp -i ${SSH_KEY_PATH} ${ENV_FILE} hadoop@${EMR_MASTER_DNS}:~/spark.env"
[[ -n "${EMR_KEY_PAIR_NAME}" ]] && echo "   # AWS key pair associated with the cluster: ${EMR_KEY_PAIR_NAME}"
echo
echo "2. SSH into the EMR master:"
echo "   ssh -i ${SSH_KEY_PATH} hadoop@${EMR_MASTER_DNS}"
echo
echo "3. On the master node, source the env file and submit the Bronze job:"
cat <<'EOS'
   source ~/spark.env
   
   # Bronze stage - Kafka ingestion
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
     --conf spark.sql.hive.convertMetastoreParquet=false \
     --py-files s3://$SPARK_ARTIFACT_BUCKET/spark/spark_package.zip \
     s3://$SPARK_ARTIFACT_BUCKET/spark/bronze_stream.py \
     --bootstrap-servers "$KAFKA_BOOTSTRAP" \
     --telemetry-topic telemetry.raw \
     --events-topic race.events \
     --output-base "$SPARK_BRONZE_BASE" \
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/bronze"

   # Silver stage - Data transformation
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
     --conf spark.sql.hive.convertMetastoreParquet=false \
     --py-files s3://$SPARK_ARTIFACT_BUCKET/spark/spark_package.zip \
     s3://$SPARK_ARTIFACT_BUCKET/spark/silver_stream.py \
     --bronze-base "$SPARK_BRONZE_BASE" \
     --output-base "$SPARK_SILVER_BASE" \
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/silver"

   # Gold stage - Aggregations
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
     --conf spark.sql.hive.convertMetastoreParquet=false \
     --py-files s3://$SPARK_ARTIFACT_BUCKET/spark/spark_package.zip \
     s3://$SPARK_ARTIFACT_BUCKET/spark/gold_stream.py \
     --silver-base "$SPARK_SILVER_BASE" \
     --output-base "$SPARK_GOLD_BASE" \
     --checkpoint-base "$SPARK_CHECKPOINT_BASE/gold"
EOS

echo
echo "All artifact uploads and environment exports are complete."
