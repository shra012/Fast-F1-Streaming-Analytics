#!/usr/bin/env bash
# Comprehensive setup script for EMR + Kafka producer + Spark jobs
# This script combines functionality from:
# - setup_producer_on_emr.sh
# - prepare_emr_job.sh
# - install_kafka_tools_on_emr.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INFRA_DIR="${REPO_ROOT}/infra"
KAFKA_DIR="${REPO_ROOT}/kafka"
SPARK_DIR="${REPO_ROOT}/spark"
PRODUCER_DIR="${KAFKA_DIR}/producer"
CREATE_TOPICS_SRC="${KAFKA_DIR}/scripts/create_topics.py"
TOPICS_FILE_SRC="${KAFKA_DIR}/topics.yaml"
REQUIREMENTS_FILE="${KAFKA_DIR}/requirements.txt"
BOOTSTRAP_DIR="${PRODUCER_DIR}/emr_bootstrap"
ENV_FILE="${SPARK_DIR}/emr_job.env"
SSH_KEY="${LOCAL_SSH_KEY:-${HOME}/.ssh/id_rsa}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
  echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
  echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "Required command '$1' not found in PATH"
    exit 1
  fi
}

tf_output() {
  local name=$1
  terraform -chdir="${INFRA_DIR}" output -raw "${name}" 2>/dev/null || true
}

cleanup_bootstrap_dir() {
  if [[ -d "${BOOTSTRAP_DIR}" ]]; then
    rm -rf "${BOOTSTRAP_DIR}"
  fi
}

prepare_bootstrap_payload() {
  if [[ -e "${BOOTSTRAP_DIR}" ]]; then
    log_error "Bootstrap directory ${BOOTSTRAP_DIR} already exists. Remove it before rerunning."
    exit 1
  fi

  mkdir -p "${BOOTSTRAP_DIR}"
  cp "${CREATE_TOPICS_SRC}" "${BOOTSTRAP_DIR}/create_topics.py"
  cp "${TOPICS_FILE_SRC}" "${BOOTSTRAP_DIR}/topics.yaml"
}

trap cleanup_bootstrap_dir EXIT

# Check prerequisites
log_info "Checking prerequisites..."
require_cmd terraform
require_cmd make
require_cmd scp
require_cmd ssh

# Gather Terraform outputs
log_info "Gathering Terraform outputs..."
EMR_MASTER_DNS=$(tf_output emr_cluster_master_public_dns)
EMR_CLUSTER_ID=$(tf_output emr_cluster_id)
KAFKA_BOOTSTRAP=$(tf_output msk_bootstrap_brokers)
SPARK_BRONZE_BASE=$(tf_output spark_bronze_base_uri)
SPARK_SILVER_BASE=$(tf_output spark_silver_base_uri)
SPARK_GOLD_BASE=$(tf_output spark_gold_base_uri)
SPARK_CHECKPOINT_BASE=$(tf_output s3_checkpoint_uri)
SPARK_ARTIFACT_BUCKET=$(tf_output s3_artifacts_bucket)
EMR_KEY_PAIR_NAME=$(tf_output emr_key_pair_name)

# Validate required outputs
missing_vars=()
[[ -n "${EMR_MASTER_DNS}" ]] || missing_vars+=("emr_cluster_master_public_dns")
[[ -n "${EMR_CLUSTER_ID}" ]] || missing_vars+=("emr_cluster_id")
[[ -n "${KAFKA_BOOTSTRAP}" ]] || missing_vars+=("msk_bootstrap_brokers")
[[ -n "${SPARK_ARTIFACT_BUCKET}" ]] || missing_vars+=("s3_artifacts_bucket")
[[ -n "${SPARK_BRONZE_BASE}" ]] || missing_vars+=("spark_bronze_base_uri")
[[ -n "${SPARK_CHECKPOINT_BASE}" ]] || missing_vars+=("s3_checkpoint_uri")

if (( ${#missing_vars[@]} > 0 )); then
  log_error "Missing Terraform outputs: ${missing_vars[*]}"
  log_error "Run 'cd ${INFRA_DIR} && terraform apply' before executing this script."
  exit 1
fi

if [[ ! -f "${SSH_KEY}" ]]; then
  log_error "SSH private key not found at ${SSH_KEY}"
  log_error "Set LOCAL_SSH_KEY environment variable to the correct path"
  exit 1
fi

ARTIFACT_PREFIX="s3://${SPARK_ARTIFACT_BUCKET}/spark"

log_info "EMR Master: ${EMR_MASTER_DNS}"
log_info "Kafka Bootstrap: ${KAFKA_BOOTSTRAP}"
log_info "Artifact Bucket: ${ARTIFACT_PREFIX}"

# ============================================================================
# STEP 1: Package and upload Spark artifacts
# ============================================================================
log_info "STEP 1: Packaging Spark sources..."
make -C "${SPARK_DIR}" package

log_info "Uploading Spark artifacts to ${ARTIFACT_PREFIX}..."
make -C "${SPARK_DIR}" upload S3_PREFIX="${ARTIFACT_PREFIX}"

# ============================================================================
# STEP 2: Create Spark environment file and copy to EMR
# ============================================================================
log_info "STEP 2: Creating Spark environment file..."
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

log_info "Copying Spark environment file to EMR..."
scp -i "${SSH_KEY}" "${ENV_FILE}" "hadoop@${EMR_MASTER_DNS}:~/spark.env"

# ============================================================================
# STEP 3: Prepare and copy producer files
# ============================================================================
log_info "STEP 3: Preparing producer bootstrap payload..."
prepare_bootstrap_payload

log_info "Copying producer to EMR master..."
scp -i "${SSH_KEY}" -r "${PRODUCER_DIR}" "hadoop@${EMR_MASTER_DNS}:~/"

# ============================================================================
# STEP 4: Setup Python venv, install dependencies, create topics, install Kafka tools
# ============================================================================
log_info "STEP 4: Setting up Python environment and creating Kafka topics on EMR..."

# Ask about IAM signer installation
echo
read -p "Install aws-msk-iam-sasl-signer-python for IAM auth? (y/N): " -n 1 -r
echo
INSTALL_IAM_SIGNER="${REPLY}"

ssh -i "${SSH_KEY}" "hadoop@${EMR_MASTER_DNS}" bash -s "${KAFKA_BOOTSTRAP}" "${INSTALL_IAM_SIGNER}" <<'EOF'
set -euo pipefail

KAFKA_BOOTSTRAP="$1"
INSTALL_IAM_SIGNER="$2"
KAFKA_VERSION="3.6.0"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_TAR="${KAFKA_DIR}.tgz"
KAFKA_TOOLS_DIR="${HOME}/kafka-tools"

echo "========================================="
echo "Setting up Python virtual environment..."
echo "========================================="

# Create virtual environment
if [[ ! -d ~/producer/.venv ]]; then
  python3 -m venv ~/producer/.venv
  echo "[OK] Virtual environment created"
else
  echo "[OK] Virtual environment already exists"
fi

# Install dependencies
echo "Installing Python dependencies..."
~/producer/.venv/bin/pip install --upgrade pip -q
~/producer/.venv/bin/pip install -q \
  fastf1 \
  kafka-python==2.0.2 \
  python-dotenv \
  pandas \
  tqdm \
  PyYAML

echo "[OK] Python dependencies installed"

# Install IAM signer if requested
if [[ "${INSTALL_IAM_SIGNER}" =~ ^[Yy]$ ]]; then
  echo "Installing aws-msk-iam-sasl-signer-python..."
  
  if ! command -v git >/dev/null 2>&1; then
    echo "Installing git..."
    sudo yum install -y git -q
  fi
  
  if ~/producer/.venv/bin/pip install -q git+https://github.com/aws/aws-msk-iam-sasl-signer-python@v1.0.2; then
    echo "[OK] aws-msk-iam-sasl-signer-python installed"
  else
    echo "[WARN] Failed to install aws-msk-iam-sasl-signer-python"
  fi
fi

echo
echo "========================================="
echo "Creating Kafka topics..."
echo "========================================="

# Create topics using the create_topics.py script
~/producer/.venv/bin/python ~/producer/emr_bootstrap/create_topics.py \
  --auth-mode plain \
  --bootstrap "${KAFKA_BOOTSTRAP}" \
  --topics-file ~/producer/emr_bootstrap/topics.yaml

echo "[OK] Kafka topics created"

echo
echo "========================================="
echo "Installing Kafka console tools..."
echo "========================================="

if [[ -d "${KAFKA_TOOLS_DIR}" ]]; then
  echo "[OK] Kafka tools already installed at ${KAFKA_TOOLS_DIR}"
else
  echo "Downloading Kafka ${KAFKA_VERSION}..."
  cd ~
  
  if [[ ! -f "${KAFKA_TAR}" ]]; then
    if ! wget -q --show-progress "https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TAR}" 2>/dev/null; then
      echo "Trying archive mirror..."
      wget -q --show-progress "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TAR}"
    fi
  fi
  
  echo "Extracting Kafka..."
  tar -xzf "${KAFKA_TAR}"
  mv "${KAFKA_DIR}" "${KAFKA_TOOLS_DIR}"
  rm -f "${KAFKA_TAR}"
  
  echo "[OK] Kafka tools installed at ${KAFKA_TOOLS_DIR}"
fi

echo
echo "========================================="
echo "Kafka Console Tools Usage Examples"
echo "========================================="
echo
echo "List topics:"
echo "  ${KAFKA_TOOLS_DIR}/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list"
echo
echo "Describe a topic:"
echo "  ${KAFKA_TOOLS_DIR}/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --describe --topic telemetry.raw"
echo
echo "Consume messages:"
echo "  ${KAFKA_TOOLS_DIR}/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic telemetry.raw --from-beginning --max-messages 10"
echo
echo "Check consumer groups:"
echo "  ${KAFKA_TOOLS_DIR}/bin/kafka-consumer-groups.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list"
echo "  ${KAFKA_TOOLS_DIR}/bin/kafka-consumer-groups.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --describe --all-groups"
echo

echo "========================================="
echo "Configuring shell auto-load for spark.env..."
echo "========================================="

# Add spark.env to .bashrc for auto-loading on SSH login
if ! grep -q "source ~/spark.env" ~/.bashrc 2>/dev/null; then
  echo "" >> ~/.bashrc
  echo "# Auto-load Spark environment variables" >> ~/.bashrc
  echo "if [ -f ~/spark.env ]; then" >> ~/.bashrc
  echo "    source ~/spark.env" >> ~/.bashrc
  echo "fi" >> ~/.bashrc
  echo "[OK] Added spark.env auto-load to .bashrc"
else
  echo "[OK] spark.env already configured in .bashrc"
fi

# Also add to .bash_profile if it exists
if [ -f ~/.bash_profile ]; then
  if ! grep -q "source ~/spark.env" ~/.bash_profile 2>/dev/null; then
    echo "" >> ~/.bash_profile
    echo "# Auto-load Spark environment variables" >> ~/.bash_profile
    echo "if [ -f ~/spark.env ]; then" >> ~/.bash_profile
    echo "    source ~/spark.env" >> ~/.bash_profile
    echo "fi" >> ~/.bash_profile
    echo "[OK] Added spark.env auto-load to .bash_profile"
  else
    echo "[OK] spark.env already configured in .bash_profile"
  fi
fi

EOF

# ============================================================================
# Summary
# ============================================================================
echo
echo "========================================================================"
log_info "Setup Complete! [OK]"
echo "========================================================================"
echo
log_info "Next steps:"
echo
echo "1. Run the producer on EMR:"
echo "   ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS}"
echo "   ~/producer/.venv/bin/python ~/producer/producer.py \\"
echo "     --bootstrap ${KAFKA_BOOTSTRAP} \\"
echo "     --start-year 2024 \\"
echo "     --event Bahrain \\"
echo "     --session R \\"
echo "     --speedup 50"
echo
echo "2. Start the Bronze streaming job:"
echo "   ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS}"
echo "   source ~/spark.env"
echo "   spark-submit \\"
echo "     --master yarn \\"
echo "     --deploy-mode cluster \\"
echo "     --name bronze_stream \\"
echo "     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \\"
echo "     --py-files s3://\${SPARK_ARTIFACT_BUCKET}/spark/spark_package.zip \\"
echo "     s3://\${SPARK_ARTIFACT_BUCKET}/spark/bronze_stream.py \\"
echo "     --bootstrap-servers \"\${KAFKA_BOOTSTRAP}\" \\"
echo "     --telemetry-topic telemetry.raw \\"
echo "     --events-topic race.events \\"
echo "     --output-base \"\${SPARK_BRONZE_BASE}\" \\"
echo "     --checkpoint-base \"\${SPARK_CHECKPOINT_BASE}/bronze\""
echo
echo "3. Monitor the Spark job:"
echo "   yarn application -list"
echo "   yarn logs -applicationId <app_id>"
echo
echo "4. Check Kafka topics and messages:"
echo "   ~/kafka-tools/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list"
echo "   ~/kafka-tools/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic telemetry.raw --max-messages 5"
echo
echo "========================================================================"
