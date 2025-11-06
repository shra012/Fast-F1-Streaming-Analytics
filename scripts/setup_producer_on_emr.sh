#!/usr/bin/env bash
# Setup and run producer on EMR master

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INFRA_DIR="${REPO_ROOT}/infra"

# Get EMR and MSK details from Terraform
EMR_MASTER_DNS=$(terraform -chdir="${INFRA_DIR}" output -raw emr_cluster_master_public_dns)
KAFKA_BOOTSTRAP=$(terraform -chdir="${INFRA_DIR}" output -raw msk_bootstrap_brokers)
SSH_KEY="${HOME}/.ssh/id_rsa"

if [[ -z "${EMR_MASTER_DNS}" ]]; then
  echo "Error: Could not get EMR master DNS"
  exit 1
fi

echo "Copying producer to EMR master..."
scp -i "${SSH_KEY}" -r "${REPO_ROOT}/kafka/producer" "hadoop@${EMR_MASTER_DNS}:~/"

echo
echo "Setting up Python venv and installing dependencies on EMR..."
ssh -i "${SSH_KEY}" "hadoop@${EMR_MASTER_DNS}" << 'EOF'
# Create virtual environment
python3 -m venv ~/producer/.venv

# Install dependencies in venv
~/producer/.venv/bin/pip install --upgrade pip
~/producer/.venv/bin/pip install fastf1 kafka-python-ng python-dotenv pandas tqdm

echo "Virtual environment created at ~/producer/.venv"
EOF

echo
echo "========================================================================"
echo "Producer setup complete!"
echo "========================================================================"
echo
echo
echo "To run the producer, SSH to EMR:"
echo "  ssh -i ${SSH_KEY} hadoop@${EMR_MASTER_DNS}"
echo
echo "Then run:"
echo "  cd producer"
echo "  source .venv/bin/activate"
echo "  ./producer.py \\"
echo "    --bootstrap \"${KAFKA_BOOTSTRAP}\" \\"
echo "    --start-year 2024 \\"
echo "    --event \"Bahrain\" \\"
echo "    --speedup 50"
echo
