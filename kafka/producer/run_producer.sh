#!/usr/bin/env bash
# Helper script to run the comprehensive producer with infrastructure outputs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
INFRA_DIR="${REPO_ROOT}/infra"

# Get Kafka bootstrap servers from Terraform
echo "Fetching MSK bootstrap servers from Terraform..."
BOOTSTRAP=$(terraform -chdir="${INFRA_DIR}" output -raw msk_bootstrap_brokers 2>/dev/null || echo "")

if [[ -z "$BOOTSTRAP" ]]; then
    echo "Error: Could not retrieve MSK bootstrap brokers from Terraform"
    echo "Run 'cd ${INFRA_DIR} && terraform apply' first"
    exit 1
fi

echo "Bootstrap servers: ${BOOTSTRAP}"
echo

# Run the producer with all arguments passed through
exec "${SCRIPT_DIR}/producer.py" \
    --bootstrap "$BOOTSTRAP" \
    "$@"
