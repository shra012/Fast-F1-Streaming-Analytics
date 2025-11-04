#!/usr/bin/env bash

set -euo pipefail

# Synchronize Terraform MSK outputs into kafka/.env so that local tooling can
# connect without manual copy/paste.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
KAFKA_DIR=${KAFKA_DIR:-"$REPO_DIR/kafka"}
INFRA_DIR=${INFRA_DIR:-"$REPO_DIR/infra"}
ENV_FILE=${ENV_FILE:-"$KAFKA_DIR/.env"}

log() {
  echo "[sync-msk-env] $*"
}

require_dir() {
  local path=$1
  if [[ ! -d "$path" ]]; then
    log "Directory not found: $path" >&2
    exit 1
  fi
}

terraform_output() {
  local name=$1
  local value
  if value=$(cd "$INFRA_DIR" && terraform output -raw "$name" 2>/dev/null); then
    value=${value%$'\r'}
    if [[ -n "$value" ]]; then
      printf '%s' "$value"
      return 0
    fi
  fi
  return 1
}

update_env_var() {
  local key=$1
  local value=$2

  mkdir -p "$(dirname "$ENV_FILE")"
  touch "$ENV_FILE"

  local tmp
  tmp=$(mktemp "${ENV_FILE}.XXXXXX")
  if grep -q "^${key}=" "$ENV_FILE"; then
    awk -v key="$key" -v value="$value" -F'=' 'BEGIN { OFS = "=" } $1 == key { $0 = key "=" value } 1' "$ENV_FILE" >"$tmp"
  else
    cat "$ENV_FILE" >"$tmp"
    printf '%s=%s\n' "$key" "$value" >>"$tmp"
  fi
  mv "$tmp" "$ENV_FILE"
  log "Set $key in $ENV_FILE"
}

require_dir "$INFRA_DIR"
require_dir "$KAFKA_DIR"

if ! command -v terraform >/dev/null 2>&1; then
  log "Terraform not found in PATH" >&2
  exit 1
fi

auth_mode=${KAFKA_AUTH_MODE:-iam}
case "$auth_mode" in
  iam|plain) ;;
  *) auth_mode=iam ;;
esac
update_env_var KAFKA_AUTH_MODE "$auth_mode"

bootstrap=""
cluster_arn=""
region=""

if bootstrap=$(terraform_output msk_bootstrap_brokers_sasl_iam); then
  update_env_var MSK_BOOTSTRAP_BROKERS "$bootstrap"
  update_env_var KAFKA_BOOTSTRAP_BROKERS "$bootstrap"
else
  log "Terraform output 'msk_bootstrap_brokers_sasl_iam' not available; skipping." >&2
fi

if cluster_arn=$(terraform_output msk_cluster_arn); then
  update_env_var MSK_CLUSTER_ARN "$cluster_arn"
else
  log "Terraform output 'msk_cluster_arn' not available; skipping." >&2
fi

if region=$(terraform_output aws_region); then
  update_env_var AWS_REGION "$region"
elif [[ -n "$cluster_arn" ]]; then
  region=$(printf '%s' "$cluster_arn" | awk -F: '{print $4}')
  if [[ -n "$region" ]]; then
    update_env_var AWS_REGION "$region"
  else
    log "Unable to derive AWS region from MSK cluster ARN." >&2
  fi
else
  log "Terraform output 'aws_region' not available; skipping AWS_REGION." >&2
fi

if bronze_base=$(terraform_output spark_bronze_base_uri); then
  update_env_var SPARK_BRONZE_BASE "$bronze_base"
else
  log "Terraform output 'spark_bronze_base_uri' not available; skipping SPARK_BRONZE_BASE." >&2
fi

if silver_base=$(terraform_output spark_silver_base_uri); then
  update_env_var SPARK_SILVER_BASE "$silver_base"
else
  log "Terraform output 'spark_silver_base_uri' not available; skipping SPARK_SILVER_BASE." >&2
fi

if gold_base=$(terraform_output spark_gold_base_uri); then
  update_env_var SPARK_GOLD_BASE "$gold_base"
else
  log "Terraform output 'spark_gold_base_uri' not available; skipping SPARK_GOLD_BASE." >&2
fi

if checkpoint_uri=$(terraform_output s3_checkpoint_uri); then
  update_env_var SPARK_CHECKPOINT_BASE "${checkpoint_uri%/}"
else
  log "Terraform output 's3_checkpoint_uri' not available; skipping SPARK_CHECKPOINT_BASE." >&2
fi

if checkpoint_bucket=$(terraform_output s3_checkpoints_bucket); then
  update_env_var S3_CHECKPOINTS_BUCKET "$checkpoint_bucket"
fi

if artifacts_bucket=$(terraform_output s3_artifacts_bucket); then
  update_env_var S3_ARTIFACTS_BUCKET "$artifacts_bucket"
fi

if raw_bucket=$(terraform_output s3_raw_bucket); then
  update_env_var S3_RAW_BUCKET "$raw_bucket"
fi

log "Environment sync complete."
