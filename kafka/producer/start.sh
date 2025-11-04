#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
VENV=${VENV:-"$SCRIPT_DIR/../.venv"}
ENV_FILE=${ENV_FILE:-"$SCRIPT_DIR/../.env"}
export PYTHONPATH="$SCRIPT_DIR"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ -d "$VENV" ]]; then
  # shellcheck disable=SC1090
  source "$VENV/bin/activate"
fi

python "$SCRIPT_DIR/simple_producer.py" \
  --auth-mode "${KAFKA_AUTH_MODE:-plain}" \
  --bootstrap "${KAFKA_BOOTSTRAP_BROKERS:-localhost:9092}" \
  --topic "${PRODUCER_TOPIC:-telemetry.fastf1.raw}" \
  --season "${PRODUCER_SEASON:-2023}" \
  --event "${PRODUCER_EVENT:-Bahrain Grand Prix}" \
  --session "${PRODUCER_SESSION:-R}" \
  --speedup "${PRODUCER_SPEEDUP:-30}" \
  --max-events "${PRODUCER_MAX_EVENTS:-20}" \
  --max-runtime "${PRODUCER_MAX_RUNTIME:-120}"
