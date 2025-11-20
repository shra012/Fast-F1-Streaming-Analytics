#!/bin/bash
#
# Simple Pipeline State Check
# Quick overview of all pipeline components
# Usage: ./simple_clean_state_check.sh
#

set -eo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Load environment
if [[ -f ~/spark.env ]]; then
  source ~/spark.env
else
  echo -e "${RED}Error: ~/spark.env not found${NC}"
  exit 1
fi

echo -e "${CYAN}🔍 PIPELINE STATE CHECK${NC}"
echo -e "${CYAN}======================${NC}"
echo ""

echo -e "${BLUE}1️⃣  YARN Applications:${NC}"
yarn application -list 2>/dev/null | grep -E 'Application-Id|bronze_stream|gold_stream' || echo -e "${YELLOW}  No streaming apps running${NC}"

echo ""
echo -e "${BLUE}2️⃣  Producer Status:${NC}"
producer_count=$(ps aux | grep '[p]roducer.py' || true | wc -l)
producer_count=$(echo "$producer_count" | tr -d ' ')
echo "  Producer processes: $producer_count"
if [[ $producer_count -gt 0 ]]; then
  ps aux | grep '[p]roducer.py' | head -1 | awk '{print "  PID: " $2 ", Running time: " $10}' || true
fi

echo ""
echo -e "${BLUE}3️⃣  Kafka Message Counts:${NC}"
if [[ -d ~/kafka-tools ]] && [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
  telem_count=$(~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list ${KAFKA_BOOTSTRAP} --topic telemetry.raw --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print sum}' || echo "0")
  events_count=$(~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list ${KAFKA_BOOTSTRAP} --topic race.events --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print sum}' || echo "0")
  echo "  telemetry.raw: ${telem_count:-0} messages"
  echo "  race.events: ${events_count:-0} messages"
else
  echo -e "${YELLOW}  Kafka tools not found or KAFKA_BOOTSTRAP not set${NC}"
fi

echo ""
echo -e "${BLUE}4️⃣  S3 Bronze Data (file counts):${NC}"
for dir in telemetry_raw-parsed telemetry_raw-raw race_events-parsed race_events-raw; do
  count=$(aws s3 ls s3://f1-streaming-graph-dev-9ea1ff-raw/bronze/$dir/ --recursive 2>/dev/null || true | wc -l | tr -d ' ')
  count=${count:-0}
  echo "  $dir: $count files"
done

echo ""
echo -e "${BLUE}5️⃣  Bronze/Gold Checkpoint Status:${NC}"
bronze_ckpt=$(aws s3 ls s3://f1-streaming-graph-dev-9ea1ff-checkpoints/checkpoints/bronze/ --recursive 2>/dev/null || true | wc -l | tr -d ' ')
gold_ckpt=$(aws s3 ls s3://f1-streaming-graph-dev-9ea1ff-checkpoints/checkpoints/gold/ --recursive 2>/dev/null || true | wc -l | tr -d ' ')
bronze_ckpt=${bronze_ckpt:-0}
gold_ckpt=${gold_ckpt:-0}
echo "  Bronze checkpoint files: $bronze_ckpt"
echo "  Gold checkpoint files: $gold_ckpt"

echo ""
echo -e "${BLUE}6️⃣  Neo4j Data:${NC}"
if [[ -n "${NEO4J_URI:-}" ]]; then
  echo "MATCH (n) RETURN labels(n)[0] as label, count(n) as count ORDER BY count DESC LIMIT 5;" | \
    cypher-shell -a ${NEO4J_URI} -u ${NEO4J_USERNAME} -p ${NEO4J_PASSWORD} --database ${NEO4J_DATABASE} --format plain 2>&1 | \
    grep -v 'WARNING\|unsupported' || echo -e "${YELLOW}  Neo4j empty or connection error${NC}"
else
  echo -e "${YELLOW}  Neo4j credentials not configured${NC}"
fi

echo ""
echo -e "${GREEN}✅ State check complete!${NC}"

