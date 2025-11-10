#!/bin/bash
#
# Pipeline Status Checker
# Runs directly on EMR master node to monitor Kafka, Spark, and S3
# Usage: ./check_pipeline_status.sh [--detailed] [--reset]
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Mode flags
DETAILED_MODE=false
RESET_MODE=false

# Parse arguments
for arg in "$@"; do
  case $arg in
    --detailed)
      DETAILED_MODE=true
      ;;
    --reset)
      RESET_MODE=true
      ;;
  esac
done

# Load environment variables
if [[ -f ~/spark.env ]]; then
  source ~/spark.env
else
  echo -e "${RED}Error: ~/spark.env not found${NC}"
  echo "Run setup_all.sh first to generate the environment file"
  exit 1
fi

# Extract bucket names from full S3 paths
S3_RAW_BUCKET=$(echo "$SPARK_BRONZE_BASE" | sed 's|s3://||' | cut -d'/' -f1)
S3_CHECKPOINTS_BUCKET=$(echo "$SPARK_CHECKPOINT_BASE" | sed 's|s3://||' | cut -d'/' -f1)

# Get first broker for kafka tools
FIRST_BROKER=$(echo "$KAFKA_BOOTSTRAP" | cut -d',' -f1)

# Helper functions
print_header() {
  echo
  echo -e "${CYAN}========================================${NC}"
  echo -e "${CYAN}$1${NC}"
  echo -e "${CYAN}========================================${NC}"
}

print_subheader() {
  echo
  echo -e "${BLUE}--- $1 ---${NC}"
}

print_success() {
  echo -e "${GREEN}[OK] $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}[WARN] $1${NC}"
}

print_error() {
  echo -e "${RED}[FAIL] $1${NC}"
}

print_info() {
  echo -e "  $1"
}

# Check YARN applications
check_yarn_applications() {
  print_subheader "YARN Applications"
  
  local yarn_output
  yarn_output=$(yarn application -list 2>/dev/null || true)
  
  if echo "$yarn_output" | grep -q "bronze_stream"; then
    local app_info
    app_info=$(echo "$yarn_output" | grep "bronze_stream" | head -1)
    local app_id=$(echo "$app_info" | awk '{print $1}')
    local state=$(echo "$app_info" | awk '{print $6}')
    
    if [[ "$state" == "RUNNING" ]]; then
      print_success "Bronze stream job is RUNNING"
      print_info "Application ID: ${app_id}"
      
      if [[ "$DETAILED_MODE" == true ]]; then
        local tracking_url=$(echo "$app_info" | awk '{print $9}')
        print_info "Tracking URL: ${tracking_url}"
        
        local app_status
        app_status=$(yarn application -status ${app_id} 2>/dev/null || true)
        
        if echo "$app_status" | grep -q "Start-Time"; then
          local start_time=$(echo "$app_status" | grep "Start-Time" | awk '{print $3}')
          local start_date=$(date -d @$((start_time / 1000)) 2>/dev/null || echo "N/A")
          print_info "Started: ${start_date}"
        fi
      fi
    else
      print_warning "Bronze stream job state: ${state}"
    fi
  else
    print_warning "No bronze_stream application found"
    print_info "Check if the job was submitted"
  fi
  
  local total_apps=$(echo "$yarn_output" | grep -c "RUNNING" || echo "0")
  print_info "Total RUNNING applications: ${total_apps}"
}

# Check Kafka topics and message counts
check_kafka_topics() {
  print_subheader "Kafka Topics & Message Counts"
  
  if [[ ! -d ~/kafka-tools ]]; then
    print_warning "Kafka tools not installed at ~/kafka-tools"
    print_info "Run setup_all.sh to install Kafka console tools"
    return 0
  fi
  
  local topics
  topics=$(~/kafka-tools/bin/kafka-topics.sh --bootstrap-server ${FIRST_BROKER} --list 2>/dev/null \
    | grep -v "^__" || true)
  
  if [[ -z "$topics" ]]; then
    print_warning "No user topics found"
    return 0
  fi
  
  print_success "Topics found:"
  
  for topic in $topics; do
    local offsets
    offsets=$(~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
       --broker-list ${FIRST_BROKER} --topic ${topic} 2>/dev/null || true)
    
    if [[ -n "$offsets" ]]; then
      local total=$(echo "$offsets" | awk -F: '{sum+=$3} END {print sum}')
      local partition_count=$(echo "$offsets" | wc -l | tr -d ' ')
      print_info "[STATS] ${topic}: ${total} messages (${partition_count} partitions)"
      
      if [[ "$DETAILED_MODE" == true ]]; then
        echo "$offsets" | while read -r line; do
          local partition=$(echo "$line" | cut -d: -f2)
          local offset=$(echo "$line" | cut -d: -f3)
          print_info "   Partition ${partition}: ${offset} messages"
        done
      fi
    else
      print_info "[STATS] ${topic}: Unable to get message count"
    fi
  done
}

# Check Spark streaming checkpoints
check_checkpoints() {
  print_subheader "Spark Streaming Checkpoints"
  
  local checkpoint_base="s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze"
  
  local checkpoint_dirs
  checkpoint_dirs=$(aws s3 ls "${checkpoint_base}/" 2>/dev/null | grep "PRE" | awk '{print $2}' | sed 's/\///g' || true)
  
  if [[ -z "$checkpoint_dirs" ]]; then
    print_warning "No checkpoint directories found"
    print_info "Location: ${checkpoint_base}/"
    return 0
  fi
  
  print_success "Active streaming queries:"
  
  for stream in $checkpoint_dirs; do
    local commits
    commits=$(aws s3 ls "${checkpoint_base}/${stream}/commits/" 2>/dev/null | grep -v "PRE" | wc -l | tr -d ' ')
    
    local latest_offset
    latest_offset=$(aws s3 ls "${checkpoint_base}/${stream}/offsets/" 2>/dev/null | grep -v "PRE" | tail -1 | awk '{print $4}')
    
    if [[ $commits -gt 0 ]]; then
      print_info "[OK] ${stream}: ${commits} batches processed"
      
      if [[ "$DETAILED_MODE" == true && -n "$latest_offset" ]]; then
        print_info "   Latest offset: ${latest_offset}"
      fi
    else
      print_warning "  ${stream}: No batches committed yet"
    fi
  done
}

# Check S3 output data
check_s3_output() {
  print_subheader "S3 Bronze Layer Output"
  
  local bronze_base="s3://${S3_RAW_BUCKET}/bronze"
  
  local output_dirs
  output_dirs=$(aws s3 ls "${bronze_base}/" 2>/dev/null | grep "PRE" | awk '{print $2}' | sed 's/\///g' || true)
  
  if [[ -z "$output_dirs" ]]; then
    print_warning "No output directories found"
    print_info "Location: ${bronze_base}/"
    return 0
  fi
  
  print_success "Output streams:"
  
  for stream in $output_dirs; do
    local summary
    summary=$(aws s3 ls "${bronze_base}/${stream}/" --recursive --summarize 2>/dev/null | tail -2)
    
    if [[ -n "$summary" ]]; then
      local total_objects=$(echo "$summary" | grep "Total Objects:" | awk '{print $3}')
      local total_size=$(echo "$summary" | grep "Total Size:" | awk '{print $3}')
      
      local size_hr
      if [[ $total_size -gt 1073741824 ]]; then
        size_hr="$(awk "BEGIN {printf \"%.2f\", $total_size/1073741824}") GB"
      elif [[ $total_size -gt 1048576 ]]; then
        size_hr="$(awk "BEGIN {printf \"%.2f\", $total_size/1048576}") MB"
      elif [[ $total_size -gt 1024 ]]; then
        size_hr="$(awk "BEGIN {printf \"%.2f\", $total_size/1024}") KB"
      else
        size_hr="${total_size} bytes"
      fi
      
      print_info "[STORAGE] ${stream}: ${total_objects} files, ${size_hr}"
    else
      print_info "[STORAGE] ${stream}: Empty"
    fi
  done
}

# Check recent streaming job logs
check_streaming_logs() {
  print_subheader "Recent Streaming Activity"
  
  local app_id
  app_id=$(yarn application -list 2>/dev/null | grep bronze_stream | head -1 | awk '{print $1}' || true)
  
  if [[ -z "$app_id" ]]; then
    print_warning "No bronze_stream application found"
    return 0
  fi
  
  local recent_logs
  recent_logs=$(yarn logs -applicationId ${app_id} -log_files stderr -size -5000 2>/dev/null | \
     grep -E 'ProcessingTimeExecutor|Started stream' | tail -10 || true)
  
  if [[ -n "$recent_logs" ]]; then
    if echo "$recent_logs" | grep -q "falling behind"; then
      local warn_count=$(echo "$recent_logs" | grep -c "falling behind" || echo "0")
      print_warning "Job is falling behind (${warn_count} recent warnings)"
      print_info "This is normal for large initial batches"
    else
      print_success "Job is processing within trigger interval"
    fi
    
    if [[ "$DETAILED_MODE" == true ]]; then
      print_info "Recent log entries:"
      echo "$recent_logs" | while read -r line; do
        print_info "  $(echo "$line" | cut -c1-100)"
      done
    fi
  else
    print_info "No recent activity logs found"
  fi
}

# Check for errors in logs
check_for_errors() {
  print_subheader "Error Detection"
  
  local app_id
  app_id=$(yarn application -list 2>/dev/null | grep bronze_stream | head -1 | awk '{print $1}' || true)
  
  if [[ -z "$app_id" ]]; then
    print_info "No running application to check"
    return 0
  fi
  
  local errors
  errors=$(yarn logs -applicationId ${app_id} -log_files stderr -size -10000 2>/dev/null | \
     grep -E 'ERROR|Exception' | grep -v 'WARN' | tail -5 || true)
  
  if [[ -n "$errors" ]]; then
    print_error "Recent errors detected:"
    echo "$errors" | while read -r line; do
      print_info "  $(echo "$line" | cut -c1-100)"
    done
  else
    print_success "No recent errors detected"
  fi
}

# Check data processing efficiency
check_data_efficiency() {
  print_subheader "Data Processing Efficiency"
  
  # Get Kafka message counts
  local kafka_total=0
  if [[ -d ~/kafka-tools ]]; then
    local telemetry_count=$(~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
       --broker-list ${FIRST_BROKER} --topic telemetry.raw 2>/dev/null | \
       awk -F: '{sum+=$3} END {print sum}' || echo "0")
    local events_count=$(~/kafka-tools/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
       --broker-list ${FIRST_BROKER} --topic race.events 2>/dev/null | \
       awk -F: '{sum+=$3} END {print sum}' || echo "0")
    kafka_total=$((telemetry_count + events_count))
  fi
  
  # Get S3 output size
  local s3_size=$(aws s3 ls "s3://${S3_RAW_BUCKET}/bronze/" --recursive --summarize 2>/dev/null | \
    grep "Total Size:" | awk '{print $3}' || echo "0")
  
  # Get checkpoint offsets
  local checkpoint_telemetry=0
  local checkpoint_exists=false
  
  if aws s3 ls "s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze/telemetry_raw-parsed/offsets/" 2>/dev/null | grep -q "0"; then
    checkpoint_exists=true
    local offset_file=$(aws s3 cp "s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze/telemetry_raw-parsed/offsets/0" - 2>/dev/null || echo "")
    
    if [[ -n "$offset_file" ]]; then
      # Extract the JSON line (skip v1 header)
      local offset_json=$(echo "$offset_file" | tail -1)
      
      if echo "$offset_json" | grep -q "telemetry.raw"; then
        # Parse JSON to sum all partition offsets
        checkpoint_telemetry=$(echo "$offset_json" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    partitions = data.get('telemetry.raw', {})
    print(sum(partitions.values()))
except:
    print(0)
" 2>/dev/null || echo "0")
      fi
    fi
  fi
  
  # Calculate efficiency
  if [[ $kafka_total -gt 0 ]]; then
    print_info "[MAIL] Kafka messages available: $kafka_total"
    
    if [[ $checkpoint_exists == true && $checkpoint_telemetry -gt 0 ]]; then
      print_info "[CHECKPOINT] Checkpoint shows consumed: $checkpoint_telemetry"
      
      # Check if checkpoint consumed all but output is small
      local threshold=$((kafka_total - 10000))
      if [[ $checkpoint_telemetry -ge $threshold ]]; then
        local size_mb=$(awk "BEGIN {printf \"%.2f\", $s3_size/1048576}")
        
        # If output is less than 10MB but consumed nearly all messages, there's an issue
        if [[ $s3_size -lt 10485760 ]]; then
          print_warning "Checkpoint consumed ~all messages but S3 output is only ${size_mb} MB!"
          print_error "[WARN]  DATA LOSS DETECTED: Most data was read but not written"
          print_info "Possible causes:"
          print_info "  - Schema mismatch causing data to be filtered"
          print_info "  - Processing errors dropping records"
          print_info "  - Memory issues preventing writes"
          print_info ""
          print_info "[TIP] Solution: Run with --reset to clean checkpoints and reprocess"
          return 1
        else
          print_success "Data processing looks healthy (${size_mb} MB written)"
        fi
      else
        local size_mb=$(awk "BEGIN {printf \"%.2f\", $s3_size/1048576}")
        print_info "[STORAGE] S3 output size: ${size_mb} MB"
        print_info "Processing in progress..."
      fi
    else
      local size_mb=$(awk "BEGIN {printf \"%.2f\", $s3_size/1048576}")
      print_info "[STORAGE] S3 output size: ${size_mb} MB"
    fi
  else
    print_warning "No Kafka messages found to compare"
  fi
  
  return 0
}

# Reset pipeline (delete checkpoints and optionally output)
reset_pipeline() {
  print_header "PIPELINE RESET"
  
  echo
  echo -e "${YELLOW}This will clean up the pipeline and allow reprocessing from the beginning.${NC}"
  echo
  echo "Current state:"
  aws s3 ls "s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze/" 2>/dev/null | grep "PRE" | wc -l | \
    xargs -I {} echo "  - {} checkpoint directories"
  aws s3 ls "s3://${S3_RAW_BUCKET}/bronze/" 2>/dev/null | grep "PRE" | wc -l | \
    xargs -I {} echo "  - {} output directories"
  
  local app_id=$(yarn application -list 2>/dev/null | grep bronze_stream | head -1 | awk '{print $1}' || true)
  if [[ -n "$app_id" ]]; then
    echo "  - Running application: $app_id"
  fi
  
  echo
  echo -e "${CYAN}What would you like to do?${NC}"
  echo "  1) Clean checkpoints only (keeps existing output, reprocesses from Kafka beginning)"
  echo "  2) Clean checkpoints and output (complete fresh start)"
  echo "  3) Cancel (no changes)"
  echo
  read -p "Enter choice [1-3]: " choice
  
  case $choice in
    1)
      echo
      print_info "Cleaning checkpoints only..."
      
      # Kill running job
      if [[ -n "$app_id" ]]; then
        print_info "Killing application $app_id..."
        yarn application -kill "$app_id" 2>/dev/null || true
        sleep 5
      fi
      
      # Delete checkpoints
      print_info "Deleting checkpoints..."
      aws s3 rm "s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze/" --recursive
      
      print_success "Checkpoints deleted"
      echo
      print_info "Next steps:"
      print_info "  1. Restart the Spark job (it will reprocess from Kafka beginning)"
      print_info "  2. Run: ./check_pipeline_status.sh to monitor progress"
      ;;
      
    2)
      echo
      print_warning "This will delete ALL bronze layer data and checkpoints!"
      read -p "Are you sure? (yes/no): " confirm
      
      if [[ "$confirm" == "yes" ]]; then
        # Kill running job
        if [[ -n "$app_id" ]]; then
          print_info "Killing application $app_id..."
          yarn application -kill "$app_id" 2>/dev/null || true
          sleep 5
        fi
        
        # Delete checkpoints
        print_info "Deleting checkpoints..."
        aws s3 rm "s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze/" --recursive
        
        # Delete output
        print_info "Deleting bronze layer output..."
        aws s3 rm "s3://${S3_RAW_BUCKET}/bronze/" --recursive
        
        print_success "Complete cleanup done"
        echo
        print_info "Next steps:"
        print_info "  1. Restart the Spark job with updated code"
        print_info "  2. Job will process with maxOffsetsPerTrigger=100K limit"
        print_info "  3. Run: ./check_pipeline_status.sh to monitor progress"
      else
        print_info "Cancelled"
      fi
      ;;
      
    3)
      print_info "No changes made"
      ;;
      
    *)
      print_error "Invalid choice"
      ;;
  esac
  
  echo
}

# Generate summary
generate_summary() {
  print_header "SUMMARY & RECOMMENDATIONS"
  
  local health_score=0
  local max_score=4
  
  # Check if bronze_stream is running
  if yarn application -list 2>/dev/null | grep -q "bronze_stream.*RUNNING"; then
    ((health_score++))
  fi
  
  # Check if there are checkpoints
  if aws s3 ls "s3://${S3_CHECKPOINTS_BUCKET}/checkpoints/bronze/" 2>/dev/null | grep -q "PRE"; then
    ((health_score++))
  fi
  
  # Check if there's output data
  if aws s3 ls "s3://${S3_RAW_BUCKET}/bronze/" 2>/dev/null | grep -q "PRE"; then
    ((health_score++))
  fi
  
  # Check if Kafka has messages
  if [[ -d ~/kafka-tools ]] && ~/kafka-tools/bin/kafka-topics.sh --bootstrap-server ${FIRST_BROKER} --list 2>/dev/null | grep -q "telemetry.raw"; then
    ((health_score++))
  fi
  
  echo
  if [[ $health_score -eq $max_score ]]; then
    print_success "Pipeline Status: HEALTHY (${health_score}/${max_score})"
  elif [[ $health_score -ge 2 ]]; then
    print_warning "Pipeline Status: DEGRADED (${health_score}/${max_score})"
  else
    print_error "Pipeline Status: UNHEALTHY (${health_score}/${max_score})"
  fi
  
  echo
  echo -e "${CYAN}Quick Commands:${NC}"
  print_info "View all YARN apps: yarn application -list"
  print_info "View app status: yarn application -status <APP_ID>"
  print_info "Stream logs: yarn logs -applicationId <APP_ID> -log_files stderr | tail -f"
  print_info "List Kafka topics: ~/kafka-tools/bin/kafka-topics.sh --bootstrap-server ${FIRST_BROKER} --list"
  print_info "Consume messages: ~/kafka-tools/bin/kafka-console-consumer.sh --bootstrap-server ${FIRST_BROKER} --topic <TOPIC> --from-beginning --max-messages 10"
  print_info "Detailed check: ./check_pipeline_status.sh --detailed"
  print_info "Reset pipeline: ./check_pipeline_status.sh --reset"
  
  echo
}

# Main execution
main() {
  # Handle reset mode first
  if [[ "$RESET_MODE" == true ]]; then
    reset_pipeline
    exit 0
  fi
  
  print_header "F1 STREAMING PIPELINE STATUS CHECK"
  echo -e "${CYAN}Checking pipeline at: $(date)${NC}"
  echo -e "${CYAN}Kafka Bootstrap: ${KAFKA_BOOTSTRAP}${NC}"
  
  check_yarn_applications
  check_kafka_topics
  check_checkpoints
  check_s3_output
  
  # Always run data efficiency check
  local efficiency_ok=true
  check_data_efficiency || efficiency_ok=false
  
  if [[ "$DETAILED_MODE" == true ]]; then
    check_streaming_logs
    check_for_errors
  fi
  
  generate_summary
  
  # Show reset hint if efficiency check failed
  if [[ "$efficiency_ok" == false ]]; then
    echo
    echo -e "${YELLOW}===========================================${NC}"
    echo -e "${YELLOW}[WARN]  To fix data loss issue, run:${NC}"
    echo -e "${CYAN}    ./check_pipeline_status.sh --reset${NC}"
    echo -e "${YELLOW}===========================================${NC}"
  fi
  
  echo
  echo -e "${GREEN}Check completed at: $(date)${NC}"
  echo
}

# Run main function
main "$@"
