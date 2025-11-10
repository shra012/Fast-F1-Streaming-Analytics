#!/usr/bin/env bash
# Diagnostic script to test Kafka connectivity from EMR master

set -euo pipefail

BOOTSTRAP="${1:-}"

if [[ -z "$BOOTSTRAP" ]]; then
  echo "Usage: $0 <bootstrap_servers>"
  echo "Example: $0 'b-1.kafka.amazonaws.com:9092,b-2.kafka.amazonaws.com:9092'"
  exit 1
fi

echo "========================================================================"
echo "Kafka Connectivity Diagnostics"
echo "========================================================================"
echo "Bootstrap servers: $BOOTSTRAP"
echo

# Parse bootstrap servers
IFS=',' read -ra BROKERS <<< "$BOOTSTRAP"

echo "Testing ${#BROKERS[@]} broker(s)..."
echo

for b in "${BROKERS[@]}"; do
  host="${b%%:*}"
  port="${b##*:}"
  
  echo "--------------------------------------------------------------------"
  echo "Broker: $b"
  echo "--------------------------------------------------------------------"
  
  # DNS resolution
  echo -n "DNS lookup... "
  if nslookup "$host" > /dev/null 2>&1; then
    IP=$(nslookup "$host" | grep -A1 "Name:" | grep "Address:" | awk '{print $2}' | head -1)
    echo "[OK] Resolved to: $IP"
  elif host "$host" > /dev/null 2>&1; then
    IP=$(host "$host" | grep "has address" | awk '{print $4}' | head -1)
    echo "[OK] Resolved to: $IP"
  else
    echo "[FAIL] FAILED - Cannot resolve $host"
    continue
  fi
  
  # TCP connectivity
  echo -n "TCP connection to $host:$port... "
  if timeout 5 bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null; then
    echo "[OK] SUCCESS"
  elif command -v nc > /dev/null 2>&1; then
    if nc -zv -w 3 "$host" "$port" > /dev/null 2>&1; then
      echo "[OK] SUCCESS (via nc)"
    else
      echo "[FAIL] FAILED - Cannot connect to $host:$port"
      echo "  This usually means:"
      echo "  - Security group blocking port $port"
      echo "  - Network routing issue"
      echo "  - Broker is down"
    fi
  else
    echo "[FAIL] Cannot test - no nc command available"
  fi
  
  # Test with telnet if available
  if command -v telnet > /dev/null 2>&1; then
    echo -n "Telnet test... "
    if timeout 3 telnet "$host" "$port" </dev/null 2>&1 | grep -q "Connected"; then
      echo "[OK] Connected"
    else
      echo "[FAIL] Failed"
    fi
  fi
  
  echo
done

echo "========================================================================"
echo "Network Information"
echo "========================================================================"
echo "Current hostname: $(hostname)"
echo "Current IP addresses:"
ip addr show | grep "inet " | grep -v "127.0.0.1" || ifconfig | grep "inet " | grep -v "127.0.0.1"
echo

echo "========================================================================"
echo "Security Group Check"
echo "========================================================================"
echo "Instance metadata (if available):"
if command -v curl > /dev/null 2>&1; then
  echo "Instance ID:"
  curl -s http://169.254.169.254/latest/meta-data/instance-id || echo "  Not available"
  echo
  echo "Security Groups:"
  curl -s http://169.254.169.254/latest/meta-data/security-groups || echo "  Not available"
  echo
fi

echo "========================================================================"
echo "Recommendations"
echo "========================================================================"
echo "If TCP connections fail:"
echo "1. Check MSK security group allows ingress from EMR master security group"
echo "2. Verify port 9092 (PLAINTEXT) is allowed"
echo "3. Confirm EMR and MSK are in same VPC"
echo "4. Check network ACLs and route tables"
echo

