#!/usr/bin/env bash
# drop_all.sh
# Safely delete all nodes & relationships in Neo4j while preserving constraints/indexes
# Usage: ./drop_all.sh [--yes] [--dry-run]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEO4J_FILE="${SCRIPT_DIR}/Neo4j.txt"

DRY_RUN=false
AUTO_YES=false

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;; 
    -n) DRY_RUN=true ;; 
    --yes) AUTO_YES=true ;; 
    -y) AUTO_YES=true ;; 
    --help|-h)
      echo "Usage: $0 [--yes] [--dry-run]"
      echo "  --dry-run  : Show the queries that will be executed, do not run them"
      echo "  --yes      : Skip confirmation prompt and run the deletion"
      exit 0 ;;
    *) ;;
  esac
done

if [ ! -f "$NEO4J_FILE" ]; then
  echo "Error: Neo4j.txt not found at $NEO4J_FILE"
  exit 1
fi

# Source variables (robust to comments/blank lines)
set -a
while IFS= read -r line; do
  [[ "$line" =~ ^[[:space:]]*# ]] && continue
  [[ -z "${line// }" ]] && continue
  [[ "$line" =~ ^[A-Z_][A-Z0-9_]*= ]] && export "$line"
done < "$NEO4J_FILE"
set +a

if [ -z "${NEO4J_URI:-}" ] || [ -z "${NEO4J_USERNAME:-}" ] || [ -z "${NEO4J_PASSWORD:-}" ]; then
  echo "Error: Missing required Neo4j credentials in Neo4j.txt"
  echo "Required variables: NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD"
  exit 1
fi

if ! command -v cypher-shell &> /dev/null; then
  echo "Error: cypher-shell not found in PATH. Please install it first."
  exit 1
fi

DELETE_QUERY="MATCH (n) DETACH DELETE n;"
COUNT_QUERY="MATCH (n) RETURN count(n) AS remaining;"
SHOW_CONSTRAINTS_QUERY="SHOW CONSTRAINTS;"
SHOW_INDEXES_QUERY="SHOW INDEXES;"

echo "Neo4j URI: ${NEO4J_URI}"
echo "Database: ${NEO4J_DATABASE:-neo4j}"

echo "This script will DELETE ALL NODES AND RELATIONSHIPS in the target Neo4j database but will KEEP all constraints and indexes."

if [ "$DRY_RUN" = true ]; then
  echo "\n-- DRY RUN --\nThe following Cypher queries would be executed:\n"
  echo "${DELETE_QUERY}\n"
  echo "After deletion the script will run:\n${COUNT_QUERY}\n${SHOW_CONSTRAINTS_QUERY}\n${SHOW_INDEXES_QUERY}\n"
  exit 0
fi

if [ "$AUTO_YES" != true ]; then
  read -r -p "Type YES (all caps) to proceed with deleting all data: " CONFIRM
  if [ "$CONFIRM" != "YES" ]; then
    echo "Aborting. No data was deleted."
    exit 0
  fi
fi

echo "Running deletion..."

# Execute deletion
set +e
# Wrap calls so a failure in one prints a helpful message
echo "$DELETE_QUERY" | cypher-shell -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" --database "${NEO4J_DATABASE:-neo4j}" --format plain
RC=$?
if [ $RC -ne 0 ]; then
  echo "Warning: Deletion query failed with exit code $RC"
  echo "You may need to run the deletion from a Neo4j instance with more heap or use APOC's periodic commit if the dataset is very large."
fi

# Check remaining nodes
echo "Checking remaining node count..."
echo "$COUNT_QUERY" | cypher-shell -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" --database "${NEO4J_DATABASE:-neo4j}" --format plain || true

# Re-show constraints & indexes to confirm schema still present
echo "Listing constraints (schema preserved):"
echo "$SHOW_CONSTRAINTS_QUERY" | cypher-shell -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" --database "${NEO4J_DATABASE:-neo4j}" --format plain || true

echo "Listing indexes (schema preserved):"
echo "$SHOW_INDEXES_QUERY" | cypher-shell -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" --database "${NEO4J_DATABASE:-neo4j}" --format plain || true

set -e

echo "Done. All data deletion attempted; constraints/indexes should remain intact."

exit 0
