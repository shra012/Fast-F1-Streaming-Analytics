#!/usr/bin/env bash
# Test Neo4j analytics queries in one shot
# This script runs all common analytics queries to verify data and relationships

set -euo pipefail

# Load Neo4j connection details
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEO4J_FILE="${SCRIPT_DIR}/Neo4j.txt"

if [ ! -f "$NEO4J_FILE" ]; then
    echo "Error: Neo4j.txt not found at $NEO4J_FILE"
    exit 1
fi

# Source Neo4j credentials
set -a
while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue
    [[ "$line" =~ ^[A-Z_][A-Z0-9_]*= ]] && export "$line"
done < "$NEO4J_FILE"
set +a

# Verify required variables are set
if [ -z "${NEO4J_URI:-}" ] || [ -z "${NEO4J_USERNAME:-}" ] || [ -z "${NEO4J_PASSWORD:-}" ]; then
    echo "Error: Missing required Neo4j credentials in Neo4j.txt"
    exit 1
fi

# Check if cypher-shell is available
if ! command -v cypher-shell &> /dev/null; then
    echo "Error: cypher-shell not found. Please install Neo4j tools."
    exit 1
fi

# Function to execute Cypher queries
execute_query() {
    local query="$1"
    local description="${2:-Running query}"
    
    echo ""
    echo "=========================================="
    echo "$description"
    echo "=========================================="
    echo "$query" | cypher-shell \
        -a "$NEO4J_URI" \
        -u "$NEO4J_USERNAME" \
        -p "$NEO4J_PASSWORD" \
        --database "${NEO4J_DATABASE:-neo4j}" \
        --format plain 2>&1 | grep -v "WARNING:\|You are using"
    echo ""
}

echo "=========================================="
echo "Neo4j Analytics Query Tests"
echo "=========================================="
echo "Neo4j URI: ${NEO4J_URI}"
echo "Database: ${NEO4J_DATABASE:-neo4j}"
echo "=========================================="

# Query 1: Check all node types and counts
execute_query \
    "MATCH (n) RETURN labels(n) AS NodeType, count(n) AS Count ORDER BY Count DESC;" \
    "1. Node Types and Counts"

# Query 2: Check all relationship types
execute_query \
    "MATCH ()-[r]->() RETURN type(r) AS RelType, count(r) AS Count ORDER BY Count DESC;" \
    "2. Relationship Types and Counts"

# Query 3: Driver statistics
execute_query \
    "MATCH (d:Driver) RETURN d.driverId, d.avgSpeedKph, d.maxSpeedKph, d.maxLapSeen ORDER BY d.maxSpeedKph DESC LIMIT 10;" \
    "3. Top 10 Drivers by Max Speed"

# Query 4: Session overview
execute_query \
    "MATCH (s:Session) RETURN s.sessionId, s.driverCount, s.lapCount, s.firstEventTs, s.lastEventTs ORDER BY s.lastEventTs DESC LIMIT 5;" \
    "4. Recent Sessions"

# Query 5: Lap statistics per driver
execute_query \
    "MATCH (l:Lap)-[:COMPLETED_BY]->(d:Driver) RETURN d.driverId, count(l) AS lap_count, avg(l.avgSpeedKph) AS avg_lap_speed, max(l.maxSpeedKph) AS fastest_speed ORDER BY lap_count DESC LIMIT 10;" \
    "5. Lap Statistics per Driver"

# Query 6: Driver-Session relationships
execute_query \
    "MATCH (d:Driver)-[r:DROVE_IN]->(s:Session) RETURN d.driverId, s.sessionId, r.firstLap, r.lastLap, r.firstEventTs ORDER BY r.lastEventTs DESC LIMIT 10;" \
    "6. Driver-Session Relationships"

# Query 7: Fastest laps
execute_query \
    "MATCH (l:Lap) WHERE l.avgSpeedKph IS NOT NULL RETURN l.lapId, l.driverId, l.lapNumber, l.avgSpeedKph, l.maxSpeedKph ORDER BY l.maxSpeedKph DESC LIMIT 10;" \
    "7. Top 10 Fastest Laps"

# Query 8: Overtake interactions (if they exist)
execute_query \
    "MATCH (attacker:Driver)-[r:OVERTAKE]->(defender:Driver) RETURN attacker.driverId AS attacker, defender.driverId AS defender, r.sessionId, r.lapNumber, r.eventTs ORDER BY r.eventTs DESC LIMIT 10;" \
    "8. Recent Overtake Interactions" || echo "No OVERTAKE relationships found (expected if gold stream not yet processing events)"

# Query 9: Battle interactions (if they exist)
execute_query \
    "MATCH (attacker:Driver)-[r:BATTLE]->(defender:Driver) RETURN attacker.driverId AS attacker, defender.driverId AS defender, r.sessionId, r.lapNumber, r.lapCount ORDER BY r.lapCount DESC LIMIT 10;" \
    "9. Top Battle Interactions" || echo "No BATTLE relationships found (expected if gold stream not yet processing events)"

# Query 10: Interaction summary
execute_query \
    "MATCH (d1:Driver)-[r:OVERTAKE|BATTLE]->(d2:Driver) RETURN d1.driverId AS driver1, type(r) AS interaction_type, d2.driverId AS driver2, count(r) AS occurrences ORDER BY occurrences DESC LIMIT 20;" \
    "10. Interaction Summary (OVERTAKE + BATTLE)" || echo "No interaction relationships found yet"

# Query 11: Graph statistics
execute_query \
    "MATCH (n) WITH labels(n) AS nodeType, count(n) AS nodeCount MATCH ()-[r]->() WITH nodeType, nodeCount, type(r) AS relType, count(r) AS relCount RETURN nodeType, nodeCount, relType, relCount ORDER BY nodeCount DESC, relCount DESC;" \
    "11. Comprehensive Graph Statistics"

# Query 12: Sample data from each node type
execute_query \
    "MATCH (d:Driver) RETURN d LIMIT 1;" \
    "12a. Sample Driver Node"

execute_query \
    "MATCH (s:Session) RETURN s LIMIT 1;" \
    "12b. Sample Session Node"

execute_query \
    "MATCH (l:Lap) RETURN l LIMIT 1;" \
    "12c. Sample Lap Node"

execute_query \
    "MATCH (t:Team) RETURN t LIMIT 1;" \
    "12d. Sample Team Node" || echo "No Team nodes found yet"

echo ""
echo "=========================================="
echo "Analytics Query Tests Complete!"
echo "=========================================="
echo ""
echo "Summary:"
echo "- Use queries 1-2 to verify data is loaded"
echo "- Queries 3-7 test basic node and relationship data"
echo "- Queries 8-10 test interaction relationships (OVERTAKE/BATTLE)"
echo "- Query 11 provides comprehensive graph statistics"
echo "- Query 12 shows sample data from each node type"
echo ""
echo "If OVERTAKE/BATTLE queries return empty, restart gold_stream job"
echo "to apply the latest code with interaction relationship writes."
echo ""
