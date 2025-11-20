#!/usr/bin/env bash
# Setup Neo4j schema: indexes, constraints, and initial structure
# This script should be run BEFORE running the Gold Spark job

set -euo pipefail

# Load Neo4j connection details
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEO4J_FILE="${SCRIPT_DIR}/Neo4j.txt"

if [ ! -f "$NEO4J_FILE" ]; then
    echo "Error: Neo4j.txt not found at $NEO4J_FILE"
    exit 1
fi

# Source Neo4j credentials
# Read and export variables directly (handling comments and empty lines)
set -a  # Automatically export all variables
while IFS= read -r line; do
    # Skip comments and empty lines
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue
    # Export if it looks like KEY=VALUE
    [[ "$line" =~ ^[A-Z_][A-Z0-9_]*= ]] && export "$line"
done < "$NEO4J_FILE"
set +a  # Turn off automatic export

# Verify required variables are set
if [ -z "${NEO4J_URI:-}" ] || [ -z "${NEO4J_USERNAME:-}" ] || [ -z "${NEO4J_PASSWORD:-}" ]; then
    echo "Error: Missing required Neo4j credentials in Neo4j.txt"
    echo "Required variables: NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD"
    echo "Found variables:"
    env | grep -E '^NEO4J_' || echo "  (none)"
    exit 1
fi

echo "=========================================="
echo "Neo4j Schema Setup"
echo "=========================================="
echo "Neo4j URI: ${NEO4J_URI}"
echo "Database: ${NEO4J_DATABASE:-neo4j}"
echo "=========================================="

# Check if cypher-shell is available
if ! command -v cypher-shell &> /dev/null; then
    echo "Warning: cypher-shell not found. Installing..."
    echo "Please install Neo4j Desktop or download cypher-shell from:"
    echo "https://neo4j.com/download-center/#cypher-shell"
    exit 1
fi

# Function to execute Cypher queries
execute_cypher() {
    local query="$1"
    local description="${2:-Executing query}"
    
    echo ""
    echo "[SETUP] $description"
    echo "$query" | cypher-shell \
        -a "$NEO4J_URI" \
        -u "$NEO4J_USERNAME" \
        -p "$NEO4J_PASSWORD" \
        --database "${NEO4J_DATABASE:-neo4j}" \
        --format plain || {
        echo "Warning: Query failed (may already exist)"
    }
}

# ==========================================
# Create Constraints (for uniqueness)
# ==========================================

echo ""
echo "=========================================="
echo "Creating Constraints"
echo "=========================================="

# Driver nodes: unique driverId
execute_cypher \
    "CREATE CONSTRAINT driver_id_unique IF NOT EXISTS FOR (d:Driver) REQUIRE d.driverId IS UNIQUE;" \
    "Driver node uniqueness constraint"

# Session nodes: unique sessionId
execute_cypher \
    "CREATE CONSTRAINT session_id_unique IF NOT EXISTS FOR (s:Session) REQUIRE s.sessionId IS UNIQUE;" \
    "Session node uniqueness constraint"

# Team nodes: unique teamId
execute_cypher \
    "CREATE CONSTRAINT team_id_unique IF NOT EXISTS FOR (t:Team) REQUIRE t.teamId IS UNIQUE;" \
    "Team node uniqueness constraint"

# Lap nodes: unique lapId
execute_cypher \
    "CREATE CONSTRAINT lap_id_unique IF NOT EXISTS FOR (l:Lap) REQUIRE l.lapId IS UNIQUE;" \
    "Lap node uniqueness constraint"

# Interaction aggregation nodes
execute_cypher \
    "CREATE CONSTRAINT interaction_id_unique IF NOT EXISTS FOR (i:Interaction) REQUIRE i.interactionId IS UNIQUE;" \
    "Interaction node uniqueness constraint"

# Sector comparison nodes
execute_cypher \
    "CREATE CONSTRAINT sector_comp_id_unique IF NOT EXISTS FOR (sc:SectorComparison) REQUIRE sc.sectorCompId IS UNIQUE;" \
    "SectorComparison node uniqueness constraint"

# Community nodes
execute_cypher \
    "CREATE CONSTRAINT community_id_unique IF NOT EXISTS FOR (c:Community) REQUIRE c.communityId IS UNIQUE;" \
    "Community node uniqueness constraint"

# ==========================================
# Create Indexes (for performance)
# ==========================================

echo ""
echo "=========================================="
echo "Creating Indexes"
echo "=========================================="

# Indexes on Driver properties
execute_cypher \
    "CREATE INDEX driver_session_id_idx IF NOT EXISTS FOR (d:Driver) ON (d.sessionId);" \
    "Driver sessionId index"

execute_cypher \
    "CREATE INDEX driver_id_idx IF NOT EXISTS FOR (d:Driver) ON (d.driverId);" \
    "Driver driverId index"

execute_cypher \
    "CREATE INDEX driver_avg_speed_idx IF NOT EXISTS FOR (d:Driver) ON (d.avgSpeedKph);" \
    "Driver avgSpeedKph index"

execute_cypher \
    "CREATE INDEX driver_max_speed_idx IF NOT EXISTS FOR (d:Driver) ON (d.maxSpeedKph);" \
    "Driver maxSpeedKph index"

# Driver analytics indexes (streaming algorithms)
execute_cypher \
    "CREATE INDEX driver_pagerank_idx IF NOT EXISTS FOR (d:Driver) ON (d.pagerankScore);" \
    "Driver PageRank score index"

execute_cypher \
    "CREATE INDEX driver_community_idx IF NOT EXISTS FOR (d:Driver) ON (d.communityId);" \
    "Driver community ID index"

execute_cypher \
    "CREATE INDEX driver_hll_interactions_idx IF NOT EXISTS FOR (d:Driver) ON (d.uniqueInteractionCount);" \
    "Driver HyperLogLog interaction count index"

# Indexes on Session properties
execute_cypher \
    "CREATE INDEX session_driver_count_idx IF NOT EXISTS FOR (s:Session) ON (s.driverCount);" \
    "Session driverCount index"

execute_cypher \
    "CREATE INDEX session_lap_count_idx IF NOT EXISTS FOR (s:Session) ON (s.lapCount);" \
    "Session lapCount index"

execute_cypher \
    "CREATE INDEX session_first_event_idx IF NOT EXISTS FOR (s:Session) ON (s.firstEventTs);" \
    "Session firstEventTs index"

execute_cypher \
    "CREATE INDEX session_last_event_idx IF NOT EXISTS FOR (s:Session) ON (s.lastEventTs);" \
    "Session lastEventTs index"

# Indexes on Lap properties
execute_cypher \
    "CREATE INDEX lap_session_id_idx IF NOT EXISTS FOR (l:Lap) ON (l.sessionId);" \
    "Lap sessionId index"

execute_cypher \
    "CREATE INDEX lap_driver_id_idx IF NOT EXISTS FOR (l:Lap) ON (l.driverId);" \
    "Lap driverId index"

execute_cypher \
    "CREATE INDEX lap_lap_number_idx IF NOT EXISTS FOR (l:Lap) ON (l.lapNumber);" \
    "Lap lapNumber index"

execute_cypher \
    "CREATE INDEX lap_avg_speed_idx IF NOT EXISTS FOR (l:Lap) ON (l.avgSpeedKph);" \
    "Lap avgSpeedKph index"

execute_cypher \
    "CREATE INDEX lap_max_speed_idx IF NOT EXISTS FOR (l:Lap) ON (l.maxSpeedKph);" \
    "Lap maxSpeedKph index"

execute_cypher \
    "CREATE INDEX lap_last_event_idx IF NOT EXISTS FOR (l:Lap) ON (l.lastEventTs);" \
    "Lap lastEventTs index"

# Indexes on Interaction nodes
execute_cypher \
    "CREATE INDEX interaction_session_idx IF NOT EXISTS FOR (i:Interaction) ON (i.sessionId);" \
    "Interaction sessionId index"

execute_cypher \
    "CREATE INDEX interaction_strength_idx IF NOT EXISTS FOR (i:Interaction) ON (i.avgStrength);" \
    "Interaction avgStrength index"

execute_cypher \
    "CREATE INDEX interaction_hll_count_idx IF NOT EXISTS FOR (i:Interaction) ON (i.uniqueInteractionCount);" \
    "Interaction HyperLogLog count index"

# Indexes on SectorComparison nodes
execute_cypher \
    "CREATE INDEX sector_comp_driver_idx IF NOT EXISTS FOR (sc:SectorComparison) ON (sc.driverId);" \
    "SectorComparison driverId index"

execute_cypher \
    "CREATE INDEX sector_comp_session_idx IF NOT EXISTS FOR (sc:SectorComparison) ON (sc.sessionId);" \
    "SectorComparison sessionId index"

execute_cypher \
    "CREATE INDEX sector_comp_delta_idx IF NOT EXISTS FOR (sc:SectorComparison) ON (sc.avgDelta);" \
    "SectorComparison avgDelta index"

# Indexes on Community nodes
execute_cypher \
    "CREATE INDEX community_member_count_idx IF NOT EXISTS FOR (c:Community) ON (c.memberCount);" \
    "Community memberCount index"

# Indexes on Relationships
execute_cypher \
    "CREATE INDEX drove_in_session_idx IF NOT EXISTS FOR ()-[r:DROVE_IN]-() ON (r.sessionId);" \
    "DROVE_IN relationship sessionId index"

execute_cypher \
    "CREATE INDEX drove_in_driver_idx IF NOT EXISTS FOR ()-[r:DROVE_IN]-() ON (r.driverId);" \
    "DROVE_IN relationship driverId index"

execute_cypher \
    "CREATE INDEX drove_in_first_lap_idx IF NOT EXISTS FOR ()-[r:DROVE_IN]-() ON (r.firstLap);" \
    "DROVE_IN relationship firstLap index"

execute_cypher \
    "CREATE INDEX drove_in_last_lap_idx IF NOT EXISTS FOR ()-[r:DROVE_IN]-() ON (r.lastLap);" \
    "DROVE_IN relationship lastLap index"

execute_cypher \
    "CREATE INDEX completed_by_session_idx IF NOT EXISTS FOR ()-[r:COMPLETED_BY]-() ON (r.sessionId);" \
    "COMPLETED_BY relationship sessionId index"

execute_cypher \
    "CREATE INDEX completed_by_lap_id_idx IF NOT EXISTS FOR ()-[r:COMPLETED_BY]-() ON (r.lapId);" \
    "COMPLETED_BY relationship lapId index"

execute_cypher \
    "CREATE INDEX completed_by_lap_number_idx IF NOT EXISTS FOR ()-[r:COMPLETED_BY]-() ON (r.lapNumber);" \
    "COMPLETED_BY relationship lapNumber index"

execute_cypher \
    "CREATE INDEX completed_by_driver_idx IF NOT EXISTS FOR ()-[r:COMPLETED_BY]-() ON (r.driverId);" \
    "COMPLETED_BY relationship driverId index"

# Interaction relationships (OVERTAKE and BATTLE from gold_stream)
execute_cypher \
    "CREATE INDEX drove_in_last_lap_idx IF NOT EXISTS FOR ()-[r:DROVE_IN]-() ON (r.lastLap);" \
    "DROVE_IN relationship lastLap index"

# Interaction relationships (OVERTAKE and BATTLE)
execute_cypher \
    "CREATE INDEX overtake_session_idx IF NOT EXISTS FOR ()-[r:OVERTAKE]-() ON (r.sessionId);" \
    "OVERTAKE relationship sessionId index"

execute_cypher \
    "CREATE INDEX overtake_lap_idx IF NOT EXISTS FOR ()-[r:OVERTAKE]-() ON (r.lapNumber);" \
    "OVERTAKE relationship lapNumber index"

execute_cypher \
    "CREATE INDEX overtake_event_idx IF NOT EXISTS FOR ()-[r:OVERTAKE]-() ON (r.eventId);" \
    "OVERTAKE relationship eventId index"

execute_cypher \
    "CREATE INDEX battle_session_idx IF NOT EXISTS FOR ()-[r:BATTLE]-() ON (r.sessionId);" \
    "BATTLE relationship sessionId index"

execute_cypher \
    "CREATE INDEX battle_lap_idx IF NOT EXISTS FOR ()-[r:BATTLE]-() ON (r.lapNumber);" \
    "BATTLE relationship lapNumber index"

execute_cypher \
    "CREATE INDEX battle_event_idx IF NOT EXISTS FOR ()-[r:BATTLE]-() ON (r.eventId);" \
    "BATTLE relationship eventId index"

# Advanced streaming algorithm relationships
execute_cypher \
    "CREATE INDEX influenced_by_weight_idx IF NOT EXISTS FOR ()-[r:INFLUENCED_BY]-() ON (r.influenceWeight);" \
    "INFLUENCED_BY relationship PageRank weight index"

execute_cypher \
    "CREATE INDEX influenced_by_session_idx IF NOT EXISTS FOR ()-[r:INFLUENCED_BY]-() ON (r.sessionId);" \
    "INFLUENCED_BY relationship sessionId index"

execute_cypher \
    "CREATE INDEX same_community_idx IF NOT EXISTS FOR ()-[r:SAME_COMMUNITY]-() ON (r.communityId);" \
    "SAME_COMMUNITY relationship LSH communityId index"

execute_cypher \
    "CREATE INDEX faster_than_delta_idx IF NOT EXISTS FOR ()-[r:FASTER_THAN]-() ON (r.timeDeltaMs);" \
    "FASTER_THAN relationship time delta index"

execute_cypher \
    "CREATE INDEX faster_than_session_idx IF NOT EXISTS FOR ()-[r:FASTER_THAN]-() ON (r.sessionId);" \
    "FASTER_THAN relationship sessionId index"

# ==========================================

# ==========================================
# Verify Schema
# ==========================================

echo ""
echo "=========================================="
echo "Verifying Schema"
echo "=========================================="

execute_cypher \
    "SHOW CONSTRAINTS;" \
    "Listing all constraints"

execute_cypher \
    "SHOW INDEXES;" \
    "Listing all indexes"

echo ""
echo "=========================================="
echo "Neo4j Schema Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Verify constraints and indexes were created"
echo "2. Monitor data ingestion in Neo4j Browser"
echo ""
echo "Connect to Neo4j Browser:"
echo "  ${NEO4J_URI}"
echo ""

