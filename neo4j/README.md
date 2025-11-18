# Neo4j Configuration and Setup

This directory contains configuration files and setup scripts for Neo4j integration.

## Files

- `Neo4j.txt` - Neo4j Aura connection details (credentials)
- `setup_neo4j_schema.sh` - Script to create Neo4j indexes and constraints

## Prerequisites

1. **Neo4j Aura Instance**: Ensure your Neo4j Aura instance is running
2. **Neo4j Credentials**: Update `Neo4j.txt` with your Neo4j connection details
3. **Kafka Topics**: The Gold job publishes to Kafka topics for monitoring:
   - `graph.neo4j.driver_nodes`
   - `graph.neo4j.session_nodes`
   - `graph.neo4j.team_nodes`
   - `graph.neo4j.lap_nodes`
   - `graph.neo4j.driver_session_edges`
   - `graph.neo4j.lap_completed_edges`
   - `graph.neo4j.overtake_edges`
   - `graph.neo4j.battle_edges`

## Setup Steps

### 1. Create Neo4j Schema (Indexes and Constraints)

**Before running the Gold Spark job**, run the schema setup script:

```bash
cd neo4j
./setup_neo4j_schema.sh
```

This script will:
- Create uniqueness constraints on node IDs (driverId, sessionId, teamId, lapId)
- Create indexes on frequently queried properties
- Verify the schema was created correctly

**Requirements:**
- `cypher-shell` must be installed and in your PATH
- Neo4j Aura instance must be accessible

**Install cypher-shell:**
```bash
# macOS
brew install neo4j

# Or download from: https://neo4j.com/download-center/#cypher-shell
```

### 2. Configure Spark Gold Job for Neo4j Writes

The Gold Spark job can write directly to Neo4j using the Neo4j Spark Connector. Configure Neo4j connection in your Spark job:

```bash
# Set environment variables or pass as arguments
export NEO4J_URI="neo4j+s://xxx.databases.neo4j.io"
export NEO4J_USERNAME="neo4j"
export NEO4J_PASSWORD="your-password"
export NEO4J_DATABASE="neo4j"
export SPARK_WRITE_TO_NEO4J="true"
```

Or pass as Spark job arguments:
```bash
--neo4j-uri "neo4j+s://xxx.databases.neo4j.io" \
--neo4j-username "neo4j" \
--neo4j-password "your-password" \
--neo4j-database "neo4j" \
--write-to-neo4j
```

The Gold job will write to both Kafka topics (for monitoring) and Neo4j (for graph storage).

## Neo4j Spark Connector

The Gold job uses the Neo4j Spark Connector package:
```
org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0
```

Add this to your `spark-submit` command:
```bash
--packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0
```

## Monitoring

### Neo4j Browser

Connect to your Neo4j instance and run queries:

```cypher
// Count nodes
MATCH (n) RETURN labels(n), count(n)

// View drivers
MATCH (d:Driver) RETURN d LIMIT 10

// View relationships
MATCH ()-[r]->() RETURN type(r), count(r)
```

### Check Kafka Topics (for monitoring)

The Gold job also publishes to Kafka topics for monitoring:

```bash
~/kafka-tools/bin/kafka-console-consumer.sh \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic graph.neo4j.driver_nodes \
  --max-messages 1
```

## Troubleshooting

### No Data in Neo4j

1. Verify Kafka topics have messages (for monitoring):
   ```bash
   ~/kafka-tools/bin/kafka-console-consumer.sh \
     --bootstrap-server ${KAFKA_BOOTSTRAP} \
     --topic graph.neo4j.driver_nodes \
     --max-messages 1
   ```

2. Check Spark job logs for Neo4j write errors
3. Verify Neo4j connection credentials in `Neo4j.txt`
4. Ensure `--write-to-neo4j` flag is set when running Gold job

### Schema Errors

If you see constraint violations:
1. Drop existing constraints/indexes:
   ```cypher
   DROP CONSTRAINT driver_id_unique IF EXISTS;
   ```
2. Re-run `setup_neo4j_schema.sh`

## Security Notes

- **Neo4j.txt contains credentials** - Do NOT commit to git (already in `.gitignore`)
- Neo4j connection uses TLS encryption (neo4j+s://)
- Credentials should be passed securely to Spark jobs (environment variables or secrets)

## Next Steps

After setting up Neo4j:
1. Run `setup_neo4j_schema.sh` to create indexes and constraints
2. Start the Gold stream job with `--write-to-neo4j` flag
3. Monitor data ingestion in Neo4j Browser
4. Create graph queries for analytics
