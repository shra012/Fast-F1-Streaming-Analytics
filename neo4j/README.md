# Neo4j Configuration

Neo4j Aura integration for graph storage.

## Files

- `Neo4j.txt` - Connection credentials
- `setup_neo4j_schema.sh` - Create indexes and constraints

## Setup Schema

```bash
cd neo4j
./setup_neo4j_schema.sh
```

## Configure Spark Job

```bash
export NEO4J_URI="neo4j+s://xxx.databases.neo4j.io"
export NEO4J_USERNAME="neo4j"
export NEO4J_PASSWORD="your-password"
export NEO4J_DATABASE="neo4j"
export SPARK_WRITE_TO_NEO4J="true"
```

## Neo4j Spark Connector

```
org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0
```

## Graph Schema

**Nodes:**
- Driver: driverId, driverName, avgSpeedKph, maxSpeedKph
- Session: sessionId, driverCount, lapCount
- Lap: lapId, lapNumber, avgSpeedKph
- Team: teamId, name, engineSupplier

**Relationships:**
- (Driver)-[DROVE_IN]->(Session)
- (Lap)-[COMPLETED_BY]->(Driver)
- (Driver)-[OVERTAKE]->(Driver)
- (Driver)-[BATTLE]->(Driver)
