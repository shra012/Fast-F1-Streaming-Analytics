# Data Ingestion & Streaming Design

This note captures the canonical telemetry metrics, Kafka topic strategy, and the multi-stage pipeline (Bronze -> Silver -> Gold -> Platinum) used by the FastF1 streaming platform.

## 1. Telemetry Metrics
### 1.1 Raw Telemetry Payload
- `timestamp_utc`
- `session_id`, `driver_id`, `car_number`
- `lap_number`, `micro_sector_id`
- GPS: `gps_lat`, `gps_lon`, `gps_alt`
- Vehicle dynamics: `speed_kph`, `throttle_pct`, `brake_pressure_bar`, `steering_angle_deg`, `gear`, `engine_rpm`, `drs_state`, `ers_mode`, `battery_pct`, `fuel_mass_kg`
- Tyres: `tyre_compound`, `tyre_age_laps`, `tyre_surface_temp_c`, `tyre_inner_temp_c`, `tyre_pressure_bar`
- Race control: `track_status_code`, `flag_state`
- Weather: `weather_air_temp_c`, `weather_track_temp_c`, `weather_humidity_pct`, `weather_wind_speed_kph`, `weather_wind_direction_deg`
- Metadata: ingestion timestamp, source identifier, quality flags

### 1.2 Race-Control Event Payload
- `event_id`, `session_id`, `driver_id?`, `lap_number?`
- `pit_stop_id`, `pit_in_time`, `pit_out_time`, `stop_duration_ms`, `tyres_changed_to`
- `safety_car_state`, `virtual_safety_car_state`, `incident_type`
- `penalty_type`, `penalty_seconds`, `penalty_lap`, `retire_reason`
- `payload_json` for provider-specific fields

### 1.3 Context / Dimension Tables
- `DimSession` (season, grand prix, session code, start time, circuit)
- `DimDriver` (driver code, name, nationality, rookie year, team)
- `DimTeam` (constructor, engine supplier)
- `DimCircuit` (layout version, country, sector definitions)
- `CarConfig` (downforce trim, wing angles, power unit mode)
- `StrategyPlan` (planned stop laps, compound strategy)

### 1.4 Derived / Streaming Metrics
- Lap and sector splits, `delta_vs_best_ms`, `delta_vs_leader_ms`
- Rolling averages (3-lap pace, tyre degradation rate, fuel consumption per lap)
- Energy metrics: `battery_delta_pct`, braking efficiency, acceleration 0-200 kph, corner exit vs straight-line speeds
- Operational KPIs: blue-flag exposure time, pit-stop efficiency, stint consistency score
- Team/driver analytics: influence/centrality scores, team gap predictions, undercut probability, forecast confidence intervals

## 2. Pipeline Stages (Simplified: Bronze → Gold)

**Architecture Decision**: Two-stage pipeline eliminating Silver and Platinum layers. Gold combines all transformations, aggregations, graph computation, and serving layer preparation.

| Stage    | Purpose | Storage | Key Actions |
|----------|---------|---------|-------------|
| **Bronze** | Raw landing zone | S3 (`SPARK_BRONZE_BASE`) | **Kafka → S3**: Ingest from `telemetry.raw` and `race.events` topics, parse JSON, validate schemas, add ingestion metadata, write both parsed and raw streams to Delta/Parquet, maintain checkpoints for exactly-once semantics |
| **Gold** | Analytics, graph, and serving | S3 (`SPARK_GOLD_BASE`) + Neo4j Aura | **S3 → S3/Neo4j/Serving**: Read Bronze Delta tables, clean & deduplicate, aggregate lap metrics, compute driver interactions, derive graph edges/metrics, calculate PageRank/centrality, write facts to S3, stream nodes/relationships to Neo4j, expose query endpoints for dashboards/APIs |

**Consolidated Gold Responsibilities** (formerly Silver + Gold + Platinum):
- **Cleansing**: Deduplicate `(session_id, driver_id, timestamp)`, filter invalid records, type conversions
- **Enrichment**: Join with dimensions (drivers, teams, circuits), derive stint boundaries
- **Aggregation**: Lap-level metrics, stint summaries, rolling averages, degradation rates
- **Graph Computation**: Overtake detection, battle identification, interaction networks
- **Analytics**: PageRank, centrality scores, influence metrics computed inline or via micro-batch
- **Serving**: Query-ready graph in Neo4j, dimensional facts in S3 Delta for BI tools

Shared checkpointing location: `SPARK_CHECKPOINT_BASE` (partitioned by stage: `/bronze` and `/gold`).

## 3. Kafka Topics (Simplified)
1. `telemetry.raw`
   - Key: `session_id.driver_id.timestamp`
   - Value: raw telemetry payload (speed, throttle, GPS, tyre temps, etc.)
   - Retention: 7 days (delete after Bronze checkpoint established)
   - Consumed by: Bronze streaming job
   
2. `race.events`
   - Key: `session_id.event_id`
   - Value: race-control events (pit stops, safety car, incidents, penalties)
   - Retention: 30 days with compaction for latest status
   - Consumed by: Bronze streaming job

**Removed Topics** (no longer needed with simplified pipeline):
- ~~`metrics.enriched`~~ - Gold writes aggregated metrics directly to S3 and Neo4j instead of intermediate Kafka topic

Static dimensions (drivers, teams, circuits) loaded into Neo4j as seed data via batch script, not through Kafka streaming.

## 4. Data Models

### 4.1 Bronze Layer (S3 Delta Tables)
- `telemetry_raw_parsed`: Parsed telemetry with all fields typed (FloatType, IntegerType, etc.)
- `telemetry_raw_raw`: Raw Kafka messages for replay/audit
- `race_events_parsed`: Parsed race events with typed fields
- `race_events_raw`: Raw event messages for audit trail

All Bronze tables include Kafka metadata: `kafka_key`, `topic`, `partition`, `offset`, `kafka_timestamp`, `ingested_at`

### 4.2 Gold Layer (S3 Delta + Neo4j)

**S3 Gold Tables** (aggregated facts for analytics):
- `fact_lap`: Aggregated lap-level metrics
  - Keys: `session_id`, `driver_id`, `lap_number`
  - Metrics: `lap_time_ms`, `sector1_ms`, `sector2_ms`, `sector3_ms`, `avg_speed_kph`, `max_speed_kph`, `tyre_compound`, `fuel_used_kg`, `battery_delta_pct`, `stint_id`
  
- `fact_stint`: Stint-level aggregates (consecutive laps on same tyre compound)
  - Keys: `stint_id`, `session_id`, `driver_id`
  - Metrics: `tyre_compound`, `start_lap`, `end_lap`, `avg_pace_ms`, `degradation_rate_ms_per_lap`, `total_fuel_used_kg`

**Neo4j Graph Schema** (driver interactions):

Nodes:
- `Driver`: Properties: `driver_id`, `driver_code`, `full_name`, `nationality`, `team_id`
- `Session`: Properties: `session_id`, `season`, `grand_prix`, `circuit_id`, `session_code`, `start_time_utc`
- `Team`: Properties: `team_id`, `name`, `engine_supplier`
- `Lap`: Properties: `session_id`, `driver_id`, `lap_number`, `lap_time_ms`, `position`, `tyre_compound`

Relationships:
- `(Driver)-[DROVE_IN]->(Session)`: Links driver to session participation
- `(Driver)-[RACES_FOR]->(Team)`: Current team assignment
- `(Driver)-[OVERTOOK {lap_number, delta_time_ms, location}]->(Driver)`: Overtake events
- `(Driver)-[BATTLED {lap_count, avg_gap_ms}]->(Driver)`: Multi-lap battles (gap < 1.0s)
- `(Lap)-[NEXT]->(Lap)`: Sequential laps for same driver in session
- `(Lap)-[COMPLETED_BY]->(Driver)`: Links lap performance to driver

Graph Metrics (computed as node properties):
- `Driver.centrality`: PageRank score (influence in interaction network)
- `Driver.overtake_success_rate`: % of attempted overtakes completed
- `Driver.avg_battle_duration`: Average laps spent in close racing

## 5. Processing Flow (Bronze → Gold)

### 5.1 Bronze Stage: Kafka → S3
**Job**: `bronze_stream.py` (Spark Structured Streaming on EMR)

**Input**: 
- Kafka topics: `telemetry.raw`, `race.events`
- Bootstrap servers: MSK cluster (IAM auth)

**Processing**:
1. Read from Kafka topics with `maxOffsetsPerTrigger: 100000`
2. Parse JSON payloads against `TELEMETRY_SCHEMA` and `RACE_EVENT_SCHEMA`
3. Add ingestion metadata: `ingested_at`, `kafka_timestamp`, `partition`, `offset`
4. Write to S3 in two streams per topic:
   - `-parsed`: Structured data with typed fields
   - `-raw`: Raw Kafka messages for audit/replay

**Output**:
- S3 Delta tables: `s3://bucket/bronze/telemetry_raw_parsed/`, `telemetry_raw_raw/`, `race_events_parsed/`, `race_events_raw/`
- Checkpoints: `s3://bucket/checkpoints/bronze/`
- Trigger interval: 30 seconds

**Status**: IMPLEMENTED & RUNNING (133.58 MB processed from 1.1M messages)

---

### 5.2 Gold Stage: S3 → S3/Neo4j/Serving (Unified Analytics & Serving)
**Job**: `gold_stream.py` (Spark Structured Streaming on EMR) - **TO BE IMPLEMENTED**

**Input**:
- Bronze Delta tables: `telemetry_raw_parsed`, `race_events_parsed`
- Dimension seed data: drivers, teams, circuits (S3 parquet or CSV)

**Processing** (consolidated from Silver + Gold + Platinum):

1. **Cleansing & Enrichment** (formerly Silver):
   - Deduplicate on `(session_id, driver_id, timestamp_utc)` using watermarking
   - Filter invalid records (null session_id, negative speeds, impossible GPS coordinates)
   - Convert timestamp strings to TimestampType, harmonize units
   - Join with dimension tables to enrich driver/team/circuit metadata
   
2. **Lap Aggregation & Stint Detection**:
   - Window telemetry by `session_id`, `driver_id`, `lap_number`
   - Compute: `lap_time_ms`, sector splits (sector1/2/3_ms), `avg_speed_kph`, `max_speed_kph`
   - Calculate deltas: `fuel_used_kg`, `battery_delta_pct`, `delta_vs_leader_ms`, `delta_vs_best_ms`
   - Join with race events to capture pit stops, penalties, safety car periods
   - Derive `stint_id` based on tyre compound changes and pit stop events
   - Compute stint-level metrics: degradation rate, avg pace, consistency score
   
3. **Graph Computation & Interaction Detection**:
   - Track position changes lap-over-lap per session
   - Detect overtakes: position swap where trailing driver gap was < 1.0s on previous lap
   - Identify battles: consecutive laps (3+) where gap remains < 1.0s between same drivers
   - Compute interaction strength (lap count × proximity factor)
   - Tag overtake types (DRS-assisted, under braking, strategic undercut)
   
4. **Graph Analytics** (formerly Platinum, now inline):
   - Maintain mini-graph per session window (last N laps or current session)
   - Compute incremental centrality metrics using GraphFrames on windowed data
   - Calculate driver influence score based on overtakes completed vs received
   - Derive team battle intensity (sum of inter-team driver interactions)
   
5. **Multi-Sink Output**:
   - **S3 Gold Delta Tables**: Write `fact_lap`, `fact_stint`, `fact_driver_session_summary`
   - **Neo4j Graph**: Stream nodes (Driver, Session, Team, Lap) and relationships (OVERTOOK, BATTLED, DROVE_IN, RACES_FOR) via foreachBatch using Neo4j Spark Connector
   - **Serving Layer Prep**: Compute and cache query-ready aggregates (top overtakers per session, rivalry pairs, team standings)

**Output**:
- S3 Gold tables: 
  - `s3://bucket/gold/fact_lap/` - lap-level metrics
  - `s3://bucket/gold/fact_stint/` - stint aggregates
  - `s3://bucket/gold/fact_driver_session/` - per-driver session summaries with influence scores
  - `s3://bucket/gold/fact_overtakes/` - detailed overtake event log
- Neo4j writes: 
  - Nodes: Driver (with centrality properties), Session, Team, Lap
  - Relationships: OVERTOOK (with delta_time_ms, lap_number), BATTLED (with lap_count, avg_gap_ms)
  - Graph properties updated: Driver.centrality, Driver.influence_score, Driver.overtake_success_rate
- Checkpoints: `s3://bucket/checkpoints/gold/`
- Trigger interval: 60 seconds (allows lap completion and mini-batch analytics)

**Neo4j Connection**:
- Neo4j Spark Connector: `org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0`
- GraphFrames (for inline analytics): `graphframes:graphframes:0.8.3-spark3.5-s_2.12`
- Connection via `spark.neo4j.url`, `spark.neo4j.authentication` (stored in `~/spark.env`)
- Write mode: MERGE for nodes (upsert on unique constraints), CREATE for relationships
- Node property updates via foreachBatch custom Cypher (SET centrality, influence_score)

**Status**: PLANNED - Design complete, implementation pending

---

## 6. Implementation Priorities

### Phase 1: COMPLETE
- [x] Bronze streaming job operational
- [x] Kafka topics consuming messages
- [x] 133.58 MB written to S3 (45.17 MB parsed telemetry, 88.02 MB raw)
- [x] Monitoring script deployed

### Phase 2: IN PROGRESS (Unified Gold Development)
**Core Streaming Pipeline**:
- [ ] Implement `gold_stream.py` with lap aggregation logic
- [ ] Add deduplication and data quality filters (watermarking, null checks)
- [ ] Implement dimension joins (drivers, teams, circuits enrichment)
- [ ] Add stint detection logic (track tyre compound changes)

**Graph Computation**:
- [ ] Design and test overtake/battle detection algorithms
- [ ] Implement position tracking and lap-over-lap comparison
- [ ] Add interaction strength scoring logic
- [ ] Tag overtake types (DRS, braking, strategy)

**Analytics Integration** (formerly Platinum):
- [ ] Integrate GraphFrames for windowed centrality computation
- [ ] Implement incremental influence score calculation
- [ ] Add team battle intensity metrics
- [ ] Compute driver/session summary aggregates

**Neo4j Integration**:
- [ ] Set up Neo4j Aura instance and configure connection
- [ ] Create Neo4j schema constraints (unique driver_id, session_id)
- [ ] Create indexes on frequently queried properties
- [ ] Implement Neo4j Spark Connector integration in `foreachBatch`
- [ ] Add custom Cypher for node property updates (centrality, influence)
- [ ] Load dimension seed data (drivers, teams, circuits) into Neo4j

**Serving Layer**:
- [ ] Create Cypher query library (top overtakers, rivalry pairs, team standings)
- [ ] Implement query API endpoints (FastAPI or GraphQL)
- [ ] Add caching for frequently accessed graph queries
- [ ] Build validation dashboard comparing graph metrics to race results

### Phase 3: PLANNED (Operations & Optimization)
- [ ] Performance tuning (trigger intervals, micro-batch sizing)
- [ ] Cost optimization (S3 lifecycle policies, Neo4j instance sizing)
- [ ] Advanced analytics (anomaly detection, predictive undercut modeling)
- [ ] Multi-session historical analysis and trend detection

---

This document should be kept in sync with `docs/architecture.md` and the latest Terraform outputs. Update whenever schema revisions, topic strategies, or pipeline implementations change.
