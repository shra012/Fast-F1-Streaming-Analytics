# F1 Streaming Graph Architecture Plan

This document outlines the incremental plan for delivering a Formula 1 streaming analytics platform focused on Kafka -> Spark -> Neo4j graph insights. Flink components are deferred and called out as stretch goals to integrate later.

## 1. Guiding Goals
- Deliver near-real-time driver/constructor interaction analytics backed by Neo4j graph queries.
- Combine historical telemetry (bulk ingest) with live Kafka streaming powered by Spark Structured Streaming.
- Maintain production-ready infrastructure with Terraform, least-privilege security, and automated deployment workflows.
- Provide clear validation, monitoring, and documentation so the platform can be operated by the wider team.

## 2. High-Level Architecture (Current Scope)
1. **Data Sources**
   - Historical datasets staged in S3 (raw zone).
   - Live telemetry/event feeds delivered to Kafka topics (Amazon MSK).
2. **Processing Layer**
   - Spark applications (batch and streaming) running on an EMR cluster (YARN).
   - Stateful checkpoints and intermediate artifacts stored in S3.
3. **Graph Storage & Access**
   - Neo4j Aura for driver interaction graphs, PageRank scoring, and query APIs.
4. **Orchestration & Infrastructure**
   - Terraform modules in `infra/` provisioning VPC, MSK, EMR, IAM, security groups, and S3 buckets.
   - Optional ECS producer for synthetic data feeds.
5. **Observability & Operations**
   - CloudWatch metrics/logs, alarms, and operational runbooks.

## 3. Telemetry Data Model & Streaming Plan

### 3.1 Telemetry Metrics
- **Raw stream**: `timestamp_utc`, `session_id`, `driver_id`, `car_number`, `lap_number`, `micro_sector_id`, `gps_lat`, `gps_lon`, `gps_alt`, `speed_kph`, `throttle_pct`, `brake_pressure_bar`, `steering_angle_deg`, `gear`, `engine_rpm`, `drs_state`, `ers_mode`, `battery_pct`, `fuel_mass_kg`, `tyre_compound`, `tyre_age_laps`, `tyre_surface_temp_c`, `tyre_inner_temp_c`, `tyre_pressure_bar`, `track_status_code`, `flag_state`, `weather_air_temp_c`, `weather_track_temp_c`, `weather_humidity_pct`, `weather_wind_speed_kph`, `weather_wind_direction_deg`, ingestion metadata + quality flags.
- **Race-control events**: `pit_stop_id`, `pit_in_time`, `pit_out_time`, `stop_duration_ms`, `tyres_changed_to`, `safety_car_state`, `virtual_safety_car_state`, `incident_type`, `penalty_type`, `penalty_seconds`, `penalty_lap`, `retire_reason`, `payload_json`.
- **Context tables** (slow moving dims): `session` (metadata), `driver`, `team`, `circuit` (layout & sectors), `car_config` (downforce, wing angle), `strategy_plan` (planned stop laps).
- **Derived metrics (streamed)**: lap/sector splits, `delta_vs_best_ms`, `delta_vs_leader_ms`, 3-lap rolling averages, tyre degradation rate, fuel consumption, `battery_delta_pct`, braking efficiency (avg decel vs entry), `acceleration_0_200_kph_ms2`, corner exit/straight-line speeds, blue flag exposure, pit-stop efficiency, stint consistency, driver graph centrality, team gaps, undercut predictions, confidence intervals.

### 3.2 Pipeline Stages (Simplified: Bronze → Gold)

**Architecture Decision**: Two-stage pipeline consolidating Silver + Gold + Platinum into unified Gold stage. Reduces latency, operational complexity, and infrastructure overhead.

- **Bronze (raw landing)**: 
  - **Kafka → S3 streaming ingestion**
  - Consume from `telemetry.raw` and `race.events` MSK topics
  - Parse JSON against validated schemas (`TELEMETRY_SCHEMA`, `RACE_EVENT_SCHEMA`)
  - Write both parsed (typed fields) and raw (audit trail) streams to S3 Delta tables
  - Partitioned by ingestion date: `s3://bucket/bronze/telemetry_raw_parsed/date=2025-11-09/`
  - Checkpointed for exactly-once semantics: `s3://bucket/checkpoints/bronze/`
  - Trigger interval: 30 seconds
  - **Status**: Implemented in `bronze_stream.py`, running on EMR
  
- **Gold (unified analytics, graph, and serving)**:
  - **S3 → S3 + Neo4j + Serving Layer**
  - Read Bronze Delta tables in streaming mode
  - **Cleansing** (formerly Silver): Deduplicate `(session_id, driver_id, timestamp_utc)`, filter invalid data, type conversions, unit harmonization
  - **Enrichment** (formerly Silver): Join with dimension data (drivers, teams, circuits), derive `stint_id` from tyre changes
  - **Aggregation** (formerly Gold): Window by lap to compute `fact_lap` metrics (lap times, sector splits, speeds, fuel/battery deltas), stint-level summaries
  - **Graph Computation** (formerly Gold): Detect overtakes (position swaps with gap < 1.0s), identify multi-lap battles, compute interaction strength
  - **Analytics** (formerly Platinum): Inline PageRank/centrality using GraphFrames on windowed data, driver influence scores, team battle intensity
  - **Multi-Sink Write**:
    - S3 Gold Delta: `fact_lap`, `fact_stint`, `fact_driver_session`, `fact_overtakes` tables
    - Neo4j Aura: Stream Driver/Session/Team/Lap nodes and OVERTOOK/BATTLED relationships with computed properties (centrality, influence_score)
    - Serving prep: Query-ready aggregates, cached metrics
  - Checkpointed: `s3://bucket/checkpoints/gold/`
  - Trigger interval: 60 seconds (allows lap completion and mini-batch analytics)
  - **Status**: Planned in `gold_stream.py`, design complete

**Removed Stages**:
- ~~Silver~~: Cleansing/enrichment moved to Gold
- ~~Platinum~~: Analytics and serving integrated into Gold streaming job (no separate batch or feature store)

### 3.3 Kafka Topics (Simplified)
1. `telemetry.raw` 
   - key: `session_id.driver_id.timestamp`
   - value: full telemetry payload (speed, throttle, brake, GPS, tyre temps, ERS, fuel, weather)
   - retention: 7 days delete policy
   - partitions: 3 (current), scale to 6-12 for higher throughput
   
2. `race.events` 
   - key: `session_id.event_id`
   - value: race control events (pit stops, safety car, incidents, penalties)
   - retention: 30 days with compaction
   - partitions: 3
   
**Removed Topics**:
- ~~`metrics.enriched`~~: Gold writes lap metrics directly to S3 and Neo4j, no intermediate Kafka pub/sub needed

Static dimensions (drivers, teams, circuits) seeded into Neo4j via batch CSV/Parquet import, not streamed through Kafka.

### 3.4 Canonical Relationships (Simplified for Bronze → Gold)

**S3 Delta Lake (Analytical Store)**:
```
Bronze Layer:
  telemetry_raw_parsed (streaming)
  telemetry_raw_raw (audit)
  race_events_parsed (streaming)
  race_events_raw (audit)
    ↓
Gold Layer:
  fact_lap (session_id, driver_id, lap_number → lap metrics)
  fact_stint (stint_id → tyre compound, degradation, fuel usage)
```

**Neo4j Graph Database (Interaction Network)**:
```
Nodes:
  Driver (driver_id, driver_code, name, team_id, centrality*)
  Session (session_id, season, grand_prix, circuit_id, session_code)
  Team (team_id, name, engine_supplier)
  Lap (session_id, driver_id, lap_number, lap_time_ms, position)

Relationships:
  (Driver)-[DROVE_IN]->(Session)
  (Driver)-[RACES_FOR]->(Team)
  (Driver)-[OVERTOOK {lap_number, delta_time_ms}]->(Driver)
  (Driver)-[BATTLED {lap_count, avg_gap_ms}]->(Driver)
  (Lap)-[NEXT]->(Lap)
  (Lap)-[COMPLETED_BY]->(Driver)
```

**Data Flow**:
1. Kafka (`telemetry.raw`, `race.events`) → Bronze S3 Delta (parsed + raw)
2. Bronze Delta → Gold processing:
   - Lap aggregation → `fact_lap`, `fact_stint` (S3 Delta)
   - Interaction detection → Driver/Lap nodes + OVERTOOK/BATTLED relationships (Neo4j)
3. Batch analytics: Neo4j graph → PageRank/centrality → update Driver node properties
4. Query layer: Cypher queries on Neo4j for real-time driver influence/interaction insights

## 4. Iterative Delivery Plan (Updated for Bronze → Gold)

### Delivery Team Work Items

| Person | Primary Focus | Current Status |
| --- | --- | --- |
| Shravan - Platform & Terraform | AWS/Terraform/Infrastructure | MSK + EMR cluster operational, producer deployed |
| Chaithanya - Data Ingestion | FastF1 telemetry → Kafka | Producer running, 1.1M+ messages in topics |
| Shreyas - Spark Processing | Bronze + Gold pipelines | Bronze complete, Gold in development |
| Kalyan - Graph & Observability | Neo4j + monitoring | Neo4j Aura setup, CloudWatch dashboards |

### Phase 0 - Foundation & Alignment COMPLETE
1. [x] AWS infrastructure provisioned (VPC, MSK, EMR, S3, IAM)
2. [x] Dataset catalog defined (telemetry schema, race events)
3. [x] Success metrics: sub-60s latency Bronze → Neo4j, 99.9% message processing

### Phase 1 - Infrastructure Baseline COMPLETE
1. [x] Terraform applied successfully (dev environment)
2. [x] VPC, subnets, security groups validated
3. [x] MSK cluster operational (3 brokers, 3 partitions per topic)
4. [x] EMR cluster launched (1 master + 2 core nodes, Spark 3.5.5)
5. [x] S3 buckets created: raw, artifacts, checkpoints, bronze, gold
6. [x] IAM roles configured for EMR → MSK + S3 access

### Phase 2 - Kafka Producer COMPLETE
1. [x] FastF1 Python producer implemented (`kafka/producer/producer.py`)
2. [x] Topics created: `telemetry.raw` (1,116,826 messages), `race.events` (2,343 messages)
3. [x] Producer deployed on EMR master node with IAM auth
4. [x] Replay mechanism working (timestamp-based or accelerated)
5. [x] Message validation and schema adherence confirmed

### Phase 3 - Bronze Streaming COMPLETE
1. [x] `bronze_stream.py` implemented with Spark Structured Streaming
2. [x] Kafka → S3 Delta pipeline operational
3. [x] Exactly-once checkpointing verified
4. [x] Output validated: 133.58 MB written (45.17 MB parsed telemetry, 88.02 MB raw)
5. [x] Monitoring script deployed (`check_pipeline_status.sh`)

### Phase 4 - Unified Gold Streaming & Neo4j Integration IN PROGRESS
**Goal**: S3 Bronze → Gold S3 + Neo4j graph streaming with inline analytics

**Tasks**:
1. Set up Neo4j Aura instance
   - [ ] Create Aura database (GCP/AWS region aligned)
   - [ ] Configure connection credentials
   - [ ] Add Spark cluster IP to allowlist
   - [ ] Create schema constraints (unique Driver.driver_id, Session.session_id)
   - [ ] Create indexes on frequently queried properties
   
2. Implement `gold_stream.py` - Cleansing & Aggregation (formerly Silver + Gold)
   - [ ] Read Bronze Delta tables in streaming mode
   - [ ] Add deduplication logic using watermarking (`(session_id, driver_id, timestamp_utc)`)
   - [ ] Implement data quality filters (null checks, range validation)
   - [ ] Join with dimension tables (drivers, teams, circuits enrichment)
   - [ ] Implement lap windowing and aggregation (group by session_id, driver_id, lap_number)
   - [ ] Join telemetry with race events (pit stops, penalties, safety car)
   - [ ] Derive stint_id from tyre compound changes
   - [ ] Compute lap metrics: lap_time_ms, sector splits, avg/max speed, fuel/battery deltas
   - [ ] Compute stint-level aggregates: degradation rate, consistency score
   
3. Implement overtake/battle detection (Graph Computation)
   - [ ] Track position changes lap-over-lap per session
   - [ ] Detect overtakes: position swap + gap < 1.0s criterion
   - [ ] Identify battles: multi-lap sequences with sustained proximity (3+ laps)
   - [ ] Compute interaction strength scoring
   - [ ] Tag overtake types (DRS-assisted, under braking, strategic)
   
4. Integrate inline graph analytics (formerly Platinum)
   - [ ] Add GraphFrames package: `graphframes:graphframes:0.8.3-spark3.5-s_2.12`
   - [ ] Implement windowed graph construction (last N laps per session)
   - [ ] Run incremental PageRank on mini-graphs per micro-batch
   - [ ] Compute driver influence scores (overtakes completed vs received)
   - [ ] Calculate team battle intensity metrics
   - [ ] Prepare serving layer aggregates (top overtakers, rivalry pairs)
   
5. Integrate Neo4j Spark Connector
   - [ ] Add connector JAR: `org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0`
   - [ ] Configure `spark.neo4j.url` and authentication in `~/spark.env`
   - [ ] Implement `foreachBatch` function to write nodes/relationships
   - [ ] Use MERGE for Driver/Session/Team/Lap nodes (idempotent upserts)
   - [ ] Use CREATE for OVERTOOK/BATTLED relationships
   - [ ] Add custom Cypher for node property updates (SET centrality, influence_score)
   
6. Write Gold S3 tables
   - [ ] Persist `fact_lap` Delta table with lap-level metrics
   - [ ] Persist `fact_stint` Delta table with stint aggregates
   - [ ] Persist `fact_driver_session` Delta table with per-driver summaries and influence scores
   - [ ] Persist `fact_overtakes` Delta table with detailed overtake event log
   - [ ] Set up checkpointing for Gold stage
   
7. Testing & validation
   - [ ] Verify lap aggregation accuracy vs raw telemetry
   - [ ] Validate Neo4j writes (node counts, relationship cardinality)
   - [ ] Test checkpoint recovery after intentional job failure
   - [ ] Monitor Gold processing latency (target: < 60s batch interval)
   - [ ] Validate inline PageRank computation accuracy
   - [ ] Compare influence scores with race results for sanity check

### Phase 5 - Query API & Serving Layer PLANNED
**Goal**: Expose Neo4j graph queries and cached analytics via API

**Tasks**:
1. Create Cypher query library
   - [ ] Most influential drivers (top PageRank/influence scores)
   - [ ] Rivalry pairs (highest BATTLED relationship count)
   - [ ] Overtake leaders (most OVERTOOK relationships)
   - [ ] Team interaction heatmaps
   - [ ] Session-specific leaderboards
   
2. Expose query API
   - [ ] FastAPI or GraphQL endpoint implementation
   - [ ] Authentication and rate limiting
   - [ ] Caching layer for frequently accessed queries
   - [ ] Example notebook with sample queries
   - [ ] API documentation and usage examples
   
3. Build validation dashboards
   - [ ] Compare graph metrics to actual race results
   - [ ] Track influence score correlation with finishing positions
   - [ ] Monitor data quality metrics (null rates, late arrivals)

### Phase 6 - Observability & Operations PLANNED
1. CloudWatch dashboards
   - [ ] MSK consumer lag per partition
   - [ ] EMR job metrics (batch duration, records processed)
   - [ ] Neo4j write throughput and errors
   - [ ] Gold processing latency trends
   - [ ] GraphFrames computation duration
   
2. Alerting
   - [ ] Pipeline stalled (no checkpoint updates > 5 min)
   - [ ] Data quality degradation (null rate spike)
   - [ ] Neo4j connection failures
   - [ ] Micro-batch duration exceeding trigger interval
   - [ ] Graph computation timeouts
   
3. Documentation updates
   - [ ] Deployment runbooks
   - [ ] Schema evolution procedures
   - [ ] Cost optimization recommendations
   - [ ] Performance tuning guidelines
   - [ ] Troubleshooting common issues

## 5. Stretch Goals (Flink & Advanced Enhancements)
1. **Flink Stream Processing**
   - Replace or complement Spark streaming with Flink jobs for lower latency use cases.
   - Implement Flink CEP for complex event patterns (e.g., overtake detection).
2. **Real-Time Dashboards**
   - Serve live driver influence scores via web dashboards (Grafana, custom UI) with WebSocket updates.
3. **Advanced Analytics**
   - Integrate anomaly detection models (e.g., streaming z-score) on telemetry metrics.
   - Explore LSH-based community detection pipelines for emerging driver rivalries.
4. **Multi-Region Resilience**
   - Add cross-region replication strategies for MSK and S3; investigate Neo4j Aura DR options.

## 6. Documentation & Collaboration Checklist
- Update `README.md` with links to this plan and environment-specific guides.
- Maintain data dictionaries, schema evolution notes, and API references in `docs/`.
- Schedule regular architecture reviews to reassess the roadmap and prioritize stretch goals.

By executing these phases sequentially, the team can stand up a robust streaming graph analytics platform while keeping future Flink integration and advanced analytics within reach.
