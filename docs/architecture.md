# F1 Streaming Graph Architecture Plan

This document outlines the incremental plan for delivering a Formula 1 streaming analytics platform focused on Kafka → Spark → Neo4j graph insights. Flink components are deferred and called out as stretch goals to integrate later.

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

### 3.2 Pipeline Stages
- **Bronze (raw landing)**: ingest from FastF1 pull + timing/race-control/weather APIs into S3 (partitioned by `event_date/stream_type`), validate schemas via MSK registry, tag missing telemetry, keep WAL/replay retention.
- **Silver (cleansed & conformed)**: dedupe `(session_id, car_number, timestamp_utc)`, resample to 10 Hz metric units, enrich with driver/team/circuit metadata, join weather & race events, derive stint segments, capture SLA metrics (late arrival %, null coverage) to monitoring topics.
- **Gold (analytics-ready)**: build `fact_lap` (pace deltas, tyre state) & `fact_stint` (degradation, fuel usage), compute `driver_overtake_edge` and graph metrics, persist curated star schema in warehouse + materialized views.
- **Platinum (serving / ML)**: maintain feature store records (`driver_state_vector`, `team_gap_features`, predictive outputs), warm API caches (Redis/Pinot) for dashboards/broadcast, log model monitoring metrics (MAE, calibration, drift indicators).

### 3.3 Kafka Topics (minimal footprint)
1. `telemetry.raw` – key: `session_id.driver_id.timestamp`; value: full raw payload + quality flags; retention ~7 days (delete or compact+delete after Silver checkpoint).
2. `race.events` – key: `session_id.event_id`; value: event metadata (pit, safety car, incident, weather); retention ~30 days with compaction.
3. `metrics.enriched` – key: `session_id.driver_id.lap_number`; value: lap-level aggregates, tyre degradation, ERS/fuel status, predictive features; retention ~14 days with compaction to keep most recent version.
Static dimensions (drivers, teams, circuits) remain in the warehouse/feature store unless update cadence warrants dedicated topics.

### 3.4 Canonical Relationships
- `DimSession(session_id, season, grand_prix, session_code, start_time_utc, circuit_id)` ← `DimCircuit(circuit_id, name, layout_version, country, sector_count)`.
- `DimDriver(driver_id, driver_code, full_name, nationality, rookie_year, team_id)` ← `DimTeam(team_id, name, engine_supplier)`.
- `FactTelemetry(event_id, session_id, driver_id, timestamp_utc, lap_number, micro_sector, speed_kph, throttle_pct, …)` links to `DimSession` & `DimDriver`.
- `FactLap(session_id, driver_id, lap_number, lap_time_ms, sector1_ms, sector2_ms, sector3_ms, delta_vs_leader_ms, tyre_compound, stint_id)` generated from Silver telemetry + race events.
- `FactStint(stint_id, session_id, driver_id, tyre_compound, start_lap, end_lap, avg_pace_ms, degradation_rate, fuel_used_kg)` aggregates `FactLap`.
- `EventLog(session_id, event_id, event_type, driver_id?, lap_number?, payload_json)` originates from `race.events`.
- `GraphEdges(session_id, from_driver_id, to_driver_id, lap_number, overtake_type, delta_time_ms)` and `GraphMetrics(session_id, driver_id, centrality, influence_index)` feed graph analytics.
- `FeatureStoreDriver(driver_id, session_id, feature_vector, generated_at, valid_until)` powers Platinum-level ML/serving APIs.

Flow: `telemetry.raw` → Bronze lake → Silver cleansing → `metrics.enriched` + warehouse tables → Gold/Platinum marts & caches, with `race.events` enriching all downstream stages.

## 4. Iterative Delivery Plan

### Delivery Team Work Items

| Person | Primary Focus | Current Work Items |
| --- | --- | --- |
| Shravan – Platform & Terraform | AWS/Terraform/Producer foundation | Finalize `terraform.tfvars`, run `make preflight` & `terraform plan`, build MSK + EMR cluster infrastructure, and automate producer deployment inside the VPC |
| Chaithanya – Data Ingestion Schema Design | FastF1 telemetry into MSK (json schema) | Define topic schemas, build FastF1 schema, and coordinate Spark Bronze/Silver transformations |
| Shreyas – Spark Processing | Batch + streaming pipelines | historical S3 datasets / live kafka streaming, build Spark batch normalization, prototype Structured Streaming flow |
| Kalyan – Graph & Observability | Neo4j analytics + operations | Use Graph schema/PageRank cadence, expose query API/notebooks, configure initial CloudWatch dashboards |

### Phase 0 – Foundation & Alignment
1. Confirm AWS accounts, regions, and quotas; document required permissions.
2. Finalize dataset catalog (historical telemetry files, schema definitions, expected live topics).
3. Capture success metrics and KPIs (latency budget, PageRank accuracy targets, cost budgets).

### Phase 1 – Infrastructure Baseline (Terraform `infra/`)
1. Parameterize `terraform.tfvars` for development environment (small MSK, single NAT if acceptable).
2. Run `make preflight` to detect existing resources; review generated `*_auto.tfvars` outputs.
3. Execute `terraform init && terraform plan` to validate infrastructure changes.
4. Apply infrastructure in a dev AWS account; verify VPC, subnets, security groups, KMS, S3 buckets, and MSK cluster reachability.
5. Launch the EMR cluster; confirm IAM roles, bootstrap actions, and S3 permissions.
6. Document network endpoints (MSK bootstrap, S3 bucket names, IAM role ARNs) for downstream use.

### Phase 2 – Replaying FastF1 Telemetry to Kafka (Initial Real-Time Simulation)
1. **Topic schema**: Define Kafka topic(s) such as `telemetry.fastf1.raw` and `events.fastf1.lap`; document JSON schema and versioning in `docs/`.
2. **FastF1 API integration**:
   - Install `fastf1` Python package and set up local caching (SQLite + parquet) for session data.
   - Use session endpoints (e.g., `fastf1.get_session(year, grand_prix, session)`) to retrieve timing, laps, and telemetry frames.
   - Normalize API responses into event payloads containing timestamp, driver, lap, sector, and telemetry metrics.
3. **Producer service**:
   - Implement a Python producer that replays session data in near-real-time by sleeping based on original telemetry timestamps or a configurable acceleration factor.
   - Run inside the VPC (EC2/EMR master) with IAM authentication; manage offsets/checkpoints per session; optionally containerize later if needed.
4. **Kafka connectivity**:
   - Configure IAM-authenticated MSK connections, TLS certificates, and topic ACLs for the producer.
   - Validate end-to-end publish/subscription using sample telemetry sessions.
5. **Retention & scaling**:
   - Set topic retention, partitions, and compaction strategy aligned with downstream processing load.
   - Document operational procedures (rerunning sessions, handling backfills, rotating cache data).

### Phase 3 – Historical Batch Load
1. Upload historical datasets to the S3 raw bucket (organized by season/event).
2. Develop Spark batch jobs (PySpark or Scala) to normalize telemetry into structured parquet tables (stored in S3 artifacts bucket).
3. Implement quality checks (schema validation, deduplication using Bloom filters where appropriate).
4. Load curated batch outputs into Neo4j (via Spark connector or bulk import) to bootstrap the interaction graph.
5. Version control ETL jobs and configuration in `spark/` (or equivalent) with clear deployment runbooks.

### Phase 4 – Streaming Analytics (Spark Structured Streaming)
1. Author Structured Streaming job consuming MSK telemetry topics; perform parsing, enrichment, and windowed aggregations.
2. Apply probabilistic data structures (HyperLogLog for unique interactions, Reservoir Sampling for downsampling) inside the streaming job.
3. Persist stateful checkpoints to dedicated S3 path; verify recovery from failure scenarios.
4. Stream processed interaction events to Neo4j via transactional batches or micro-bulk writes.
5. Monitor latency and throughput; tune Spark/YARN micro-batch duration and resource limits on the EMR cluster.

### Phase 5 – Graph Analytics & APIs
1. Model driver/construction graph schema in Neo4j (nodes, relationships, properties) with supporting documentation.
2. Implement recurring PageRank/centrality updates (batch or streaming triggers) and store results as node properties.
3. Expose key graph queries via scripts or lightweight API (e.g., FastAPI) to serve downstream analysis/dashboards.
4. Define validation harness comparing PageRank outputs against historical race standings for sanity checking.
5. Provide example Cypher notebooks and sample queries for analysts.

### Phase 6 – Observability, CI/CD, and Operations
1. Configure CloudWatch dashboards for MSK lag, EMR job metrics, and Neo4j connector errors.
2. Add Terraform outputs to `README.md` and document manual post-apply steps (e.g., Neo4j IP allowlists).
3. Integrate linting/formatting (`terraform fmt`, `tflint`, Spark code style) into CI pipelines.
4. Establish deployment workflows (GitHub Actions or equivalent) covering Terraform validation and Spark job packaging.
5. Create operational runbooks for incident response, data backfills, and cost optimization levers.

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
