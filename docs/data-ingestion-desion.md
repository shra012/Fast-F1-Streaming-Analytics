# Data Ingestion & Streaming Design

This note captures the canonical telemetry metrics, Kafka topic strategy, and the multi-stage pipeline (Bronze → Silver → Gold → Platinum) used by the FastF1 streaming platform.

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
- Energy metrics: `battery_delta_pct`, braking efficiency, acceleration 0–200 kph, corner exit vs straight-line speeds
- Operational KPIs: blue-flag exposure time, pit-stop efficiency, stint consistency score
- Team/driver analytics: influence/centrality scores, team gap predictions, undercut probability, forecast confidence intervals

## 2. Pipeline Stages
| Stage    | Purpose | Storage | Key Actions |
|----------|---------|---------|-------------|
| **Bronze** | Raw landing zone | S3 (`SPARK_BRONZE_BASE`) | Land telemetry & events, schema validation, quality flags, WAL for replay |
| **Silver** | Cleansed & conformed | S3 (`SPARK_SILVER_BASE`) | Deduplicate `(session_id, car_number, timestamp)`, unit harmonization, join weather & race events, derive stint segments, emit SLA metrics |
| **Gold** | Analytics-ready | S3 (`SPARK_GOLD_BASE`) + Warehouse | Build `fact_lap`, `fact_stint`, compute graph edges/metrics, deliver dimensional model |
| **Platinum** | Serving / ML | Feature store + low-latency stores | Maintain driver/team feature vectors, serve dashboards/APIs, log model performance |

Shared checkpointing location: `SPARK_CHECKPOINT_BASE` (partitioned by stage).

## 3. Kafka Topics
1. `telemetry.raw`
   - Key: `session_id.driver_id.timestamp`
   - Value: raw telemetry payload + ingestion metadata
   - Retention: 7 days (consider delete or compact+delete once Bronze → Silver is settled)
2. `race.events`
   - Key: `session_id.event_id`
  - Value: race-control events (pit/safety_car/incident/weather)
   - Retention: 30 days, compact for latest status
3. `metrics.enriched`
   - Key: `session_id.driver_id.lap_number`
   - Value: lap-level aggregates, degradation, ERS/fuel status, predictive features
   - Retention: 14 days with compaction to keep freshest version

Static dimensions remain in the warehouse/feature store unless higher churn demands dedicated topics.

## 4. Canonical Schema Relationships
```
DimCircuit ──► DimSession ──► FactTelemetry ──► FactLap ──► FactStint
                     ▲             │               │
                     │             │               └─► GraphEdges / GraphMetrics
DimTeam ──► DimDriver ┘             └─► EventLog (race.events)

FactLap ──► FeatureStoreDriver (generates feature_vector, timestamps)
```
- `FactTelemetry` keys off `session_id`, `driver_id`, exact `timestamp_utc`, `lap_number`, `micro_sector`
- `FactLap` aggregates telemetry + events per driver/session/lap
- `FactStint` aggregates contiguous laps on the same compound for a driver
- `GraphEdges` capture overtakes/encounters; `GraphMetrics` stores centrality/influence
- `FeatureStoreDriver` exposes latest feature vectors to ML/serving layers

## 5. Flow Summary
1. `telemetry.raw` and `race.events` feed the Bronze landing zone.
2. Silver cleans and enriches data, emitting standardized tables and SLA metrics.
3. Gold builds analytic facts, graph structures, and (optionally) fans out to `metrics.enriched` for real-time consumers.
4. Platinum layers (feature store, APIs, dashboards) consume Gold outputs and Graph metrics for strategic decision-making.

This document should be kept in sync with `docs/architecture.md` and the latest Terraform outputs. Update it whenever schema revisions, topic strategies, or pipeline SLAs change.
