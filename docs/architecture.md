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
   - Spark applications (batch and streaming) running on EMR Serverless.
   - Stateful checkpoints and intermediate artifacts stored in S3.
3. **Graph Storage & Access**
   - Neo4j Aura for driver interaction graphs, PageRank scoring, and query APIs.
4. **Orchestration & Infrastructure**
   - Terraform modules in `infra/` provisioning VPC, MSK, EMR, IAM, security groups, and S3 buckets.
   - Optional ECS producer for synthetic data feeds.
5. **Observability & Operations**
   - CloudWatch metrics/logs, alarms, and operational runbooks.

## 3. Iterative Delivery Plan

### Delivery Team Work Items

| Person | Primary Focus | Current Work Items |
| --- | --- | --- |
| Shravan – Platform & Terraform | AWS/Terraform/Producer foundation | Finalize `terraform.tfvars`, run `make preflight` & `terraform plan`, setup mongo and neo4j cluster and write serverless fargate producer which reads data from fast-f1 and ingests it |
| Chaithanya – Data Ingestion Schema Design | FastF1 telemetry into MSK (json schema) | Define topic schemas, build FastF1 schema, mongo and neo4j schema |
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
5. Enable EMR Serverless application; confirm IAM roles and S3 permissions.
6. Document network endpoints (MSK bootstrap, S3 bucket names, IAM role ARNs) for downstream use.

### Phase 2 – Replaying FastF1 Telemetry to Kafka (Initial Real-Time Simulation)
1. **Topic schema**: Define Kafka topic(s) such as `telemetry.fastf1.raw` and `events.fastf1.lap`; document JSON schema and versioning in `docs/`.
2. **FastF1 API integration**:
   - Install `fastf1` Python package and set up local caching (SQLite + parquet) for session data.
   - Use session endpoints (e.g., `fastf1.get_session(year, grand_prix, session)`) to retrieve timing, laps, and telemetry frames.
   - Normalize API responses into event payloads containing timestamp, driver, lap, sector, and telemetry metrics.
3. **Producer service**:
   - Implement a Python producer that replays session data in near-real-time by sleeping based on original telemetry timestamps or a configurable acceleration factor.
   - Package as an ECS Fargate task (optional) or run locally for development; manage offsets/checkpoints per session.
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
5. Monitor latency and throughput; tune micro-batch duration and resource limits in EMR Serverless configuration.

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

## 4. Stretch Goals (Flink & Advanced Enhancements)
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

## 5. Documentation & Collaboration Checklist
- Update `README.md` with links to this plan and environment-specific guides.
- Maintain data dictionaries, schema evolution notes, and API references in `docs/`.
- Schedule regular architecture reviews to reassess the roadmap and prioritize stretch goals.

By executing these phases sequentially, the team can stand up a robust streaming graph analytics platform while keeping future Flink integration and advanced analytics within reach.
