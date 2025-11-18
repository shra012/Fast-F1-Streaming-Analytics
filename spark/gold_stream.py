"""Gold stage Structured Streaming job.

Reads Bronze Delta tables, derives graph-friendly node/edge payloads, and
publishes them to Kafka topics consumed by the Neo4j sink connector.
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas import INTERACTION_EVENT_PAYLOAD_SCHEMA
from spark.utils import add_common_kafka_options, create_spark_session, option_dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold stage job producing Neo4j node/edge topics.")
    add_common_kafka_options(parser)

    parser.add_argument(
        "--bronze-base",
        default=os.getenv("SPARK_BRONZE_BASE", os.getenv("BRONZE_OUTPUT_PATH", "s3://fastf1/bronze")),
        help="Base path containing bronze Delta tables produced by bronze_stream.py.",
    )
    parser.add_argument(
        "--bronze-format",
        default=os.getenv("SPARK_BRONZE_FORMAT", "delta"),
        choices=["delta", "parquet"],
        help="Storage format for bronze tables.",
    )
    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=int(os.getenv("SPARK_MAX_FILES_PER_TRIGGER", "10")),
        help="Maximum number of files to process per trigger (for Delta tables). Higher = faster processing of existing data.",
    )
    parser.add_argument(
        "--starting-version",
        type=int,
        default=None,
        help="Delta table starting version (for reprocessing from a specific version). If not set, uses checkpoint or starts from beginning.",
    )
    parser.add_argument("--telemetry-table", default="telemetry_raw-parsed")
    parser.add_argument("--events-table", default="race_events-parsed")

    default_checkpoint = os.getenv("SPARK_GOLD_CHECKPOINT_BASE")
    if not default_checkpoint:
        checkpoint_root = os.getenv("SPARK_CHECKPOINT_BASE")
        if checkpoint_root:
            default_checkpoint = checkpoint_root.rstrip("/") + "/gold"
        else:
            default_checkpoint = "s3://fastf1/checkpoints/gold"

    parser.add_argument(
        "--checkpoint-base",
        default=default_checkpoint,
        help="Checkpoint root for the gold job. Each sink gets a child directory.",
    )

    # Neo4j topics
    parser.add_argument("--driver-topic", default="graph.neo4j.driver_nodes")
    parser.add_argument("--session-topic", default="graph.neo4j.session_nodes")
    parser.add_argument("--team-topic", default="graph.neo4j.team_nodes")
    parser.add_argument("--lap-topic", default="graph.neo4j.lap_nodes")
    parser.add_argument("--driver-session-topic", default="graph.neo4j.driver_session_edges")
    parser.add_argument("--lap-edge-topic", default="graph.neo4j.lap_completed_edges")
    parser.add_argument("--overtake-topic", default="graph.neo4j.overtake_edges")
    parser.add_argument("--battle-topic", default="graph.neo4j.battle_edges")

    parser.add_argument(
        "--kafka-sink-option",
        action="append",
        default=[],
        help="Extra Kafka sink options (key=value). Applied to every Kafka write.",
    )
    parser.add_argument(
        "--teams-lookup-path",
        default=os.getenv("SPARK_TEAMS_DIM_PATH"),
        help="Optional path to a teams dimension dataset (Parquet or Delta).",
    )
    
    # Neo4j direct write options
    parser.add_argument(
        "--neo4j-uri",
        default=os.getenv("NEO4J_URI"),
        help="Neo4j connection URI (e.g., neo4j+s://xxx.databases.neo4j.io). If not provided, only writes to Kafka topics.",
    )
    parser.add_argument(
        "--neo4j-username",
        default=os.getenv("NEO4J_USERNAME", "neo4j"),
        help="Neo4j username.",
    )
    parser.add_argument(
        "--neo4j-password",
        default=os.getenv("NEO4J_PASSWORD"),
        help="Neo4j password.",
    )
    parser.add_argument(
        "--neo4j-database",
        default=os.getenv("NEO4J_DATABASE", "neo4j"),
        help="Neo4j database name.",
    )
    parser.add_argument(
        "--write-to-neo4j",
        action="store_true",
        default=os.getenv("SPARK_WRITE_TO_NEO4J", "false").lower() == "true",
        help="Enable direct writes to Neo4j in addition to Kafka topics.",
    )

    return parser.parse_args()


def build_kafka_options(args: argparse.Namespace) -> Dict[str, str]:
    options = {"kafka.bootstrap.servers": args.bootstrap_servers}
    if args.kafka_sink_option:
        options.update(option_dict(args.kafka_sink_option))
    return options


def write_to_neo4j(df: DataFrame, spark, cypher_query: str, args: argparse.Namespace) -> None:
    """Write DataFrame to Neo4j using Neo4j Spark Connector."""
    if not args.write_to_neo4j or not args.neo4j_uri:
        return
    
    if df is None:
        return
    
    try:
        row_count = df.count()
        if row_count == 0:
            return
        
        # Configure Neo4j connection
        spark.conf.set("spark.neo4j.url", args.neo4j_uri)
        spark.conf.set("spark.neo4j.authentication.type", "basic")
        spark.conf.set("spark.neo4j.authentication.basic.username", args.neo4j_username)
        spark.conf.set("spark.neo4j.authentication.basic.password", args.neo4j_password)
        spark.conf.set("spark.neo4j.database", args.neo4j_database)
        
        # Write using Neo4j connector
        # The connector expects the DataFrame columns to match the Cypher query parameters
        df.write.format("org.neo4j.spark.DataSource").mode("Overwrite").option("query", cypher_query).save()
        print(f"✓ Wrote {row_count} rows to Neo4j")
    except Exception as e:
        print(f"✗ Error writing to Neo4j: {str(e)}")
        import traceback
        traceback.print_exc()
        # Don't raise - allow Kafka writes to continue


def read_bronze_table(spark, args: argparse.Namespace, table: str) -> DataFrame:
    path = os.path.join(args.bronze_base.rstrip("/"), table)
    stream = spark.readStream.format(args.bronze_format)
    
    # For Delta tables, configure how existing data is processed
    if args.bronze_format == "delta":
        # maxFilesPerTrigger processes existing files in batches
        # Higher values = faster processing of historical data, but larger batches
        stream = stream.option("maxFilesPerTrigger", str(args.max_files_per_trigger))
        
        # If starting_version is specified, start from that version (for reprocessing)
        # Otherwise, checkpoint will determine starting point, or start from beginning
        if args.starting_version is not None:
            print(f"Starting Delta stream from version {args.starting_version} for {table}")
            stream = stream.option("startingVersion", str(args.starting_version))
        else:
            print(f"Processing Delta table {table} with maxFilesPerTrigger={args.max_files_per_trigger}")
            print(f"  - If checkpoint exists: resumes from last processed version")
            print(f"  - If no checkpoint: processes all existing data in batches")
    
    return stream.load(path)


def publish_to_kafka(df: Optional[DataFrame], topic: str, kafka_options: Dict[str, str]) -> None:
    if df is None:
        print(f"Skipping Kafka publish for {topic}: DataFrame is None")
        return
    try:
        # Ensure required columns exist
        if "key" not in df.columns or "value" not in df.columns:
            print(f"Warning: DataFrame for topic {topic} missing 'key' or 'value' columns. Columns: {df.columns}")
            return
        
        row_count = df.count()
        if row_count == 0:
            print(f"Skipping Kafka publish for {topic}: DataFrame is empty")
            return
        
        print(f"Publishing {row_count} rows to Kafka topic: {topic}")
        (
            df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
            .write.format("kafka")
            .options(**kafka_options)
            .option("topic", topic)
            .save()
        )
        print(f"Successfully published {row_count} rows to Kafka topic: {topic}")
    except Exception as e:
        print(f"Error publishing to Kafka topic {topic}: {str(e)}")
        import traceback
        traceback.print_exc()
        # Don't re-raise - allow other topics to be processed


def build_driver_nodes(df: DataFrame) -> DataFrame:
    aggregated = (
        df.filter(F.col("driver_id").isNotNull())
        .groupBy("driver_id")
        .agg(
            F.first("session_id", ignorenulls=True).alias("first_session_id"),
            F.last("session_id", ignorenulls=True).alias("latest_session_id"),
            F.max("lap_number").alias("max_lap_seen"),
            F.avg("speed_kph").alias("avg_speed_kph"),
            F.max("speed_kph").alias("max_speed_kph"),
            F.avg("battery_pct").alias("avg_battery_pct"),
            F.avg("fuel_mass_kg").alias("avg_fuel_mass_kg"),
        )
    )
    payload = aggregated.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.lit("Driver").alias("node_type"),
                F.col("driver_id"),
                F.col("latest_session_id").alias("session_id"),
                F.struct(
                    F.col("first_session_id"),
                    F.col("latest_session_id"),
                    F.col("max_lap_seen"),
                    F.col("avg_speed_kph"),
                    F.col("max_speed_kph"),
                    F.col("avg_battery_pct"),
                    F.col("avg_fuel_mass_kg"),
                ).alias("properties"),
            )
        ),
    )
    return payload.select(F.col("driver_id").alias("key"), "value")


def build_session_nodes(df: DataFrame) -> DataFrame:
    aggregated = (
        df.filter(F.col("session_id").isNotNull())
        .groupBy("session_id")
        .agg(
            F.countDistinct("driver_id").alias("driver_count"),
            F.countDistinct("lap_number").alias("lap_count"),
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
        )
    )
    payload = aggregated.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.lit("Session").alias("node_type"),
                F.col("session_id"),
                F.struct(
                    F.col("driver_count"),
                    F.col("lap_count"),
                    F.col("first_event_ts"),
                    F.col("last_event_ts"),
                ).alias("properties"),
            )
        ),
    )
    return payload.select(F.col("session_id").alias("key"), "value")


def build_team_nodes(teams_lookup_path: Optional[str], spark) -> Optional[DataFrame]:
    if not teams_lookup_path:
        return None
    fmt = "delta" if teams_lookup_path.endswith(".delta") else "parquet"
    team_df = spark.read.format(fmt).load(teams_lookup_path)
    payload = team_df.select("team_id", "name", "engine_supplier").withColumn(
        "value",
        F.to_json(
            F.struct(
                F.lit("Team").alias("node_type"),
                F.col("team_id"),
                F.struct(
                    F.col("name"),
                    F.col("engine_supplier"),
                ).alias("properties"),
            )
        ),
    )
    return payload.select(F.col("team_id").alias("key"), "value")


def build_lap_stats(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("lap_number").isNotNull()) & (F.col("lap_number") >= 0))
        .groupBy("session_id", "driver_id", "lap_number")
        .agg(
            F.max("event_ts").alias("event_ts"),
            F.avg("speed_kph").alias("avg_speed_kph"),
            F.max("speed_kph").alias("max_speed_kph"),
            F.min("speed_kph").alias("min_speed_kph"),
            F.avg("battery_pct").alias("avg_battery_pct"),
            F.avg("fuel_mass_kg").alias("avg_fuel_mass_kg"),
        )
        .withColumn("lap_id", F.concat_ws(":", "session_id", "driver_id", "lap_number"))
    )


def build_lap_nodes(lap_stats: DataFrame) -> DataFrame:
    payload = lap_stats.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.lit("Lap").alias("node_type"),
                F.col("lap_id").alias("lap_id"),
                F.struct(
                    F.col("session_id"),
                    F.col("driver_id"),
                    F.col("lap_number"),
                    F.col("avg_speed_kph"),
                    F.col("max_speed_kph"),
                    F.col("min_speed_kph"),
                    F.col("avg_battery_pct"),
                    F.col("avg_fuel_mass_kg"),
                    F.col("event_ts").alias("last_event_ts"),
                ).alias("properties"),
            )
        ),
    )
    return payload.select(F.col("lap_id").alias("key"), "value")


def build_driver_session_edges(df: DataFrame) -> DataFrame:
    aggregated = (
        df.filter((F.col("session_id").isNotNull()) & (F.col("driver_id").isNotNull()))
        .groupBy("session_id", "driver_id")
        .agg(
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
            F.min("lap_number").alias("first_lap"),
            F.max("lap_number").alias("last_lap"),
        )
        .withColumn("edge_id", F.concat_ws(":", "session_id", "driver_id"))
    )
    payload = aggregated.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.lit("DROVE_IN").alias("relationship_type"),
                F.struct(F.lit("Driver").alias("label"), F.col("driver_id").alias("id")).alias("start"),
                F.struct(F.lit("Session").alias("label"), F.col("session_id").alias("id")).alias("end"),
                F.struct(
                    F.col("first_event_ts"),
                    F.col("last_event_ts"),
                    F.col("first_lap"),
                    F.col("last_lap"),
                ).alias("properties"),
            )
        ),
    )
    return payload.select(F.col("edge_id").alias("key"), "value")


def build_lap_edges(lap_stats: DataFrame) -> DataFrame:
    payload = lap_stats.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.lit("COMPLETED_BY").alias("relationship_type"),
                F.struct(F.lit("Lap").alias("label"), F.col("lap_id").alias("id")).alias("start"),
                F.struct(F.lit("Driver").alias("label"), F.col("driver_id").alias("id")).alias("end"),
                F.struct(
                    F.col("session_id"),
                    F.col("lap_number"),
                    F.col("avg_speed_kph"),
                    F.col("event_ts").alias("last_event_ts"),
                ).alias("properties"),
            )
        ),
    )
    return payload.select(F.col("lap_id").alias("key"), "value")


def build_interaction_edges(df: DataFrame, relationship_type: str) -> Optional[DataFrame]:
    target_type = relationship_type.lower()
    filtered = df.filter(F.lower(F.col("event_type")) == target_type)

    # Handle null or missing payload gracefully
    expanded = filtered.withColumn(
        "interaction",
        F.when(
            F.col("payload").isNotNull() & (F.col("payload") != ""),
            F.from_json("payload", INTERACTION_EVENT_PAYLOAD_SCHEMA)
        ).otherwise(F.struct(*[F.lit(None).cast(field.dataType).alias(field.name) for field in INTERACTION_EVENT_PAYLOAD_SCHEMA.fields]))
    )
    attacker_col = F.coalesce(
        F.col("interaction.attacker_id"),
        F.col("interaction.driver_a_id"),
        F.col("driver_id"),
    )
    defender_col = F.coalesce(
        F.col("interaction.defender_id"),
        F.col("interaction.driver_b_id"),
    )
    enriched = (
        expanded.withColumn("attacker_id", attacker_col)
        .withColumn("defender_id", defender_col)
        .withColumn("lap_number", F.coalesce(F.col("interaction.lap_number"), F.col("lap_number")))
        .withColumn("lap_count", F.col("interaction.lap_count"))
        .withColumn("avg_gap_ms", F.col("interaction.avg_gap_ms"))
        .withColumn("delta_time_ms", F.col("interaction.delta_time_ms"))
        .filter(F.col("attacker_id").isNotNull() & F.col("defender_id").isNotNull())
    )
    payload = enriched.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.lit(relationship_type.upper()).alias("relationship_type"),
                F.struct(F.lit("Driver").alias("label"), F.col("attacker_id").alias("id")).alias("start"),
                F.struct(F.lit("Driver").alias("label"), F.col("defender_id").alias("id")).alias("end"),
                F.struct(
                    F.col("session_id"),
                    F.col("event_id"),
                    F.col("lap_number"),
                    F.col("lap_count"),
                    F.col("avg_gap_ms"),
                    F.col("delta_time_ms"),
                    F.col("event_ts_utc"),
                ).alias("properties"),
            )
        ),
    )
    return payload.select(
        F.concat_ws(":", F.col("session_id"), F.col("attacker_id"), F.col("defender_id"), F.col("lap_number")).alias("key"),
        "value",
    )


def process_telemetry_batch(
    df: DataFrame,
    batch_id: int,
    args: argparse.Namespace,
    kafka_options: Dict[str, str],
    spark,
    team_nodes_df: Optional[DataFrame],
) -> None:
    print(f"[Batch {batch_id}] Starting telemetry processing...")
    row_count = df.count()
    print(f"[Batch {batch_id}] Input DataFrame has {row_count} rows")
    if row_count == 0:
        print(f"[Batch {batch_id}] Empty batch, skipping")
        return
    
    try:
        print(f"[Batch {batch_id}] Filtering and cleaning data...")
        cleaned = (
            df.filter((F.col("session_id").isNotNull()) & (F.col("driver_id").isNotNull()))
            .withColumn("event_ts", F.to_timestamp("timestamp_utc"))
            .cache()
        )
        cleaned_count = cleaned.count()
        print(f"[Batch {batch_id}] Cleaned DataFrame has {cleaned_count} rows")

        print(f"[Batch {batch_id}] Building driver nodes...")
        driver_nodes = build_driver_nodes(cleaned)
        publish_to_kafka(driver_nodes, args.driver_topic, kafka_options)
        if args.write_to_neo4j:
            # Aggregate driver stats for Neo4j
            driver_nodes_for_neo4j = cleaned.groupBy("driver_id", "session_id").agg(
                F.min("session_id").alias("first_session_id"),
                F.max("session_id").alias("latest_session_id"),
                F.max("lap_number").alias("max_lap_seen"),
                F.avg("speed_kph").alias("avg_speed_kph"),
                F.max("speed_kph").alias("max_speed_kph"),
                F.avg("battery_pct").alias("avg_battery_pct"),
                F.avg("fuel_mass_kg").alias("avg_fuel_mass_kg"),
            ).select(
                F.col("driver_id").alias("driverId"),
                F.col("session_id").alias("sessionId"),
                F.col("first_session_id").alias("firstSessionId"),
                F.col("latest_session_id").alias("latestSessionId"),
                F.col("max_lap_seen").alias("maxLapSeen"),
                F.col("avg_speed_kph").alias("avgSpeedKph"),
                F.col("max_speed_kph").alias("maxSpeedKph"),
                F.col("avg_battery_pct").alias("avgBatteryPct"),
                F.col("avg_fuel_mass_kg").alias("avgFuelMassKg"),
            ).distinct()
            write_to_neo4j(
                driver_nodes_for_neo4j,
                spark,
                "MERGE (d:Driver {driverId: event.driverId}) SET d += event",
                args,
            )
        
        print(f"[Batch {batch_id}] Building session nodes...")
        session_nodes = build_session_nodes(cleaned)
        publish_to_kafka(session_nodes, args.session_topic, kafka_options)
        if args.write_to_neo4j:
            session_nodes_for_neo4j = cleaned.groupBy("session_id").agg(
                F.countDistinct("driver_id").alias("driverCount"),
                F.countDistinct("lap_number").alias("lapCount"),
                F.min("event_ts").alias("firstEventTs"),
                F.max("event_ts").alias("lastEventTs"),
            ).select(
                F.col("session_id").alias("sessionId"),
                F.col("driverCount"),
                F.col("lapCount"),
                F.col("firstEventTs"),
                F.col("lastEventTs"),
            )
            write_to_neo4j(
                session_nodes_for_neo4j,
                spark,
                "MERGE (s:Session {sessionId: event.sessionId}) SET s += event",
                args,
            )

        if team_nodes_df is not None:
            print(f"[Batch {batch_id}] Publishing team nodes...")
            publish_to_kafka(team_nodes_df, args.team_topic, kafka_options)
            if args.write_to_neo4j:
                team_nodes_for_neo4j = spark.read.format("delta" if args.teams_lookup_path.endswith(".delta") else "parquet").load(args.teams_lookup_path).select(
                    F.col("team_id").alias("teamId"),
                    F.col("name"),
                    F.col("engine_supplier").alias("engineSupplier"),
                )
                write_to_neo4j(
                    team_nodes_for_neo4j,
                    spark,
                    "MERGE (t:Team {teamId: event.teamId}) SET t += event",
                    args,
                )

        print(f"[Batch {batch_id}] Building lap statistics...")
        lap_stats = build_lap_stats(cleaned)
        lap_stats_count = lap_stats.count()
        print(f"[Batch {batch_id}] Lap stats DataFrame has {lap_stats_count} rows")
        
        print(f"[Batch {batch_id}] Publishing lap nodes...")
        lap_nodes = build_lap_nodes(lap_stats)
        publish_to_kafka(lap_nodes, args.lap_topic, kafka_options)
        if args.write_to_neo4j:
            lap_nodes_for_neo4j = lap_stats.select(
                F.col("lap_id").alias("lapId"),
                F.col("session_id").alias("sessionId"),
                F.col("driver_id").alias("driverId"),
                F.col("lap_number").alias("lapNumber"),
                F.col("avg_speed_kph").alias("avgSpeedKph"),
                F.col("max_speed_kph").alias("maxSpeedKph"),
                F.col("min_speed_kph").alias("minSpeedKph"),
                F.col("avg_battery_pct").alias("avgBatteryPct"),
                F.col("avg_fuel_mass_kg").alias("avgFuelMassKg"),
                F.col("event_ts").alias("lastEventTs"),
            )
            write_to_neo4j(
                lap_nodes_for_neo4j,
                spark,
                "MERGE (l:Lap {lapId: event.lapId}) SET l += event",
                args,
            )
        
        print(f"[Batch {batch_id}] Publishing lap edges...")
        lap_edges = build_lap_edges(lap_stats)
        publish_to_kafka(lap_edges, args.lap_edge_topic, kafka_options)
        if args.write_to_neo4j:
            lap_edges_for_neo4j = lap_stats.select(
                F.col("lap_id").alias("lapId"),
                F.col("driver_id").alias("driverId"),
                F.col("session_id").alias("sessionId"),
                F.col("lap_number").alias("lapNumber"),
                F.col("avg_speed_kph").alias("avgSpeedKph"),
                F.col("event_ts").alias("lastEventTs"),
            )
            write_to_neo4j(
                lap_edges_for_neo4j,
                spark,
                "MATCH (l:Lap {lapId: event.lapId}) MATCH (d:Driver {driverId: event.driverId}) MERGE (l)-[r:COMPLETED_BY]->(d) SET r += event",
                args,
            )
        
        print(f"[Batch {batch_id}] Publishing driver-session edges...")
        driver_session_edges = build_driver_session_edges(cleaned)
        publish_to_kafka(driver_session_edges, args.driver_session_topic, kafka_options)
        if args.write_to_neo4j:
            driver_session_edges_for_neo4j = cleaned.groupBy("session_id", "driver_id").agg(
                F.min("event_ts").alias("firstEventTs"),
                F.max("event_ts").alias("lastEventTs"),
                F.min("lap_number").alias("firstLap"),
                F.max("lap_number").alias("lastLap"),
            ).select(
                F.col("driver_id").alias("driverId"),
                F.col("session_id").alias("sessionId"),
                F.col("firstEventTs"),
                F.col("lastEventTs"),
                F.col("firstLap"),
                F.col("lastLap"),
            )
            write_to_neo4j(
                driver_session_edges_for_neo4j,
                spark,
                "MATCH (d:Driver {driverId: event.driverId}) MATCH (s:Session {sessionId: event.sessionId}) MERGE (d)-[r:DROVE_IN]->(s) SET r += event",
                args,
            )

        cleaned.unpersist()
        print(f"[Batch {batch_id}] Telemetry batch processing completed successfully")
    except Exception as e:
        print(f"[Batch {batch_id}] ✗ Error processing telemetry batch: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def process_event_batch(
    df: DataFrame,
    batch_id: int,
    args: argparse.Namespace,
    kafka_options: Dict[str, str],
    spark,
) -> None:
    print(f"[Batch {batch_id}] Starting event processing...")
    row_count = df.count()
    print(f"[Batch {batch_id}] Input DataFrame has {row_count} rows")
    if row_count == 0:
        print(f"[Batch {batch_id}] Empty batch, skipping")
        return
    
    try:
        print(f"[Batch {batch_id}] Cleaning event data...")
        cleaned = df.withColumn("event_ts", F.to_timestamp("event_ts_utc")).cache()
        cleaned_count = cleaned.count()
        print(f"[Batch {batch_id}] Cleaned DataFrame has {cleaned_count} rows")

        print(f"[Batch {batch_id}] Building overtake edges...")
        overtake_edges = build_interaction_edges(cleaned, "overtake")
        publish_to_kafka(overtake_edges, args.overtake_topic, kafka_options)
        if args.write_to_neo4j and overtake_edges is not None:
            # Parse JSON payload to extract overtake data
            overtake_edges_for_neo4j = cleaned.filter(F.col("event_type") == "overtake").select(
                F.col("session_id").alias("sessionId"),
                F.col("attacker_id").alias("attackerId"),
                F.col("defender_id").alias("defenderId"),
                F.col("lap_number").alias("lapNumber"),
                F.col("delta_time_ms").alias("deltaTimeMs"),
                F.col("event_ts").alias("eventTs"),
            )
            write_to_neo4j(
                overtake_edges_for_neo4j,
                spark,
                "MATCH (a:Driver {driverId: event.attackerId}) MATCH (d:Driver {driverId: event.defenderId}) MERGE (a)-[r:OVERTOOK]->(d) SET r += event",
                args,
            )
        
        print(f"[Batch {batch_id}] Building battle edges...")
        battle_edges = build_interaction_edges(cleaned, "battle")
        publish_to_kafka(battle_edges, args.battle_topic, kafka_options)
        if args.write_to_neo4j and battle_edges is not None:
            battle_edges_for_neo4j = cleaned.filter(F.col("event_type") == "battle").select(
                F.col("session_id").alias("sessionId"),
                F.col("attacker_id").alias("attackerId"),
                F.col("defender_id").alias("defenderId"),
                F.col("lap_number").alias("lapNumber"),
                F.col("lap_count").alias("lapCount"),
                F.col("avg_gap_ms").alias("avgGapMs"),
                F.col("event_ts").alias("eventTs"),
            )
            write_to_neo4j(
                battle_edges_for_neo4j,
                spark,
                "MATCH (a:Driver {driverId: event.attackerId}) MATCH (d:Driver {driverId: event.defenderId}) MERGE (a)-[r:BATTLED]->(d) SET r += event",
                args,
            )

        cleaned.unpersist()
        print(f"[Batch {batch_id}] Event batch processing completed successfully")
    except Exception as e:
        print(f"[Batch {batch_id}] ✗ Error processing event batch: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def main() -> None:
    args = parse_args()
    kafka_options = build_kafka_options(args)

    extra_conf: Dict[str, str] = {}
    if args.bronze_format == "delta":
        extra_conf["spark.sql.shuffle.partitions"] = os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")

    spark = create_spark_session("fastf1-gold-stream", enable_delta=args.bronze_format == "delta", extra_conf=extra_conf)

    # Load team nodes once (static lookup) if path is provided
    team_nodes_df: Optional[DataFrame] = None
    if args.teams_lookup_path:
        try:
            team_nodes_df = build_team_nodes(args.teams_lookup_path, spark)
            print(f"Loaded team nodes from {args.teams_lookup_path}")
        except Exception as e:
            print(f"Warning: Could not load team nodes from {args.teams_lookup_path}: {str(e)}")
            team_nodes_df = None

    # Verify bronze tables exist before starting streams
    telemetry_path = os.path.join(args.bronze_base.rstrip("/"), args.telemetry_table)
    events_path = os.path.join(args.bronze_base.rstrip("/"), args.events_table)
    
    print(f"==========================================")
    print(f"Gold Stream Job Starting")
    print(f"==========================================")
    print(f"Telemetry path: {telemetry_path}")
    print(f"Events path: {events_path}")
    print(f"Bronze format: {args.bronze_format}")
    print(f"Checkpoint base: {args.checkpoint_base}")
    print(f"Kafka bootstrap: {args.bootstrap_servers}")
    if args.write_to_neo4j:
        print(f"Neo4j URI: {args.neo4j_uri}")
        print(f"Neo4j database: {args.neo4j_database}")
        print(f"✓ Direct Neo4j writes enabled")
    else:
        print(f"Neo4j writes: disabled (only Kafka topics)")
    print(f"==========================================")
    
    print(f"Verifying telemetry table exists...")
    try:
        # Try to read a small sample to verify tables exist
        print(f"Loading telemetry table from {telemetry_path}...")
        test_df = spark.read.format(args.bronze_format).load(telemetry_path).limit(1)
        print(f"Counting rows in telemetry table...")
        count = test_df.count()  # Trigger action to verify table exists
        print(f"✓ Verified telemetry table exists: {args.telemetry_table} (sample count: {count})")
        if count == 0:
            print("⚠ WARNING: Telemetry table exists but is empty. Stream will wait for new data.")
    except Exception as e:
        print(f"✗ Error: Could not read telemetry table {telemetry_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        print("Make sure bronze_stream.py has written data to this location.")
        raise

    print(f"Verifying events table exists...")
    try:
        print(f"Loading events table from {events_path}...")
        test_df = spark.read.format(args.bronze_format).load(events_path).limit(1)
        print(f"Counting rows in events table...")
        count = test_df.count()
        print(f"✓ Verified events table exists: {args.events_table} (sample count: {count})")
        if count == 0:
            print("⚠ WARNING: Events table exists but is empty. Stream will wait for new data.")
    except Exception as e:
        print(f"⚠ Warning: Could not read events table {events_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        print("Events stream may be empty. Continuing anyway...")
    
    print(f"Creating streaming DataFrames...")

    telemetry_stream = read_bronze_table(spark, args, args.telemetry_table)
    events_stream = read_bronze_table(spark, args, args.events_table)
    print(f"✓ Streaming DataFrames created")

    # Create closure with team_nodes_df
    def process_telemetry_with_teams(batch_df: DataFrame, batch_id: int) -> None:
        print(f"=== Telemetry batch {batch_id} triggered ===")
        process_telemetry_batch(batch_df, batch_id, args, kafka_options, spark, team_nodes_df)
        print(f"=== Telemetry batch {batch_id} completed ===")

    telemetry_checkpoint = os.path.join(args.checkpoint_base, "telemetry")
    print(f"Starting telemetry stream with checkpoint: {telemetry_checkpoint}")
    telemetry_query = (
        telemetry_stream.writeStream.foreachBatch(process_telemetry_with_teams)
        .outputMode("append")
        .option("checkpointLocation", telemetry_checkpoint)
        .trigger(processingTime="60 seconds")
        .start()
    )
    print(f"✓ Telemetry stream started. Query ID: {telemetry_query.id}")

    def process_events_with_logging(batch_df: DataFrame, batch_id: int) -> None:
        print(f"=== Events batch {batch_id} triggered ===")
        process_event_batch(batch_df, batch_id, args, kafka_options, spark)
        print(f"=== Events batch {batch_id} completed ===")

    events_checkpoint = os.path.join(args.checkpoint_base, "events")
    print(f"Starting events stream with checkpoint: {events_checkpoint}")
    events_query = (
        events_stream.writeStream.foreachBatch(process_events_with_logging)
        .outputMode("append")
        .option("checkpointLocation", events_checkpoint)
        .trigger(processingTime="60 seconds")
        .start()
    )
    print(f"✓ Events stream started. Query ID: {events_query.id}")

    print(f"==========================================")
    print(f"Gold stream started successfully!")
    print(f"Telemetry query ID: {telemetry_query.id}")
    print(f"Events query ID: {events_query.id}")
    print(f"Active streams: {[s.id for s in spark.streams.active]}")
    print(f"Waiting for stream triggers (60 second intervals)...")
    print(f"==========================================")
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
