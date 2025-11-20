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

from schemas import INTERACTION_EVENT_PAYLOAD_SCHEMA
from utils import add_common_kafka_options, create_spark_session, option_dict
from streaming_algorithms import (
    compute_cardinality_metrics,
    reservoir_sample,
    compute_pagerank,
    detect_communities_lsh,
)


def read_bronze_table(spark, args, table_name: str) -> DataFrame:
    """Read a Bronze Delta table as a streaming DataFrame."""
    table_path = os.path.join(args.bronze_base.rstrip("/"), table_name)
    return (
        spark.readStream
        .format(args.bronze_format)
        .option("maxFilesPerTrigger", args.max_files_per_trigger)
        .load(table_path)
    )


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

    parser.add_argument(
        "--enable-hyperloglog",
        action="store_true",
        default=os.getenv("ENABLE_HYPERLOGLOG", "true").lower() == "true",
        help="Use HyperLogLog for cardinality estimation instead of exact countDistinct.",
    )
    parser.add_argument(
        "--enable-sampling",
        action="store_true",
        default=os.getenv("ENABLE_SAMPLING", "true").lower() == "true",
        help="Enable Reservoir Sampling for dashboard/visualization data.",
    )
    parser.add_argument(
        "--enable-pagerank",
        action="store_true",
        default=os.getenv("ENABLE_PAGERANK", "false").lower() == "true",
        help="Compute PageRank scores for driver influence analysis.",
    )
    parser.add_argument(
        "--enable-communities",
        action="store_true",
        default=os.getenv("ENABLE_COMMUNITIES", "false").lower() == "true",
        help="Detect driver communities using LSH (MinHash) clustering.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=int(os.getenv("RESERVOIR_SAMPLE_SIZE", "1000")),
        help="Number of samples for Reservoir Sampling (default: 1000).",
    )

    return parser.parse_args()





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
        
        (df.write
            .format("org.neo4j.spark.DataSource")
            .mode("Overwrite")
            .option("url", args.neo4j_uri)
            .option("authentication.type", "basic")
            .option("authentication.basic.username", args.neo4j_username)
            .option("authentication.basic.password", args.neo4j_password)
            .option("database", args.neo4j_database)
            .option("query", cypher_query)
            .save())
        print(f"Wrote {row_count} rows to Neo4j")
    except Exception as e:
        print(f"Error writing to Neo4j: {str(e)}")
        import traceback
        traceback.print_exc()


def read_bronze_table(spark, args: argparse.Namespace, table: str) -> DataFrame:
    path = os.path.join(args.bronze_base.rstrip("/"), table)
    stream = spark.readStream.format(args.bronze_format)
    
    if args.bronze_format == "delta":
        stream = stream.option("maxFilesPerTrigger", str(args.max_files_per_trigger))
        
        if args.starting_version is not None:
            print(f"Starting Delta stream from version {args.starting_version} for {table}")
            stream = stream.option("startingVersion", str(args.starting_version))
        else:
            print(f"Processing Delta table {table} with maxFilesPerTrigger={args.max_files_per_trigger}")
            print(f"  - If checkpoint exists: resumes from last processed version")
            print(f"  - If no checkpoint: processes all existing data in batches")
    
    return stream.load(path)


def build_driver_nodes(df: DataFrame) -> DataFrame:
    aggregated = (
        df.filter(F.col("driver_id").isNotNull())
        .groupBy("driver_id")
        .agg(
            F.first("driver_name", ignorenulls=True).alias("driver_name"),
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
                    F.col("driver_name"),
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


def build_session_nodes(df: DataFrame, use_hll: bool = True) -> DataFrame:
    if use_hll:
        aggregated = (
            df.filter(F.col("session_id").isNotNull())
            .groupBy("session_id")
            .agg(
                F.approx_count_distinct("driver_id", rsd=0.05).alias("driver_count"),
                F.approx_count_distinct("lap_number", rsd=0.05).alias("lap_count"),
                F.min("event_ts").alias("first_event_ts"),
                F.max("event_ts").alias("last_event_ts"),
            )
        )
    else:
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
        
        if args.enable_sampling and cleaned_count > args.sample_size:
            sampled = publish_sampled_telemetry(cleaned, batch_id, args.sample_size)
        
        if args.enable_communities:
            communities = detect_driver_communities(cleaned, batch_id, spark)
            if communities is not None and args.write_to_neo4j:
                communities_neo4j = communities.select(
                    F.col("driver_id").alias("driverId"),
                    F.col("community_id").alias("communityId")
                )
                write_to_neo4j(
                    communities_neo4j,
                    spark,
                    "MATCH (d:Driver {driverId: event.driverId}) SET d.communityId = event.communityId",
                    args,
                )

        print(f"[Batch {batch_id}] Building driver nodes...")
        driver_nodes = build_driver_nodes(cleaned)
        if args.write_to_neo4j:
            driver_nodes_for_neo4j = cleaned.filter(F.col("driver_name").isNotNull()).groupBy("driver_id").agg(
                F.first("driver_name", ignorenulls=True).alias("driver_name"),
                F.min("session_id").alias("first_session_id"),
                F.max("session_id").alias("latest_session_id"),
                F.max("lap_number").alias("max_lap_seen"),
                F.avg("speed_kph").alias("avg_speed_kph"),
                F.max("speed_kph").alias("max_speed_kph"),
                F.avg("battery_pct").alias("avg_battery_pct"),
                F.avg("fuel_mass_kg").alias("avg_fuel_mass_kg"),
            ).select(
                F.col("driver_id").alias("driverId"),
                F.col("driver_name").alias("driverName"),
                F.col("first_session_id").alias("firstSessionId"),
                F.col("latest_session_id").alias("latestSessionId"),
                F.col("max_lap_seen").alias("maxLapSeen"),
                F.col("avg_speed_kph").alias("avgSpeedKph"),
                F.col("max_speed_kph").alias("maxSpeedKph"),
                F.col("avg_battery_pct").alias("avgBatteryPct"),
                F.col("avg_fuel_mass_kg").alias("avgFuelMassKg"),
            )
            driver_count = driver_nodes_for_neo4j.count()
            if driver_count > 0:
                write_to_neo4j(
                    driver_nodes_for_neo4j,
                    spark,
                    "MERGE (d:Driver {driverId: event.driverId}) SET d.driverName = event.driverName, d.firstSessionId = event.firstSessionId, d.latestSessionId = event.latestSessionId, d.maxLapSeen = event.maxLapSeen, d.avgSpeedKph = event.avgSpeedKph, d.maxSpeedKph = event.maxSpeedKph, d.avgBatteryPct = event.avgBatteryPct, d.avgFuelMassKg = event.avgFuelMassKg",
                    args,
                )
            
            driver_stats_only = cleaned.groupBy("driver_id").agg(
                F.min("session_id").alias("first_session_id"),
                F.max("session_id").alias("latest_session_id"),
                F.max("lap_number").alias("max_lap_seen"),
                F.avg("speed_kph").alias("avg_speed_kph"),
                F.max("speed_kph").alias("max_speed_kph"),
                F.avg("battery_pct").alias("avg_battery_pct"),
                F.avg("fuel_mass_kg").alias("avg_fuel_mass_kg"),
            ).select(
                F.col("driver_id").alias("driverId"),
                F.col("first_session_id").alias("firstSessionId"),
                F.col("latest_session_id").alias("latestSessionId"),
                F.col("max_lap_seen").alias("maxLapSeen"),
                F.col("avg_speed_kph").alias("avgSpeedKph"),
                F.col("max_speed_kph").alias("maxSpeedKph"),
                F.col("avg_battery_pct").alias("avgBatteryPct"),
                F.col("avg_fuel_mass_kg").alias("avgFuelMassKg"),
            )
            write_to_neo4j(
                driver_stats_only,
                spark,
                "MERGE (d:Driver {driverId: event.driverId}) SET d.firstSessionId = event.firstSessionId, d.latestSessionId = event.latestSessionId, d.maxLapSeen = event.maxLapSeen, d.avgSpeedKph = event.avgSpeedKph, d.maxSpeedKph = event.maxSpeedKph, d.avgBatteryPct = event.avgBatteryPct, d.avgFuelMassKg = event.avgFuelMassKg",
                args,
            )
        
        print(f"[Batch {batch_id}] Building session nodes...")
        session_nodes = build_session_nodes(cleaned, use_hll=args.enable_hyperloglog)
        if args.write_to_neo4j:
            if args.enable_hyperloglog:
                session_nodes_for_neo4j = cleaned.filter(F.col("session_id").isNotNull()).groupBy("session_id").agg(
                    F.approx_count_distinct("driver_id", rsd=0.05).alias("driverCount"),
                    F.approx_count_distinct("lap_number", rsd=0.05).alias("lapCount"),
                    F.min("event_ts").alias("firstEventTs"),
                    F.max("event_ts").alias("lastEventTs"),
                ).select(
                    F.col("session_id").alias("sessionId"),
                    F.col("driverCount"),
                    F.col("lapCount"),
                    F.col("firstEventTs"),
                    F.col("lastEventTs"),
                )
            else:
                session_nodes_for_neo4j = cleaned.filter(F.col("session_id").isNotNull()).groupBy("session_id").agg(
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
        print(f"[Batch {batch_id}] Error processing telemetry batch: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def process_event_batch(
    df: DataFrame,
    batch_id: int,
    args: argparse.Namespace,
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

        if args.write_to_neo4j:
            print(f"[Batch {batch_id}] Creating/updating Driver nodes from events...")
            driver_nodes_from_events = cleaned.filter(
                F.col("driver_id").isNotNull() & F.col("driver_name").isNotNull()
            ).groupBy("driver_id").agg(
                F.first("driver_name", ignorenulls=True).alias("driver_name"),
                F.min("session_id").alias("first_session_id"),
                F.max("session_id").alias("latest_session_id"),
            ).select(
                F.col("driver_id").alias("driverId"),
                F.col("driver_name").alias("driverName"),
                F.col("first_session_id").alias("firstSessionId"),
                F.col("latest_session_id").alias("latestSessionId"),
            )
            write_to_neo4j(
                driver_nodes_from_events,
                spark,
                "MERGE (d:Driver {driverId: event.driverId}) SET d.driverName = COALESCE(event.driverName, d.driverName), d.firstSessionId = COALESCE(event.firstSessionId, d.firstSessionId), d.latestSessionId = COALESCE(event.latestSessionId, d.latestSessionId)",
                args,
            )
            
            print(f"[Batch {batch_id}] Creating/updating Session nodes from events...")
            session_nodes_from_events = cleaned.filter(
                F.col("session_id").isNotNull()
            ).groupBy("session_id").agg(
                F.min("event_ts").alias("firstEventTs"),
                F.max("event_ts").alias("lastEventTs"),
            ).select(
                F.col("session_id").alias("sessionId"),
                F.col("firstEventTs"),
                F.col("lastEventTs"),
            )
            write_to_neo4j(
                session_nodes_from_events,
                spark,
                "MERGE (s:Session {sessionId: event.sessionId}) SET s.firstEventTs = COALESCE(event.firstEventTs, s.firstEventTs), s.lastEventTs = COALESCE(event.lastEventTs, s.lastEventTs)",
                args,
            )

        print(f"[Batch {batch_id}] Building overtake edges...")
        overtake_edges = build_interaction_edges(cleaned, "overtake")
        if overtake_edges is not None:
            overtake_count = overtake_edges.count()
            print(f"[Batch {batch_id}] Found {overtake_count} overtake interactions")
            if overtake_count > 0:
                if args.write_to_neo4j:
                    overtake_for_neo4j = cleaned.filter(F.lower(F.col("event_type")) == "overtake").withColumn(
                        "interaction",
                        F.when(
                            F.col("payload").isNotNull() & (F.col("payload") != ""),
                            F.from_json("payload", INTERACTION_EVENT_PAYLOAD_SCHEMA)
                        ).otherwise(F.struct(*[F.lit(None).cast(field.dataType).alias(field.name) for field in INTERACTION_EVENT_PAYLOAD_SCHEMA.fields]))
                    ).select(
                        F.coalesce(F.col("interaction.attacker_id"), F.col("interaction.driver_a_id"), F.col("driver_id")).alias("attackerId"),
                        F.coalesce(F.col("interaction.defender_id"), F.col("interaction.driver_b_id")).alias("defenderId"),
                        F.col("session_id").alias("sessionId"),
                        F.col("event_id").alias("eventId"),
                        F.coalesce(F.col("interaction.lap_number"), F.col("lap_number")).alias("lapNumber"),
                        F.col("interaction.lap_count").alias("lapCount"),
                        F.col("interaction.avg_gap_ms").alias("avgGapMs"),
                        F.col("interaction.delta_time_ms").alias("deltaTimeMs"),
                        F.col("event_ts_utc").alias("eventTs"),
                    ).filter(F.col("attackerId").isNotNull() & F.col("defenderId").isNotNull())
                    write_to_neo4j(
                        overtake_for_neo4j,
                        spark,
                        "MATCH (attacker:Driver {driverId: event.attackerId}) MATCH (defender:Driver {driverId: event.defenderId}) MERGE (attacker)-[r:OVERTAKE]->(defender) SET r += event",
                        args,
                    )
        
        print(f"[Batch {batch_id}] Building battle edges...")
        battle_edges = build_interaction_edges(cleaned, "battle")
        if battle_edges is not None:
            battle_count = battle_edges.count()
            print(f"[Batch {batch_id}] Found {battle_count} battle interactions")
            if battle_count > 0:
                if args.write_to_neo4j:
                    battle_for_neo4j = cleaned.filter(F.lower(F.col("event_type")) == "battle").withColumn(
                        "interaction",
                        F.when(
                            F.col("payload").isNotNull() & (F.col("payload") != ""),
                            F.from_json("payload", INTERACTION_EVENT_PAYLOAD_SCHEMA)
                        ).otherwise(F.struct(*[F.lit(None).cast(field.dataType).alias(field.name) for field in INTERACTION_EVENT_PAYLOAD_SCHEMA.fields]))
                    ).select(
                        F.coalesce(F.col("interaction.attacker_id"), F.col("interaction.driver_a_id"), F.col("driver_id")).alias("attackerId"),
                        F.coalesce(F.col("interaction.defender_id"), F.col("interaction.driver_b_id")).alias("defenderId"),
                        F.col("session_id").alias("sessionId"),
                        F.col("event_id").alias("eventId"),
                        F.coalesce(F.col("interaction.lap_number"), F.col("lap_number")).alias("lapNumber"),
                        F.col("interaction.lap_count").alias("lapCount"),
                        F.col("interaction.avg_gap_ms").alias("avgGapMs"),
                        F.col("interaction.delta_time_ms").alias("deltaTimeMs"),
                        F.col("event_ts_utc").alias("eventTs"),
                    ).filter(F.col("attackerId").isNotNull() & F.col("defenderId").isNotNull())
                    write_to_neo4j(
                        battle_for_neo4j,
                        spark,
                        "MATCH (attacker:Driver {driverId: event.attackerId}) MATCH (defender:Driver {driverId: event.defenderId}) MERGE (attacker)-[r:BATTLE]->(defender) SET r += event",
                        args,
                    )
        
        if args.enable_pagerank and (overtake_count > 0 or battle_count > 0):
            if overtake_edges is not None and overtake_count > 0:
                overtake_for_neo4j.createOrReplaceGlobalTempView("gold_overtakes")
            if battle_edges is not None and battle_count > 0:
                battle_for_neo4j.createOrReplaceGlobalTempView("gold_battles")
            
            session_id = cleaned.select("session_id").first()[0] if cleaned.count() > 0 else None
            if session_id:
                pagerank_df = compute_driver_pagerank(session_id, spark, args)
                if pagerank_df is not None and args.write_to_neo4j:
                    pagerank_neo4j = pagerank_df.select(
                        F.col("id").alias("driverId"),
                        F.col("pagerank_score").alias("pagerankScore"),
                        F.col("sessionId")
                    )
                    write_to_neo4j(
                        pagerank_neo4j,
                        spark,
                        "MATCH (d:Driver {driverId: event.driverId}) SET d.pagerankScore = event.pagerankScore",
                        args,
                    )

        cleaned.unpersist()
        print(f"[Batch {batch_id}] Event batch processing completed successfully")
    except Exception as e:
        print(f"[Batch {batch_id}] Error processing event batch: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def publish_sampled_telemetry(df: DataFrame, batch_id: int, sample_size: int) -> Optional[DataFrame]:
    """Apply Reservoir Sampling for dashboard/visualization data."""
    try:
        print(f"[Batch {batch_id}] Applying Reservoir Sampling (size={sample_size})...")
        sampled = reservoir_sample(df, sample_size=sample_size, seed=batch_id)
        sampled_count = sampled.count()
        print(f"[Batch {batch_id}] Sampled {sampled_count} telemetry points")
        return sampled
    except Exception as e:
        print(f"[Batch {batch_id}] Warning: Reservoir Sampling failed: {str(e)}")
        return None


def compute_driver_pagerank(session_id: str, spark, args) -> Optional[DataFrame]:
    """Compute PageRank for drivers based on overtake/battle graph."""
    try:
        print(f"Computing PageRank for session {session_id}...")
        
        overtakes = spark.sql(f"""
            SELECT DISTINCT attackerId as src, defenderId as dst
            FROM global_temp.gold_overtakes
            WHERE sessionId = '{session_id}'
        """)
        
        battles = spark.sql(f"""
            SELECT DISTINCT attackerId as src, defenderId as dst
            FROM global_temp.gold_battles
            WHERE sessionId = '{session_id}'
        """)
        
        edges = overtakes.union(battles).distinct()
        edge_count = edges.count()
        
        if edge_count == 0:
            print(f"No interaction edges for session {session_id}, skipping PageRank")
            return None
        
        vertices = edges.select(F.col("src").alias("id")).union(
            edges.select(F.col("dst").alias("id"))
        ).distinct()
        
        pagerank_df = compute_pagerank(vertices, edges, max_iter=10, reset_prob=0.15)
        pagerank_df = pagerank_df.withColumn("sessionId", F.lit(session_id))
        
        print(f"PageRank computed for {pagerank_df.count()} drivers in session {session_id}")
        return pagerank_df
    except Exception as e:
        print(f"Warning: PageRank computation failed for session {session_id}: {str(e)}")
        return None


def detect_driver_communities(cleaned_df: DataFrame, batch_id: int, spark) -> Optional[DataFrame]:
    """Cluster drivers by racing behavior using LSH."""
    try:
        from pyspark.ml.feature import VectorAssembler
        
        print(f"[Batch {batch_id}] Detecting driver communities using LSH...")
        
        feature_df = cleaned_df.groupBy("driver_id", "session_id").agg(
            F.avg("speed_kph").alias("avg_speed"),
            F.avg("throttle_pct").alias("avg_throttle"),
            F.avg("brake_pressure_bar").alias("avg_brake"),
            F.max("engine_rpm").alias("max_rpm")
        ).filter(
            F.col("avg_speed").isNotNull() & 
            F.col("avg_throttle").isNotNull() & 
            F.col("avg_brake").isNotNull() & 
            F.col("max_rpm").isNotNull()
        )
        
        if feature_df.count() == 0:
            print(f"[Batch {batch_id}] No valid features for community detection")
            return None
        
        assembler = VectorAssembler(
            inputCols=["avg_speed", "avg_throttle", "avg_brake", "max_rpm"],
            outputCol="features"
        )
        features_df = assembler.transform(feature_df)
        
        communities = detect_communities_lsh(features_df, num_hash_tables=5)
        
        print(f"[Batch {batch_id}] Detected communities for {communities.count()} drivers")
        return communities
    except Exception as e:
        print(f"[Batch {batch_id}] Warning: Community detection failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def main() -> None:
    args = parse_args()

    extra_conf: Dict[str, str] = {}
    if args.bronze_format == "delta":
        extra_conf["spark.sql.shuffle.partitions"] = os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")

    spark = create_spark_session("fastf1-gold-stream", enable_delta=args.bronze_format == "delta", extra_conf=extra_conf)

    team_nodes_df: Optional[DataFrame] = None
    if args.teams_lookup_path:
        try:
            team_nodes_df = build_team_nodes(args.teams_lookup_path, spark)
            print(f"Loaded team nodes from {args.teams_lookup_path}")
        except Exception as e:
            print(f"Warning: Could not load team nodes from {args.teams_lookup_path}: {str(e)}")
            team_nodes_df = None
    
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
        print(f"Direct Neo4j writes enabled")
    else:
        print(f"Neo4j writes: disabled (only Kafka topics)")
    print(f"==========================================")
    
    print(f"Verifying telemetry table exists...")
    try:
        print(f"Loading telemetry table from {telemetry_path}...")
        test_df = spark.read.format(args.bronze_format).load(telemetry_path).limit(1)
        print(f"Counting rows in telemetry table...")
        count = test_df.count()
        print(f"Verified telemetry table exists: {args.telemetry_table} (sample count: {count})")
        if count == 0:
            print("WARNING: Telemetry table exists but is empty. Stream will wait for new data.")
    except Exception as e:
        print(f"Error: Could not read telemetry table {telemetry_path}: {str(e)}")
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
        print(f"Verified events table exists: {args.events_table} (sample count: {count})")
        if count == 0:
            print("WARNING: Events table exists but is empty. Stream will wait for new data.")
    except Exception as e:
        print(f"Warning: Could not read events table {events_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        print("Events stream may be empty. Continuing anyway...")
    
    print(f"Creating streaming DataFrames...")

    telemetry_stream = read_bronze_table(spark, args, args.telemetry_table)
    events_stream = read_bronze_table(spark, args, args.events_table)
    print(f"Streaming DataFrames created")

    def process_telemetry_with_teams(batch_df: DataFrame, batch_id: int) -> None:
        print(f"=== Telemetry batch {batch_id} triggered ===")
        process_telemetry_batch(batch_df, batch_id, args, spark, team_nodes_df)
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
    print(f"Telemetry stream started. Query ID: {telemetry_query.id}")

    def process_events_with_logging(batch_df: DataFrame, batch_id: int) -> None:
        print(f"=== Events batch {batch_id} triggered ===")
        process_event_batch(batch_df, batch_id, args, spark)
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
    print(f"Events stream started. Query ID: {events_query.id}")

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
