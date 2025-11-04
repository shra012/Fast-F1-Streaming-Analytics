"""Silver stage Structured Streaming job.

Consumes bronze Delta tables, performs cleansing, deduplication, and joins
telemetry records with race-control events to produce conformant telemetry
tables suitable for analytics.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs,
    col,
    from_unixtime,
    lit,
    to_timestamp,
    when,
)

from spark.utils import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver enrichment job for telemetry and race events.")

    bronze_base_default = os.getenv("SPARK_BRONZE_BASE", os.getenv("BRONZE_OUTPUT_PATH", "s3://fastf1/bronze"))
    silver_base_default = os.getenv("SPARK_SILVER_BASE", os.getenv("SILVER_OUTPUT_PATH", "s3://fastf1/silver"))
    checkpoint_default = os.getenv("SPARK_SILVER_CHECKPOINT_BASE")
    if not checkpoint_default:
        checkpoint_root = os.getenv("SPARK_CHECKPOINT_BASE")
        if checkpoint_root:
            checkpoint_default = checkpoint_root.rstrip("/") + "/silver"
        else:
            checkpoint_default = os.getenv("SILVER_CHECKPOINT_PATH", "s3://fastf1/checkpoints/silver")

    parser.add_argument(
        "--bronze-base",
        default=bronze_base_default,
        help="Base path of bronze tables written by bronze_stream.py.",
    )
    parser.add_argument(
        "--output-base",
        default=silver_base_default,
        help="Base path to write silver tables.",
    )
    parser.add_argument(
        "--checkpoint-base",
        default=checkpoint_default,
        help="Base checkpoint path.",
    )
    parser.add_argument(
        "--input-format",
        default=os.getenv("BRONZE_WRITE_FORMAT", "delta"),
        choices=["delta", "parquet"],
        help="Storage format of bronze tables.",
    )
    parser.add_argument(
        "--output-format",
        default=os.getenv("SILVER_WRITE_FORMAT", "delta"),
        choices=["delta", "parquet"],
        help="Storage format for silver outputs.",
    )
    parser.add_argument(
        "--telemetry-subdir",
        default=os.getenv("BRONZE_TELEMETRY_DIR", "telemetry_raw-parsed"),
        help="Subdirectory containing parsed telemetry bronze data.",
    )
    parser.add_argument(
        "--events-subdir",
        default=os.getenv("BRONZE_EVENTS_DIR", "race_events-parsed"),
        help="Subdirectory containing parsed race event bronze data.",
    )
    parser.add_argument(
        "--event-join-seconds",
        type=int,
        default=int(os.getenv("SILVER_EVENT_JOIN_SECONDS", "10")),
        help="Join tolerance (seconds) between telemetry and race events.",
    )
    parser.add_argument(
        "--telemetry-watermark",
        default=os.getenv("SILVER_TELEMETRY_WATERMARK", "2 minutes"),
        help="Watermark duration for telemetry deduplication.",
    )
    parser.add_argument(
        "--events-watermark",
        default=os.getenv("SILVER_EVENTS_WATERMARK", "10 minutes"),
        help="Watermark duration for race events.",
    )
    return parser.parse_args()


def read_bronze_table(spark, base_path: str, subdir: str, fmt: str) -> DataFrame:
    path = os.path.join(base_path, subdir)
    return spark.readStream.format(fmt).load(path)


def write_stream(df: DataFrame, output_path: str, checkpoint_path: str, fmt: str, output_mode: str = "append"):
    writer = (
        df.writeStream.format(fmt)
        .option("checkpointLocation", checkpoint_path)
        .outputMode(output_mode)
        .option("path", output_path)
    )
    return writer.start()


def main() -> None:
    args = parse_args()
    spark = create_spark_session("fastf1-silver-stream", enable_delta=args.output_format == "delta")

    telemetry_bronze = read_bronze_table(spark, args.bronze_base, args.telemetry_subdir, args.input_format)
    events_bronze = read_bronze_table(spark, args.bronze_base, args.events_subdir, args.input_format)

    telemetry_clean = (
        telemetry_bronze.withColumn("event_time", to_timestamp("timestamp_utc"))
        .withWatermark("event_time", args.telemetry_watermark)
        .dropDuplicates(["session_id", "driver_id", "event_time", "lap_number"])
        .withColumn("speed_mps", col("speed_kph") / lit(3.6))
        .withColumn("throttle_pct", when(col("throttle_pct").isNull(), lit(0.0)).otherwise(col("throttle_pct")))
        .withColumn("brake_pressure_bar", when(col("brake_pressure_bar").isNull(), lit(0.0)).otherwise(col("brake_pressure_bar")))
        .withColumn("battery_pct", when(col("battery_pct").isNull(), lit(0.0)).otherwise(col("battery_pct")))
        .withColumn("fuel_mass_kg", when(col("fuel_mass_kg").isNull(), lit(0.0)).otherwise(col("fuel_mass_kg")))
    )

    events_clean = (
        events_bronze.withColumn("event_ts", to_timestamp("event_ts_utc"))
        .withWatermark("event_ts", args.events_watermark)
        .withColumnRenamed("event_type", "event_name")
        .withColumnRenamed("driver_id", "event_driver_id")
    )

    events_prepared = events_clean.select(
        col("session_id").alias("event_session_id"),
        col("event_driver_id"),
        col("event_ts"),
        "event_id",
        "event_name",
        "lap_number",
        "pit_stop_id",
        "pit_duration_ms",
        "penalty_seconds",
        "safety_car_state",
        "payload",
    )

    join_condition = (
        (telemetry_clean.session_id == events_prepared.event_session_id)
        & (
            events_prepared.event_driver_id.isNull()
            | (telemetry_clean.driver_id == events_prepared.event_driver_id)
        )
        & (abs(telemetry_clean.event_time.cast("long") - events_prepared.event_ts.cast("long")) <= args.event_join_seconds)
    )

    telemetry_enriched = (
        telemetry_clean.join(events_prepared, join_condition, "leftOuter")
        .withColumn("event_indicator", when(col("event_id").isNull(), lit(0)).otherwise(lit(1)))
        .withColumn("event_ts_unix", from_unixtime(col("event_ts").cast("long")))
    )

    telemetry_output = os.path.join(args.output_base, "telemetry_enriched")
    telemetry_checkpoint = os.path.join(args.checkpoint_base, "telemetry_enriched")
    events_output = os.path.join(args.output_base, "race_events_clean")
    events_checkpoint = os.path.join(args.checkpoint_base, "race_events_clean")

    queries = [
        write_stream(telemetry_enriched, telemetry_output, telemetry_checkpoint, args.output_format),
        write_stream(events_clean, events_output, events_checkpoint, args.output_format),
    ]

    print(f"Started silver telemetry stream -> {telemetry_output}")
    print(f"Started silver events stream -> {events_output}")

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
