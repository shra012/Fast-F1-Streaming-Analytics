"""Gold stage Structured Streaming job.

Consumes silver telemetry tables and produces lap-level aggregates along with
derived metrics suitable for analytics dashboards and downstream feature
generation. Outputs are written to Delta/Parquet and optionally published to
Kafka for real-time consumers.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.utils import add_common_kafka_options, create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold aggregation job for lap-level metrics.")

    silver_base_default = os.getenv("SPARK_SILVER_BASE", os.getenv("SILVER_OUTPUT_PATH", "s3://fastf1/silver"))
    gold_base_default = os.getenv("SPARK_GOLD_BASE", os.getenv("GOLD_OUTPUT_PATH", "s3://fastf1/gold"))
    checkpoint_default = os.getenv("SPARK_GOLD_CHECKPOINT_BASE")
    if not checkpoint_default:
        checkpoint_root = os.getenv("SPARK_CHECKPOINT_BASE")
        if checkpoint_root:
            checkpoint_default = checkpoint_root.rstrip("/") + "/gold"
        else:
            checkpoint_default = os.getenv("GOLD_CHECKPOINT_PATH", "s3://fastf1/checkpoints/gold")

    parser.add_argument(
        "--silver-base",
        default=silver_base_default,
        help="Base path containing silver tables.",
    )
    parser.add_argument(
        "--output-base",
        default=gold_base_default,
        help="Base path to write gold tables.",
    )
    parser.add_argument(
        "--checkpoint-base",
        default=checkpoint_default,
        help="Base checkpoint path for gold outputs.",
    )
    parser.add_argument(
        "--input-format",
        default=os.getenv("SILVER_WRITE_FORMAT", "delta"),
        choices=["delta", "parquet"],
        help="Storage format of silver inputs.",
    )
    parser.add_argument(
        "--output-format",
        default=os.getenv("GOLD_WRITE_FORMAT", "delta"),
        choices=["delta", "parquet"],
        help="Storage format for gold outputs.",
    )
    parser.add_argument(
        "--telemetry-subdir",
        default="telemetry_enriched",
        help="Subdirectory for silver telemetry data.",
    )
    parser.add_argument(
        "--watermark",
        default=os.getenv("GOLD_TELEMETRY_WATERMARK", "5 minutes"),
        help="Watermark duration for lap aggregations.",
    )
    parser.add_argument(
        "--kafka-output-topic",
        default=os.getenv("GOLD_KAFKA_TOPIC", ""),
        help="Optional Kafka topic to publish lap metrics.",
    )
    add_common_kafka_options(parser)
    return parser.parse_args()


def read_silver_telemetry(spark, base_path: str, subdir: str, fmt: str) -> DataFrame:
    path = os.path.join(base_path, subdir)
    return spark.readStream.format(fmt).load(path)


def write_table(df: DataFrame, output_path: str, checkpoint_path: str, fmt: str):
    writer = (
        df.writeStream.format(fmt)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .option("path", output_path)
    )
    return writer.start()


def main() -> None:
    args = parse_args()
    spark = create_spark_session("fastf1-gold-stream", enable_delta=args.output_format == "delta")

    telemetry = read_silver_telemetry(spark, args.silver_base, args.telemetry_subdir, args.input_format)

    telemetry_with_watermark = telemetry.withWatermark("event_time", args.watermark)

    lap_metrics = (
        telemetry_with_watermark.groupBy("session_id", "driver_id", "lap_number")
        .agg(
            F.min("event_time").alias("lap_start_time"),
            F.max("event_time").alias("lap_end_time"),
            F.avg("speed_kph").alias("avg_speed_kph"),
            F.max("speed_kph").alias("max_speed_kph"),
            F.min("speed_kph").alias("min_speed_kph"),
            F.avg("throttle_pct").alias("avg_throttle_pct"),
            F.avg("brake_pressure_bar").alias("avg_brake_pressure_bar"),
            F.sum("fuel_mass_kg").alias("fuel_mass_used_kg"),
            F.sum("battery_pct").alias("battery_pct_total"),
            F.last("tyre_compound", ignorenulls=True).alias("tyre_compound"),
            F.last("event_id", ignorenulls=True).alias("last_event_id"),
            F.max("event_indicator").alias("event_indicator"),
        )
        .withColumn("lap_time_ms", (F.col("lap_end_time").cast("long") - F.col("lap_start_time").cast("long")) * F.lit(1000))
        .withColumn("battery_delta_pct", F.col("battery_pct_total"))
        .withColumn("generated_at", F.current_timestamp())
    )

    output_path = os.path.join(args.output_base, "fact_lap")
    checkpoint_path = os.path.join(args.checkpoint_base, "fact_lap")
    query = write_table(lap_metrics, output_path, checkpoint_path, args.output_format)
    print(f"Started gold lap metrics stream -> {output_path}")

    queries = [query]

    if args.kafka_output_topic:
        kafka_ready = (
            lap_metrics.select(
                F.concat_ws(
                    "_",
                    F.col("session_id"),
                    F.col("driver_id"),
                    F.col("lap_number").cast("string"),
                ).alias("key"),
                F.to_json(F.struct("*")).alias("value"),
            )
        )
        kafka_writer = (
            kafka_ready.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.kafka_output_topic)
            .option("checkpointLocation", os.path.join(args.checkpoint_base, "kafka_fact_lap"))
            .outputMode("update")
        )
        kafka_query = kafka_writer.start()
        queries.append(kafka_query)
        print(f"Publishing lap metrics to Kafka topic {args.kafka_output_topic}")

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
