"""Bronze stage Structured Streaming job.

Ingests Kafka telemetry and race event topics, parses the JSON payloads, and
persists them to object storage with minimal transformations. Each Kafka topic
is written to its own Delta/Parquet table alongside ingestion metadata.
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json, lit

from spark.schemas import RACE_EVENT_SCHEMA, TELEMETRY_SCHEMA
from spark.utils import add_common_kafka_options, create_spark_session, option_dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingestion job for telemetry and race events.")
    add_common_kafka_options(parser)
    parser.add_argument("--telemetry-topic", default=os.getenv("TELEMETRY_TOPIC", "telemetry.raw"))
    parser.add_argument("--events-topic", default=os.getenv("EVENTS_TOPIC", "race.events"))

    default_checkpoint = os.getenv("SPARK_BRONZE_CHECKPOINT_BASE")
    if not default_checkpoint:
        checkpoint_root = os.getenv("SPARK_CHECKPOINT_BASE")
        if checkpoint_root:
            default_checkpoint = checkpoint_root.rstrip("/") + "/bronze"
        else:
            default_checkpoint = os.getenv("BRONZE_CHECKPOINT_PATH", "s3://fastf1/checkpoints/bronze")

    parser.add_argument(
        "--output-base",
        default=os.getenv("SPARK_BRONZE_BASE", os.getenv("BRONZE_OUTPUT_PATH", "s3://fastf1/bronze")),
        help="Base output path for bronze tables.",
    )
    parser.add_argument(
        "--checkpoint-base",
        default=default_checkpoint,
        help="Base checkpoint path (one subdirectory per topic).",
    )
    parser.add_argument(
        "--write-format",
        default=os.getenv("BRONZE_WRITE_FORMAT", "delta"),
        choices=["delta", "parquet"],
        help="Storage format for bronze tables.",
    )
    parser.add_argument(
        "--kafka-option",
        action="append",
        default=[],
        help="Additional kafka reader options (key=value). Can be specified multiple times.",
    )
    return parser.parse_args()


def build_kafka_reader_options(args: argparse.Namespace) -> Dict[str, str]:
    options = {
        "kafka.bootstrap.servers": args.bootstrap_servers,
        "startingOffsets": args.starting_offsets,
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "100000",  # Limit to 100K messages per batch
    }
    if args.kafka_option:
        options.update(option_dict(args.kafka_option))
    return options


def read_kafka_topic(
    spark, topic: str, schema, options: Dict[str, str]
) -> Tuple[DataFrame, DataFrame]:
    """Return parsed and raw DataFrames for a Kafka topic."""
    base_df = (
        spark.readStream.format("kafka")
        .options(**options)
        .option("subscribe", topic)
        .load()
    )

    raw_df = base_df.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
    ).withColumn("ingested_at", current_timestamp())

    # Parse JSON and extract fields
    with_payload = raw_df.withColumn("payload", from_json(col("kafka_value"), schema))
    
    # For debugging: create a view to check parsing
    parsed_df = (
        with_payload
        .select("payload.*", "kafka_key", "topic", "partition", "offset", "kafka_timestamp", "ingested_at")
        .withColumn("bronze_topic", lit(topic))
    )
    
    # Don't filter out NULLs yet - let's see what we get
    # .filter(col("session_id").isNotNull())

    return parsed_df, raw_df


def main() -> None:
    args = parse_args()
    spark = create_spark_session("fastf1-bronze-stream", enable_delta=args.write_format == "delta")

    kafka_options = build_kafka_reader_options(args)

    telemetry_df, telemetry_raw = read_kafka_topic(
        spark, args.telemetry_topic, TELEMETRY_SCHEMA, kafka_options
    )
    events_df, events_raw = read_kafka_topic(
        spark, args.events_topic, RACE_EVENT_SCHEMA, kafka_options
    )

    queries = []
    for name, df in {
        f"{args.telemetry_topic}-parsed": telemetry_df,
        f"{args.telemetry_topic}-raw": telemetry_raw,
        f"{args.events_topic}-parsed": events_df,
        f"{args.events_topic}-raw": events_raw,
    }.items():
        sanitized = name.replace(".", "_")
        output_path = os.path.join(args.output_base, sanitized)
        checkpoint_path = os.path.join(args.checkpoint_base, sanitized)
        writer = (
            df.writeStream.outputMode("append")
            .trigger(processingTime="30 seconds")
            .option("checkpointLocation", checkpoint_path)
        )
        writer = writer.format(args.write_format)
        if args.write_format != "delta":
            writer = writer.option("path", output_path)
        else:
            writer = writer.option("path", output_path)
        query = writer.start()
        queries.append(query)
        print(f"Started stream for {name} -> {output_path}")

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
