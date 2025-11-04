"""Utility helpers shared across Spark streaming jobs."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, Iterable, Optional

from pyspark.sql import SparkSession
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

_ENV_LOADED = False


def _load_env() -> None:
    """Load environment variables from .env files with sensible fallbacks."""
    global _ENV_LOADED
    if _ENV_LOADED or load_dotenv is None:
        return

    candidates = []
    env_override = os.getenv("FASTF1_ENV_FILE")
    if env_override:
        candidates.append(Path(env_override))

    module_root = Path(__file__).resolve().parent
    candidates.extend(
        [
            module_root / ".env",
            module_root.parent / ".env",
            module_root.parent / "kafka" / ".env",
            Path.cwd() / ".env",
        ]
    )

    for candidate in candidates:
        if candidate.is_file():
            load_dotenv(candidate)
            _ENV_LOADED = True
            return

    # Fall back to default search if nothing matched.
    load_dotenv()
    _ENV_LOADED = True


def create_spark_session(app_name: str, enable_delta: bool = True, extra_conf: Optional[Dict[str, str]] = None) -> SparkSession:
    """Create or reuse a SparkSession with optional Delta Lake support."""
    _load_env()
    builder = SparkSession.builder.appName(app_name)
    if enable_delta:
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def add_common_kafka_options(parser: argparse.ArgumentParser) -> None:
    """Attach commonly used Kafka CLI arguments to a parser."""
    _load_env()
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_BROKERS", "localhost:9092"),
        help="Kafka bootstrap servers (comma separated). Defaults to KAFKA_BOOTSTRAP_BROKERS env var or localhost:9092.",
    )
    parser.add_argument(
        "--starting-offsets",
        default=os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        choices=["latest", "earliest"],
        help="Kafka starting offsets (latest or earliest). Defaults to KAFKA_STARTING_OFFSETS env var.",
    )


def option_dict(pairs: Iterable[str]) -> Dict[str, str]:
    """Convert key=value CLI overrides into a dictionary."""
    options: Dict[str, str] = {}
    for item in pairs:
        if "=" not in item:
            raise ValueError(f"Invalid option '{item}'. Expected key=value.")
        key, value = item.split("=", 1)
        options[key.strip()] = value.strip()
    return options
