"""Common Spark StructType schemas for telemetry streaming jobs."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Base telemetry payload emitted by the producer.
TELEMETRY_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("car_number", StringType(), True),
        StructField("lap_number", IntegerType(), True),
        StructField("micro_sector_id", IntegerType(), True),
        StructField("timestamp_utc", StringType(), False),
        StructField("speed_kph", FloatType(), True),
        StructField("throttle_pct", FloatType(), True),
        StructField("brake_pressure_bar", FloatType(), True),
        StructField("steering_angle_deg", FloatType(), True),
        StructField("gear", IntegerType(), True),
        StructField("engine_rpm", IntegerType(), True),
        StructField("drs_state", StringType(), True),
        StructField("ers_mode", StringType(), True),
        StructField("battery_pct", FloatType(), True),
        StructField("fuel_mass_kg", FloatType(), True),
        StructField("tyre_compound", StringType(), True),
        StructField("tyre_age_laps", IntegerType(), True),
        StructField("tyre_surface_temp_c", FloatType(), True),
        StructField("tyre_inner_temp_c", FloatType(), True),
        StructField("tyre_pressure_bar", FloatType(), True),
        StructField("gps_lat", DoubleType(), True),
        StructField("gps_lon", DoubleType(), True),
        StructField("gps_alt", DoubleType(), True),
        StructField("track_status_code", StringType(), True),
        StructField("flag_state", StringType(), True),
        StructField("weather_air_temp_c", FloatType(), True),
        StructField("weather_track_temp_c", FloatType(), True),
        StructField("weather_humidity_pct", FloatType(), True),
        StructField("weather_wind_speed_kph", FloatType(), True),
        StructField("weather_wind_direction_deg", FloatType(), True),
        StructField("source", StringType(), True),
        StructField("ingest_id", StringType(), True),
    ]
)

# Race control and pit-stop events.
RACE_EVENT_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_ts_utc", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("driver_id", StringType(), True),
        StructField("lap_number", IntegerType(), True),
        StructField("pit_stop_id", StringType(), True),
        StructField("pit_duration_ms", LongType(), True),
        StructField("penalty_seconds", FloatType(), True),
        StructField("safety_car_state", StringType(), True),
        StructField("payload", StringType(), True),
        StructField("source", StringType(), True),
    ]
)

# Enriched lap-level metrics flowing into the gold/platinum stages.
LAP_METRICS_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("lap_number", IntegerType(), False),
        StructField("lap_time_ms", LongType(), True),
        StructField("sector1_ms", LongType(), True),
        StructField("sector2_ms", LongType(), True),
        StructField("sector3_ms", LongType(), True),
        StructField("avg_speed_kph", FloatType(), True),
        StructField("max_speed_kph", FloatType(), True),
        StructField("min_speed_kph", FloatType(), True),
        StructField("tyre_compound", StringType(), True),
        StructField("fuel_used_kg", FloatType(), True),
        StructField("battery_delta_pct", FloatType(), True),
        StructField("stint_id", StringType(), True),
        StructField("generated_at", TimestampType(), True),
        StructField("metadata", ArrayType(StringType()), True),
    ]
)

