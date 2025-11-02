#!/usr/bin/env python3
"""
Minimal FastF1 telemetry replay producer.

Fetches telemetry for a single session, converts each lap into JSON, and publishes
messages to an MSK topic using IAM authentication.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple

import fastf1
import pandas as pd
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import importlib
import os

importlib.import_module("kafka.vendor.six")
from kafka import KafkaProducer
from kafka.oauth.abstract import AbstractTokenProvider
from tqdm import tqdm


@dataclass
class LapEvent:
    session_key: str
    driver_number: int
    driver_code: str
    lap_number: int
    lap_time_ms: int
    sector_times_ms: Dict[str, int]
    pit_in: bool
    pit_out: bool
    track_status: str
    timestamp_utc: str


class IAMSaslTokenProvider(AbstractTokenProvider):
    """Adapter that generates tokens via the MSK IAM SASL signer."""

    def __init__(self, cluster_arn: str, region: str) -> None:
        self._cluster_arn = cluster_arn
        self._signer = MSKAuthTokenProvider(region_name=region)

    def token(self) -> Tuple[str, float, str, Dict[str, str]]:
        token, expiry = self._signer.get_token(cluster_arn=self._cluster_arn)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        lifetime_seconds = max(1.0, (expiry - now).total_seconds())
        return token, lifetime_seconds, "", {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay FastF1 telemetry to Kafka.")
    parser.add_argument(
        "--auth-mode",
        choices=["iam", "plain"],
        default="iam",
        help="Authentication mode for Kafka connection.",
    )
    parser.add_argument("--cluster-arn", help="MSK cluster ARN (required for iam).")
    parser.add_argument("--region", help="AWS region (required for iam).")
    parser.add_argument(
        "--bootstrap",
        required=True,
        help="Comma-separated bootstrap brokers.",
    )
    parser.add_argument(
        "--topic", default="telemetry.fastf1.raw", help="Kafka topic for telemetry."
    )
    parser.add_argument(
        "--season", type=int, required=True, help="F1 season year (e.g., 2023)."
    )
    parser.add_argument(
        "--event",
        required=True,
        help="Event name or round (e.g., 'Bahrain Grand Prix' or 1).",
    )
    parser.add_argument(
        "--session",
        required=True,
        help="Session code (P1, P2, P3, Q, R, SQ, SR).",
    )
    parser.add_argument(
        "--cache-dir",
        default=".fastf1-cache",
        help="Directory for FastF1 on-disk cache (default: .fastf1-cache).",
    )
    parser.add_argument(
        "--speedup",
        type=float,
        default=1.0,
        help="Playback speed multiplier (e.g., 30 for 30x faster).",
    )
    parser.add_argument(
        "--driver",
        help="Optional driver code/abbreviation to filter laps (e.g., VER).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse session but do not send messages.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum number of lap events to publish (0 = no limit).",
    )
    parser.add_argument(
        "--max-runtime",
        type=float,
        default=0.0,
        help="Maximum runtime in seconds before stopping (0 = no limit).",
    )
    return parser.parse_args()


def enable_fastf1_cache(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    fastf1.Cache.enable_cache(path)


def load_session(season: int, event: str, session_code: str) -> fastf1.core.Session:
    session = fastf1.get_session(season, event, session_code)
    session.load()
    return session


def resolve_session_start(session: fastf1.core.Session) -> datetime:
    event = session.event
    for key, value in event.items():
        if key.startswith("Session") and not key.endswith("DateUtc") and isinstance(value, str):
            if value.lower() == session.name.lower():
                date_key = f"{key}DateUtc"
                date_val = event.get(date_key)
                if pd.notnull(date_val):
                    start = pd.Timestamp(date_val).to_pydatetime()
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    return start.astimezone(timezone.utc)
    raise ValueError("Unable to resolve session start time.")


def laps_to_events(
    session: fastf1.core.Session, driver_filter: str | None = None
) -> Iterable[LapEvent]:
    laps: pd.DataFrame = session.laps
    if driver_filter:
        laps = laps.pick_driver(driver_filter)

    session_key = f"{session.event.year}-{session.event.EventName}-{session.name}"
    session_start = resolve_session_start(session)

    for _, lap in laps.iterlaps():  # type: ignore[attr-defined]
        lap_start_delta = lap["LapStartTime"]
        lap_start_utc = session_start + lap_start_delta

        event = LapEvent(
            session_key=session_key,
            driver_number=int(lap["DriverNumber"]),
            driver_code=str(lap["Driver"]),
            lap_number=int(lap["LapNumber"]),
            lap_time_ms=int(lap["LapTime"].total_seconds() * 1000)
            if pd.notnull(lap["LapTime"])
            else -1,
            sector_times_ms={
                "sector1": int(lap["Sector1Time"].total_seconds() * 1000)
                if pd.notnull(lap["Sector1Time"])
                else -1,
                "sector2": int(lap["Sector2Time"].total_seconds() * 1000)
                if pd.notnull(lap["Sector2Time"])
                else -1,
                "sector3": int(lap["Sector3Time"].total_seconds() * 1000)
                if pd.notnull(lap["Sector3Time"])
                else -1,
            },
            pit_in=bool(pd.notnull(lap["PitInTime"])),
            pit_out=bool(pd.notnull(lap["PitOutTime"])),
            track_status=str(lap.get("TrackStatus", "")),
            timestamp_utc=lap_start_utc.isoformat(),
        )
        yield event


def main() -> None:
    args = parse_args()
    enable_fastf1_cache(args.cache_dir)
    session = load_session(args.season, args.event, args.session)
    events = list(laps_to_events(session, args.driver))

    print(
        f"Loaded {len(events)} lap events from "
        f"{session.event.EventName} {session.name}"
    )

    if args.dry_run:
        print(json.dumps(asdict(events[0]), indent=2) if events else "No laps found.")
        return

    kafka_kwargs = {
        "bootstrap_servers": args.bootstrap,
        "value_serializer": lambda m: json.dumps(m).encode("utf-8"),
        "linger_ms": 0,
        "acks": "all",
        "retries": 5,
    }
    if args.auth_mode == "iam":
        if not args.cluster_arn or not args.region:
            raise SystemExit("cluster-arn and region are required when auth-mode is iam.")
        token_provider = IAMSaslTokenProvider(
            cluster_arn=args.cluster_arn, region=args.region
        )
        kafka_kwargs.update(
            {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "OAUTHBEARER",
                "sasl_oauth_token_provider": token_provider,
            }
        )
    else:
        kafka_kwargs.update({"security_protocol": "PLAINTEXT"})

    producer = KafkaProducer(**kafka_kwargs)

    try:
        sent = 0
        start_time = time.monotonic()
        for event in tqdm(events, desc="Publishing laps"):
            payload = asdict(event)
            producer.send(args.topic, value=payload)
            sent += 1
            if args.max_events and sent >= args.max_events:
                break
            if args.max_runtime and (time.monotonic() - start_time) >= args.max_runtime:
                break
            if event.lap_time_ms > 0 and args.speedup > 0:
                delay = max(0.0, (event.lap_time_ms / 1000.0) / args.speedup)
                time.sleep(delay)
        producer.flush()
    finally:
        producer.close()
    print(f"Completed publishing {sent} lap events.")


if __name__ == "__main__":
    main()
