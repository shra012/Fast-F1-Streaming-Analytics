#!/usr/bin/env python3
"""
Comprehensive FastF1 Data Producer for Kafka.

Streams telemetry and race event data from F1 sessions to Kafka topics.
Supports multiple seasons, events, and sessions with configurable playback speed.

Topics produced:
- telemetry.raw: Detailed car telemetry (speed, throttle, GPS, etc.)
- race.events: Race control events (pit stops, flags, penalties, etc.)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import fastf1
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tqdm import tqdm

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TelemetryEvent:
    """Telemetry event matching TELEMETRY_SCHEMA in spark/schemas.py."""
    session_id: str
    driver_id: str
    car_number: str
    lap_number: int
    micro_sector_id: Optional[int]
    timestamp_utc: str
    speed_kph: Optional[float]
    throttle_pct: Optional[float]
    brake_pressure_bar: Optional[float]
    steering_angle_deg: Optional[float]
    gear: Optional[int]
    engine_rpm: Optional[int]
    drs_state: Optional[str]
    ers_mode: Optional[str]
    battery_pct: Optional[float]
    fuel_mass_kg: Optional[float]
    tyre_compound: Optional[str]
    tyre_age_laps: Optional[int]
    tyre_surface_temp_c: Optional[float]
    tyre_inner_temp_c: Optional[float]
    tyre_pressure_bar: Optional[float]
    gps_lat: Optional[float]
    gps_lon: Optional[float]
    gps_alt: Optional[float]
    track_status_code: Optional[str]
    flag_state: Optional[str]
    weather_air_temp_c: Optional[float]
    weather_track_temp_c: Optional[float]
    weather_humidity_pct: Optional[float]
    weather_wind_speed_kph: Optional[float]
    weather_wind_direction_deg: Optional[float]
    source: str = "fastf1"
    ingest_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class RaceEvent:
    """Race event matching RACE_EVENT_SCHEMA in spark/schemas.py."""
    session_id: str
    event_id: str
    event_ts_utc: str
    event_type: str
    driver_id: Optional[str] = None
    lap_number: Optional[int] = None
    pit_stop_id: Optional[str] = None
    pit_duration_ms: Optional[int] = None
    penalty_seconds: Optional[float] = None
    safety_car_state: Optional[str] = None
    payload: Optional[str] = None
    source: str = "fastf1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Comprehensive F1 data producer for Kafka.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stream all 2024 data at 30x speed
  ./comprehensive_producer.py --start-year 2024 --speedup 30

  # Stream 2020-2024 data for specific event
  ./comprehensive_producer.py --start-year 2020 --end-year 2024 --event "Monaco"

  # Dry run to see what would be produced
  ./comprehensive_producer.py --start-year 2023 --dry-run
        """
    )

    # Kafka connection
    env_bootstrap = os.getenv("KAFKA_BOOTSTRAP_BROKERS", os.getenv("MSK_BOOTSTRAP_BROKERS"))
    parser.add_argument(
        "--bootstrap",
        default=env_bootstrap,
        required=env_bootstrap is None,
        help="Comma-separated Kafka bootstrap brokers (default: env KAFKA_BOOTSTRAP_BROKERS)",
    )
    parser.add_argument(
        "--telemetry-topic",
        default=os.getenv("TELEMETRY_TOPIC", "telemetry.raw"),
        help="Kafka topic for telemetry data (default: telemetry.raw)",
    )
    parser.add_argument(
        "--events-topic",
        default=os.getenv("EVENTS_TOPIC", "race.events"),
        help="Kafka topic for race events (default: race.events)",
    )

    # Data selection
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Starting season year (default: 2020)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Ending season year (default: current year)",
    )
    parser.add_argument(
        "--event",
        help="Specific event name or round number (e.g., 'Monaco' or '5'). If omitted, processes all events.",
    )
    parser.add_argument(
        "--session",
        choices=["FP1", "FP2", "FP3", "Q", "SQ", "S", "R", "SS"],
        help="Specific session type. If omitted, processes all sessions (FP1, FP2, FP3, Q, Sprint, Race).",
    )
    parser.add_argument(
        "--driver",
        help="Filter telemetry for specific driver code (e.g., VER, HAM, LEC)",
    )

    # Producer behavior
    parser.add_argument(
        "--cache-dir",
        default=os.getenv("FASTF1_CACHE_DIR", ".fastf1-cache"),
        help="FastF1 cache directory (default: .fastf1-cache)",
    )
    parser.add_argument(
        "--speedup",
        type=float,
        default=1.0,
        help="Playback speed multiplier (e.g., 30 for 30x faster, 0 for no delay)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of telemetry samples per batch (default: 100)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum number of events to publish per session (0 = unlimited)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse data but don't send to Kafka",
    )
    parser.add_argument(
        "--skip-telemetry",
        action="store_true",
        help="Skip telemetry data, only produce race events",
    )
    parser.add_argument(
        "--skip-events",
        action="store_true",
        help="Skip race events, only produce telemetry",
    )

    return parser.parse_args()


def enable_fastf1_cache(cache_dir: str) -> None:
    """Enable FastF1 on-disk cache."""
    os.makedirs(cache_dir, exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)
    logger.info(f"FastF1 cache enabled at: {cache_dir}")


def get_session_id(session: fastf1.core.Session) -> str:
    """Generate unique session identifier."""
    year = session.event.year
    event_name = session.event.EventName.replace(" ", "_")
    session_name = session.name.replace(" ", "_")
    return f"{year}_{event_name}_{session_name}"


def get_session_start_time(session: fastf1.core.Session) -> datetime:
    """Extract session start time with timezone."""
    try:
        # Try to get session start time from session data
        if hasattr(session, 'session_start_time') and pd.notna(session.session_start_time):
            start_time = pd.Timestamp(session.session_start_time)
        else:
            # Fallback to event schedule
            event = session.event
            session_key = f"Session{session.name.replace(' ', '')}DateUtc"
            if session_key in event and pd.notna(event[session_key]):
                start_time = pd.Timestamp(event[session_key])
            else:
                # Last resort: use event date
                start_time = pd.Timestamp(event.EventDate)
        
        # Ensure timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.tz_localize('UTC')
        
        return start_time.to_pydatetime()
    except Exception as e:
        logger.warning(f"Could not determine session start time: {e}. Using current time.")
        return datetime.now(timezone.utc)


def extract_telemetry_events(
    session: fastf1.core.Session,
    driver_filter: Optional[str] = None,
    batch_size: int = 100,
) -> Iterable[List[TelemetryEvent]]:
    """
    Extract telemetry events from session car data.
    
    Yields batches of telemetry events for efficient Kafka publishing.
    """
    session_id = get_session_id(session)
    
    # Get laps for context
    laps = session.laps
    if driver_filter:
        laps = laps.pick_driver(driver_filter)
    
    if laps.empty:
        logger.warning(f"No laps found for session {session_id}")
        return
    
    # Get telemetry data for all drivers or filtered driver
    drivers = [driver_filter] if driver_filter else session.drivers
    
    batch = []
    for driver in drivers:
        try:
            driver_laps = session.laps.pick_driver(driver)
            if driver_laps.empty:
                continue
            
            driver_info = session.get_driver(driver)
            driver_number = str(driver_info.get('DriverNumber', driver))
            
            # Get telemetry for this driver
            tel = driver_laps.get_telemetry()
            if tel is None or tel.empty:
                continue
            
            # Get driver's laps for tyre/stint info
            for idx, row in tel.iterrows():
                # Find corresponding lap
                lap_time = row.get('Time', row.get('SessionTime'))
                if pd.isna(lap_time):
                    continue
                
                # Match to lap number
                matching_lap = driver_laps[driver_laps['Time'] >= lap_time].head(1)
                lap_number = int(matching_lap.iloc[0]['LapNumber']) if not matching_lap.empty else 0
                
                # Create telemetry event
                event_time = get_session_start_time(session) + lap_time
                
                event = TelemetryEvent(
                    session_id=session_id,
                    driver_id=driver,
                    car_number=driver_number,
                    lap_number=lap_number,
                    micro_sector_id=None,  # Not available in FastF1
                    timestamp_utc=event_time.isoformat(),
                    speed_kph=float(row['Speed']) if pd.notna(row.get('Speed')) else None,
                    throttle_pct=float(row['Throttle']) if pd.notna(row.get('Throttle')) else None,
                    brake_pressure_bar=float(row['Brake']) if pd.notna(row.get('Brake')) else None,
                    steering_angle_deg=None,  # Not directly available
                    gear=int(row['nGear']) if pd.notna(row.get('nGear')) else None,
                    engine_rpm=int(row['RPM']) if pd.notna(row.get('RPM')) else None,
                    drs_state=str(row['DRS']) if pd.notna(row.get('DRS')) else None,
                    ers_mode=None,  # Not available in FastF1
                    battery_pct=None,  # Not available in FastF1
                    fuel_mass_kg=None,  # Not available in FastF1
                    tyre_compound=str(matching_lap.iloc[0]['Compound']) if not matching_lap.empty and pd.notna(matching_lap.iloc[0].get('Compound')) else None,
                    tyre_age_laps=int(matching_lap.iloc[0]['TyreLife']) if not matching_lap.empty and pd.notna(matching_lap.iloc[0].get('TyreLife')) else None,
                    tyre_surface_temp_c=None,  # Not directly available
                    tyre_inner_temp_c=None,  # Not directly available
                    tyre_pressure_bar=None,  # Not available in FastF1
                    gps_lat=float(row['X']) if pd.notna(row.get('X')) else None,  # X/Y are track coordinates
                    gps_lon=float(row['Y']) if pd.notna(row.get('Y')) else None,
                    gps_alt=float(row['Z']) if pd.notna(row.get('Z')) else None,
                    track_status_code=str(matching_lap.iloc[0].get('TrackStatus', '')) if not matching_lap.empty else None,
                    flag_state=None,  # Not directly available
                    weather_air_temp_c=float(session.weather_data.iloc[-1]['AirTemp']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_track_temp_c=float(session.weather_data.iloc[-1]['TrackTemp']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_humidity_pct=float(session.weather_data.iloc[-1]['Humidity']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_wind_speed_kph=float(session.weather_data.iloc[-1]['WindSpeed']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_wind_direction_deg=float(session.weather_data.iloc[-1]['WindDirection']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                )
                
                batch.append(event)
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        
        except Exception as e:
            logger.error(f"Error extracting telemetry for driver {driver}: {e}")
            continue
    
    # Yield remaining batch
    if batch:
        yield batch


def extract_race_events(session: fastf1.core.Session) -> Iterable[RaceEvent]:
    """
    Extract race events from session data.
    
    Includes: lap completions, pit stops, and any available race control messages.
    """
    session_id = get_session_id(session)
    session_start = get_session_start_time(session)
    
    # Extract lap completion events
    laps = session.laps
    for idx, lap in laps.iterrows():
        lap_time = lap.get('Time')
        if pd.isna(lap_time):
            continue
        
        event_time = session_start + lap_time
        
        # Lap completion event
        event = RaceEvent(
            session_id=session_id,
            event_id=f"{session_id}_lap_{lap['Driver']}_{lap['LapNumber']}",
            event_ts_utc=event_time.isoformat(),
            event_type="LAP_COMPLETION",
            driver_id=str(lap['Driver']),
            lap_number=int(lap['LapNumber']),
            payload=json.dumps({
                'lap_time_ms': int(lap['LapTime'].total_seconds() * 1000) if pd.notna(lap['LapTime']) else None,
                'sector1_ms': int(lap['Sector1Time'].total_seconds() * 1000) if pd.notna(lap.get('Sector1Time')) else None,
                'sector2_ms': int(lap['Sector2Time'].total_seconds() * 1000) if pd.notna(lap.get('Sector2Time')) else None,
                'sector3_ms': int(lap['Sector3Time'].total_seconds() * 1000) if pd.notna(lap.get('Sector3Time')) else None,
                'compound': str(lap['Compound']) if pd.notna(lap.get('Compound')) else None,
                'tyre_life': int(lap['TyreLife']) if pd.notna(lap.get('TyreLife')) else None,
                'stint': int(lap['Stint']) if pd.notna(lap.get('Stint')) else None,
                'fresh_tyre': bool(lap.get('FreshTyre', False)),
            })
        )
        yield event
        
        # Pit stop events
        if pd.notna(lap.get('PitInTime')):
            pit_in_time = session_start + lap['PitInTime']
            pit_out_time = session_start + lap['PitOutTime'] if pd.notna(lap.get('PitOutTime')) else pit_in_time
            pit_duration_ms = int((pit_out_time - pit_in_time).total_seconds() * 1000)
            
            pit_event = RaceEvent(
                session_id=session_id,
                event_id=f"{session_id}_pit_{lap['Driver']}_{lap['LapNumber']}",
                event_ts_utc=pit_in_time.isoformat(),
                event_type="PIT_STOP",
                driver_id=str(lap['Driver']),
                lap_number=int(lap['LapNumber']),
                pit_stop_id=f"pit_{lap['Driver']}_{lap['Stint']}",
                pit_duration_ms=pit_duration_ms,
                payload=json.dumps({
                    'compound_before': str(lap.get('Compound', '')),
                    'pit_in_time': pit_in_time.isoformat(),
                    'pit_out_time': pit_out_time.isoformat(),
                })
            )
            yield pit_event


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create Kafka producer with PLAINTEXT authentication."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(','),
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=5,
        max_in_flight_requests_per_connection=5,
        compression_type='gzip',
        linger_ms=10,
        batch_size=32768,
        security_protocol='PLAINTEXT',
    )


def process_session(
    producer: Optional[KafkaProducer],
    session: fastf1.core.Session,
    args: argparse.Namespace,
) -> Dict[str, int]:
    """
    Process a single F1 session and publish to Kafka.
    
    Returns dict with counts of telemetry and event messages published.
    """
    stats = {'telemetry': 0, 'events': 0, 'errors': 0}
    
    try:
        logger.info(f"Loading session: {session.event.EventName} - {session.name}")
        session.load()
        
        session_id = get_session_id(session)
        
        # Publish race events
        if not args.skip_events:
            logger.info(f"Extracting race events from {session_id}")
            for event in extract_race_events(session):
                if args.dry_run:
                    if stats['events'] == 0:  # Print first event as sample
                        logger.info(f"Sample race event: {json.dumps(asdict(event), indent=2, default=str)}")
                else:
                    try:
                        producer.send(
                            args.events_topic,
                            key=event.event_id,
                            value=asdict(event)
                        )
                        stats['events'] += 1
                    except KafkaError as e:
                        logger.error(f"Kafka error publishing race event: {e}")
                        stats['errors'] += 1
                
                if args.max_events and stats['events'] >= args.max_events:
                    break
        
        # Publish telemetry
        if not args.skip_telemetry:
            logger.info(f"Extracting telemetry from {session_id}")
            for batch in extract_telemetry_events(session, args.driver, args.batch_size):
                if args.dry_run:
                    if stats['telemetry'] == 0:  # Print first event as sample
                        logger.info(f"Sample telemetry event: {json.dumps(asdict(batch[0]), indent=2, default=str)}")
                else:
                    for event in batch:
                        try:
                            producer.send(
                                args.telemetry_topic,
                                key=f"{event.driver_id}_{event.lap_number}",
                                value=asdict(event)
                            )
                            stats['telemetry'] += 1
                        except KafkaError as e:
                            logger.error(f"Kafka error publishing telemetry: {e}")
                            stats['errors'] += 1
                    
                    # Add delay based on speedup
                    if args.speedup > 0:
                        time.sleep((len(batch) * 0.01) / args.speedup)
                
                if args.max_events and stats['telemetry'] >= args.max_events:
                    break
        
        if not args.dry_run:
            producer.flush()
        
        logger.info(f"Session {session_id} complete: {stats['telemetry']} telemetry, {stats['events']} events, {stats['errors']} errors")
        
    except Exception as e:
        logger.error(f"Error processing session {session.event.EventName} - {session.name}: {e}")
        stats['errors'] += 1
    
    return stats


def main() -> None:
    """Main entry point."""
    args = parse_args()
    
    # Validate arguments
    if args.skip_telemetry and args.skip_events:
        logger.error("Cannot skip both telemetry and events")
        sys.exit(1)
    
    # Setup FastF1 cache
    enable_fastf1_cache(args.cache_dir)
    
    # Determine year range
    end_year = args.end_year or datetime.now().year
    years = range(args.start_year, end_year + 1)
    
    logger.info(f"Processing seasons: {args.start_year} to {end_year}")
    logger.info(f"Topics: telemetry={args.telemetry_topic}, events={args.events_topic}")
    logger.info(f"Speedup: {args.speedup}x")
    
    # Create Kafka producer
    producer = None
    if not args.dry_run:
        if not args.bootstrap:
            logger.error("Bootstrap servers required. Set --bootstrap or KAFKA_BOOTSTRAP_BROKERS env var")
            sys.exit(1)
        producer = create_kafka_producer(args.bootstrap)
        logger.info(f"Kafka producer connected to: {args.bootstrap}")
    
    # Global statistics
    total_stats = {'telemetry': 0, 'events': 0, 'errors': 0, 'sessions': 0}
    
    try:
        for year in years:
            logger.info(f"\n{'='*80}\nProcessing season {year}\n{'='*80}")
            
            try:
                # Get event schedule
                schedule = fastf1.get_event_schedule(year)
                
                # Filter events if specified
                if args.event:
                    try:
                        event_round = int(args.event)
                        schedule = schedule[schedule['RoundNumber'] == event_round]
                    except ValueError:
                        schedule = schedule[schedule['EventName'].str.contains(args.event, case=False, na=False)]
                
                if schedule.empty:
                    logger.warning(f"No events found for year {year}")
                    continue
                
                # Process each event
                for _, event_info in schedule.iterrows():
                    event_name = event_info['EventName']
                    round_num = event_info['RoundNumber']
                    
                    logger.info(f"\nEvent {round_num}: {event_name}")
                    
                    # Determine sessions to process
                    session_types = [args.session] if args.session else ['FP1', 'FP2', 'FP3', 'Q', 'S', 'R']
                    
                    for session_type in session_types:
                        try:
                            session = fastf1.get_session(year, round_num, session_type)
                            
                            # Check if session exists
                            if session is None:
                                continue
                            
                            stats = process_session(producer, session, args)
                            total_stats['telemetry'] += stats['telemetry']
                            total_stats['events'] += stats['events']
                            total_stats['errors'] += stats['errors']
                            total_stats['sessions'] += 1
                            
                        except Exception as e:
                            logger.warning(f"Could not load session {session_type}: {e}")
                            continue
            
            except Exception as e:
                logger.error(f"Error processing year {year}: {e}")
                continue
    
    finally:
        if producer:
            producer.close()
        
        # Print summary
        logger.info(f"\n{'='*80}")
        logger.info("SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Sessions processed: {total_stats['sessions']}")
        logger.info(f"Telemetry messages: {total_stats['telemetry']}")
        logger.info(f"Race event messages: {total_stats['events']}")
        logger.info(f"Errors: {total_stats['errors']}")
        logger.info(f"{'='*80}")


if __name__ == "__main__":
    main()
