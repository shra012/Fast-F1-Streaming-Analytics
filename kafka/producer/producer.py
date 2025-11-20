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
import hashlib
import json
import logging
import os
import random
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
    driver_name: Optional[str]
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
    # Streaming algorithm fields
    sensor_quality_score: Optional[float] = None
    sequence_number: Optional[int] = None
    sampling_weight: Optional[float] = None
    telemetry_hash: Optional[str] = None
    correlation_id: Optional[str] = None


@dataclass
class RaceEvent:
    """Race event matching RACE_EVENT_SCHEMA in spark/schemas.py."""
    session_id: str
    event_id: str
    event_ts_utc: str
    event_type: str
    driver_id: Optional[str] = None
    driver_name: Optional[str] = None
    lap_number: Optional[int] = None
    pit_stop_id: Optional[str] = None
    pit_duration_ms: Optional[int] = None
    penalty_seconds: Optional[float] = None
    safety_car_state: Optional[str] = None
    payload: Optional[str] = None
    source: str = "fastf1"
    # Streaming algorithm fields
    event_hash: Optional[str] = None
    interaction_strength: Optional[float] = None
    community_id: Optional[str] = None
    strategic_context: Optional[str] = None
    position_delta: Optional[int] = None
    sector_time_delta_ms: Optional[int] = None


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
        "--telemetry-sample-rate",
        type=int,
        default=1,
        help="Send every Nth telemetry point (e.g., 10 = 1/10th of data, reduces volume)",
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
            # Handle timedelta objects (sometimes FastF1 returns session duration instead of start time)
            if isinstance(session.session_start_time, pd.Timedelta):
                # Use event date as fallback for timedelta
                event = session.event
                start_time = pd.Timestamp(event.EventDate)
            else:
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
        # Silently fall back to current time - this is normal for some sessions
        return datetime.now(timezone.utc)


def extract_telemetry_events(
    session: fastf1.core.Session,
    driver_filter: Optional[str] = None,
    batch_size: int = 100,
    sample_rate: int = 1,
) -> Iterable[List[TelemetryEvent]]:
    """
    Extract telemetry events from session car data.
    
    Yields batches of telemetry events for efficient Kafka publishing.
    
    Args:
        sample_rate: Send every Nth telemetry point (1 = all data, 10 = 10% of data)
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
            driver_name = str(driver_info.get('Abbreviation', driver_info.get('FullName', driver)))
            
            # Get telemetry for this driver
            tel = driver_laps.get_telemetry()
            if tel is None or tel.empty:
                continue
            
            # Get driver's laps for tyre/stint info
            for idx, row in tel.iterrows():
                # Apply sampling rate (only process every Nth row)
                if sample_rate > 1 and idx % sample_rate != 0:
                    continue
                
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
                    driver_name=driver_name,
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
                    tyre_compound=str(matching_lap.iloc[0]['Compound']) if not matching_lap.empty and pd.notna(matching_lap.iloc[0]['Compound']) else None,
                    tyre_age_laps=int(matching_lap.iloc[0]['TyreLife']) if not matching_lap.empty and pd.notna(matching_lap.iloc[0]['TyreLife']) else None,
                    tyre_surface_temp_c=None,  # Not directly available
                    tyre_inner_temp_c=None,  # Not directly available
                    tyre_pressure_bar=None,  # Not available in FastF1
                    gps_lat=float(row['X']) if pd.notna(row.get('X')) else None,  # X/Y are track coordinates
                    gps_lon=float(row['Y']) if pd.notna(row.get('Y')) else None,
                    gps_alt=float(row['Z']) if pd.notna(row.get('Z')) else None,
                    track_status_code=str(matching_lap.iloc[0]['TrackStatus']) if not matching_lap.empty and 'TrackStatus' in matching_lap.columns and pd.notna(matching_lap.iloc[0]['TrackStatus']) else None,
                    flag_state=None,  # Not directly available
                    weather_air_temp_c=float(session.weather_data.iloc[-1]['AirTemp']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_track_temp_c=float(session.weather_data.iloc[-1]['TrackTemp']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_humidity_pct=float(session.weather_data.iloc[-1]['Humidity']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_wind_speed_kph=float(session.weather_data.iloc[-1]['WindSpeed']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    weather_wind_direction_deg=float(session.weather_data.iloc[-1]['WindDirection']) if hasattr(session, 'weather_data') and not session.weather_data.empty else None,
                    # Streaming algorithm fields
                    sensor_quality_score=random.uniform(0.85, 1.0),  # Simulated sensor quality
                    sequence_number=idx,  # Row index as sequence number
                    sampling_weight=1.0 / (idx + 1) if idx > 0 else 1.0,  # Reservoir sampling weight
                    telemetry_hash=hashlib.md5(f"{session_id}_{driver}_{idx}_{lap_time}".encode()).hexdigest(),  # Hash for HyperLogLog
                    correlation_id=f"{session_id}_{driver}_{lap_number}",  # Link telemetry to lap
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


def detect_overtakes(session: fastf1.core.Session, session_id: str, session_start: datetime) -> Iterable[RaceEvent]:
    """
    Detect overtake events by analyzing position changes between laps.
    
    An overtake is detected when a driver improves their position from one lap to the next.
    """
    laps = session.laps
    
    # Group laps by lap number to compare positions
    for lap_num in sorted(laps['LapNumber'].unique()):
        current_lap_data = laps[laps['LapNumber'] == lap_num].copy()
        previous_lap_data = laps[laps['LapNumber'] == lap_num - 1].copy()
        
        if previous_lap_data.empty:
            continue
        
        # Create position lookup for previous lap
        prev_positions = {}
        for idx, lap in previous_lap_data.iterrows():
            if pd.notna(lap.get('Position')) and pd.notna(lap.get('Driver')):
                prev_positions[str(lap['Driver'])] = int(lap['Position'])
        
        # Check current lap for position improvements
        for idx, lap in current_lap_data.iterrows():
            driver = str(lap['Driver'])
            curr_pos = lap.get('Position')
            
            if pd.isna(curr_pos) or driver not in prev_positions:
                continue
            
            curr_pos = int(curr_pos)
            prev_pos = prev_positions[driver]
            
            # Position improved (lower number = better position)
            if curr_pos < prev_pos:
                positions_gained = prev_pos - curr_pos
                
                # Find who was overtaken (drivers who lost positions)
                overtaken_drivers = []
                for other_driver, other_prev_pos in prev_positions.items():
                    if other_driver == driver:
                        continue
                    
                    # Check if this driver is now behind
                    other_current = current_lap_data[current_lap_data['Driver'] == other_driver]
                    if not other_current.empty:
                        other_curr_pos = other_current.iloc[0].get('Position')
                        if pd.notna(other_curr_pos):
                            other_curr_pos = int(other_curr_pos)
                            # If they were ahead before and behind now, they were overtaken
                            if other_prev_pos < prev_pos and other_curr_pos > curr_pos:
                                overtaken_drivers.append(other_driver)
                
                # Create overtake event for each overtaken driver
                for defender in overtaken_drivers:
                    lap_time = lap.get('Time')
                    if pd.isna(lap_time):
                        continue
                    
                    event_time = session_start + lap_time
                    
                    # Calculate time gap if telemetry is available
                    avg_gap_ms = None
                    delta_time_ms = None
                    try:
                        attacker_lap_time = lap.get('LapTime')
                        defender_lap_row = current_lap_data[current_lap_data['Driver'] == defender]
                        if not defender_lap_row.empty and pd.notna(attacker_lap_time):
                            defender_lap_time = defender_lap_row.iloc[0].get('LapTime')
                            if pd.notna(defender_lap_time):
                                delta_time_ms = int((defender_lap_time - attacker_lap_time).total_seconds() * 1000)
                                avg_gap_ms = abs(delta_time_ms)
                    except Exception:
                        pass
                    
                    event = RaceEvent(
                        session_id=session_id,
                        event_id=f"{session_id}_overtake_{driver}_{defender}_{lap_num}",
                        event_ts_utc=event_time.isoformat(),
                        event_type="OVERTAKE",
                        driver_id=driver,
                        driver_name=get_driver_name(session, driver),
                        lap_number=int(lap_num),
                        payload=json.dumps({
                            'attacker_id': driver,
                            'defender_id': defender,
                            'lap_number': int(lap_num),
                            'lap_count': 1,  # Single overtake event
                            'avg_gap_ms': avg_gap_ms,
                            'delta_time_ms': delta_time_ms,
                            'notes': f"{driver} overtook {defender} on lap {lap_num}"
                        }),
                        event_hash=hashlib.md5(f"{session_id}_overtake_{driver}_{defender}_{lap_num}".encode()).hexdigest(),
                        interaction_strength=3.0,  # High strength for overtakes
                        community_id=None,
                        strategic_context=json.dumps({'type': 'overtake', 'positions_gained': positions_gained}),
                        position_delta=positions_gained,
                        sector_time_delta_ms=delta_time_ms,
                    )
                    yield event


def detect_battles(session: fastf1.core.Session, session_id: str, session_start: datetime) -> Iterable[RaceEvent]:
    """
    Detect battle events by analyzing drivers who stay close in position over multiple laps.
    
    A battle is detected when two drivers swap positions or stay within 1 position for 2+ consecutive laps.
    """
    laps = session.laps
    
    # Track position battles across laps
    battles = {}  # (driver_a, driver_b) -> [lap_numbers]
    
    for lap_num in sorted(laps['LapNumber'].unique()):
        current_lap_data = laps[laps['LapNumber'] == lap_num].copy()
        
        # Get all drivers and their positions this lap
        positions = {}
        for idx, lap in current_lap_data.iterrows():
            if pd.notna(lap.get('Position')) and pd.notna(lap.get('Driver')):
                positions[str(lap['Driver'])] = (int(lap['Position']), lap)
        
        # Find drivers close to each other (within 1 position)
        sorted_drivers = sorted(positions.items(), key=lambda x: x[1][0])
        
        for i in range(len(sorted_drivers) - 1):
            driver_a, (pos_a, lap_a) = sorted_drivers[i]
            driver_b, (pos_b, lap_b) = sorted_drivers[i + 1]
            
            # Only consider adjacent positions or swaps
            if abs(pos_b - pos_a) <= 1:
                # Create a canonical pair key (alphabetically sorted)
                pair = tuple(sorted([driver_a, driver_b]))
                
                if pair not in battles:
                    battles[pair] = []
                battles[pair].append((lap_num, driver_a, driver_b, pos_a, pos_b, lap_a, lap_b))
    
    # Generate battle events for pairs that battled for 2+ consecutive laps
    for (driver_a, driver_b), battle_laps in battles.items():
        if len(battle_laps) < 2:
            continue
        
        # Check for consecutive laps
        consecutive_groups = []
        current_group = [battle_laps[0]]
        
        for i in range(1, len(battle_laps)):
            prev_lap_num = battle_laps[i-1][0]
            curr_lap_num = battle_laps[i][0]
            
            if curr_lap_num == prev_lap_num + 1:
                current_group.append(battle_laps[i])
            else:
                if len(current_group) >= 2:
                    consecutive_groups.append(current_group)
                current_group = [battle_laps[i]]
        
        if len(current_group) >= 2:
            consecutive_groups.append(current_group)
        
        # Generate battle events for each consecutive group
        for group in consecutive_groups:
            first_lap = group[0]
            last_lap = group[-1]
            
            lap_num = first_lap[0]
            lap_count = len(group)
            
            # Use the first lap's data for timing
            lap_a = first_lap[5]
            lap_time = lap_a.get('Time')
            
            if pd.isna(lap_time):
                continue
            
            event_time = session_start + lap_time
            
            # Calculate average gap
            avg_gap_ms = None
            delta_time_ms = None
            try:
                lap_time_a = lap_a.get('LapTime')
                lap_b = first_lap[6]
                lap_time_b = lap_b.get('LapTime')
                
                if pd.notna(lap_time_a) and pd.notna(lap_time_b):
                    delta_time_ms = int((lap_time_b - lap_time_a).total_seconds() * 1000)
                    avg_gap_ms = abs(delta_time_ms)
            except Exception:
                pass
            
            event = RaceEvent(
                session_id=session_id,
                event_id=f"{session_id}_battle_{driver_a}_{driver_b}_{lap_num}",
                event_ts_utc=event_time.isoformat(),
                event_type="BATTLE",
                driver_id=driver_a,
                driver_name=get_driver_name(session, driver_a),
                lap_number=int(lap_num),
                payload=json.dumps({
                    'driver_a_id': driver_a,
                    'driver_b_id': driver_b,
                    'lap_number': int(lap_num),
                    'lap_count': lap_count,
                    'avg_gap_ms': avg_gap_ms,
                    'delta_time_ms': delta_time_ms,
                    'battle_type': 'position_battle',
                    'notes': f"{driver_a} and {driver_b} battled for {lap_count} laps starting at lap {lap_num}"
                }),
                event_hash=hashlib.md5(f"{session_id}_battle_{driver_a}_{driver_b}_{lap_num}".encode()).hexdigest(),
                interaction_strength=2.5,  # High strength for battles
                community_id=None,
                strategic_context=json.dumps({'type': 'battle', 'lap_count': lap_count}),
                position_delta=None,
                sector_time_delta_ms=delta_time_ms,
            )
            yield event


def get_driver_name(session: fastf1.core.Session, driver_id: str) -> Optional[str]:
    """Get driver name from session."""
    try:
        driver_info = session.get_driver(driver_id)
        return str(driver_info.get('Abbreviation', driver_info.get('FullName', driver_id)))
    except:
        return driver_id


def extract_race_events(session: fastf1.core.Session) -> Iterable[RaceEvent]:
    """
    Extract race events from session data.
    
    Includes: lap completions, pit stops, overtakes, and battles.
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
        driver_id = str(lap['Driver'])
        
        # Lap completion event
        event = RaceEvent(
            session_id=session_id,
            event_id=f"{session_id}_lap_{driver_id}_{lap['LapNumber']}",
            event_ts_utc=event_time.isoformat(),
            event_type="LAP_COMPLETION",
            driver_id=driver_id,
            driver_name=get_driver_name(session, driver_id),
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
            }),
            # Streaming algorithm fields
            event_hash=hashlib.md5(f"{session_id}_lap_{lap['Driver']}_{lap['LapNumber']}".encode()).hexdigest(),
            interaction_strength=1.0,  # Default strength
            community_id=None,  # Will be computed in gold layer
            strategic_context=None,  # No strategic data for lap completion
            position_delta=int(lap.get('Position', 0)) if pd.notna(lap.get('Position')) else None,
            sector_time_delta_ms=None,  # Will be computed in gold layer
        )
        yield event
        
        # Pit stop events
        if pd.notna(lap.get('PitInTime')):
            pit_in_time = session_start + lap['PitInTime']
            pit_out_time = session_start + lap['PitOutTime'] if pd.notna(lap.get('PitOutTime')) else pit_in_time
            pit_duration_ms = int((pit_out_time - pit_in_time).total_seconds() * 1000)
            pit_driver_id = str(lap['Driver'])
            
            pit_event = RaceEvent(
                session_id=session_id,
                event_id=f"{session_id}_pit_{pit_driver_id}_{lap['LapNumber']}",
                event_ts_utc=pit_in_time.isoformat(),
                event_type="PIT_STOP",
                driver_id=pit_driver_id,
                driver_name=get_driver_name(session, pit_driver_id),
                lap_number=int(lap['LapNumber']),
                pit_stop_id=f"pit_{lap['Driver']}_{lap['Stint']}",
                pit_duration_ms=pit_duration_ms,
                payload=json.dumps({
                    'compound_before': str(lap.get('Compound', '')),
                    'pit_in_time': pit_in_time.isoformat(),
                    'pit_out_time': pit_out_time.isoformat(),
                }),
                # Streaming algorithm fields
                event_hash=hashlib.md5(f"{session_id}_pit_{lap['Driver']}_{lap['LapNumber']}".encode()).hexdigest(),
                interaction_strength=2.0,  # Higher strength for pit events (strategic importance)
                community_id=None,
                strategic_context=json.dumps({'type': 'pit_stop', 'duration_ms': pit_duration_ms}),
                position_delta=None,
                sector_time_delta_ms=None,
            )
            yield pit_event
    
    # Extract overtake events
    logger.info(f"Detecting overtakes for {session_id}")
    overtake_count = 0
    for event in detect_overtakes(session, session_id, session_start):
        overtake_count += 1
        yield event
    logger.info(f"Detected {overtake_count} overtake events")
    
    # Extract battle events
    logger.info(f"Detecting battles for {session_id}")
    battle_count = 0
    for event in detect_battles(session, session_id, session_start):
        battle_count += 1
        yield event
    logger.info(f"Detected {battle_count} battle events")


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
        logger.info(f"Started processing session: {session.event.EventName} - {session.name}")
        session.load()
        
        session_id = get_session_id(session)
        logger.info(f"Session loaded successfully: {session_id}")
        
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
                        
                        # Log progress every 50 events
                        if stats['events'] % 50 == 0:
                            logger.info(f"Sent {stats['events']} race event messages")
                    except KafkaError as e:
                        logger.error(f"Kafka error publishing race event: {e}")
                        stats['errors'] += 1
                
                if args.max_events and stats['events'] >= args.max_events:
                    break
            
            if stats['events'] > 0:
                logger.info(f"Completed race events: {stats['events']} messages sent")
        
        # Publish telemetry
        if not args.skip_telemetry:
            logger.info(f"Extracting telemetry from {session_id} (sample rate: 1/{args.telemetry_sample_rate})")
            batch_count = 0
            for batch in extract_telemetry_events(session, args.driver, args.batch_size, args.telemetry_sample_rate):
                if args.dry_run:
                    if stats['telemetry'] == 0:  # Print first event as sample
                        logger.info(f"Sample telemetry event: {json.dumps(asdict(batch[0]), indent=2, default=str)}")
                else:
                    batch_start = stats['telemetry']
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
                    
                    batch_count += 1
                    batch_size = stats['telemetry'] - batch_start
                    
                    # Log progress every 10 batches or if last batch
                    if batch_count % 10 == 0:
                        logger.info(f"Sent {stats['telemetry']} telemetry messages ({batch_count} batches)")
                    
                    # Add delay based on speedup
                    if args.speedup > 0:
                        time.sleep((batch_size * 0.01) / args.speedup)
                
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
