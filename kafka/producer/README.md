# F1 Kafka Producer

Comprehensive producer for streaming F1 telemetry and race event data to Kafka topics.

## Architecture

```
FastF1 API → producer.py → MSK Kafka
                              ├─ telemetry.raw
                              └─ race.events
                                      ↓
                              Bronze Spark Job
```

## Features

- Streams data for **multiple seasons** (2020+)
- Produces to **two topics**:
  - `telemetry.raw` - Detailed car telemetry
  - `race.events` - Race control events (laps, pit stops)
- **Matches Spark schemas** exactly (from `spark/schemas.py`)
- Configurable playback speed (1x to 100x+)
- Session filtering (specific events, drivers)
- Batch processing for efficiency
- PLAINTEXT authentication (MSK compatible)

## Data Produced

### Telemetry (`telemetry.raw`)
Per-sample car data including:
- Speed, throttle, brake, gear, RPM
- DRS state
- GPS coordinates (track position)
- Tyre compound and age
- Weather conditions
- Track status

### Race Events (`race.events`)
Event-based data including:
- Lap completions with sector times
- Pit stops with duration
- Tyre strategy (compound, stint)

## Requirements

```bash
pip install fastf1 kafka-python python-dotenv pandas tqdm
```

## Usage

### Stream All 2024 Data
```bash
./producer.py \
  --bootstrap b-1.msk.amazonaws.com:9092,b-2.msk.amazonaws.com:9092,b-3.msk.amazonaws.com:9092 \
  --start-year 2024 \
  --speedup 30
```

### Stream 2020-2024 Data
```bash
./producer.py \
  --start-year 2020 \
  --end-year 2024 \
  --speedup 50
```

### Stream Specific Event
```bash
./producer.py \
  --start-year 2024 \
  --event "Monaco" \
  --speedup 10
```

### Stream Single Driver
```bash
./producer.py \
  --start-year 2024 \
  --driver VER \
  --speedup 20
```

### Dry Run (No Kafka)
```bash
./producer.py \
  --start-year 2024 \
  --event "Bahrain" \
  --dry-run
```

### Only Telemetry or Only Events
```bash
# Only telemetry
./producer.py --start-year 2024 --skip-events

# Only race events
./producer.py --start-year 2024 --skip-telemetry
```

## Environment Variables

Create a `.env` file:

```bash
# Kafka connection
KAFKA_BOOTSTRAP_BROKERS=b-1.msk.amazonaws.com:9092,b-2.msk.amazonaws.com:9092,b-3.msk.amazonaws.com:9092

# Topics
TELEMETRY_TOPIC=telemetry.raw
EVENTS_TOPIC=race.events

# FastF1 cache
FASTF1_CACHE_DIR=.fastf1-cache
```

## Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--bootstrap` | Kafka bootstrap servers | From env |
| `--telemetry-topic` | Topic for telemetry | `telemetry.raw` |
| `--events-topic` | Topic for race events | `race.events` |
| `--start-year` | Starting season | `2020` |
| `--end-year` | Ending season | Current year |
| `--event` | Specific event name or round | All events |
| `--session` | Session type (FP1/FP2/FP3/Q/S/R) | All sessions |
| `--driver` | Filter by driver code | All drivers |
| `--speedup` | Playback speed multiplier | `1.0` |
| `--batch-size` | Telemetry samples per batch | `100` |
| `--cache-dir` | FastF1 cache directory | `.fastf1-cache` |
| `--dry-run` | Don't send to Kafka | `false` |
| `--skip-telemetry` | Skip telemetry data | `false` |
| `--skip-events` | Skip race events | `false` |
| `--max-events` | Max messages per session | Unlimited |

## Session Types

- `FP1`, `FP2`, `FP3` - Free Practice
- `Q` - Qualifying
- `S` - Sprint Qualifying (pre-2023)
- `SQ` - Sprint Qualifying (2023+)
- `R` - Race
- `SS` - Sprint Shootout

## Data Schema Mapping

The producer matches the exact schemas in `spark/schemas.py`:

### TELEMETRY_SCHEMA (33 fields)
```python
session_id, driver_id, car_number, lap_number, timestamp_utc,
speed_kph, throttle_pct, brake_pressure_bar, gear, engine_rpm,
drs_state, tyre_compound, tyre_age_laps, gps_lat, gps_lon,
track_status_code, weather_*, etc.
```

### RACE_EVENT_SCHEMA (12 fields)
```python
session_id, event_id, event_ts_utc, event_type, driver_id,
lap_number, pit_stop_id, pit_duration_ms, payload, source
```

## Performance Tips

1. **Speedup**: Use `--speedup 30` or higher for historical data
2. **Batch size**: Increase `--batch-size` to 500+ for large datasets
3. **Cache**: First run will be slow (FastF1 download), subsequent runs use cache
4. **Filtering**: Use `--driver` and `--event` to reduce data volume
5. **Network**: Run from EC2 in same region as MSK for best performance

## Examples with Your Infrastructure

Using Terraform outputs:

```bash
# Get bootstrap servers
BOOTSTRAP=$(cd infra && terraform output -raw msk_bootstrap_brokers)

# Stream 2024 Monaco GP at 20x speed
./producer.py \
  --bootstrap "$BOOTSTRAP" \
  --start-year 2024 \
  --event "Monaco" \
  --speedup 20

# Stream 2020-2024 for Max Verstappen only
./producer.py \
  --bootstrap "$BOOTSTRAP" \
  --start-year 2020 \
  --end-year 2024 \
  --driver VER \
  --speedup 50
```

## Monitoring

The producer logs:
- Session loading progress
- Messages published per topic
- Errors and warnings
- Final summary statistics

Example output:
```
2024-11-05 - INFO - Processing season 2024
2024-11-05 - INFO - Event 1: Bahrain Grand Prix
2024-11-05 - INFO - Loading session: Bahrain Grand Prix - Race
2024-11-05 - INFO - Session 2024_Bahrain_Grand_Prix_Race complete: 125430 telemetry, 380 events, 0 errors
```

## Troubleshooting

**Error: Bootstrap servers required**
- Set `--bootstrap` or `KAFKA_BOOTSTRAP_BROKERS` env var

**Error: Could not load session**
- Session may not exist (e.g., no FP3 for sprint weekends)
- Use `--session R` to only load race sessions

**Slow performance**
- Increase `--speedup` to skip delays
- Use `--batch-size 500` or higher
- Check network latency to MSK cluster

**Memory issues**
- Process one year at a time
- Use `--driver` to filter data
- Increase `--speedup` to process faster

## Next Steps

After running the producer:
1. Verify topics in MSK console
2. Check Bronze Spark job is consuming: `yarn application -list`
3. Monitor Kafka lag and throughput
4. Validate data in S3 bronze layer
