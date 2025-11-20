# Kafka Module

Producer streams F1 telemetry and race events from FastF1 API to Kafka topics.

## Architecture

```
FastF1 API -> producer.py -> MSK Kafka
                              +- telemetry.raw
                              +- race.events
                                      v
                              Bronze Spark Job
```

## Prerequisites

- Python 3.10+
- AWS credentials configured
- MSK cluster with PLAINTEXT authentication

## Setup

```bash
uv sync
source .venv/bin/activate
```

## Create Topics

```bash
python scripts/create_topics.py
```

## Run Producer

```bash
cd producer
./run_producer.sh --start-year 2024 --speedup 30
```

## Topics

- `telemetry.raw` - Car telemetry (speed, throttle, GPS, etc.)
- `race.events` - Race events (laps, pit stops, overtakes, battles)

## Event Types

- **LAP_COMPLETION**: Lap times and sector times
- **PIT_STOP**: Pit entry/exit times and tyre strategy
- **OVERTAKE**: Position changes detected between laps
- **BATTLE**: Multi-lap position battles between drivers
