# F1 Kafka Producer

Producer for streaming F1 telemetry and race events to Kafka topics.

## Features

- Multi-season support (2020+)
- Two topics: `telemetry.raw` and `race.events`
- Four event types: LAP_COMPLETION, PIT_STOP, OVERTAKE, BATTLE
- Configurable playback speed
- Session filtering

## Usage

```bash
./producer.py \
  --bootstrap "$BOOTSTRAP" \
  --start-year 2024 \
  --speedup 30
```

## Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--bootstrap` | Kafka bootstrap servers | From env |
| `--start-year` | Starting season | `2020` |
| `--end-year` | Ending season | Current year |
| `--event` | Specific event name | All events |
| `--driver` | Filter by driver code | All drivers |
| `--speedup` | Playback speed multiplier | `1.0` |
| `--dry-run` | Don't send to Kafka | `false` |
