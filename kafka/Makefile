.PHONY: help create-topics produce-local consume-local

PYTHON ?= python
REGION ?= us-east-1
ENV_FILE ?= .env

# Default session selection for the producer (adjust as needed)
PRODUCER_SEASON ?= 2024
PRODUCER_EVENT ?= Bahrain
PRODUCER_SESSION ?= R
PRODUCER_TOPIC ?= telemetry.raw
PRODUCER_SPEEDUP ?= 50
PRODUCER_MAX_EVENTS ?=
CONSUMER_TOPIC ?= $(PRODUCER_TOPIC)

help:
	@echo "Kafka tooling targets:"
	@echo "  make create-topics    - Create topics defined in topics.yaml"
	@echo "  make produce-local    - Replay a session to the local broker"
	@echo "  make consume-local    - Start a console consumer against the local broker"
	@echo ""
	@echo "Override defaults, e.g.: make PRODUCER_EVENT='Spanish' produce-local"

create-topics:
	$(PYTHON) scripts/create_topics.py --auth-mode plain --bootstrap ${KAFKA_BOOTSTRAP_BROKERS:-localhost:9092} --topics-file topics.yaml $(ARGS)

produce-local:
	@KAFKA_BOOTSTRAP_BROKERS=$${KAFKA_BOOTSTRAP_BROKERS:-localhost:9092}; \
	$(PYTHON) producer/producer.py \
		--bootstrap $$KAFKA_BOOTSTRAP_BROKERS \
		--telemetry-topic $(PRODUCER_TOPIC) \
		--events-topic race.events \
		--start-year $(PRODUCER_SEASON) \
		--event "$(PRODUCER_EVENT)" \
		--session $(PRODUCER_SESSION) \
		--speedup $(PRODUCER_SPEEDUP) \
		$(if $(PRODUCER_MAX_EVENTS),--max-events $(PRODUCER_MAX_EVENTS),) \
		$(ARGS)

consume-local:
	KAFKA_TOPIC=$(CONSUMER_TOPIC) docker compose run --rm console-consumer
