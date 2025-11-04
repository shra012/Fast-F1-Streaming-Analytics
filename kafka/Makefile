.PHONY: help create-topics update-env produce-local produce-msk consume-local

PYTHON ?= python
REGION ?= us-east-1
ENV_FILE ?= .env

# Default session selection for the producer (adjust as needed)
PRODUCER_SEASON ?= 2023
PRODUCER_EVENT ?= Bahrain Grand Prix
PRODUCER_SESSION ?= R
PRODUCER_TOPIC ?= telemetry.fastf1.raw
PRODUCER_SPEEDUP ?= 30
PRODUCER_MAX_EVENTS ?= 30
CONSUMER_TOPIC ?= $(PRODUCER_TOPIC)

help:
	@echo "Kafka tooling targets:"
	@echo "  make create-topics    - Create topics defined in topics.yaml"
	@echo "  make update-env       - Discover MSK cluster details and update .env"
	@echo "  make produce-local    - Replay a sample session to the local broker"
	@echo "  make produce-msk      - Replay a sample session to MSK (IAM auth)"
	@echo "  make consume-local    - Start a console consumer against the local broker"
	@echo ""
	@echo "Override defaults, e.g.: make PRODUCER_EVENT='Spanish Grand Prix' produce-local"

create-topics:
	$(PYTHON) scripts/create_topics.py $(ARGS)

update-env:
	$(PYTHON) scripts/update_bootstrap_env.py --region $(REGION) --env-file $(ENV_FILE) $(ARGS)

produce-local:
	@KAFKA_BOOTSTRAP_BROKERS=$${KAFKA_BOOTSTRAP_BROKERS:-localhost:9092}; \
		env KAFKA_AUTH_MODE=plain KAFKA_BOOTSTRAP_BROKERS="$$KAFKA_BOOTSTRAP_BROKERS" \
	$(PYTHON) producer/simple_producer.py \
		--auth-mode plain \
		--bootstrap $$KAFKA_BOOTSTRAP_BROKERS \
		--topic $(PRODUCER_TOPIC) \
		--season $(PRODUCER_SEASON) \
		--event "$(PRODUCER_EVENT)" \
		--session $(PRODUCER_SESSION) \
		--speedup $(PRODUCER_SPEEDUP) \
		--max-events $(PRODUCER_MAX_EVENTS) \
		$(ARGS)

produce-msk:
	@if [ -z "$${MSK_BOOTSTRAP_BROKERS:-}" ]; then \
		echo "MSK_BOOTSTRAP_BROKERS is required (run make update-env first)."; \
		exit 1; \
	fi
	@if [ -z "$${MSK_CLUSTER_ARN:-}" ] || [ -z "$${AWS_REGION:-}" ]; then \
		echo "MSK_CLUSTER_ARN and AWS_REGION must be set for IAM auth (check ../.env)."; \
		exit 1; \
	fi
	env KAFKA_AUTH_MODE=iam KAFKA_BOOTSTRAP_BROKERS="$$MSK_BOOTSTRAP_BROKERS" \
	$(PYTHON) producer/simple_producer.py \
		--auth-mode iam \
		--bootstrap $$MSK_BOOTSTRAP_BROKERS \
		--cluster-arn $$MSK_CLUSTER_ARN \
		--region $$AWS_REGION \
		--topic $(PRODUCER_TOPIC) \
		--season $(PRODUCER_SEASON) \
		--event "$(PRODUCER_EVENT)" \
		--session $(PRODUCER_SESSION) \
		--speedup $(PRODUCER_SPEEDUP) \
		--max-events $(PRODUCER_MAX_EVENTS) \
		$(ARGS)

consume-local:
	KAFKA_TOPIC=$(CONSUMER_TOPIC) docker compose run --rm console-consumer
