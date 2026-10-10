BINARY := kafkaesque
BIN_DIR := bin
GORELEASER ?= goreleaser
KAFKA_COMPOSE := docker compose -f kafka-env/compose.yaml
KAFKA_CI_COMPOSE := $(KAFKA_COMPOSE) -f kafka-env/compose.ci.yaml
KAFKA_SIMULATOR_COMPOSE := $(KAFKA_COMPOSE) --profile simulator
KAFKA_BROKERS := broker-1 broker-2 broker-3 broker-4 broker-5
KAFKA_CI_BROKERS := broker-1 broker-2

.PHONY: build run test release-test kafka-up kafka-down kafka-reset kafka-logs kafka-simulator-start kafka-simulator-stop kafka-simulator-logs kafka-simulator-status kafka-ci-up kafka-ci-down kafka-ci-diagnostics

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/kafkaesque

run:
	go run ./cmd/kafkaesque

test:
	@packages=$$(find . -name '*_test.go' -type f -exec dirname {} \; | sort -u); \
	if [ -z "$$packages" ]; then echo "No test packages found"; exit 0; fi; \
	go test $$packages

release-test:
	$(GORELEASER) release --snapshot --clean

kafka-up:
	$(KAFKA_COMPOSE) up -d --wait $(KAFKA_BROKERS)
	$(KAFKA_COMPOSE) run --rm init

kafka-down:
	$(KAFKA_SIMULATOR_COMPOSE) down --volumes --remove-orphans

kafka-reset:
	$(KAFKA_SIMULATOR_COMPOSE) down --volumes --remove-orphans
	$(KAFKA_COMPOSE) up -d --wait $(KAFKA_BROKERS)
	$(KAFKA_COMPOSE) run --rm init

kafka-logs:
	$(KAFKA_COMPOSE) logs --follow $(KAFKA_BROKERS)

kafka-simulator-start:
	@$(KAFKA_COMPOSE) exec -T broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092 --describe --topic orders >/dev/null 2>&1 || { echo "Kafka fixture is not available; run 'make kafka-up' first."; exit 1; }
	$(KAFKA_SIMULATOR_COMPOSE) up -d --no-deps --wait simulator

kafka-simulator-stop:
	$(KAFKA_SIMULATOR_COMPOSE) rm --stop --force simulator

kafka-simulator-logs:
	$(KAFKA_SIMULATOR_COMPOSE) logs --follow simulator

kafka-simulator-status:
	$(KAFKA_SIMULATOR_COMPOSE) ps --all simulator

kafka-ci-up:
	$(KAFKA_CI_COMPOSE) up -d --wait $(KAFKA_CI_BROKERS)
	$(KAFKA_CI_COMPOSE) run --rm --no-deps init

kafka-ci-down:
	$(KAFKA_CI_COMPOSE) down --volumes --remove-orphans

kafka-ci-diagnostics:
	$(KAFKA_CI_COMPOSE) ps
	$(KAFKA_CI_COMPOSE) logs --no-color $(KAFKA_CI_BROKERS)
