BINARY := kafkaesque
BIN_DIR := bin
KAFKA_COMPOSE := docker compose -f kafka-env/compose.yaml
KAFKA_CI_COMPOSE := $(KAFKA_COMPOSE) -f kafka-env/compose.ci.yaml
KAFKA_BROKERS := broker-1 broker-2 broker-3 broker-4 broker-5
KAFKA_CI_BROKERS := broker-1 broker-2

.PHONY: build run test kafka-up kafka-down kafka-reset kafka-logs kafka-ci-up kafka-ci-down kafka-ci-diagnostics

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/kafkaesque

run:
	go run ./cmd/kafkaesque

test:
	@packages=$$(find . -name '*_test.go' -type f -exec dirname {} \; | sort -u); \
	if [ -z "$$packages" ]; then echo "No test packages found"; exit 0; fi; \
	go test $$packages

kafka-up:
	$(KAFKA_COMPOSE) up -d --wait $(KAFKA_BROKERS)
	$(KAFKA_COMPOSE) run --rm init

kafka-down:
	$(KAFKA_COMPOSE) down --volumes --remove-orphans

kafka-reset:
	$(KAFKA_COMPOSE) down --volumes --remove-orphans
	$(KAFKA_COMPOSE) up -d --wait $(KAFKA_BROKERS)
	$(KAFKA_COMPOSE) run --rm init

kafka-logs:
	$(KAFKA_COMPOSE) logs --follow $(KAFKA_BROKERS)

kafka-ci-up:
	$(KAFKA_CI_COMPOSE) up -d --wait $(KAFKA_CI_BROKERS)
	$(KAFKA_CI_COMPOSE) run --rm --no-deps init

kafka-ci-down:
	$(KAFKA_CI_COMPOSE) down --volumes --remove-orphans

kafka-ci-diagnostics:
	$(KAFKA_CI_COMPOSE) ps
	$(KAFKA_CI_COMPOSE) logs --no-color $(KAFKA_CI_BROKERS)
