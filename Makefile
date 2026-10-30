BINARY := kafkaesque
BIN_DIR := bin
GORELEASER ?= goreleaser
DOCKER_IMAGE ?= kafkaesque
VERSION ?= dev
KAFKA_COMPOSE := docker compose -f kafka-env/compose.yaml
KAFKA_CI_COMPOSE := $(KAFKA_COMPOSE) -f kafka-env/compose.ci.yaml
KAFKA_SIMULATOR_COMPOSE := $(KAFKA_COMPOSE) --profile simulator
KAFKA_CI_SIMULATOR_COMPOSE := $(KAFKA_CI_COMPOSE) --profile simulator
KAFKA_BROKERS := broker-1 broker-2 broker-3 broker-4 broker-5
KAFKA_CI_BROKERS := broker-1 broker-2
POSTGRES_COMPOSE := docker compose -f postgres-env/compose.yaml
KAFKAESQUE_POSTGRES_PORT ?= 5434
POSTGRES_TEST_DATABASE := kafkaesque_auth_test
POSTGRES_TEST_URL := postgres://kafkaesque:kafkaesque-dev-only@127.0.0.1:$(KAFKAESQUE_POSTGRES_PORT)/$(POSTGRES_TEST_DATABASE)?sslmode=disable

.PHONY: check fmt-check lint build docker-build run test release-test postgres-up postgres-down postgres-reset postgres-logs postgres-test kafka-up kafka-down kafka-reset kafka-logs kafka-simulator-start kafka-simulator-stop kafka-simulator-logs kafka-simulator-status kafka-lag-simulate kafka-lag-resolve kafka-ci-up kafka-ci-simulator-start kafka-ci-down kafka-ci-diagnostics

check: fmt-check lint test build

fmt-check:
	@unformatted="$$(git ls-files -z -- '*.go' | xargs -0 gofmt -l)"; \
	if [ -n "$$unformatted" ]; then \
		echo "The following Go files need formatting:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

lint:
	go vet ./...

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/kafkaesque

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t $(DOCKER_IMAGE):$(VERSION) .

run:
	go run ./cmd/kafkaesque

test:
	@packages=$$(find . -name '*_test.go' -type f -exec dirname {} \; | sort -u); \
	if [ -z "$$packages" ]; then echo "No test packages found"; exit 0; fi; \
	go test $$packages

release-test:
	$(GORELEASER) release --snapshot --clean

postgres-up:
	$(POSTGRES_COMPOSE) up -d --wait

postgres-down:
	$(POSTGRES_COMPOSE) down

postgres-reset:
	$(POSTGRES_COMPOSE) down --volumes --remove-orphans
	$(POSTGRES_COMPOSE) up -d --wait

postgres-logs:
	$(POSTGRES_COMPOSE) logs --follow postgres

postgres-test: postgres-up
	@$(POSTGRES_COMPOSE) exec -T postgres sh -ec 'if [ "$$(psql -U kafkaesque -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='"'"'$(POSTGRES_TEST_DATABASE)'"'"'")" != "1" ]; then createdb -U kafkaesque $(POSTGRES_TEST_DATABASE); fi'
	KAFKAESQUE_TEST_DATABASE_URL='$(POSTGRES_TEST_URL)' go test -count=1 -race ./internals/auth
	KAFKAESQUE_TEST_DATABASE_URL='$(POSTGRES_TEST_URL)' go test -count=1 -race ./internals/alerts

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

kafka-lag-simulate:
	@bash kafka-env/lag-simulate.sh "$(GROUP)" "$(TOPIC)" "$(or $(COUNT),10)"

kafka-lag-resolve:
	@bash kafka-env/lag-simulate.sh --resolve "$(GROUP)" "$(TOPIC)"

kafka-ci-up:
	$(KAFKA_CI_COMPOSE) up -d --wait $(KAFKA_CI_BROKERS)
	$(KAFKA_CI_COMPOSE) run --rm --no-deps init

kafka-ci-simulator-start:
	$(KAFKA_CI_SIMULATOR_COMPOSE) up -d --no-deps --wait simulator

kafka-ci-down:
	$(KAFKA_CI_SIMULATOR_COMPOSE) down --volumes --remove-orphans

kafka-ci-diagnostics:
	$(KAFKA_CI_COMPOSE) ps
	$(KAFKA_CI_SIMULATOR_COMPOSE) logs --no-color $(KAFKA_CI_BROKERS) simulator
