BINARY := kafkaesque
BIN_DIR := bin

.PHONY: build run test

build:
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/kafkaesque

run:
	go run ./cmd/kafkaesque

test:
	@packages=$$(find . -name '*_test.go' -type f -exec dirname {} \; | sort -u); \
	if [ -z "$$packages" ]; then echo "No test packages found"; exit 0; fi; \
	go test $$packages
