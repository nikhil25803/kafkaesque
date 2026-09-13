BINARY := kafkaesque
BIN_DIR := bin

.PHONY: build run

build:
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/kafkaesque

run:
	go run ./cmd/kafkaesque
