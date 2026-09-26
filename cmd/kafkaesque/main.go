package main

import (
	"log"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals"
)

func main() {
	if err := kafkaesque.Execute(); err != nil {
		log.Fatalf("Error executing command: %v", err)
	}
}
