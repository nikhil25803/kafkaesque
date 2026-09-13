package main

import (
	"log"

	kafaesque "github.com/nikhil25803/kafkaesque/internals"
)

func main() {
	if err := kafaesque.Execute(); err != nil {
		log.Fatalf("Error executing command: %v", err)
	}
}
