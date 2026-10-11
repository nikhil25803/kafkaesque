package main

import (
	"fmt"
	"os"

	kafkaesque "github.com/nikhil25803/kafkaesque/internals"
)

func main() {
	if err := kafkaesque.Execute(); err != nil {
		if !kafkaesque.ErrorWasReported(err) {
			fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		}
		os.Exit(kafkaesque.ExitCode(err))
	}
}
