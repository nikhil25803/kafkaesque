package internals

import (
	"context"
	"fmt"

	"github.com/nikhil25803/kafkaesque/internals/kafka"
	"github.com/spf13/cobra"
)

type KafkaInformation struct {
	Metadata *kafka.ClusterInformation `json:"metadata"`
}

var (
	metadata bool
)

func GetKafkaInformation(
	ctx context.Context,
	conn *kafka.KafkaesqueConn,
) (*KafkaInformation, error) {

	metadata, err := conn.GetMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
	}

	return &KafkaInformation{
		Metadata: metadata,
	}, nil
}

var rootCmd = &cobra.Command{
	Use:           "kafkaesque",
	Short:         "Kafkaesque is a CLI tool for interacting with Kafka clusters.",
	Long:          `Kafkaesque is a command-line interface (CLI) tool designed to facilitate interactions with Kafka clusters. It provides users with the ability to retrieve cluster metadata, manage topics, and perform various administrative tasks related to Kafka.`,
	Args:          cobra.NoArgs,
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		conn, err := kafka.Connect(ctx, "localhost:9092")
		if err != nil {
			return fmt.Errorf("failed to connect to Kafka: %w", err)
		}
		defer conn.Close()

		info, err := GetKafkaInformation(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get Kafka information: %w", err)
		}

		if metadata {
			fmt.Printf("Cluster Metadata:\n")
			fmt.Printf("  Cluster ID: %s\n", info.Metadata.ClusterID)
			fmt.Printf("  Controller ID: %d\n", info.Metadata.ControllerID)
			fmt.Printf("  Brokers: %d\n", info.Metadata.Brokers)
			fmt.Printf("  Topics: %d\n", info.Metadata.Topics)
		}

		return nil
	},
}

func init() {
	rootCmd.Flags().BoolVarP(&metadata, "metadata", "m", false, "Retrieve cluster metadata")
}

func Execute() error {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
	return nil
}
