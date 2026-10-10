package internals

import (
	"context"
	"fmt"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	"github.com/spf13/cobra"
)

// InformationRequest identifies the Kafka information the caller needs.
type InformationRequest struct {
	Metadata   bool
	Topics     bool
	Brokers    bool
	Partitions bool
	Topic      string
	Consumers  bool
	Consumer   bool
	Group      string

	lagThresholds kafkaesque_consumers.LagThresholds
}

func (r InformationRequest) any() bool {
	return r.Metadata || r.Topics || r.Brokers || r.Partitions || r.Consumers || r.Consumer || r.Group != "" || r.Topic != ""
}

func newRootCommand() *cobra.Command {
	return newRootCommandWithConnectionCheck(checkKafkaConnection)
}

func newRootCommandWithConnectionCheck(checkConnection func(context.Context, string) error) *cobra.Command {
	var request InformationRequest
	var configPath string
	var check string

	cmd := &cobra.Command{
		Use:   "kafkaesque",
		Short: "Kafkaesque is a tool for interacting with Kafka clusters.",
		Long: `Kafkaesque is a lightweight, read-only Kafka cluster inspector for viewing cluster
metadata, brokers, topics, partitions, and consumer groups.`,
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRootCommand(cmd, request, configPath, check, checkConnection)
		},
	}

	cmd.Flags().SortFlags = false

	cmd.Flags().BoolVarP(&request.Metadata, "metadata", "m", false, "Retrieve cluster metadata")
	cmd.Flags().BoolVarP(&request.Brokers, "brokers", "b", false, "Retrieve broker information")
	cmd.Flags().BoolVarP(&request.Topics, "topics", "t", false, "Retrieve topic information")
	cmd.Flags().BoolVarP(&request.Partitions, "partitions", "p", false, "Retrieve partition information for a topic")
	cmd.Flags().StringVar(&request.Topic, "topic", "", "Topic name for partition or consumer group inspection")
	cmd.Flags().BoolVarP(&request.Consumers, "consumers", "c", false, "Retrieve consumer group information")
	cmd.Flags().BoolVar(&request.Consumer, "consumer", false, "Retrieve detailed information for one consumer group")
	cmd.Flags().StringVar(&request.Group, "group", "", "Consumer group name")
	cmd.Flags().StringVar(&configPath, "config", "", "Path to the YAML configuration file")
	cmd.Flags().StringVar(&check, "check", "", "Check configuration or Kafka connection (config|conn)")

	return cmd
}

func runRootCommand(
	cmd *cobra.Command,
	request InformationRequest,
	configPath string,
	check string,
	checkConnection func(context.Context, string) error,
) error {
	if check == "" && !request.any() {
		return cmd.Help()
	}
	if check != "" && request.any() {
		return fmt.Errorf("--check cannot be combined with information flags")
	}
	if check != "" && check != "config" && check != "conn" {
		return fmt.Errorf("invalid --check value %q: must be config or conn", check)
	}
	if request.Partitions && request.Topic == "" {
		return fmt.Errorf("please provide a topic name using the --topic flag")
	}
	if request.Topic != "" && !request.Partitions && !request.Consumer {
		return fmt.Errorf("--topic requires --partitions or --consumer")
	}
	if request.Consumer && request.Consumers {
		return fmt.Errorf("--consumer cannot be combined with --consumers")
	}
	if request.Consumer && request.Group == "" {
		return fmt.Errorf("please provide a consumer group name using the --group flag")
	}
	if !request.Consumer && request.Group != "" {
		return fmt.Errorf("--group requires --consumer")
	}

	cfg, err := kafkaesque_config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	if check == "config" {
		fmt.Fprintln(cmd.OutOrStdout(), "Configuration is valid")
		return nil
	}
	request.lagThresholds = kafkaesque_consumers.LagThresholds{
		Warning:   cfg.Lag.WarningThreshold,
		Unhealthy: cfg.Lag.UnhealthyThreshold,
	}

	ctx := cmd.Context()
	if check == "conn" {
		if err := checkConnection(ctx, cfg.Kafka.BootstrapServer); err != nil {
			return fmt.Errorf("failed to connect to Kafka at %s: %w", cfg.Kafka.BootstrapServer, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Kafka connection successful: %s\n", cfg.Kafka.BootstrapServer)
		return nil
	}

	conn, err := kafkaesque.Connect(ctx, cfg.Kafka.BootstrapServer)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka at %s: %w", cfg.Kafka.BootstrapServer, err)
	}
	defer conn.Close()

	info, err := GetKafkaInformation(ctx, conn, request)
	if err != nil {
		return fmt.Errorf("failed to get Kafka information: %w", err)
	}

	return printKafkaInformation(cmd, request, info)
}

func checkKafkaConnection(ctx context.Context, bootstrapServer string) error {
	conn, err := kafkaesque.Connect(ctx, bootstrapServer)
	if err != nil {
		return err
	}
	return conn.Close()
}

var rootCmd = newRootCommand()

// Execute runs the kafkaesque CLI and returns command errors to the caller.
func Execute() error {
	return rootCmd.Execute()
}
