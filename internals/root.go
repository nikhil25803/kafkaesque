package internals

import (
	"context"
	"fmt"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	"github.com/spf13/cobra"
)

// InformationRequest identifies the Kafka information the caller needs.
type InformationRequest struct {
	Metadata   bool
	Topics     bool
	Brokers    bool
	Partitions bool
	Consumers  bool
	Topic      string
}

func (r InformationRequest) any() bool {
	return r.Metadata || r.Topics || r.Brokers || r.Partitions || r.Consumers
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
		Long: `Kafkaesque is a lightweight, read-only Kafka observability tool focused primarily on
consumer groups, partition offsets, consumer lag, continuous monitoring, a server-rendered HTMX web
UI, and Slack-based alerting.`,
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRootCommand(cmd, request, configPath, check, checkConnection)
		},
	}

	cmd.Flags().BoolVarP(&request.Metadata, "metadata", "m", false, "Retrieve cluster metadata")
	cmd.Flags().BoolVarP(&request.Topics, "topics", "t", false, "Retrieve topic information")
	cmd.Flags().BoolVarP(&request.Brokers, "brokers", "b", false, "Retrieve broker information")
	cmd.Flags().BoolVarP(&request.Partitions, "partitions", "p", false, "Retrieve partition information for a topic")
	cmd.Flags().StringVar(&request.Topic, "topic", "", "Topic name")
	cmd.Flags().BoolVarP(&request.Consumers, "consumers", "c", false, "Retrieve consumer information")
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

	cfg, err := kafkaesque_config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	if check == "config" {
		fmt.Fprintln(cmd.OutOrStdout(), "Configuration is valid")
		return nil
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
