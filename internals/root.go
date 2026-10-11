package internals

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
)

// Version is replaced with the release tag by GoReleaser.
var Version = "dev"

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

type connectionChecker func(context.Context, kafkaesque_config.KafkaConfig) (*kafkaesque.CheckResult, error)

func newRootCommandWithConnectionCheck(checkConnection connectionChecker) *cobra.Command {
	var request InformationRequest
	var configPath string
	var check string

	cmd := &cobra.Command{
		Use:     "kafkaesque",
		Version: Version,
		Short:   "Kafkaesque is a tool for interacting with Kafka clusters.",
		Long: `Kafkaesque is a lightweight, read-only Kafka cluster inspector for viewing cluster
metadata, brokers, topics, partitions, and consumer groups.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 && cmd.Flags().Changed("check") && check == "conn" {
				check = args[0]
				return nil
			}
			return cobra.NoArgs(cmd, args)
		},
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
	cmd.Flags().Lookup("check").NoOptDefVal = "conn"
	cmd.Flags().BoolP("version", "v", false, "Show Kafkaesque version")

	return cmd
}

func runRootCommand(
	cmd *cobra.Command,
	request InformationRequest,
	configPath string,
	check string,
	checkConnection connectionChecker,
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
		result, err := checkConnection(ctx, cfg.Kafka)
		if err != nil {
			code, reason := classifyConnectionCheckError(err)
			printConnectionCheckFailure(cmd, cfg.Kafka, reason, err, code)
			return &reportedExitError{code: code}
		}
		printConnectionCheckSuccess(cmd, cfg.Kafka, result)
		return nil
	}

	conn, err := kafkaesque.Connect(ctx, cfg.Kafka)
	if err != nil {
		return fmt.Errorf("failed to configure Kafka connection to %s: %w", strings.Join(cfg.Kafka.BootstrapServers, ", "), err)
	}
	defer conn.Close()

	info, err := GetKafkaInformation(ctx, conn, request)
	if err != nil {
		return fmt.Errorf("failed to get Kafka information: %w", err)
	}

	return printKafkaInformation(cmd, request, info)
}

func checkKafkaConnection(ctx context.Context, cfg kafkaesque_config.KafkaConfig) (*kafkaesque.CheckResult, error) {
	conn, err := kafkaesque.Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.Check(ctx, cfg.ConnectionTimeout)
}

type reportedExitError struct{ code int }

func (e *reportedExitError) Error() string { return "Kafka connection check failed" }
func (e *reportedExitError) ExitCode() int { return e.code }

// ExitCode returns the process exit code represented by err.
func ExitCode(err error) int {
	var exitError interface{ ExitCode() int }
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return 1
}

// ErrorWasReported reports whether the command already rendered err for the user.
func ErrorWasReported(err error) bool {
	var reported *reportedExitError
	return errors.As(err, &reported)
}

func classifyConnectionCheckError(err error) (int, string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return 3, "connection timeout"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return 3, "connection timeout"
	}

	securityErrors := []error{
		kafkago.SASLAuthenticationFailed,
		kafkago.UnsupportedSASLMechanism,
		kafkago.IllegalSASLState,
		kafkago.TopicAuthorizationFailed,
		kafkago.GroupAuthorizationFailed,
		kafkago.ClusterAuthorizationFailed,
		kafkago.BrokerAuthorizationFailed,
	}
	for _, securityError := range securityErrors {
		if errors.Is(err, securityError) {
			if errors.Is(err, kafkago.SASLAuthenticationFailed) || errors.Is(err, kafkago.UnsupportedSASLMechanism) || errors.Is(err, kafkago.IllegalSASLState) {
				return 2, "SASL authentication failed"
			}
			return 2, "Kafka authorization failed"
		}
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "tls") || strings.Contains(lower, "x509") || strings.Contains(lower, "certificate") {
		return 2, "TLS authentication failed"
	}
	return 1, "connection failed"
}

func printConnectionCheckSuccess(cmd *cobra.Command, cfg kafkaesque_config.KafkaConfig, result *kafkaesque.CheckResult) {
	out := cmd.OutOrStdout()
	printConnectionCheckHeader(out, cfg)
	fmt.Fprintln(out, "Status: CONNECTED")
	fmt.Fprintln(out, "Cluster")
	fmt.Fprintf(out, "  Brokers: %d\n", result.BrokerCount)
	fmt.Fprintf(out, "  Controller: broker-%d\n", result.ControllerID)
	fmt.Fprintf(out, "  Topics: %d\n", result.TopicCount)
	fmt.Fprintf(out, "  Partitions: %d\n", result.PartitionCount)
	fmt.Fprintf(out, "Connection latency: %dms\n", result.Latency.Milliseconds())
}

func printConnectionCheckFailure(cmd *cobra.Command, cfg kafkaesque_config.KafkaConfig, reason string, err error, code int) {
	out := cmd.OutOrStdout()
	printConnectionCheckHeader(out, cfg)
	fmt.Fprintln(out, "Status: FAILED")
	fmt.Fprintln(out, "Reason:")
	fmt.Fprintf(out, "  %s\n", reason)
	if code == 3 {
		fmt.Fprintln(out, "Timeout:")
		fmt.Fprintf(out, "  %s\n", cfg.ConnectionTimeout)
	}
	fmt.Fprintln(out, "Error:")
	message := err.Error()
	if cfg.Security.SASL.Password != "" {
		message = strings.ReplaceAll(message, cfg.Security.SASL.Password, "[REDACTED]")
	}
	fmt.Fprintf(out, "  %s\n", message)
	fmt.Fprintf(out, "Exit code: %d\n", code)
}

func printConnectionCheckHeader(out io.Writer, cfg kafkaesque_config.KafkaConfig) {
	fmt.Fprintln(out, "Kafka Connection")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintln(out, "Bootstrap Servers")
	for _, server := range cfg.BootstrapServers {
		fmt.Fprintf(out, "  %s\n", server)
	}
	fmt.Fprintln(out, "Security")
	fmt.Fprintf(out, "  Protocol: %s\n", securityProtocol(cfg))
	if cfg.Security.SASL.Mechanism != "" {
		fmt.Fprintf(out, "  Mechanism: %s\n", cfg.Security.SASL.Mechanism)
	}
	if cfg.Security.TLS.ClientCertFile != "" {
		fmt.Fprintln(out, "  Authentication: mTLS")
	}
}

func securityProtocol(cfg kafkaesque_config.KafkaConfig) string {
	if cfg.Security.SASL.Mechanism != "" {
		if cfg.Security.TLS.Enabled {
			return "SASL_SSL"
		}
		return "SASL_PLAINTEXT"
	}
	if cfg.Security.TLS.Enabled {
		return "SSL"
	}
	return "PLAINTEXT"
}

var rootCmd = newRootCommand()

// Execute runs the kafkaesque CLI and returns command errors to the caller.
func Execute() error {
	return rootCmd.Execute()
}
