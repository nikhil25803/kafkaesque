package internals

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/nikhil25803/kafkaesque/apis"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	"github.com/spf13/cobra"
)

type apiServer func(context.Context, string, http.Handler) error

func serveAPI(ctx context.Context, address string, handler http.Handler) error {
	return apis.Serve(ctx, address, handler)
}

func newServeCommand(serve apiServer) *cobra.Command {
	var configPath, host string
	var port int
	cmd := &cobra.Command{
		Use:          "serve",
		Short:        "Serve the read-only Kafka HTTP API",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := kafkaesque_config.Load(configPath)
			if err != nil {
				return fmt.Errorf("failed to load configuration: %w", err)
			}
			if cmd.Flags().Changed("host") {
				cfg.API.Host = host
			}
			if cmd.Flags().Changed("port") {
				cfg.API.Port = port
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid configuration: %w", err)
			}
			conn, err := kafkaesque.Connect(cmd.Context(), cfg.Kafka)
			if err != nil {
				return fmt.Errorf("failed to configure Kafka connection: %w", err)
			}
			defer conn.Close()

			address := net.JoinHostPort(strings.TrimSpace(cfg.API.Host), strconv.Itoa(cfg.API.Port))
			fmt.Fprintf(cmd.OutOrStdout(), "Kafkaesque API listening on http://%s\n", address)
			return serve(cmd.Context(), address, apis.NewHandler(conn, cfg))
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().StringVar(&configPath, "config", "", "Path to the YAML configuration file")
	cmd.Flags().StringVar(&host, "host", "", "API listen host")
	cmd.Flags().IntVar(&port, "port", 0, "API listen port")
	return cmd
}
