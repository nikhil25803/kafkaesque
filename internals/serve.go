package internals

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nikhil25803/kafkaesque/apis"
	kafkaesque_alerts "github.com/nikhil25803/kafkaesque/internals/alerts"
	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
	"github.com/nikhil25803/kafkaesque/ui"
	"github.com/spf13/cobra"
)

type apiServer func(context.Context, string, http.Handler) error

type serverRuntime struct {
	handler http.Handler
	close   func()
}

type serverInitializer func(context.Context, *kafkaesque_config.Config, *slog.Logger) (*serverRuntime, error)

func serveAPI(ctx context.Context, address string, handler http.Handler) error {
	return apis.Serve(ctx, address, handler)
}

func newApplicationHandler(
	conn *kafkaesque.KafkaesqueConn,
	cfg *kafkaesque_config.Config,
	authentication kafkaesque_auth.RuntimeManager,
	logger *slog.Logger,
	topicRates kafkaesque_topics.MessageRateProvider,
	alertManager kafkaesque_alerts.Manager,
) http.Handler {
	mux := http.NewServeMux()

	// API routes.
	mux.Handle("/api/", apis.NewHandlerWithTopicRates(conn, cfg, authentication, logger, topicRates))

	// UI routes.
	uiHandler := ui.NewHandlerWithTopicRates(conn, cfg, authentication, logger, topicRates, alertManager)
	mux.Handle("/ui", uiHandler)
	mux.Handle("/ui/", uiHandler)

	// Root discovery.
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")

		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"status":  http.StatusOK,
			"data": map[string]string{
				"name":   "Kafkaesque",
				"api":    "/api/v1",
				"health": "/api/v1/health",
				"ui":     "/ui",
			},
		})
	})

	return requestLogger(logger, mux)
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	userID      string
	wroteHeader bool
}

// SetAuthenticatedUser records identity for the outer request log.
func (w *statusWriter) SetAuthenticatedUser(userID string) {
	w.userID = userID
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

// requestLogger records one safe summary after every HTTP request.
func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}

		arguments := []any{
			"method", r.Method,
			"route", route,
			"status", wrapped.status,
			"duration_ms", time.Since(start).Milliseconds(),
		}
		if wrapped.userID != "" {
			arguments = append(arguments, "user_id", wrapped.userID)
		}

		switch {
		case wrapped.status >= http.StatusInternalServerError:
			logger.Error("http_request", arguments...)
		case wrapped.status >= http.StatusBadRequest:
			logger.Warn("http_request", arguments...)
		default:
			logger.Info("http_request", arguments...)
		}
	})
}

func newServeCommand(serve apiServer, initialize serverInitializer) *cobra.Command {
	var configPath, host string
	var port int
	cmd := &cobra.Command{
		Use:          "serve",
		Short:        "Serve the read-only Kafka HTTP API and UI",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := slog.New(slog.NewJSONHandler(cmd.ErrOrStderr(), nil))
			cfg, err := kafkaesque_config.Load(configPath)
			if err != nil {
				logger.Error("configuration_load_failed", "error_type", fmt.Sprintf("%T", err))
				return fmt.Errorf("failed to load configuration: %w", err)
			}
			if cmd.Flags().Changed("host") {
				cfg.API.Host = host
			}
			if cmd.Flags().Changed("port") {
				cfg.API.Port = port
			}
			runtime, err := initialize(cmd.Context(), cfg, logger)
			if err != nil {
				logger.Error("server_initialization_failed", "error_type", fmt.Sprintf("%T", err))
				return err
			}
			defer runtime.close()
			address := net.JoinHostPort(strings.TrimSpace(cfg.API.Host), strconv.Itoa(cfg.API.Port))
			logger.Info("server_started", "address", address)
			if err := serve(cmd.Context(), address, runtime.handler); err != nil {
				logger.Error("server_failed", "error_type", fmt.Sprintf("%T", err))
				return err
			}
			logger.Info("server_stopped")
			return nil
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().StringVar(&configPath, "config", "", "Path to the YAML configuration file")
	cmd.Flags().StringVar(&host, "host", "", "API listen host")
	cmd.Flags().IntVar(&port, "port", 0, "API listen port")
	return cmd
}

func initializeServer(ctx context.Context, cfg *kafkaesque_config.Config, logger *slog.Logger) (*serverRuntime, error) {
	if err := cfg.ValidateServe(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	logger.Info("config_validated")
	authentication, err := kafkaesque_auth.Open(ctx, cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize authentication: %w", err)
	}
	conn, err := kafkaesque.Connect(ctx, cfg.Kafka)
	if err != nil {
		authentication.Close()
		return nil, fmt.Errorf("failed to configure Kafka connection: %w", err)
	}
	topicRates := kafkaesque_topics.NewMessageRateCollector(conn, logger)
	topicRates.Start(ctx)
	alertService, err := kafkaesque_alerts.NewService(authentication.Database(), conn, cfg.Alerts.EncryptionKey)
	if err != nil {
		conn.Close()
		authentication.Close()
		return nil, fmt.Errorf("initialize alerts: %w", err)
	}
	kafkaesque_alerts.NewMonitor(alertService, conn, kafkaesque_alerts.NewHTTPSlackSender(), logger).Start(ctx)
	return &serverRuntime{
		handler: newApplicationHandler(conn, cfg, authentication, logger, topicRates, alertService),
		close: func() {
			conn.Close()
			authentication.Close()
		},
	}, nil
}
