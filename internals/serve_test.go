package internals

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/listgroups"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
)

type serveTransport struct{}

type serveAuth struct{}

func (serveAuth) Login(context.Context, string, string) (kafkaesque_auth.Tokens, error) {
	return kafkaesque_auth.Tokens{}, nil
}

func (serveAuth) AuthenticateAccess(context.Context, string) (kafkaesque_auth.User, error) {
	return kafkaesque_auth.User{ID: "user-1", Username: "super_admin", Role: "super_admin"}, nil
}

func (serveAuth) Refresh(context.Context, string) (kafkaesque_auth.Tokens, error) {
	return kafkaesque_auth.Tokens{}, nil
}

func (serveAuth) Logout(context.Context, string) error { return nil }

func (serveAuth) ValidateCSRF(context.Context, string, string) (bool, error) {
	return true, nil
}

func (serveAuth) ValidateRefreshCSRF(context.Context, string, string) (bool, error) {
	return true, nil
}

func (serveAuth) ListUsers(context.Context, int, int) ([]kafkaesque_auth.ManagedUser, int, error) {
	return []kafkaesque_auth.ManagedUser{}, 0, nil
}

func (serveAuth) CreateUser(context.Context, kafkaesque_auth.CreateUserInput) (kafkaesque_auth.ManagedUser, error) {
	return kafkaesque_auth.ManagedUser{}, nil
}

func testServerInitializer(_ context.Context, cfg *kafkaesque_config.Config, _ *slog.Logger) (*serverRuntime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return &serverRuntime{handler: http.NotFoundHandler(), close: func() {}}, nil
}

func (serveTransport) RoundTrip(_ context.Context, _ net.Addr, request kafka.Request) (kafka.Response, error) {
	switch request.(type) {
	case *metadataAPI.Request:
		return &metadataAPI.Response{}, nil
	case *listgroups.Request:
		return &listgroups.Response{}, nil
	default:
		return nil, errors.New("unexpected Kafka request")
	}
}

func TestServeCommandUsesConfigAndExplicitOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kafkaesque.yaml")
	if err := os.WriteFile(path, []byte("api:\n  host: 127.0.0.2\n  port: 8086\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "YAML", args: []string{"--config", path}, want: "127.0.0.2:8086"},
		{name: "flags", args: []string{"--config", path, "--host", "0.0.0.0", "--port", "9000"}, want: "0.0.0.0:9000"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got string
			cmd := newServeCommand(func(_ context.Context, address string, handler http.Handler) error {
				got = address
				if handler == nil {
					t.Fatal("handler is nil")
				}
				return nil
			}, testServerInitializer)
			cmd.SetArgs(test.args)
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("address = %q, want %q", got, test.want)
			}
		})
	}
}

func TestServeCommandRejectsInvalidFlagOverrides(t *testing.T) {
	cmd := newServeCommand(func(context.Context, string, http.Handler) error { return nil }, testServerInitializer)
	cmd.SetArgs([]string{"--host", "", "--port", "0"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected invalid API configuration")
	}
}

func TestServeCommandFlagsOverrideEnvironment(t *testing.T) {
	t.Setenv(kafkaesque_config.APIHostEnv, "127.0.0.2")
	t.Setenv(kafkaesque_config.APIPortEnv, "8086")
	var got string
	cmd := newServeCommand(func(_ context.Context, address string, _ http.Handler) error {
		got = address
		return nil
	}, testServerInitializer)
	cmd.SetArgs([]string{"--host", "0.0.0.0", "--port", "9000"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got != "0.0.0.0:9000" {
		t.Fatalf("address = %q, want 0.0.0.0:9000", got)
	}
}

func TestApplicationRoutes(t *testing.T) {
	conn := &kafkaesque.KafkaesqueConn{Client: &kafka.Client{Addr: kafka.TCP("broker:9092"), Transport: serveTransport{}}}
	handler := newApplicationHandler(
		conn,
		&kafkaesque_config.Config{},
		serveAuth{},
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
	)

	t.Run("discovery", func(t *testing.T) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
		}
		var body struct {
			Success bool              `json:"success"`
			Status  int               `json:"status"`
			Data    map[string]string `json:"data"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if !body.Success || body.Status != http.StatusOK || body.Data["ui"] != "/ui" || body.Data["api"] != "/api/v1" {
			t.Fatalf("unexpected discovery response: %+v", body)
		}
	})

	t.Run("dashboard", func(t *testing.T) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ui", nil))
		if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/html; charset=utf-8" {
			t.Fatalf("dashboard response = %d %q", response.Code, response.Header().Get("Content-Type"))
		}
	})

	t.Run("API", func(t *testing.T) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
		if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("API response = %d %q", response.Code, response.Header().Get("Content-Type"))
		}
	})
}

func TestRequestLoggerUsesStatusSeverityAndSafeFields(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ok/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /bad", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	mux.HandleFunc("GET /failed", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	handler := requestLogger(logger, mux)

	for _, target := range []string{
		"/ok/secret-value?token=must-not-be-logged",
		"/bad",
		"/failed",
		"/unknown/private-value?token=also-private",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
	}

	logs := output.String()
	for _, expected := range []string{
		`"level":"INFO"`,
		`"level":"WARN"`,
		`"level":"ERROR"`,
		`"route":"GET /ok/{id}"`,
		`"route":"unmatched"`,
	} {
		if !strings.Contains(logs, expected) {
			t.Fatalf("missing %s in logs: %s", expected, logs)
		}
	}
	for _, secret := range []string{
		"secret-value",
		"must-not-be-logged",
		"private-value",
		"also-private",
	} {
		if strings.Contains(logs, secret) {
			t.Fatalf("request log contains %q: %s", secret, logs)
		}
	}
}
