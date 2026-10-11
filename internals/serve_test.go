package internals

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
)

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
			})
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
	cmd := newServeCommand(func(context.Context, string, http.Handler) error { return nil })
	cmd.SetArgs([]string{"--host", "", "--port", "0"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected invalid API configuration")
	}
}

func TestApplicationRoutes(t *testing.T) {
	handler := newApplicationHandler(&kafkaesque.KafkaesqueConn{}, &kafkaesque_config.Config{})

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
