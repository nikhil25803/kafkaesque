package internals

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
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
