package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadUsesDefaultsWithoutOptionalFile(t *testing.T) {
	unsetEnvironment(t, BootstrapServerEnv)

	cfg, err := load(filepath.Join(t.TempDir(), "missing.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.BootstrapServer != DefaultBootstrapServer {
		t.Fatalf("bootstrap server = %q, want %q", cfg.Kafka.BootstrapServer, DefaultBootstrapServer)
	}
	if cfg.Lag.WarningThreshold != DefaultLagWarningThreshold || cfg.Lag.UnhealthyThreshold != DefaultLagUnhealthyThreshold {
		t.Fatalf("lag thresholds = %+v, want %d/%d", cfg.Lag, DefaultLagWarningThreshold, DefaultLagUnhealthyThreshold)
	}
}

func TestLoadReadsLagThresholds(t *testing.T) {
	unsetEnvironment(t, BootstrapServerEnv)
	path := writeConfig(t, "kafka:\n  bootstrap_server: localhost:9092\nlag:\n  warning_threshold: 25\n  unhealthy_threshold: 200\n")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Lag.WarningThreshold != 25 || cfg.Lag.UnhealthyThreshold != 200 {
		t.Fatalf("lag thresholds = %+v, want 25/200", cfg.Lag)
	}
}

func TestLoadReadsYAMLAndAppliesEnvironmentOverride(t *testing.T) {
	unsetEnvironment(t, BootstrapServerEnv)
	path := writeConfig(t, "kafka:\n  bootstrap_server: yaml-broker:9092\n")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.BootstrapServer != "yaml-broker:9092" {
		t.Fatalf("YAML bootstrap server = %q", cfg.Kafka.BootstrapServer)
	}

	t.Setenv(BootstrapServerEnv, "env-broker:9093")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.BootstrapServer != "env-broker:9093" {
		t.Fatalf("environment bootstrap server = %q", cfg.Kafka.BootstrapServer)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	unsetEnvironment(t, BootstrapServerEnv)
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "unknown field", content: "kafka:\n  broker: localhost:9092\n", want: "field broker not found"},
		{name: "missing port", content: "kafka:\n  bootstrap_server: localhost\n", want: "expected host:port"},
		{name: "URL scheme", content: "kafka:\n  bootstrap_server: kafka://localhost:9092\n", want: "expected host:port"},
		{name: "invalid port", content: "kafka:\n  bootstrap_server: localhost:70000\n", want: "port must be between"},
		{name: "multiple documents", content: "kafka: {}\n---\nkafka: {}\n", want: "multiple YAML documents"},
		{name: "negative warning threshold", content: "lag:\n  warning_threshold: -1\n", want: "warning threshold cannot be negative"},
		{name: "negative unhealthy threshold", content: "lag:\n  unhealthy_threshold: -1\n", want: "unhealthy threshold cannot be negative"},
		{name: "equal lag thresholds", content: "lag:\n  warning_threshold: 500\n", want: "warning threshold must be lower"},
		{name: "reversed lag thresholds", content: "lag:\n  warning_threshold: 600\n  unhealthy_threshold: 500\n", want: "warning threshold must be lower"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, test.content))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsMissingExplicitFileAndEmptyEnvironment(t *testing.T) {
	unsetEnvironment(t, BootstrapServerEnv)
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil || !strings.Contains(err.Error(), "read config") {
		t.Fatalf("missing file error = %v", err)
	}

	path := writeConfig(t, "kafka: {}\n")
	t.Setenv(BootstrapServerEnv, "   ")
	_, err = Load(path)
	if err == nil || !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("empty environment error = %v", err)
	}
}

func unsetEnvironment(t *testing.T, key string) {
	t.Helper()
	value, exists := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if exists {
			_ = os.Setenv(key, value)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
