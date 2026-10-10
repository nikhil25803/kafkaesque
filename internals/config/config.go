package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultBootstrapServer       = "localhost:9092"
	DefaultLagWarningThreshold   = int64(100)
	DefaultLagUnhealthyThreshold = int64(500)
	BootstrapServerEnv           = "KAFKAESQUE_BOOTSTRAP_SERVER"
)

// Config contains Kafkaesque's runtime configuration.
type Config struct {
	Kafka KafkaConfig `yaml:"kafka"`
	Lag   LagConfig   `yaml:"lag"`
}

// KafkaConfig contains Kafka connection settings.
type KafkaConfig struct {
	BootstrapServer        string            `yaml:"bootstrap_server"`
	BrokerAddressOverrides map[string]string `yaml:"broker_address_overrides"`
}

// LagConfig contains consumer lag health thresholds.
type LagConfig struct {
	WarningThreshold   int64 `yaml:"warning_threshold"`
	UnhealthyThreshold int64 `yaml:"unhealthy_threshold"`
}

// DefaultPath returns the platform-specific default configuration path.
func DefaultPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("find user config directory: %w", err)
	}
	return filepath.Join(dir, "kafkaesque", "config.yaml"), nil
}

// Load reads, resolves, and validates Kafkaesque configuration.
func Load(path string) (*Config, error) {
	explicit := path != ""
	if !explicit {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return nil, err
		}
	}

	return load(path, explicit)
}

func load(path string, explicit bool) (*Config, error) {
	cfg := &Config{
		Kafka: KafkaConfig{
			BootstrapServer:        DefaultBootstrapServer,
			BrokerAddressOverrides: map[string]string{},
		},
		Lag: LagConfig{
			WarningThreshold:   DefaultLagWarningThreshold,
			UnhealthyThreshold: DefaultLagUnhealthyThreshold,
		},
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if !explicit && errors.Is(err, os.ErrNotExist) {
			data = nil
		} else {
			return nil, fmt.Errorf("read config %q: %w", path, err)
		}
	}

	if len(bytes.TrimSpace(data)) > 0 {
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(cfg); err != nil {
			return nil, fmt.Errorf("parse config %q: %w", path, err)
		}
		if err := rejectAdditionalDocuments(decoder, path); err != nil {
			return nil, err
		}
	}

	if value, ok := os.LookupEnv(BootstrapServerEnv); ok {
		cfg.Kafka.BootstrapServer = value
	}
	cfg.Kafka.BootstrapServer = strings.TrimSpace(cfg.Kafka.BootstrapServer)
	overrides, err := normalizeBrokerAddressOverrides(cfg.Kafka.BrokerAddressOverrides)
	if err != nil {
		return nil, err
	}
	cfg.Kafka.BrokerAddressOverrides = overrides

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func rejectAdditionalDocuments(decoder *yaml.Decoder, path string) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return fmt.Errorf("parse config %q: %w", path, err)
		}
		return fmt.Errorf("parse config %q: multiple YAML documents are not supported", path)
	}
	return nil
}

// Validate checks the effective configuration.
func (c *Config) Validate() error {
	address := c.Kafka.BootstrapServer
	if address == "" {
		return fmt.Errorf("kafka bootstrap server cannot be empty")
	}
	if err := validateKafkaAddress(address, "bootstrap server"); err != nil {
		return err
	}
	for advertised, connect := range c.Kafka.BrokerAddressOverrides {
		if err := validateKafkaAddress(advertised, "broker address override source"); err != nil {
			return err
		}
		if err := validateKafkaAddress(connect, "broker address override destination"); err != nil {
			return err
		}
	}
	if c.Lag.WarningThreshold < 0 {
		return fmt.Errorf("lag warning threshold cannot be negative")
	}
	if c.Lag.UnhealthyThreshold < 0 {
		return fmt.Errorf("lag unhealthy threshold cannot be negative")
	}
	if c.Lag.WarningThreshold >= c.Lag.UnhealthyThreshold {
		return fmt.Errorf("lag warning threshold must be lower than unhealthy threshold")
	}
	return nil
}

func normalizeBrokerAddressOverrides(overrides map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(overrides))
	for advertised, connect := range overrides {
		advertised = strings.TrimSpace(advertised)
		connect = strings.TrimSpace(connect)
		if _, exists := normalized[advertised]; exists {
			return nil, fmt.Errorf("duplicate Kafka broker address override source %q", advertised)
		}
		normalized[advertised] = connect
	}
	return normalized, nil
}

func validateKafkaAddress(address, name string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || port == "" {
		return fmt.Errorf("invalid Kafka %s %q: expected host:port", name, address)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("invalid Kafka %s %q: port must be between 1 and 65535", name, address)
	}
	return nil
}
