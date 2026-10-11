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
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultBootstrapServer       = "localhost:9092"
	DefaultConnectionTimeout     = 10 * time.Second
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
	BootstrapServers       []string          `yaml:"bootstrap_servers"`
	ConnectionTimeout      time.Duration     `yaml:"connection_timeout"`
	BrokerAddressOverrides map[string]string `yaml:"broker_address_overrides"`
	Security               SecurityConfig    `yaml:"security"`
}

// SecurityConfig contains Kafka transport security settings.
type SecurityConfig struct {
	TLS  TLSConfig  `yaml:"tls"`
	SASL SASLConfig `yaml:"sasl"`
}

// TLSConfig contains TLS and optional mutual-TLS settings.
type TLSConfig struct {
	Enabled        bool   `yaml:"enabled"`
	CAFile         string `yaml:"ca_file"`
	ClientCertFile string `yaml:"client_cert_file"`
	ClientKeyFile  string `yaml:"client_key_file"`
	ServerName     string `yaml:"server_name"`
}

// SASLConfig contains SASL credentials. A mechanism enables SASL.
type SASLConfig struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
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
			ConnectionTimeout:      DefaultConnectionTimeout,
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

	if cfg.Kafka.BootstrapServer != "" && len(cfg.Kafka.BootstrapServers) > 0 {
		return nil, fmt.Errorf("kafka bootstrap_server and bootstrap_servers cannot both be set")
	}
	if value, ok := os.LookupEnv(BootstrapServerEnv); ok {
		cfg.Kafka.BootstrapServers = []string{value}
	} else if len(cfg.Kafka.BootstrapServers) == 0 {
		if cfg.Kafka.BootstrapServer == "" {
			cfg.Kafka.BootstrapServers = []string{DefaultBootstrapServer}
		} else {
			cfg.Kafka.BootstrapServers = []string{cfg.Kafka.BootstrapServer}
		}
	}
	cfg.Kafka.BootstrapServer = ""
	for i := range cfg.Kafka.BootstrapServers {
		cfg.Kafka.BootstrapServers[i] = strings.TrimSpace(cfg.Kafka.BootstrapServers[i])
	}
	normalizeSecurity(&cfg.Kafka.Security, filepath.Dir(path))
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
	if len(c.Kafka.BootstrapServers) == 0 {
		return fmt.Errorf("kafka bootstrap servers cannot be empty")
	}
	seen := make(map[string]struct{}, len(c.Kafka.BootstrapServers))
	for _, address := range c.Kafka.BootstrapServers {
		if address == "" {
			return fmt.Errorf("kafka bootstrap server cannot be empty")
		}
		if err := validateKafkaAddress(address, "bootstrap server"); err != nil {
			return err
		}
		if _, exists := seen[address]; exists {
			return fmt.Errorf("duplicate Kafka bootstrap server %q", address)
		}
		seen[address] = struct{}{}
	}
	if c.Kafka.ConnectionTimeout <= 0 {
		return fmt.Errorf("kafka connection timeout must be positive")
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
	if err := validateSecurity(c.Kafka.Security); err != nil {
		return err
	}
	return nil
}

func normalizeSecurity(security *SecurityConfig, configDir string) {
	security.SASL.Mechanism = strings.ToUpper(strings.TrimSpace(security.SASL.Mechanism))
	security.SASL.Username = strings.TrimSpace(security.SASL.Username)
	security.TLS.CAFile = resolveConfigPath(configDir, strings.TrimSpace(security.TLS.CAFile))
	security.TLS.ClientCertFile = resolveConfigPath(configDir, strings.TrimSpace(security.TLS.ClientCertFile))
	security.TLS.ClientKeyFile = resolveConfigPath(configDir, strings.TrimSpace(security.TLS.ClientKeyFile))
	security.TLS.ServerName = strings.TrimSpace(security.TLS.ServerName)
}

func resolveConfigPath(configDir, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Clean(filepath.Join(configDir, path))
}

func validateSecurity(security SecurityConfig) error {
	tlsConfigured := security.TLS.CAFile != "" || security.TLS.ClientCertFile != "" || security.TLS.ClientKeyFile != "" || security.TLS.ServerName != ""
	if tlsConfigured && !security.TLS.Enabled {
		return fmt.Errorf("kafka TLS settings require tls.enabled")
	}
	if (security.TLS.ClientCertFile == "") != (security.TLS.ClientKeyFile == "") {
		return fmt.Errorf("kafka TLS client certificate and key must be provided together")
	}

	sasl := security.SASL
	if sasl.Mechanism == "" {
		if sasl.Username != "" || sasl.Password != "" {
			return fmt.Errorf("kafka SASL credentials require a mechanism")
		}
		return nil
	}
	switch sasl.Mechanism {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		return fmt.Errorf("unsupported Kafka SASL mechanism %q", sasl.Mechanism)
	}
	if sasl.Username == "" || sasl.Password == "" {
		return fmt.Errorf("kafka SASL username and password are required")
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
