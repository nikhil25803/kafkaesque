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
	DefaultBootstrapServer = "localhost:9092"
	BootstrapServerEnv     = "KAFKAESQUE_BOOTSTRAP_SERVER"
)

// Config contains Kafkaesque's runtime configuration.
type Config struct {
	Kafka KafkaConfig `yaml:"kafka"`
}

// KafkaConfig contains Kafka connection settings.
type KafkaConfig struct {
	BootstrapServer string `yaml:"bootstrap_server"`
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
	cfg := &Config{Kafka: KafkaConfig{BootstrapServer: DefaultBootstrapServer}}

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

	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("invalid Kafka bootstrap server %q: expected host:port", address)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("invalid Kafka bootstrap server %q: port must be between 1 and 65535", address)
	}
	return nil
}
