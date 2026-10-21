package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nikhil25803/kafkaesque/internals/credentials"
	"gopkg.in/yaml.v3"
)

const (
	DefaultBootstrapServer       = "localhost:9092"
	DefaultConnectionTimeout     = 10 * time.Second
	DefaultLagWarningThreshold   = int64(100)
	DefaultLagUnhealthyThreshold = int64(500)
	DefaultAPIHost               = "0.0.0.0"
	DefaultAPIPort               = 8085
	DefaultAuthAccessTTL         = 15 * time.Minute
	DefaultAuthRefreshTTL        = 7 * 24 * time.Hour
	DefaultSuperAdminRecovery    = "verify"
)

const (
	BootstrapServerEnv        = "KAFKAESQUE_KAFKA_BOOTSTRAP_SERVER"
	BootstrapServersEnv       = "KAFKAESQUE_KAFKA_BOOTSTRAP_SERVERS"
	ConnectionTimeoutEnv      = "KAFKAESQUE_KAFKA_CONNECTION_TIMEOUT"
	BrokerAddressOverridesEnv = "KAFKAESQUE_KAFKA_BROKER_ADDRESS_OVERRIDES"
	TLSEnabledEnv             = "KAFKAESQUE_KAFKA_SECURITY_TLS_ENABLED"
	TLSCAFileEnv              = "KAFKAESQUE_KAFKA_SECURITY_TLS_CA_FILE"
	TLSClientCertFileEnv      = "KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_CERT_FILE"
	TLSClientKeyFileEnv       = "KAFKAESQUE_KAFKA_SECURITY_TLS_CLIENT_KEY_FILE"
	TLSServerNameEnv          = "KAFKAESQUE_KAFKA_SECURITY_TLS_SERVER_NAME"
	SASLMechanismEnv          = "KAFKAESQUE_KAFKA_SECURITY_SASL_MECHANISM"
	SASLUsernameEnv           = "KAFKAESQUE_KAFKA_SECURITY_SASL_USERNAME"
	SASLPasswordEnv           = "KAFKAESQUE_KAFKA_SECURITY_SASL_PASSWORD"
)

const (
	LagWarningThresholdEnv   = "KAFKAESQUE_LAG_WARNING_THRESHOLD"
	LagUnhealthyThresholdEnv = "KAFKAESQUE_LAG_UNHEALTHY_THRESHOLD"
)

const (
	SuperAdminUsernameEnv = "KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_USERNAME"
	SuperAdminEmailEnv    = "KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_EMAIL"
	SuperAdminPasswordEnv = "KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_PASSWORD"
	SuperAdminRecoveryEnv = "KAFKAESQUE_AUTHENTICATION_SUPER_ADMIN_RECOVERY"
	AuthAccessTTLEnv      = "KAFKAESQUE_AUTHENTICATION_ACCESS_TTL"
	AuthRefreshTTLEnv     = "KAFKAESQUE_AUTHENTICATION_REFRESH_TTL"
	AuthCookieSecureEnv   = "KAFKAESQUE_AUTHENTICATION_COOKIE_SECURE"
)

const DatabaseURLEnv = "KAFKAESQUE_DATABASE_URL"

const (
	APIHostEnv = "KAFKAESQUE_API_HOST"
	APIPortEnv = "KAFKAESQUE_API_PORT"
)

type legacyEnvironmentVariable struct {
	old string
	new string
}

var legacyEnvironmentVariables = []legacyEnvironmentVariable{
	{old: "KAFKAESQUE_BOOTSTRAP_SERVER", new: BootstrapServerEnv},
	{old: "KAFKAESQUE_BOOTSTRAP_SERVERS", new: BootstrapServersEnv},
	{old: "KAFKAESQUE_CONNECTION_TIMEOUT", new: ConnectionTimeoutEnv},
	{old: "KAFKAESQUE_BROKER_ADDRESS_OVERRIDES", new: BrokerAddressOverridesEnv},
	{old: "KAFKAESQUE_TLS_ENABLED", new: TLSEnabledEnv},
	{old: "KAFKAESQUE_TLS_CA_FILE", new: TLSCAFileEnv},
	{old: "KAFKAESQUE_TLS_CLIENT_CERT_FILE", new: TLSClientCertFileEnv},
	{old: "KAFKAESQUE_TLS_CLIENT_KEY_FILE", new: TLSClientKeyFileEnv},
	{old: "KAFKAESQUE_TLS_SERVER_NAME", new: TLSServerNameEnv},
	{old: "KAFKAESQUE_SASL_MECHANISM", new: SASLMechanismEnv},
	{old: "KAFKAESQUE_SASL_USERNAME", new: SASLUsernameEnv},
	{old: "KAFKAESQUE_SASL_PASSWORD", new: SASLPasswordEnv},
	{old: "KAFKAESQUE_ADMIN_USERNAME", new: SuperAdminUsernameEnv},
	{old: "KAFKAESQUE_ADMIN_PASSWORD", new: SuperAdminPasswordEnv},
	{old: "KAFKAESQUE_AUTHENTICATION_ADMIN_USERNAME", new: SuperAdminUsernameEnv},
	{old: "KAFKAESQUE_AUTHENTICATION_ADMIN_PASSWORD", new: SuperAdminPasswordEnv},
	{old: "KAFKAESQUE_AUTH_ACCESS_TTL", new: AuthAccessTTLEnv},
	{old: "KAFKAESQUE_AUTH_REFRESH_TTL", new: AuthRefreshTTLEnv},
	{old: "KAFKAESQUE_AUTH_COOKIE_SECURE", new: AuthCookieSecureEnv},
}

// Config contains Kafkaesque's runtime configuration.
type Config struct {
	Kafka          KafkaConfig          `yaml:"kafka"`
	Lag            LagConfig            `yaml:"lag"`
	Authentication AuthenticationConfig `yaml:"authentication"`
	Database       DatabaseConfig       `yaml:"database"`
	API            APIConfig            `yaml:"api"`
}

// AuthenticationConfig contains Super Admin and session settings.
type AuthenticationConfig struct {
	SuperAdminUsername string        `yaml:"super_admin_username"`
	SuperAdminEmail    string        `yaml:"super_admin_email"`
	SuperAdminPassword string        `yaml:"super_admin_password"`
	SuperAdminRecovery string        `yaml:"super_admin_recovery"`
	AccessTTL          time.Duration `yaml:"access_ttl"`
	RefreshTTL         time.Duration `yaml:"refresh_ttl"`
	CookieSecure       bool          `yaml:"cookie_secure"`
}

// DatabaseConfig contains PostgreSQL connection settings.
type DatabaseConfig struct {
	URL string `yaml:"url"`
}

// APIConfig contains HTTP API listener settings.
type APIConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
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
		Authentication: AuthenticationConfig{
			SuperAdminRecovery: DefaultSuperAdminRecovery,
			AccessTTL:          DefaultAuthAccessTTL,
			RefreshTTL:         DefaultAuthRefreshTTL,
			CookieSecure:       true,
		},
		API: APIConfig{
			Host: DefaultAPIHost,
			Port: DefaultAPIPort,
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
			if replacement := legacyYAMLReplacement(err); replacement != "" {
				return nil, fmt.Errorf("parse config %q: %s", path, replacement)
			}
			return nil, fmt.Errorf("parse config %q: %w", path, err)
		}
		if err := rejectAdditionalDocuments(decoder, path); err != nil {
			return nil, err
		}
	}

	if cfg.Kafka.BootstrapServer != "" && len(cfg.Kafka.BootstrapServers) > 0 {
		return nil, fmt.Errorf("kafka bootstrap_server and bootstrap_servers cannot both be set")
	}

	normalizeSecurity(&cfg.Kafka.Security, filepath.Dir(path))
	if err := applyEnvironment(cfg); err != nil {
		return nil, err
	}

	if len(cfg.Kafka.BootstrapServers) == 0 {
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

	normalizeSecurityValues(&cfg.Kafka.Security)
	cfg.Authentication.SuperAdminUsername = strings.ToLower(strings.TrimSpace(cfg.Authentication.SuperAdminUsername))
	cfg.Authentication.SuperAdminEmail = strings.ToLower(strings.TrimSpace(cfg.Authentication.SuperAdminEmail))
	cfg.Authentication.SuperAdminRecovery = strings.ToLower(strings.TrimSpace(cfg.Authentication.SuperAdminRecovery))
	cfg.Database.URL = strings.TrimSpace(cfg.Database.URL)

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

// legacyYAMLReplacement explains renamed authentication keys.
func legacyYAMLReplacement(err error) string {
	message := err.Error()
	switch {
	case strings.Contains(message, "field admin_username not found"):
		return "authentication.admin_username was renamed to authentication.super_admin_username"
	case strings.Contains(message, "field admin_password not found"):
		return "authentication.admin_password was renamed to authentication.super_admin_password"
	default:
		return ""
	}
}

func applyEnvironment(cfg *Config) error {
	if err := rejectLegacyEnvironment(); err != nil {
		return err
	}

	// Kafka connection.
	bootstrapServer, singularSet := os.LookupEnv(BootstrapServerEnv)
	bootstrapServers, pluralSet := os.LookupEnv(BootstrapServersEnv)
	if singularSet && pluralSet {
		return fmt.Errorf("%s and %s cannot both be set", BootstrapServerEnv, BootstrapServersEnv)
	}
	if singularSet {
		cfg.Kafka.BootstrapServer = ""
		cfg.Kafka.BootstrapServers = []string{bootstrapServer}
	}
	if pluralSet {
		cfg.Kafka.BootstrapServer = ""
		cfg.Kafka.BootstrapServers = strings.Split(bootstrapServers, ",")
	}

	if value, ok := os.LookupEnv(ConnectionTimeoutEnv); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", ConnectionTimeoutEnv, err)
		}
		cfg.Kafka.ConnectionTimeout = parsed
	}
	if value, ok := os.LookupEnv(BrokerAddressOverridesEnv); ok {
		var overrides map[string]string
		if err := json.Unmarshal([]byte(value), &overrides); err != nil {
			return fmt.Errorf("parse %s: %w", BrokerAddressOverridesEnv, err)
		}
		if overrides == nil {
			return fmt.Errorf("parse %s: expected a JSON object", BrokerAddressOverridesEnv)
		}
		cfg.Kafka.BrokerAddressOverrides = overrides
	}

	// Kafka TLS.
	if value, ok := os.LookupEnv(TLSEnabledEnv); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", TLSEnabledEnv, err)
		}
		cfg.Kafka.Security.TLS.Enabled = parsed
	}
	if value, ok := os.LookupEnv(TLSCAFileEnv); ok {
		cfg.Kafka.Security.TLS.CAFile = value
	}
	if value, ok := os.LookupEnv(TLSClientCertFileEnv); ok {
		cfg.Kafka.Security.TLS.ClientCertFile = value
	}
	if value, ok := os.LookupEnv(TLSClientKeyFileEnv); ok {
		cfg.Kafka.Security.TLS.ClientKeyFile = value
	}
	if value, ok := os.LookupEnv(TLSServerNameEnv); ok {
		cfg.Kafka.Security.TLS.ServerName = value
	}

	// Kafka SASL.
	if value, ok := os.LookupEnv(SASLMechanismEnv); ok {
		cfg.Kafka.Security.SASL.Mechanism = value
	}
	if value, ok := os.LookupEnv(SASLUsernameEnv); ok {
		cfg.Kafka.Security.SASL.Username = value
	}
	if value, ok := os.LookupEnv(SASLPasswordEnv); ok {
		cfg.Kafka.Security.SASL.Password = value
	}

	// Consumer lag.
	if value, ok := os.LookupEnv(LagWarningThresholdEnv); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse %s: %w", LagWarningThresholdEnv, err)
		}
		cfg.Lag.WarningThreshold = parsed
	}
	if value, ok := os.LookupEnv(LagUnhealthyThresholdEnv); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse %s: %w", LagUnhealthyThresholdEnv, err)
		}
		cfg.Lag.UnhealthyThreshold = parsed
	}

	// API listener.
	if value, ok := os.LookupEnv(APIHostEnv); ok {
		cfg.API.Host = value
	}
	if value, ok := os.LookupEnv(APIPortEnv); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", APIPortEnv, err)
		}
		cfg.API.Port = parsed
	}

	// PostgreSQL.
	if value, ok := os.LookupEnv(DatabaseURLEnv); ok {
		cfg.Database.URL = strings.TrimSpace(value)
	}

	// Authentication.
	if value, ok := os.LookupEnv(SuperAdminUsernameEnv); ok {
		cfg.Authentication.SuperAdminUsername = strings.ToLower(strings.TrimSpace(value))
	}
	if value, ok := os.LookupEnv(SuperAdminEmailEnv); ok {
		cfg.Authentication.SuperAdminEmail = strings.ToLower(strings.TrimSpace(value))
	}
	if value, ok := os.LookupEnv(SuperAdminPasswordEnv); ok {
		cfg.Authentication.SuperAdminPassword = value
	}
	if value, ok := os.LookupEnv(SuperAdminRecoveryEnv); ok {
		cfg.Authentication.SuperAdminRecovery = strings.ToLower(strings.TrimSpace(value))
	}
	if value, ok := os.LookupEnv(AuthAccessTTLEnv); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", AuthAccessTTLEnv, err)
		}
		cfg.Authentication.AccessTTL = parsed
	}
	if value, ok := os.LookupEnv(AuthRefreshTTLEnv); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", AuthRefreshTTLEnv, err)
		}
		cfg.Authentication.RefreshTTL = parsed
	}
	if value, ok := os.LookupEnv(AuthCookieSecureEnv); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse %s: %w", AuthCookieSecureEnv, err)
		}
		cfg.Authentication.CookieSecure = parsed
	}

	return nil
}

func rejectLegacyEnvironment() error {
	for _, variable := range legacyEnvironmentVariables {
		if _, exists := os.LookupEnv(variable.old); exists {
			return fmt.Errorf("%s was removed; use %s", variable.old, variable.new)
		}
	}

	return nil
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

// ValidateServe validates settings required only by the HTTP server.
func (c *Config) ValidateServe() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.Database.URL) == "" {
		return fmt.Errorf("database.url or %s is required", DatabaseURLEnv)
	}
	username, usernameValid := credentials.NormalizeUsername(c.Authentication.SuperAdminUsername)
	if !usernameValid {
		return fmt.Errorf(
			"authentication.super_admin_username or %s must be a 3-64 character handle or the configured email",
			SuperAdminUsernameEnv,
		)
	}
	email, emailValid := credentials.NormalizeEmail(c.Authentication.SuperAdminEmail)
	if !emailValid {
		return fmt.Errorf("authentication.super_admin_email or %s must be a valid email address", SuperAdminEmailEnv)
	}
	if strings.Contains(username, "@") && username != email {
		return fmt.Errorf(
			"authentication.super_admin_username must match authentication.super_admin_email when it is an email address",
		)
	}
	if err := credentials.ValidatePassword(c.Authentication.SuperAdminPassword); err != nil {
		return fmt.Errorf("authentication.super_admin_password or %s %s", SuperAdminPasswordEnv, err)
	}
	if recovery := c.Authentication.SuperAdminRecovery; recovery != "verify" && recovery != "update" && recovery != "reinitialize" {
		return fmt.Errorf(
			"authentication.super_admin_recovery or %s must be verify, update, or reinitialize",
			SuperAdminRecoveryEnv,
		)
	}
	if c.Authentication.AccessTTL <= 0 {
		return fmt.Errorf("authentication.access_ttl or %s must be positive", AuthAccessTTLEnv)
	}
	if c.Authentication.RefreshTTL <= c.Authentication.AccessTTL {
		return fmt.Errorf("authentication.refresh_ttl or %s must exceed authentication.access_ttl", AuthRefreshTTLEnv)
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
	if strings.TrimSpace(c.API.Host) == "" {
		return fmt.Errorf("api host cannot be empty")
	}
	if c.API.Port < 1 || c.API.Port > 65535 {
		return fmt.Errorf("api port must be between 1 and 65535")
	}
	if err := validateSecurity(c.Kafka.Security); err != nil {
		return err
	}
	return nil
}

func normalizeSecurity(security *SecurityConfig, configDir string) {
	normalizeSecurityValues(security)
	security.TLS.CAFile = resolveConfigPath(configDir, security.TLS.CAFile)
	security.TLS.ClientCertFile = resolveConfigPath(configDir, security.TLS.ClientCertFile)
	security.TLS.ClientKeyFile = resolveConfigPath(configDir, security.TLS.ClientKeyFile)
}

func normalizeSecurityValues(security *SecurityConfig) {
	security.SASL.Mechanism = strings.ToUpper(strings.TrimSpace(security.SASL.Mechanism))
	security.SASL.Username = strings.TrimSpace(security.SASL.Username)
	security.TLS.CAFile = strings.TrimSpace(security.TLS.CAFile)
	security.TLS.ClientCertFile = strings.TrimSpace(security.TLS.ClientCertFile)
	security.TLS.ClientKeyFile = strings.TrimSpace(security.TLS.ClientKeyFile)
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
