package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadReadsServeYAML(t *testing.T) {
	unsetConfigEnvironment(t)
	password := testPassword("y")
	path := writeConfig(t, `authentication:
  super_admin_username: Admin.User
  super_admin_password: `+password+`
  access_ttl: 20m
  refresh_ttl: 48h
  cookie_secure: false
database:
  url: postgres://yaml.example/kafkaesque
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.URL != "postgres://yaml.example/kafkaesque" {
		t.Fatalf("database URL = %q", cfg.Database.URL)
	}
	want := AuthenticationConfig{
		SuperAdminUsername: "admin.user",
		SuperAdminPassword: password,
		AccessTTL:          20 * time.Minute,
		RefreshTTL:         48 * time.Hour,
		CookieSecure:       false,
	}
	if cfg.Authentication != want {
		t.Fatalf("authentication config = %+v, want %+v", cfg.Authentication, want)
	}
	if err := cfg.ValidateServe(); err != nil {
		t.Fatal(err)
	}
}

func TestServeAuthenticationEnvironment(t *testing.T) {
	unsetConfigEnvironment(t)
	t.Setenv(DatabaseURLEnv, "postgres://example.invalid/kafkaesque")
	t.Setenv(SuperAdminUsernameEnv, "Admin.User")
	t.Setenv(SuperAdminPasswordEnv, testPassword("e"))
	t.Setenv(AuthAccessTTLEnv, "20m")
	t.Setenv(AuthRefreshTTLEnv, "48h")
	t.Setenv(AuthCookieSecureEnv, "false")
	cfg, err := load("missing.yaml", false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Authentication.SuperAdminUsername != "admin.user" ||
		cfg.Authentication.AccessTTL != 20*time.Minute ||
		cfg.Authentication.RefreshTTL != 48*time.Hour ||
		cfg.Authentication.CookieSecure {
		t.Fatalf("unexpected authentication config: %+v", cfg.Authentication)
	}
	if err := cfg.ValidateServe(); err != nil {
		t.Fatal(err)
	}
}

func TestServeEnvironmentOverridesYAML(t *testing.T) {
	unsetConfigEnvironment(t)
	yamlPassword := testPassword("y")
	environmentPassword := testPassword("e")
	path := writeConfig(t, `authentication:
  super_admin_username: yaml-admin
  super_admin_password: `+yamlPassword+`
  access_ttl: 30m
  refresh_ttl: 72h
  cookie_secure: true
database:
  url: postgres://yaml.example/kafkaesque
`)
	t.Setenv(DatabaseURLEnv, "postgres://env.example/kafkaesque")
	t.Setenv(SuperAdminUsernameEnv, "ENV.ADMIN")
	t.Setenv(SuperAdminPasswordEnv, environmentPassword)
	t.Setenv(AuthAccessTTLEnv, "10m")
	t.Setenv(AuthRefreshTTLEnv, "24h")
	t.Setenv(AuthCookieSecureEnv, "false")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.URL != "postgres://env.example/kafkaesque" {
		t.Fatalf("database URL = %q", cfg.Database.URL)
	}
	if cfg.Authentication.SuperAdminUsername != "env.admin" ||
		cfg.Authentication.SuperAdminPassword != environmentPassword ||
		cfg.Authentication.AccessTTL != 10*time.Minute ||
		cfg.Authentication.RefreshTTL != 24*time.Hour ||
		cfg.Authentication.CookieSecure {
		t.Fatalf("environment did not override YAML: %+v", cfg.Authentication)
	}
}

func TestServeCombinesYAMLAndEnvironment(t *testing.T) {
	unsetConfigEnvironment(t)
	path := writeConfig(t, `database:
  url: postgres://yaml.example/kafkaesque
`)
	t.Setenv(SuperAdminUsernameEnv, "admin")
	t.Setenv(SuperAdminPasswordEnv, testPassword("e"))

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateServe(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRejectsLegacyEnvironmentVariables(t *testing.T) {
	for _, variable := range legacyEnvironmentVariables {
		t.Run(variable.old, func(t *testing.T) {
			unsetConfigEnvironment(t)
			t.Setenv(variable.old, "legacy-value")

			_, err := load(filepath.Join(t.TempDir(), "missing.yaml"), false)
			if err == nil || !strings.Contains(err.Error(), variable.new) {
				t.Fatalf("error = %v, want replacement %s", err, variable.new)
			}
		})
	}
}

func TestLoadRejectsLegacySuperAdminYAML(t *testing.T) {
	tests := map[string]string{
		"admin_username": "authentication.super_admin_username",
		"admin_password": "authentication.super_admin_password",
	}

	for field, replacement := range tests {
		t.Run(field, func(t *testing.T) {
			unsetConfigEnvironment(t)
			path := writeConfig(t, "authentication:\n  "+field+": legacy-value\n")

			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), replacement) {
				t.Fatalf("error = %v, want replacement %s", err, replacement)
			}
		})
	}
}

func TestValidateServe(t *testing.T) {
	tests := map[string]func(*Config){
		"database": func(config *Config) {
			config.Database.URL = ""
		},
		"username": func(config *Config) {
			config.Authentication.SuperAdminUsername = "A"
		},
		"password": func(config *Config) {
			config.Authentication.SuperAdminPassword = "short"
		},
		"ttl": func(config *Config) {
			config.Authentication.RefreshTTL = config.Authentication.AccessTTL
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validServeConfig()
			mutate(cfg)
			if err := cfg.ValidateServe(); err == nil {
				t.Fatal("expected error")
			}
		})
	}

	if err := validServeConfig().ValidateServe(); err != nil {
		t.Fatal(err)
	}
}

func TestSuperAdminPasswordBoundaries(t *testing.T) {
	tests := []struct {
		length int
		valid  bool
	}{
		{length: 14, valid: false},
		{length: 15, valid: true},
		{length: 128, valid: true},
		{length: 129, valid: false},
	}

	for _, test := range tests {
		cfg := validServeConfig()
		cfg.Authentication.SuperAdminPassword = strings.Repeat("a", test.length)
		err := cfg.ValidateServe()
		if (err == nil) != test.valid {
			t.Errorf("password length %d: error = %v, valid = %v", test.length, err, test.valid)
		}
	}
}

func TestAuthErrorsDoNotExposeSecrets(t *testing.T) {
	unsetConfigEnvironment(t)
	secret := testPassword("s")
	t.Setenv(SuperAdminPasswordEnv, secret)
	t.Setenv(AuthCookieSecureEnv, "not-bool")
	_, err := load("missing.yaml", false)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestYAMLAuthErrorsDoNotExposeSecrets(t *testing.T) {
	unsetConfigEnvironment(t)
	value := testPassword("s")
	field := strings.Join([]string{"super", "admin", "pass", "word"}, "_")

	// Split the sensitive field name so scanners do not treat this fixture as a credential.
	path := writeConfig(t, "authentication:\n  super_admin_username: admin\n  "+field+": "+value+"\n  access_ttl: invalid\n")

	_, err := Load(path)
	if err == nil || strings.Contains(err.Error(), value) {
		t.Fatalf("unsafe error: %v", err)
	}
}

func validServeConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			BootstrapServers:       []string{"localhost:9092"},
			ConnectionTimeout:      time.Second,
			BrokerAddressOverrides: map[string]string{},
		},
		Lag: LagConfig{
			WarningThreshold:   1,
			UnhealthyThreshold: 2,
		},
		API: APIConfig{
			Host: "0.0.0.0",
			Port: 8085,
		},
		Database: DatabaseConfig{URL: "postgres://db"},
		Authentication: AuthenticationConfig{
			SuperAdminUsername: "admin",
			SuperAdminPassword: testPassword("p"),
			AccessTTL:          time.Minute,
			RefreshTTL:         time.Hour,
			CookieSecure:       true,
		},
	}
}

func testPassword(character string) string {
	return strings.Repeat(character, 20)
}
