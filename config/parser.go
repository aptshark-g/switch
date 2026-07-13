package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/aptshark/gateway/provider"
)

type GatewayConfig struct {
	Providers []provider.ProviderConfig `json:"providers" yaml:"providers"`
	Defaults  DefaultsConfig            `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Server    ServerConfig              `json:"server,omitempty" yaml:"server,omitempty"`
	Auth      AuthConfig                `json:"auth,omitempty" yaml:"auth,omitempty"`
}

type DefaultsConfig struct {
	TimeoutMs  int    `json:"timeout_ms,omitempty" yaml:"timeout_ms,omitempty"`
	MaxRetries int    `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
	Proxy      string `json:"proxy,omitempty" yaml:"proxy,omitempty"`
}

type ServerConfig struct {
	Host         string `json:"host,omitempty" yaml:"host,omitempty"`
	Port         int    `json:"port,omitempty" yaml:"port,omitempty"`
	ReadTimeout  int    `json:"read_timeout_ms,omitempty" yaml:"read_timeout_ms,omitempty"`
	WriteTimeout int    `json:"write_timeout_ms,omitempty" yaml:"write_timeout_ms,omitempty"`
	TLSCert      string `json:"tls_cert,omitempty" yaml:"tls_cert,omitempty"`
	TLSKey       string `json:"tls_key,omitempty" yaml:"tls_key,omitempty"`
}

type AuthConfig struct {
	Enabled    bool     `json:"enabled" yaml:"enabled"`
	APIKeys    []string `json:"api_keys" yaml:"api_keys"`
	AdminToken string   `json:"admin_token" yaml:"admin_token"`
}

func ParseFile(path string) (*GatewayConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	return Parse(data)
}

func Parse(data []byte) (*GatewayConfig, error) {
	cfg := &GatewayConfig{}
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "{") {
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("config: parse json: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("config: parse yaml: %w", err)
		}
	}
	applyDefaults(cfg)
	expandEnvVars(cfg)
	if err := validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}


func expandEnvVars(cfg *GatewayConfig) {
	for i := range cfg.Providers {
		p := &cfg.Providers[i]
		p.APIKey = os.ExpandEnv(p.APIKey)
		p.BaseURL = os.ExpandEnv(p.BaseURL)
	}
	for i := range cfg.Auth.APIKeys {
		cfg.Auth.APIKeys[i] = os.ExpandEnv(cfg.Auth.APIKeys[i])
	}
	cfg.Auth.AdminToken = os.ExpandEnv(cfg.Auth.AdminToken)
}

func applyDefaults(cfg *GatewayConfig) {
	for i := range cfg.Providers {
		p := &cfg.Providers[i]
		if p.TimeoutMs <= 0 {
			p.TimeoutMs = cfg.Defaults.TimeoutMs
		}
		if p.TimeoutMs <= 0 {
			p.TimeoutMs = 30000
		}
		if p.MaxRetries <= 0 {
			p.MaxRetries = cfg.Defaults.MaxRetries
		}
		if p.MaxRetries <= 0 {
			p.MaxRetries = 2
		}
	}
}

func validate(cfg *GatewayConfig) error {
	for _, p := range cfg.Providers {
		if p.Name == "" {
			return fmt.Errorf("config: provider requires a name")
		}
		if p.BaseURL == "" && p.Kind != "mock" {
			return fmt.Errorf("config: provider %q requires base_url", p.Name)
		}
	}
	return nil
}

