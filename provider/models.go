package provider

import "time"

type RoutingStrategy string

const (
	StrategyBalanced    RoutingStrategy = "balanced"
	StrategyCostFirst   RoutingStrategy = "cost_first"
	StrategyPerformance RoutingStrategy = "performance"
	StrategyRoundRobin  RoutingStrategy = "round_robin"
)

type ProviderBackend string

const (
	BackendOpenAI           ProviderBackend = "openai"
	BackendAnthropic        ProviderBackend = "anthropic"
	BackendGemini           ProviderBackend = "gemini"
	BackendOllama           ProviderBackend = "ollama"
	BackendOpenAICompatible ProviderBackend = "openai_compatible"
)

type ProviderConfig struct {
	Name         string            `yaml:"name" json:"name"`
	Kind         string            `yaml:"kind" json:"kind"`
	Label        string            `yaml:"label,omitempty" json:"label,omitempty"`
	BaseURL      string            `yaml:"base_url" json:"base_url"`
	APIKey       string            `yaml:"api_key" json:"api_key"`
	APISecret    string            `yaml:"api_secret,omitempty" json:"api_secret,omitempty"`
	ExtraHeaders map[string]string `yaml:"extra_headers,omitempty" json:"extra_headers,omitempty"`
	ExtraBody    map[string]any    `yaml:"extra_body,omitempty" json:"extra_body,omitempty"`
	Models       []string          `yaml:"models" json:"models"`
	DefaultModel string            `yaml:"default_model,omitempty" json:"default_model,omitempty"`
	TimeoutMs    int               `yaml:"timeout_ms,omitempty" json:"timeout_ms,omitempty"`
	MaxRetries   int               `yaml:"max_retries,omitempty" json:"max_retries,omitempty"`
	Enabled      bool              `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	Default      bool              `yaml:"default,omitempty" json:"default,omitempty"`
	Pricing      *TokenPricing     `yaml:"pricing,omitempty" json:"pricing,omitempty"`
	Proxy        string            `yaml:"proxy,omitempty" json:"proxy,omitempty"`
	Backend      ProviderBackend   `yaml:"backend,omitempty" json:"backend,omitempty"`
	ModelAliases map[string]string `yaml:"model_aliases,omitempty" json:"model_aliases,omitempty"`
	// P1: Production hardening
	MaxConcurrency      int  `yaml:"max_concurrency,omitempty" json:"max_concurrency,omitempty"`
	AdaptiveConcurrency bool `yaml:"adaptive_concurrency,omitempty" json:"adaptive_concurrency,omitempty"`
	RateLimitRPM        int  `yaml:"rate_limit_rpm,omitempty" json:"rate_limit_rpm,omitempty"`
	RateLimitTPM        int  `yaml:"rate_limit_tpm,omitempty" json:"rate_limit_tpm,omitempty"`
	RetryBudget         int  `yaml:"retry_budget,omitempty" json:"retry_budget,omitempty"`
}

func (c *ProviderConfig) ResolveModel(requested string) string {
	if requested != "" && requested != "auto" {
		return requested
	}
	return c.DefaultModel
}

func (c *ProviderConfig) Timeout() time.Duration {
	if c.TimeoutMs <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.TimeoutMs) * time.Millisecond
}

type TokenPricing struct {
	InputPrice  float64 `yaml:"input_price" json:"input_price"`
	OutputPrice float64 `yaml:"output_price" json:"output_price"`
	Currency    string  `yaml:"currency,omitempty" json:"currency,omitempty"`
}

func (p *TokenPricing) Cost(promptTokens, completionTokens int) float64 {
	if p == nil {
		return 0
	}
	return (float64(promptTokens)/1_000_000)*p.InputPrice +
		(float64(completionTokens)/1_000_000)*p.OutputPrice
}

type HealthStatus struct {
	Healthy   bool      `json:"healthy"`
	LatencyMs int64     `json:"latency_ms"`
	LastCheck time.Time `json:"last_check"`
	Error     string    `json:"error,omitempty"`
}

type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

type ProviderSnapshot struct {
	Name      string       `json:"name"`
	Kind      string       `json:"kind"`
	Active    bool         `json:"active"`
	Models    []string     `json:"models"`
	Healthy   bool         `json:"healthy"`
	Circuit   CircuitState `json:"circuit_state"`
	KeyConfigured bool     `json:"key_configured"`
	LatencyMs int64        `json:"latency_ms,omitempty"`
}

