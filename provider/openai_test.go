package provider

import (
	"encoding/json"
	"testing"
)

func TestResolveModelAlias(t *testing.T) {
	cfg := ProviderConfig{
		DefaultModel: "deepseek-v4-flash",
		ModelAliases: map[string]string{
			"fast": "deepseek-v4-flash",
			"pro":  "deepseek-v4-pro",
		},
	}
	cases := []struct{ in, want string }{
		{"fast", "deepseek-v4-flash"},
		{"pro", "deepseek-v4-pro"},
		{"", "deepseek-v4-flash"},
		{"auto", "deepseek-v4-flash"},
		{"gpt-4o", "gpt-4o"},
	}
	for _, c := range cases {
		if got := cfg.ResolveModelAlias(c.in); got != c.want {
			t.Fatalf("ResolveModelAlias(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBuildRequestBodyAliasResolved(t *testing.T) {
	p, err := NewOpenAIProvider(ProviderConfig{
		Name: "ds", Kind: "openai_compatible", BaseURL: "http://x",
		APIKey: "k", DefaultModel: "deepseek-v4-flash",
		ModelAliases: map[string]string{"fast": "deepseek-v4-flash"},
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	body, err := p.buildRequestBody(&GenerateRequest{
		Model:    "fast",
		Messages: []Message{{Role: "user", Content: "hi"}},
	}, true)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw["model"] != "deepseek-v4-flash" {
		t.Fatalf("model = %v, want alias resolved to deepseek-v4-flash", raw["model"])
	}
}

func TestBuildRequestBodyThinkingOverride(t *testing.T) {
	p, _ := NewOpenAIProvider(ProviderConfig{
		Name: "ds", Kind: "openai_compatible", BaseURL: "http://x",
		APIKey: "k", DefaultModel: "m",
		Thinking: map[string]any{"type": "enabled"},
	})
	body, _ := p.buildRequestBody(&GenerateRequest{
		Model:    "m",
		Messages: []Message{{Role: "user", Content: "hi"}},
		// 请求级 thinking 覆盖厂商级
		Thinking: map[string]any{"type": "disabled"},
	}, true)
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)
	th, _ := raw["thinking"].(map[string]any)
	if th["type"] != "disabled" {
		t.Fatalf("thinking = %v, want request-level disabled override", raw["thinking"])
	}
}
