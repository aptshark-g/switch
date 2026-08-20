package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aptshark/gateway/config"
	"github.com/aptshark/gateway/provider"
)

// ── mock provider（智能路由单测）──────────────────────────────────────

type mockProvider struct {
	name string
	cfg  provider.ProviderConfig
	fail bool
}

func (m *mockProvider) Name() string                         { return m.name }
func (m *mockProvider) Config() *provider.ProviderConfig     { return &m.cfg }
func (m *mockProvider) Health(ctx context.Context) *provider.HealthStatus {
	return &provider.HealthStatus{Healthy: !m.fail}
}
func (m *mockProvider) Generate(ctx context.Context, req *provider.GenerateRequest) (*provider.GenerateResponse, error) {
	if m.fail {
		return nil, errors.New("mock failure")
	}
	return &provider.GenerateResponse{
		Choices: []provider.Choice{{Message: provider.Message{Role: "assistant", Content: "ok"}}},
	}, nil
}

func newRoutingServer(t *testing.T, cfgs []provider.ProviderConfig) (*Server, *provider.Manager) {
	t.Helper()
	mgr := provider.NewManager()
	mgr.RegisterFactory("mock", func(cfg provider.ProviderConfig) (provider.Provider, error) {
		return &mockProvider{name: cfg.Name, cfg: cfg}, nil
	})
	mgr.RegisterFactory("mock-broken", func(cfg provider.ProviderConfig) (provider.Provider, error) {
		return &mockProvider{name: cfg.Name, cfg: cfg, fail: true}, nil
	})
	if err := mgr.Bootstrap(cfgs); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return &Server{
		manager:     mgr,
		routingPool: map[string]bool{},
		poolMutex:   sync.RWMutex{},
		healthCache: map[string]*provider.HealthStatus{},
		healthMu:    sync.RWMutex{},
		routingMu:   sync.RWMutex{},
	}, mgr
}

func pcfg(name string, prio, weight int) provider.ProviderConfig {
	return provider.ProviderConfig{
		Name: name, Kind: "mock", BaseURL: "http://mock",
		APIKey: "k", Enabled: true, Priority: prio, Weight: weight,
	}
}

// ── 池内加权随机 ─────────────────────────────────────────────────────

func TestSelectFromPoolPriorityTier(t *testing.T) {
	s, _ := newRoutingServer(t, []provider.ProviderConfig{
		pcfg("low", 1, 100),
		pcfg("high", 10, 1),
	})
	for i := 0; i < 50; i++ {
		if got := s.selectFromPool(); got != "high" {
			t.Fatalf("selectFromPool = %q, want high (priority tier)", got)
		}
	}
}

func TestSelectFromPoolWeightedDistribution(t *testing.T) {
	s, _ := newRoutingServer(t, []provider.ProviderConfig{
		pcfg("heavy", 1, 9),
		pcfg("light", 1, 1),
	})
	heavy, total := 0, 6000
	for i := 0; i < total; i++ {
		if s.selectFromPool() == "heavy" {
			heavy++
		}
	}
	share := float64(heavy) / float64(total)
	if share < 0.6 || share > 0.96 {
		t.Fatalf("heavy share = %.3f, want ≈0.9 (weighted random)", share)
	}
}

func TestSelectFromPoolCircuitOpenExcluded(t *testing.T) {
	s, mgr := newRoutingServer(t, []provider.ProviderConfig{
		pcfg("healthy", 1, 1),
		{Name: "broken", Kind: "mock-broken", BaseURL: "http://mock",
			APIKey: "k", Enabled: true, Priority: 10, Weight: 1},
	})
	// 10 次失败调用 → 熔断 OPEN（默认配置: 失败率 >50% 且调用数 ≥5）。
	for i := 0; i < 10; i++ {
		_, _ = mgr.Generate(context.Background(), "broken",
			&provider.GenerateRequest{
				Messages: []provider.Message{{Role: "user", Content: "x"}},
			})
	}
	snap, ok := s.providerSnapshot("broken")
	if !ok || snap.Circuit != provider.CircuitOpen {
		t.Fatalf("broken circuit = %v, want open", snap.Circuit)
	}
	for i := 0; i < 20; i++ {
		if got := s.selectFromPool(); got != "healthy" {
			t.Fatalf("selectFromPool = %q, want healthy (circuit open excluded)", got)
		}
	}
}

func TestSelectFromPoolUnhealthyDeprioritized(t *testing.T) {
	s, _ := newRoutingServer(t, []provider.ProviderConfig{
		pcfg("sick", 1, 1),
		pcfg("fine", 1, 1),
	})
	s.healthMu.Lock()
	s.healthCache["sick"] = &provider.HealthStatus{Healthy: false, LatencyMs: 3000}
	s.healthMu.Unlock()
	fine, total := 0, 4000
	for i := 0; i < total; i++ {
		if s.selectFromPool() == "fine" {
			fine++
		}
	}
	share := float64(fine) / float64(total)
	if share < 0.7 {
		t.Fatalf("fine share = %.3f, want >0.7 (unhealthy ×0.1 保底)", share)
	}
}

func TestSelectFromPoolEmpty(t *testing.T) {
	s, _ := newRoutingServer(t, []provider.ProviderConfig{
		{Name: "no-key", Kind: "mock", BaseURL: "http://mock",
			Enabled: true}, // 无 key → 不可路由
	})
	if got := s.selectFromPool(); got != "" {
		t.Fatalf("selectFromPool = %q, want empty", got)
	}
}

// ── 意图/复杂度规则层 ────────────────────────────────────────────────

func TestRouteRequestRuleIntentComplexity(t *testing.T) {
	s, _ := newRoutingServer(t, []provider.ProviderConfig{
		pcfg("deepseek", 1, 1),
		pcfg("lmstudio", 1, 1),
	})
	s.SetRoutingRules([]config.RoutingRule{
		{
			Name:  "casual_local",
			Match: config.RoutingMatch{Intent: "casual, 闲聊", Complexity: "simple"},
			Route: config.RoutingAction{Provider: "lmstudio", Model: "small",
				Thinking: map[string]any{"type": "disabled"}},
		},
		{
			Name:  "code_strong",
			Match: config.RoutingMatch{Intent: "代码分析", Complexity: "complex"},
			Route: config.RoutingAction{Provider: "deepseek", Model: "pro"},
		},
	})

	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("X-Intent", "casual")
	req.Header.Set("X-Complexity", "simple")
	d := s.routeRequest(req, &provider.GenerateRequest{Model: "auto"})
	if d.provider != "lmstudio" || d.model != "small" || d.by != "rule:casual_local" {
		t.Fatalf("casual decision = %+v, want lmstudio/small/rule", d)
	}
	if d.thinking == nil {
		t.Fatal("casual rule should override thinking to disabled")
	}

	req2 := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req2.Header.Set("X-Intent", "代码分析")
	req2.Header.Set("X-Complexity", "complex")
	d2 := s.routeRequest(req2, &provider.GenerateRequest{Model: "auto"})
	if d2.provider != "deepseek" || d2.model != "pro" || d2.by != "rule:code_strong" {
		t.Fatalf("code decision = %+v, want deepseek/pro/rule", d2)
	}
}

func TestRouteRequestRuleProviderUnavailableFallback(t *testing.T) {
	s, _ := newRoutingServer(t, []provider.ProviderConfig{pcfg("pool-a", 1, 1)})
	s.SetRoutingRules([]config.RoutingRule{
		{
			Name:  "ghost",
			Match: config.RoutingMatch{Intent: "casual"},
			Route: config.RoutingAction{Provider: "not-registered", Model: "small"},
		},
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("X-Intent", "casual")
	d := s.routeRequest(req, &provider.GenerateRequest{Model: "auto"})
	if d.provider != "" || d.model != "small" {
		t.Fatalf("decision = %+v, want provider empty (fallback) + model override", d)
	}
	if !strings.HasPrefix(d.by, "rule:ghost:") {
		t.Fatalf("by = %q, want rule:ghost:provider-unavailable", d.by)
	}
	// handler 回落池（api.go: providerName=="" → getRoutingProvider）
	if got := s.getRoutingProvider(); got != "pool-a" {
		t.Fatalf("fallback provider = %q, want pool-a", got)
	}
}

func TestRouteRequestNoSignalFallsToPool(t *testing.T) {
	s, _ := newRoutingServer(t, []provider.ProviderConfig{
		pcfg("a", 1, 1),
		pcfg("b", 1, 1),
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	d := s.routeRequest(req, &provider.GenerateRequest{Model: "auto"})
	if d.by != "weighted" || d.provider == "" {
		t.Fatalf("decision = %+v, want weighted + pool pick", d)
	}
}

// ── 降级候选链（确定性顺序） ─────────────────────────────────────────

func TestRoutingCandidatesDeterministic(t *testing.T) {
	s, mgr := newRoutingServer(t, []provider.ProviderConfig{
		pcfg("mid", 5, 1),
		pcfg("top", 10, 1),
		{Name: "broken", Kind: "mock-broken", BaseURL: "http://mock",
			APIKey: "k", Enabled: true, Priority: 20, Weight: 1},
	})
	for i := 0; i < 10; i++ {
		_, _ = mgr.Generate(context.Background(), "broken",
			&provider.GenerateRequest{
				Messages: []provider.Message{{Role: "user", Content: "x"}},
			})
	}
	got := s.routingCandidates()
	want := []string{"top", "mid", "broken"}
	if len(got) != len(want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidates[%d] = %q, want %q (deterministic, open last)", i, got[i], want[i])
		}
	}
}

// ── 规则匹配工具 ─────────────────────────────────────────────────────

func TestMatchList(t *testing.T) {
	cases := []struct {
		rule, req string
		want      bool
	}{
		{"", "anything", true},
		{"casual", "casual", true},
		{"casual, 闲聊", "闲聊", true},
		{"代码分析", "代码分析", true},
		{"casual", "代码分析", false},
		{"SIMPLE", "simple", true}, // 大小写不敏感
	}
	for _, c := range cases {
		if got := matchList(c.rule, c.req); got != c.want {
			t.Fatalf("matchList(%q, %q) = %v, want %v", c.rule, c.req, got, c.want)
		}
	}
}
