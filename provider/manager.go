package provider

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	mu           sync.RWMutex
	providers    map[string]Provider
	circuits     map[string]*CircuitBreaker
	factories    map[string]Factory
	semaphores   map[string]*Semaphore
	ratelimiters map[string]*ProviderRateLimiter
	allConfigs   map[string]ProviderConfig
}

type Factory func(cfg ProviderConfig) (Provider, error)

func NewManager() *Manager {
	return &Manager{
		providers:    make(map[string]Provider),
		circuits:     make(map[string]*CircuitBreaker),
		factories:    make(map[string]Factory),
		semaphores:   make(map[string]*Semaphore),
		ratelimiters: make(map[string]*ProviderRateLimiter),
		allConfigs:   make(map[string]ProviderConfig),
	}
}

func (m *Manager) RegisterFactory(kind string, f Factory) {
	m.mu.Lock(); defer m.mu.Unlock()
	m.factories[kind] = f
}

func (m *Manager) Bootstrap(configs []ProviderConfig) error {
	for _, cfg := range configs {
		cfgCopy := cfg
		m.mu.Lock()
		m.allConfigs[cfgCopy.Name] = cfgCopy
		m.mu.Unlock()
		if !cfgCopy.Enabled {
			continue
		}
		if _, err := m.Register(cfgCopy); err != nil {
			log.Printf("manager: failed to register %s: %v", cfgCopy.Name, err)
		}
	}
	return nil
}

func (m *Manager) Register(cfg ProviderConfig) (Provider, error) {
	m.mu.RLock()
	if _, exists := m.providers[cfg.Name]; exists {
		m.mu.RUnlock()
		m.mu.Lock()
		m.allConfigs[cfg.Name] = cfg
		m.mu.Unlock()
		return m.providers[cfg.Name], nil
	}
	m.mu.RUnlock()
	factory, ok := m.factories[cfg.Kind]
	if !ok { return nil, fmt.Errorf("no factory for kind %q", cfg.Kind) }
	p, err := factory(cfg)
	if err != nil { return nil, fmt.Errorf("construct %s: %w", cfg.Kind, err) }
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allConfigs[cfg.Name] = cfg
	m.providers[cfg.Name] = p
	m.circuits[cfg.Name] = NewCircuitBreaker(cfg.Name)
	m.semaphores[cfg.Name] = NewSemaphore(cfg.MaxConcurrency)
	m.ratelimiters[cfg.Name] = NewProviderRateLimiter(cfg.RateLimitRPM, cfg.RateLimitTPM)
	log.Printf("manager: registered provider %q (kind=%s)", cfg.Name, cfg.Kind)
	return p, nil
}

func (m *Manager) Unregister(name string) {
	m.mu.Lock(); defer m.mu.Unlock()
	delete(m.providers, name)
	delete(m.circuits, name)
	delete(m.semaphores, name)
	delete(m.ratelimiters, name)
	delete(m.allConfigs, name)
}

func (m *Manager) Get(name string) (Provider, error) {
	m.mu.RLock(); defer m.mu.RUnlock()
	p, ok := m.providers[name]
	if !ok { return nil, fmt.Errorf("provider %q not found", name) }
	return p, nil
}

func (m *Manager) List() []ProviderSnapshot {
	m.mu.RLock(); defer m.mu.RUnlock()
	out := make([]ProviderSnapshot, 0, len(m.allConfigs))
	for name, cfg := range m.allConfigs {
		_, active := m.providers[name]
		hasKey := isKeyConfigured(cfg)
		active = active && hasKey
		s := ProviderSnapshot{Name: name, Kind: cfg.Kind, Active: active, Models: cfg.Models, KeyConfigured: isKeyConfigured(cfg)}
		if cb, ok := m.circuits[name]; ok { s.Circuit = cb.State() }
		out = append(out, s)
	}
	return out
}

func (m *Manager) Generate(ctx context.Context, name string, req *GenerateRequest) (*GenerateResponse, error) {
	p, err := m.Get(name)
	if err != nil { return nil, err }
	m.mu.RLock()
	sem := m.semaphores[name]
	rl := m.ratelimiters[name]
	cb := m.circuits[name]
	m.mu.RUnlock()
	if sem != nil { if err := sem.Acquire(ctx); err != nil { return nil, err }; defer sem.Release() }
	if rl != nil { if !rl.Allow(tokenEstimate(req)) { return nil, fmt.Errorf("rate limit exceeded for %s", name) } }
	if cb != nil { if !cb.Allow() { return nil, fmt.Errorf("circuit open for %s", name) }; t0 := time.Now(); resp, err := p.Generate(ctx, req); cb.Record(err, time.Since(t0)); return resp, err }
	return p.Generate(ctx, req)
}

func (m *Manager) UsageStats() *UsageStats {
	m.mu.RLock(); defer m.mu.RUnlock()
	stats := &UsageStats{ByProvider: make(map[string]int64)}
	for name := range m.allConfigs { stats.ByProvider[name] = 0 }
	return stats
}

func (m *Manager) TotalSemaphoreWaiting() int64 {
	m.mu.RLock(); defer m.mu.RUnlock()
	var total int64
	for _, s := range m.semaphores { total += s.Waiting() }
	return total
}

func (m *Manager) Diagnostics() map[string]any {
	m.mu.RLock(); defer m.mu.RUnlock()
	providers := make([]map[string]any, 0, len(m.allConfigs))
	for name, cfg := range m.allConfigs {
		_, active := m.providers[name]
		info := map[string]any{"name": name, "kind": cfg.Kind, "active": active}
		if cb, ok := m.circuits[name]; ok { info["circuit_state"] = cb.State() }
		if sem, ok := m.semaphores[name]; ok && sem != nil { info["semaphore_waiting"] = sem.Waiting() }
		providers = append(providers, info)
	}
	return map[string]any{"providers": providers}
}

func tokenEstimate(req *GenerateRequest) int {
	n := 0
	for _, msg := range req.Messages { n += len(msg.Content) }
	return n/4 + req.MaxTokens
}

type UsageStats struct {
	TotalRequests int64            `json:"total_requests"`
	TotalTokens   int64            `json:"total_tokens"`
	ByProvider    map[string]int64 `json:"by_provider"`
}



func isKeyConfigured(cfg ProviderConfig) bool {
	if cfg.APIKey == "" { return false }
	if strings.HasPrefix(cfg.APIKey, "${") && strings.HasSuffix(cfg.APIKey, "}") { return false }
	return true
}
