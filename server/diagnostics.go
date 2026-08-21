package server

import (
	"context"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aptshark/gateway/provider"
)

// ── Diagnostics: per-provider detail + connectivity self-test ──

// ProviderDiagnostic holds detailed health info for one provider.
type ProviderDiagnostic struct {
	Name           string `json:"name"`
	Kind           string `json:"kind"`
	KeyConfigured  bool   `json:"key_configured"`
	BaseURL        string `json:"base_url"`
	Active         bool   `json:"active"`
	CircuitState   string `json:"circuit_state"`
	ModelsCount    int    `json:"models_count"`
	ConnectivityOK *bool  `json:"connectivity_ok,omitempty"`
}

// handleDiagnostics returns full per-provider detail.
func (s *Server) handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	providers := s.manager.List()
	diags := make([]ProviderDiagnostic, 0, len(providers))
	problemCount := 0

	for _, p := range providers {
		d := ProviderDiagnostic{
			Name:          p.Name,
			Kind:          p.Kind,
			KeyConfigured: p.KeyConfigured,
			BaseURL:       p.BaseURL,
			Active:        p.Active,
			CircuitState:  string(p.Circuit),
			ModelsCount:   len(p.Models),
		}
		if !p.KeyConfigured {
			log.Printf("diagnostic: %s — NO API KEY", p.Name)
			problemCount++
		}
		if p.BaseURL == "" {
			log.Printf("diagnostic: %s — NO base_url", p.Name)
			problemCount++
		}
		diags = append(diags, d)
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	writeJSON(w, http.StatusOK, map[string]any{
		"providers":         diags,
		"routing_strategy":  "weighted_random",
		"routing_rules":     s.routingRules,
		"prefix_tracked":    s.prefixProfiler.Size(),
		"coalescer_pending": s.coalescer.Pending(),
		"problems_detected": problemCount,
		"uptime_seconds":    s.metrics.Uptime(),
		"go_version":        runtime.Version(),
		"num_goroutines":    runtime.NumGoroutine(),
		"memory_mb":         float64(mem.Alloc) / 1024 / 1024,
		"cache_entries":     s.cache.Size(),
	})
}

// ── Startup self-test ──

// SelfTest runs connectivity checks on enabled providers at startup.
func (s *Server) SelfTest() []ProviderDiagnostic {
	providers := s.manager.List()
	results := make([]ProviderDiagnostic, 0, len(providers))

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, p := range providers {
		d := ProviderDiagnostic{
			Name:          p.Name,
			Kind:          p.Kind,
			KeyConfigured: p.KeyConfigured,
			BaseURL:       p.BaseURL,
			Active:        p.Active,
			CircuitState:  string(p.Circuit),
			ModelsCount:   len(p.Models),
		}
		results = append(results, d)

		if !p.KeyConfigured || !p.Active {
			continue
		}
		i := len(results) - 1
		wg.Add(1)
		go func(idx int, pn string) {
			defer wg.Done()
			ok := s.testConnectivity(ctx, pn)
			results[idx].ConnectivityOK = &ok
		}(i, p.Name)
	}

	wg.Wait()
	return results
}

func (s *Server) testConnectivity(ctx context.Context, providerName string) bool {
	prov, err := s.manager.Get(providerName)
	if err != nil {
		log.Printf("selftest: %s — not found: %v", providerName, err)
		return false
	}
	req := &provider.GenerateRequest{
		Model:     "_ping_",
		Messages:  []provider.Message{{Role: "user", Content: "ping"}},
		MaxTokens: 1,
	}
	_, err = prov.Generate(ctx, req)
	if err != nil {
		if strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "403") {
			log.Printf("selftest: %s — auth error (endpoint reachable)", providerName)
			return true
		}
		log.Printf("selftest: %s — FAILED: %v", providerName, err)
		return false
	}
	return true
}

// ── Startup config dump ──

func (s *Server) LogStartupConfig() {
	providers := s.manager.List()
	active, withKeys := 0, 0
	log.Printf("═══ gateway startup: %d providers ═══", len(providers))
	for _, p := range providers {
		status, keyInfo := "INACTIVE", "NO_KEY"
		if p.Active { status = "ACTIVE"; active++ }
		if p.KeyConfigured { keyInfo = "KEY_SET"; withKeys++ }
		log.Printf("  %s %s | kind=%s | key=%s | url=%s | models=%d | circuit=%s",
			status, p.Name, p.Kind, keyInfo, p.BaseURL, len(p.Models), p.Circuit)
	}
	log.Printf("═══ %d active, %d with keys ═══", active, withKeys)
}
