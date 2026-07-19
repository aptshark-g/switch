package provider

import (
	"sync"
	"time"
)

// ── Cost Tracker: per-request cost calculation + per-tenant aggregation ──

// CostTracker records token usage and cost per API key, model, and provider.
type CostTracker struct {
	mu     sync.RWMutex
	byKey  map[string]*TenantUsage
	byModel map[string]*ModelUsage
	total  TotalUsage
}

// TenantUsage tracks one API key's usage.
type TenantUsage struct {
	Key             string
	PromptTokens    int64
	CompletionTokens int64
	Requests        int64
	CostUSD         float64
	LastRequest     time.Time
	Models          map[string]int64 // model → request count
}

// ModelUsage tracks per-model aggregate usage.
type ModelUsage struct {
	Model           string
	PromptTokens    int64
	CompletionTokens int64
	Requests        int64
	CostUSD         float64
}

// TotalUsage aggregates all usage.
type TotalUsage struct {
	PromptTokens    int64
	CompletionTokens int64
	Requests        int64
	CostUSD         float64
}

// NewCostTracker creates a cost tracker.
func NewCostTracker() *CostTracker {
	return &CostTracker{
		byKey:   make(map[string]*TenantUsage),
		byModel: make(map[string]*ModelUsage),
	}
}

// Record records token usage for a request and calculates cost.
func (ct *CostTracker) Record(apiKey, provider, model string, promptTokens, completionTokens int, pricing *TokenPricing) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	cost := 0.0
	if pricing != nil {
		cost = pricing.Cost(promptTokens, completionTokens)
	}

	// Per-key
	tu := ct.byKey[apiKey]
	if tu == nil {
		tu = &TenantUsage{Key: apiKey, Models: make(map[string]int64)}
		ct.byKey[apiKey] = tu
	}
	tu.PromptTokens += int64(promptTokens)
	tu.CompletionTokens += int64(completionTokens)
	tu.Requests++
	tu.CostUSD += cost
	tu.LastRequest = time.Now()
	tu.Models[model]++

	// Per-model
	mu := ct.byModel[model]
	if mu == nil {
		mu = &ModelUsage{Model: model}
		ct.byModel[model] = mu
	}
	mu.PromptTokens += int64(promptTokens)
	mu.CompletionTokens += int64(completionTokens)
	mu.Requests++
	mu.CostUSD += cost

	// Total
	ct.total.PromptTokens += int64(promptTokens)
	ct.total.CompletionTokens += int64(completionTokens)
	ct.total.Requests++
	ct.total.CostUSD += cost
}

// TenantSnapshot returns usage for a specific API key.
func (ct *CostTracker) TenantSnapshot(apiKey string) *TenantUsage {
	ct.mu.RLock(); defer ct.mu.RUnlock()
	if tu, ok := ct.byKey[apiKey]; ok {
		copy := *tu
		copy.Models = make(map[string]int64)
		for k, v := range tu.Models { copy.Models[k] = v }
		return &copy
	}
	return nil
}

// Snapshot returns a full usage snapshot suitable for API responses.
func (ct *CostTracker) Snapshot() *CostSnapshot {
	ct.mu.RLock(); defer ct.mu.RUnlock()

	byKey := make(map[string]TenantUsage, len(ct.byKey))
	for k, v := range ct.byKey {
		copy := *v
		copy.Models = make(map[string]int64)
		for mk, mv := range v.Models { copy.Models[mk] = mv }
		byKey[k] = copy
	}

	byModel := make(map[string]ModelUsage, len(ct.byModel))
	for k, v := range ct.byModel {
		byModel[k] = *v
	}

	return &CostSnapshot{
		Total:       ct.total,
		ByKey:       byKey,
		ByModel:     byModel,
		TenantCount: len(ct.byKey),
	}
}

// CostSnapshot is the JSON-serialisable view of cost data.
type CostSnapshot struct {
	Total       TotalUsage              `json:"total"`
	ByKey       map[string]TenantUsage  `json:"by_key,omitempty"`
	ByModel     map[string]ModelUsage   `json:"by_model,omitempty"`
	TenantCount int                     `json:"tenant_count"`
}

// ModelWhitelist controls which models a tenant can access.
type ModelWhitelist struct {
	mu sync.RWMutex
	m  map[string]map[string]bool // apiKey → model → allowed
}

// NewModelWhitelist creates an empty whitelist (all models allowed by default).
func NewModelWhitelist() *ModelWhitelist {
	return &ModelWhitelist{m: make(map[string]map[string]bool)}
}

// SetWhitelist sets allowed models for an API key. Empty list = all allowed.
func (mw *ModelWhitelist) SetWhitelist(apiKey string, models []string) {
	mw.mu.Lock(); defer mw.mu.Unlock()
	set := make(map[string]bool, len(models))
	for _, m := range models { set[m] = true }
	mw.m[apiKey] = set
}

// IsAllowed checks if a tenant can use the given model.
func (mw *ModelWhitelist) IsAllowed(apiKey, model string) bool {
	mw.mu.RLock(); defer mw.mu.RUnlock()
	set, ok := mw.m[apiKey]
	if !ok || len(set) == 0 { return true } // no whitelist → all allowed
	return set[model]
}
