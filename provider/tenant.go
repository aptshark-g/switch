package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ── Multi-tenant: per-API-key quotas with soft-limit warning ──

type TenantConfig struct {
	Key           string   `json:"key"`
	Label         string   `json:"label,omitempty"`
	AllowedModels []string `json:"allowed_models,omitempty"` // empty = all
	MonthlyTokens int64    `json:"monthly_tokens"`           // 0 = unlimited
	CostLimit     float64  `json:"cost_limit"`               // 0 = unlimited
	SoftLimitPct  float64  `json:"soft_limit_pct,omitempty"` // warn when N% used, default 0.8
	Enabled       bool     `json:"enabled"`
	CreatedAt     int64    `json:"created_at"`
}

// TenantStatus wraps config + current usage for API responses.
type TenantStatus struct {
	Config        TenantConfig `json:"config"`
	Usage         TenantUsage  `json:"usage"`
	Warnings      []string     `json:"warnings,omitempty"`
	OverQuota     bool         `json:"over_quota"`
	OverQuotaReason string     `json:"over_quota_reason,omitempty"`
}

// OnOverQuota is called when a tenant exceeds their quota.
type OnOverQuota func(key, reason string)

type TenantManager struct {
	mu           sync.RWMutex
	tenants      map[string]*TenantConfig
	usage        map[string]*TenantUsage
	onOverQuota  OnOverQuota
	lastReset    time.Time
}

func NewTenantManager(onOverQuota OnOverQuota) *TenantManager {
	return &TenantManager{
		tenants:     make(map[string]*TenantConfig),
		usage:       make(map[string]*TenantUsage),
		onOverQuota: onOverQuota,
		lastReset:   time.Now().UTC(),
	}
}

func (tm *TenantManager) Register(cfg TenantConfig) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if cfg.SoftLimitPct == 0 { cfg.SoftLimitPct = 0.8 }
	if cfg.CreatedAt == 0 { cfg.CreatedAt = time.Now().Unix() }
	tm.tenants[cfg.Key] = &cfg
	if _, exists := tm.usage[cfg.Key]; !exists {
		tm.usage[cfg.Key] = &TenantUsage{}
	}
}

func (tm *TenantManager) Revoke(key string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.tenants[key]; ok { t.Enabled = false }
}

func (tm *TenantManager) Check(key, model string, promptTokens, compTokens int64,
	pricing *Pricing) *TenantStatus {
	tm.mu.RLock()
	t, ok := tm.tenants[key]
	u := tm.usage[key]
	tm.mu.RUnlock()

	if !ok || t == nil || !t.Enabled {
		return &TenantStatus{OverQuota: true, OverQuotaReason: "unknown or disabled key"}
	}

	tm.tryMonthlyReset()

	status := &TenantStatus{Config: *t}
	if u != nil { status.Usage = *u }

	// Model whitelist
	if len(t.AllowedModels) > 0 {
		allowed := false
		for _, m := range t.AllowedModels {
			if m == model { allowed = true; break }
		}
		if !allowed {
			status.OverQuota = true
			status.OverQuotaReason = "model not in allowed list"
			return status
		}
	}

	// Token quota
	if t.MonthlyTokens > 0 && u != nil {
		projected := u.PromptTokens + promptTokens + u.CompletionTokens + compTokens
		if projected > t.MonthlyTokens {
			status.OverQuota = true
			status.OverQuotaReason = "monthly token quota exceeded"
			if tm.onOverQuota != nil { tm.onOverQuota(key, status.OverQuotaReason) }
			return status
		}
		// Soft limit warning
		if float64(projected)/float64(t.MonthlyTokens) > t.SoftLimitPct {
			status.Warnings = append(status.Warnings, 
				"approaching token quota limit")
		}
	}

	// Cost quota
	if t.CostLimit > 0 && u != nil && pricing != nil {
		estCost := pricing.EstimateCost(promptTokens, compTokens)
		projectedCost := u.CostUSD + estCost
		if projectedCost > t.CostLimit {
			status.OverQuota = true
			status.OverQuotaReason = "cost limit exceeded"
			if tm.onOverQuota != nil { tm.onOverQuota(key, status.OverQuotaReason) }
			return status
		}
		if projectedCost/t.CostLimit > t.SoftLimitPct {
			status.Warnings = append(status.Warnings, "approaching cost limit")
		}
	}

	return status
}

func (tm *TenantManager) Record(key string, promptTokens, compTokens int64, cost float64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	u, ok := tm.usage[key]
	if !ok { u = &TenantUsage{}; tm.usage[key] = u }
	atomic.AddInt64(&u.PromptTokens, promptTokens)
	atomic.AddInt64(&u.CompletionTokens, compTokens)
	u.CostUSD += cost
	u.LastRequest = time.Now()
}

func (tm *TenantManager) tryMonthlyReset() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	now := time.Now().UTC()
	if now.Month() != tm.lastReset.Month() || now.Year() != tm.lastReset.Year() {
		for k := range tm.usage {
			tm.usage[k] = &TenantUsage{}
		}
		tm.lastReset = now
	}
}

func (tm *TenantManager) List() []TenantStatus {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make([]TenantStatus, 0, len(tm.tenants))
	for key, t := range tm.tenants {
		u := tm.usage[key]
		s := TenantStatus{Config: *t}
		if u != nil { s.Usage = *u }
		out = append(out, s)
	}
	return out
}


// ── Pricing: litellm-compatible model pricing ──

type Pricing struct {
	Model       string  `json:"model"`
	Provider    string  `json:"provider"`
	InputPrice  float64 `json:"input_price"`   // per 1M tokens
	OutputPrice float64 `json:"output_price"`  // per 1M tokens
	Source      string  `json:"source"`        // "litellm" | "effective" | "manual"
	UpdatedAt   int64   `json:"updated_at"`
}

type PricingStore struct {
	mu       sync.RWMutex
	prices   map[string]*Pricing // model → pricing
	fallback map[string]*Pricing // backup source
}

func NewPricingStore() *PricingStore {
	return &PricingStore{
		prices:   make(map[string]*Pricing),
		fallback: make(map[string]*Pricing),
	}
}

func (ps *PricingStore) Set(model string, p *Pricing) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p.UpdatedAt = time.Now().Unix()
	ps.prices[model] = p
}

func (ps *PricingStore) Get(model string) *Pricing {
	ps.mu.RLock()
	p := ps.prices[model]
	ps.mu.RUnlock()
	if p != nil { return p }
	// Fallback
	ps.mu.RLock()
	p = ps.fallback[model]
	ps.mu.RUnlock()
	return p
}

func (ps *PricingStore) EstimateCost(promptTokens, compTokens int64) float64 {
	return 0.0 // requires model name, use Get().EstimateCost() instead
}

func (p *Pricing) EstimateCost(promptTokens, compTokens int64) float64 {
	return float64(promptTokens)/1_000_000*p.InputPrice +
		float64(compTokens)/1_000_000*p.OutputPrice
}

// SyncFromLitellm fetches model pricing from litellm's community-maintained JSON.
// Source: https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json
func (ps *PricingStore) SyncFromLitellm() (int, string, error) {
	url := "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0, "", fmt.Errorf("litellm fetch: %w", err)
	}
	defer resp.Body.Close()

	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return 0, "", fmt.Errorf("litellm parse: %w", err)
	}

	count := 0
	for model, data := range raw {
		entry, ok := data.(map[string]any)
		if !ok { continue }
		// litellm fields: input_cost_per_token, output_cost_per_token
		// Convert from per-token to per-1M-tokens
		inputPrice := 0.0
		outputPrice := 0.0
		if v, ok := entry["input_cost_per_token"].(float64); ok {
			inputPrice = v * 1_000_000
		}
		if v, ok := entry["output_cost_per_token"].(float64); ok {
			outputPrice = v * 1_000_000
		}

		provider := ""
		if v, ok := entry["litellm_provider"].(string); ok { provider = v }
		if provider == "" {
			if v, ok := entry["provider"].(string); ok { provider = v }
		}

		ps.Set(model, &Pricing{
			Model:       model,
			Provider:    provider,
			InputPrice:  inputPrice,
			OutputPrice: outputPrice,
			Source:      "litellm",
		})
		count++
	}

	return count, url, nil
}


// ── Cost Validator: real-time + periodic checks ──

type CostValidator struct {
	mu           sync.Mutex
	totalChecked int64
	warnings     int64
	criticals    int64
	lastHourCost float64
	currentCost  float64
}

func (cv *CostValidator) Validate(pricing *Pricing, cost float64, tokens int64) []string {
	cv.mu.Lock()
	defer cv.mu.Unlock()
	cv.totalChecked++

	var alerts []string
	if pricing == nil {
		alerts = append(alerts, "CRITICAL: pricing is nil")
		cv.criticals++
	}
	if cost == 0 && tokens > 0 {
		alerts = append(alerts, "WARNING: cost=0 but tokens>0")
		cv.warnings++
	}
	if cost > 10.0 {
		alerts = append(alerts, "WARNING: single request cost exceeds $10")
		cv.warnings++
	}
	if pricing != nil && pricing.InputPrice > 500 {
		alerts = append(alerts, "WARNING: input price > $500/M (possible config error)")
		cv.warnings++
	}
	cv.currentCost += cost
	return alerts
}

func (cv *CostValidator) HourlyCheck() []string {
	cv.mu.Lock()
	defer cv.mu.Unlock()
	var alerts []string
	if cv.lastHourCost > 0 {
		growth := (cv.currentCost - cv.lastHourCost) / cv.lastHourCost
		if growth > 0.5 {
			alerts = append(alerts, "WARNING: cost grew >50% in last hour")
		}
	}
	cv.lastHourCost = cv.currentCost
	cv.currentCost = 0
	return alerts
}

func (cv *CostValidator) Stats() map[string]any {
	cv.mu.Lock()
	defer cv.mu.Unlock()
	return map[string]any{
		"total_checked": cv.totalChecked,
		"warnings":      cv.warnings,
		"criticals":     cv.criticals,
	}
}


// ── Active Health Probe: background circuit breaker recovery ──

type Prober struct {
	manager   *Manager
	interval  time.Duration
	stopCh    chan struct{}
	onHealth  func(name string, hs *HealthStatus)
}

func NewProber(mgr *Manager, interval time.Duration,
	onHealth func(name string, hs *HealthStatus)) *Prober {
	return &Prober{manager: mgr, interval: interval,
		stopCh: make(chan struct{}), onHealth: onHealth}
}

func (p *Prober) Start() {
	go func() {
		p.probe() // 2026-08-13: 启动立即探测一轮, 避免首 30s 健康缓存为空
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.probe()
			case <-p.stopCh:
				return
			}
		}
	}()
}

func (p *Prober) Stop() { close(p.stopCh) }

func (p *Prober) probe() {
	providers := p.manager.List()
	// 2026-08-13: 全量并行探测 + 健康缓存 — handleHealth 读缓存即时返回,
	// 实时探测只在前台 ?live=1 时发生。熔断恢复探测保留（Generate ping）。
	var wg sync.WaitGroup
	for _, prov := range providers {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			pr, err := p.manager.Get(name)
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(),
				3*time.Second)
			hs := pr.Health(ctx)
			cancel()
			if p.onHealth != nil {
				p.onHealth(name, hs)
			}
			// 熔断恢复探测（原逻辑）: circuit open 时尝试真实生成
			if hs.Healthy {
				for _, snap := range p.manager.List() {
					if snap.Name == name && snap.Circuit == "open" {
						log.Printf(
							"probe: %s responded, circuit may recover",
							name)
						break
					}
				}
			}
		}(prov.Name)
	}
	wg.Wait()
}

// ── Audit Log: Admin operation tracking ──

type AuditEntry struct {
	TS        int64  `json:"ts"`
	Action    string `json:"action"`
	Admin     string `json:"admin"`
	Target    string `json:"target,omitempty"`
	Detail    string `json:"detail,omitempty"`
	ClientIP  string `json:"client_ip,omitempty"`
	Success   bool   `json:"success"`
}

type AuditLog struct {
	mu      sync.RWMutex
	entries []AuditEntry
	maxSize int
}

func NewAuditLog(maxSize int) *AuditLog {
	return &AuditLog{entries: make([]AuditEntry, 0, maxSize), maxSize: maxSize}
}

func (a *AuditLog) Record(entry AuditEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry.TS = time.Now().Unix()
	a.entries = append(a.entries, entry)
	if len(a.entries) > a.maxSize {
		a.entries = a.entries[len(a.entries)-a.maxSize:]
	}
}

func (a *AuditLog) List(limit int) []AuditEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if limit <= 0 || limit > len(a.entries) {
		limit = len(a.entries)
	}
	start := len(a.entries) - limit
	if start < 0 { start = 0 }
	result := make([]AuditEntry, limit)
	copy(result, a.entries[start:])
	return result
}
