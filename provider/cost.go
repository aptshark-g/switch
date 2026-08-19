package provider

import (
	"bufio"
	"encoding/json"
	"os"
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
	// 持久化（2026-08-13）: JSONL 追加日志 + 启动重放重建汇总。
	logPath string
	// 2026-08-20: 持久化日志句柄 + 独立写锁（此前每请求 OpenFile+Close
	// 且占用全局锁 → 高并发 miss 路径吞吐暴跌, 实测 128 并发仅 138 req/s）
	logFile *os.File
	logMu   sync.Mutex
	// 按 key|日期 的 token 用量（per-key 配额判定, replay 重建）。
	dailyTokens map[string]int64
}

// TenantUsage tracks one API key's usage.
type TenantUsage struct {
	Key              string           `json:"key"`
	PromptTokens     int64            `json:"prompt_tokens"`
	CompletionTokens int64            `json:"completion_tokens"`
	Requests         int64            `json:"requests"`
	CostUSD          float64          `json:"cost_usd"`
	LastRequest      time.Time        `json:"last_request"`
	Models           map[string]int64 `json:"models"`
}

// ModelUsage tracks per-model aggregate usage.
type ModelUsage struct {
	Model            string  `json:"model"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	Requests         int64   `json:"requests"`
	CostUSD          float64 `json:"cost_usd"`
}

// TotalUsage aggregates all usage.
type TotalUsage struct {
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	Requests         int64   `json:"requests"`
	CostUSD          float64 `json:"cost_usd"`
}

// NewCostTracker creates a cost tracker. logPath != "" 时启用 JSONL
// 持久化（启动重放重建汇总 + 每笔追加）。
func NewCostTracker(logPath string) *CostTracker {
	ct := &CostTracker{
		byKey:       make(map[string]*TenantUsage),
		byModel:     make(map[string]*ModelUsage),
		logPath:     logPath,
		dailyTokens: make(map[string]int64),
	}
	if logPath != "" {
		ct.replayLog()
		if f, err := os.OpenFile(logPath,
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			ct.logFile = f
		}
	}
	return ct
}

// Record records token usage for a request and calculates cost.
func (ct *CostTracker) Record(apiKey, provider, model string, promptTokens, completionTokens int, pricing *TokenPricing) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	cost := 0.0
	if pricing != nil {
		cost = pricing.Cost(promptTokens, completionTokens)
	}
	ts := time.Now()
	ct.recordLocked(apiKey, provider, model, promptTokens,
		completionTokens, cost, ts)
	if ct.logPath != "" {
		ct.appendLog(apiKey, provider, model, promptTokens,
			completionTokens, cost, ts)
	}
}

func (ct *CostTracker) recordLocked(apiKey, provider, model string,
	promptTokens, completionTokens int, cost float64, ts time.Time) {

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
	tu.LastRequest = ts
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
	ct.dailyTokens[apiKey+"|"+ts.Format("2006-01-02")] +=
		int64(promptTokens + completionTokens)
}

type usageRecord struct {
	TS       string  `json:"ts"`
	Key      string  `json:"key"`
	Provider string  `json:"provider"`
	Model    string  `json:"model"`
	Prompt   int     `json:"prompt_tokens"`
	Complete int     `json:"completion_tokens"`
	CostUSD  float64 `json:"cost_usd"`
}

func (ct *CostTracker) appendLog(apiKey, provider, model string,
	promptTokens, completionTokens int, cost float64, ts time.Time) {
	line, err := json.Marshal(usageRecord{
		TS: ts.Format(time.RFC3339), Key: apiKey, Provider: provider,
		Model: model, Prompt: promptTokens, Complete: completionTokens,
		CostUSD: cost,
	})
	if err != nil {
		return
	}
	ct.logMu.Lock()
	defer ct.logMu.Unlock()
	if ct.logFile == nil {
		return
	}
	_, _ = ct.logFile.Write(append(line, '\n'))
}

func (ct *CostTracker) replayLog() {
	f, err := os.Open(ct.logPath)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		var rec usageRecord
		if json.Unmarshal(sc.Bytes(), &rec) != nil {
			continue
		}
		ts, _ := time.Parse(time.RFC3339, rec.TS)
		ct.recordLocked(rec.Key, rec.Provider, rec.Model,
			rec.Prompt, rec.Complete, rec.CostUSD, ts)
	}
}

// DailyTokens returns today's total tokens for a key (per-key 配额判定)。
func (ct *CostTracker) DailyTokens(apiKey string) int64 {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.dailyTokens[apiKey+"|"+time.Now().Format("2006-01-02")]
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
