package prefix

import (
	"sort"
	"sync"
	"time"
)

// Record 一个前缀指纹的观测统计（B-1 纯观测）。
type Record struct {
	FP           string    `json:"fp"`
	ReqCount     int64     `json:"req_count"`
	HitTokens    int64     `json:"hit_tokens"`
	MissTokens   int64     `json:"miss_tokens"`
	InputTokens  int64     `json:"input_tokens"`
	LastHitBlock int64     `json:"last_hit_block_tokens"`
	HitLayer     int       `json:"last_hit_layer"`
	LastAccess   time.Time `json:"last_access"`
}

// Profiler 前缀命中观测器（内存, 单实例阶段; Redis 实现预留接口）。
type Profiler struct {
	mu      sync.Mutex
	records map[string]*Record
	window  time.Duration
}

// NewProfiler creates a Profiler with a heat window (default 15m).
func NewProfiler() *Profiler {
	return &Profiler{
		records: make(map[string]*Record),
		window:  15 * time.Minute,
	}
}

// Observe 记录一次上游返回的缓存命中/未命中（由 server 在真实请求后调用）。
func (p *Profiler) Observe(tree *FingerprintTree, hitTokens, missTokens int) {
	if tree == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	rec := p.records[tree.FP]
	if rec == nil {
		rec = &Record{FP: tree.FP}
		p.records[tree.FP] = rec
	}
	rec.ReqCount++
	rec.HitTokens += int64(hitTokens)
	rec.MissTokens += int64(missTokens)
	rec.InputTokens += int64(hitTokens + missTokens)
	if hitTokens > 0 {
		rec.LastHitBlock = int64(hitTokens)
		rec.HitLayer = HitBlockLayer(tree, hitTokens)
	}
	rec.LastAccess = time.Now()
}

// IsHot 近 window 内 req_count ≥ minReq（预热/分层 TTL 的热度判定）。
func (p *Profiler) IsHot(fp string, minReq int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	rec := p.records[fp]
	if rec == nil {
		return false
	}
	if time.Since(rec.LastAccess) > p.window {
		return false
	}
	return rec.ReqCount >= minReq
}

// Top 返回最近访问的 TopN 记录（按 hit_tokens 排序, /v1/prefix/stats 用）。
func (p *Profiler) Top(n int) []Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Record, 0, len(p.records))
	for _, r := range p.records {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].HitTokens > out[j].HitTokens
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// Size returns the number of tracked prefixes.
func (p *Profiler) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

// HitBlockLayer 命中归属推断（基线 §5.1）: 从 Seg0 起逐层累加 token 长度,
// 首次 ≥ hitTokens 的层即命中块所在层。
func HitBlockLayer(tree *FingerprintTree, hitTokens int) int {
	acc := 0
	for i := 0; i < SegCount; i++ {
		acc += tree.TokenLen[i]
		if acc >= hitTokens {
			return i
		}
	}
	return SegCount - 1
}
