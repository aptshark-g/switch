package prefix

import (
	"sync"
	"time"
)

// ── QuotaCoordinator（基线 §5.2）────────────────────────────────────
// 每 (provider, fp) 令牌桶, 真实请求优先: 预热只在桶剩余 > warmReserve
// 且 ≥1 token 时允许; 真实请求立即扣, 桶空则阻塞预热直到 refill。
// 解决 OpenAI 每前缀 ~15 req/min 硬约束 + 预热抢真实配额的问题。

type quotaBucket struct {
	tokens   float64
	lastFill time.Time
	rpm      float64
}

// QuotaCoordinator 预热配额协调器。
type QuotaCoordinator struct {
	mu          sync.Mutex
	buckets     map[string]*quotaBucket
	rpm         float64 // 每 (provider,fp) 的桶速率（0=不限）
	warmReserve float64 // 预热只可用剩余 > reserve 的余量（默认 0.30）
}

// NewQuotaCoordinator creates a coordinator. rpm<=0 不限; warmReserve<=0 → 0.30。
func NewQuotaCoordinator(rpm float64, warmReserve float64) *QuotaCoordinator {
	if rpm <= 0 {
		rpm = 30 // DeepSeek 无硬限制, OpenAI 15; 默认取 30 保守值
	}
	if warmReserve <= 0 {
		warmReserve = 0.30
	}
	return &QuotaCoordinator{
		buckets:     make(map[string]*quotaBucket),
		rpm:         rpm,
		warmReserve: warmReserve,
	}
}

func (q *QuotaCoordinator) bucket(provider, fp string) *quotaBucket {
	key := provider + "|" + fp
	b := q.buckets[key]
	if b == nil {
		b = &quotaBucket{tokens: q.rpm, lastFill: time.Now(), rpm: q.rpm}
		q.buckets[key] = b
	}
	b.refill()
	return b
}

func (b *quotaBucket) refill() {
	elapsed := time.Since(b.lastFill).Minutes()
	if elapsed <= 0 {
		return
	}
	b.lastFill = time.Now()
	b.tokens += elapsed * b.rpm
	if b.tokens > b.rpm {
		b.tokens = b.rpm
	}
}

// RealRequest 真实请求: 立即消耗 1 token（优先于预热）。
func (q *QuotaCoordinator) RealRequest(provider, fp string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	b := q.bucket(provider, fp)
	if b.tokens > 0 {
		b.tokens--
	}
}

// WarmAllowed 预热是否允许: 桶剩余 > reserve 且 ≥1 token; 允许则消耗。
func (q *QuotaCoordinator) WarmAllowed(provider, fp string) bool {
	if q == nil {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	b := q.bucket(provider, fp)
	reserved := b.rpm * q.warmReserve
	if b.tokens <= reserved || b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Remaining 剩余 token（观测用）。
func (q *QuotaCoordinator) Remaining(provider, fp string) float64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bucket(provider, fp).tokens
}
