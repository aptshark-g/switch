package provider

import (
	"sync"
	"time"
)

// ── Circuit Breaker v2: Sliding window + per-endpoint + gradual half-open ──

// Bucket stores aggregated counts for one time slice.
type cbBucket struct {
	successes  int
	failures   int
	slowCalls  int
	totalCalls int
}

// CircuitBreakerConfig holds tunable parameters.
type CircuitBreakerConfig struct {
	WindowSize               time.Duration // sliding window span (default 60s)
	NumBuckets               int           // number of buckets in the window (default 10)
	FailureRateThreshold     float64       // 0.0-1.0 (default 0.5)
	SlowCallRateThreshold    float64       // 0.0-1.0 (default 0.5)
	SlowCallDurationThreshold time.Duration // > this is "slow" (default 10s)
	WaitDurationInOpen       time.Duration // how long OPEN lasts (default 30s)
	HalfOpenPermits          []int         // gradual permits e.g. [1,3,10]
	MinCallsBeforeEvaluation int           // cold-start guard (default 5)
}

// DefaultCircuitBreakerConfig returns reasonable defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		WindowSize:               60 * time.Second,
		NumBuckets:               10,
		FailureRateThreshold:     0.5,
		SlowCallRateThreshold:    0.5,
		SlowCallDurationThreshold: 10 * time.Second,
		WaitDurationInOpen:       30 * time.Second,
		HalfOpenPermits:          []int{1, 3, 10},
		MinCallsBeforeEvaluation: 5,
	}
}

// CircuitBreaker is a sliding-window circuit breaker.
type CircuitBreaker struct {
	cfg    CircuitBreakerConfig
	name   string
	mu     sync.Mutex
	state  CircuitState
	buckets    []cbBucket
	currentIdx int
	openedAt   time.Time
	halfOpenIdx  int // which permit stage we're in
	halfOpenOK   int // successes during current half-open phase
	halfOpenNeed int // permits needed for this stage
}

// NewCircuitBreaker creates a sliding-window circuit breaker with defaults.
func NewCircuitBreaker(name string) *CircuitBreaker {
	return NewCircuitBreakerWithConfig(name, DefaultCircuitBreakerConfig())
}

// NewCircuitBreakerWithConfig creates a circuit breaker with custom config.
func NewCircuitBreakerWithConfig(name string, cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.NumBuckets <= 0 { cfg.NumBuckets = 10 }
	if cfg.MinCallsBeforeEvaluation <= 0 { cfg.MinCallsBeforeEvaluation = 5 }
	if len(cfg.HalfOpenPermits) == 0 { cfg.HalfOpenPermits = []int{1, 3, 10} }
	return &CircuitBreaker{
		cfg:          cfg,
		name:         name,
		state:        CircuitClosed,
		buckets:      make([]cbBucket, cfg.NumBuckets),
		halfOpenNeed: cfg.HalfOpenPermits[0],
	}
}

// State returns the current circuit state (lazy evaluation).
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock(); defer cb.mu.Unlock()
	cb.evaluate()
	return cb.state
}

// Allow returns true if a request may proceed.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock(); defer cb.mu.Unlock()
	cb.evaluate()
	return cb.state != CircuitOpen
}

// Record records the outcome of a call. duration is used for slow-call detection.
func (cb *CircuitBreaker) Record(err error, duration time.Duration) {
	cb.mu.Lock(); defer cb.mu.Unlock()
	bkt := &cb.buckets[cb.currentIdx]
	bkt.totalCalls++
	if err == nil {
		bkt.successes++
	} else {
		bkt.failures++
	}
	if duration > cb.cfg.SlowCallDurationThreshold {
		bkt.slowCalls++
	}
	// Reset half-open counters on success in half-open
	if cb.state == CircuitHalfOpen {
		if err == nil {
			cb.halfOpenOK++
			if cb.halfOpenOK >= cb.halfOpenNeed {
				cb.transitionTo(CircuitClosed)
			}
		} else {
			cb.transitionTo(CircuitOpen)
		}
	}
	cb.evaluate()
}

func (cb *CircuitBreaker) evaluate() {
	now := time.Now()

	// Handle OPEN → HALF_OPEN timer
	if cb.state == CircuitOpen {
		if now.Sub(cb.openedAt) >= cb.cfg.WaitDurationInOpen {
			cb.transitionTo(CircuitHalfOpen)
		}
		return
	}

	if cb.state != CircuitClosed {
		return
	}

	// Aggregate rolling window
	var total, failures, slow int
	var recentTotal, recentFail int // 近 2 桶（约 12s）用于敏感度
	for _, b := range cb.buckets {
		total += b.totalCalls
		failures += b.failures
		slow += b.slowCalls
	}

	// 自适应敏感度（2026-08-13）: 常出问题的 provider 更快熔断,
	// 稳定的更宽容 — 阈值在 3-5 次区间内按近期失败率滑动。
	//   unstable（近 12s 失败率 > 15%）: 3 次 + 40% 阈值（快开）
	//   stable:                          5 次 + 60% 阈值（宽容）
	// 近 2 桶 = 最近 ~12s（60s/10 桶）。
	recentTotal = cb.buckets[cb.currentIdx].totalCalls
	recentFail = cb.buckets[cb.currentIdx].failures
	prevIdx := (cb.currentIdx - 1 + len(cb.buckets)) % len(cb.buckets)
	recentTotal += cb.buckets[prevIdx].totalCalls
	recentFail += cb.buckets[prevIdx].failures

	minCalls := cb.cfg.MinCallsBeforeEvaluation
	rateThr := cb.cfg.FailureRateThreshold
	slowThr := cb.cfg.SlowCallRateThreshold
	if recentTotal > 0 {
		recentRate := float64(recentFail) / float64(recentTotal)
		if recentRate > 0.15 {
			minCalls = 3          // 常出问题: 更快熔断
			rateThr = 0.4
			slowThr = 0.4
		} else if recentRate < 0.03 && total >= 20 {
			minCalls = 5          // 稳定: 更宽容（防瞬时抖动误伤）
			rateThr = 0.6
			slowThr = 0.6
		}
	}

	if total < minCalls {
		return // cold-start guard
	}

	failureRate := float64(failures) / float64(total)
	slowRate := float64(slow) / float64(total)

	if failureRate >= rateThr || slowRate >= slowThr {
		cb.transitionTo(CircuitOpen)
	}
}

func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	prev := cb.state
	cb.state = newState
	switch newState {
	case CircuitOpen:
		cb.openedAt = time.Now()
		// Reset window on open to avoid stale data poisoning half-open
		cb.buckets = make([]cbBucket, cb.cfg.NumBuckets)
	case CircuitHalfOpen:
		cb.halfOpenIdx = 0
		cb.halfOpenNeed = cb.cfg.HalfOpenPermits[0]
		cb.halfOpenOK = 0
	}
	if prev != newState {
		// Could emit a metric here
		_ = prev
	}
}

// AdvanceBucket moves the window forward by one bucket. Called externally on a timer.
func (cb *CircuitBreaker) AdvanceBucket() {
	cb.mu.Lock(); defer cb.mu.Unlock()
	cb.currentIdx = (cb.currentIdx + 1) % cb.cfg.NumBuckets
	cb.buckets[cb.currentIdx] = cbBucket{}
}
