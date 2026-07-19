package provider

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ── AdaptiveConcurrency: Gradient2 self-tuning limiter ──
//
// Reference: Envoy Adaptive Concurrency (Gradient2)
// Core idea: Track minRTT over a window. When currentRTT diverges from minRTT,
// reduce concurrency. When close to minRTT, increase.

// AdaptiveSemaphore is a self-tuning concurrency limiter.
// It starts with initialMax and adjusts based on latency feedback.
type AdaptiveSemaphore struct {
	mu          sync.Mutex
	ch          chan struct{}      // concurrency slots
	waiting     atomic.Int64        // goroutines waiting

	minRTT      atomic.Int64        // nanoseconds — best recent latency
	currentMax  int                 // current concurrency cap
	minConcurrency int              // floor
	maxConcurrency int              // ceiling

	lastAdjust  time.Time
	adjustInterval time.Duration    // how often to recalculate (100ms)
	gradientWindow  time.Duration   // window for minRTT tracking (30s)

	// For minRTT estimation: keep a histogram of recent latencies
	latencies   []time.Duration
	latIdx      int
}

// AdaptiveConcurrencyConfig holds tuning parameters.
type AdaptiveConcurrencyConfig struct {
	MinConcurrency  int
	MaxConcurrency  int
	InitialMax      int
	AdjustInterval  time.Duration
	GradientWindow  time.Duration
}

// DefaultAdaptiveConfig returns sensible defaults.
func DefaultAdaptiveConfig() AdaptiveConcurrencyConfig {
	return AdaptiveConcurrencyConfig{
		MinConcurrency: 2,
		MaxConcurrency: 100,
		InitialMax:     10,
		AdjustInterval: 100 * time.Millisecond,
		GradientWindow: 30 * time.Second,
	}
}

// NewAdaptiveSemaphore creates a self-tuning semaphore.
func NewAdaptiveSemaphore(cfg AdaptiveConcurrencyConfig) *AdaptiveSemaphore {
	if cfg.InitialMax <= 0 { cfg.InitialMax = 10 }
	if cfg.MinConcurrency <= 0 { cfg.MinConcurrency = 2 }
	if cfg.MaxConcurrency <= 0 { cfg.MaxConcurrency = 100 }
	if cfg.AdjustInterval <= 0 { cfg.AdjustInterval = 100 * time.Millisecond }
	if cfg.GradientWindow <= 0 { cfg.GradientWindow = 30 * time.Second }

	a := &AdaptiveSemaphore{
		ch:             make(chan struct{}, cfg.InitialMax),
		currentMax:     cfg.InitialMax,
		minConcurrency:  cfg.MinConcurrency,
		maxConcurrency:  cfg.MaxConcurrency,
		adjustInterval:  cfg.AdjustInterval,
		gradientWindow:  cfg.GradientWindow,
		latencies:       make([]time.Duration, 100),
	}
	a.minRTT.Store(int64(1 * time.Hour)) // start high
	return a
}

// Acquire takes a concurrency slot. Blocks until available or ctx expired.
func (a *AdaptiveSemaphore) Acquire(ctx context.Context) error {
	if a == nil { return nil }
	a.waiting.Add(1)
	defer a.waiting.Add(-1)
	select {
	case a.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("adaptive concurrency limit: %w", ctx.Err())
	}
}

// Release returns a slot.
func (a *AdaptiveSemaphore) Release() {
	if a == nil { return }
	<-a.ch
}

// RecordLatency feeds a response latency sample for gradient computation.
func (a *AdaptiveSemaphore) RecordLatency(d time.Duration) {
	if a == nil { return }
	a.mu.Lock()
	a.latencies[a.latIdx] = d
	a.latIdx = (a.latIdx + 1) % len(a.latencies)
	a.mu.Unlock()

	// Update minRTT
	for {
		prev := a.minRTT.Load()
		if d.Nanoseconds() >= prev { break }
		if a.minRTT.CompareAndSwap(prev, d.Nanoseconds()) { break }
	}
}

// Adjust recalculates the concurrency target (called on a timer).
func (a *AdaptiveSemaphore) Adjust() {
	if a == nil { return }
	a.mu.Lock()
	// Don't adjust too frequently
	if time.Since(a.lastAdjust) < a.adjustInterval {
		a.mu.Unlock()
		return
	}
	a.lastAdjust = time.Now()

	// Compute p50 of recent latencies as currentRTT
	var sorted []time.Duration
	for _, l := range a.latencies {
		if l > 0 { sorted = append(sorted, l) }
	}
	if len(sorted) == 0 { a.mu.Unlock(); return }
	// Quick p50: use middle element of unsorted (close enough for gradient)
	currentRTT := sorted[len(sorted)/2]

	minRTT := time.Duration(a.minRTT.Load())

	rate := float64(currentRTT) / float64(minRTT)
	newMax := a.currentMax

	switch {
	case rate > 2.0:
		// RTT is more than 2× optimal — aggressive scale down
		newMax = a.currentMax / 2
	case rate > 1.3:
		// RTT is elevated — gentle scale down
		newMax = int(float64(a.currentMax) * 0.9)
	default:
		// RTT close to optimal — gradual scale up
		newMax = a.currentMax + 1
	}

	// Clamp
	if newMax < a.minConcurrency { newMax = a.minConcurrency }
	if newMax > a.maxConcurrency { newMax = a.maxConcurrency }

	if newMax == a.currentMax {
		a.mu.Unlock()
		return
	}

	// Resize channel: create new channel and migrate
	old := a.ch
	a.ch = make(chan struct{}, newMax)
	a.currentMax = newMax

	// Drain old channel's in-flight slots into new
	count := len(old)
	for i := 0; i < count; i++ {
		select {
		case a.ch <- struct{}{}:
		default:
		}
	}
	a.mu.Unlock()

	// Release old channel's remaining slots
	for len(old) > 0 {
		<-old
	}
}

// Waiting returns the number of goroutines waiting for a slot.
func (a *AdaptiveSemaphore) Waiting() int64 {
	if a == nil { return 0 }
	return a.waiting.Load()
}

// CurrentMax returns the current concurrency cap.
func (a *AdaptiveSemaphore) CurrentMax() int {
	if a == nil { return 0 }
	a.mu.Lock(); defer a.mu.Unlock()
	return a.currentMax
}
