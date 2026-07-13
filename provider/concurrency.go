package provider

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Semaphore is a lightweight concurrency limiter backed by a buffered channel.
// Acquire blocks until a slot is available or the context is cancelled.
type Semaphore struct {
	ch      chan struct{}
	waiting atomic.Int64
}

// NewSemaphore creates a semaphore with the given maximum concurrency.
// A max <= 0 means unlimited.
func NewSemaphore(max int) *Semaphore {
	if max <= 0 {
		return nil
	}
	return &Semaphore{ch: make(chan struct{}, max)}
}

// Acquire takes a slot. It blocks until one is available or ctx expires.
func (s *Semaphore) Acquire(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.waiting.Add(1)
	defer s.waiting.Add(-1)
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("concurrency limit: %w", ctx.Err())
	}
}

// Release returns a slot to the pool.
func (s *Semaphore) Release() {
	if s == nil {
		return
	}
	<-s.ch
}

// Waiting returns the number of goroutines currently waiting for a slot.
func (s *Semaphore) Waiting() int64 {
	if s == nil {
		return 0
	}
	return s.waiting.Load()
}

// ---------------------------------------------------------------------------
// Provider-level rate limiter (RPM / TPM)
// ---------------------------------------------------------------------------

// ProviderRateLimiter enforces requests-per-minute and tokens-per-minute
// limits on a single provider using the token bucket algorithm.
type ProviderRateLimiter struct {
	mu             sync.Mutex
	rpmBucket      float64 // remaining requests this window
	tpmBucket      float64 // remaining tokens this window
	rpmLimit       float64
	tpmLimit       float64
	lastRefill     time.Time
}

// NewProviderRateLimiter creates a rate limiter with RPM and TPM caps.
// Zero values disable the respective limit.
func NewProviderRateLimiter(rpm, tpm int) *ProviderRateLimiter {
	return &ProviderRateLimiter{
		rpmBucket:  float64(rpm),
		tpmBucket:  float64(tpm),
		rpmLimit:   float64(rpm),
		tpmLimit:   float64(tpm),
		lastRefill: time.Now(),
	}
}

// Allow checks if a request with the given token count can proceed.
// Returns true if both RPM and TPM allow it, and deducts from the buckets.
func (rl *ProviderRateLimiter) Allow(tokenCount int) bool {
	if rl.rpmLimit <= 0 && rl.tpmLimit <= 0 {
		return true
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.refill()
	if rl.rpmLimit > 0 && rl.rpmBucket < 1 {
		return false
	}
	if rl.tpmLimit > 0 && rl.tpmBucket < float64(tokenCount) {
		return false
	}
	if rl.rpmLimit > 0 {
		rl.rpmBucket--
	}
	if rl.tpmLimit > 0 {
		rl.tpmBucket -= float64(tokenCount)
	}
	return true
}

func (rl *ProviderRateLimiter) refill() {
	elapsed := time.Since(rl.lastRefill).Minutes()
	if elapsed <= 0 {
		return
	}
	rl.lastRefill = time.Now()
	if rl.rpmLimit > 0 {
		rl.rpmBucket += elapsed * rl.rpmLimit
		if rl.rpmBucket > rl.rpmLimit {
			rl.rpmBucket = rl.rpmLimit
		}
	}
	if rl.tpmLimit > 0 {
		rl.tpmBucket += elapsed * rl.tpmLimit
		if rl.tpmBucket > rl.tpmLimit {
			rl.tpmBucket = rl.tpmLimit
		}
	}
}
