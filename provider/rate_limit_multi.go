package provider

import (
	"sync"
	"time"
)

// ── Multi-level rate limiter: per-key → per-model → per-provider ──

// MultiRateLimiter enforces rate limits at three levels.
// Any level exhausted → request rejected.
type MultiRateLimiter struct {
	mu         sync.Mutex
	perKey    map[string]*tokenBucket
	perModel   map[string]*tokenBucket
	perProvider *tokenBucket
}

type tokenBucket struct {
	rpm       float64 // requests per minute (0=unlimited)
	tpm       float64 // tokens per minute (0=unlimited)
	reqBucket float64 // remaining requests this window
	tokBucket float64 // remaining tokens this window
	lastFill  time.Time
}

// NewMultiRateLimiter creates a rate limiter with per-provider defaults.
func NewMultiRateLimiter(providerRPM, providerTPM int) *MultiRateLimiter {
	return &MultiRateLimiter{
		perKey:      make(map[string]*tokenBucket),
		perModel:    make(map[string]*tokenBucket),
		perProvider: newBucket(providerRPM, providerTPM),
	}
}

func newBucket(rpm, tpm int) *tokenBucket {
	return &tokenBucket{
		rpm:       float64(rpm),
		tpm:       float64(tpm),
		reqBucket: float64(rpm),
		tokBucket: float64(tpm),
		lastFill:  time.Now(),
	}
}

// SetKeyLimit configures a per-API-key rate limit.
func (mrl *MultiRateLimiter) SetKeyLimit(key string, rpm, tpm int) {
	mrl.mu.Lock(); defer mrl.mu.Unlock()
	mrl.perKey[key] = newBucket(rpm, tpm)
}

// SetModelLimit configures a per-model rate limit.
func (mrl *MultiRateLimiter) SetModelLimit(model string, rpm, tpm int) {
	mrl.mu.Lock(); defer mrl.mu.Unlock()
	mrl.perModel[model] = newBucket(rpm, tpm)
}

// Allow checks all three levels. tokenCount is the estimated tokens for this request.
// Returns false if any level is exhausted, with a reason string.
func (mrl *MultiRateLimiter) Allow(apiKey, model string, tokenCount int) (bool, string) {
	mrl.mu.Lock(); defer mrl.mu.Unlock()

	// Level 1: Per API Key
	if b, ok := mrl.perKey[apiKey]; ok {
		b.refill()
		if b.rpm > 0 && b.reqBucket < 1 { return false, "key_rate_limit" }
		if b.tpm > 0 && b.tokBucket < float64(tokenCount) { return false, "key_token_limit" }
	}

	// Level 2: Per Model
	if b, ok := mrl.perModel[model]; ok {
		b.refill()
		if b.rpm > 0 && b.reqBucket < 1 { return false, "model_rate_limit" }
		if b.tpm > 0 && b.tokBucket < float64(tokenCount) { return false, "model_token_limit" }
	}

	// Level 3: Per Provider
	if mrl.perProvider != nil {
		mrl.perProvider.refill()
		if mrl.perProvider.rpm > 0 && mrl.perProvider.reqBucket < 1 { return false, "provider_rate_limit" }
		if mrl.perProvider.tpm > 0 && mrl.perProvider.tokBucket < float64(tokenCount) { return false, "provider_token_limit" }
	}

	// All passed — deduct from all levels
	if b, ok := mrl.perKey[apiKey]; ok {
		if b.rpm > 0 { b.reqBucket-- }
		if b.tpm > 0 { b.tokBucket -= float64(tokenCount) }
	}
	if b, ok := mrl.perModel[model]; ok {
		if b.rpm > 0 { b.reqBucket-- }
		if b.tpm > 0 { b.tokBucket -= float64(tokenCount) }
	}
	if mrl.perProvider != nil {
		if mrl.perProvider.rpm > 0 { mrl.perProvider.reqBucket-- }
		if mrl.perProvider.tpm > 0 { mrl.perProvider.tokBucket -= float64(tokenCount) }
	}
	return true, ""
}

func (b *tokenBucket) refill() {
	elapsed := time.Since(b.lastFill).Minutes()
	if elapsed <= 0 { return }
	b.lastFill = time.Now()
	if b.rpm > 0 {
		b.reqBucket += elapsed * b.rpm
		if b.reqBucket > b.rpm { b.reqBucket = b.rpm }
	}
	if b.tpm > 0 {
		b.tokBucket += elapsed * b.tpm
		if b.tokBucket > b.tpm { b.tokBucket = b.tpm }
	}
}
