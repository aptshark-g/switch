// Package cache provides an in-memory LRU-like cache for prompt deduplication
// and response caching with TTL-based eviction.
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// CacheEntry holds a cached response with its expiry time.
type CacheEntry struct {
	Value     any
	ExpiresAt time.Time
}

// Cache is a thread-safe in-memory cache with TTL eviction.
// It uses a hash of the request as the key for deduplication.
type Cache struct {
	mu       sync.RWMutex
	entries  map[string]*CacheEntry
	maxSize  int
	ttl      time.Duration
}

// New creates a cache with the given maximum number of entries and TTL.
func New(maxSize int, ttl time.Duration) *Cache {
	c := &Cache{
		entries: make(map[string]*CacheEntry),
		maxSize: maxSize,
		ttl:     ttl,
	}
	go c.evictLoop()
	return c
}

// Get returns the cached value if present and not expired.
func (c *Cache) Get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry.Value, true
}

// Set stores a value in the cache with the configured TTL.
func (c *Cache) Set(key string, value any) {
	c.SetWithTTL(key, value, c.ttl)
}

// SetWithTTL stores a value with a per-entry TTL（2026-08-22, D1 分层 TTL:
// 高价值前缀 30min / 普通 5min; warmup 禁写 L0 由调用方保证）。
func (c *Cache) SetWithTTL(key string, value any, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict oldest if at capacity.
	if len(c.entries) >= c.maxSize {
		var oldest string
		var oldestTime time.Time
		for k, e := range c.entries {
			if oldest == "" || e.ExpiresAt.Before(oldestTime) {
				oldest = k
				oldestTime = e.ExpiresAt
			}
		}
		delete(c.entries, oldest)
	}

	c.entries[key] = &CacheEntry{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	}
}

func (c *Cache) evictLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.evictExpired()
	}
}

func (c *Cache) evictExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, e := range c.entries {
		if now.After(e.ExpiresAt) {
			delete(c.entries, k)
		}
	}
}

// Size returns the current number of cached entries.
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// HashKey creates a deterministic hash key from a request's messages and model.
// This enables prompt deduplication: identical prompts hash to the same key.
func HashKey(messages string, model string) string {
	h := sha256.Sum256([]byte(model + "::" + messages))
	return hex.EncodeToString(h[:])
}
