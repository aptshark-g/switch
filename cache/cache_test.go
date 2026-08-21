package cache

import (
	"testing"
	"time"
)

func TestSetWithTTL(t *testing.T) {
	c := New(10, time.Minute)
	c.SetWithTTL("k", "v", 50*time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("entry should exist before TTL expiry")
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("entry should be expired after per-entry TTL")
	}
}

func TestSetUsesBaseTTL(t *testing.T) {
	c := New(10, 30*time.Minute)
	c.Set("k", "v")
	ent, ok := c.entries["k"]
	if !ok {
		t.Fatal("entry missing")
	}
	if time.Until(ent.ExpiresAt) < 29*time.Minute {
		t.Fatalf("base TTL not applied: %v", time.Until(ent.ExpiresAt))
	}
}

func TestCacheEvictionAtCapacity(t *testing.T) {
	c := New(3, time.Hour)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	c.Set("d", 4) // 触发淘汰最旧
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should be evicted (oldest)")
	}
	if _, ok := c.Get("d"); !ok {
		t.Fatal("d should exist")
	}
}
