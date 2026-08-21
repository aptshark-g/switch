package prefix

import (
	"testing"
	"time"
)

func TestRingLookupDeterministic(t *testing.T) {
	r := NewRing([]AffinityNode{{Name: "a", Weight: 1}, {Name: "b", Weight: 1}})
	for _, key := range []string{"k1", "k2", "fp0123:tenant"} {
		first := r.Lookup(key)
		for i := 0; i < 20; i++ {
			if got := r.Lookup(key); got != first {
				t.Fatalf("key %s: lookup non-deterministic: %s vs %s", key, got, first)
			}
		}
	}
}

func TestRingWeightDistribution(t *testing.T) {
	r := NewRing([]AffinityNode{{Name: "heavy", Weight: 9}, {Name: "light", Weight: 1}})
	heavy := 0
	const n = 2000
	for i := 0; i < n; i++ {
		if r.Lookup("key-" + itoa(i)) == "heavy" {
			heavy++
		}
	}
	share := float64(heavy) / n
	if share < 0.82 || share > 0.96 {
		t.Fatalf("heavy share = %.3f, want ≈0.9", share)
	}
}

func TestAffinityDecidePrimary(t *testing.T) {
	a := NewAffinityRouter([]AffinityNode{{Name: "a", Weight: 1}, {Name: "b", Weight: 1}}, 1.25, time.Second)
	got, reason := a.Decide("fp1", func(string) bool { return true })
	if got == "" || reason != "primary" {
		t.Fatalf("Decide = (%q, %s), want primary", got, reason)
	}
	// 同 key 稳定
	got2, _ := a.Decide("fp1", func(string) bool { return true })
	if got2 != got {
		t.Fatalf("same key routed to %q then %q, want stable", got, got2)
	}
}

func TestAffinityDecideOverflow(t *testing.T) {
	a := NewAffinityRouter([]AffinityNode{{Name: "primary", Weight: 1}, {Name: "other", Weight: 1}}, 1.0, time.Second)
	// 先确定 primary（不 overloaded）
	p, _ := a.Decide("fp", func(string) bool { return true })
	// 把 primary 打到过载（cap = 1.0*1 = 1 → inflight 2 即过载）
	a.Inc(p)
	a.Inc(p)
	got, reason := a.Decide("fp", func(string) bool { return true })
	if got == p {
		t.Fatalf("overloaded primary should be skipped, got %q", got)
	}
	if got != "other" || reason != "primary" {
		t.Fatalf("Decide = (%q, %s), want (other, primary)", got, reason)
	}
	a.Dec(p)
	a.Dec(p)
}

func TestAffinityOverflowSticky(t *testing.T) {
	a := NewAffinityRouter([]AffinityNode{{Name: "a", Weight: 1}, {Name: "b", Weight: 1}}, 1.0, 5*time.Second)
	// 找 a 为 primary 的 key; 让 a 过载 → 溢出到 b, 记录 sticky
	var key string
	for i := 0; i < 100; i++ {
		k := "k" + itoa(i)
		if p, _ := a.Decide(k, func(string) bool { return true }); p == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key mapping to a found")
	}
	a.Inc("a")
	a.Inc("a")
	got, reason := a.Decide(key, func(string) bool { return true })
	if reason != "primary" || got == "a" {
		t.Fatalf("got (%q, %s), want overflow to b", got, reason)
	}
	a.RecordOverflow(key, got)
	// a 恢复后, sticky 仍走 b
	a.Dec("a")
	a.Dec("a")
	got2, reason2 := a.Decide(key, func(string) bool { return true })
	if reason2 != "overflow" || got2 != got {
		t.Fatalf("sticky: got (%q, %s), want (%q, overflow)", got2, reason2, got)
	}
}

func TestAffinityDecideFallback(t *testing.T) {
	a := NewAffinityRouter([]AffinityNode{{Name: "a", Weight: 1}, {Name: "b", Weight: 1}}, 1.0, time.Second)
	a.Inc("a")
	a.Inc("a")
	a.Inc("b")
	a.Inc("b")
	got, reason := a.Decide("fp", func(string) bool { return true })
	if got != "" || reason != "fallback" {
		t.Fatalf("Decide = (%q, %s), want fallback when all overloaded", got, reason)
	}
}

func TestAffinityDecideSkipsUnhealthy(t *testing.T) {
	a := NewAffinityRouter([]AffinityNode{{Name: "a", Weight: 1}, {Name: "b", Weight: 1}}, 1.25, time.Second)
	// 找到 primary 为 a 的 key; 若 a 不健康 → 应落到 b
	var key string
	for i := 0; i < 100; i++ {
		k := "k" + itoa(i)
		if p, _ := a.Decide(k, func(string) bool { return true }); p == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key mapping to a found")
	}
	got, _ := a.Decide(key, func(name string) bool { return name != "a" })
	if got != "b" {
		t.Fatalf("unhealthy a should be skipped, got %q", got)
	}
}

func TestAffinityInflight(t *testing.T) {
	a := NewAffinityRouter(nil, 1.25, time.Second)
	a.Inc("x")
	a.Inc("x")
	if a.Inflight("x") != 2 {
		t.Fatalf("inflight x = %d, want 2", a.Inflight("x"))
	}
	a.Dec("x")
	a.Dec("x")
	a.Dec("x") // 不应负
	if a.Inflight("x") != 0 {
		t.Fatalf("inflight x = %d, want 0", a.Inflight("x"))
	}
}
