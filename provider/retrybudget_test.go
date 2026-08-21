package provider

import (
	"testing"
	"time"
)

func TestRetryBudgetCaps(t *testing.T) {
	rb := NewRetryBudget(2)
	if !rb.TryConsume() || !rb.TryConsume() {
		t.Fatal("first two consumes should pass")
	}
	if rb.TryConsume() {
		t.Fatal("3rd consume should be rejected (budget 2/min)")
	}
}

func TestRetryBudgetUnlimited(t *testing.T) {
	rb := NewRetryBudget(0)
	for i := 0; i < 100; i++ {
		if !rb.TryConsume() {
			t.Fatal("budget 0 = unlimited, should always pass")
		}
	}
}

func TestRetryBudgetRefill(t *testing.T) {
	// maxPerMin=60 → 1 token/秒。耗光后等 1.1s 应恢复 ≥1 token。
	rb := NewRetryBudget(60)
	for rb.TryConsume() {
	}
	time.Sleep(1100 * time.Millisecond)
	if !rb.TryConsume() {
		t.Fatal("after refill window, consume should pass")
	}
}
