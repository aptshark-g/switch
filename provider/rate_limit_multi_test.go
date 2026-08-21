package provider

import "testing"

func TestMultiRateLimiterKeyLimit(t *testing.T) {
	mrl := NewMultiRateLimiter(0, 0)
	mrl.SetKeyLimit("k1", 2, 10000)
	for i := 0; i < 2; i++ {
		if ok, reason := mrl.Allow("k1", "m", 10); !ok {
			t.Fatalf("call %d rejected: %s (want allowed)", i, reason)
		}
	}
	if ok, reason := mrl.Allow("k1", "m", 10); ok {
		t.Fatal("3rd call allowed, want key_rate_limit")
	} else if reason != "key_rate_limit" {
		t.Fatalf("reason = %s, want key_rate_limit", reason)
	}
	// 其他 key 不受影响
	if ok, _ := mrl.Allow("k2", "m", 10); !ok {
		t.Fatal("k2 should not be limited by k1")
	}
}

func TestMultiRateLimiterModelLimit(t *testing.T) {
	mrl := NewMultiRateLimiter(0, 0)
	mrl.SetModelLimit("m1", 1, 10000)
	if ok, _ := mrl.Allow("k", "m1", 10); !ok {
		t.Fatal("first call should be allowed")
	}
	if ok, reason := mrl.Allow("k", "m1", 10); ok || reason != "model_rate_limit" {
		t.Fatalf("2nd call: ok=%v reason=%s, want model_rate_limit", ok, reason)
	}
	if ok, _ := mrl.Allow("k", "m2", 10); !ok {
		t.Fatal("m2 should not be limited by m1")
	}
}

func TestMultiRateLimiterClearAndSnapshots(t *testing.T) {
	mrl := NewMultiRateLimiter(0, 0)
	mrl.SetKeyLimit("k1", 5, 500)
	mrl.SetModelLimit("m1", 3, 300)
	if got := mrl.KeyLimits()["k1"]; got != [2]int{5, 500} {
		t.Fatalf("KeyLimits k1 = %v, want [5 500]", got)
	}
	if got := mrl.ModelLimits()["m1"]; got != [2]int{3, 300} {
		t.Fatalf("ModelLimits m1 = %v, want [3 300]", got)
	}
	mrl.ClearKeyLimit("k1")
	mrl.ClearModelLimit("m1")
	if _, ok := mrl.KeyLimits()["k1"]; ok {
		t.Fatal("k1 should be cleared")
	}
	if _, ok := mrl.ModelLimits()["m1"]; ok {
		t.Fatal("m1 should be cleared")
	}
}

func TestMultiRateLimiterTokenLimit(t *testing.T) {
	mrl := NewMultiRateLimiter(0, 0)
	mrl.SetKeyLimit("k1", 0, 100)
	if ok, _ := mrl.Allow("k1", "m", 60); !ok {
		t.Fatal("60 < 100 should pass")
	}
	if ok, reason := mrl.Allow("k1", "m", 60); ok || reason != "key_token_limit" {
		t.Fatalf("ok=%v reason=%s, want key_token_limit", ok, reason)
	}
}
