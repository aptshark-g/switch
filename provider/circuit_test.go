package provider

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerOpensOnFailures(t *testing.T) {
	cb := NewCircuitBreakerWithConfig("t", CircuitBreakerConfig{
		NumBuckets:               10,
		FailureRateThreshold:     0.5,
		MinCallsBeforeEvaluation: 5,
		WaitDurationInOpen:       60 * time.Second,
		HalfOpenPermits:          []int{1},
	})
	for i := 0; i < 10; i++ {
		cb.Record(errors.New("fail"), 100*time.Millisecond)
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want open after 10 failures", cb.State())
	}
	if cb.Allow() {
		t.Fatal("Allow should be false when open")
	}
}

func TestCircuitBreakerStaysClosedOnSuccess(t *testing.T) {
	cb := NewCircuitBreakerWithConfig("t", CircuitBreakerConfig{
		NumBuckets:               10,
		FailureRateThreshold:     0.5,
		MinCallsBeforeEvaluation: 5,
		WaitDurationInOpen:       60 * time.Second,
		HalfOpenPermits:          []int{1},
	})
	for i := 0; i < 20; i++ {
		cb.Record(nil, 10*time.Millisecond)
	}
	if cb.State() != CircuitClosed {
		t.Fatalf("state = %v, want closed on all-success", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("Allow should be true when closed")
	}
}

func TestCircuitBreakerRecoversHalfOpen(t *testing.T) {
	cb := NewCircuitBreakerWithConfig("t", CircuitBreakerConfig{
		NumBuckets:               10,
		FailureRateThreshold:     0.5,
		MinCallsBeforeEvaluation: 5,
		WaitDurationInOpen:       1 * time.Millisecond, // 立即进入 half-open
		HalfOpenPermits:          []int{1},
	})
	for i := 0; i < 10; i++ {
		cb.Record(errors.New("fail"), 100*time.Millisecond)
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want open", cb.State())
	}
	time.Sleep(5 * time.Millisecond)
	if cb.State() != CircuitHalfOpen {
		t.Fatalf("state = %v, want half-open after wait", cb.State())
	}
	// half-open 成功 → 回 closed
	if !cb.Allow() {
		t.Fatal("half-open should allow a probe")
	}
	cb.Record(nil, 10*time.Millisecond)
	if cb.State() != CircuitClosed {
		t.Fatalf("state = %v, want closed after half-open success", cb.State())
	}
}
