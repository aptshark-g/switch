package provider

import (
	"sync"
	"time"
)

type CircuitBreaker struct {
	name string

	mu              sync.Mutex
	state           CircuitState
	failures        int
	lastFailureTime time.Time

	maxFailures int
	cooldownMs  int64
}

const (
	defaultMaxFailures = 5
	defaultCooldownMs  = 30000
)

func NewCircuitBreaker(name string) *CircuitBreaker {
	return &CircuitBreaker{
		name:        name,
		state:       CircuitClosed,
		maxFailures: defaultMaxFailures,
		cooldownMs:  defaultCooldownMs,
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeTransition()
	return cb.state
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeTransition()
	return cb.state != CircuitOpen
}

func (cb *CircuitBreaker) Record(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err == nil {
		cb.onSuccess()
	} else {
		cb.onFailure()
	}
}

func (cb *CircuitBreaker) maybeTransition() {
	if cb.state == CircuitOpen {
		if time.Since(cb.lastFailureTime).Milliseconds() > cb.cooldownMs {
			cb.state = CircuitHalfOpen
		}
	}
}

func (cb *CircuitBreaker) onSuccess() {
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitClosed
	}
	cb.failures = 0
}

func (cb *CircuitBreaker) onFailure() {
	cb.failures++
	cb.lastFailureTime = time.Now()
	if cb.failures >= cb.maxFailures {
		cb.state = CircuitOpen
	}
}
