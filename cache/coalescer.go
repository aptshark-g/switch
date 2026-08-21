package cache

import (
	"sync"
	"time"
)

// ── Request Coalescer: merge identical concurrent requests ──
//
// When multiple goroutines request the same cache key simultaneously,
// only the first one executes the upstream call. Others wait and
// receive the same result.

// Coalescer merges concurrent identical-key fetch requests.
type Coalescer struct {
	mu      sync.Mutex
	pending map[string]*coalesceCall
}

type coalesceCall struct {
	done chan struct{}
	val  any
	err  error
}

// NewCoalescer creates a request coalescer.
func NewCoalescer() *Coalescer {
	return &Coalescer{pending: make(map[string]*coalesceCall)}
}

// Do executes fn for key. If another goroutine is already executing fn
// for the same key, Do blocks until that call completes and returns its
// result without calling fn.
func (c *Coalescer) Do(key string, fn func() (any, error), timeout time.Duration) (any, error) {
	c.mu.Lock()
	if call, ok := c.pending[key]; ok {
		// Already in-flight — wait for it
		c.mu.Unlock()
		select {
		case <-call.done:
			return call.val, call.err
		case <-time.After(timeout):
			return nil, errCoalesceTimeout
		}
	}

	// First caller — create the pending entry
	call := &coalesceCall{done: make(chan struct{})}
	c.pending[key] = call
	c.mu.Unlock()

	// Execute
	val, err := fn()

	// Notify waiters
	call.val = val
	call.err = err
	close(call.done)

	// Cleanup
	c.mu.Lock()
	delete(c.pending, key)
	c.mu.Unlock()

	return val, err
}

// Pending returns the number of in-flight coalesced calls.
func (c *Coalescer) Pending() int {
	c.mu.Lock(); defer c.mu.Unlock()
	return len(c.pending)
}

// errCoalesceTimeout is returned when a waiter times out.
var errCoalesceTimeout = &coalesceTimeoutError{}

type coalesceTimeoutError struct{}
func (e *coalesceTimeoutError) Error() string { return "coalesce timeout" }
func (e *coalesceTimeoutError) Timeout() bool { return true }
