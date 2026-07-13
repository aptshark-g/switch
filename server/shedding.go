package server

import (
	"log"
	"net/http"
	"sync/atomic"
)

// LoadShedder rejects new requests when the number of in-flight requests
// exceeds the configured maximum. This prevents cascading failures when
// all providers are slow or circuit-open.
type LoadShedder struct {
	max      int64
	inFlight atomic.Int64
	shed     atomic.Int64 // total shed requests since start
}

// NewLoadShedder creates a shedder with the given max concurrency.
// Use 0 to disable (defaults to a generous value).
func NewLoadShedder(max int64) *LoadShedder {
	if max <= 0 {
		max = 5000
	}
	return &LoadShedder{max: max}
}

// Allow returns true if the request can proceed. When false, the caller
// should return 503 immediately.
func (ls *LoadShedder) Allow() bool {
	current := ls.inFlight.Add(1)
	if current > ls.max {
		ls.inFlight.Add(-1)
		ls.shed.Add(1)
		return false
	}
	return true
}

// Done signals that a request has completed.
func (ls *LoadShedder) Done() {
	ls.inFlight.Add(-1)
}

// InFlight returns the current number of in-flight requests.
func (ls *LoadShedder) InFlight() int64 {
	return ls.inFlight.Load()
}

// Shed returns the total number of shed (rejected) requests.
func (ls *LoadShedder) Shed() int64 {
	return ls.shed.Load()
}

// LoadSheddingMiddleware wraps a handler and rejects requests when
// the global in-flight count exceeds the limit.
func LoadSheddingMiddleware(shedder *LoadShedder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !shedder.Allow() {
				log.Printf("shedding: rejecting request (in-flight=%d, shed=%d)", shedder.InFlight(), shedder.Shed())
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "server overloaded, try again later",
				})
				return
			}
			defer shedder.Done()
			next.ServeHTTP(w, r)
		})
	}
}
