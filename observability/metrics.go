package observability

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	name  string
	value atomic.Int64
}

func (c *Counter) Inc()         { c.value.Add(1) }
func (c *Counter) Add(n int64)  { c.value.Add(n) }
func (c *Counter) Value() int64 { return c.value.Load() }

type Gauge struct {
	name  string
	value atomic.Int64
}

func (g *Gauge) Set(v int64)  { g.value.Store(v) }
func (g *Gauge) Inc()         { g.value.Add(1) }
func (g *Gauge) Dec()         { g.value.Add(-1) }
func (g *Gauge) Value() int64 { return g.value.Load() }

type Histogram struct {
	name    string
	buckets []float64
	counts  []atomic.Int64
	sum     atomic.Int64
	count   atomic.Int64
}

var defaultLatencyBuckets = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, math.Inf(1)}

func NewHistogram(name string) *Histogram {
	b := defaultLatencyBuckets
	return &Histogram{name: name, buckets: b, counts: make([]atomic.Int64, len(b))}
}

func (h *Histogram) Observe(ms float64) {
	h.sum.Add(int64(ms * 1000))
	h.count.Add(1)
	for i, b := range h.buckets {
		if ms <= b {
			h.counts[i].Add(1)
			return
		}
	}
}

type HistogramSnapshot struct {
	Count int64   `json:"count"`
	Avg   float64 `json:"avg_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
}

func (h *Histogram) Snapshot() *HistogramSnapshot {
	bc := make([]int64, len(h.buckets))
	for i := range h.buckets {
		bc[i] = h.counts[i].Load()
	}
	cnt := h.count.Load()
	sum := h.sum.Load()
	var avg, p50, p95, p99 float64
	if cnt > 0 {
		avg = float64(sum) / float64(cnt) / 1000.0
		p50 = h.percentile(0.50, bc, cnt)
		p95 = h.percentile(0.95, bc, cnt)
		p99 = h.percentile(0.99, bc, cnt)
	}
	return &HistogramSnapshot{Count: cnt, Avg: avg, P50: p50, P95: p95, P99: p99}
}

func (h *Histogram) percentile(p float64, counts []int64, total int64) float64 {
	if total == 0 {
		return 0
	}
	target := float64(total) * p
	var cum int64
	for i, c := range counts {
		cum += c
		if float64(cum) >= target {
			lo := 0.0
			if i > 0 {
				lo = h.buckets[i-1]
			}
			hi := h.buckets[i]
			if hi == math.Inf(1) {
				return lo
			}
			return lo + (target-float64(cum-c))/float64(c)*(hi-lo)
		}
	}
	return h.buckets[len(h.buckets)-2]
}

// ---------------------------------------------------------------------------
// Labeled helpers
// ---------------------------------------------------------------------------

type labeledCounter struct {
	mu      sync.RWMutex
	m       map[string]*Counter
}

func newLabeledCounter() *labeledCounter {
	return &labeledCounter{m: make(map[string]*Counter)}
}

func (lc *labeledCounter) get(key string) *Counter {
	lc.mu.RLock()
	v, ok := lc.m[key]
	lc.mu.RUnlock()
	if ok {
		return v
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if v, ok = lc.m[key]; ok {
		return v
	}
	v = &Counter{}
	lc.m[key] = v
	return v
}

func (lc *labeledCounter) snapshot() map[string]int64 {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	out := make(map[string]int64, len(lc.m))
	for k, v := range lc.m {
		if v.Value() > 0 {
			out[k] = v.Value()
		}
	}
	return out
}

type labeledHistogram struct {
	mu sync.RWMutex
	m  map[string]*Histogram
}

func newLabeledHistogram() *labeledHistogram {
	return &labeledHistogram{m: make(map[string]*Histogram)}
}

func (lh *labeledHistogram) get(key string) *Histogram {
	lh.mu.RLock()
	v, ok := lh.m[key]
	lh.mu.RUnlock()
	if ok {
		return v
	}
	lh.mu.Lock()
	defer lh.mu.Unlock()
	if v, ok = lh.m[key]; ok {
		return v
	}
	v = NewHistogram("")
	lh.m[key] = v
	return v
}

func (lh *labeledHistogram) snapshot() map[string]*HistogramSnapshot {
	lh.mu.RLock()
	defer lh.mu.RUnlock()
	out := make(map[string]*HistogramSnapshot, len(lh.m))
	for k, v := range lh.m {
		s := v.Snapshot()
		if s.Count > 0 {
			out[k] = s
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

type Registry struct {
	RequestsByProvider  *labeledCounter
	RequestsByModel     *labeledCounter
	RequestsByEndpoint  *labeledCounter
	RequestsByStatus    *labeledCounter
	ErrorsByProvider    *labeledCounter
	TokensPrompt        *labeledCounter
	TokensCompletion    *labeledCounter
	LatencyByProvider   *labeledHistogram

	CacheHits      Counter
	CacheMisses    Counter
	RateLimitHits  Counter
	CircuitOpens   Counter
	StreamRequests Counter
	NonStreamReqs  Counter

	ActiveConnections Gauge
	CacheSize         Gauge

	startTime time.Time
}

func NewRegistry() *Registry {
	return &Registry{
		RequestsByProvider: newLabeledCounter(),
		RequestsByModel:    newLabeledCounter(),
		RequestsByEndpoint: newLabeledCounter(),
		RequestsByStatus:   newLabeledCounter(),
		ErrorsByProvider:   newLabeledCounter(),
		TokensPrompt:       newLabeledCounter(),
		TokensCompletion:   newLabeledCounter(),
		LatencyByProvider:  newLabeledHistogram(),
		startTime:          time.Now(),
	}
}

func (r *Registry) IncModel(model string) { r.RequestsByModel.get(model).Inc() }

func (r *Registry) Uptime() float64 { return time.Since(r.startTime).Seconds() }


// MetricsSnapshot is the JSON-serialisable view.
type MetricsSnapshot struct {
	UptimeSeconds      float64                          `json:"uptime_seconds"`
	RequestsByProvider map[string]int64                 `json:"requests_by_provider"`
	RequestsByModel    map[string]int64                 `json:"requests_by_model"`
	RequestsByEndpoint map[string]int64                 `json:"requests_by_endpoint"`
	RequestsByStatus   map[string]int64                 `json:"requests_by_status"`
	ErrorsByProvider   map[string]int64                 `json:"errors_by_provider"`
	TokensPrompt       map[string]int64                 `json:"tokens_prompt"`
	TokensCompletion   map[string]int64                 `json:"tokens_completion"`
	LatencyByProvider  map[string]*HistogramSnapshot     `json:"latency_by_provider"`
	CacheHits          int64                            `json:"cache_hits"`
	CacheMisses        int64                            `json:"cache_misses"`
	RateLimitHits      int64                            `json:"rate_limit_hits"`
	CircuitOpens       int64                            `json:"circuit_opens"`
	StreamRequests     int64                            `json:"stream_requests"`
	NonStreamRequests  int64                            `json:"non_stream_requests"`
	ActiveConnections  int64                            `json:"active_connections"`
	CacheSize          int64                            `json:"cache_size"`
}

func (r *Registry) Snapshot() *MetricsSnapshot {
	return &MetricsSnapshot{
		UptimeSeconds:      r.Uptime(),
		RequestsByProvider: r.RequestsByProvider.snapshot(),
		RequestsByModel:    r.RequestsByModel.snapshot(),
		RequestsByEndpoint: r.RequestsByEndpoint.snapshot(),
		RequestsByStatus:   r.RequestsByStatus.snapshot(),
		ErrorsByProvider:   r.ErrorsByProvider.snapshot(),
		TokensPrompt:       r.TokensPrompt.snapshot(),
		TokensCompletion:   r.TokensCompletion.snapshot(),
		LatencyByProvider:  r.LatencyByProvider.snapshot(),
		CacheHits:          r.CacheHits.Value(),
		CacheMisses:        r.CacheMisses.Value(),
		RateLimitHits:      r.RateLimitHits.Value(),
		CircuitOpens:       r.CircuitOpens.Value(),
		StreamRequests:     r.StreamRequests.Value(),
		NonStreamRequests:  r.NonStreamReqs.Value(),
		ActiveConnections:  r.ActiveConnections.Value(),
		CacheSize:          r.CacheSize.Value(),
	}
}

func (r *Registry) PrometheusText() string {
	var b strings.Builder
	s := r.Snapshot()

	writeCounter(&b, "gateway_cache_hits", s.CacheHits)
	writeCounter(&b, "gateway_cache_misses", s.CacheMisses)
	writeCounter(&b, "gateway_rate_limit_hits", s.RateLimitHits)
	writeCounter(&b, "gateway_circuit_opens", s.CircuitOpens)
	writeCounter(&b, "gateway_stream_requests", s.StreamRequests)
	writeCounter(&b, "gateway_nonstream_requests", s.NonStreamRequests)
	writeGauge(&b, "gateway_active_connections", s.ActiveConnections)
	writeGauge(&b, "gateway_cache_size", s.CacheSize)
	writeGauge(&b, "gateway_uptime_seconds", int64(s.UptimeSeconds))

	labeled(&b, "gateway_requests", "provider", s.RequestsByProvider)
	labeled(&b, "gateway_requests", "model", s.RequestsByModel)
	labeled(&b, "gateway_requests", "endpoint", s.RequestsByEndpoint)
	labeled(&b, "gateway_requests", "status", s.RequestsByStatus)
	labeled(&b, "gateway_errors", "provider", s.ErrorsByProvider)
	labeled(&b, "gateway_tokens_prompt", "provider", s.TokensPrompt)
	labeled(&b, "gateway_tokens_completion", "provider", s.TokensCompletion)

	keys := sortedKeys(s.LatencyByProvider)
	for _, k := range keys {
		h := s.LatencyByProvider[k]
		fmt.Fprintf(&b, "gateway_request_latency_ms_count{provider=\"%s\"} %d\n", k, h.Count)
		fmt.Fprintf(&b, "gateway_request_latency_ms_p50{provider=\"%s\"} %.3f\n", k, h.P50)
		fmt.Fprintf(&b, "gateway_request_latency_ms_p95{provider=\"%s\"} %.3f\n", k, h.P95)
		fmt.Fprintf(&b, "gateway_request_latency_ms_p99{provider=\"%s\"} %.3f\n", k, h.P99)
	}
	return b.String()
}

func writeCounter(b *strings.Builder, name string, value int64) {
	fmt.Fprintf(b, "# TYPE %s counter\n%s %d\n", name, name, value)
}

func writeGauge(b *strings.Builder, name string, value int64) {
	fmt.Fprintf(b, "# TYPE %s gauge\n%s %d\n", name, name, value)
}

func labeled(b *strings.Builder, metric, label string, m map[string]int64) {
	for k, v := range m {
		fmt.Fprintf(b, "%s{%s=\"%s\"} %d\n", metric, label, k, v)
	}
}

func sortedKeys(m map[string]*HistogramSnapshot) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}






