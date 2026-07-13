package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// ---------------------------------------------------------------------------
// Structured logger
// ---------------------------------------------------------------------------

// LogEntry is a structured JSON log line.
type LogEntry struct {
	Timestamp  string `json:"ts"`
	Level      string `json:"level"`
	RequestID  string `json:"request_id,omitempty"`
	Method     string `json:"method,omitempty"`
	Path       string `json:"path,omitempty"`
	Provider   string `json:"provider,omitempty"`
	Status     int    `json:"status,omitempty"`
	LatencyMs  float64 `json:"latency_ms,omitempty"`
	ClientIP   string `json:"client_ip,omitempty"`
	Msg        string `json:"msg"`
}

// StructuredLogger writes JSON log lines to the standard logger.
type StructuredLogger struct{}

// NewStructuredLogger creates a structured JSON logger.
func NewStructuredLogger() *StructuredLogger {
	log.SetFlags(0) // disable default timestamp; we write our own
	return &StructuredLogger{}
}

func (l *StructuredLogger) Info(entry LogEntry) {
	entry.Level = "info"
	entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.Marshal(entry)
	log.Println(string(data))
}

func (l *StructuredLogger) Error(entry LogEntry) {
	entry.Level = "error"
	entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.Marshal(entry)
	log.Println(string(data))
}

// ---------------------------------------------------------------------------
// Request ID
// ---------------------------------------------------------------------------

type ctxKey int
const requestIDKey ctxKey = iota

// NewRequestID generates a short unique request identifier.
func NewRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// GetRequestID extracts the request ID from the context.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// ---------------------------------------------------------------------------
// Metrics middleware
// ---------------------------------------------------------------------------

// MetricsMiddleware wraps an http.Handler to collect metrics and structured logs.
func MetricsMiddleware(reg *Registry, logger *StructuredLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := NewRequestID()
			ctx := context.WithValue(r.Context(), requestIDKey, reqID)
			r = r.WithContext(ctx)

			start := time.Now()
			reg.ActiveConnections.Inc()
			defer reg.ActiveConnections.Dec()

			// Wrap the ResponseWriter to capture status code.
			wr := &responseWrapper{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(wr, r)

			elapsed := time.Since(start).Seconds() * 1000 // ms
			provider := r.URL.Query().Get("provider")
			if provider == "" {
				provider = "default"
			}

			// Record metrics.
			reg.RequestsByProvider.get(provider).Inc()
			reg.RequestsByEndpoint.get(r.URL.Path).Inc()
			reg.LatencyByProvider.get(provider).Observe(elapsed)
			if wr.status >= 400 {
				reg.ErrorsByProvider.get(provider).Inc()
			}
			if wr.status == http.StatusTooManyRequests {
				reg.RateLimitHits.Inc()
			}

	// Record status code group.
	reg.RequestsByStatus.get(statusGroup(wr.status)).Inc()

	// Structured log.
	// Record status code group.
	reg.RequestsByStatus.get(statusGroup(wr.status)).Inc()
	entry := LogEntry{
				RequestID: reqID,
				Method:    r.Method,
				Path:      r.URL.Path,
				Provider:  provider,
				Status:    wr.status,
				LatencyMs: elapsed,
				ClientIP:  r.RemoteAddr,
			}
			if wr.status >= 500 {
				entry.Msg = "request error"
				logger.Error(entry)
			} else {
				entry.Msg = "request complete"
				logger.Info(entry)
			}
		})
	}
}

// responseWrapper captures the HTTP status code written by inner handlers.
type responseWrapper struct {
	http.ResponseWriter
	status int
}

func (w *responseWrapper) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func statusGroup(code int) string {
	switch {
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

// RecordTokens records token usage for a provider.
func (r *Registry) RecordTokens(provider string, prompt, completion int) {
	r.TokensPrompt.get(provider).Add(int64(prompt))
	r.TokensCompletion.get(provider).Add(int64(completion))
}


