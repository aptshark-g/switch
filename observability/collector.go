package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Structured logger
// ---------------------------------------------------------------------------

// LogEntry is a structured JSON log line.
type LogEntry struct {
	Timestamp string  `json:"ts"`
	Level     string  `json:"level"`
	RequestID string  `json:"request_id,omitempty"`
	Method    string  `json:"method,omitempty"`
	Path      string  `json:"path,omitempty"`
	Provider  string  `json:"provider,omitempty"`
	Status    int     `json:"status,omitempty"`
	LatencyMs float64 `json:"latency_ms,omitempty"`
	ClientIP  string  `json:"client_ip,omitempty"`
	Msg       string  `json:"msg"`
}

// StructuredLogger writes JSON log lines to the standard logger.
type StructuredLogger struct {
	// 2026-08-20: 异步批量写入（Info 非阻塞入队, 满则丢; 错误保持同步）。
	// 修复: 每请求 log.Println（全局锁）在高压下压制吞吐
	// —— 实测关日志 c=256 2780→9839 req/s（3.5x）。
	ch      chan string
	dropped atomic.Int64
}

// NewStructuredLogger creates a structured JSON logger.
func NewStructuredLogger() *StructuredLogger {
	log.SetFlags(0) // disable default timestamp; we write our own
	l := &StructuredLogger{ch: make(chan string, 4096)}
	go l.writer()
	return l
}

func (l *StructuredLogger) Info(entry LogEntry) {
	entry.Level = "info"
	entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.Marshal(entry)
	select {
	case l.ch <- string(data):
	default:
		l.dropped.Add(1)
	}
}

func (l *StructuredLogger) Error(entry LogEntry) {
	entry.Level = "error"
	entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.Marshal(entry)
	log.Println(string(data))
}

// writer 批量刷盘: 攒批 + 50ms tick, 只由一个 goroutine 碰 log 全局锁。
func (l *StructuredLogger) writer() {
	var buf strings.Builder
	flush := func() {
		if buf.Len() > 0 {
			log.Print(buf.String())
			buf.Reset()
		}
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case s := <-l.ch:
			buf.WriteString(s)
			buf.WriteByte('\n')
			if buf.Len() > 8192 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
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
				// 2026-08-20: DM_GATEWAY_REQUEST_LOG=0 关每请求 Info 日志
				// （对照压测定位吞吐上限; 错误日志始终保留）
				if os.Getenv("DM_GATEWAY_REQUEST_LOG") != "0" {
					logger.Info(entry)
				}
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

// Unwrap 让 http.NewResponseController 穿透包装层，恢复底层 Flusher
// （2026-08-17 FIX: 网关流式恒 500 "feature not supported" — 无 Unwrap 时
// ResponseController 找不到真实 Flusher, SSE 无法建立）。
func (w *responseWrapper) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
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

// RecordTokensFull records token usage + upstream context-cache hit/miss.
func (r *Registry) RecordTokensFull(provider string, prompt, completion, cached, miss int) {
	r.RecordTokens(provider, prompt, completion)
	if cached > 0 {
		r.PromptCacheHits.Inc()
	}
	if miss > 0 {
		r.PromptCacheMisses.Inc()
	}
}
