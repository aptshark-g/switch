package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
)

// TraceContext carries W3C trace context across services.
// Format: https://www.w3.org/TR/trace-context/
type TraceContext struct {
	TraceID    string
	SpanID     string
	ParentSpan string
	Sampled    bool
}

// NewTraceContext generates a new trace context with random IDs.
func NewTraceContext() *TraceContext {
	return &TraceContext{
		TraceID: randomHex(32),
		SpanID:  randomHex(16),
		Sampled: true,
	}
}

// NewChildSpan creates a child span from the current context.
func (tc *TraceContext) NewChildSpan() *TraceContext {
	return &TraceContext{
		TraceID:    tc.TraceID,
		SpanID:     randomHex(16),
		ParentSpan: tc.SpanID,
		Sampled:    tc.Sampled,
	}
}

// Traceparent returns the W3C traceparent header value.
// Format: 00-{trace_id}-{span_id}-{flags}
func (tc *TraceContext) Traceparent() string {
	flags := "00"
	if tc.Sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", tc.TraceID, tc.SpanID, flags)
}

// ExtractTraceContext parses a traceparent header if present, otherwise
// generates a new trace context.
func ExtractTraceContext(r *http.Request) *TraceContext {
	h := r.Header.Get("traceparent")
	if h == "" {
		return NewTraceContext()
	}
	tc := &TraceContext{Sampled: true}
	_, err := fmt.Sscanf(h, "00-%32s-%16s-%2s", &tc.TraceID, &tc.SpanID, &h)
	if err != nil || len(tc.TraceID) != 32 || len(tc.SpanID) != 16 {
		return NewTraceContext()
	}
	tc.Sampled = h != "00"
	return tc
}

// InjectTraceHeaders adds trace headers to an outgoing HTTP request.
func InjectTraceHeaders(tc *TraceContext, req *http.Request) {
	if tc == nil {
		return
	}
	req.Header.Set("traceparent", tc.Traceparent())
	if tc.ParentSpan != "" {
		req.Header.Set("tracestate", fmt.Sprintf("dm=%s", tc.ParentSpan))
	}
}

type traceCtxKey struct{}

// WithTraceContext stores the trace context in the request context.
func WithTraceContext(ctx context.Context, tc *TraceContext) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, tc)
}

// GetTraceContext retrieves the trace context from the request context.
func GetTraceContext(ctx context.Context) *TraceContext {
	if tc, ok := ctx.Value(traceCtxKey{}).(*TraceContext); ok {
		return tc
	}
	return nil
}

func randomHex(n int) string {
	b := make([]byte, n/2)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// TracingMiddleware extracts or creates trace context for every request
// and stores it in the context. Downstream handlers and providers can
// retrieve it via GetTraceContext.
func TracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc := ExtractTraceContext(r)
		ctx := WithTraceContext(r.Context(), tc)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
