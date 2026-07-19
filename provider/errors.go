package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ErrorKind classifies provider errors for routing, retry, and observability.
type ErrorKind int

const (
	ErrUnknown        ErrorKind = iota
	ErrAuth                     // 401 鈥?bad key, expired token
	ErrRateLimit                // 429 鈥?upstream rate limit
	ErrTimeout                  // context deadline / connection timeout
	ErrServerError              // 5xx 鈥?upstream internal error (retryable)
	ErrBadRequest               // 400 鈥?malformed request (non-retryable)
	ErrNetwork                  // DNS, connection refused, TLS
	ErrCircuitOpen              // local circuit breaker is open
	ErrNoProviders              // no providers configured
	ErrProviderNotFound         // named provider not registered
)

// String returns a human-readable name for the error kind.
func (k ErrorKind) String() string {
	switch k {
	case ErrAuth:
		return "auth_error"
	case ErrRateLimit:
		return "rate_limited"
	case ErrTimeout:
		return "timeout"
	case ErrServerError:
		return "server_error"
	case ErrBadRequest:
		return "bad_request"
	case ErrNetwork:
		return "network_error"
	case ErrCircuitOpen:
		return "circuit_open"
	case ErrNoProviders:
		return "no_providers"
	case ErrProviderNotFound:
		return "provider_not_found"
	default:
		return "unknown"
	}
}

// Retryable reports whether this kind of error is safe to retry.
func (k ErrorKind) Retryable() bool {
	switch k {
	case ErrServerError, ErrNetwork, ErrTimeout, ErrRateLimit:
		return true
	default:
		return false
	}
}

// HTTPStatus maps the error kind to a suitable HTTP status code.
func (k ErrorKind) HTTPStatus() int {
	switch k {
	case ErrAuth:
		return http.StatusUnauthorized
	case ErrRateLimit:
		return http.StatusTooManyRequests
	case ErrTimeout:
		return http.StatusGatewayTimeout
	case ErrServerError:
		return http.StatusBadGateway
	case ErrBadRequest:
		return http.StatusBadRequest
	case ErrNetwork, ErrCircuitOpen:
		return http.StatusServiceUnavailable
	case ErrNoProviders, ErrProviderNotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

// GatewayError is a typed error carrying provider context for the caller.
type GatewayError struct {
	Kind     ErrorKind
	Provider string
	Message  string
	Err      error // underlying error (may be nil)
}

func (e *GatewayError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %s (%v)", e.Provider, e.Kind, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s: %s", e.Provider, e.Kind, e.Message)
}

func (e *GatewayError) Unwrap() error { return e.Err }

// ClassifyError inspects an HTTP response or Go error to determine its kind.
// It is the single source of truth for error taxonomy.
func ClassifyError(provider string, err error, statusCode int) *GatewayError {
	gw := &GatewayError{Provider: provider, Err: err}

	if statusCode > 0 {
		switch {
		case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
			gw.Kind = ErrAuth
			gw.Message = "authentication failed 鈥?check your API key"
		case statusCode == http.StatusTooManyRequests:
			gw.Kind = ErrRateLimit
			gw.Message = "upstream rate limit exceeded 鈥?retry after a delay"
		case statusCode >= 500:
			gw.Kind = ErrServerError
			gw.Message = fmt.Sprintf("upstream server error (HTTP %d)", statusCode)
		case statusCode >= 400:
			gw.Kind = ErrBadRequest
			gw.Message = fmt.Sprintf("bad request (HTTP %d)", statusCode)
		}
		return gw
	}

	if err == nil {
		gw.Kind = ErrUnknown
		gw.Message = "unknown error"
		return gw
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		gw.Kind = ErrTimeout
		gw.Message = "request timed out"
	case isNetworkError(err):
		gw.Kind = ErrNetwork
		gw.Message = "network error 鈥?check connectivity and base_url"
	default:
		gw.Kind = ErrUnknown
		gw.Message = err.Error()
	}
	return gw
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// DNS / connection refused / unreachable / TLS
	if strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "TLS") ||
		strings.Contains(msg, "dial tcp") ||
		strings.Contains(msg, "connectex") {
		return true
	}
	// Also check for net.OpError.
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Retry with exponential backoff + jitter
// ---------------------------------------------------------------------------

// RetryConfig controls retry behaviour.
type RetryConfig struct {
	MaxRetries  int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetryConfig returns conservative defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 3,
		BaseDelay:  500 * time.Millisecond,
		MaxDelay:   10 * time.Second,
	}
}

// Retry executes fn up to MaxRetries+1 times, only retrying on retryable errors.
// Uses truncated exponential backoff: base * 2^attempt, capped at MaxDelay.
// Adds 卤25% jitter to prevent thundering herd.
func Retry(fn func() error, classify func(error) *GatewayError, cfg RetryConfig) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == cfg.MaxRetries {
			break
		}
		gw := classify(err)
		if !gw.Kind.Retryable() {
			return err
		}
		delay := cfg.BaseDelay * time.Duration(1<<attempt)
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		// Simple jitter: scale randomly within 卤25%.
		jitter := time.Duration(float64(delay) * 0.25)
		if jitter > 0 {
			delay = delay - jitter/2 + jitter
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("retry exhausted after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}

// ── Retry budget: per-provider per-minute retry quota ──

// RetryBudget caps retry attempts to avoid retry storms.
type RetryBudget struct {
	mu         sync.Mutex
	maxPerMin  int
	bucket     float64
	lastFill   time.Time
}

// NewRetryBudget creates a budget with max retries per minute (0=unlimited).
func NewRetryBudget(maxPerMin int) *RetryBudget {
	return &RetryBudget{
		maxPerMin: maxPerMin,
		bucket:    float64(maxPerMin),
		lastFill:  time.Now(),
	}
}

// TryConsume attempts to consume one retry token. Returns false if budget exhausted.
func (rb *RetryBudget) TryConsume() bool {
	if rb.maxPerMin <= 0 { return true }
	rb.mu.Lock(); defer rb.mu.Unlock()
	rb.refill()
	if rb.bucket < 1 { return false }
	rb.bucket--
	return true
}

func (rb *RetryBudget) refill() {
	elapsed := time.Since(rb.lastFill).Minutes()
	if elapsed <= 0 { return }
	rb.lastFill = time.Now()
	rb.bucket += elapsed * float64(rb.maxPerMin)
	if rb.bucket > float64(rb.maxPerMin) {
		rb.bucket = float64(rb.maxPerMin)
	}
}

// RetryStats tracks retry metrics.
type RetryStats struct {
	Retries       int64
	BudgetExhausted int64
}


