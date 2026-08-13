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
	ErrContextWindow            // 400: 上下文超限（LiteLLM ContextWindowExceededError）
	ErrContentPolicy            // 400/422: 内容策略拒绝（ContentPolicyViolationError）
	ErrPermission               // 403: 权限不足（PermissionDeniedError）
	ErrModelNotFound            // 404: 模型不存在（NotFoundError）
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
	case ErrContextWindow:
		return "context_window_exceeded"
	case ErrContentPolicy:
		return "content_policy_violation"
	case ErrPermission:
		return "permission_denied"
	case ErrModelNotFound:
		return "model_not_found"
	default:
		return "unknown"
	}
}

// Code returns a stable, catalog-lookupable error code (2026-08-13).
// 前端/客户端按 code 查表（gateway/error_catalog.yaml）, 不做文本匹配。
func (k ErrorKind) Code() string {
	switch k {
	case ErrAuth:
		return "AUTH_FAILED"
	case ErrRateLimit:
		return "RATE_LIMITED"
	case ErrTimeout:
		return "UPSTREAM_TIMEOUT"
	case ErrServerError:
		return "UPSTREAM_5XX"
	case ErrBadRequest:
		return "BAD_REQUEST"
	case ErrNetwork:
		return "NETWORK_ERROR"
	case ErrCircuitOpen:
		return "CIRCUIT_OPEN"
	case ErrNoProviders:
		return "NO_PROVIDER"
	case ErrProviderNotFound:
		return "PROVIDER_NOT_FOUND"
	case ErrContextWindow:
		return "CONTEXT_WINDOW_EXCEEDED"
	case ErrContentPolicy:
		return "CONTENT_POLICY_VIOLATION"
	case ErrPermission:
		return "PERMISSION_DENIED"
	case ErrModelNotFound:
		return "MODEL_NOT_FOUND"
	default:
		return "UNKNOWN_ERROR"
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
	case ErrContextWindow:
		return http.StatusRequestEntityTooLarge
	case ErrContentPolicy:
		return http.StatusUnprocessableEntity
	case ErrPermission:
		return http.StatusForbidden
	case ErrModelNotFound:
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

// Code exposes the stable error code for catalog lookup.
func (e *GatewayError) Code() string { return e.Kind.Code() }

// ClassifyError inspects an HTTP response or Go error to determine its kind.
// It is the single source of truth for error taxonomy.
func ClassifyError(provider string, err error, statusCode int) *GatewayError {
	gw := &GatewayError{Provider: provider, Err: err}
	// 已分类错误直接复用（parseError 等上游已带状态码+消息）
	var existing *GatewayError
	if errors.As(err, &existing) {
		return existing
	}

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

// ClassifyErrorWithMessage 按 状态码 + 上游错误消息 分类（2026-08-13）。
// 消息模式对齐 LiteLLM 分类: context window / content policy / model not found。
func ClassifyErrorWithMessage(provider string, status int,
	message string) *GatewayError {
	gw := &GatewayError{Provider: provider, Kind: ErrUnknown,
		Message: message}
	low := strings.ToLower(message)
	switch {
	case status == http.StatusUnauthorized:
		gw.Kind, gw.Message = ErrAuth, "authentication failed — check your API key"
	case status == http.StatusForbidden:
		gw.Kind, gw.Message = ErrPermission, "permission denied (403)"
	case status == http.StatusTooManyRequests:
		gw.Kind, gw.Message = ErrRateLimit,
			"upstream rate limit exceeded — retry after a delay"
	case status == http.StatusNotFound:
		gw.Kind, gw.Message = ErrModelNotFound, "model or endpoint not found (404)"
	case status == http.StatusRequestEntityTooLarge:
		gw.Kind, gw.Message = ErrContextWindow, "request too large (413)"
	case status >= 500:
		gw.Kind, gw.Message = ErrServerError,
			fmt.Sprintf("upstream server error (HTTP %d)", status)
	case status >= 400:
		gw.Kind, gw.Message = ErrBadRequest,
			fmt.Sprintf("bad request (HTTP %d)", status)
	}
	// 消息模式精化（400 家族内细分, LiteLLM 同款）
	if status >= 400 && status < 500 {
		switch {
		case containsAny(low, "context", "maximum context", "token limit",
			"input is too long", "length"):
			gw.Kind, gw.Message = ErrContextWindow,
				"context window exceeded — 缩短输入或摘要"
		case containsAny(low, "policy", "content.filtered", "sensitive",
			"moderation", "safety"):
			gw.Kind, gw.Message = ErrContentPolicy,
				"content policy violation (upstream)"
		case containsAny(low, "model", "not found", "does not exist"):
			gw.Kind, gw.Message = ErrModelNotFound, "model not found"
		}
	}
	return gw
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Connection / DNS / timeout / TLS
	for _, pattern := range []string{
		"connection refused", "no such host", "i/o timeout",
		"tls", "dial tcp", "connectex", "eof", "broken pipe",
		"connection reset", "use of closed network",
	} {
		if strings.Contains(msg, pattern) { return true }
	}
	var netErr *net.OpError
	return errors.As(err, &netErr)
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


