package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aptshark/gateway/cache"
	"github.com/aptshark/gateway/config"
	"github.com/aptshark/gateway/observability"
	"github.com/aptshark/gateway/persistence"
	"github.com/aptshark/gateway/provider"
	"github.com/aptshark/gateway/stream"
)

// messagesCacheKey serializes the full message list (roles + contents,
// including system prompts) so the response cache distinguishes prompts
// that share the same last user message but differ in system context.
func messagesCacheKey(msgs []provider.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Role)
		b.WriteString("::")
		b.WriteString(m.Content)
		b.WriteString("\n")
	}
	return b.String()
}

// requestCacheKey = messages + generation parameters. Without the gen
// params, a cached response for max_tokens=16 (truncated, empty content)
// would be served to a max_tokens=128 request with the same messages —
// the "LLM returns empty" bug observed 2026-08-11.
func requestCacheKey(req provider.GenerateRequest) string {
	var b strings.Builder
	b.WriteString(messagesCacheKey(req.Messages))
	b.WriteString("::model=")
	b.WriteString(req.Model)
	b.WriteString("::max_tokens=")
	b.WriteString(fmt.Sprintf("%d", req.MaxTokens))
	b.WriteString("::temperature=")
	b.WriteString(fmt.Sprintf("%g", req.Temperature))
	b.WriteString("::top_p=")
	b.WriteString(fmt.Sprintf("%g", req.TopP))
	b.WriteString("::stream=")
	b.WriteString(fmt.Sprintf("%v", req.Stream))
	b.WriteString("::thinking=")
	b.WriteString(fmt.Sprintf("%v", req.Thinking))
	return b.String()
}

type Server struct {
	mux      *http.ServeMux
	manager  *provider.Manager
	addr     string
	limiter  *RateLimiter
	metrics  *observability.Registry
	logger   *observability.StructuredLogger
	cache    *cache.Cache
	watcher  *config.Watcher
	authCfg  config.AuthConfig
	store    *persistence.Store
	shedder     *LoadShedder
	routingPool map[string]bool
	poolMutex   sync.RWMutex
	// 健康缓存（2026-08-13）: 后台 Prober 每 30s 并行探测写入;
	// /v1/health 读缓存即时返回, ?live=1 才实时探测。
	healthCache map[string]*provider.HealthStatus
	healthMu    sync.RWMutex
	// 计费跟踪（2026-08-13 接线）: CostTracker 此前孤儿 — 生产从不调用。
	// 成功请求按 provider pricing 计费, /v1/usage 聚合展示。
	costs *provider.CostTracker
}

func NewWithWatcher(manager *provider.Manager, addr string, watcher *config.Watcher, authCfg config.AuthConfig, store *persistence.Store) *Server {
	// 限流可配置（2026-08-13）: 默认关闭（70+ req/s 指标不被卡死）;
	// DM_GATEWAY_RATE_LIMIT=60/120 开启（rps/burst）; 0/空 = 关闭。
	var limiter *RateLimiter
	if v := os.Getenv("DM_GATEWAY_RATE_LIMIT"); v != "" && v != "0" {
		rps, burst := 60, 120
		if _, err := fmt.Sscanf(v, "%d/%d", &rps, &burst); err != nil ||
			rps <= 0 {
			rps, burst = 60, 120
		}
		limiter = NewRateLimiter(float64(rps), float64(burst))
	}
	s := &Server{
		mux:      http.NewServeMux(),
		manager:  manager,
		addr:     addr,
		limiter:  limiter,
		metrics:  observability.NewRegistry(),
		logger:   observability.NewStructuredLogger(),
		cache:    cache.New(1000, 5*time.Minute),
		watcher:  watcher,
		authCfg:  authCfg,
		store:    store,
		shedder:  NewLoadShedder(5000),
		healthCache: make(map[string]*provider.HealthStatus),
		costs: provider.NewCostTracker(func() string {
			if p := os.Getenv("DM_GATEWAY_USAGE_LOG"); p != "" {
				return p
			}
			return "usage_log.jsonl"
		}()),
	}
	s.routes()
	return s
}

// recordUsage 计费接线: usage → CostTracker（按 auth key 聚合）。
func (s *Server) recordUsage(providerName, model string,
	usage *provider.TokenUsage, r *http.Request) {
	if usage == nil || s.costs == nil {
		return
	}
	var pricing *provider.TokenPricing
	if p, err := s.manager.Get(providerName); err == nil {
		pricing = p.Config().Pricing
	}
	key := "dm-client"
	if h := r.Header.Get("Authorization"); len(h) > 7 {
		key = h[7:]
	}
	s.costs.Record(key, providerName, model,
		usage.PromptTokens, usage.CompletionTokens, pricing)
}

// UpdateHealth stores a probe result for a provider (called by Prober).
func (s *Server) UpdateHealth(name string, hs *provider.HealthStatus) {
	if hs == nil {
		return
	}
	s.healthMu.Lock()
	s.healthCache[name] = hs
	s.healthMu.Unlock()
}

// HealthSnapshot returns cached health for all providers (instant).
func (s *Server) HealthSnapshot() map[string]*provider.HealthStatus {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	out := make(map[string]*provider.HealthStatus, len(s.healthCache))
	for k, v := range s.healthCache {
		out[k] = v
	}
	return out
}

func (s *Server) routes() {
	s.mux.HandleFunc("/v1/health", s.handleHealth)
	s.mux.HandleFunc("/v1/health/detail", s.handleHealthDetail)
	s.mux.HandleFunc("/v1/metrics", s.handlePrometheusMetrics)
	s.mux.HandleFunc("/v1/stats", s.handleStats)
	s.mux.HandleFunc("/v1/chat/completions", s.handleChatCompletions)
	s.mux.HandleFunc("/v1/providers", s.handleListProviders)
	s.mux.HandleFunc("/v1/usage", s.handleUsage)
	s.mux.HandleFunc("/v1/error-catalog", s.handleErrorCatalog)
	s.mux.HandleFunc("/admin", s.handleAdminPage)
	s.mux.HandleFunc("/v1/admin/reload", s.handleAdminReload)
	s.mux.HandleFunc("/v1/admin/providers", s.handleAdminProviders)
	s.mux.HandleFunc("/v1/diagnostics", s.handleDiagnostics)
	s.mux.HandleFunc("/v1/admin/routing", s.handleRoutingPool)
	s.mux.HandleFunc("/v1/admin/providers/", s.handleAdminProviders)
}

func (s *Server) BuildHandler() http.Handler {
	return LoadSheddingMiddleware(s.shedder)(
		observability.TracingMiddleware(
			observability.MetricsMiddleware(s.metrics, s.logger)(
				CORSMiddleware(
					panicRecoveryMiddleware(
						LoggingMiddleware(
							AuthMiddleware(s.authCfg)(
								RateLimitMiddleware(s.limiter, DefaultKeyFunc)(s.mux),
							),
						),
					),
				),
			),
		),
	)
}

func (s *Server) Start() error {
	return NewGracefulServer(s.addr, s.BuildHandler(), s.onShutdown).ListenAndServe()
}

func (s *Server) StartTLS(certFile, keyFile string) error {
	return NewGracefulServer(s.addr, s.BuildHandler(), s.onShutdown).ListenAndServeTLS(certFile, keyFile)
}

func (s *Server) onShutdown() {
	if s.store != nil {
		if err := s.store.Save(); err != nil {
			log.Printf("gateway: persist on shutdown: %v", err)
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	providers := s.manager.List()
	// 2026-08-13: 默认读后台 Prober 缓存（30s 新鲜度, 即时返回）;
	// ?live=1 才实时并行探测（调试/冷启动首查用）。
	if r.URL.Query().Get("live") != "1" {
		snap := s.HealthSnapshot()
		healthyCount := 0
		for _, p := range providers {
			if hs, ok := snap[p.Name]; ok && hs.Healthy {
				healthyCount++
			}
		}
		status := "ok"
		code := http.StatusOK
		if healthyCount == 0 && len(providers) > 0 {
			status = "degraded"
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{
			"status": status, "providers_healthy": healthyCount,
			"providers_total": len(providers), "cached": true,
		})
		return
	}
	healthyCount := 0
	// 并行探测（2026-08-12）: 此前串行逐个 Health()（每个 3s 超时 +
	// 真实网络往返）→ 9 provider 时 /v1/health 被拉高到 ~100ms+
	// （无 key provider 也发请求, 阻塞叠加）。并行后耗时 ≈ 最慢单点。
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, p := range providers {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			pv, err := s.manager.Get(name)
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			hs := pv.Health(ctx)
			if hs.Healthy {
				mu.Lock()
				healthyCount++
				mu.Unlock()
			}
		}(p.Name)
	}
	wg.Wait()
	if healthyCount == 0 && len(providers) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "degraded", "providers_healthy": 0, "providers_total": len(providers),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "providers_healthy": healthyCount, "providers_total": len(providers),
	})
}

func (s *Server) handleHealthDetail(w http.ResponseWriter, r *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "uptime_seconds": s.metrics.Uptime(),
		"go_version": runtime.Version(), "num_goroutines": runtime.NumGoroutine(),
		"memory_alloc_mb": float64(mem.Alloc) / 1024 / 1024,
		"memory_total_mb": float64(mem.Sys) / 1024 / 1024,
		"cache_entries": s.cache.Size(), "semaphore_waiting": s.manager.TotalSemaphoreWaiting(), "load_shed_total": s.shedder.Shed(), "load_shed_inflight": s.shedder.InFlight(), "active_connections": s.metrics.Snapshot().ActiveConnections,
		"providers": s.manager.List(),
	})
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s.metrics.PrometheusText())
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics.Snapshot())
}

func (s *Server) handleListProviders(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"providers": s.manager.List()})
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	stats := s.manager.UsageStats()
	out := map[string]any{
		"total_requests": stats.TotalRequests,
		"total_tokens":   stats.TotalTokens,
		"by_provider":    stats.ByProvider,
	}
	// 计费（2026-08-13 接线）: CostTracker 快照并入 /v1/usage
	if s.costs != nil {
		cs := s.costs.Snapshot()
		out["cost"] = map[string]any{
			"total":        cs.Total,
			"by_key":       cs.ByKey,
			"by_model":     cs.ByModel,
			"tenant_count": cs.TenantCount,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func validateRequest(req *provider.GenerateRequest) error {
	if len(req.Messages) == 0 { return fmt.Errorf("messages array must not be empty") }
	for i, msg := range req.Messages {
		if msg.Role == "" { return fmt.Errorf("messages[%d]: role is required", i) }
	}
	if req.Temperature < 0 || req.Temperature > 2.0 {
		return fmt.Errorf("temperature must be between 0 and 2 (got %.2f)", req.Temperature)
	}
	if req.MaxTokens < 0 { return fmt.Errorf("max_tokens must be non-negative") }
	return nil
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req provider.GenerateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid JSON body", "code": "BAD_REQUEST"})
		return
	}
	if err := validateRequest(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": err.Error(), "code": "BAD_REQUEST"})
		return
	}
	s.metrics.IncModel(req.Model)
	if req.Stream { s.metrics.StreamRequests.Inc() } else { s.metrics.NonStreamReqs.Inc() }
	providerName := r.URL.Query().Get("provider")
	if providerName == "" {
		providerName = s.getRoutingProvider()
		if providerName == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "no routing provider available — configure keys in Gateway page",
				"code":  "NO_PROVIDER"})
			return
		}
	}
	// per-key 日配额（2026-08-13）: DM_GATEWAY_KEY_QUOTA_DAILY（token/天,
	// 0=不限）。按 auth key 当日已用 token 判定, 超限 429 + BUDGET_EXCEEDED。
	if q := os.Getenv("DM_GATEWAY_KEY_QUOTA_DAILY"); q != "" &&
		s.costs != nil {
		var qn int64
		if n, _ := fmt.Sscanf(q, "%d", &qn); n == 1 && qn > 0 {
			key := "dm-client"
			if h := r.Header.Get("Authorization"); len(h) > 7 {
				key = h[7:]
			}
			if s.costs.DailyTokens(key) >= qn {
				writeJSON(w, http.StatusTooManyRequests, map[string]string{
					"error":    "daily token quota exceeded",
					"code":     "BUDGET_EXCEEDED",
					"provider": providerName})
				return
			}
		}
	}
	if !req.Stream {
		cacheKey := cache.HashKey(requestCacheKey(req), "")
		if cached, ok := s.cache.Get(cacheKey); ok {
			s.metrics.CacheHits.Inc()
			writeJSON(w, http.StatusOK, cached)
			return
		}
		s.metrics.CacheMisses.Inc()
	}
	if req.Stream {
		s.handleStream(w, r, providerName, &req)
		return
	}
	resp, err := s.manager.Generate(r.Context(), providerName, &req)
	if err != nil {
		gw := provider.ClassifyError(providerName, err, 0)
		log.Printf("gateway: generate error [%s] %s: %v", providerName, gw.Kind, err)
		if gw.Kind.Retryable() {
			if s.gracefulDegradation(w, r, &req, providerName) {
				return
			}
		}
		writeJSON(w, gw.Kind.HTTPStatus(), map[string]string{
			"error": gw.Message, "kind": gw.Kind.String(),
			"code":  gw.Code(), "provider": providerName,
		})
		return
	}
	if resp.Usage != nil {
		s.metrics.RecordTokens(providerName, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
		s.recordUsage(providerName, req.Model, resp.Usage, r)
	}
	cacheKey := cache.HashKey(requestCacheKey(req), "")
	s.cache.Set(cacheKey, resp)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) gracefulDegradation(w http.ResponseWriter, r *http.Request, req *provider.GenerateRequest, exclude string) bool {
	for _, name := range s.getRoutingCandidates() {
		if name == exclude { continue }
		resp, err := s.manager.Generate(r.Context(), name, req)
		if err == nil {
			if resp.Usage != nil {
				s.metrics.RecordTokens(name, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
				s.recordUsage(name, req.Model, resp.Usage, r)
			}
			log.Printf("gateway: degraded to %s", name)
			writeJSON(w, http.StatusOK, resp); return true
		}
		if !provider.ClassifyError(name, err, 0).Kind.Retryable() { continue }
	}
	return false
}

func (s *Server) getRoutingCandidates() []string {
	s.poolMutex.RLock(); defer s.poolMutex.RUnlock()
	seen := map[string]bool{}
	var res []string
	for name, in := range s.routingPool {
		if !in { continue }
		for _, p := range s.manager.List() {
			if p.Name == name && p.Active && p.KeyConfigured { seen[name]=true; res=append(res, name); break }
		}
	}
	for _, p := range s.manager.List() {
		if !seen[p.Name] && p.Active && p.KeyConfigured { res=append(res, p.Name) }
	}
	return res
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request, name string, req *provider.GenerateRequest) {
	p, err := s.manager.Get(name)
	if err != nil { writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()}); return }
	sp, ok := p.(provider.StreamProvider)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("%s does not support streaming", name),
			"code":  "BAD_REQUEST"})
		return
	}
	sse, err := stream.NewSSEWriter(w)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "SSE not supported", "code": "UNKNOWN_ERROR"})
		return
	}
	ch, err := sp.GenerateStream(r.Context(), req)
	if err != nil {
		gw := provider.ClassifyError(name, err, 0)
		_ = sse.Send("error", map[string]string{"message": gw.Message, "kind": gw.Kind.String()}); return
	}
	var pt, ct int
	for chunk := range ch {
		if chunk.Error != nil { _ = sse.Send("error", map[string]string{"message": chunk.Error.Error()}); return }
		if chunk.Usage != nil { pt = chunk.Usage.PromptTokens; ct = chunk.Usage.CompletionTokens }
		_ = sse.Send("", map[string]any{"id": chunk.ID, "model": chunk.Model, "choices": []map[string]any{
			{"index": 0, "delta": chunk.Delta, "finish_reason": chunk.FinishReason},
		}})
	}
	if pt > 0 || ct > 0 {
		s.metrics.RecordTokens(name, pt, ct)
		s.recordUsage(name, req.Model,
			&provider.TokenUsage{PromptTokens: pt, CompletionTokens: ct}, r)
	}
	sse.SendDone()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("gateway: json encode error: %v", err)
	}
}





