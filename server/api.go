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
	"github.com/aptshark/gateway/prefix"
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
	mux         *http.ServeMux
	manager     *provider.Manager
	addr        string
	limiter     *RateLimiter
	metrics     *observability.Registry
	logger      *observability.StructuredLogger
	cache       *cache.Cache
	watcher     *config.Watcher
	authCfg     config.AuthConfig
	store       *persistence.Store
	shedder     *LoadShedder
	routingPool map[string]bool
	poolMutex   sync.RWMutex
	// 智能路由规则（2026-08-21）: provider.yaml routing.rules, watcher
	// 热更新时由 main 调 SetRoutingRules 刷新（意图/复杂度 → provider）。
	routingRules []config.RoutingRule
	routingMu    sync.RWMutex
	// 健康缓存（2026-08-13）: 后台 Prober 每 30s 并行探测写入;
	// /v1/health 读缓存即时返回, ?live=1 才实时探测。
	healthCache map[string]*provider.HealthStatus
	healthMu    sync.RWMutex
	// 计费跟踪（2026-08-13 接线）: CostTracker 此前孤儿 — 生产从不调用。
	// 成功请求按 provider pricing 计费, /v1/usage 聚合展示。
	costs *provider.CostTracker
	// 多级限流（2026-08-21 接线）: per-key/per-model RPM-TPM。
	// MultiRateLimiter 此前孤儿; provider 层由 manager 现有限流负责。
	keyLimiter *provider.MultiRateLimiter
	// 请求合并（2026-08-21 接线）: 同 cacheKey 并发只打一次上游。
	// cache.Coalescer 此前孤儿 — README 宣称「请求合并」但从未接入。
	coalescer *cache.Coalescer
	prefixProfiler *prefix.Profiler
	// SLO burn-rate 告警（2026-08-21 接线）: 此前孤儿, 现由
	// MetricsMiddleware 喂成功/失败, 超阈值触发 onAlert。
	slo *observability.SLOMonitor
	// B-2 有界负载亲和路由（2026-08-22）: 池内加权回落的"加权随机"
	// 升级为 一致哈希亲和(FP+tenant) + 过载溢出。DM_GATEWAY_AFFINITY=1 启用。
	affinity *prefix.AffinityRouter
	affinitySig string // 环签名（eligible 集合变化才重建）
	affinityMu  sync.Mutex
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
		mux:         http.NewServeMux(),
		manager:     manager,
		addr:        addr,
		limiter:     limiter,
		metrics:     observability.NewRegistry(),
		logger:      observability.NewStructuredLogger(),
		cache:       cache.New(1000, 5*time.Minute),
		watcher:     watcher,
		authCfg:     authCfg,
		store:       store,
		shedder:     NewLoadShedder(5000),
		healthCache: make(map[string]*provider.HealthStatus),
		costs: provider.NewCostTracker(func() string {
			if p := os.Getenv("DM_GATEWAY_USAGE_LOG"); p != "" {
				return p
			}
			return "usage_log.jsonl"
		}()),
		keyLimiter: newKeyLimiterFromEnv(),
		coalescer:  cache.NewCoalescer(),
		prefixProfiler: prefix.NewProfiler(),
		slo: observability.NewSLOMonitor(observability.DefaultSLOConfig()),
		affinity: newAffinityFromEnv(),
	}
	s.slo.OnAlert(func(a observability.SLOAlert) {
		log.Printf("SLO ALERT [%s] burn_rate=%.2f error_rate=%.3f budget_remaining=%.3f",
			a.Level, a.BurnRate, a.ErrorRate, a.BudgetRemaining)
	})
	s.routes()
	return s
}

// newAffinityFromEnv B-2 亲和路由（默认关闭, DM_GATEWAY_AFFINITY=1 启用）。
func newAffinityFromEnv() *prefix.AffinityRouter {
	if os.Getenv("DM_GATEWAY_AFFINITY") != "1" {
		return nil
	}
	overload := 1.25
	if v := os.Getenv("DM_GATEWAY_AFFINITY_OVERLOAD"); v != "" {
		var f float64
		if _, err := fmt.Sscanf(v, "%f", &f); err == nil && f > 0 {
			overload = f
		}
	}
	ttl := 60 * time.Second
	if v := os.Getenv("DM_GATEWAY_AFFINITY_OVERFLOW_TTL"); v != "" {
		var sec int
		if _, err := fmt.Sscanf(v, "%d", &sec); err == nil && sec > 0 {
			ttl = time.Duration(sec) * time.Second
		}
	}
	return prefix.NewAffinityRouter(nil, overload, ttl)
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
	s.mux.HandleFunc("/v1/admin/ratelimit", s.handleAdminRateLimit)
	s.mux.HandleFunc("/v1/prefix/stats", s.handlePrefixStats)
	s.mux.HandleFunc("/v1/admin/providers/", s.handleAdminProviders)
}

func (s *Server) BuildHandler() http.Handler {
	return LoadSheddingMiddleware(s.shedder)(
		observability.TracingMiddleware(
			observability.MetricsMiddleware(s.metrics, s.logger, s.slo)(
				CORSMiddleware(
					panicRecoveryMiddleware(
						// 2026-08-20: 移除冗余 LoggingMiddleware —— 每请求日志
						// 已由 MetricsMiddleware 异步结构化日志承担（同步
						// log.Printf 全局锁压制吞吐, 实测移除后 ~2.8K→9.8K）
						AuthMiddleware(s.authCfg)(
							RateLimitMiddleware(s.limiter, DefaultKeyFunc)(s.mux),
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
		"cache_entries":   s.cache.Size(), "semaphore_waiting": s.manager.TotalSemaphoreWaiting(), "load_shed_total": s.shedder.Shed(), "load_shed_inflight": s.shedder.InFlight(), "active_connections": s.metrics.Snapshot().ActiveConnections,
		"providers": s.manager.List(),
	})
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s.metrics.PrometheusText())
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var out map[string]any
	if raw, err := json.Marshal(s.metrics.Snapshot()); err == nil {
		_ = json.Unmarshal(raw, &out)
	}
	if out == nil {
		out = map[string]any{}
	}
	// SLO burn-rate（2026-08-21 接线）
	if s.slo != nil {
		snap := s.slo.Snapshot()
		out["slo"] = map[string]any{
			"burn_rate":        snap.BurnRate,
			"error_rate":       snap.ErrorRate,
			"short_rate":       snap.ShortRate,
			"long_rate":        snap.LongRate,
			"budget_remaining": snap.BudgetRemaining,
			"alert_level":      snap.Level.String(),
		}
	}
	if s.affinity != nil {
		out["affinity"] = s.affinity.Snapshot()
	}
	writeJSON(w, http.StatusOK, out)
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
	if len(req.Messages) == 0 {
		return fmt.Errorf("messages array must not be empty")
	}
	for i, msg := range req.Messages {
		if msg.Role == "" {
			return fmt.Errorf("messages[%d]: role is required", i)
		}
	}
	if req.Temperature < 0 || req.Temperature > 2.0 {
		return fmt.Errorf("temperature must be between 0 and 2 (got %.2f)", req.Temperature)
	}
	if req.MaxTokens < 0 {
		return fmt.Errorf("max_tokens must be non-negative")
	}
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
	if req.Stream {
		s.metrics.StreamRequests.Inc()
	} else {
		s.metrics.NonStreamReqs.Inc()
	}
	// 2026-08-21: 智能路由 — 显式 ?provider= 优先; 否则规则层
	// （X-Intent/X-Complexity → routing.rules）→ 池内加权随机。
	// 规则可覆盖 model/thinking（如 casual → 小模型关思考）。
	providerName := r.URL.Query().Get("provider")
	if providerName == "" {
		decision := s.routeRequest(r, &req)
		if decision.affinityPicked {
			// B-2: 亲和选中后记负载, 请求完成时释放（含流式/合并等待路径）。
			defer func() { s.affinity.Dec(providerName) }()
		}
		providerName = decision.provider
		if providerName == "" {
			providerName = s.getRoutingProvider()
		}
		if decision.model != "" {
			req.Model = decision.model
		}
		if decision.thinking != nil {
			req.Thinking = decision.thinking
		}
		s.logRouting(r, decision, providerName)
		if providerName == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "no routing provider available — configure keys in Gateway page",
				"code":  "NO_PROVIDER"})
			return
		}
	}
	// 多级限流（2026-08-21 接线）: per-key/per-model RPM-TPM。
	if ok, reason := s.checkKeyLimits(r, req.Model, provider.TokenEstimate(&req)); !ok {
		s.metrics.RateLimitHits.Inc()
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error":    "rate limit exceeded",
			"code":     "RATE_LIMITED",
			"reason":   reason,
			"provider": providerName,
		})
		return
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
		// 2026-08-19: 缓存隔离 — 键纳入 provider|model|api_key,
		// 避免跨租户/跨模型/跨 Provider 误命中（此前 model 传空串）。
		// X-Context-Hash: 后端（DialogMesh 上下文编译器）可传编译上下文
		// 的稳定哈希, 相同编译上下文即使原始消息格式有差异也能命中。
		apiKey := "anon"
		if h := r.Header.Get("Authorization"); len(h) > 7 {
			apiKey = h[7:]
		}
		cacheKey := cache.HashKey(requestCacheKey(req),
			providerName+"|"+req.Model+"|"+apiKey+"|"+r.Header.Get("X-Context-Hash"))
		if cached, ok := s.cache.Get(cacheKey); ok {
			s.metrics.CacheHits.Inc()
			writeJSON(w, http.StatusOK, cached)
			return
		}
		s.metrics.CacheMisses.Inc()
		// 请求合并（2026-08-21）: 同 cacheKey 并发请求只执行一次上游,
		// 其余等待共享结果（coalescer 超时只约束等待者, 执行者跑完）。
		v, cErr := s.coalescer.Do(cacheKey, func() (any, error) {
			resp, err := s.manager.Generate(r.Context(), providerName, &req)
			if err != nil {
				return nil, err
			}
			if resp.Usage != nil {
				s.metrics.RecordTokensFull(providerName,
					resp.Usage.PromptTokens, resp.Usage.CompletionTokens,
					resp.Usage.Cached(), resp.Usage.CachedMiss())
				s.recordUsage(providerName, req.Model, resp.Usage, r)
				// B-1 前缀命中观测（仅执行者记一次; 等待者共享结果不重复）。
				s.prefixProfiler.Observe(prefix.FingerprintRequest(&req),
					resp.Usage.Cached(), resp.Usage.CachedMiss())
			}
			s.cache.Set(cacheKey, resp)
			return resp, nil
		}, 120*time.Second)
		if cErr != nil {
			err := cErr
			gw := provider.ClassifyError(providerName, err, 0)
			log.Printf("gateway: generate error [%s] %s: %v", providerName, gw.Kind, err)
			if gw.Kind.Retryable() {
				if s.gracefulDegradation(w, r, &req, providerName) {
					return
				}
			}
			writeJSON(w, gw.Kind.HTTPStatus(), map[string]string{
				"error": gw.Message, "kind": gw.Kind.String(),
				"code": gw.Code(), "provider": providerName,
			})
			return
		}
		writeJSON(w, http.StatusOK, v)
		return
	}
	if req.Stream {
		s.handleStream(w, r, providerName, &req)
		return
	}
	// 不可达：非流式路径已在上面 return。
	writeJSON(w, http.StatusInternalServerError, map[string]string{
		"error": "unreachable", "code": "UNKNOWN_ERROR"})
}

func (s *Server) gracefulDegradation(w http.ResponseWriter, r *http.Request, req *provider.GenerateRequest, exclude string) bool {
	for _, name := range s.getRoutingCandidates() {
		if name == exclude {
			continue
		}
		resp, err := s.manager.Generate(r.Context(), name, req)
		if err == nil {
			if resp.Usage != nil {
				s.metrics.RecordTokensFull(name,
					resp.Usage.PromptTokens, resp.Usage.CompletionTokens,
					resp.Usage.Cached(), resp.Usage.CachedMiss())
				s.recordUsage(name, req.Model, resp.Usage, r)
			}
			log.Printf("gateway: degraded to %s", name)
			writeJSON(w, http.StatusOK, resp)
			return true
		}
		if !provider.ClassifyError(name, err, 0).Kind.Retryable() {
			continue
		}
	}
	return false
}

func (s *Server) getRoutingCandidates() []string {
	// 2026-08-21: 确定性偏好序（熔断靠后, priority desc → score desc）。
	return s.routingCandidates()
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request, name string, req *provider.GenerateRequest) {
	p, err := s.manager.Get(name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
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
			"error": fmt.Sprintf("SSE init failed: %v", err), "code": "UNKNOWN_ERROR"})
		return
	}
	ch, err := sp.GenerateStream(r.Context(), req)
	if err != nil {
		gw := provider.ClassifyError(name, err, 0)
		_ = sse.Send("error", map[string]string{"message": gw.Message, "kind": gw.Kind.String()})
		return
	}
	var pt, ct, cached, miss int
	for chunk := range ch {
		if chunk.Error != nil {
			_ = sse.Send("error", map[string]string{"message": chunk.Error.Error()})
			return
		}
		if chunk.Usage != nil {
			pt = chunk.Usage.PromptTokens
			ct = chunk.Usage.CompletionTokens
			cached = chunk.Usage.Cached()
			miss = chunk.Usage.CachedMiss()
		}
		_ = sse.Send("", map[string]any{"id": chunk.ID, "model": chunk.Model, "choices": []map[string]any{
			{"index": 0, "delta": chunk.Delta, "finish_reason": chunk.FinishReason},
		}})
	}
	if pt > 0 || ct > 0 {
		s.metrics.RecordTokensFull(name, pt, ct, cached, miss)
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
