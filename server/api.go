package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/aptshark/gateway/cache"
	"github.com/aptshark/gateway/config"
	"github.com/aptshark/gateway/observability"
	"github.com/aptshark/gateway/persistence"
	"github.com/aptshark/gateway/provider"
	"github.com/aptshark/gateway/stream"
)

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
	shedder  *LoadShedder
}

func NewWithWatcher(manager *provider.Manager, addr string, watcher *config.Watcher, authCfg config.AuthConfig, store *persistence.Store) *Server {
	s := &Server{
		mux:      http.NewServeMux(),
		manager:  manager,
		addr:     addr,
		limiter:  NewRateLimiter(60, 120),
		metrics:  observability.NewRegistry(),
		logger:   observability.NewStructuredLogger(),
		cache:    cache.New(1000, 5*time.Minute),
		watcher:  watcher,
		authCfg:  authCfg,
		store:    store,
		shedder:  NewLoadShedder(5000),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/v1/health", s.handleHealth)
	s.mux.HandleFunc("/v1/health/detail", s.handleHealthDetail)
	s.mux.HandleFunc("/v1/metrics", s.handlePrometheusMetrics)
	s.mux.HandleFunc("/v1/stats", s.handleStats)
	s.mux.HandleFunc("/v1/chat/completions", s.handleChatCompletions)
	s.mux.HandleFunc("/v1/providers", s.handleListProviders)
	s.mux.HandleFunc("/v1/usage", s.handleUsage)
	s.mux.HandleFunc("/v1/admin/reload", s.handleAdminReload)
	s.mux.HandleFunc("/v1/admin/providers", s.handleAdminProviders)
	s.mux.HandleFunc("/v1/diagnostics", s.handleDiagnostics)
	s.mux.HandleFunc("/v1/admin/providers/", s.handleAdminProviders)
}

func (s *Server) BuildHandler() http.Handler {
	return LoadSheddingMiddleware(s.shedder)(
		observability.TracingMiddleware(
			observability.MetricsMiddleware(s.metrics, s.logger)(
				panicRecoveryMiddleware(
					LoggingMiddleware(
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
	healthyCount := 0
	for _, p := range providers {
		pv, err := s.manager.Get(p.Name)
		if err == nil {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			hs := pv.Health(ctx)
			cancel()
			if hs.Healthy {
				healthyCount++
			}
		}
	}
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
	writeJSON(w, http.StatusOK, s.manager.UsageStats())
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
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := validateRequest(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.metrics.IncModel(req.Model)
	if req.Stream { s.metrics.StreamRequests.Inc() } else { s.metrics.NonStreamReqs.Inc() }
	providerName := r.URL.Query().Get("provider")
	if providerName == "" {
		providers := s.manager.List()
		if len(providers) == 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no providers configured"})
			return
		}
		providerName = providers[0].Name
	}
	if !req.Stream {
		cacheKey := cache.HashKey(req.Messages[len(req.Messages)-1].Content, req.Model)
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
			s.gracefulDegradation(w, r, &req)
			return
		}
		writeJSON(w, gw.Kind.HTTPStatus(), map[string]string{
			"error": gw.Message, "kind": gw.Kind.String(), "provider": providerName,
		})
		return
	}
	if resp.Usage != nil {
		s.metrics.RecordTokens(providerName, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
	}
	cacheKey := cache.HashKey(req.Messages[len(req.Messages)-1].Content, req.Model)
	s.cache.Set(cacheKey, resp)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) gracefulDegradation(w http.ResponseWriter, r *http.Request, req *provider.GenerateRequest) {
	for _, p := range s.manager.List() {
		resp, err := s.manager.Generate(r.Context(), p.Name, req)
		if err == nil {
			if resp.Usage != nil {
				s.metrics.RecordTokens(p.Name, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
			}
			writeJSON(w, http.StatusOK, resp); return
		}
		if !provider.ClassifyError(p.Name, err, 0).Kind.Retryable() { break }
	}
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": "all providers exhausted"})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request, name string, req *provider.GenerateRequest) {
	p, err := s.manager.Get(name)
	if err != nil { writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()}); return }
	sp, ok := p.(provider.StreamProvider)
	if !ok { writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("%s does not support streaming", name)}); return }
	sse, err := stream.NewSSEWriter(w)
	if err != nil { writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "SSE not supported"}); return }
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
	if pt > 0 || ct > 0 { s.metrics.RecordTokens(name, pt, ct) }
	sse.SendDone()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("gateway: json encode error: %v", err)
	}
}





