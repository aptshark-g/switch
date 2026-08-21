package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aptshark/gateway/cache"
	"github.com/aptshark/gateway/config"
	"github.com/aptshark/gateway/observability"
	"github.com/aptshark/gateway/prefix"
	"github.com/aptshark/gateway/provider"
)

// sseMock 一个 OpenAI 兼容 SSE 上游, 计数请求数。
type sseMock struct {
	hits atomic.Int64
	srv  *httptest.Server
}

func newSSEMock(t *testing.T, delay time.Duration) *sseMock {
	t.Helper()
	m := &sseMock{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.hits.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(
			"data: {\"id\":\"c1\",\"model\":\"mock-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"}}]}\n\n" +
				"data: {\"id\":\"c1\",\"model\":\"mock-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n" +
				"data: [DONE]\n\n"))
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func newIntegrationServer(t *testing.T, m *sseMock) (*Server, *httptest.Server) {
	t.Helper()
	mgr := provider.NewManager()
	mgr.RegisterFactory("openai_compatible", func(cfg provider.ProviderConfig) (provider.Provider, error) {
		return provider.NewOpenAIProvider(cfg)
	})
	if err := mgr.Bootstrap([]provider.ProviderConfig{
		{Name: "mock", Kind: "openai_compatible", BaseURL: m.srv.URL,
			APIKey: "k", Enabled: true, DefaultModel: "mock-model",
			Priority: 1, Weight: 1},
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	authCfg := config.AuthConfig{Enabled: true, APIKeys: []string{"dm-client"},
		AdminToken: "admin-test"}
	s := &Server{
		mux:            http.NewServeMux(),
		manager:        mgr,
		metrics:        observability.NewRegistry(),
		logger:         observability.NewStructuredLogger(),
		cache:          cache.New(1000, 5*time.Minute),
		authCfg:        authCfg,
		shedder:        NewLoadShedder(5000),
		routingPool:    map[string]bool{},
		poolMutex:      sync.RWMutex{},
		healthCache:    map[string]*provider.HealthStatus{},
		healthMu:       sync.RWMutex{},
		costs:          provider.NewCostTracker(""),
		keyLimiter:     provider.NewMultiRateLimiter(0, 0),
		coalescer:      cache.NewCoalescer(),
		prefixProfiler: prefix.NewProfiler(),
		slo:            observability.NewSLOMonitor(observability.DefaultSLOConfig()),
	}
	s.routes()
	return s, httptest.NewServer(s.BuildHandler())
}

func chatRequest(t *testing.T, base, body string, headers map[string]string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", base+"/v1/chat/completions",
		strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return resp, sb.String()
}

func TestIntegrationFullChatPath(t *testing.T) {
	m := newSSEMock(t, 0)
	_, api := newIntegrationServer(t, m)
	body := `{"messages":[{"role":"user","content":"hi"}]}`
	resp, out := chatRequest(t, api.URL, body,
		map[string]string{"Authorization": "Bearer dm-client",
			"Content-Type": "application/json"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, out)
	}
	if !strings.Contains(out, "Hello world") {
		t.Fatalf("content missing 'Hello world': %s", out)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	if m.hits.Load() != 1 {
		t.Fatalf("upstream hits = %d, want 1", m.hits.Load())
	}
}

func TestIntegrationCacheHit(t *testing.T) {
	m := newSSEMock(t, 0)
	s, api := newIntegrationServer(t, m)
	body := `{"messages":[{"role":"user","content":"cache me"}]}`
	h := map[string]string{"Authorization": "Bearer dm-client",
		"Content-Type": "application/json"}
	if _, out := chatRequest(t, api.URL, body, h); out == "" {
		t.Fatal("first call empty")
	}
	if _, out := chatRequest(t, api.URL, body, h); out == "" {
		t.Fatal("second call empty")
	}
	if m.hits.Load() != 1 {
		t.Fatalf("upstream hits = %d, want 1 (2nd from L0 cache)", m.hits.Load())
	}
	if s.metrics.Snapshot().CacheHits != 1 {
		t.Fatalf("cache_hits = %d, want 1", s.metrics.Snapshot().CacheHits)
	}
}

func TestIntegrationCoalescing(t *testing.T) {
	m := newSSEMock(t, 40*time.Millisecond) // 慢上游保证并发重叠
	_, api := newIntegrationServer(t, m)
	body := `{"messages":[{"role":"user","content":"same req"}]}`
	h := map[string]string{"Authorization": "Bearer dm-client",
		"Content-Type": "application/json"}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, out := chatRequest(t, api.URL, body, h)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("status = %d: %s", resp.StatusCode, out)
			}
		}()
	}
	wg.Wait()
	if m.hits.Load() != 1 {
		t.Fatalf("upstream hits = %d, want 1 (5 concurrent coalesced)", m.hits.Load())
	}
}

func TestIntegrationAuthRequired(t *testing.T) {
	m := newSSEMock(t, 0)
	_, api := newIntegrationServer(t, m)
	body := `{"messages":[{"role":"user","content":"hi"}]}`
	resp, _ := chatRequest(t, api.URL, body, map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 without token", resp.StatusCode)
	}
	if m.hits.Load() != 0 {
		t.Fatalf("upstream hits = %d, want 0 (auth rejected before upstream)", m.hits.Load())
	}
}

func TestIntegrationPrefixProfilerRecorded(t *testing.T) {
	m := newSSEMock(t, 0)
	s, api := newIntegrationServer(t, m)
	body := `{"messages":[{"role":"system","content":"You are a stable system prompt."},{"role":"user","content":"track me"}]}`
	h := map[string]string{"Authorization": "Bearer dm-client",
		"Content-Type": "application/json"}
	if _, out := chatRequest(t, api.URL, body, h); out == "" {
		t.Fatal("empty response")
	}
	if s.prefixProfiler.Size() != 1 {
		t.Fatalf("prefix profiler size = %d, want 1", s.prefixProfiler.Size())
	}
	top := s.prefixProfiler.Top(1)
	if len(top) != 1 || top[0].ReqCount != 1 || top[0].HitTokens != 0 {
		t.Fatalf("top = %+v, want req=1 hit=0 (mock 无上游缓存)", top)
	}
}
