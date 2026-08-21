package prefix

import (
	"errors"
	"testing"
	"time"

	"github.com/aptshark/gateway/provider"
)

func warmScheduler(t *testing.T, opts ...func(*WarmupConfig)) (*WarmScheduler, *Profiler, *int) {
	t.Helper()
	prof := NewProfiler()
	cfg := WarmupConfig{
		Enabled:          true,
		TriggerReq:       1,
		TriggerHitTokens: 0,
		IdleAfter:        -time.Second, // 恒空闲（测试）
		TailToken:        ".",
		GlobalCapRatio:   0.02,
		MaxWarmPerTick:   10,
	}
	for _, o := range opts {
		o(&cfg)
	}
	var warmed int
	w := NewWarmScheduler(cfg, prof, NewQuotaCoordinator(100, 0.10),
		func(fp, tenant string) string { return "p1" },
		func(p string, req *provider.GenerateRequest) (*provider.GenerateResponse, error) {
			warmed++
			return &provider.GenerateResponse{
				Usage: &provider.TokenUsage{PromptTokens: 10,
					PromptTokensDetails: &provider.PromptTokensDetails{CachedTokens: 0}},
			}, nil
		},
		func() int64 { return 1_000_000 },
	)
	w.warmRequests.Store(0)
	w.warmLate.Store(0)
	w.warmInputTokens.Store(0)
	// late 计数回调
	return w, prof, &warmed
}

func TestWarmupHotIdleFires(t *testing.T) {
	w, prof, warmed := warmScheduler(t)
	tree := FingerprintRequest(&provider.GenerateRequest{
		Messages: []provider.Message{
			{Role: "system", Content: "stable system"},
			{Role: "user", Content: "hello"},
		},
	})
	prof.ObserveFull(tree, 100, 10, "tenant-1", []provider.Message{
		{Role: "system", Content: "stable system"},
		{Role: "user", Content: "hello"},
	})
	if err := w.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if *warmed != 1 {
		t.Fatalf("warmed = %d, want 1", *warmed)
	}
	if c := w.Counters(); c["requests"] != 1 || c["late"] != 1 {
		t.Fatalf("counters = %v, want requests=1 late=1 (cached=0 → 迟到)", c)
	}
}

func TestWarmupSkipsNotHot(t *testing.T) {
	w, prof, warmed := warmScheduler(t, func(c *WarmupConfig) { c.TriggerReq = 5 })
	tree := FingerprintRequest(&provider.GenerateRequest{
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	// ReqCount=1 < 5 → 不热
	prof.ObserveFull(tree, 0, 5, "t", []provider.Message{{Role: "user", Content: "hi"}})
	if err := w.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if *warmed != 0 {
		t.Fatalf("warmed = %d, want 0 (not hot)", *warmed)
	}
}

func TestWarmupSkipsNoPayload(t *testing.T) {
	w, prof, warmed := warmScheduler(t)
	tree := FingerprintRequest(&provider.GenerateRequest{
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	// Observe（无载荷）→ ReqCount=1 热, 但 LastMessages 空 → 跳过
	for i := 0; i < 2; i++ {
		prof.Observe(tree, 0, 5)
	}
	if err := w.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if *warmed != 0 {
		t.Fatalf("warmed = %d, want 0 (no payload stored)", *warmed)
	}
}

func TestWarmupGenerateError(t *testing.T) {
	w, prof, warmed := warmScheduler(t)
	w.generate = func(p string, req *provider.GenerateRequest) (*provider.GenerateResponse, error) {
		return nil, errors.New("upstream down")
	}
	tree := FingerprintRequest(&provider.GenerateRequest{
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	prof.ObserveFull(tree, 0, 0, "t", []provider.Message{{Role: "user", Content: "hi"}})
	// ReqCount=1 热（TriggerReq=1）; generate 报错 → warmed 不增
	if err := w.Tick(); err != nil {
		t.Fatalf("tick should not fail on warm error: %v", err)
	}
	if *warmed != 0 {
		t.Fatalf("warmed = %d, want 0 (error)", *warmed)
	}
}

func TestWarmupDisabled(t *testing.T) {
	w, prof, warmed := warmScheduler(t, func(c *WarmupConfig) { c.Enabled = false })
	tree := FingerprintRequest(&provider.GenerateRequest{
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	prof.ObserveFull(tree, 0, 0, "t", []provider.Message{{Role: "user", Content: "hi"}})
	if err := w.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if *warmed != 0 {
		t.Fatalf("warmed = %d, want 0 (disabled)", *warmed)
	}
}
