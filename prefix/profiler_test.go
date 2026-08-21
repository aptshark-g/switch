package prefix

import (
	"testing"

	"github.com/aptshark/gateway/provider"
)

func testRequest() *provider.GenerateRequest {
	return &provider.GenerateRequest{
		Messages: []provider.Message{
			{Role: "system", Content: "You are a helpful assistant with a long stable system prompt that repeats across requests."},
			{Role: "user", Content: "what is the weather in shanghai"},
		},
	}
}

func TestFingerprintStableAcrossRequests(t *testing.T) {
	a := FingerprintRequest(testRequest())
	b := FingerprintRequest(testRequest())
	if a.FP != b.FP {
		t.Fatalf("same request should produce same fingerprint: %s vs %s", a.FP, b.FP)
	}
	if a.Segments[SegSystem] == a.Segments[SegLast] {
		t.Fatal("system segment should differ from last-message segment")
	}
	if a.TokenLen[SegSystem] <= 0 || a.TokenLen[SegLast] <= 0 {
		t.Fatalf("token lens should be > 0: %v", a.TokenLen)
	}
}

func TestFingerprintChangesOnMessageChange(t *testing.T) {
	a := FingerprintRequest(testRequest())
	req := testRequest()
	req.Messages[1].Content = "what is the weather in beijing"
	b := FingerprintRequest(req)
	if a.FP == b.FP {
		t.Fatal("different last message should change fingerprint")
	}
	if a.Segments[SegSystem] != b.Segments[SegSystem] {
		t.Fatal("system segment should stay stable when only last message changes")
	}
}

func TestHitBlockLayer(t *testing.T) {
	tree := FingerprintRequest(testRequest())
	// system 段长度 > 0; 命中全在 system 段 → layer 0
	if got := HitBlockLayer(tree, tree.TokenLen[SegSystem]-1); got != SegSystem {
		t.Fatalf("hit within system = layer %d, want %d", got, SegSystem)
	}
	// 命中超过 system+history → 最后层
	total := tree.TokenLen[0] + tree.TokenLen[1] + tree.TokenLen[2]
	if got := HitBlockLayer(tree, total); got != SegLast {
		t.Fatalf("hit full = layer %d, want %d", got, SegLast)
	}
}

func TestProfilerObserveAndStats(t *testing.T) {
	p := NewProfiler()
	tree := FingerprintRequest(testRequest())
	for i := 0; i < 3; i++ {
		p.Observe(tree, 100, 20)
	}
	if p.Size() != 1 {
		t.Fatalf("size = %d, want 1", p.Size())
	}
	top := p.Top(5)
	if len(top) != 1 || top[0].ReqCount != 3 || top[0].HitTokens != 300 {
		t.Fatalf("top = %+v, want req=3 hit=300", top)
	}
	if p.IsHot(tree.FP, 3) != true {
		t.Fatal("req=3 >= min 3 should be hot")
	}
	if p.IsHot(tree.FP, 4) != false {
		t.Fatal("req=3 < min 4 should not be hot")
	}
}

func TestProfilerNilTreeIgnored(t *testing.T) {
	p := NewProfiler()
	p.Observe(nil, 1, 1)
	if p.Size() != 0 {
		t.Fatalf("nil tree should be ignored, size = %d", p.Size())
	}
}
