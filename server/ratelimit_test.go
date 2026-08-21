package server

import (
	"net/http/httptest"
	"testing"

	"github.com/aptshark/gateway/provider"
)

func TestParseLimitItem(t *testing.T) {
	cases := []struct {
		item     string
		name     string
		rpm, tpm int
		ok       bool
	}{
		{"dm-client:60/100000", "dm-client", 60, 100000, true},
		{"deepseek-v4-pro:10/5000", "deepseek-v4-pro", 10, 5000, true},
		{"bad", "", 0, 0, false},
		{"key:abc/10", "", 0, 0, false},
	}
	for _, c := range cases {
		name, rpm, tpm, ok := parseLimitItem(c.item)
		if ok != c.ok || name != c.name || rpm != c.rpm || tpm != c.tpm {
			t.Fatalf("parseLimitItem(%q) = (%q,%d,%d,%v), want (%q,%d,%d,%v)",
				c.item, name, rpm, tpm, ok, c.name, c.rpm, c.tpm, c.ok)
		}
	}
}

func TestCheckKeyLimits(t *testing.T) {
	mrl := provider.NewMultiRateLimiter(0, 0)
	mrl.SetKeyLimit("k1", 1, 100000)
	s := &Server{keyLimiter: mrl}

	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer k1")
	if ok, _ := s.checkKeyLimits(req, "m", 10); !ok {
		t.Fatal("first call should pass")
	}
	if ok, reason := s.checkKeyLimits(req, "m", 10); ok || reason != "key_rate_limit" {
		t.Fatalf("second call ok=%v reason=%s, want key_rate_limit", ok, reason)
	}

	anon := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	if ok, _ := s.checkKeyLimits(anon, "m", 10); !ok {
		t.Fatal("anon key (no limit) should pass")
	}
}

func TestCheckKeyLimitsDisabled(t *testing.T) {
	s := &Server{keyLimiter: nil}
	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	if ok, reason := s.checkKeyLimits(req, "m", 10); !ok || reason != "" {
		t.Fatalf("disabled limiter: ok=%v reason=%s, want true/''", ok, reason)
	}
}
