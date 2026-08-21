package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aptshark/gateway/config"
)

func authServer() http.Handler {
	cfg := config.AuthConfig{
		Enabled:    true,
		APIKeys:    []string{"dm-client"},
		AdminToken: "admin-test",
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return AuthMiddleware(cfg)(next)
}

func TestAuthPublicEndpoints(t *testing.T) {
	for _, path := range []string{"/v1/health", "/v1/metrics", "/v1/stats", "/v1/diagnostics"} {
		req := httptest.NewRequest("GET", path, nil)
		rr := httptest.NewRecorder()
		authServer().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: code = %d, want 200 (public)", path, rr.Code)
		}
	}
}

func TestAuthProtectedRequiresKey(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	rr := httptest.NewRecorder()
	authServer().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rr.Code)
	}

	req = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer dm-client")
	rr = httptest.NewRecorder()
	authServer().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("valid key: code = %d, want 200", rr.Code)
	}
}

func TestAuthAdminRequiresAdminToken(t *testing.T) {
	// api key 访问 admin → 403
	req := httptest.NewRequest("GET", "/v1/admin/ratelimit", nil)
	req.Header.Set("Authorization", "Bearer dm-client")
	rr := httptest.NewRecorder()
	authServer().ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("api key on admin: code = %d, want 403", rr.Code)
	}
	// admin token → 放行
	req = httptest.NewRequest("GET", "/v1/admin/ratelimit", nil)
	req.Header.Set("Authorization", "Bearer admin-test")
	rr = httptest.NewRecorder()
	authServer().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("admin token: code = %d, want 200", rr.Code)
	}
}
