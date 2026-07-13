package server

import ("net/http"; "strings"; "github.com/aptshark/gateway/config")

// AuthConfig controls API authentication.


// AuthMiddleware validates Bearer tokens on protected routes.
// Health endpoints are always public. Admin endpoints require the admin token.
func AuthMiddleware(cfg config.AuthConfig) func(http.Handler) http.Handler {
	if !cfg.Enabled || len(cfg.APIKeys) == 0 {
		return func(next http.Handler) http.Handler { return next }
	}

	keySet := make(map[string]bool, len(cfg.APIKeys))
	for _, k := range cfg.APIKeys {
		keySet[k] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Health endpoints are always public.
			if strings.HasPrefix(r.URL.Path, "/v1/health") ||
				strings.HasPrefix(r.URL.Path, "/v1/metrics") ||
				strings.HasPrefix(r.URL.Path, "/v1/stats") {
				next.ServeHTTP(w, r)
				return
			}

			token := extractBearer(r)

			// Admin endpoints require admin token.
			if strings.HasPrefix(r.URL.Path, "/v1/admin") {
				if cfg.AdminToken == "" || token != cfg.AdminToken {
					writeJSON(w, http.StatusForbidden, map[string]string{
						"error": "admin token required",
					})
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			// All other endpoints require any valid API key.
			if token == "" || !keySet[token] {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeJSON(w, http.StatusUnauthorized, map[string]string{
					"error": "valid API key required",
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func extractBearer(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	return parts[1]
}
