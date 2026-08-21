package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/aptshark/gateway/provider"
)

// ── 多级限流接线（2026-08-21）──────────────────────────────────────
// MultiRateLimiter（per-key → per-model → per-provider）此前是孤儿组件:
// 只有 provider 级限流在 manager.Generate 生效。现在接线:
//   - server 持有 keyLimiter（per-key + per-model 两层; provider 层由
//     manager 现有 per-provider 限流负责, 避免双扣）。
//   - 配置: DM_GATEWAY_KEY_LIMIT / DM_GATEWAY_MODEL_LIMIT
//     （格式 "key:rpm/tpm,key2:rpm/tpm", 0=不限）。
//   - Admin: GET /v1/admin/ratelimit 列表; POST 设置; DELETE 清除。

// newKeyLimiterFromEnv builds the server-level MultiRateLimiter from env.
func newKeyLimiterFromEnv() *provider.MultiRateLimiter {
	mrl := provider.NewMultiRateLimiter(0, 0) // provider 层由 manager 负责
	if spec := os.Getenv("DM_GATEWAY_KEY_LIMIT"); spec != "" {
		for _, item := range strings.Split(spec, ",") {
			key, rpm, tpm, ok := parseLimitItem(item)
			if ok && key != "" {
				mrl.SetKeyLimit(key, rpm, tpm)
			}
		}
	}
	if spec := os.Getenv("DM_GATEWAY_MODEL_LIMIT"); spec != "" {
		for _, item := range strings.Split(spec, ",") {
			model, rpm, tpm, ok := parseLimitItem(item)
			if ok && model != "" {
				mrl.SetModelLimit(model, rpm, tpm)
			}
		}
	}
	return mrl
}

// parseLimitItem parses "name:rpm/tpm".
func parseLimitItem(item string) (name string, rpm, tpm int, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(item), ":", 2)
	if len(parts) != 2 {
		return "", 0, 0, false
	}
	name = strings.TrimSpace(parts[0])
	lim := strings.SplitN(parts[1], "/", 2)
	if len(lim) != 2 {
		return "", 0, 0, false
	}
	if _, err := fmt.Sscanf(lim[0], "%d", &rpm); err != nil || rpm < 0 {
		return "", 0, 0, false
	}
	if _, err := fmt.Sscanf(lim[1], "%d", &tpm); err != nil || tpm < 0 {
		return "", 0, 0, false
	}
	return name, rpm, tpm, true
}

// checkKeyLimits applies per-key/per-model limits; returns (allowed, reason).
func (s *Server) checkKeyLimits(r *http.Request, model string, tokenCount int) (bool, string) {
	if s.keyLimiter == nil {
		return true, ""
	}
	key := "anon"
	if h := r.Header.Get("Authorization"); len(h) > 7 {
		key = h[7:]
	}
	return s.keyLimiter.Allow(key, model, tokenCount)
}

type rateLimitReq struct {
	Name string `json:"name"` // key 或 model
	RPM  int    `json:"rpm"`
	TPM  int    `json:"tpm"`
}

// handleAdminRateLimit lists/updates per-key & per-model rate limits.
func (s *Server) handleAdminRateLimit(w http.ResponseWriter, r *http.Request) {
	if s.keyLimiter == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled": true,
			"keys":    s.keyLimiter.KeyLimits(),
			"models":  s.keyLimiter.ModelLimits(),
		})
	case http.MethodPost:
		var req rateLimitReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name required"})
			return
		}
		if req.RPM < 0 || req.TPM < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "rpm/tpm must be >= 0"})
			return
		}
		if strings.Contains(req.Name, " ") {
			s.keyLimiter.SetModelLimit(req.Name, req.RPM, req.TPM)
		} else {
			s.keyLimiter.SetKeyLimit(req.Name, req.RPM, req.TPM)
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "set", "name": req.Name,
			"rpm": req.RPM, "tpm": req.TPM})
	case http.MethodDelete:
		var req rateLimitReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name required"})
			return
		}
		if strings.Contains(req.Name, " ") {
			s.keyLimiter.ClearModelLimit(req.Name)
		} else {
			s.keyLimiter.ClearKeyLimit(req.Name)
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "cleared", "name": req.Name})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// keyLimitEntry mirrors provider.MultiRateLimiter limits for admin display.
type keyLimitEntry struct {
	Name string `json:"name"`
	RPM  int    `json:"rpm"`
	TPM  int    `json:"tpm"`
}

func sortedLimitMap(m map[string][2]int) []keyLimitEntry {
	out := make([]keyLimitEntry, 0, len(m))
	for name, lim := range m {
		out = append(out, keyLimitEntry{Name: name, RPM: lim[0], TPM: lim[1]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
