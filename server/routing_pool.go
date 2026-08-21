package server

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type RoutingPoolEntry struct {
	Name            string  `json:"name"`
	Active          bool    `json:"active"`
	HasKey          bool    `json:"has_key"`
	InPool          bool    `json:"in_pool"`
	Priority        int     `json:"priority"`
	Weight          int     `json:"weight"`
	Score           float64 `json:"score,omitempty"`
	Circuit         string  `json:"circuit,omitempty"`
	Health          string  `json:"health,omitempty"`
	HealthLatencyMs int64   `json:"health_latency_ms,omitempty"`
}

func (s *Server) handleRoutingPool(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listRoutingPool(w, r)
	case http.MethodPost:
		s.toggleRoutingPool(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) listRoutingPool(w http.ResponseWriter, r *http.Request) {
	s.poolMutex.RLock()
	defer s.poolMutex.RUnlock()

	providers := s.manager.List()
	var entries []RoutingPoolEntry
	for _, p := range providers {
		health := "unknown"
		var latency int64
		if hs := s.getHealth(p.Name); hs != nil {
			if hs.Healthy {
				health = "healthy"
			} else {
				health = "unhealthy"
			}
			latency = hs.LatencyMs
		}
		score := 0.0
		if p.Active && p.KeyConfigured {
			if cfg, ok := s.manager.Config(p.Name); ok {
				score = s.effectiveWeight(p, cfg, 0)
			}
		}
		entries = append(entries, RoutingPoolEntry{
			Name:            p.Name,
			Active:          p.Active,
			HasKey:          p.KeyConfigured,
			InPool:          s.routingPool[p.Name],
			Priority:        p.Priority,
			Weight:          p.Weight,
			Score:           score,
			Circuit:         string(p.Circuit),
			Health:          health,
			HealthLatencyMs: latency,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"strategy":      "weighted_random",
		"routing_pool":  entries,
		"routing_rules": s.routingRules,
	})
}

type toggleRoutingReq struct {
	Provider string `json:"provider"`
	Action   string `json:"action"`
}

func (s *Server) toggleRoutingPool(w http.ResponseWriter, r *http.Request) {
	var req toggleRoutingReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if req.Provider == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provider required"})
		return
	}

	s.poolMutex.Lock()
	defer s.poolMutex.Unlock()

	if s.routingPool == nil {
		s.routingPool = make(map[string]bool)
	}

	switch req.Action {
	case "add":
		providers := s.manager.List()
		for _, p := range providers {
			if p.Name == req.Provider {
				if !p.Active || !p.KeyConfigured {
					writeJSON(w, http.StatusBadRequest, map[string]string{
						"error": "provider must be active and have key",
					})
					return
				}
				s.routingPool[req.Provider] = true
				writeJSON(w, http.StatusOK, map[string]string{
					"status": "added", "provider": req.Provider,
				})
				return
			}
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})

	case "remove":
		delete(s.routingPool, req.Provider)
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "removed", "provider": req.Provider,
		})

	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "action must be add or remove",
		})
	}
}

func (s *Server) getRoutingProvider() string {
	// 2026-08-21: 原"首个匹配" → 智能加权随机（priority 分层 × weight ×
	// health × latency × cost; 熔断 OPEN 不进首选）。
	return s.selectFromPool()
}

// handlePrefixStats B-1 前缀命中观测端点（TopN 按 hit_tokens 排序）。
func (s *Server) handlePrefixStats(w http.ResponseWriter, r *http.Request) {
	n := 20
	if v := r.URL.Query().Get("top"); v != "" {
		var parsed int
		if _, err := fmt.Sscanf(v, "%d", &parsed); err == nil &&
			parsed > 0 && parsed <= 200 {
			n = parsed
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tracked_prefixes": s.prefixProfiler.Size(),
		"window_minutes":   15,
		"top":              s.prefixProfiler.Top(n),
	})
}
