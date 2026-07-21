package server

import (
	"encoding/json"
	"net/http"
)

type RoutingPoolEntry struct {
	Name   string `json:"name"`
	Active bool   `json:"active"`
	HasKey bool   `json:"has_key"`
	InPool bool   `json:"in_pool"`
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
		entries = append(entries, RoutingPoolEntry{
			Name:   p.Name,
			Active: p.Active,
			HasKey: p.KeyConfigured,
			InPool: s.routingPool[p.Name],
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"routing_pool": entries})
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
	s.poolMutex.RLock()
	defer s.poolMutex.RUnlock()

	for name, inPool := range s.routingPool {
		if !inPool { continue }
		for _, p := range s.manager.List() {
			if p.Name == name && p.Active && p.KeyConfigured {
				return name
			}
		}
	}
	// Fallback to first active+key
	for _, p := range s.manager.List() {
		if p.Active && p.KeyConfigured {
			return p.Name
		}
	}
	return ""
}
